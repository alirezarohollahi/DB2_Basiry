/*
===============================================================================
 Finance MART 2 FIRST LOAD ETL - Synced with newest DW/STG architecture

 Rules preserved:
   - Procedures still use persistent etl_work tables instead of #temp tables.
   - Type 1 dimensions still refresh by work table -> TRUNCATE dimension -> INSERT.
   - Transaction/event facts remain append-only in incremental mode.
   - Monthly snapshot keeps the existing month loop.
   - Donation lifecycle still rebuilds with TRUNCATE + INSERT because its shape is
     now derived from current staging history and no longer stores source IDs in DW.

 Sync changes:
   - No source_* columns are inserted into DW facts.
   - No source_system column is inserted into DW facts or dimensions.
   - Allocation-type dimension/key logic is removed.
   - Donation/expense/payment facts exclude pending rows.
   - Donation lifecycle uses min_donation, max_donation, avg_donation instead of
     days_to_confirm/days_to_allocate.
===============================================================================
*/
USE Charity_DW_DB;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_assert_finance_mart2_prerequisites
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
        THROW 52000, 'Missing Charity_DW_DB.etl_admin.etl_batch.', 1;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
        THROW 52001, 'Missing Charity_DW_DB.etl_admin.etl_load_log.', 1;

    IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL AND OBJECT_ID(N'dw.DimDate', N'U') IS NULL
        THROW 52002, 'Missing DW date dimension.', 1;

    IF OBJECT_ID(N'dw.dim_center', N'U') IS NULL
        THROW 52003, 'Missing dw.dim_center.', 1;

    IF OBJECT_ID(N'dw.dim_child', N'U') IS NULL
        THROW 52004, 'Missing dw.dim_child.', 1;

    IF OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.donors', N'U') IS NULL
        THROW 52005, 'Missing staging finance tables.', 1;

    IF OBJECT_ID(N'etl_work.tmp_dim_donor_load', N'U') IS NULL
        THROW 52006, 'Missing etl_work tables. Run 15_create_dw_finance_etl_work_tables_fixed.sql first.', 1;

    IF OBJECT_ID(N'etl_work.fact_source_load_map', N'U') IS NULL
        THROW 52007, 'Missing etl_work.fact_source_load_map. Run 15_create_dw_finance_etl_work_tables_fixed.sql first.', 1;
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_dw_start_batch
      @target_layer NVARCHAR(100),
      @mart_name    NVARCHAR(100) = N'FINANCE_MART2',
      @etl_batch_id INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO Charity_DW_DB.etl_admin.etl_batch
    (
          source_system,
          target_layer,
          mart_name,
          batch_status,
          started_at,
          rows_read,
          rows_inserted,
          rows_updated,
          rows_rejected,
          created_by
    )
    VALUES
    (
          N'FINANCE_OPS',
          @target_layer,
          @mart_name,
          N'running',
          SYSDATETIME(),
          0,
          0,
          0,
          0,
          COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL')
    );

    SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_dw_log_step
      @etl_batch_id    INT,
      @source_table    NVARCHAR(128),
      @target_table    NVARCHAR(128),
      @load_status     NVARCHAR(50),
      @rows_read       INT = 0,
      @rows_inserted   INT = 0,
      @rows_updated    INT = 0,
      @rows_rejected   INT = 0,
      @started_at      DATETIME2(0) = NULL,
      @message         NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
    (
          etl_batch_id,
          source_database,
          source_schema,
          source_table,
          target_database,
          target_schema,
          target_table,
          load_status,
          rows_read,
          rows_inserted,
          rows_updated,
          rows_rejected,
          started_at,
          ended_at,
          message
    )
    VALUES
    (
          @etl_batch_id,
          N'Stg_FinanceOps_DB',
          N'stg_finance_ops',
          @source_table,
          N'Charity_DW_DB',
          N'dw',
          @target_table,
          @load_status,
          @rows_read,
          @rows_inserted,
          @rows_updated,
          @rows_rejected,
          ISNULL(@started_at, SYSDATETIME()),
          SYSDATETIME(),
          @message
    );
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_dw_finish_batch
      @etl_batch_id    INT,
      @batch_status    NVARCHAR(50),
      @rows_read       INT = 0,
      @rows_inserted   INT = 0,
      @rows_updated    INT = 0,
      @rows_rejected   INT = 0,
      @error_message   NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE Charity_DW_DB.etl_admin.etl_batch
       SET batch_status  = @batch_status,
           ended_at       = SYSDATETIME(),
           rows_read      = ISNULL(@rows_read, 0),
           rows_inserted  = ISNULL(@rows_inserted, 0),
           rows_updated   = ISNULL(@rows_updated, 0),
           rows_rejected  = ISNULL(@rows_rejected, 0),
           error_message  = @error_message
     WHERE etl_batch_id = @etl_batch_id;
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_donor
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @step_started DATETIME2(0);
    EXEC etl_admin.usp_dw_start_batch @target_layer=N'DW_DIMENSION', @mart_name=N'FINANCE_MART2', @etl_batch_id=@etl_batch_id OUTPUT;

    BEGIN TRY
        SET @step_started = SYSDATETIME();
        TRUNCATE TABLE etl_work.tmp_dim_donor_load;

        INSERT INTO etl_work.tmp_dim_donor_load
        (existing_key, donor_id, full_name, donor_type, is_active, row_hash, created_at, updated_at)
        VALUES (-1, -1, N'Unknown', N'unknown', 0, NULL, SYSDATETIME(), NULL);

        ;WITH src AS (
            SELECT id, full_name, donor_type, is_active, row_hash, created_at, updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.donors
            WHERE is_valid = 1 AND id IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_donor_load
        (existing_key, donor_id, full_name, donor_type, is_active, row_hash, created_at, updated_at)
        SELECT d.donor_key, s.id, s.full_name, s.donor_type, s.is_active, s.row_hash, s.created_at, s.updated_at
        FROM src s
        FULL JOIN dw.dim_donor d ON d.donor_id = s.id AND d.donor_key <> -1
        WHERE s.id IS NOT NULL;

        SELECT @rows_read = COUNT(*) FROM etl_work.tmp_dim_donor_load WHERE donor_id <> -1;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donors', N'dim_donor', N'work_ready', @rows_read, 0, 0, 0, @step_started, N'Donor Type 1 dimension work table prepared.';

        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_donor;

            SET IDENTITY_INSERT dw.dim_donor ON;
            INSERT INTO dw.dim_donor
            (donor_key, donor_id, full_name, donor_type, is_active, row_hash, created_at, updated_at)
            SELECT existing_key, donor_id, full_name, donor_type, is_active, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_donor_load
            WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_donor OFF;

            INSERT INTO dw.dim_donor
            (donor_id, full_name, donor_type, is_active, row_hash, created_at, updated_at)
            SELECT donor_id, full_name, donor_type, is_active, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_donor_load
            WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT TRANSACTION;

        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'etl_work.tmp_dim_donor_load', N'dim_donor', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Donor dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_campaign
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @step_started DATETIME2(0);
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        SET @step_started = SYSDATETIME();
        TRUNCATE TABLE etl_work.tmp_dim_campaign_load;
        INSERT INTO etl_work.tmp_dim_campaign_load
        (existing_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, row_hash, created_at, updated_at)
        VALUES (-1, -1, N'Unknown', N'unknown', NULL, NULL, NULL, NULL, SYSDATETIME(), NULL);

        ;WITH src AS (
            SELECT id, title, status, target_amount, start_date, end_date, row_hash, created_at, updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns
            WHERE is_valid = 1 AND id IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_campaign_load
        (existing_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, row_hash, created_at, updated_at)
        SELECT d.campaign_key, s.id, s.title, s.status, s.target_amount, s.start_date, s.end_date, s.row_hash, s.created_at, s.updated_at
        FROM src s
        FULL JOIN dw.dim_campaign d ON d.campaign_id = s.id AND d.campaign_key <> -1
        WHERE s.id IS NOT NULL;
        SELECT @rows_read = COUNT(*) FROM etl_work.tmp_dim_campaign_load WHERE campaign_id <> -1;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'campaigns', N'dim_campaign', N'work_ready', @rows_read, 0, 0, 0, @step_started, N'Campaign Type 1 dimension work table prepared.';

        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_campaign;
            SET IDENTITY_INSERT dw.dim_campaign ON;
            INSERT INTO dw.dim_campaign
            (campaign_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, row_hash, created_at, updated_at)
            SELECT existing_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_campaign_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_campaign OFF;
            INSERT INTO dw.dim_campaign
            (campaign_id, title, campaign_status, target_amount, start_date, end_date, row_hash, created_at, updated_at)
            SELECT campaign_id, title, campaign_status, target_amount, start_date, end_date, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_campaign_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT TRANSACTION;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'etl_work.tmp_dim_campaign_load', N'dim_campaign', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Campaign dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_category
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @step_started DATETIME2(0);
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        SET @step_started = SYSDATETIME();
        TRUNCATE TABLE etl_work.tmp_dim_category_load;
        INSERT INTO etl_work.tmp_dim_category_load
        (existing_key, category_id, category_name, parent_category_id, parent_category_name, category_status, row_hash, created_at, updated_at)
        VALUES (-1, -1, N'Unknown', NULL, NULL, N'unknown', NULL, SYSDATETIME(), NULL);

        ;WITH src AS (
            SELECT c.id,
                   c.name,
                   CASE WHEN ISNULL(c.is_active, 0)=1 THEN N'active' ELSE N'inactive' END AS category_status,
                   c.row_hash,
                   c.created_at,
                   c.updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories c
            WHERE c.is_valid = 1 AND c.id IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_category_load
        (existing_key, category_id, category_name, parent_category_id, parent_category_name, category_status, row_hash, created_at, updated_at)
        SELECT d.category_key, s.id, s.name, NULL, NULL, s.category_status, s.row_hash, s.created_at, s.updated_at
        FROM src s
        FULL JOIN dw.dim_category d ON d.category_id = s.id AND d.category_key <> -1
        WHERE s.id IS NOT NULL;
        SELECT @rows_read = COUNT(*) FROM etl_work.tmp_dim_category_load WHERE category_id <> -1;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expense_categories', N'dim_category', N'work_ready', @rows_read, 0, 0, 0, @step_started, N'Category dimension prepared from new non-hierarchical STG category structure.';

        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_category;
            SET IDENTITY_INSERT dw.dim_category ON;
            INSERT INTO dw.dim_category
            (category_key, category_id, category_name, parent_category_id, parent_category_name, category_status, row_hash, created_at, updated_at)
            SELECT existing_key, category_id, category_name, parent_category_id, parent_category_name, category_status, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_category_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_category OFF;
            INSERT INTO dw.dim_category
            (category_id, category_name, parent_category_id, parent_category_name, category_status, row_hash, created_at, updated_at)
            SELECT category_id, category_name, parent_category_id, parent_category_name, category_status, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_category_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT TRANSACTION;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'etl_work.tmp_dim_category_load', N'dim_category', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Category dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_donation_type
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_donation_type_load;
        INSERT INTO etl_work.tmp_dim_donation_type_load(existing_key, code, title, created_at, updated_at)
        VALUES(-1, N'unknown', N'Unknown', SYSDATETIME(), NULL);
        ;WITH src AS (
            SELECT DISTINCT LOWER(LTRIM(RTRIM(donation_type))) AS code,
                   LTRIM(RTRIM(donation_type)) AS title
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations
            WHERE is_valid=1 AND NULLIF(LTRIM(RTRIM(donation_type)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_donation_type_load(existing_key, code, title, created_at, updated_at)
        SELECT d.donation_type_key, s.code, s.title, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_donation_type d ON d.code = s.code AND d.donation_type_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_donation_type_load WHERE code<>N'unknown';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_donation_type;
            SET IDENTITY_INSERT dw.dim_donation_type ON;
            INSERT INTO dw.dim_donation_type(donation_type_key, code, title, created_at, updated_at)
            SELECT existing_key, code, title, created_at, updated_at FROM etl_work.tmp_dim_donation_type_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_donation_type OFF;
            INSERT INTO dw.dim_donation_type(code, title, created_at, updated_at)
            SELECT code, title, created_at, updated_at FROM etl_work.tmp_dim_donation_type_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations.donation_type', N'dim_donation_type', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Donation type dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_status
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_status_load;
        INSERT INTO etl_work.tmp_dim_status_load(existing_key, status_type, code, title, category, created_at, updated_at)
        VALUES(-1, N'unknown', N'unknown', N'Unknown', N'unknown', SYSDATETIME(), NULL);
        ;WITH raw_status AS (
            SELECT v.status_type, v.code
            FROM (VALUES
                (N'donation', N'pending'),
                (N'donation', N'confirmed'),
                (N'donation', N'rejected'),
                (N'donation', N'refunded'),
                (N'expense',  N'pending'),
                (N'expense',  N'approved'),
                (N'expense',  N'rejected'),
                (N'payment',  N'pending'),
                (N'payment',  N'approved'),
                (N'payment',  N'paid'),
                (N'payment',  N'cancelled'),
                (N'payment',  N'rejected')
            ) v(status_type, code)
            UNION
            SELECT N'campaign' AS status_type, status AS code
            FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns
            WHERE is_valid=1 AND NULLIF(LTRIM(RTRIM(status)), N'') IS NOT NULL
        ), src AS (
            SELECT DISTINCT status_type,
                   LOWER(LTRIM(RTRIM(code))) AS code,
                   LTRIM(RTRIM(code)) AS title,
                   status_type AS category
            FROM raw_status
            WHERE NULLIF(LTRIM(RTRIM(code)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_status_load(existing_key, status_type, code, title, category, created_at, updated_at)
        SELECT d.status_key, s.status_type, s.code, s.title, s.category, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_status d ON d.status_type = s.status_type AND d.code = s.code AND d.status_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_status_load WHERE code<>N'unknown';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_status;
            SET IDENTITY_INSERT dw.dim_status ON;
            INSERT INTO dw.dim_status(status_key, status_type, code, title, category, created_at, updated_at)
            SELECT existing_key, status_type, code, title, category, created_at, updated_at FROM etl_work.tmp_dim_status_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_status OFF;
            INSERT INTO dw.dim_status(status_type, code, title, category, created_at, updated_at)
            SELECT status_type, code, title, category, created_at, updated_at FROM etl_work.tmp_dim_status_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'status_sources', N'dim_status', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Status dimension refreshed with explicit donation/expense/payment status domains.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_currency
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_currency_load;
        INSERT INTO etl_work.tmp_dim_currency_load(existing_key, code, name, created_at, updated_at)
        VALUES(-1, N'UNK', N'Unknown', SYSDATETIME(), NULL);
        ;WITH raw_currency AS (
            SELECT currency AS code FROM Stg_FinanceOps_DB.stg_finance_ops.donations WHERE is_valid=1
            UNION SELECT currency FROM Stg_FinanceOps_DB.stg_finance_ops.expenses WHERE is_valid=1
            UNION SELECT currency FROM Stg_FinanceOps_DB.stg_finance_ops.payments WHERE is_valid=1
            UNION SELECT from_currency FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates WHERE is_valid=1
            UNION SELECT to_currency FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates WHERE is_valid=1
        ), src AS (
            SELECT DISTINCT UPPER(LTRIM(RTRIM(code))) AS code,
                   UPPER(LTRIM(RTRIM(code))) AS name
            FROM raw_currency
            WHERE NULLIF(LTRIM(RTRIM(code)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_currency_load(existing_key, code, name, created_at, updated_at)
        SELECT d.currency_key, s.code, s.name, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_currency d ON d.code = s.code AND d.currency_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_currency_load WHERE code<>N'UNK';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_currency;
            SET IDENTITY_INSERT dw.dim_currency ON;
            INSERT INTO dw.dim_currency(currency_key, code, name, created_at, updated_at)
            SELECT existing_key, code, name, created_at, updated_at FROM etl_work.tmp_dim_currency_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_currency OFF;
            INSERT INTO dw.dim_currency(code, name, created_at, updated_at)
            SELECT code, name, created_at, updated_at FROM etl_work.tmp_dim_currency_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'currency_sources', N'dim_currency', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Currency dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_donation_transaction
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_donation_transaction_load;
        INSERT INTO etl_work.tmp_fact_donation_transaction_load
        (date_key, donor_key, campaign_key, center_key, donation_type_key, currency_key, status_key, amount, is_confirmed, is_refunded,
         source_donation_id, etl_batch_id, loaded_at)
        SELECT
            ISNULL(dd.TimeKey, -1),
            ISNULL(donor.donor_key, -1),
            ISNULL(camp.campaign_key, -1),
            -1,
            ISNULL(dt.donation_type_key, -1),
            ISNULL(cur.currency_key, -1),
            ISNULL(st.status_key, -1),
            d.amount,
            CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) = N'confirmed' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) = N'refunded' THEN 1 ELSE 0 END,
            d.id,
            @etl_batch_id,
            SYSDATETIME()
        FROM Stg_FinanceOps_DB.stg_finance_ops.donations d
        INNER JOIN (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) d_latest ON d_latest.max_stg_row_id = d.stg_row_id
        LEFT JOIN dw.dim_date dd ON dd.FullDateAlternateKey = d.donation_date
        LEFT JOIN (SELECT donor_id, MIN(donor_key) AS donor_key FROM dw.dim_donor GROUP BY donor_id) donor ON donor.donor_id = d.donor_id
        LEFT JOIN (SELECT campaign_id, MIN(campaign_key) AS campaign_key FROM dw.dim_campaign GROUP BY campaign_id) camp ON camp.campaign_id = d.campaign_id
        LEFT JOIN (SELECT code, MIN(donation_type_key) AS donation_type_key FROM dw.dim_donation_type GROUP BY code) dt ON dt.code = LOWER(LTRIM(RTRIM(d.donation_type)))
        LEFT JOIN (SELECT code, MIN(currency_key) AS currency_key FROM dw.dim_currency GROUP BY code) cur ON cur.code = UPPER(LTRIM(RTRIM(d.currency)))
        LEFT JOIN (SELECT status_type, code, MIN(status_key) AS status_key FROM dw.dim_status GROUP BY status_type, code) st ON st.status_type = N'donation' AND st.code = LOWER(LTRIM(RTRIM(d.status)))
        WHERE d.is_valid = 1
          AND d.id IS NOT NULL
          AND LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) IN (N'confirmed', N'rejected', N'refunded')
          AND CONVERT(DATETIME2(0), d.donation_date) >= @start_time
          AND CONVERT(DATETIME2(0), d.donation_date) < @end_time;

        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_donation_transaction_load;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_donation_transaction;
            DBCC CHECKIDENT ('dw.fact_donation_transaction', RESEED, -1);
            DELETE FROM etl_work.fact_source_load_map WHERE fact_name=N'fact_donation_transaction' AND source_table=N'donations';
            INSERT INTO dw.fact_donation_transaction
            (date_key, donor_key, campaign_key, center_key, donation_type_key, currency_key, status_key, amount, is_confirmed, is_refunded, etl_batch_id, loaded_at)
            SELECT t.date_key, t.donor_key, t.campaign_key, t.center_key, t.donation_type_key, t.currency_key, t.status_key, t.amount, t.is_confirmed, t.is_refunded, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_donation_transaction_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_donation_transaction'
                  AND m.source_table=N'donations'
                  AND m.source_id=t.source_donation_id
            );
            SET @rows_inserted=@@ROWCOUNT;

            INSERT INTO etl_work.fact_source_load_map(fact_name, source_table, source_id, loaded_etl_batch_id, loaded_at)
            SELECT N'fact_donation_transaction', N'donations', t.source_donation_id, @etl_batch_id, SYSDATETIME()
            FROM etl_work.tmp_fact_donation_transaction_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_donation_transaction'
                  AND m.source_table=N'donations'
                  AND m.source_id=t.source_donation_id
            );
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'fact_donation_transaction', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Donation fact loaded without pending rows and without source columns in DW.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_expense_transaction
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_expense_transaction_load;
        INSERT INTO etl_work.tmp_fact_expense_transaction_load
        (date_key, center_key, child_key, category_key, currency_key, status_key, amount, is_approved, is_rejected,
         source_expense_id, etl_batch_id, loaded_at)
        SELECT ISNULL(dd.TimeKey,-1), ISNULL(c.center_key,-1), ISNULL(ch.child_key,-1), ISNULL(cat.category_key,-1), ISNULL(cur.currency_key,-1), ISNULL(st.status_key,-1),
               e.amount,
               CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(e.status,N'')))) = N'approved' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(e.status,N'')))) = N'rejected' THEN 1 ELSE 0 END,
               e.id, @etl_batch_id, SYSDATETIME()
        FROM Stg_FinanceOps_DB.stg_finance_ops.expenses e
        INNER JOIN (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.expenses
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) e_latest ON e_latest.max_stg_row_id = e.stg_row_id
        LEFT JOIN dw.dim_date dd ON dd.FullDateAlternateKey = e.expense_date
        LEFT JOIN (SELECT center_id, MIN(center_key) AS center_key FROM dw.dim_center GROUP BY center_id) c ON c.center_id = e.center_id
        LEFT JOIN (SELECT child_id, MIN(child_key) AS child_key FROM dw.dim_child GROUP BY child_id) ch ON ch.child_id = e.child_id
        LEFT JOIN (SELECT category_id, MIN(category_key) AS category_key FROM dw.dim_category GROUP BY category_id) cat ON cat.category_id = e.category_id
        LEFT JOIN (SELECT code, MIN(currency_key) AS currency_key FROM dw.dim_currency GROUP BY code) cur ON cur.code = UPPER(LTRIM(RTRIM(e.currency)))
        LEFT JOIN (SELECT status_type, code, MIN(status_key) AS status_key FROM dw.dim_status GROUP BY status_type, code) st ON st.status_type=N'expense' AND st.code = LOWER(LTRIM(RTRIM(e.status)))
        WHERE e.is_valid=1
          AND e.id IS NOT NULL
          AND LOWER(LTRIM(RTRIM(ISNULL(e.status,N'')))) IN (N'approved', N'rejected')
          AND CONVERT(DATETIME2(0), e.expense_date) >= @start_time
          AND CONVERT(DATETIME2(0), e.expense_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_expense_transaction_load;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_expense_transaction;
            DBCC CHECKIDENT ('dw.fact_expense_transaction', RESEED, -1);
            DELETE FROM etl_work.fact_source_load_map WHERE fact_name=N'fact_expense_transaction' AND source_table=N'expenses';
            INSERT INTO dw.fact_expense_transaction
            (date_key, center_key, child_key, category_key, currency_key, status_key, amount, is_approved, is_rejected, etl_batch_id, loaded_at)
            SELECT t.date_key, t.center_key, t.child_key, t.category_key, t.currency_key, t.status_key, t.amount, t.is_approved, t.is_rejected, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_expense_transaction_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_expense_transaction'
                  AND m.source_table=N'expenses'
                  AND m.source_id=t.source_expense_id
            );
            SET @rows_inserted=@@ROWCOUNT;

            INSERT INTO etl_work.fact_source_load_map(fact_name, source_table, source_id, loaded_etl_batch_id, loaded_at)
            SELECT N'fact_expense_transaction', N'expenses', t.source_expense_id, @etl_batch_id, SYSDATETIME()
            FROM etl_work.tmp_fact_expense_transaction_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_expense_transaction'
                  AND m.source_table=N'expenses'
                  AND m.source_id=t.source_expense_id
            );
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expenses', N'fact_expense_transaction', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Expense fact loaded without pending rows and without description/source columns in DW.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_payment_transaction
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_payment_transaction_load;
        INSERT INTO etl_work.tmp_fact_payment_transaction_load
        (date_key, center_key, currency_key, status_key, payment_type, amount, is_paid, is_cancelled,
         source_payment_id, etl_batch_id, loaded_at)
        SELECT ISNULL(dd.TimeKey,-1), ISNULL(c.center_key,-1), ISNULL(cur.currency_key,-1), ISNULL(st.status_key,-1),
               p.payment_type, p.amount,
               CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(p.status,N'')))) = N'paid' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(p.status,N'')))) = N'cancelled' THEN 1 ELSE 0 END,
               p.id, @etl_batch_id, SYSDATETIME()
        FROM Stg_FinanceOps_DB.stg_finance_ops.payments p
        INNER JOIN (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.payments
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) p_latest ON p_latest.max_stg_row_id = p.stg_row_id
        LEFT JOIN dw.dim_date dd ON dd.FullDateAlternateKey = p.payment_date
        LEFT JOIN (SELECT center_id, MIN(center_key) AS center_key FROM dw.dim_center GROUP BY center_id) c ON c.center_id = p.center_id
        LEFT JOIN (SELECT code, MIN(currency_key) AS currency_key FROM dw.dim_currency GROUP BY code) cur ON cur.code = UPPER(LTRIM(RTRIM(p.currency)))
        LEFT JOIN (SELECT status_type, code, MIN(status_key) AS status_key FROM dw.dim_status GROUP BY status_type, code) st ON st.status_type=N'payment' AND st.code = LOWER(LTRIM(RTRIM(p.status)))
        WHERE p.is_valid=1
          AND p.id IS NOT NULL
          AND LOWER(LTRIM(RTRIM(ISNULL(p.status,N'')))) IN (N'approved', N'paid', N'cancelled', N'rejected')
          AND CONVERT(DATETIME2(0), p.payment_date) >= @start_time
          AND CONVERT(DATETIME2(0), p.payment_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_payment_transaction_load;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_payment_transaction;
            DBCC CHECKIDENT ('dw.fact_payment_transaction', RESEED, -1);
            DELETE FROM etl_work.fact_source_load_map WHERE fact_name=N'fact_payment_transaction' AND source_table=N'payments';
            INSERT INTO dw.fact_payment_transaction
            (date_key, center_key, currency_key, status_key, payment_type, amount, is_paid, is_cancelled, etl_batch_id, loaded_at)
            SELECT t.date_key, t.center_key, t.currency_key, t.status_key, t.payment_type, t.amount, t.is_paid, t.is_cancelled, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_payment_transaction_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_payment_transaction'
                  AND m.source_table=N'payments'
                  AND m.source_id=t.source_payment_id
            );
            SET @rows_inserted=@@ROWCOUNT;

            INSERT INTO etl_work.fact_source_load_map(fact_name, source_table, source_id, loaded_etl_batch_id, loaded_at)
            SELECT N'fact_payment_transaction', N'payments', t.source_payment_id, @etl_batch_id, SYSDATETIME()
            FROM etl_work.tmp_fact_payment_transaction_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_payment_transaction'
                  AND m.source_table=N'payments'
                  AND m.source_id=t.source_payment_id
            );
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'payments', N'fact_payment_transaction', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Payment fact loaded without pending rows and without source columns in DW.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_budget_allocation_event
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_budget_allocation_event_load;
        INSERT INTO etl_work.tmp_fact_budget_allocation_event_load
        (date_key, donor_key, center_key, child_key, category_key, campaign_key, source_allocation_id, etl_batch_id, loaded_at)
        SELECT ISNULL(dd.TimeKey,-1),
               CASE WHEN LOWER(ISNULL(a.source_type,N''))=N'donation' THEN ISNULL(donor.donor_key,-1) ELSE -1 END,
               ISNULL(c.center_key,-1),
               ISNULL(ch.child_key,-1),
               ISNULL(cat.category_key,-1),
               CASE WHEN LOWER(ISNULL(a.source_type,N''))=N'donation' THEN ISNULL(camp.campaign_key,-1) ELSE -1 END,
               a.id,
               @etl_batch_id,
               SYSDATETIME()
        FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations a
        INNER JOIN (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) a_latest ON a_latest.max_stg_row_id = a.stg_row_id
        LEFT JOIN (
            SELECT sd1.*
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations sd1
            INNER JOIN (
                SELECT id, MAX(stg_row_id) AS max_stg_row_id
                FROM Stg_FinanceOps_DB.stg_finance_ops.donations
                WHERE is_valid = 1 AND id IS NOT NULL
                GROUP BY id
            ) sd_latest ON sd_latest.max_stg_row_id = sd1.stg_row_id
        ) sd ON LOWER(ISNULL(a.source_type,N''))=N'donation' AND sd.id = a.source_id AND sd.is_valid=1
        LEFT JOIN dw.dim_date dd ON dd.FullDateAlternateKey = a.allocation_date
        LEFT JOIN (SELECT donor_id, MIN(donor_key) AS donor_key FROM dw.dim_donor GROUP BY donor_id) donor ON donor.donor_id = sd.donor_id
        LEFT JOIN (SELECT campaign_id, MIN(campaign_key) AS campaign_key FROM dw.dim_campaign GROUP BY campaign_id) camp ON camp.campaign_id = sd.campaign_id
        LEFT JOIN (SELECT center_id, MIN(center_key) AS center_key FROM dw.dim_center GROUP BY center_id) c ON c.center_id = a.center_id
        LEFT JOIN (SELECT child_id, MIN(child_key) AS child_key FROM dw.dim_child GROUP BY child_id) ch ON ch.child_id = a.child_id
        LEFT JOIN (SELECT category_id, MIN(category_key) AS category_key FROM dw.dim_category GROUP BY category_id) cat ON cat.category_id = a.category_id
        WHERE a.is_valid=1
          AND a.id IS NOT NULL
          AND CONVERT(DATETIME2(0), a.allocation_date) >= @start_time
          AND CONVERT(DATETIME2(0), a.allocation_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_budget_allocation_event_load;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_budget_allocation_event;
            DBCC CHECKIDENT ('dw.fact_budget_allocation_event', RESEED, -1);
            DELETE FROM etl_work.fact_source_load_map WHERE fact_name=N'fact_budget_allocation_event' AND source_table=N'budget_allocations';
            INSERT INTO dw.fact_budget_allocation_event
            (date_key, donor_key, center_key, child_key, category_key, campaign_key, etl_batch_id, loaded_at)
            SELECT t.date_key, t.donor_key, t.center_key, t.child_key, t.category_key, t.campaign_key, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_budget_allocation_event_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_budget_allocation_event'
                  AND m.source_table=N'budget_allocations'
                  AND m.source_id=t.source_allocation_id
            );
            SET @rows_inserted=@@ROWCOUNT;

            INSERT INTO etl_work.fact_source_load_map(fact_name, source_table, source_id, loaded_etl_batch_id, loaded_at)
            SELECT N'fact_budget_allocation_event', N'budget_allocations', t.source_allocation_id, @etl_batch_id, SYSDATETIME()
            FROM etl_work.tmp_fact_budget_allocation_event_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM etl_work.fact_source_load_map m
                WHERE m.fact_name=N'fact_budget_allocation_event'
                  AND m.source_table=N'budget_allocations'
                  AND m.source_id=t.source_allocation_id
            );
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'fact_budget_allocation_event', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Fact-less allocation relationship event loaded without measures or source columns in DW.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_monthly_financial_snapshot
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    DECLARE @month_start DATE = DATEFROMPARTS(YEAR(CONVERT(DATE,@start_time)), MONTH(CONVERT(DATE,@start_time)), 1);
    DECLARE @period_end DATE = CONVERT(DATE, @end_time);
    DECLARE @month_end DATE, @month_key INT;

    EXEC etl_admin.usp_dw_start_batch N'DW_FACT_SNAPSHOT', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_monthly_snapshot_load;

        WHILE @month_start < @period_end
        BEGIN
            SET @month_end = EOMONTH(@month_start);
            SELECT @month_key = MAX(TimeKey)
            FROM dw.dim_date
            WHERE FullDateAlternateKey >= @month_start AND FullDateAlternateKey <= @month_end;

            IF @month_key IS NOT NULL
            BEGIN
                INSERT INTO etl_work.tmp_fact_monthly_snapshot_load
                (month_key, center_key, total_donation_amount, total_expense_amount, total_payment_amount, net_balance,
                 donation_count, expense_count, payment_count, allocation_count, etl_batch_id, loaded_at)
                SELECT
                    @month_key,
                    c.center_key,
                    ISNULL(don.total_donation_amount, 0),
                    ISNULL(exp.total_expense_amount, 0),
                    ISNULL(pay.total_payment_amount, 0),
                    ISNULL(don.total_donation_amount, 0) - ISNULL(exp.total_expense_amount, 0) - ISNULL(pay.total_payment_amount, 0),
                    ISNULL(don.donation_count, 0),
                    ISNULL(exp.expense_count, 0),
                    ISNULL(pay.payment_count, 0),
                    ISNULL(alloc.allocation_count, 0),
                    @etl_batch_id,
                    SYSDATETIME()
                FROM dw.dim_center c
                OUTER APPLY (
                    SELECT SUM(a.allocated_amount) AS total_donation_amount,
                           COUNT(DISTINCT sd.id) AS donation_count
                    FROM (
                        SELECT a1.*
                        FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations a1
                        INNER JOIN (
                            SELECT id, MAX(stg_row_id) AS max_stg_row_id
                            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
                            WHERE is_valid=1 AND id IS NOT NULL
                            GROUP BY id
                        ) al ON al.max_stg_row_id = a1.stg_row_id
                    ) a
                    INNER JOIN (
                        SELECT d1.*
                        FROM Stg_FinanceOps_DB.stg_finance_ops.donations d1
                        INNER JOIN (
                            SELECT id, MAX(stg_row_id) AS max_stg_row_id
                            FROM Stg_FinanceOps_DB.stg_finance_ops.donations
                            WHERE is_valid=1 AND id IS NOT NULL
                            GROUP BY id
                        ) dl ON dl.max_stg_row_id = d1.stg_row_id
                    ) sd ON LOWER(ISNULL(a.source_type,N'')) = N'donation'
                        AND sd.id = a.source_id
                        AND LOWER(LTRIM(RTRIM(ISNULL(sd.status,N'')))) = N'confirmed'
                    WHERE a.is_valid=1
                      AND a.center_id = c.center_id
                      AND a.allocation_date >= @month_start
                      AND a.allocation_date <= @month_end
                ) don
                OUTER APPLY (
                    SELECT SUM(e.amount) AS total_expense_amount,
                           COUNT(*) AS expense_count
                    FROM dw.fact_expense_transaction e
                    WHERE e.center_key = c.center_key
                      AND e.is_approved = 1
                      AND e.date_key BETWEEN CONVERT(INT, CONVERT(CHAR(8), @month_start, 112)) AND @month_key
                ) exp
                OUTER APPLY (
                    SELECT SUM(p.amount) AS total_payment_amount,
                           COUNT(*) AS payment_count
                    FROM dw.fact_payment_transaction p
                    WHERE p.center_key = c.center_key
                      AND p.is_paid = 1
                      AND p.date_key BETWEEN CONVERT(INT, CONVERT(CHAR(8), @month_start, 112)) AND @month_key
                ) pay
                OUTER APPLY (
                    SELECT COUNT(*) AS allocation_count
                    FROM (
                        SELECT a2.*
                        FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations a2
                        INNER JOIN (
                            SELECT id, MAX(stg_row_id) AS max_stg_row_id
                            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
                            WHERE is_valid=1 AND id IS NOT NULL
                            GROUP BY id
                        ) a2l ON a2l.max_stg_row_id = a2.stg_row_id
                    ) a2
                    WHERE a2.is_valid=1
                      AND a2.center_id = c.center_id
                      AND a2.allocation_date >= @month_start
                      AND a2.allocation_date <= @month_end
                ) alloc;
            END
            SET @month_start = DATEADD(MONTH, 1, @month_start);
        END

        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_monthly_snapshot_load;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_monthly_financial_snapshot;
            DBCC CHECKIDENT ('dw.fact_monthly_financial_snapshot', RESEED, -1);
            INSERT INTO dw.fact_monthly_financial_snapshot
            (month_key, center_key, total_donation_amount, total_expense_amount, total_payment_amount, net_balance,
             donation_count, expense_count, payment_count, allocation_count, etl_batch_id, loaded_at)
            SELECT t.month_key, t.center_key, t.total_donation_amount, t.total_expense_amount, t.total_payment_amount, t.net_balance,
                   t.donation_count, t.expense_count, t.payment_count, t.allocation_count, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_monthly_snapshot_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM dw.fact_monthly_financial_snapshot f
                WHERE f.month_key = t.month_key AND f.center_key = t.center_key
            );
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'dw_fact_transactions_and_stg_allocations', N'fact_monthly_financial_snapshot', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Monthly snapshot loaded with donation allocation amounts calculated from staging allocations because allocation event fact is fact-less.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_donation_lifecycle
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_FACT_LIFECYCLE', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_donation_lifecycle_current;
        TRUNCATE TABLE etl_work.tmp_fact_donation_lifecycle_final;

        ;WITH latest_donations AS (
            SELECT d1.*
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations d1
            INNER JOIN (
                SELECT id, MAX(stg_row_id) AS max_stg_row_id
                FROM Stg_FinanceOps_DB.stg_finance_ops.donations
                WHERE is_valid=1 AND id IS NOT NULL
                GROUP BY id
            ) dl ON dl.max_stg_row_id = d1.stg_row_id
            WHERE d1.is_valid=1
              AND d1.id IS NOT NULL
              AND CONVERT(DATETIME2(0), d1.donation_date) < @end_time
              AND LOWER(LTRIM(RTRIM(ISNULL(d1.status,N'')))) IN (N'confirmed', N'rejected', N'refunded')
        ), latest_allocations AS (
            SELECT a1.*
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations a1
            INNER JOIN (
                SELECT id, MAX(stg_row_id) AS max_stg_row_id
                FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
                WHERE is_valid=1 AND id IS NOT NULL
                GROUP BY id
            ) al ON al.max_stg_row_id = a1.stg_row_id
            WHERE a1.is_valid=1
        ), alloc AS (
            SELECT source_id AS donation_id,
                   MIN(dd.TimeKey) AS allocated_date_key
            FROM latest_allocations a
            LEFT JOIN dw.dim_date dd ON dd.FullDateAlternateKey = a.allocation_date
            WHERE LOWER(ISNULL(a.source_type,N'')) = N'donation'
            GROUP BY source_id
        ), base AS (
            SELECT
                d.id AS source_donation_id,
                ISNULL(donor.donor_key, -1) AS donor_key,
                ISNULL(camp.campaign_key, -1) AS campaign_key,
                ISNULL(dd.TimeKey, -1) AS created_date_key,
                CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) IN (N'confirmed', N'refunded') THEN ISNULL(dd.TimeKey, -1) ELSE -1 END AS confirmed_date_key,
                ISNULL(alloc.allocated_date_key, -1) AS allocated_date_key,
                CASE
                    WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) = N'refunded' THEN ISNULL(st_ref.status_key, -1)
                    WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) = N'rejected' THEN ISNULL(st_rej.status_key, -1)
                    ELSE ISNULL(st_conf.status_key, -1)
                END AS lifecycle_status_key,
                CASE
                    WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) = N'refunded' THEN N'refunded'
                    WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status,N'')))) = N'rejected' THEN N'rejected'
                    WHEN alloc.allocated_date_key IS NOT NULL THEN N'allocated'
                    ELSE N'confirmed'
                END AS current_stage,
                d.amount AS donation_amount,
                @etl_batch_id AS etl_batch_id,
                SYSDATETIME() AS loaded_at
            FROM latest_donations d
            LEFT JOIN dw.dim_date dd ON dd.FullDateAlternateKey = d.donation_date
            LEFT JOIN (SELECT donor_id, MIN(donor_key) AS donor_key FROM dw.dim_donor GROUP BY donor_id) donor ON donor.donor_id = d.donor_id
            LEFT JOIN (SELECT campaign_id, MIN(campaign_key) AS campaign_key FROM dw.dim_campaign GROUP BY campaign_id) camp ON camp.campaign_id = d.campaign_id
            LEFT JOIN alloc ON alloc.donation_id = d.id
            LEFT JOIN dw.dim_status st_conf ON st_conf.status_type=N'donation' AND st_conf.code=N'confirmed'
            LEFT JOIN dw.dim_status st_rej ON st_rej.status_type=N'donation' AND st_rej.code=N'rejected'
            LEFT JOIN dw.dim_status st_ref ON st_ref.status_type=N'donation' AND st_ref.code=N'refunded'
        ), lifecycle_agg AS (
            SELECT
                MIN(source_donation_id) AS source_donation_id,
                donor_key,
                campaign_key,
                COALESCE(MIN(NULLIF(created_date_key, -1)), -1) AS created_date_key,
                COALESCE(MIN(NULLIF(confirmed_date_key, -1)), -1) AS confirmed_date_key,
                COALESCE(MIN(NULLIF(allocated_date_key, -1)), -1) AS allocated_date_key,
                lifecycle_status_key,
                current_stage,
                SUM(donation_amount) AS donation_amount,
                MIN(donation_amount) AS min_donation,
                MAX(donation_amount) AS max_donation,
                AVG(CONVERT(DECIMAL(18,4), donation_amount)) AS avg_donation,
                @etl_batch_id AS etl_batch_id,
                SYSDATETIME() AS loaded_at
            FROM base
            GROUP BY donor_key, campaign_key, lifecycle_status_key, current_stage
        )
        INSERT INTO etl_work.tmp_fact_donation_lifecycle_current
        (source_donation_id, donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
         current_stage, donation_amount, min_donation, max_donation, avg_donation, etl_batch_id, loaded_at)
        SELECT source_donation_id, donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
               current_stage, donation_amount, min_donation, max_donation, CONVERT(DECIMAL(18,2), avg_donation), etl_batch_id, loaded_at
        FROM lifecycle_agg;

        INSERT INTO etl_work.tmp_fact_donation_lifecycle_final
        (source_donation_id, donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
         current_stage, donation_amount, min_donation, max_donation, avg_donation, etl_batch_id, loaded_at)
        SELECT source_donation_id, donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
               current_stage, donation_amount, min_donation, max_donation, avg_donation, etl_batch_id, loaded_at
        FROM etl_work.tmp_fact_donation_lifecycle_current;

        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_donation_lifecycle_current;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_donation_lifecycle;
            DBCC CHECKIDENT ('dw.fact_donation_lifecycle', RESEED, -1);
            INSERT INTO dw.fact_donation_lifecycle
            (donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
             current_stage, donation_amount, min_donation, max_donation, avg_donation, etl_batch_id, loaded_at)
            SELECT donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
                   current_stage, donation_amount, min_donation, max_donation, avg_donation, etl_batch_id, loaded_at
            FROM etl_work.tmp_fact_donation_lifecycle_final;
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'stg_donations_and_allocations', N'fact_donation_lifecycle', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Lifecycle fact rebuilt at donor/campaign/stage grain. donation_amount is SUM(donation amount); min/max/avg are calculated per same grain. Pending donations excluded.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_finance_mart2_all
      @start_time  DATETIME2(0),
      @end_time    DATETIME2(0),
      @run_staging BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    EXEC etl_admin.usp_assert_finance_mart2_prerequisites;

    IF @run_staging = 1
    BEGIN
        EXEC Stg_FinanceOps_DB.etl_admin.usp_run_stg_finance_ops_all @to_date=@end_time, @etl_batch_id=NULL;
    END

    EXEC etl_admin.usp_first_load_dw_dim_donor @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_campaign @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_category @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_donation_type @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_status @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_currency @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_donation_transaction @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_expense_transaction @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_payment_transaction @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_budget_allocation_event @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_monthly_financial_snapshot @start_time, @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_donation_lifecycle @start_time, @end_time;
END
GO
