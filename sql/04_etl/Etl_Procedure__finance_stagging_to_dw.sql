
/*
===============================================================================
 Finance MART 2 NORMAL/INCREMENTAL ETL - Rule-Based Rewrite

 Rules applied in this version:
   - No SQL Server session temp tables are used.
   - Work tables are permanent DW tables in etl_work schema and are TRUNCATEd.
   - Type 1 dimensions use: build work table -> TRUNCATE dimension -> INSERT.
   - Facts do not use UPDATE.
   - Transaction/event/factless-style facts are append-only and avoid duplicates.
   - Monthly snapshot is append-only and is the only WHILE-loop based fact.
   - Monthly snapshot reads DW fact tables, not source/staging transaction tables.
   - Donation lifecycle rebuilds by using old lifecycle fact + new calculated lifecycle rows.
   - Unknown rows are not checked; they are assumed by DW creation and reloaded for Type 1 dimensions after truncate.
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

    IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL AND OBJECT_ID(N'dw.DimDate', N'U') IS NULL
        THROW 52001, 'Missing DW date dimension.', 1;

    IF OBJECT_ID(N'dw.dim_center', N'U') IS NULL
        THROW 52002, 'Missing dw.dim_center.', 1;

    IF OBJECT_ID(N'dw.dim_child', N'U') IS NULL
        THROW 52003, 'Missing dw.dim_child.', 1;

    IF OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.donors', N'U') IS NULL
        THROW 52004, 'Missing staging finance tables.', 1;

    IF OBJECT_ID(N'etl_work.tmp_dim_donor_load', N'U') IS NULL
        THROW 52005, 'Missing etl_work temp/work tables. Run 30_create_dw_finance_etl_work_tables.sql first.', 1;
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


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_donor_incremental
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
        (existing_key, donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
        VALUES (-1, -1, N'Unknown', N'unknown', 0, N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);

        ;WITH src AS (
            SELECT id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.donors
            WHERE is_valid = 1 AND id IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_donor_load
        (existing_key, donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
        SELECT d.donor_key, s.id, s.full_name, s.donor_type, s.is_active, s.source_system, s.row_hash, s.created_at, s.updated_at
        FROM src s
        FULL JOIN dw.dim_donor d ON d.donor_id = s.id AND d.donor_key <> -1
        WHERE s.id IS NOT NULL;

        SELECT @rows_read = COUNT(*) FROM etl_work.tmp_dim_donor_load WHERE donor_id <> -1;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donors', N'dim_donor', N'work_ready', @rows_read, 0, 0, 0, @step_started, N'Type 1 dimension work table prepared with FULL JOIN.';

        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_donor;

            SET IDENTITY_INSERT dw.dim_donor ON;
            INSERT INTO dw.dim_donor
            (donor_key, donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
            SELECT existing_key, donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_donor_load
            WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_donor OFF;

            INSERT INTO dw.dim_donor
            (donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
            SELECT donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_donor_load
            WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT TRANSACTION;

        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'etl_work.tmp_dim_donor_load', N'dim_donor', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 dimension refreshed by truncate + insert.';
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


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_campaign_incremental
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
        TRUNCATE TABLE etl_work.tmp_dim_campaign_load;
        INSERT INTO etl_work.tmp_dim_campaign_load
        (existing_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
        VALUES (-1, -1, N'Unknown', N'unknown', NULL, NULL, NULL, N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);

        ;WITH src AS (
            SELECT id, title, status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns
            WHERE is_valid = 1 AND id IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_campaign_load
        (existing_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
        SELECT d.campaign_key, s.id, s.title, s.status, s.target_amount, s.start_date, s.end_date, s.source_system, s.row_hash, s.created_at, s.updated_at
        FROM src s
        FULL JOIN dw.dim_campaign d ON d.campaign_id = s.id AND d.campaign_key <> -1
        WHERE s.id IS NOT NULL;
        SELECT @rows_read = COUNT(*) FROM etl_work.tmp_dim_campaign_load WHERE campaign_id <> -1;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'campaigns', N'dim_campaign', N'work_ready', @rows_read, 0, 0, 0, @step_started, N'Type 1 dimension work table prepared with FULL JOIN.';

        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_campaign;
            SET IDENTITY_INSERT dw.dim_campaign ON;
            INSERT INTO dw.dim_campaign
            (campaign_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
            SELECT existing_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_campaign_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_campaign OFF;
            INSERT INTO dw.dim_campaign
            (campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
            SELECT campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_campaign_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT TRANSACTION;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'etl_work.tmp_dim_campaign_load', N'dim_campaign', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 dimension refreshed by truncate + insert.';
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


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_category_incremental
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
        TRUNCATE TABLE etl_work.tmp_dim_category_load;
        INSERT INTO etl_work.tmp_dim_category_load
        (existing_key, category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
        VALUES (-1, -1, N'Unknown', NULL, NULL, N'unknown', N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);

        ;WITH src AS (
            SELECT c.id, c.name, c.parent_id, p.name AS parent_name,
                   CASE WHEN ISNULL(c.is_active, 0)=1 THEN N'active' ELSE N'inactive' END AS category_status,
                   c.source_system, c.row_hash, c.created_at, c.updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories c
            LEFT JOIN Stg_FinanceOps_DB.stg_finance_ops.expense_categories p ON p.id = c.parent_id AND p.is_valid = 1
            WHERE c.is_valid = 1 AND c.id IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_category_load
        (existing_key, category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
        SELECT d.category_key, s.id, s.name, s.parent_id, s.parent_name, s.category_status, s.source_system, s.row_hash, s.created_at, s.updated_at
        FROM src s
        FULL JOIN dw.dim_category d ON d.category_id = s.id AND d.category_key <> -1
        WHERE s.id IS NOT NULL;
        SELECT @rows_read = COUNT(*) FROM etl_work.tmp_dim_category_load WHERE category_id <> -1;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expense_categories', N'dim_category', N'work_ready', @rows_read, 0, 0, 0, @step_started, N'Type 1 hierarchical dimension work table prepared with FULL JOIN.';

        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_category;
            SET IDENTITY_INSERT dw.dim_category ON;
            INSERT INTO dw.dim_category
            (category_key, category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
            SELECT existing_key, category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_category_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_category OFF;
            INSERT INTO dw.dim_category
            (category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
            SELECT category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at
            FROM etl_work.tmp_dim_category_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT TRANSACTION;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'etl_work.tmp_dim_category_load', N'dim_category', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 dimension refreshed by truncate + insert.';
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


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_donation_type_incremental
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_donation_type_load;
        INSERT INTO etl_work.tmp_dim_donation_type_load(existing_key, code, title, source_system, created_at, updated_at)
        VALUES(-1, N'unknown', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
        ;WITH src AS (
            SELECT DISTINCT LOWER(LTRIM(RTRIM(donation_type))) AS code,
                   LTRIM(RTRIM(donation_type)) AS title,
                   N'FINANCE_OPS' AS source_system
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations
            WHERE is_valid=1 AND NULLIF(LTRIM(RTRIM(donation_type)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_donation_type_load(existing_key, code, title, source_system, created_at, updated_at)
        SELECT d.donation_type_key, s.code, s.title, s.source_system, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_donation_type d ON d.code = s.code AND d.donation_type_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_donation_type_load WHERE code<>N'unknown';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_donation_type;
            SET IDENTITY_INSERT dw.dim_donation_type ON;
            INSERT INTO dw.dim_donation_type(donation_type_key, code, title, source_system, created_at, updated_at)
            SELECT existing_key, code, title, source_system, created_at, updated_at FROM etl_work.tmp_dim_donation_type_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_donation_type OFF;
            INSERT INTO dw.dim_donation_type(code, title, source_system, created_at, updated_at)
            SELECT code, title, source_system, created_at, updated_at FROM etl_work.tmp_dim_donation_type_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'dim_donation_type', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 reference dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_status_incremental
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_status_load;
        INSERT INTO etl_work.tmp_dim_status_load(existing_key, status_type, code, title, category, source_system, created_at, updated_at)
        VALUES(-1, N'unknown', N'unknown', N'Unknown', N'unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
        ;WITH raw_status AS (
            SELECT N'campaign' AS status_type, status AS code FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns WHERE is_valid=1
            UNION SELECT N'donation', status FROM Stg_FinanceOps_DB.stg_finance_ops.donations WHERE is_valid=1
            UNION SELECT N'expense', status FROM Stg_FinanceOps_DB.stg_finance_ops.expenses WHERE is_valid=1
            UNION SELECT N'payment', status FROM Stg_FinanceOps_DB.stg_finance_ops.payments WHERE is_valid=1
        ), src AS (
            SELECT DISTINCT status_type,
                   LOWER(LTRIM(RTRIM(code))) AS code,
                   LTRIM(RTRIM(code)) AS title,
                   status_type AS category,
                   N'FINANCE_OPS' AS source_system
            FROM raw_status
            WHERE NULLIF(LTRIM(RTRIM(code)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_status_load(existing_key, status_type, code, title, category, source_system, created_at, updated_at)
        SELECT d.status_key, s.status_type, s.code, s.title, s.category, s.source_system, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_status d ON d.status_type = s.status_type AND d.code = s.code AND d.status_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_status_load WHERE code<>N'unknown';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_status;
            SET IDENTITY_INSERT dw.dim_status ON;
            INSERT INTO dw.dim_status(status_key, status_type, code, title, category, source_system, created_at, updated_at)
            SELECT existing_key, status_type, code, title, category, source_system, created_at, updated_at FROM etl_work.tmp_dim_status_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_status OFF;
            INSERT INTO dw.dim_status(status_type, code, title, category, source_system, created_at, updated_at)
            SELECT status_type, code, title, category, source_system, created_at, updated_at FROM etl_work.tmp_dim_status_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'status_sources', N'dim_status', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 status dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_currency_incremental
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_currency_load;
        INSERT INTO etl_work.tmp_dim_currency_load(existing_key, code, name, source_system, created_at, updated_at)
        VALUES(-1, N'UNK', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
        ;WITH raw_currency AS (
            SELECT currency AS code FROM Stg_FinanceOps_DB.stg_finance_ops.donations WHERE is_valid=1
            UNION SELECT currency FROM Stg_FinanceOps_DB.stg_finance_ops.expenses WHERE is_valid=1
            UNION SELECT currency FROM Stg_FinanceOps_DB.stg_finance_ops.payments WHERE is_valid=1
            UNION SELECT from_currency FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates WHERE is_valid=1
            UNION SELECT to_currency FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates WHERE is_valid=1
        ), src AS (
            SELECT DISTINCT UPPER(LTRIM(RTRIM(code))) AS code,
                   UPPER(LTRIM(RTRIM(code))) AS name,
                   N'FINANCE_OPS' AS source_system
            FROM raw_currency
            WHERE NULLIF(LTRIM(RTRIM(code)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_currency_load(existing_key, code, name, source_system, created_at, updated_at)
        SELECT d.currency_key, s.code, s.name, s.source_system, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_currency d ON d.code = s.code AND d.currency_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_currency_load WHERE code<>N'UNK';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_currency;
            SET IDENTITY_INSERT dw.dim_currency ON;
            INSERT INTO dw.dim_currency(currency_key, code, name, source_system, created_at, updated_at)
            SELECT existing_key, code, name, source_system, created_at, updated_at FROM etl_work.tmp_dim_currency_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_currency OFF;
            INSERT INTO dw.dim_currency(code, name, source_system, created_at, updated_at)
            SELECT code, name, source_system, created_at, updated_at FROM etl_work.tmp_dim_currency_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'currency_sources', N'dim_currency', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 currency dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_allocation_type_incremental
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_dim_allocation_type_load;
        INSERT INTO etl_work.tmp_dim_allocation_type_load(existing_key, code, title, source_system, created_at, updated_at)
        VALUES(-1, N'unknown', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
        ;WITH src AS (
            SELECT DISTINCT LOWER(LTRIM(RTRIM(source_type))) AS code,
                   LTRIM(RTRIM(source_type)) AS title,
                   N'FINANCE_OPS' AS source_system
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
            WHERE is_valid=1 AND NULLIF(LTRIM(RTRIM(source_type)), N'') IS NOT NULL
        )
        INSERT INTO etl_work.tmp_dim_allocation_type_load(existing_key, code, title, source_system, created_at, updated_at)
        SELECT d.allocation_type_key, s.code, s.title, s.source_system, SYSDATETIME(), NULL
        FROM src s
        FULL JOIN dw.dim_allocation_type d ON d.code = s.code AND d.allocation_type_key <> -1
        WHERE s.code IS NOT NULL;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_dim_allocation_type_load WHERE code<>N'unknown';
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.dim_allocation_type;
            SET IDENTITY_INSERT dw.dim_allocation_type ON;
            INSERT INTO dw.dim_allocation_type(allocation_type_key, code, title, source_system, created_at, updated_at)
            SELECT existing_key, code, title, source_system, created_at, updated_at FROM etl_work.tmp_dim_allocation_type_load WHERE existing_key IS NOT NULL;
            SET @rows_inserted += @@ROWCOUNT;
            SET IDENTITY_INSERT dw.dim_allocation_type OFF;
            INSERT INTO dw.dim_allocation_type(code, title, source_system, created_at, updated_at)
            SELECT code, title, source_system, created_at, updated_at FROM etl_work.tmp_dim_allocation_type_load WHERE existing_key IS NULL;
            SET @rows_inserted += @@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'dim_allocation_type', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Type 1 allocation type dimension refreshed.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_donation_transaction_incremental
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
         source_donation_id, source_donor_id, source_campaign_id, source_reference_code, source_system, etl_batch_id, loaded_at)
        SELECT
            ISNULL(dd.TimeKey, -1),
            ISNULL(donor.donor_key, -1),
            ISNULL(camp.campaign_key, -1),
            -1,
            ISNULL(dt.donation_type_key, -1),
            ISNULL(cur.currency_key, -1),
            ISNULL(st.status_key, -1),
            d.amount,
            CASE WHEN LOWER(ISNULL(d.status,N'')) = N'confirmed' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(ISNULL(d.status,N'')) = N'refunded' THEN 1 ELSE 0 END,
            d.id, d.donor_id, d.campaign_id, d.reference_code, d.source_system, @etl_batch_id, SYSDATETIME()
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
        WHERE d.is_valid = 1 AND d.id IS NOT NULL AND CONVERT(DATETIME2(0), d.donation_date) >= @start_time AND CONVERT(DATETIME2(0), d.donation_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_donation_transaction_load;
        BEGIN TRANSACTION;
            
            INSERT INTO dw.fact_donation_transaction
            (date_key, donor_key, campaign_key, center_key, donation_type_key, currency_key, status_key, amount, is_confirmed, is_refunded,
             source_donation_id, source_donor_id, source_campaign_id, source_reference_code, source_system, etl_batch_id, loaded_at)
            SELECT t.date_key, t.donor_key, t.campaign_key, t.center_key, t.donation_type_key, t.currency_key, t.status_key, t.amount, t.is_confirmed, t.is_refunded,
                   t.source_donation_id, t.source_donor_id, t.source_campaign_id, t.source_reference_code, t.source_system, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_donation_transaction_load t
            WHERE NOT EXISTS (SELECT 1 FROM dw.fact_donation_transaction f WHERE f.source_donation_id = t.source_donation_id);
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'fact_donation_transaction', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Transaction fact append-only load. No updates.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_expense_transaction_incremental
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
        (date_key, center_key, child_key, category_key, currency_key, status_key, amount, is_approved, is_rejected, description,
         source_expense_id, source_center_id, source_child_id, source_category_id, source_system, etl_batch_id, loaded_at)
        SELECT ISNULL(dd.TimeKey,-1), ISNULL(c.center_key,-1), ISNULL(ch.child_key,-1), ISNULL(cat.category_key,-1), ISNULL(cur.currency_key,-1), ISNULL(st.status_key,-1),
               e.amount,
               CASE WHEN LOWER(ISNULL(e.status,N'')) = N'approved' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(ISNULL(e.status,N'')) = N'rejected' THEN 1 ELSE 0 END,
               e.description, e.id, e.center_id, e.child_id, e.category_id, e.source_system, @etl_batch_id, SYSDATETIME()
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
        WHERE e.is_valid=1 AND e.id IS NOT NULL AND CONVERT(DATETIME2(0), e.expense_date) >= @start_time AND CONVERT(DATETIME2(0), e.expense_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_expense_transaction_load;
        BEGIN TRANSACTION;
            
            INSERT INTO dw.fact_expense_transaction
            (date_key, center_key, child_key, category_key, currency_key, status_key, amount, is_approved, is_rejected, description,
             source_expense_id, source_center_id, source_child_id, source_category_id, source_system, etl_batch_id, loaded_at)
            SELECT t.date_key, t.center_key, t.child_key, t.category_key, t.currency_key, t.status_key, t.amount, t.is_approved, t.is_rejected, t.description,
                   t.source_expense_id, t.source_center_id, t.source_child_id, t.source_category_id, t.source_system, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_expense_transaction_load t
            WHERE NOT EXISTS (SELECT 1 FROM dw.fact_expense_transaction f WHERE f.source_expense_id=t.source_expense_id);
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expenses', N'fact_expense_transaction', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Transaction fact append-only load. No updates.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_payment_transaction_incremental
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
        (date_key, center_key, currency_key, status_key, payment_type, source_teacher_id, amount, is_paid, is_cancelled,
         source_payment_id, source_center_id, source_system, etl_batch_id, loaded_at)
        SELECT ISNULL(dd.TimeKey,-1), ISNULL(c.center_key,-1), ISNULL(cur.currency_key,-1), ISNULL(st.status_key,-1),
               p.payment_type, p.teacher_id, p.amount,
               CASE WHEN LOWER(ISNULL(p.status,N'')) = N'paid' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(ISNULL(p.status,N'')) IN (N'cancelled', N'canceled') THEN 1 ELSE 0 END,
               p.id, p.center_id, p.source_system, @etl_batch_id, SYSDATETIME()
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
        WHERE p.is_valid=1 AND p.id IS NOT NULL AND CONVERT(DATETIME2(0), p.payment_date) >= @start_time AND CONVERT(DATETIME2(0), p.payment_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_payment_transaction_load;
        BEGIN TRANSACTION;
            
            INSERT INTO dw.fact_payment_transaction
            (date_key, center_key, currency_key, status_key, payment_type, source_teacher_id, amount, is_paid, is_cancelled,
             source_payment_id, source_center_id, source_system, etl_batch_id, loaded_at)
            SELECT t.date_key, t.center_key, t.currency_key, t.status_key, t.payment_type, t.source_teacher_id, t.amount, t.is_paid, t.is_cancelled,
                   t.source_payment_id, t.source_center_id, t.source_system, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_payment_transaction_load t
            WHERE NOT EXISTS (SELECT 1 FROM dw.fact_payment_transaction f WHERE f.source_payment_id=t.source_payment_id);
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'payments', N'fact_payment_transaction', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Transaction fact append-only load. No updates.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_budget_allocation_event_incremental
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
        (date_key, donor_key, center_key, child_key, category_key, campaign_key, allocation_type_key, allocated_amount, reason,
         source_allocation_id, source_type, source_id, source_center_id, source_child_id, source_category_id, source_system, etl_batch_id, loaded_at)
        SELECT ISNULL(dd.TimeKey,-1),
               CASE WHEN LOWER(ISNULL(a.source_type,N''))=N'donation' THEN ISNULL(donor.donor_key,-1) ELSE -1 END,
               ISNULL(c.center_key,-1), ISNULL(ch.child_key,-1), ISNULL(cat.category_key,-1),
               CASE WHEN LOWER(ISNULL(a.source_type,N''))=N'donation' THEN ISNULL(camp.campaign_key,-1) ELSE -1 END,
               ISNULL(at.allocation_type_key,-1),
               a.allocated_amount, a.reason, a.id, a.source_type, a.source_id, a.center_id, a.child_id, a.category_id, a.source_system, @etl_batch_id, SYSDATETIME()
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
        LEFT JOIN (SELECT code, MIN(allocation_type_key) AS allocation_type_key FROM dw.dim_allocation_type GROUP BY code) at ON at.code = LOWER(LTRIM(RTRIM(a.source_type)))
        WHERE a.is_valid=1 AND a.id IS NOT NULL AND CONVERT(DATETIME2(0), a.allocation_date) >= @start_time AND CONVERT(DATETIME2(0), a.allocation_date) < @end_time;
        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_budget_allocation_event_load;
        BEGIN TRANSACTION;
            
            INSERT INTO dw.fact_budget_allocation_event
            (date_key, donor_key, center_key, child_key, category_key, campaign_key, allocation_type_key, allocated_amount, reason,
             source_allocation_id, source_type, source_id, source_center_id, source_child_id, source_category_id, source_system, etl_batch_id, loaded_at)
            SELECT t.date_key, t.donor_key, t.center_key, t.child_key, t.category_key, t.campaign_key, t.allocation_type_key, t.allocated_amount, t.reason,
                   t.source_allocation_id, t.source_type, t.source_id, t.source_center_id, t.source_child_id, t.source_category_id, t.source_system, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_budget_allocation_event_load t
            WHERE NOT EXISTS (SELECT 1 FROM dw.fact_budget_allocation_event f WHERE f.source_allocation_id=t.source_allocation_id);
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'fact_budget_allocation_event', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Event/factless-style fact append-only load. No updates.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_monthly_financial_snapshot_incremental
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
                 donation_count, expense_count, payment_count, allocation_count, source_system, etl_batch_id, loaded_at)
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
                    N'FINANCE_OPS',
                    @etl_batch_id,
                    SYSDATETIME()
                FROM dw.dim_center c
                OUTER APPLY (
                    SELECT SUM(a.allocated_amount) AS total_donation_amount,
                           COUNT(DISTINCT a.source_id) AS donation_count
                    FROM dw.fact_budget_allocation_event a
                    JOIN dw.fact_donation_transaction d ON d.source_donation_id = a.source_id
                    WHERE a.center_key = c.center_key
                      AND LOWER(ISNULL(a.source_type,N'')) = N'donation'
                      AND d.is_confirmed = 1
                      AND ISNULL(d.is_refunded,0) = 0
                      AND a.date_key BETWEEN CONVERT(INT, CONVERT(CHAR(8), @month_start, 112)) AND @month_key
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
                    FROM dw.fact_budget_allocation_event a2
                    WHERE a2.center_key = c.center_key
                      AND a2.date_key BETWEEN CONVERT(INT, CONVERT(CHAR(8), @month_start, 112)) AND @month_key
                ) alloc;
            END
            SET @month_start = DATEADD(MONTH, 1, @month_start);
        END

        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_monthly_snapshot_load;
        BEGIN TRANSACTION;
            
            INSERT INTO dw.fact_monthly_financial_snapshot
            (month_key, center_key, total_donation_amount, total_expense_amount, total_payment_amount, net_balance,
             donation_count, expense_count, payment_count, allocation_count, source_system, etl_batch_id, loaded_at)
            SELECT t.month_key, t.center_key, t.total_donation_amount, t.total_expense_amount, t.total_payment_amount, t.net_balance,
                   t.donation_count, t.expense_count, t.payment_count, t.allocation_count, t.source_system, t.etl_batch_id, t.loaded_at
            FROM etl_work.tmp_fact_monthly_snapshot_load t
            WHERE NOT EXISTS (
                SELECT 1 FROM dw.fact_monthly_financial_snapshot f
                WHERE f.month_key = t.month_key AND f.center_key = t.center_key
            );
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'dw_fact_transactions', N'fact_monthly_financial_snapshot', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Append-only monthly snapshot from DW facts. WHILE loop used only here.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_donation_lifecycle_incremental
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @etl_batch_id INT, @rows_read INT=0, @rows_inserted INT=0;
    EXEC etl_admin.usp_dw_start_batch N'DW_FACT_LIFECYCLE', N'FINANCE_MART2', @etl_batch_id OUTPUT;
    BEGIN TRY
        TRUNCATE TABLE etl_work.tmp_fact_donation_lifecycle_old;
        TRUNCATE TABLE etl_work.tmp_fact_donation_lifecycle_current;
        TRUNCATE TABLE etl_work.tmp_fact_donation_lifecycle_final;
        
        INSERT INTO etl_work.tmp_fact_donation_lifecycle_old
        (donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
         current_stage, donation_amount, days_to_confirm, days_to_allocate, source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at)
        SELECT donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
               current_stage, donation_amount, days_to_confirm, days_to_allocate, source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at
        FROM dw.fact_donation_lifecycle;
    

        INSERT INTO etl_work.tmp_fact_donation_lifecycle_current
        (donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
         current_stage, donation_amount, days_to_confirm, days_to_allocate, source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at)
        SELECT
            d.donor_key,
            d.campaign_key,
            d.date_key AS created_date_key,
            CASE WHEN d.is_confirmed=1 THEN d.date_key ELSE -1 END AS confirmed_date_key,
            ISNULL(alloc.allocated_date_key, -1) AS allocated_date_key,
            CASE
                WHEN ISNULL(d.is_refunded,0)=1 THEN ISNULL(st_ref.status_key, -1)
                WHEN alloc.allocated_date_key IS NOT NULL THEN ISNULL(st_conf.status_key, -1)
                WHEN d.is_confirmed=1 THEN ISNULL(st_conf.status_key, -1)
                ELSE ISNULL(st_pending.status_key, -1)
            END AS lifecycle_status_key,
            CASE
                WHEN ISNULL(d.is_refunded,0)=1 THEN N'refunded'
                WHEN alloc.allocated_date_key IS NOT NULL THEN N'allocated'
                WHEN d.is_confirmed=1 THEN N'confirmed'
                ELSE N'created'
            END AS current_stage,
            d.amount,
            CASE WHEN d.is_confirmed=1 AND d.date_key > 0 THEN DATEDIFF(DAY, CONVERT(DATE, CONVERT(CHAR(8), d.date_key)), CONVERT(DATE, CONVERT(CHAR(8), d.date_key))) ELSE NULL END,
            CASE WHEN d.is_confirmed=1 AND alloc.allocated_date_key IS NOT NULL THEN DATEDIFF(DAY, CONVERT(DATE, CONVERT(CHAR(8), d.date_key)), CONVERT(DATE, CONVERT(CHAR(8), alloc.allocated_date_key))) ELSE NULL END,
            d.source_donation_id, d.source_donor_id, d.source_campaign_id, d.source_system, @etl_batch_id, SYSDATETIME()
        FROM dw.fact_donation_transaction d
        OUTER APPLY (
            SELECT MIN(a.date_key) AS allocated_date_key
            FROM dw.fact_budget_allocation_event a
            WHERE LOWER(ISNULL(a.source_type,N''))=N'donation'
              AND a.source_id = d.source_donation_id
        ) alloc
        LEFT JOIN dw.dim_status st_conf ON st_conf.status_type=N'donation' AND st_conf.code=N'confirmed'
        LEFT JOIN dw.dim_status st_pending ON st_pending.status_type=N'donation' AND st_pending.code=N'pending'
        LEFT JOIN dw.dim_status st_ref ON st_ref.status_type=N'donation' AND st_ref.code=N'refunded'
        WHERE (d.date_key BETWEEN CONVERT(INT, CONVERT(CHAR(8), CONVERT(DATE,@start_time), 112)) AND CONVERT(INT, CONVERT(CHAR(8), DATEADD(DAY,-1,CONVERT(DATE,@end_time)), 112)) OR EXISTS (SELECT 1 FROM dw.fact_budget_allocation_event ax WHERE ax.source_id=d.source_donation_id AND LOWER(ISNULL(ax.source_type,N''))=N'donation' AND ax.date_key BETWEEN CONVERT(INT, CONVERT(CHAR(8), CONVERT(DATE,@start_time), 112)) AND CONVERT(INT, CONVERT(CHAR(8), DATEADD(DAY,-1,CONVERT(DATE,@end_time)), 112))));

        INSERT INTO etl_work.tmp_fact_donation_lifecycle_final
        (donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
         current_stage, donation_amount, days_to_confirm, days_to_allocate, source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at)
        SELECT o.donor_key, o.campaign_key, o.created_date_key, o.confirmed_date_key, o.allocated_date_key, o.lifecycle_status_key,
               o.current_stage, o.donation_amount, o.days_to_confirm, o.days_to_allocate, o.source_donation_id, o.source_donor_id, o.source_campaign_id, o.source_system, o.etl_batch_id, o.loaded_at
        FROM etl_work.tmp_fact_donation_lifecycle_old o
        WHERE NOT EXISTS (SELECT 1 FROM etl_work.tmp_fact_donation_lifecycle_current c WHERE c.source_donation_id = o.source_donation_id)
        UNION ALL
        SELECT c.donor_key, c.campaign_key, c.created_date_key, c.confirmed_date_key, c.allocated_date_key, c.lifecycle_status_key,
               c.current_stage, c.donation_amount, c.days_to_confirm, c.days_to_allocate, c.source_donation_id, c.source_donor_id, c.source_campaign_id, c.source_system, c.etl_batch_id, c.loaded_at
        FROM etl_work.tmp_fact_donation_lifecycle_current c;

        SELECT @rows_read=COUNT(*) FROM etl_work.tmp_fact_donation_lifecycle_current;
        BEGIN TRANSACTION;
            TRUNCATE TABLE dw.fact_donation_lifecycle;
            DBCC CHECKIDENT ('dw.fact_donation_lifecycle', RESEED, -1);
            INSERT INTO dw.fact_donation_lifecycle
            (donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
             current_stage, donation_amount, days_to_confirm, days_to_allocate, source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at)
            SELECT donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key, lifecycle_status_key,
                   current_stage, donation_amount, days_to_confirm, days_to_allocate, source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at
            FROM etl_work.tmp_fact_donation_lifecycle_final;
            SET @rows_inserted=@@ROWCOUNT;
        COMMIT;
        EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'dw_fact_lifecycle_inputs', N'fact_donation_lifecycle', N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL, N'Lifecycle fact rebuilt by old fact + newly calculated rows. No UPDATE statement used.';
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, 0, 0, NULL;
    END TRY BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @error_message NVARCHAR(MAX)=ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, 0, 0, @error_message;
        ;THROW;
    END CATCH
END
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_finance_mart2_daily
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

    EXEC etl_admin.usp_load_dw_dim_donor_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_dim_campaign_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_dim_category_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_dim_donation_type_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_dim_status_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_dim_currency_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_dim_allocation_type_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_fact_donation_transaction_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_fact_expense_transaction_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_fact_payment_transaction_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_fact_budget_allocation_event_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_fact_monthly_financial_snapshot_incremental @start_time, @end_time;
    EXEC etl_admin.usp_load_dw_fact_donation_lifecycle_incremental @start_time, @end_time;
END
GO


-- /*
-- ===============================================================================
--  Finance MART 2 ETL - Optimized V4

--  Scope:
--    - Generated from the prerequisite version that already contains expense and
--      payment transaction facts.
--    - Dimension ETL is set-based. No date loop is used for dimensions.
--    - Transaction/event/lifecycle facts are set-based. No date loop is used.
--    - Monthly snapshot is the only procedure that uses WHILE, because the grain is
--      month/center snapshot.
--    - Monthly snapshot reads DW transaction/event facts, not source/staging facts.
--    - Date and center are resolved directly by joining dimensions, not by loading
--      separate lookup temp tables.
--    - No window functions are used.
--    - No destructive delete/reload or truncate pattern is used in normal business
--      procedures. First-load procedures are also written as upsert-style loads to
--      protect current state during practical testing.

--  Physical partitioning note:
--    These procedures are partition-friendly: fact loads are filtered by date_key or
--    month_key ranges. Real SQL Server table partitioning must be created in the DW
--    table/create script by putting fact tables/indexes on a partition scheme.
-- ===============================================================================
-- */

-- USE Charity_DW_DB;
-- GO

-- SET ANSI_NULLS ON;
-- GO
-- SET QUOTED_IDENTIFIER ON;
-- GO

-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
-- BEGIN
--     EXEC(N'CREATE SCHEMA etl_admin');
-- END
-- GO


-- /*=============================================================================
--   Helper: fail early if required DW/Staging structures are missing.
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_assert_finance_mart2_prerequisites
-- AS
-- BEGIN
--     SET NOCOUNT ON;

--     IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
--         THROW 70001, 'Missing Charity_DW_DB.etl_admin.etl_batch.', 1;

--     IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
--         THROW 70002, 'Missing Charity_DW_DB.etl_admin.etl_load_log.', 1;

--     IF OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.donors', N'U') IS NULL
--         THROW 70003, 'Missing staging tables in Stg_FinanceOps_DB.stg_finance_ops.', 1;
-- END
-- GO

-- /*=============================================================================
--   Helper: fact-specific prerequisites.
--   Kept separate so dimensions can be loaded before MART 1 center/child checks.
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_assert_finance_mart2_fact_prerequisites
-- AS
-- BEGIN
--     SET NOCOUNT ON;

--     EXEC etl_admin.usp_assert_finance_mart2_prerequisites;

--     IF COL_LENGTH(N'dw.dim_date', N'TimeKey') IS NULL
--        OR COL_LENGTH(N'dw.dim_date', N'FullDateAlternateKey') IS NULL
--         THROW 70004, 'dw.dim_date must have TimeKey and FullDateAlternateKey.', 1;

--     IF OBJECT_ID(N'dw.dim_center', N'U') IS NULL
--         THROW 70005, 'dw.dim_center is required from MART 1.', 1;

--     IF COL_LENGTH(N'dw.dim_center', N'center_key') IS NULL
--        OR COL_LENGTH(N'dw.dim_center', N'center_id') IS NULL
--         THROW 70006, 'dw.dim_center must have center_key and center_id for optimized joins.', 1;

--     IF OBJECT_ID(N'dw.dim_child', N'U') IS NULL
--         THROW 70007, 'dw.dim_child is required from MART 1.', 1;

--     IF COL_LENGTH(N'dw.dim_child', N'child_key') IS NULL
--        OR COL_LENGTH(N'dw.dim_child', N'child_id') IS NULL
--         THROW 70008, 'dw.dim_child must have child_key and child_id for optimized joins.', 1;
-- END
-- GO

-- /*=============================================================================
--   Helper: start one ETL batch.
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_dw_start_batch
--       @target_layer NVARCHAR(100),
--       @mart_name    NVARCHAR(100) = N'FINANCE_MART2',
--       @etl_batch_id INT OUTPUT
-- AS
-- BEGIN
--     SET NOCOUNT ON;

--     INSERT INTO Charity_DW_DB.etl_admin.etl_batch
--     (
--           source_system,
--           target_layer,
--           mart_name,
--           batch_status,
--           started_at,
--           rows_read,
--           rows_inserted,
--           rows_updated,
--           rows_rejected,
--           created_by
--     )
--     VALUES
--     (
--           N'FINANCE_OPS',
--           @target_layer,
--           @mart_name,
--           N'running',
--           SYSDATETIME(),
--           0,
--           0,
--           0,
--           0,
--           COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL')
--     );

--     SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());
-- END
-- GO

-- /*=============================================================================
--   Helper: write one ETL step log row.
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_dw_log_step
--       @etl_batch_id    INT,
--       @source_table    NVARCHAR(128),
--       @target_table    NVARCHAR(128),
--       @load_status     NVARCHAR(50),
--       @rows_read       INT = 0,
--       @rows_inserted   INT = 0,
--       @rows_updated    INT = 0,
--       @rows_rejected   INT = 0,
--       @started_at      DATETIME2(0) = NULL,
--       @message         NVARCHAR(MAX) = NULL
-- AS
-- BEGIN
--     SET NOCOUNT ON;

--     INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
--     (
--           etl_batch_id,
--           source_database,
--           source_schema,
--           source_table,
--           target_database,
--           target_schema,
--           target_table,
--           load_status,
--           rows_read,
--           rows_inserted,
--           rows_updated,
--           rows_rejected,
--           started_at,
--           ended_at,
--           message
--     )
--     VALUES
--     (
--           @etl_batch_id,
--           N'Stg_FinanceOps_DB',
--           N'stg_finance_ops',
--           @source_table,
--           N'Charity_DW_DB',
--           N'dw',
--           @target_table,
--           @load_status,
--           @rows_read,
--           @rows_inserted,
--           @rows_updated,
--           @rows_rejected,
--           ISNULL(@started_at, SYSDATETIME()),
--           SYSDATETIME(),
--           @message
--     );
-- END
-- GO

-- /*=============================================================================
--   Helper: finish one ETL batch with explicit business counts.
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_dw_finish_batch
--       @etl_batch_id    INT,
--       @batch_status    NVARCHAR(50),
--       @rows_read       INT = 0,
--       @rows_inserted   INT = 0,
--       @rows_updated    INT = 0,
--       @rows_rejected   INT = 0,
--       @error_message   NVARCHAR(MAX) = NULL
-- AS
-- BEGIN
--     SET NOCOUNT ON;

--     UPDATE Charity_DW_DB.etl_admin.etl_batch
--        SET batch_status  = @batch_status,
--            ended_at       = SYSDATETIME(),
--            rows_read      = ISNULL(@rows_read, 0),
--            rows_inserted  = ISNULL(@rows_inserted, 0),
--            rows_updated   = ISNULL(@rows_updated, 0),
--            rows_rejected  = ISNULL(@rows_rejected, 0),
--            error_message  = @error_message
--      WHERE etl_batch_id = @etl_batch_id;
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_donor_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70101, 'Invalid period for incremental dim_donor.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_donor WHERE donor_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_donor ON;
--             INSERT INTO dw.dim_donor
--                 (donor_key, donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', N'unknown', 0, N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_donor OFF;
--         END

--         SELECT
--               s.id AS donor_id,
--               s.full_name,
--               s.donor_type,
--               s.is_active,
--               s.source_system,
--               s.row_hash,
--               s.created_at,
--               s.updated_at
--         INTO #src_donor
--         FROM Stg_FinanceOps_DB.stg_finance_ops.donors s
--         INNER JOIN
--         (
--             SELECT id, MAX(stg_row_id) AS max_stg_row_id
--             FROM Stg_FinanceOps_DB.stg_finance_ops.donors
--             WHERE is_valid = 1
--               AND id IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY id
--         ) x ON x.max_stg_row_id = s.stg_row_id;

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_donor ON #src_donor(donor_id);

--         SELECT
--               CASE
--                   WHEN d.donor_key IS NULL THEN N'INSERT'
--                   WHEN s.donor_id IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(CONVERT(VARBINARY(32), d.row_hash), 0x00) <> ISNULL(CONVERT(VARBINARY(32), s.row_hash), 0x00)
--                     OR ISNULL(d.full_name, N'') <> ISNULL(s.full_name, N'')
--                     OR ISNULL(d.donor_type, N'') <> ISNULL(s.donor_type, N'')
--                     OR ISNULL(d.is_active, 0) <> ISNULL(s.is_active, 0)
--                       THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.donor_key,
--               s.donor_id,
--               s.full_name,
--               s.donor_type,
--               s.is_active,
--               s.source_system,
--               s.row_hash,
--               s.created_at,
--               s.updated_at
--         INTO #work_donor
--         FROM #src_donor s
--         FULL JOIN dw.dim_donor d
--                ON d.donor_id = s.donor_id
--               AND ISNULL(d.donor_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_donor ON #work_donor(action_code, donor_id);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.full_name     = w.full_name,
--                d.donor_type    = w.donor_type,
--                d.is_active     = w.is_active,
--                d.source_system = w.source_system,
--                d.row_hash      = w.row_hash,
--                d.created_at    = w.created_at,
--                d.updated_at    = w.updated_at
--         FROM dw.dim_donor d
--         INNER JOIN #work_donor w ON w.donor_key = d.donor_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_donor
--             (donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
--         SELECT donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at
--         FROM #work_donor
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(donor_key) FROM dw.dim_donor WHERE donor_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_donor'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donors', N'dim_donor', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_donor incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donors', N'dim_donor', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_campaign_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70201, 'Invalid period for incremental dim_campaign.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_campaign WHERE campaign_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_campaign ON;
--             INSERT INTO dw.dim_campaign
--                 (campaign_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', N'unknown', NULL, NULL, NULL, N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_campaign OFF;
--         END

--         SELECT
--               s.id AS campaign_id,
--               s.title,
--               s.status AS campaign_status,
--               s.target_amount,
--               s.start_date,
--               s.end_date,
--               s.source_system,
--               s.row_hash,
--               s.created_at,
--               s.updated_at
--         INTO #src_campaign
--         FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns s
--         INNER JOIN
--         (
--             SELECT id, MAX(stg_row_id) AS max_stg_row_id
--             FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns
--             WHERE is_valid = 1
--               AND id IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY id
--         ) x ON x.max_stg_row_id = s.stg_row_id;

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_campaign ON #src_campaign(campaign_id);

--         SELECT
--               CASE
--                   WHEN d.campaign_key IS NULL THEN N'INSERT'
--                   WHEN s.campaign_id IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(CONVERT(VARBINARY(32), d.row_hash), 0x00) <> ISNULL(CONVERT(VARBINARY(32), s.row_hash), 0x00)
--                     OR ISNULL(d.title, N'') <> ISNULL(s.title, N'')
--                     OR ISNULL(d.campaign_status, N'') <> ISNULL(s.campaign_status, N'')
--                     OR ISNULL(d.target_amount, 0) <> ISNULL(s.target_amount, 0)
--                     OR ISNULL(d.start_date, '19000101') <> ISNULL(s.start_date, '19000101')
--                     OR ISNULL(d.end_date, '19000101') <> ISNULL(s.end_date, '19000101')
--                       THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.campaign_key,
--               s.campaign_id,
--               s.title,
--               s.campaign_status,
--               s.target_amount,
--               s.start_date,
--               s.end_date,
--               s.source_system,
--               s.row_hash,
--               s.created_at,
--               s.updated_at
--         INTO #work_campaign
--         FROM #src_campaign s
--         FULL JOIN dw.dim_campaign d
--                ON d.campaign_id = s.campaign_id
--               AND ISNULL(d.campaign_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_campaign ON #work_campaign(action_code, campaign_id);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.title           = w.title,
--                d.campaign_status = w.campaign_status,
--                d.target_amount   = w.target_amount,
--                d.start_date      = w.start_date,
--                d.end_date        = w.end_date,
--                d.source_system   = w.source_system,
--                d.row_hash        = w.row_hash,
--                d.created_at      = w.created_at,
--                d.updated_at      = w.updated_at
--         FROM dw.dim_campaign d
--         INNER JOIN #work_campaign w ON w.campaign_key = d.campaign_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_campaign
--             (campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
--         SELECT campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at
--         FROM #work_campaign
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(campaign_key) FROM dw.dim_campaign WHERE campaign_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_campaign'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'campaigns', N'dim_campaign', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_campaign incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'campaigns', N'dim_campaign', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_category_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70301, 'Invalid period for incremental dim_category.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_category WHERE category_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_category ON;
--             INSERT INTO dw.dim_category
--                 (category_key, category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', NULL, NULL, N'unknown', N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_category OFF;
--         END

--         SELECT
--               s.id AS category_id,
--               s.name AS category_name,
--               s.parent_id AS parent_category_id,
--               p.name AS parent_category_name,
--               CASE WHEN ISNULL(s.is_active, 0) = 1 THEN N'active' ELSE N'inactive' END AS category_status,
--               s.source_system,
--               s.row_hash,
--               s.created_at,
--               s.updated_at
--         INTO #src_category
--         FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories s
--         INNER JOIN
--         (
--             SELECT id, MAX(stg_row_id) AS max_stg_row_id
--             FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories
--             WHERE is_valid = 1
--               AND id IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY id
--         ) x ON x.max_stg_row_id = s.stg_row_id
--         LEFT JOIN Stg_FinanceOps_DB.stg_finance_ops.expense_categories p
--                ON p.id = s.parent_id
--               AND p.is_valid = 1
--               AND NOT EXISTS
--                   (
--                       SELECT 1
--                       FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories p2
--                       WHERE p2.id = p.id
--                         AND p2.is_valid = 1
--                         AND p2.stg_row_id > p.stg_row_id
--                   );

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_category ON #src_category(category_id);

--         SELECT
--               CASE
--                   WHEN d.category_key IS NULL THEN N'INSERT'
--                   WHEN s.category_id IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(CONVERT(VARBINARY(32), d.row_hash), 0x00) <> ISNULL(CONVERT(VARBINARY(32), s.row_hash), 0x00)
--                     OR ISNULL(d.category_name, N'') <> ISNULL(s.category_name, N'')
--                     OR ISNULL(d.parent_category_id, -999999) <> ISNULL(s.parent_category_id, -999999)
--                     OR ISNULL(d.parent_category_name, N'') <> ISNULL(s.parent_category_name, N'')
--                     OR ISNULL(d.category_status, N'') <> ISNULL(s.category_status, N'')
--                       THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.category_key,
--               s.category_id,
--               s.category_name,
--               s.parent_category_id,
--               s.parent_category_name,
--               s.category_status,
--               s.source_system,
--               s.row_hash,
--               s.created_at,
--               s.updated_at
--         INTO #work_category
--         FROM #src_category s
--         FULL JOIN dw.dim_category d
--                ON d.category_id = s.category_id
--               AND ISNULL(d.category_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_category ON #work_category(action_code, category_id);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.category_name        = w.category_name,
--                d.parent_category_id   = w.parent_category_id,
--                d.parent_category_name = w.parent_category_name,
--                d.category_status      = w.category_status,
--                d.source_system        = w.source_system,
--                d.row_hash             = w.row_hash,
--                d.created_at           = w.created_at,
--                d.updated_at           = w.updated_at
--         FROM dw.dim_category d
--         INNER JOIN #work_category w ON w.category_key = d.category_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_category
--             (category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
--         SELECT category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at
--         FROM #work_category
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(category_key) FROM dw.dim_category WHERE category_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_category'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expense_categories', N'dim_category', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_category incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expense_categories', N'dim_category', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_donation_type_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70401, 'Invalid period for incremental dim_donation_type.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_donation_type WHERE donation_type_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_donation_type ON;
--             INSERT INTO dw.dim_donation_type (donation_type_key, code, title, source_system, created_at, updated_at)
--             VALUES (-1, N'unknown', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_donation_type OFF;
--         END

--         SELECT
--               LOWER(LTRIM(RTRIM(s.donation_type))) AS code,
--               MIN(LTRIM(RTRIM(s.donation_type))) AS title,
--               N'FINANCE_OPS' AS source_system,
--               MIN(COALESCE(s.created_at, s.extracted_at)) AS created_at,
--               MAX(COALESCE(s.updated_at, s.source_updated_at, s.extracted_at)) AS updated_at
--         INTO #src_donation_type
--         FROM Stg_FinanceOps_DB.stg_finance_ops.donations s
--         WHERE s.is_valid = 1
--           AND NULLIF(LTRIM(RTRIM(s.donation_type)), N'') IS NOT NULL
--           AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @start_time
--           AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @end_time
--         GROUP BY LOWER(LTRIM(RTRIM(s.donation_type)));

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_donation_type ON #src_donation_type(code);

--         SELECT
--               CASE
--                   WHEN d.donation_type_key IS NULL THEN N'INSERT'
--                   WHEN s.code IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(d.title,N'') <> ISNULL(s.title,N'') OR ISNULL(d.source_system,N'') <> ISNULL(s.source_system,N'') THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.donation_type_key,
--               s.code, s.title, s.source_system, s.created_at, s.updated_at
--         INTO #work_donation_type
--         FROM #src_donation_type s
--         FULL JOIN dw.dim_donation_type d
--                ON d.code = s.code
--               AND ISNULL(d.donation_type_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_donation_type ON #work_donation_type(action_code, code);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.title = w.title, d.source_system = w.source_system, d.updated_at = w.updated_at
--         FROM dw.dim_donation_type d
--         INNER JOIN #work_donation_type w ON w.donation_type_key = d.donation_type_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_donation_type
--             (code, title, source_system, created_at, updated_at)
--         SELECT code, title, source_system, created_at, updated_at
--         FROM #work_donation_type
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(donation_type_key) FROM dw.dim_donation_type WHERE donation_type_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_donation_type'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'dim_donation_type', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_donation_type incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'dim_donation_type', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_status_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70501, 'Invalid period for incremental dim_status.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_status WHERE status_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_status ON;
--             INSERT INTO dw.dim_status (status_key, status_type, code, title, category, source_system, created_at, updated_at)
--             VALUES (-1, N'unknown', N'unknown', N'Unknown', N'unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_status OFF;
--         END

--         SELECT
--               status_type,
--               code,
--               MIN(title) AS title,
--               status_type AS category,
--               N'FINANCE_OPS' AS source_system,
--               MIN(created_at) AS created_at,
--               MAX(updated_at) AS updated_at
--         INTO #src_status
--         FROM
--         (
--             SELECT N'campaign' AS status_type, LOWER(LTRIM(RTRIM(status))) AS code, MIN(LTRIM(RTRIM(status))) AS title,
--                    MIN(COALESCE(created_at, extracted_at)) AS created_at, MAX(COALESCE(updated_at, source_updated_at, extracted_at)) AS updated_at
--             FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(status)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY LOWER(LTRIM(RTRIM(status)))
--             UNION ALL
--             SELECT N'donation', LOWER(LTRIM(RTRIM(status))), MIN(LTRIM(RTRIM(status))), MIN(COALESCE(created_at, extracted_at)), MAX(COALESCE(updated_at, source_updated_at, extracted_at))
--             FROM Stg_FinanceOps_DB.stg_finance_ops.donations
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(status)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY LOWER(LTRIM(RTRIM(status)))
--             UNION ALL
--             SELECT N'expense', LOWER(LTRIM(RTRIM(status))), MIN(LTRIM(RTRIM(status))), MIN(COALESCE(created_at, extracted_at)), MAX(COALESCE(updated_at, source_updated_at, extracted_at))
--             FROM Stg_FinanceOps_DB.stg_finance_ops.expenses
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(status)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY LOWER(LTRIM(RTRIM(status)))
--             UNION ALL
--             SELECT N'payment', LOWER(LTRIM(RTRIM(status))), MIN(LTRIM(RTRIM(status))), MIN(COALESCE(created_at, extracted_at)), MAX(COALESCE(updated_at, source_updated_at, extracted_at))
--             FROM Stg_FinanceOps_DB.stg_finance_ops.payments
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(status)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY LOWER(LTRIM(RTRIM(status)))
--         ) x
--         GROUP BY status_type, code;

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_status ON #src_status(status_type, code);

--         SELECT
--               CASE
--                   WHEN d.status_key IS NULL THEN N'INSERT'
--                   WHEN s.code IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(d.title,N'') <> ISNULL(s.title,N'') OR ISNULL(d.category,N'') <> ISNULL(s.category,N'') OR ISNULL(d.source_system,N'') <> ISNULL(s.source_system,N'') THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.status_key,
--               s.status_type, s.code, s.title, s.category, s.source_system, s.created_at, s.updated_at
--         INTO #work_status
--         FROM #src_status s
--         FULL JOIN dw.dim_status d
--                ON d.status_type = s.status_type AND d.code = s.code
--               AND ISNULL(d.status_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_status ON #work_status(action_code, code);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.title = w.title, d.category = w.category, d.source_system = w.source_system, d.updated_at = w.updated_at
--         FROM dw.dim_status d
--         INNER JOIN #work_status w ON w.status_key = d.status_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_status
--             (status_type, code, title, category, source_system, created_at, updated_at)
--         SELECT status_type, code, title, category, source_system, created_at, updated_at
--         FROM #work_status
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(status_key) FROM dw.dim_status WHERE status_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_status'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'campaigns/donations/expenses/payments', N'dim_status', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_status incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'campaigns/donations/expenses/payments', N'dim_status', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_currency_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70601, 'Invalid period for incremental dim_currency.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_currency WHERE currency_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_currency ON;
--             INSERT INTO dw.dim_currency (currency_key, code, name, source_system, created_at, updated_at)
--             VALUES (-1, N'UNK', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_currency OFF;
--         END

--         SELECT
--               code,
--               CASE code
--                    WHEN N'IRR' THEN N'Iranian Rial'
--                    WHEN N'USD' THEN N'US Dollar'
--                    WHEN N'EUR' THEN N'Euro'
--                    WHEN N'GBP' THEN N'British Pound'
--                    ELSE code
--               END AS name,
--               N'FINANCE_OPS' AS source_system,
--               MIN(created_at) AS created_at,
--               MAX(updated_at) AS updated_at
--         INTO #src_currency
--         FROM
--         (
--             SELECT UPPER(LTRIM(RTRIM(currency))) AS code, MIN(COALESCE(created_at, extracted_at)) AS created_at, MAX(COALESCE(updated_at, source_updated_at, extracted_at)) AS updated_at
--             FROM Stg_FinanceOps_DB.stg_finance_ops.donations
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(currency)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY UPPER(LTRIM(RTRIM(currency)))
--             UNION ALL
--             SELECT UPPER(LTRIM(RTRIM(currency))), MIN(COALESCE(created_at, extracted_at)), MAX(COALESCE(updated_at, source_updated_at, extracted_at))
--             FROM Stg_FinanceOps_DB.stg_finance_ops.expenses
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(currency)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY UPPER(LTRIM(RTRIM(currency)))
--             UNION ALL
--             SELECT UPPER(LTRIM(RTRIM(currency))), MIN(COALESCE(created_at, extracted_at)), MAX(COALESCE(updated_at, source_updated_at, extracted_at))
--             FROM Stg_FinanceOps_DB.stg_finance_ops.payments
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(currency)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
--             GROUP BY UPPER(LTRIM(RTRIM(currency)))
--             UNION ALL
--             SELECT UPPER(LTRIM(RTRIM(from_currency))), MIN(COALESCE(rate_date, extracted_at)), MAX(extracted_at)
--             FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(from_currency)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, rate_date, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, rate_date, extracted_at) <  @end_time
--             GROUP BY UPPER(LTRIM(RTRIM(from_currency)))
--             UNION ALL
--             SELECT UPPER(LTRIM(RTRIM(to_currency))), MIN(COALESCE(rate_date, extracted_at)), MAX(extracted_at)
--             FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates
--             WHERE is_valid = 1 AND NULLIF(LTRIM(RTRIM(to_currency)), N'') IS NOT NULL
--               AND COALESCE(source_updated_at, rate_date, extracted_at) >= @start_time
--               AND COALESCE(source_updated_at, rate_date, extracted_at) <  @end_time
--             GROUP BY UPPER(LTRIM(RTRIM(to_currency)))
--         ) x
--         GROUP BY code;

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_currency ON #src_currency(code);

--         SELECT
--               CASE
--                   WHEN d.currency_key IS NULL THEN N'INSERT'
--                   WHEN s.code IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(d.name,N'') <> ISNULL(s.name,N'') OR ISNULL(d.source_system,N'') <> ISNULL(s.source_system,N'') THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.currency_key,
--               s.code, s.name, s.source_system, s.created_at, s.updated_at
--         INTO #work_currency
--         FROM #src_currency s
--         FULL JOIN dw.dim_currency d
--                ON d.code = s.code
--               AND ISNULL(d.currency_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_currency ON #work_currency(action_code, code);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.name = w.name, d.source_system = w.source_system, d.updated_at = w.updated_at
--         FROM dw.dim_currency d
--         INNER JOIN #work_currency w ON w.currency_key = d.currency_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_currency
--             (code, name, source_system, created_at, updated_at)
--         SELECT code, name, source_system, created_at, updated_at
--         FROM #work_currency
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(currency_key) FROM dw.dim_currency WHERE currency_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_currency'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations/expenses/payments/currency_rates', N'dim_currency', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_currency incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations/expenses/payments/currency_rates', N'dim_currency', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_dim_allocation_type_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_DIMENSION', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 70701, 'Invalid period for incremental dim_allocation_type.', 1;

--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_allocation_type WHERE allocation_type_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_allocation_type ON;
--             INSERT INTO dw.dim_allocation_type (allocation_type_key, code, title, source_system, created_at, updated_at)
--             VALUES (-1, N'unknown', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
--             SET IDENTITY_INSERT dw.dim_allocation_type OFF;
--         END

--         SELECT
--               LOWER(LTRIM(RTRIM(source_type))) AS code,
--               MIN(LTRIM(RTRIM(source_type))) AS title,
--               N'FINANCE_OPS' AS source_system,
--               MIN(COALESCE(created_at, extracted_at)) AS created_at,
--               MAX(COALESCE(source_updated_at, created_at, extracted_at)) AS updated_at
--         INTO #src_allocation_type
--         FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
--         WHERE is_valid = 1
--           AND NULLIF(LTRIM(RTRIM(source_type)), N'') IS NOT NULL
--           AND COALESCE(source_updated_at, created_at, allocation_date, extracted_at) >= @start_time
--           AND COALESCE(source_updated_at, created_at, allocation_date, extracted_at) <  @end_time
--         GROUP BY LOWER(LTRIM(RTRIM(source_type)));

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_allocation_type ON #src_allocation_type(code);

--         SELECT
--               CASE
--                   WHEN d.allocation_type_key IS NULL THEN N'INSERT'
--                   WHEN s.code IS NULL THEN N'NO_SOURCE'
--                   WHEN ISNULL(d.title,N'') <> ISNULL(s.title,N'') OR ISNULL(d.source_system,N'') <> ISNULL(s.source_system,N'') THEN N'UPDATE'
--                   ELSE N'NO_CHANGE'
--               END AS action_code,
--               d.allocation_type_key,
--               s.code, s.title, s.source_system, s.created_at, s.updated_at
--         INTO #work_allocation_type
--         FROM #src_allocation_type s
--         FULL JOIN dw.dim_allocation_type d
--                ON d.code = s.code
--               AND ISNULL(d.allocation_type_key, -999999) <> -1;

--         CREATE CLUSTERED INDEX CX_work_allocation_type ON #work_allocation_type(action_code, code);

--         BEGIN TRAN;

--         UPDATE d
--            SET d.title = w.title, d.source_system = w.source_system, d.updated_at = w.updated_at
--         FROM dw.dim_allocation_type d
--         INNER JOIN #work_allocation_type w ON w.allocation_type_key = d.allocation_type_key
--         WHERE w.action_code = N'UPDATE';
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.dim_allocation_type
--             (code, title, source_system, created_at, updated_at)
--         SELECT code, title, source_system, created_at, updated_at
--         FROM #work_allocation_type
--         WHERE action_code = N'INSERT';
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key INT = ISNULL((SELECT MAX(allocation_type_key) FROM dw.dim_allocation_type WHERE allocation_type_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.dim_allocation_type'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'dim_allocation_type', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based FULL JOIN dim_allocation_type incremental load.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'dim_allocation_type', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_donation_transaction_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);
--     DECLARE @start_key INT = CAST(CONVERT(CHAR(8), CAST(@start_time AS DATE), 112) AS INT);
--     DECLARE @end_key_exclusive INT = CAST(CONVERT(CHAR(8), CAST(@end_time AS DATE), 112) AS INT);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_fact_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 71001, 'Invalid period for fact_donation_transaction.', 1;

--         SET @step_started = SYSDATETIME();

--         SELECT
--               ISNULL(dd.TimeKey, -1) AS date_key,
--               ISNULL(donor.donor_key, -1) AS donor_key,
--               ISNULL(camp.campaign_key, -1) AS campaign_key,
--               CONVERT(INT, -1) AS center_key,
--               ISNULL(dt.donation_type_key, -1) AS donation_type_key,
--               ISNULL(cur.currency_key, -1) AS currency_key,
--               ISNULL(st.status_key, -1) AS status_key,
--               s.amount,
--               CASE WHEN LOWER(ISNULL(s.status, N'')) IN (N'confirmed', N'paid', N'completed', N'success') THEN CONVERT(BIT,1) ELSE CONVERT(BIT,0) END AS is_confirmed,
--               CASE WHEN LOWER(ISNULL(s.status, N'')) IN (N'refunded') THEN CONVERT(BIT,1) ELSE CONVERT(BIT,0) END AS is_refunded,
--               CONVERT(BIGINT, s.id) AS source_donation_id,
--               CONVERT(BIGINT, s.donor_id) AS source_donor_id,
--               CONVERT(BIGINT, s.campaign_id) AS source_campaign_id,
--               s.reference_code AS source_reference_code,
--               s.source_system
--         INTO #src_fact_donation
--         FROM Stg_FinanceOps_DB.stg_finance_ops.donations s
--         LEFT JOIN dw.dim_date dd
--                ON dd.FullDateAlternateKey = CAST(s.donation_date AS DATE)
--         LEFT JOIN dw.dim_donor donor
--                ON donor.donor_id = s.donor_id
--               AND donor.donor_key <> -1
--         LEFT JOIN dw.dim_campaign camp
--                ON camp.campaign_id = s.campaign_id
--               AND camp.campaign_key <> -1
--         LEFT JOIN dw.dim_donation_type dt
--                ON dt.code = LOWER(LTRIM(RTRIM(s.donation_type)))
--               AND dt.donation_type_key <> -1
--         LEFT JOIN dw.dim_currency cur
--                ON cur.code = UPPER(LTRIM(RTRIM(s.currency)))
--               AND cur.currency_key <> -1
--         LEFT JOIN dw.dim_status st
--                ON st.status_type = N'donation'
--               AND st.code = LOWER(LTRIM(RTRIM(s.status)))
--               AND st.status_key <> -1
--         WHERE s.is_valid = 1
--           AND s.id IS NOT NULL
--           AND (
--                  (s.donation_date >= CAST(@start_time AS DATE) AND s.donation_date < CAST(@end_time AS DATE))
--               OR (COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @start_time
--                   AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @end_time)
--           )
--           AND NOT EXISTS
--               (
--                   SELECT 1
--                   FROM Stg_FinanceOps_DB.stg_finance_ops.donations s2
--                   WHERE s2.id = s.id
--                     AND s2.is_valid = 1
--                     AND s2.stg_row_id > s.stg_row_id
--               );

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_fact_donation ON #src_fact_donation(source_donation_id);

--         BEGIN TRAN;

--         UPDATE f
--            SET f.date_key              = s.date_key,
--                f.donor_key             = s.donor_key,
--                f.campaign_key          = s.campaign_key,
--                f.center_key            = s.center_key,
--                f.donation_type_key     = s.donation_type_key,
--                f.currency_key          = s.currency_key,
--                f.status_key            = s.status_key,
--                f.amount                = s.amount,
--                f.is_confirmed          = s.is_confirmed,
--                f.is_refunded           = s.is_refunded,
--                f.source_donor_id       = s.source_donor_id,
--                f.source_campaign_id    = s.source_campaign_id,
--                f.source_reference_code = s.source_reference_code,
--                f.source_system         = s.source_system,
--                f.etl_batch_id          = @etl_batch_id,
--                f.loaded_at             = SYSDATETIME()
--         FROM dw.fact_donation_transaction f
--         INNER JOIN #src_fact_donation s
--                 ON s.source_donation_id = f.source_donation_id
--         WHERE ISNULL(f.date_key, -1) BETWEEN @start_key AND 99991231
--           AND (
--                  ISNULL(f.date_key, -1) <> ISNULL(s.date_key, -1)
--               OR ISNULL(f.donor_key, -1) <> ISNULL(s.donor_key, -1)
--               OR ISNULL(f.campaign_key, -1) <> ISNULL(s.campaign_key, -1)
--               OR ISNULL(f.donation_type_key, -1) <> ISNULL(s.donation_type_key, -1)
--               OR ISNULL(f.currency_key, -1) <> ISNULL(s.currency_key, -1)
--               OR ISNULL(f.status_key, -1) <> ISNULL(s.status_key, -1)
--               OR ISNULL(f.amount, 0) <> ISNULL(s.amount, 0)
--               OR ISNULL(f.is_confirmed, 0) <> ISNULL(s.is_confirmed, 0)
--               OR ISNULL(f.is_refunded, 0) <> ISNULL(s.is_refunded, 0)
--               OR ISNULL(f.source_reference_code, N'') <> ISNULL(s.source_reference_code, N'')
--           );
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.fact_donation_transaction
--         (
--               date_key, donor_key, campaign_key, center_key, donation_type_key, currency_key, status_key,
--               amount, is_confirmed, is_refunded, source_donation_id, source_donor_id, source_campaign_id,
--               source_reference_code, source_system, etl_batch_id, loaded_at
--         )
--         SELECT
--               s.date_key, s.donor_key, s.campaign_key, s.center_key, s.donation_type_key, s.currency_key, s.status_key,
--               s.amount, s.is_confirmed, s.is_refunded, s.source_donation_id, s.source_donor_id, s.source_campaign_id,
--               s.source_reference_code, s.source_system, @etl_batch_id, SYSDATETIME()
--         FROM #src_fact_donation s
--         WHERE NOT EXISTS
--               (
--                   SELECT 1
--                   FROM dw.fact_donation_transaction f
--                   WHERE f.source_donation_id = s.source_donation_id
--               );
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key BIGINT = ISNULL((SELECT MAX(donation_transaction_key) FROM dw.fact_donation_transaction WHERE donation_transaction_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.fact_donation_transaction'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'fact_donation_transaction', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based upsert fact_donation_transaction.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations', N'fact_donation_transaction', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_expense_transaction_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);
--     DECLARE @start_key INT = CAST(CONVERT(CHAR(8), CAST(@start_time AS DATE), 112) AS INT);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_fact_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 71101, 'Invalid period for fact_expense_transaction.', 1;

--         SET @step_started = SYSDATETIME();

--         SELECT
--               ISNULL(dd.TimeKey, -1) AS date_key,
--               ISNULL(dc.center_key, -1) AS center_key,
--               ISNULL(ch.child_key, -1) AS child_key,
--               ISNULL(cat.category_key, -1) AS category_key,
--               ISNULL(cur.currency_key, -1) AS currency_key,
--               ISNULL(st.status_key, -1) AS status_key,
--               s.amount,
--               CASE WHEN LOWER(ISNULL(s.status, N'')) IN (N'approved', N'paid', N'completed') THEN CONVERT(BIT,1) ELSE CONVERT(BIT,0) END AS is_approved,
--               CASE WHEN LOWER(ISNULL(s.status, N'')) IN (N'rejected', N'cancelled') THEN CONVERT(BIT,1) ELSE CONVERT(BIT,0) END AS is_rejected,
--               s.description,
--               CONVERT(BIGINT, s.id) AS source_expense_id,
--               CONVERT(BIGINT, s.center_id) AS source_center_id,
--               CONVERT(BIGINT, s.child_id) AS source_child_id,
--               CONVERT(BIGINT, s.category_id) AS source_category_id,
--               s.source_system
--         INTO #src_fact_expense
--         FROM Stg_FinanceOps_DB.stg_finance_ops.expenses s
--         LEFT JOIN dw.dim_date dd
--                ON dd.FullDateAlternateKey = CAST(s.expense_date AS DATE)
--         LEFT JOIN dw.dim_center dc
--                ON dc.center_id = CONVERT(BIGINT, s.center_id)
--               AND dc.center_key <> -1
--         LEFT JOIN dw.dim_child ch
--                ON ch.child_id = CONVERT(BIGINT, s.child_id)
--               AND ch.child_key <> -1
--         LEFT JOIN dw.dim_category cat
--                ON cat.category_id = s.category_id
--               AND cat.category_key <> -1
--         LEFT JOIN dw.dim_currency cur
--                ON cur.code = UPPER(LTRIM(RTRIM(s.currency)))
--               AND cur.currency_key <> -1
--         LEFT JOIN dw.dim_status st
--                ON st.status_type = N'expense'
--               AND st.code = LOWER(LTRIM(RTRIM(s.status)))
--               AND st.status_key <> -1
--         WHERE s.is_valid = 1
--           AND s.id IS NOT NULL
--           AND (
--                  (s.expense_date >= CAST(@start_time AS DATE) AND s.expense_date < CAST(@end_time AS DATE))
--               OR (COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @start_time
--                   AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @end_time)
--           )
--           AND NOT EXISTS
--               (
--                   SELECT 1
--                   FROM Stg_FinanceOps_DB.stg_finance_ops.expenses s2
--                   WHERE s2.id = s.id
--                     AND s2.is_valid = 1
--                     AND s2.stg_row_id > s.stg_row_id
--               );

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_fact_expense ON #src_fact_expense(source_expense_id);

--         BEGIN TRAN;

--         UPDATE f
--            SET f.date_key           = s.date_key,
--                f.center_key         = s.center_key,
--                f.child_key          = s.child_key,
--                f.category_key       = s.category_key,
--                f.currency_key       = s.currency_key,
--                f.status_key         = s.status_key,
--                f.amount             = s.amount,
--                f.is_approved        = s.is_approved,
--                f.is_rejected        = s.is_rejected,
--                f.description        = s.description,
--                f.source_center_id   = s.source_center_id,
--                f.source_child_id    = s.source_child_id,
--                f.source_category_id = s.source_category_id,
--                f.source_system      = s.source_system,
--                f.etl_batch_id       = @etl_batch_id,
--                f.loaded_at          = SYSDATETIME()
--         FROM dw.fact_expense_transaction f
--         INNER JOIN #src_fact_expense s
--                 ON s.source_expense_id = f.source_expense_id
--         WHERE ISNULL(f.date_key, -1) BETWEEN @start_key AND 99991231
--           AND (
--                  ISNULL(f.date_key, -1) <> ISNULL(s.date_key, -1)
--               OR ISNULL(f.center_key, -1) <> ISNULL(s.center_key, -1)
--               OR ISNULL(f.child_key, -1) <> ISNULL(s.child_key, -1)
--               OR ISNULL(f.category_key, -1) <> ISNULL(s.category_key, -1)
--               OR ISNULL(f.currency_key, -1) <> ISNULL(s.currency_key, -1)
--               OR ISNULL(f.status_key, -1) <> ISNULL(s.status_key, -1)
--               OR ISNULL(f.amount, 0) <> ISNULL(s.amount, 0)
--               OR ISNULL(f.is_approved, 0) <> ISNULL(s.is_approved, 0)
--               OR ISNULL(f.is_rejected, 0) <> ISNULL(s.is_rejected, 0)
--               OR ISNULL(f.description, N'') <> ISNULL(s.description, N'')
--           );
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.fact_expense_transaction
--         (
--               date_key, center_key, child_key, category_key, currency_key, status_key,
--               amount, is_approved, is_rejected, description, source_expense_id, source_center_id,
--               source_child_id, source_category_id, source_system, etl_batch_id, loaded_at
--         )
--         SELECT
--               s.date_key, s.center_key, s.child_key, s.category_key, s.currency_key, s.status_key,
--               s.amount, s.is_approved, s.is_rejected, s.description, s.source_expense_id, s.source_center_id,
--               s.source_child_id, s.source_category_id, s.source_system, @etl_batch_id, SYSDATETIME()
--         FROM #src_fact_expense s
--         WHERE NOT EXISTS
--               (
--                   SELECT 1
--                   FROM dw.fact_expense_transaction f
--                   WHERE f.source_expense_id = s.source_expense_id
--               );
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key BIGINT = ISNULL((SELECT MAX(expense_transaction_key) FROM dw.fact_expense_transaction WHERE expense_transaction_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.fact_expense_transaction'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expenses', N'fact_expense_transaction', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based upsert fact_expense_transaction.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'expenses', N'fact_expense_transaction', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_payment_transaction_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);
--     DECLARE @start_key INT = CAST(CONVERT(CHAR(8), CAST(@start_time AS DATE), 112) AS INT);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_fact_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 71201, 'Invalid period for fact_payment_transaction.', 1;

--         SET @step_started = SYSDATETIME();

--         SELECT
--               ISNULL(dd.TimeKey, -1) AS date_key,
--               ISNULL(dc.center_key, -1) AS center_key,
--               ISNULL(cur.currency_key, -1) AS currency_key,
--               ISNULL(st.status_key, -1) AS status_key,
--               s.payment_type,
--               CONVERT(BIGINT, s.teacher_id) AS source_teacher_id,
--               s.amount,
--               CASE WHEN LOWER(ISNULL(s.status, N'')) IN (N'paid', N'completed', N'success') THEN CONVERT(BIT,1) ELSE CONVERT(BIT,0) END AS is_paid,
--               CASE WHEN LOWER(ISNULL(s.status, N'')) IN (N'cancelled', N'rejected') THEN CONVERT(BIT,1) ELSE CONVERT(BIT,0) END AS is_cancelled,
--               CONVERT(BIGINT, s.id) AS source_payment_id,
--               CONVERT(BIGINT, s.center_id) AS source_center_id,
--               s.source_system
--         INTO #src_fact_payment
--         FROM Stg_FinanceOps_DB.stg_finance_ops.payments s
--         LEFT JOIN dw.dim_date dd
--                ON dd.FullDateAlternateKey = CAST(s.payment_date AS DATE)
--         LEFT JOIN dw.dim_center dc
--                ON dc.center_id = CONVERT(BIGINT, s.center_id)
--               AND dc.center_key <> -1
--         LEFT JOIN dw.dim_currency cur
--                ON cur.code = UPPER(LTRIM(RTRIM(s.currency)))
--               AND cur.currency_key <> -1
--         LEFT JOIN dw.dim_status st
--                ON st.status_type = N'payment'
--               AND st.code = LOWER(LTRIM(RTRIM(s.status)))
--               AND st.status_key <> -1
--         WHERE s.is_valid = 1
--           AND s.id IS NOT NULL
--           AND (
--                  (s.payment_date >= CAST(@start_time AS DATE) AND s.payment_date < CAST(@end_time AS DATE))
--               OR (COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @start_time
--                   AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @end_time)
--           )
--           AND NOT EXISTS
--               (
--                   SELECT 1
--                   FROM Stg_FinanceOps_DB.stg_finance_ops.payments s2
--                   WHERE s2.id = s.id
--                     AND s2.is_valid = 1
--                     AND s2.stg_row_id > s.stg_row_id
--               );

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_fact_payment ON #src_fact_payment(source_payment_id);

--         BEGIN TRAN;

--         UPDATE f
--            SET f.date_key          = s.date_key,
--                f.center_key        = s.center_key,
--                f.currency_key      = s.currency_key,
--                f.status_key        = s.status_key,
--                f.payment_type      = s.payment_type,
--                f.source_teacher_id = s.source_teacher_id,
--                f.amount            = s.amount,
--                f.is_paid           = s.is_paid,
--                f.is_cancelled      = s.is_cancelled,
--                f.source_center_id  = s.source_center_id,
--                f.source_system     = s.source_system,
--                f.etl_batch_id      = @etl_batch_id,
--                f.loaded_at         = SYSDATETIME()
--         FROM dw.fact_payment_transaction f
--         INNER JOIN #src_fact_payment s
--                 ON s.source_payment_id = f.source_payment_id
--         WHERE ISNULL(f.date_key, -1) BETWEEN @start_key AND 99991231
--           AND (
--                  ISNULL(f.date_key, -1) <> ISNULL(s.date_key, -1)
--               OR ISNULL(f.center_key, -1) <> ISNULL(s.center_key, -1)
--               OR ISNULL(f.currency_key, -1) <> ISNULL(s.currency_key, -1)
--               OR ISNULL(f.status_key, -1) <> ISNULL(s.status_key, -1)
--               OR ISNULL(f.payment_type, N'') <> ISNULL(s.payment_type, N'')
--               OR ISNULL(f.source_teacher_id, -1) <> ISNULL(s.source_teacher_id, -1)
--               OR ISNULL(f.amount, 0) <> ISNULL(s.amount, 0)
--               OR ISNULL(f.is_paid, 0) <> ISNULL(s.is_paid, 0)
--               OR ISNULL(f.is_cancelled, 0) <> ISNULL(s.is_cancelled, 0)
--           );
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.fact_payment_transaction
--         (
--               date_key, center_key, currency_key, status_key, payment_type, source_teacher_id,
--               amount, is_paid, is_cancelled, source_payment_id, source_center_id,
--               source_system, etl_batch_id, loaded_at
--         )
--         SELECT
--               s.date_key, s.center_key, s.currency_key, s.status_key, s.payment_type, s.source_teacher_id,
--               s.amount, s.is_paid, s.is_cancelled, s.source_payment_id, s.source_center_id,
--               s.source_system, @etl_batch_id, SYSDATETIME()
--         FROM #src_fact_payment s
--         WHERE NOT EXISTS
--               (
--                   SELECT 1
--                   FROM dw.fact_payment_transaction f
--                   WHERE f.source_payment_id = s.source_payment_id
--               );
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key BIGINT = ISNULL((SELECT MAX(payment_transaction_key) FROM dw.fact_payment_transaction WHERE payment_transaction_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.fact_payment_transaction'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'payments', N'fact_payment_transaction', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based upsert fact_payment_transaction.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'payments', N'fact_payment_transaction', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_budget_allocation_event_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);
--     DECLARE @start_key INT = CAST(CONVERT(CHAR(8), CAST(@start_time AS DATE), 112) AS INT);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_fact_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 71301, 'Invalid period for fact_budget_allocation_event.', 1;

--         SET @step_started = SYSDATETIME();

--         SELECT
--               ISNULL(dd.TimeKey, -1) AS date_key,
--               CASE WHEN LOWER(ISNULL(s.source_type,N'')) = N'donation' THEN ISNULL(donor.donor_key, -1) ELSE -1 END AS donor_key,
--               ISNULL(dc.center_key, -1) AS center_key,
--               ISNULL(ch.child_key, -1) AS child_key,
--               ISNULL(cat.category_key, -1) AS category_key,
--               CASE WHEN LOWER(ISNULL(s.source_type,N'')) = N'donation' THEN ISNULL(camp.campaign_key, -1) ELSE -1 END AS campaign_key,
--               ISNULL(at.allocation_type_key, -1) AS allocation_type_key,
--               s.allocated_amount,
--               s.reason,
--               CONVERT(BIGINT, s.id) AS source_allocation_id,
--               s.source_type,
--               CONVERT(BIGINT, s.source_id) AS source_id,
--               CONVERT(BIGINT, s.center_id) AS source_center_id,
--               CONVERT(BIGINT, s.child_id) AS source_child_id,
--               CONVERT(BIGINT, s.category_id) AS source_category_id,
--               s.source_system
--         INTO #src_fact_allocation
--         FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations s
--         LEFT JOIN Stg_FinanceOps_DB.stg_finance_ops.donations don
--                ON don.id = s.source_id
--               AND LOWER(ISNULL(s.source_type,N'')) = N'donation'
--               AND don.is_valid = 1
--               AND NOT EXISTS
--                   (
--                       SELECT 1
--                       FROM Stg_FinanceOps_DB.stg_finance_ops.donations don2
--                       WHERE don2.id = don.id
--                         AND don2.is_valid = 1
--                         AND don2.stg_row_id > don.stg_row_id
--                   )
--         LEFT JOIN dw.dim_date dd
--                ON dd.FullDateAlternateKey = CAST(s.allocation_date AS DATE)
--         LEFT JOIN dw.dim_donor donor
--                ON donor.donor_id = don.donor_id
--               AND donor.donor_key <> -1
--         LEFT JOIN dw.dim_campaign camp
--                ON camp.campaign_id = don.campaign_id
--               AND camp.campaign_key <> -1
--         LEFT JOIN dw.dim_center dc
--                ON dc.center_id = CONVERT(BIGINT, s.center_id)
--               AND dc.center_key <> -1
--         LEFT JOIN dw.dim_child ch
--                ON ch.child_id = CONVERT(BIGINT, s.child_id)
--               AND ch.child_key <> -1
--         LEFT JOIN dw.dim_category cat
--                ON cat.category_id = s.category_id
--               AND cat.category_key <> -1
--         LEFT JOIN dw.dim_allocation_type at
--                ON at.code = LOWER(LTRIM(RTRIM(s.source_type)))
--               AND at.allocation_type_key <> -1
--         WHERE s.is_valid = 1
--           AND s.id IS NOT NULL
--           AND (
--                  (s.allocation_date >= CAST(@start_time AS DATE) AND s.allocation_date < CAST(@end_time AS DATE))
--               OR (COALESCE(s.source_updated_at, s.created_at, s.allocation_date, s.extracted_at) >= @start_time
--                   AND COALESCE(s.source_updated_at, s.created_at, s.allocation_date, s.extracted_at) <  @end_time)
--           )
--           AND NOT EXISTS
--               (
--                   SELECT 1
--                   FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations s2
--                   WHERE s2.id = s.id
--                     AND s2.is_valid = 1
--                     AND s2.stg_row_id > s.stg_row_id
--               );

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_fact_allocation ON #src_fact_allocation(source_allocation_id);

--         BEGIN TRAN;

--         UPDATE f
--            SET f.date_key             = s.date_key,
--                f.donor_key            = s.donor_key,
--                f.center_key           = s.center_key,
--                f.child_key            = s.child_key,
--                f.category_key         = s.category_key,
--                f.campaign_key         = s.campaign_key,
--                f.allocation_type_key  = s.allocation_type_key,
--                f.allocated_amount     = s.allocated_amount,
--                f.reason               = s.reason,
--                f.source_type          = s.source_type,
--                f.source_id            = s.source_id,
--                f.source_center_id     = s.source_center_id,
--                f.source_child_id      = s.source_child_id,
--                f.source_category_id   = s.source_category_id,
--                f.source_system        = s.source_system,
--                f.etl_batch_id         = @etl_batch_id,
--                f.loaded_at            = SYSDATETIME()
--         FROM dw.fact_budget_allocation_event f
--         INNER JOIN #src_fact_allocation s
--                 ON s.source_allocation_id = f.source_allocation_id
--         WHERE ISNULL(f.date_key, -1) BETWEEN @start_key AND 99991231
--           AND (
--                  ISNULL(f.date_key, -1) <> ISNULL(s.date_key, -1)
--               OR ISNULL(f.donor_key, -1) <> ISNULL(s.donor_key, -1)
--               OR ISNULL(f.center_key, -1) <> ISNULL(s.center_key, -1)
--               OR ISNULL(f.child_key, -1) <> ISNULL(s.child_key, -1)
--               OR ISNULL(f.category_key, -1) <> ISNULL(s.category_key, -1)
--               OR ISNULL(f.campaign_key, -1) <> ISNULL(s.campaign_key, -1)
--               OR ISNULL(f.allocation_type_key, -1) <> ISNULL(s.allocation_type_key, -1)
--               OR ISNULL(f.allocated_amount, 0) <> ISNULL(s.allocated_amount, 0)
--               OR ISNULL(f.reason, N'') <> ISNULL(s.reason, N'')
--               OR ISNULL(f.source_type, N'') <> ISNULL(s.source_type, N'')
--               OR ISNULL(f.source_id, -1) <> ISNULL(s.source_id, -1)
--           );
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.fact_budget_allocation_event
--         (
--               date_key, donor_key, center_key, child_key, category_key, campaign_key, allocation_type_key,
--               allocated_amount, reason, source_allocation_id, source_type, source_id, source_center_id,
--               source_child_id, source_category_id, source_system, etl_batch_id, loaded_at
--         )
--         SELECT
--               s.date_key, s.donor_key, s.center_key, s.child_key, s.category_key, s.campaign_key, s.allocation_type_key,
--               s.allocated_amount, s.reason, s.source_allocation_id, s.source_type, s.source_id, s.source_center_id,
--               s.source_child_id, s.source_category_id, s.source_system, @etl_batch_id, SYSDATETIME()
--         FROM #src_fact_allocation s
--         WHERE NOT EXISTS
--               (
--                   SELECT 1
--                   FROM dw.fact_budget_allocation_event f
--                   WHERE f.source_allocation_id = s.source_allocation_id
--               );
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key BIGINT = ISNULL((SELECT MAX(allocation_event_key) FROM dw.fact_budget_allocation_event WHERE allocation_event_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.fact_budget_allocation_event'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'fact_budget_allocation_event', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based upsert fact_budget_allocation_event.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'budget_allocations', N'fact_budget_allocation_event', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_monthly_financial_snapshot_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);
--     DECLARE @month_start DATE, @range_end DATE, @month_end DATE, @month_key INT;
--     DECLARE @inserted INT, @updated INT;

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_fact_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 71501, 'Invalid period for fact_monthly_financial_snapshot.', 1;

--         SET @step_started = SYSDATETIME();
--         SET @month_start = DATEFROMPARTS(YEAR(CAST(@start_time AS DATE)), MONTH(CAST(@start_time AS DATE)), 1);
--         SET @range_end = CAST(@end_time AS DATE);

--         WHILE @month_start < @range_end
--         BEGIN
--             SET @month_end = EOMONTH(@month_start);

--             SELECT @month_key = TimeKey
--             FROM dw.dim_date
--             WHERE FullDateAlternateKey = @month_end;

--             IF @month_key IS NULL
--             BEGIN
--                 SET @rows_rejected += 1;
--                 EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'dw_facts', N'fact_monthly_financial_snapshot', N'warning', 0, 0, 0, 1, @step_started, N'Missing month end date in dw.dim_date. Snapshot month skipped.';
--                 SET @month_start = DATEADD(MONTH, 1, @month_start);
--                 CONTINUE;
--             END

--             IF OBJECT_ID('tempdb..#month_movement') IS NOT NULL DROP TABLE #month_movement;

--             SELECT
--                   center_key,
--                   SUM(total_donation_amount) AS total_donation_amount,
--                   SUM(total_expense_amount) AS total_expense_amount,
--                   SUM(total_payment_amount) AS total_payment_amount,
--                   SUM(donation_count) AS donation_count,
--                   SUM(expense_count) AS expense_count,
--                   SUM(payment_count) AS payment_count,
--                   SUM(allocation_count) AS allocation_count
--             INTO #month_movement
--             FROM
--             (
--                 SELECT
--                       ISNULL(a.center_key, -1) AS center_key,
--                       SUM(CASE WHEN LOWER(ISNULL(a.source_type,N'')) = N'donation' AND ISNULL(d.is_confirmed,0) = 1 THEN ISNULL(a.allocated_amount,0) ELSE 0 END) AS total_donation_amount,
--                       CONVERT(DECIMAL(18,2),0) AS total_expense_amount,
--                       CONVERT(DECIMAL(18,2),0) AS total_payment_amount,
--                       COUNT(DISTINCT CASE WHEN LOWER(ISNULL(a.source_type,N'')) = N'donation' AND ISNULL(d.is_confirmed,0) = 1 THEN a.source_id END) AS donation_count,
--                       0 AS expense_count,
--                       0 AS payment_count,
--                       COUNT_BIG(*) AS allocation_count
--                 FROM dw.fact_budget_allocation_event a
--                 LEFT JOIN dw.fact_donation_transaction d
--                        ON d.source_donation_id = a.source_id
--                 WHERE a.date_key >= CAST(CONVERT(CHAR(8), @month_start, 112) AS INT)
--                   AND a.date_key <= CAST(CONVERT(CHAR(8), @month_end, 112) AS INT)
--                 GROUP BY ISNULL(a.center_key, -1)

--                 UNION ALL

--                 SELECT
--                       ISNULL(e.center_key, -1),
--                       CONVERT(DECIMAL(18,2),0),
--                       SUM(CASE WHEN ISNULL(e.is_approved,0) = 1 THEN ISNULL(e.amount,0) ELSE 0 END),
--                       CONVERT(DECIMAL(18,2),0),
--                       0,
--                       SUM(CASE WHEN ISNULL(e.is_approved,0) = 1 THEN 1 ELSE 0 END),
--                       0,
--                       0
--                 FROM dw.fact_expense_transaction e
--                 WHERE e.date_key >= CAST(CONVERT(CHAR(8), @month_start, 112) AS INT)
--                   AND e.date_key <= CAST(CONVERT(CHAR(8), @month_end, 112) AS INT)
--                 GROUP BY ISNULL(e.center_key, -1)

--                 UNION ALL

--                 SELECT
--                       ISNULL(p.center_key, -1),
--                       CONVERT(DECIMAL(18,2),0),
--                       CONVERT(DECIMAL(18,2),0),
--                       SUM(CASE WHEN ISNULL(p.is_paid,0) = 1 THEN ISNULL(p.amount,0) ELSE 0 END),
--                       0,
--                       0,
--                       SUM(CASE WHEN ISNULL(p.is_paid,0) = 1 THEN 1 ELSE 0 END),
--                       0
--                 FROM dw.fact_payment_transaction p
--                 WHERE p.date_key >= CAST(CONVERT(CHAR(8), @month_start, 112) AS INT)
--                   AND p.date_key <= CAST(CONVERT(CHAR(8), @month_end, 112) AS INT)
--                 GROUP BY ISNULL(p.center_key, -1)

--                 UNION ALL

--                 SELECT
--                       center_key,
--                       CONVERT(DECIMAL(18,2),0),
--                       CONVERT(DECIMAL(18,2),0),
--                       CONVERT(DECIMAL(18,2),0),
--                       0,0,0,0
--                 FROM dw.fact_monthly_financial_snapshot
--                 WHERE month_key = @month_key
--             ) m
--             GROUP BY center_key;

--             CREATE CLUSTERED INDEX CX_month_movement ON #month_movement(center_key);
--             SET @rows_read += @@ROWCOUNT;

--             BEGIN TRAN;

--             UPDATE f
--                SET f.total_donation_amount = ISNULL(m.total_donation_amount, 0),
--                    f.total_expense_amount  = ISNULL(m.total_expense_amount, 0),
--                    f.total_payment_amount  = ISNULL(m.total_payment_amount, 0),
--                    f.net_balance           = ISNULL(m.total_donation_amount,0) - ISNULL(m.total_expense_amount,0) - ISNULL(m.total_payment_amount,0),
--                    f.donation_count        = ISNULL(m.donation_count, 0),
--                    f.expense_count         = ISNULL(m.expense_count, 0),
--                    f.payment_count         = ISNULL(m.payment_count, 0),
--                    f.allocation_count      = ISNULL(m.allocation_count, 0),
--                    f.source_system         = N'FINANCE_OPS',
--                    f.etl_batch_id          = @etl_batch_id,
--                    f.loaded_at             = SYSDATETIME()
--             FROM dw.fact_monthly_financial_snapshot f
--             INNER JOIN #month_movement m
--                     ON m.center_key = f.center_key
--                    AND f.month_key = @month_key
--             WHERE ISNULL(f.total_donation_amount,0) <> ISNULL(m.total_donation_amount,0)
--                OR ISNULL(f.total_expense_amount,0) <> ISNULL(m.total_expense_amount,0)
--                OR ISNULL(f.total_payment_amount,0) <> ISNULL(m.total_payment_amount,0)
--                OR ISNULL(f.donation_count,0) <> ISNULL(m.donation_count,0)
--                OR ISNULL(f.expense_count,0) <> ISNULL(m.expense_count,0)
--                OR ISNULL(f.payment_count,0) <> ISNULL(m.payment_count,0)
--                OR ISNULL(f.allocation_count,0) <> ISNULL(m.allocation_count,0);
--             SET @updated = @@ROWCOUNT;
--             SET @rows_updated += @updated;

--             INSERT INTO dw.fact_monthly_financial_snapshot
--             (
--                   month_key, center_key, total_donation_amount, total_expense_amount, total_payment_amount,
--                   net_balance, donation_count, expense_count, payment_count, allocation_count,
--                   source_system, etl_batch_id, loaded_at
--             )
--             SELECT
--                   @month_key,
--                   m.center_key,
--                   ISNULL(m.total_donation_amount,0),
--                   ISNULL(m.total_expense_amount,0),
--                   ISNULL(m.total_payment_amount,0),
--                   ISNULL(m.total_donation_amount,0) - ISNULL(m.total_expense_amount,0) - ISNULL(m.total_payment_amount,0),
--                   ISNULL(m.donation_count,0),
--                   ISNULL(m.expense_count,0),
--                   ISNULL(m.payment_count,0),
--                   ISNULL(m.allocation_count,0),
--                   N'FINANCE_OPS',
--                   @etl_batch_id,
--                   SYSDATETIME()
--             FROM #month_movement m
--             WHERE NOT EXISTS
--                   (
--                       SELECT 1
--                       FROM dw.fact_monthly_financial_snapshot f
--                       WHERE f.month_key = @month_key
--                         AND f.center_key = m.center_key
--                   );
--             SET @inserted = @@ROWCOUNT;
--             SET @rows_inserted += @inserted;

--             COMMIT TRAN;

--             SET @month_start = DATEADD(MONTH, 1, @month_start);
--         END

--         DECLARE @max_key BIGINT = ISNULL((SELECT MAX(monthly_financial_snapshot_key) FROM dw.fact_monthly_financial_snapshot WHERE monthly_financial_snapshot_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.fact_monthly_financial_snapshot'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'dw_fact_transactions', N'fact_monthly_financial_snapshot', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Monthly snapshot loaded from DW transaction/event facts only.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'dw_fact_transactions', N'fact_monthly_financial_snapshot', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_fact_donation_lifecycle_incremental
--       @start_time DATETIME2(0),
--       @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE @etl_batch_id INT, @rows_read INT = 0, @rows_inserted INT = 0, @rows_updated INT = 0, @rows_rejected INT = 0, @step_started DATETIME2(0);

--     BEGIN TRY
--         EXEC etl_admin.usp_assert_finance_mart2_fact_prerequisites;
--         EXEC etl_admin.usp_dw_start_batch N'DW_FACT', N'FINANCE_MART2', @etl_batch_id OUTPUT;

--         IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--             THROW 71401, 'Invalid period for fact_donation_lifecycle.', 1;

--         SET @step_started = SYSDATETIME();

--         SELECT DISTINCT CONVERT(BIGINT, id) AS source_donation_id
--         INTO #affected_donation
--         FROM Stg_FinanceOps_DB.stg_finance_ops.donations
--         WHERE is_valid = 1
--           AND id IS NOT NULL
--           AND (
--                  (created_at >= @start_time AND created_at < @end_time)
--               OR (donation_date >= CAST(@start_time AS DATE) AND donation_date < CAST(@end_time AS DATE))
--               OR (COALESCE(source_updated_at, updated_at, extracted_at) >= @start_time AND COALESCE(source_updated_at, updated_at, extracted_at) < @end_time)
--           )
--         UNION
--         SELECT DISTINCT CONVERT(BIGINT, source_id)
--         FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
--         WHERE is_valid = 1
--           AND LOWER(ISNULL(source_type,N'')) = N'donation'
--           AND source_id IS NOT NULL
--           AND (
--                  (allocation_date >= CAST(@start_time AS DATE) AND allocation_date < CAST(@end_time AS DATE))
--               OR (COALESCE(source_updated_at, created_at, extracted_at) >= @start_time AND COALESCE(source_updated_at, created_at, extracted_at) < @end_time)
--           );

--         CREATE CLUSTERED INDEX CX_affected_donation ON #affected_donation(source_donation_id);

--         SELECT
--               MIN(allocation_date) AS allocated_date,
--               CONVERT(BIGINT, source_id) AS source_donation_id
--         INTO #alloc_min
--         FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations
--         WHERE is_valid = 1
--           AND LOWER(ISNULL(source_type,N'')) = N'donation'
--           AND source_id IN (SELECT source_donation_id FROM #affected_donation)
--         GROUP BY CONVERT(BIGINT, source_id);
--         CREATE CLUSTERED INDEX CX_alloc_min ON #alloc_min(source_donation_id);

--         SELECT
--               ISNULL(donor.donor_key, -1) AS donor_key,
--               ISNULL(camp.campaign_key, -1) AS campaign_key,
--               ISNULL(created_date.TimeKey, -1) AS created_date_key,
--               CASE WHEN LOWER(ISNULL(d.status,N'')) IN (N'confirmed', N'paid', N'completed', N'success', N'refunded') THEN ISNULL(confirm_date.TimeKey, -1) ELSE -1 END AS confirmed_date_key,
--               ISNULL(alloc_date.TimeKey, -1) AS allocated_date_key,
--               ISNULL(st.status_key, -1) AS lifecycle_status_key,
--               CASE
--                   WHEN LOWER(ISNULL(d.status,N'')) = N'refunded' THEN N'refunded'
--                   WHEN LOWER(ISNULL(d.status,N'')) IN (N'rejected', N'cancelled') THEN N'rejected'
--                   WHEN am.allocated_date IS NOT NULL THEN N'allocated'
--                   WHEN LOWER(ISNULL(d.status,N'')) IN (N'confirmed', N'paid', N'completed', N'success') THEN N'confirmed'
--                   ELSE N'created'
--               END AS current_stage,
--               d.amount AS donation_amount,
--               CASE WHEN LOWER(ISNULL(d.status,N'')) IN (N'confirmed', N'paid', N'completed', N'success', N'refunded') THEN DATEDIFF(DAY, CAST(COALESCE(d.created_at, d.donation_date) AS DATE), d.donation_date) ELSE NULL END AS days_to_confirm,
--               CASE WHEN am.allocated_date IS NOT NULL AND LOWER(ISNULL(d.status,N'')) IN (N'confirmed', N'paid', N'completed', N'success', N'refunded') THEN DATEDIFF(DAY, d.donation_date, am.allocated_date) ELSE NULL END AS days_to_allocate,
--               CONVERT(BIGINT, d.id) AS source_donation_id,
--               CONVERT(BIGINT, d.donor_id) AS source_donor_id,
--               CONVERT(BIGINT, d.campaign_id) AS source_campaign_id,
--               d.source_system
--         INTO #src_lifecycle
--         FROM Stg_FinanceOps_DB.stg_finance_ops.donations d
--         INNER JOIN #affected_donation ad
--                 ON ad.source_donation_id = CONVERT(BIGINT, d.id)
--         LEFT JOIN #alloc_min am
--                ON am.source_donation_id = CONVERT(BIGINT, d.id)
--         LEFT JOIN dw.dim_donor donor
--                ON donor.donor_id = d.donor_id
--               AND donor.donor_key <> -1
--         LEFT JOIN dw.dim_campaign camp
--                ON camp.campaign_id = d.campaign_id
--               AND camp.campaign_key <> -1
--         LEFT JOIN dw.dim_date created_date
--                ON created_date.FullDateAlternateKey = CAST(COALESCE(d.created_at, d.donation_date) AS DATE)
--         LEFT JOIN dw.dim_date confirm_date
--                ON confirm_date.FullDateAlternateKey = CAST(d.donation_date AS DATE)
--         LEFT JOIN dw.dim_date alloc_date
--                ON alloc_date.FullDateAlternateKey = CAST(am.allocated_date AS DATE)
--         LEFT JOIN dw.dim_status st
--                ON st.status_type = N'donation'
--               AND st.code = LOWER(LTRIM(RTRIM(d.status)))
--               AND st.status_key <> -1
--         WHERE d.is_valid = 1
--           AND NOT EXISTS
--               (
--                   SELECT 1
--                   FROM Stg_FinanceOps_DB.stg_finance_ops.donations d2
--                   WHERE d2.id = d.id
--                     AND d2.is_valid = 1
--                     AND d2.stg_row_id > d.stg_row_id
--               );

--         SET @rows_read = @@ROWCOUNT;
--         CREATE CLUSTERED INDEX CX_src_lifecycle ON #src_lifecycle(source_donation_id);

--         BEGIN TRAN;

--         UPDATE f
--            SET f.donor_key            = s.donor_key,
--                f.campaign_key         = s.campaign_key,
--                f.created_date_key     = s.created_date_key,
--                f.confirmed_date_key   = s.confirmed_date_key,
--                f.allocated_date_key   = s.allocated_date_key,
--                f.lifecycle_status_key = s.lifecycle_status_key,
--                f.current_stage        = s.current_stage,
--                f.donation_amount      = s.donation_amount,
--                f.days_to_confirm      = s.days_to_confirm,
--                f.days_to_allocate     = s.days_to_allocate,
--                f.source_donor_id      = s.source_donor_id,
--                f.source_campaign_id   = s.source_campaign_id,
--                f.source_system        = s.source_system,
--                f.etl_batch_id         = @etl_batch_id,
--                f.loaded_at            = SYSDATETIME()
--         FROM dw.fact_donation_lifecycle f
--         INNER JOIN #src_lifecycle s
--                 ON s.source_donation_id = f.source_donation_id
--         WHERE (
--                  ISNULL(f.donor_key, -1) <> ISNULL(s.donor_key, -1)
--               OR ISNULL(f.campaign_key, -1) <> ISNULL(s.campaign_key, -1)
--               OR ISNULL(f.created_date_key, -1) <> ISNULL(s.created_date_key, -1)
--               OR ISNULL(f.confirmed_date_key, -1) <> ISNULL(s.confirmed_date_key, -1)
--               OR ISNULL(f.allocated_date_key, -1) <> ISNULL(s.allocated_date_key, -1)
--               OR ISNULL(f.lifecycle_status_key, -1) <> ISNULL(s.lifecycle_status_key, -1)
--               OR ISNULL(f.current_stage, N'') <> ISNULL(s.current_stage, N'')
--               OR ISNULL(f.donation_amount, 0) <> ISNULL(s.donation_amount, 0)
--               OR ISNULL(f.days_to_confirm, -999999) <> ISNULL(s.days_to_confirm, -999999)
--               OR ISNULL(f.days_to_allocate, -999999) <> ISNULL(s.days_to_allocate, -999999)
--           );
--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO dw.fact_donation_lifecycle
--         (
--               donor_key, campaign_key, created_date_key, confirmed_date_key, allocated_date_key,
--               lifecycle_status_key, current_stage, donation_amount, days_to_confirm, days_to_allocate,
--               source_donation_id, source_donor_id, source_campaign_id, source_system, etl_batch_id, loaded_at
--         )
--         SELECT
--               s.donor_key, s.campaign_key, s.created_date_key, s.confirmed_date_key, s.allocated_date_key,
--               s.lifecycle_status_key, s.current_stage, s.donation_amount, s.days_to_confirm, s.days_to_allocate,
--               s.source_donation_id, s.source_donor_id, s.source_campaign_id, s.source_system, @etl_batch_id, SYSDATETIME()
--         FROM #src_lifecycle s
--         WHERE NOT EXISTS
--               (
--                   SELECT 1
--                   FROM dw.fact_donation_lifecycle f
--                   WHERE f.source_donation_id = s.source_donation_id
--               );
--         SET @rows_inserted = @@ROWCOUNT;

--         DECLARE @max_key BIGINT = ISNULL((SELECT MAX(donation_lifecycle_key) FROM dw.fact_donation_lifecycle WHERE donation_lifecycle_key > 0), 0);
--         DECLARE @checkident_sql NVARCHAR(MAX) = N'DBCC CHECKIDENT (''dw.fact_donation_lifecycle'', RESEED, ' + CONVERT(NVARCHAR(30), @max_key) + N') WITH NO_INFOMSGS';
--         EXEC sys.sp_executesql @checkident_sql;

--         COMMIT TRAN;

--         EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations/budget_allocations', N'fact_donation_lifecycle', N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, N'Set-based accumulating snapshot update for affected donations.';
--         EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'succeeded', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, NULL;
--     END TRY
--     BEGIN CATCH
--         DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();
--         IF XACT_STATE() <> 0 ROLLBACK TRAN;
--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             EXEC etl_admin.usp_dw_log_step @etl_batch_id, N'donations/budget_allocations', N'fact_donation_lifecycle', N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @step_started, @error_message;
--             EXEC etl_admin.usp_dw_finish_batch @etl_batch_id, N'failed', @rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message;
--         END
--         ;THROW;
--     END CATCH
-- END
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_finance_mart2_daily
--       @start_time  DATETIME2(0),
--       @end_time    DATETIME2(0),
--       @run_staging BIT = 0
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     IF @start_time IS NULL OR @end_time IS NULL OR @start_time >= @end_time
--         THROW 72051, 'Invalid period for daily finance mart2 orchestration.', 1;

--     IF @run_staging = 1
--     BEGIN
--         EXEC Stg_FinanceOps_DB.etl_admin.usp_run_stg_finance_ops_all
--              @to_date = @end_time,
--              @etl_batch_id = NULL;
--     END

--     EXEC etl_admin.usp_load_dw_dim_donor_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_dim_campaign_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_dim_category_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_dim_donation_type_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_dim_status_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_dim_currency_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_dim_allocation_type_incremental @start_time, @end_time;

--     EXEC etl_admin.usp_load_dw_fact_donation_transaction_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_fact_expense_transaction_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_fact_payment_transaction_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_fact_budget_allocation_event_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_fact_monthly_financial_snapshot_incremental @start_time, @end_time;
--     EXEC etl_admin.usp_load_dw_fact_donation_lifecycle_incremental @start_time, @end_time;
-- END
-- GO

