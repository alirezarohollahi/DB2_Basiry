/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 2 - Source to Staging ETL
 File         : 10_create_etl_finance_ops_to_staging_procedures.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create ETL stored procedures that extract validated data from:
       Source_FinanceOps_DB.finance_ops

   and load/upsert it into:
       Stg_FinanceOps_DB.stg_finance_ops

 Important change:
   This version uses persistent staging work tables instead of session-local #temp tables.

 Loading strategy:
   1. Small/master tables:
      - TRUNCATE staging table.
      - INSERT validated source snapshot up to @to_date.

      Tables:
      - donors
      - campaigns
      - expense_categories

   2. Large/transactional/growing tables:
      - UPDATE existing staging rows when row_hash changed.
      - INSERT new rows that do not exist in staging.
      - No truncate and no full delete/reload.

      Tables:
      - donations
      - expenses
      - payments
      - budget_allocations
      - financial_transactions
      - currency_rates

 Requirements covered:
   1. Each procedure writes detailed logs to etl_admin.etl_load_log.
   2. Each procedure inserts new rows and updates changed rows correctly.
   3. There is one ETL procedure for each Finance Operations source table.
   4. Each procedure accepts @to_date and loads source data up to that date.
   5. Each procedure validates rows before loading correct rows into staging.
   6. Large tables do not use truncate/reload.
   7. A main procedure runs all staging ETL procedures in safe order.

 Prerequisites:
   1. Source_FinanceOps_DB exists and has data.
   2. Stg_FinanceOps_DB exists.
   3. stg_finance_ops tables exist.
   4. stg_finance_ops ETL work tables exist.
   5. etl_admin.etl_batch and etl_admin.etl_load_log exist.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Stg_FinanceOps_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END
GO

/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_donors
  Loading strategy: TRUNCATE + INSERT
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_donors
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'donors',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donors',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_donors_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_donors_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_donors_valid;

        INSERT INTO stg_finance_ops.etl_tmp_donors_src
            ([id], [full_name], [national_id], [phone], [email], [donor_type], [is_active], [created_at], [updated_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[full_name] AS [full_name],
            src.[national_id] AS [national_id],
            src.[phone] AS [phone],
            src.[email] AS [email],
            src.[donor_type] AS [donor_type],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            CAST(COALESCE(updated_at, created_at) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[full_name]), CONVERT(NVARCHAR(MAX), src.[national_id]), CONVERT(NVARCHAR(MAX), src.[phone]), CONVERT(NVARCHAR(MAX), src.[email]), CONVERT(NVARCHAR(MAX), src.[donor_type]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.donors src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_donors_validated
            ([id], [full_name], [national_id], [phone], [email], [donor_type], [is_active], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[full_name],
            s.[national_id],
            s.[phone],
            s.[email],
            s.[donor_type],
            s.[is_active],
            s.[created_at],
            s.[updated_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN full_name IS NULL THEN N'full_name missing; ' ELSE N'' END, CASE WHEN donor_type IS NULL THEN N'donor_type missing; ' ELSE N'' END, CASE WHEN donor_type IS NOT NULL AND donor_type NOT IN (N'individual', N'organization') THEN N'donor_type invalid: donor_type NOT IN (individual, organization); ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_donors_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_donors_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_donors_valid
            ([id], [full_name], [national_id], [phone], [email], [donor_type], [is_active], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [full_name],
            [national_id],
            [phone],
            [email],
            [donor_type],
            [is_active],
            [created_at],
            [updated_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_donors_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Small table strategy:
          TRUNCATE staging table, then INSERT current validated source snapshot up to @to_date.
          Reason: Small master table. Safe to fully refresh in staging.
        */
        TRUNCATE TABLE stg_finance_ops.donors;

        INSERT INTO stg_finance_ops.donors
            ([id], [full_name], [national_id], [phone], [email], [donor_type], [is_active], [created_at], [updated_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[full_name],
                src.[national_id],
                src.[phone],
                src.[email],
                src.[donor_type],
                src.[is_active],
                src.[created_at],
                src.[updated_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'donors',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_donors_valid src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'TRUNCATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_campaigns
  Loading strategy: TRUNCATE + INSERT
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_campaigns
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'campaigns',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'campaigns',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_campaigns_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_campaigns_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_campaigns_valid;

        INSERT INTO stg_finance_ops.etl_tmp_campaigns_src
            ([id], [title], [description], [target_amount], [start_date], [end_date], [status], [created_at], [updated_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[title] AS [title],
            src.[description] AS [description],
            src.[target_amount] AS [target_amount],
            src.[start_date] AS [start_date],
            src.[end_date] AS [end_date],
            src.[status] AS [status],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            CAST(COALESCE(updated_at, created_at) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[title]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[target_amount]), CONVERT(NVARCHAR(MAX), src.[start_date]), CONVERT(NVARCHAR(MAX), src.[end_date]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.campaigns src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_campaigns_validated
            ([id], [title], [description], [target_amount], [start_date], [end_date], [status], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[title],
            s.[description],
            s.[target_amount],
            s.[start_date],
            s.[end_date],
            s.[status],
            s.[created_at],
            s.[updated_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN title IS NULL THEN N'title missing; ' ELSE N'' END, CASE WHEN target_amount IS NOT NULL AND target_amount < 0 THEN N'target_amount invalid: target_amount < 0; ' ELSE N'' END, CASE WHEN start_date IS NOT NULL AND end_date IS NOT NULL AND start_date > end_date THEN N'start_date invalid: end_date invalid: start_date > end_date; ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_campaigns_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_campaigns_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_campaigns_valid
            ([id], [title], [description], [target_amount], [start_date], [end_date], [status], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [title],
            [description],
            [target_amount],
            [start_date],
            [end_date],
            [status],
            [created_at],
            [updated_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_campaigns_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Small table strategy:
          TRUNCATE staging table, then INSERT current validated source snapshot up to @to_date.
          Reason: Small master table. Safe to fully refresh in staging.
        */
        TRUNCATE TABLE stg_finance_ops.campaigns;

        INSERT INTO stg_finance_ops.campaigns
            ([id], [title], [description], [target_amount], [start_date], [end_date], [status], [created_at], [updated_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[title],
                src.[description],
                src.[target_amount],
                src.[start_date],
                src.[end_date],
                src.[status],
                src.[created_at],
                src.[updated_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'campaigns',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_campaigns_valid src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'TRUNCATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_expense_categories
  Loading strategy: TRUNCATE + INSERT
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_expense_categories
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'expense_categories',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expense_categories',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_expense_categories_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_expense_categories_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_expense_categories_valid;

        INSERT INTO stg_finance_ops.etl_tmp_expense_categories_src
            ([id], [name], [is_active], [created_at], [updated_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[name] AS [name],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            CAST(COALESCE(updated_at, created_at) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[name]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.expense_categories src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_expense_categories_validated
            ([id], [name], [is_active], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[name],
            s.[is_active],
            s.[created_at],
            s.[updated_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN name IS NULL THEN N'name missing; ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_expense_categories_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_expense_categories_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_expense_categories_valid
            ([id], [name], [is_active], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [name],
            [is_active],
            [created_at],
            [updated_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_expense_categories_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Small table strategy:
          TRUNCATE staging table, then INSERT current validated source snapshot up to @to_date.
          Reason: Small lookup hierarchy. Safe to fully refresh in staging.
        */
        TRUNCATE TABLE stg_finance_ops.expense_categories;

        INSERT INTO stg_finance_ops.expense_categories
            ([id], [name], [is_active], [created_at], [updated_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[name],
                src.[is_active],
                src.[created_at],
                src.[updated_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'expense_categories',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_expense_categories_valid src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'TRUNCATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_donations
  Loading strategy: UPDATE existing rows, then INSERT new rows
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_donations
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'donations',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_donations_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_donations_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_donations_valid;

        INSERT INTO stg_finance_ops.etl_tmp_donations_src
            ([id], [donor_id], [campaign_id], [amount], [currency], [donation_type], [donation_date], [status], [reference_code], [created_at], [updated_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[donor_id] AS [donor_id],
            src.[campaign_id] AS [campaign_id],
            src.[amount] AS [amount],
            src.[currency] AS [currency],
            src.[donation_type] AS [donation_type],
            src.[donation_date] AS [donation_date],
            src.[status] AS [status],
            src.[reference_code] AS [reference_code],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            CAST(COALESCE(updated_at, created_at) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[donor_id]), CONVERT(NVARCHAR(MAX), src.[campaign_id]), CONVERT(NVARCHAR(MAX), src.[amount]), CONVERT(NVARCHAR(MAX), src.[currency]), CONVERT(NVARCHAR(MAX), src.[donation_type]), CONVERT(NVARCHAR(MAX), src.[donation_date]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[reference_code]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.donations src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_donations_validated
            ([id], [donor_id], [campaign_id], [amount], [currency], [donation_type], [donation_date], [status], [reference_code], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[donor_id],
            s.[campaign_id],
            s.[amount],
            s.[currency],
            s.[donation_type],
            s.[donation_date],
            s.[status],
            s.[reference_code],
            s.[created_at],
            s.[updated_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN donor_id IS NULL THEN N'donor_id missing; ' ELSE N'' END, CASE WHEN amount IS NULL THEN N'amount missing; ' ELSE N'' END, CASE WHEN amount IS NOT NULL AND amount <= 0 THEN N'amount invalid: amount <= 0; ' ELSE N'' END, CASE WHEN currency IS NULL THEN N'currency missing; ' ELSE N'' END, CASE WHEN donation_type IS NULL THEN N'donation_type missing; ' ELSE N'' END, CASE WHEN donation_date IS NULL THEN N'donation_date missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN donation_type IS NOT NULL AND donation_type NOT IN (N'cash', N'bank_transfer', N'online', N'in_kind') THEN N'donation_type invalid: donation_type NOT IN (cash, bank_transfer, online, in_kind); ' ELSE N'' END, CASE WHEN status IS NOT NULL AND status NOT IN (N'pending', N'confirmed', N'rejected', N'refunded') THEN N'status invalid: status NOT IN (pending, confirmed, rejected, refunded); ' ELSE N'' END, CASE WHEN donor_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.donors p WHERE p.id = s.donor_id) THEN N'donor_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.donors p WHERE p.id = s.donor_id); ' ELSE N'' END, CASE WHEN campaign_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.campaigns p WHERE p.id = s.campaign_id) THEN N'campaign_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.campaigns p WHERE p.id = s.campaign_id); ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_donations_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_donations_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_donations_valid
            ([id], [donor_id], [campaign_id], [amount], [currency], [donation_type], [donation_date], [status], [reference_code], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [donor_id],
            [campaign_id],
            [amount],
            [currency],
            [donation_type],
            [donation_date],
            [status],
            [reference_code],
            [created_at],
            [updated_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_donations_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Large/transactional table strategy:
          UPDATE existing rows first, then INSERT new rows.
          No truncate/reload for large tables.
          Reason: Large/transactional table. Do not truncate.
        */
        UPDATE tgt
        SET
                tgt.[donor_id] = src.[donor_id],
                tgt.[campaign_id] = src.[campaign_id],
                tgt.[amount] = src.[amount],
                tgt.[currency] = src.[currency],
                tgt.[donation_type] = src.[donation_type],
                tgt.[donation_date] = src.[donation_date],
                tgt.[status] = src.[status],
                tgt.[reference_code] = src.[reference_code],
                tgt.[created_at] = src.[created_at],
                tgt.[updated_at] = src.[updated_at],
                tgt.etl_batch_id = @effective_batch_id,
                tgt.source_system = N'FINANCE_OPS',
                tgt.source_database = N'Source_FinanceOps_DB',
                tgt.source_schema = N'finance_ops',
                tgt.source_table = N'donations',
                tgt.extracted_at = @extract_time,
                tgt.source_updated_at = src.source_updated_at,
                tgt.row_hash = src.row_hash,
                tgt.is_valid = 1,
                tgt.validation_message = NULL
        FROM stg_finance_ops.donations tgt
        INNER JOIN stg_finance_ops.etl_tmp_donations_valid src
            ON src.[id] = tgt.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.donations
            ([id], [donor_id], [campaign_id], [amount], [currency], [donation_type], [donation_date], [status], [reference_code], [created_at], [updated_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[donor_id],
                src.[campaign_id],
                src.[amount],
                src.[currency],
                src.[donation_type],
                src.[donation_date],
                src.[status],
                src.[reference_code],
                src.[created_at],
                src.[updated_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'donations',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_donations_valid src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_finance_ops.donations tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'UPDATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_expenses
  Loading strategy: UPDATE existing rows, then INSERT new rows
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_expenses
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'expenses',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expenses',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_expenses_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_expenses_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_expenses_valid;

        INSERT INTO stg_finance_ops.etl_tmp_expenses_src
            ([id], [center_id], [child_id], [category_id], [amount], [currency], [expense_date], [description], [approved_by_user_id], [status], [created_at], [updated_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[center_id] AS [center_id],
            src.[child_id] AS [child_id],
            src.[category_id] AS [category_id],
            src.[amount] AS [amount],
            src.[currency] AS [currency],
            src.[expense_date] AS [expense_date],
            src.[description] AS [description],
            src.[approved_by_user_id] AS [approved_by_user_id],
            src.[status] AS [status],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            CAST(COALESCE(updated_at, created_at) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[category_id]), CONVERT(NVARCHAR(MAX), src.[amount]), CONVERT(NVARCHAR(MAX), src.[currency]), CONVERT(NVARCHAR(MAX), src.[expense_date]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[approved_by_user_id]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.expenses src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_expenses_validated
            ([id], [center_id], [child_id], [category_id], [amount], [currency], [expense_date], [description], [approved_by_user_id], [status], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[center_id],
            s.[child_id],
            s.[category_id],
            s.[amount],
            s.[currency],
            s.[expense_date],
            s.[description],
            s.[approved_by_user_id],
            s.[status],
            s.[created_at],
            s.[updated_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN category_id IS NULL THEN N'category_id missing; ' ELSE N'' END, CASE WHEN amount IS NULL THEN N'amount missing; ' ELSE N'' END, CASE WHEN amount IS NOT NULL AND amount <= 0 THEN N'amount invalid: amount <= 0; ' ELSE N'' END, CASE WHEN currency IS NULL THEN N'currency missing; ' ELSE N'' END, CASE WHEN expense_date IS NULL THEN N'expense_date missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN status IS NOT NULL AND status NOT IN (N'pending', N'approved', N'rejected') THEN N'status invalid: status NOT IN (pending, approved, rejected); ' ELSE N'' END, CASE WHEN category_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.expense_categories p WHERE p.id = s.category_id) THEN N'category_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.expense_categories p WHERE p.id = s.category_id); ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_expenses_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_expenses_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_expenses_valid
            ([id], [center_id], [child_id], [category_id], [amount], [currency], [expense_date], [description], [approved_by_user_id], [status], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [center_id],
            [child_id],
            [category_id],
            [amount],
            [currency],
            [expense_date],
            [description],
            [approved_by_user_id],
            [status],
            [created_at],
            [updated_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_expenses_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Large/transactional table strategy:
          UPDATE existing rows first, then INSERT new rows.
          No truncate/reload for large tables.
          Reason: Large/transactional table. Do not truncate.
        */
        UPDATE tgt
        SET
                tgt.[center_id] = src.[center_id],
                tgt.[child_id] = src.[child_id],
                tgt.[category_id] = src.[category_id],
                tgt.[amount] = src.[amount],
                tgt.[currency] = src.[currency],
                tgt.[expense_date] = src.[expense_date],
                tgt.[description] = src.[description],
                tgt.[approved_by_user_id] = src.[approved_by_user_id],
                tgt.[status] = src.[status],
                tgt.[created_at] = src.[created_at],
                tgt.[updated_at] = src.[updated_at],
                tgt.etl_batch_id = @effective_batch_id,
                tgt.source_system = N'FINANCE_OPS',
                tgt.source_database = N'Source_FinanceOps_DB',
                tgt.source_schema = N'finance_ops',
                tgt.source_table = N'expenses',
                tgt.extracted_at = @extract_time,
                tgt.source_updated_at = src.source_updated_at,
                tgt.row_hash = src.row_hash,
                tgt.is_valid = 1,
                tgt.validation_message = NULL
        FROM stg_finance_ops.expenses tgt
        INNER JOIN stg_finance_ops.etl_tmp_expenses_valid src
            ON src.[id] = tgt.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.expenses
            ([id], [center_id], [child_id], [category_id], [amount], [currency], [expense_date], [description], [approved_by_user_id], [status], [created_at], [updated_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[center_id],
                src.[child_id],
                src.[category_id],
                src.[amount],
                src.[currency],
                src.[expense_date],
                src.[description],
                src.[approved_by_user_id],
                src.[status],
                src.[created_at],
                src.[updated_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'expenses',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_expenses_valid src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_finance_ops.expenses tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'UPDATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_payments
  Loading strategy: UPDATE existing rows, then INSERT new rows
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_payments
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'payments',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'payments',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_payments_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_payments_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_payments_valid;

        INSERT INTO stg_finance_ops.etl_tmp_payments_src
            ([id], [payment_type], [teacher_id], [center_id], [amount], [currency], [payment_date], [status], [created_at], [updated_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[payment_type] AS [payment_type],
            src.[teacher_id] AS [teacher_id],
            src.[center_id] AS [center_id],
            src.[amount] AS [amount],
            src.[currency] AS [currency],
            src.[payment_date] AS [payment_date],
            src.[status] AS [status],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            CAST(COALESCE(updated_at, created_at) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[payment_type]), CONVERT(NVARCHAR(MAX), src.[teacher_id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[amount]), CONVERT(NVARCHAR(MAX), src.[currency]), CONVERT(NVARCHAR(MAX), src.[payment_date]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.payments src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_payments_validated
            ([id], [payment_type], [teacher_id], [center_id], [amount], [currency], [payment_date], [status], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[payment_type],
            s.[teacher_id],
            s.[center_id],
            s.[amount],
            s.[currency],
            s.[payment_date],
            s.[status],
            s.[created_at],
            s.[updated_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN payment_type IS NULL THEN N'payment_type missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN amount IS NULL THEN N'amount missing; ' ELSE N'' END, CASE WHEN amount IS NOT NULL AND amount <= 0 THEN N'amount invalid: amount <= 0; ' ELSE N'' END, CASE WHEN currency IS NULL THEN N'currency missing; ' ELSE N'' END, CASE WHEN payment_date IS NULL THEN N'payment_date missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN payment_type IS NOT NULL AND payment_type NOT IN (N'salary', N'bonus', N'vendor', N'refund') THEN N'payment_type invalid: payment_type NOT IN (salary, bonus, vendor, refund); ' ELSE N'' END, CASE WHEN status IS NOT NULL AND status NOT IN (N'pending', N'approved', N'paid', N'cancelled', N'rejected') THEN N'status invalid: status NOT IN (pending, approved, paid, cancelled, rejected); ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_payments_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_payments_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_payments_valid
            ([id], [payment_type], [teacher_id], [center_id], [amount], [currency], [payment_date], [status], [created_at], [updated_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [payment_type],
            [teacher_id],
            [center_id],
            [amount],
            [currency],
            [payment_date],
            [status],
            [created_at],
            [updated_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_payments_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Large/transactional table strategy:
          UPDATE existing rows first, then INSERT new rows.
          No truncate/reload for large tables.
          Reason: Transactional table. Do not truncate.
        */
        UPDATE tgt
        SET
                tgt.[payment_type] = src.[payment_type],
                tgt.[teacher_id] = src.[teacher_id],
                tgt.[center_id] = src.[center_id],
                tgt.[amount] = src.[amount],
                tgt.[currency] = src.[currency],
                tgt.[payment_date] = src.[payment_date],
                tgt.[status] = src.[status],
                tgt.[created_at] = src.[created_at],
                tgt.[updated_at] = src.[updated_at],
                tgt.etl_batch_id = @effective_batch_id,
                tgt.source_system = N'FINANCE_OPS',
                tgt.source_database = N'Source_FinanceOps_DB',
                tgt.source_schema = N'finance_ops',
                tgt.source_table = N'payments',
                tgt.extracted_at = @extract_time,
                tgt.source_updated_at = src.source_updated_at,
                tgt.row_hash = src.row_hash,
                tgt.is_valid = 1,
                tgt.validation_message = NULL
        FROM stg_finance_ops.payments tgt
        INNER JOIN stg_finance_ops.etl_tmp_payments_valid src
            ON src.[id] = tgt.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.payments
            ([id], [payment_type], [teacher_id], [center_id], [amount], [currency], [payment_date], [status], [created_at], [updated_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[payment_type],
                src.[teacher_id],
                src.[center_id],
                src.[amount],
                src.[currency],
                src.[payment_date],
                src.[status],
                src.[created_at],
                src.[updated_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'payments',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_payments_valid src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_finance_ops.payments tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'UPDATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_budget_allocations
  Loading strategy: UPDATE existing rows, then INSERT new rows
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_budget_allocations
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'budget_allocations',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_budget_allocations_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_budget_allocations_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_budget_allocations_valid;

        INSERT INTO stg_finance_ops.etl_tmp_budget_allocations_src
            ([id], [source_type], [source_id], [center_id], [child_id], [category_id], [allocated_amount], [allocation_date], [reason], [created_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[source_type] AS [source_type],
            src.[source_id] AS [source_id],
            src.[center_id] AS [center_id],
            src.[child_id] AS [child_id],
            src.[category_id] AS [category_id],
            src.[allocated_amount] AS [allocated_amount],
            src.[allocation_date] AS [allocation_date],
            src.[reason] AS [reason],
            src.[created_at] AS [created_at],
            CAST(CAST(created_at AS DATETIME2(0)) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[source_type]), CONVERT(NVARCHAR(MAX), src.[source_id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[category_id]), CONVERT(NVARCHAR(MAX), src.[allocated_amount]), CONVERT(NVARCHAR(MAX), src.[allocation_date]), CONVERT(NVARCHAR(MAX), src.[reason]), CONVERT(NVARCHAR(MAX), src.[created_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.budget_allocations src
        WHERE CAST(created_at AS DATETIME2(0)) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_budget_allocations_validated
            ([id], [source_type], [source_id], [center_id], [child_id], [category_id], [allocated_amount], [allocation_date], [reason], [created_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[source_type],
            s.[source_id],
            s.[center_id],
            s.[child_id],
            s.[category_id],
            s.[allocated_amount],
            s.[allocation_date],
            s.[reason],
            s.[created_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN source_type IS NULL THEN N'source_type missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN allocated_amount IS NULL THEN N'allocated_amount missing; ' ELSE N'' END, CASE WHEN allocated_amount IS NOT NULL AND allocated_amount <= 0 THEN N'allocated_amount invalid: allocated_amount <= 0; ' ELSE N'' END, CASE WHEN allocation_date IS NULL THEN N'allocation_date missing; ' ELSE N'' END, CASE WHEN source_type IS NOT NULL AND source_type NOT IN (N'donation', N'internal_budget') THEN N'source_type invalid: source_type NOT IN (donation, internal_budget); ' ELSE N'' END, CASE WHEN source_type = N'donation' AND source_id IS NULL THEN N'source_type = donation AND source_id missing; ' ELSE N'' END, CASE WHEN source_type = N'donation' AND source_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.donations p WHERE p.id = s.source_id) THEN N'source_type = donation AND source_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.donations p WHERE p.id = s.source_id); ' ELSE N'' END, CASE WHEN category_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.expense_categories p WHERE p.id = s.category_id) THEN N'category_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.expense_categories p WHERE p.id = s.category_id); ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_budget_allocations_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_budget_allocations_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_budget_allocations_valid
            ([id], [source_type], [source_id], [center_id], [child_id], [category_id], [allocated_amount], [allocation_date], [reason], [created_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [source_type],
            [source_id],
            [center_id],
            [child_id],
            [category_id],
            [allocated_amount],
            [allocation_date],
            [reason],
            [created_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_budget_allocations_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Large/transactional table strategy:
          UPDATE existing rows first, then INSERT new rows.
          No truncate/reload for large tables.
          Reason: Event table. Do not truncate.
        */
        UPDATE tgt
        SET
                tgt.[source_type] = src.[source_type],
                tgt.[source_id] = src.[source_id],
                tgt.[center_id] = src.[center_id],
                tgt.[child_id] = src.[child_id],
                tgt.[category_id] = src.[category_id],
                tgt.[allocated_amount] = src.[allocated_amount],
                tgt.[allocation_date] = src.[allocation_date],
                tgt.[reason] = src.[reason],
                tgt.[created_at] = src.[created_at],
                tgt.etl_batch_id = @effective_batch_id,
                tgt.source_system = N'FINANCE_OPS',
                tgt.source_database = N'Source_FinanceOps_DB',
                tgt.source_schema = N'finance_ops',
                tgt.source_table = N'budget_allocations',
                tgt.extracted_at = @extract_time,
                tgt.source_updated_at = src.source_updated_at,
                tgt.row_hash = src.row_hash,
                tgt.is_valid = 1,
                tgt.validation_message = NULL
        FROM stg_finance_ops.budget_allocations tgt
        INNER JOIN stg_finance_ops.etl_tmp_budget_allocations_valid src
            ON src.[id] = tgt.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.budget_allocations
            ([id], [source_type], [source_id], [center_id], [child_id], [category_id], [allocated_amount], [allocation_date], [reason], [created_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[source_type],
                src.[source_id],
                src.[center_id],
                src.[child_id],
                src.[category_id],
                src.[allocated_amount],
                src.[allocation_date],
                src.[reason],
                src.[created_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'budget_allocations',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_budget_allocations_valid src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_finance_ops.budget_allocations tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'UPDATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_financial_transactions
  Loading strategy: UPDATE existing rows, then INSERT new rows
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_financial_transactions
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'financial_transactions',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'financial_transactions',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_financial_transactions_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_financial_transactions_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_financial_transactions_valid;

        INSERT INTO stg_finance_ops.etl_tmp_financial_transactions_src
            ([id], [entity_type], [entity_id], [transaction_type], [amount], [transaction_date], [created_at], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[entity_type] AS [entity_type],
            src.[entity_id] AS [entity_id],
            src.[transaction_type] AS [transaction_type],
            src.[amount] AS [amount],
            src.[transaction_date] AS [transaction_date],
            src.[created_at] AS [created_at],
            CAST(CAST(created_at AS DATETIME2(0)) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[entity_type]), CONVERT(NVARCHAR(MAX), src.[entity_id]), CONVERT(NVARCHAR(MAX), src.[transaction_type]), CONVERT(NVARCHAR(MAX), src.[amount]), CONVERT(NVARCHAR(MAX), src.[transaction_date]), CONVERT(NVARCHAR(MAX), src.[created_at]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.financial_transactions src
        WHERE CAST(created_at AS DATETIME2(0)) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_financial_transactions_validated
            ([id], [entity_type], [entity_id], [transaction_type], [amount], [transaction_date], [created_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[entity_type],
            s.[entity_id],
            s.[transaction_type],
            s.[amount],
            s.[transaction_date],
            s.[created_at],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN entity_type IS NULL THEN N'entity_type missing; ' ELSE N'' END, CASE WHEN entity_id IS NULL THEN N'entity_id missing; ' ELSE N'' END, CASE WHEN transaction_type IS NULL THEN N'transaction_type missing; ' ELSE N'' END, CASE WHEN amount IS NULL THEN N'amount missing; ' ELSE N'' END, CASE WHEN amount IS NOT NULL AND amount <= 0 THEN N'amount invalid: amount <= 0; ' ELSE N'' END, CASE WHEN transaction_date IS NULL THEN N'transaction_date missing; ' ELSE N'' END, CASE WHEN entity_type IS NOT NULL AND entity_type NOT IN (N'donation', N'expense', N'payment') THEN N'entity_type invalid: entity_type NOT IN (donation, expense, payment); ' ELSE N'' END, CASE WHEN transaction_type IS NOT NULL AND transaction_type NOT IN (N'credit', N'debit') THEN N'transaction_type invalid: transaction_type NOT IN (credit, debit); ' ELSE N'' END, CASE WHEN entity_type = N'donation' AND entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.donations p WHERE p.id = s.entity_id) THEN N'entity_type = donation AND entity_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.donations p WHERE p.id = s.entity_id); ' ELSE N'' END, CASE WHEN entity_type = N'expense' AND entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.expenses p WHERE p.id = s.entity_id) THEN N'entity_type = expense AND entity_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.expenses p WHERE p.id = s.entity_id); ' ELSE N'' END, CASE WHEN entity_type = N'payment' AND entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.payments p WHERE p.id = s.entity_id) THEN N'entity_type = payment AND entity_id invalid reference (SELECT 1 FROM Source_FinanceOps_DB.finance_ops.payments p WHERE p.id = s.entity_id); ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_financial_transactions_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_financial_transactions_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_financial_transactions_valid
            ([id], [entity_type], [entity_id], [transaction_type], [amount], [transaction_date], [created_at], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [entity_type],
            [entity_id],
            [transaction_type],
            [amount],
            [transaction_date],
            [created_at],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_financial_transactions_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Large/transactional table strategy:
          UPDATE existing rows first, then INSERT new rows.
          No truncate/reload for large tables.
          Reason: Audit/transaction table. Do not truncate.
        */
        UPDATE tgt
        SET
                tgt.[entity_type] = src.[entity_type],
                tgt.[entity_id] = src.[entity_id],
                tgt.[transaction_type] = src.[transaction_type],
                tgt.[amount] = src.[amount],
                tgt.[transaction_date] = src.[transaction_date],
                tgt.[created_at] = src.[created_at],
                tgt.etl_batch_id = @effective_batch_id,
                tgt.source_system = N'FINANCE_OPS',
                tgt.source_database = N'Source_FinanceOps_DB',
                tgt.source_schema = N'finance_ops',
                tgt.source_table = N'financial_transactions',
                tgt.extracted_at = @extract_time,
                tgt.source_updated_at = src.source_updated_at,
                tgt.row_hash = src.row_hash,
                tgt.is_valid = 1,
                tgt.validation_message = NULL
        FROM stg_finance_ops.financial_transactions tgt
        INNER JOIN stg_finance_ops.etl_tmp_financial_transactions_valid src
            ON src.[id] = tgt.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.financial_transactions
            ([id], [entity_type], [entity_id], [transaction_type], [amount], [transaction_date], [created_at], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[entity_type],
                src.[entity_id],
                src.[transaction_type],
                src.[amount],
                src.[transaction_date],
                src.[created_at],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'financial_transactions',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_financial_transactions_valid src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_finance_ops.financial_transactions tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'UPDATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO


/*=============================================================================
  Procedure: etl_admin.usp_load_stg_finance_ops_currency_rates
  Loading strategy: UPDATE existing rows, then INSERT new rows
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_finance_ops_currency_rates
    @to_date DATETIME2(0),
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @effective_batch_id INT,
        @created_own_batch BIT = 0,
        @load_log_id BIGINT,
        @extract_time DATETIME2(0) = SYSDATETIME(),
        @rows_read INT = 0,
        @rows_valid INT = 0,
        @rows_rejected INT = 0,
        @rows_inserted INT = 0,
        @rows_updated INT = 0,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        IF @etl_batch_id IS NULL
        BEGIN
            INSERT INTO etl_admin.etl_batch
                (source_system, target_layer, batch_status, started_at)
            VALUES
                (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

            SET @effective_batch_id = SCOPE_IDENTITY();
            SET @created_own_batch = 1;
        END
        ELSE
        BEGIN
            SET @effective_batch_id = @etl_batch_id;
        END;

        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status, started_at, message)
        VALUES
            (@effective_batch_id, N'Source_FinanceOps_DB', N'finance_ops', N'currency_rates',
             N'Stg_FinanceOps_DB', N'stg_finance_ops', N'currency_rates',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        TRUNCATE TABLE stg_finance_ops.etl_tmp_currency_rates_src;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_currency_rates_validated;
        TRUNCATE TABLE stg_finance_ops.etl_tmp_currency_rates_valid;

        INSERT INTO stg_finance_ops.etl_tmp_currency_rates_src
            ([id], [from_currency], [to_currency], [rate], [rate_date], [source_updated_at], [row_hash])
        SELECT
            src.[id] AS [id],
            src.[from_currency] AS [from_currency],
            src.[to_currency] AS [to_currency],
            src.[rate] AS [rate],
            src.[rate_date] AS [rate_date],
            CAST(CAST(rate_date AS DATETIME2(0)) AS DATETIME2(0)) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[from_currency]), CONVERT(NVARCHAR(MAX), src.[to_currency]), CONVERT(NVARCHAR(MAX), src.[rate]), CONVERT(NVARCHAR(MAX), src.[rate_date]))) AS row_hash
        FROM Source_FinanceOps_DB.finance_ops.currency_rates src
        WHERE CAST(rate_date AS DATETIME2(0)) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.etl_tmp_currency_rates_validated
            ([id], [from_currency], [to_currency], [rate], [rate_date], [source_updated_at], [row_hash], [validation_message])
        SELECT
            s.[id],
            s.[from_currency],
            s.[to_currency],
            s.[rate],
            s.[rate_date],
            s.[source_updated_at],
            s.[row_hash],
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN from_currency IS NULL THEN N'from_currency missing; ' ELSE N'' END, CASE WHEN to_currency IS NULL THEN N'to_currency missing; ' ELSE N'' END, CASE WHEN rate IS NULL THEN N'rate missing; ' ELSE N'' END, CASE WHEN rate IS NOT NULL AND rate <= 0 THEN N'rate invalid: rate <= 0; ' ELSE N'' END, CASE WHEN rate_date IS NULL THEN N'rate_date missing; ' ELSE N'' END), N'') AS validation_message
        FROM stg_finance_ops.etl_tmp_currency_rates_src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM stg_finance_ops.etl_tmp_currency_rates_validated
            WHERE validation_message IS NOT NULL
        );

        INSERT INTO stg_finance_ops.etl_tmp_currency_rates_valid
            ([id], [from_currency], [to_currency], [rate], [rate_date], [source_updated_at], [row_hash], [validation_message])
        SELECT
            [id],
            [from_currency],
            [to_currency],
            [rate],
            [rate_date],
            [source_updated_at],
            [row_hash],
            [validation_message]
        FROM stg_finance_ops.etl_tmp_currency_rates_validated
        WHERE validation_message IS NULL;

        SET @rows_valid = @@ROWCOUNT;


        /*
          Large/transactional table strategy:
          UPDATE existing rows first, then INSERT new rows.
          No truncate/reload for large tables.
          Reason: Can grow over time, so use update + insert.
        */
        UPDATE tgt
        SET
                tgt.[from_currency] = src.[from_currency],
                tgt.[to_currency] = src.[to_currency],
                tgt.[rate] = src.[rate],
                tgt.[rate_date] = src.[rate_date],
                tgt.etl_batch_id = @effective_batch_id,
                tgt.source_system = N'FINANCE_OPS',
                tgt.source_database = N'Source_FinanceOps_DB',
                tgt.source_schema = N'finance_ops',
                tgt.source_table = N'currency_rates',
                tgt.extracted_at = @extract_time,
                tgt.source_updated_at = src.source_updated_at,
                tgt.row_hash = src.row_hash,
                tgt.is_valid = 1,
                tgt.validation_message = NULL
        FROM stg_finance_ops.currency_rates tgt
        INNER JOIN stg_finance_ops.etl_tmp_currency_rates_valid src
            ON src.[id] = tgt.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_finance_ops.currency_rates
            ([id], [from_currency], [to_currency], [rate], [rate_date], [etl_batch_id], [source_system], [source_database], [source_schema], [source_table], [extracted_at], [source_updated_at], [row_hash], [is_valid], [validation_message])
        SELECT
                src.[id],
                src.[from_currency],
                src.[to_currency],
                src.[rate],
                src.[rate_date],
                @effective_batch_id,
                N'FINANCE_OPS',
                N'Source_FinanceOps_DB',
                N'finance_ops',
                N'currency_rates',
                @extract_time,
                src.source_updated_at,
                src.row_hash,
                1,
                NULL
        FROM stg_finance_ops.etl_tmp_currency_rates_valid src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_finance_ops.currency_rates tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;


        COMMIT TRANSACTION;

        UPDATE etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_read,
            rows_written = @rows_inserted + @rows_updated,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(
                N'Succeeded. Valid rows: ', @rows_valid,
                N'; inserted: ', @rows_inserted,
                N'; updated: ', @rows_updated,
                N'; rejected: ', @rows_rejected,
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126),
                N'; strategy: ', N'UPDATE_INSERT'
            )
        WHERE etl_load_log_id = @load_log_id;

        IF @created_own_batch = 1
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'succeeded',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = @rows_inserted + @rows_updated,
                rows_rejected = @rows_rejected
            WHERE etl_batch_id = @effective_batch_id;
        END;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @load_log_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_load_log
            SET
                load_status = N'failed',
                rows_read = @rows_read,
                rows_written = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                ended_at = SYSDATETIME(),
                message = @error_message
            WHERE etl_load_log_id = @load_log_id;
        END;

        IF @created_own_batch = 1 AND @effective_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                rows_extracted = @rows_read,
                rows_inserted = ISNULL(@rows_inserted, 0) + ISNULL(@rows_updated, 0),
                rows_rejected = @rows_rejected,
                error_message = @error_message
            WHERE etl_batch_id = @effective_batch_id;
        END;

        THROW;
    END CATCH;
END
GO

/*=============================================================================
  Main Orchestration Procedure
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_run_stg_finance_ops_all
    @to_date DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id INT,
        @error_message NVARCHAR(MAX);

    IF @to_date IS NULL
    BEGIN
        RAISERROR('@to_date is required.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at)
        VALUES
            (N'FINANCE_OPS', N'STAGING', N'running', SYSDATETIME());

        SET @etl_batch_id = SCOPE_IDENTITY();

        /*
          Safe load order:
          small/reference tables first, transactional/dependent tables later.
        */

        EXEC etl_admin.usp_load_stg_finance_ops_donors @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_campaigns @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_expense_categories @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_donations @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_expenses @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_payments @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_budget_allocations @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_financial_transactions @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_finance_ops_currency_rates @to_date = @to_date, @etl_batch_id = @etl_batch_id;

        UPDATE etl_admin.etl_batch
        SET
            batch_status = N'succeeded',
            ended_at = SYSDATETIME(),
            rows_extracted = (
                SELECT SUM(ISNULL(rows_read, 0))
                FROM etl_admin.etl_load_log
                WHERE etl_batch_id = @etl_batch_id
            ),
            rows_inserted = (
                SELECT SUM(ISNULL(rows_written, 0))
                FROM etl_admin.etl_load_log
                WHERE etl_batch_id = @etl_batch_id
            ),
            rows_rejected = (
                SELECT SUM(ISNULL(rows_rejected, 0))
                FROM etl_admin.etl_load_log
                WHERE etl_batch_id = @etl_batch_id
            )
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE etl_admin.etl_batch
            SET
                batch_status = N'failed',
                ended_at = SYSDATETIME(),
                error_message = @error_message,
                rows_extracted = (
                    SELECT SUM(ISNULL(rows_read, 0))
                    FROM etl_admin.etl_load_log
                    WHERE etl_batch_id = @etl_batch_id
                ),
                rows_inserted = (
                    SELECT SUM(ISNULL(rows_written, 0))
                    FROM etl_admin.etl_load_log
                    WHERE etl_batch_id = @etl_batch_id
                ),
                rows_rejected = (
                    SELECT SUM(ISNULL(rows_rejected, 0))
                    FROM etl_admin.etl_load_log
                    WHERE etl_batch_id = @etl_batch_id
                )
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END
GO

/*=============================================================================
  Example job command

  EXEC etl_admin.usp_run_stg_finance_ops_all
      @to_date = '2025-12-31 23:59:59';
=============================================================================*/

PRINT 'Finance Ops source-to-staging ETL procedures created successfully.';
PRINT 'Main procedure: etl_admin.usp_run_stg_finance_ops_all';
PRINT 'UPDATE/INSERT is not used in this script.';
GO
