/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 2 - Source to Staging ETL
 File         : 09_create_etl_program_ops_to_staging_procedures.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create ETL stored procedures that extract validated data from:
       Source_ProgramOps_DB.program_ops

   and upsert it into:
       Stg_ProgramOps_DB.stg_program_ops

 Requirements covered:
   1. Each procedure writes detailed logs to etl_admin.etl_load_log.
   2. Small lookup procedures reload with TRUNCATE + INSERT; large/growing procedures use UPDATE existing rows + INSERT new rows. The load logic avoids native upsert syntax.
   4. There is one ETL procedure for each Program Operations source table.
   5. Each procedure accepts @to_date and loads source data up to that date.
   6. Each procedure validates rows before loading correct rows into staging.
   7. A main procedure runs all staging ETL procedures in a safe order.

 Important:
   - Staging is a landing area, but these ETL procedures only move rows that
     pass validation into staging.
   - Rejected row counts are logged in etl_admin.etl_load_log.
   - For detailed rejected-row storage, a future enhancement can add a
     stg_reject table. For now, rejects are counted and the validation logic is
     embedded in each procedure.
   - @to_date is inclusive. Rows with source timestamp <= @to_date are eligible.
   - For most source tables, source timestamp = COALESCE(updated_at, created_at).
   - For audit_logs and note_batches, source timestamp = created_at.
   - For note_batch_items, eligibility is based on related note_batch/note dates.

 Prerequisites:
   1. Source_ProgramOps_DB exists and has data.
   2. Stg_ProgramOps_DB exists.
   3. stg_program_ops tables exist.
   4. etl_admin.etl_batch and etl_admin.etl_load_log exist.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Stg_ProgramOps_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END
GO

/*=============================================================================
  Procedure: etl_admin.usp_load_stg_program_ops_centers
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.centers
             into Stg_ProgramOps_DB.stg_program_ops.centers
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_centers
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'centers',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[name] AS [name],
            src.[city] AS [city],
            src.[address] AS [address],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[name]), CONVERT(NVARCHAR(MAX), src.[city]), CONVERT(NVARCHAR(MAX), src.[address]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.centers src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN name IS NULL THEN N'name missing; ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Small lookup table: full refresh with TRUNCATE + INSERT.
        TRUNCATE TABLE stg_program_ops.centers;

        INSERT INTO stg_program_ops.centers
            (
                [id],
                [name],
                [city],
                [address],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[name],
            src.[city],
            src.[address],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'centers',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_children
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.children
             into Stg_ProgramOps_DB.stg_program_ops.children
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_children
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'children',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'children',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[center_id] AS [center_id],
            src.[first_name] AS [first_name],
            src.[last_name] AS [last_name],
            src.[national_code] AS [national_code],
            src.[birth_date] AS [birth_date],
            src.[gender] AS [gender],
            src.[enrollment_date] AS [enrollment_date],
            src.[status] AS [status],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[first_name]), CONVERT(NVARCHAR(MAX), src.[last_name]), CONVERT(NVARCHAR(MAX), src.[national_code]), CONVERT(NVARCHAR(MAX), src.[birth_date]), CONVERT(NVARCHAR(MAX), src.[gender]), CONVERT(NVARCHAR(MAX), src.[enrollment_date]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.children src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN first_name IS NULL THEN N'first_name missing; ' ELSE N'' END, CASE WHEN last_name IS NULL THEN N'last_name missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN center_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id) THEN N'center_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[center_id] = src.[center_id],
            tgt.[first_name] = src.[first_name],
            tgt.[last_name] = src.[last_name],
            tgt.[national_code] = src.[national_code],
            tgt.[birth_date] = src.[birth_date],
            tgt.[gender] = src.[gender],
            tgt.[enrollment_date] = src.[enrollment_date],
            tgt.[status] = src.[status],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'children',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.children AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.children
            (
                [id],
                [center_id],
                [first_name],
                [last_name],
                [national_code],
                [birth_date],
                [gender],
                [enrollment_date],
                [status],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[center_id],
            src.[first_name],
            src.[last_name],
            src.[national_code],
            src.[birth_date],
            src.[gender],
            src.[enrollment_date],
            src.[status],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'children',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.children AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_teachers
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.teachers
             into Stg_ProgramOps_DB.stg_program_ops.teachers
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_teachers
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'teachers',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[center_id] AS [center_id],
            src.[first_name] AS [first_name],
            src.[last_name] AS [last_name],
            src.[phone] AS [phone],
            src.[email] AS [email],
            src.[employment_status] AS [employment_status],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[first_name]), CONVERT(NVARCHAR(MAX), src.[last_name]), CONVERT(NVARCHAR(MAX), src.[phone]), CONVERT(NVARCHAR(MAX), src.[email]), CONVERT(NVARCHAR(MAX), src.[employment_status]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.teachers src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN first_name IS NULL THEN N'first_name missing; ' ELSE N'' END, CASE WHEN last_name IS NULL THEN N'last_name missing; ' ELSE N'' END, CASE WHEN employment_status IS NULL THEN N'employment_status missing; ' ELSE N'' END, CASE WHEN center_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id) THEN N'center_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[center_id] = src.[center_id],
            tgt.[first_name] = src.[first_name],
            tgt.[last_name] = src.[last_name],
            tgt.[phone] = src.[phone],
            tgt.[email] = src.[email],
            tgt.[employment_status] = src.[employment_status],
            tgt.[is_active] = src.[is_active],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'teachers',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.teachers AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.teachers
            (
                [id],
                [center_id],
                [first_name],
                [last_name],
                [phone],
                [email],
                [employment_status],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[center_id],
            src.[first_name],
            src.[last_name],
            src.[phone],
            src.[email],
            src.[employment_status],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'teachers',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.teachers AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_users
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.users
             into Stg_ProgramOps_DB.stg_program_ops.users
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_users
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'users',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'users',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[username] AS [username],
            src.[password_hash] AS [password_hash],
            src.[role] AS [role],
            src.[teacher_id] AS [teacher_id],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[username]), CONVERT(NVARCHAR(MAX), src.[password_hash]), CONVERT(NVARCHAR(MAX), src.[role]), CONVERT(NVARCHAR(MAX), src.[teacher_id]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.users src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN username IS NULL THEN N'username missing; ' ELSE N'' END, CASE WHEN password_hash IS NULL THEN N'password_hash missing; ' ELSE N'' END, CASE WHEN role IS NULL THEN N'role missing; ' ELSE N'' END, CASE WHEN teacher_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id) THEN N'teacher_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[username] = src.[username],
            tgt.[password_hash] = src.[password_hash],
            tgt.[role] = src.[role],
            tgt.[teacher_id] = src.[teacher_id],
            tgt.[is_active] = src.[is_active],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'users',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.users AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.users
            (
                [id],
                [username],
                [password_hash],
                [role],
                [teacher_id],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[username],
            src.[password_hash],
            src.[role],
            src.[teacher_id],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'users',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.users AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_domains
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.domains
             into Stg_ProgramOps_DB.stg_program_ops.domains
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_domains
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'domains',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[name] AS [name],
            src.[description] AS [description],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[name]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.domains src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN name IS NULL THEN N'name missing; ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Small lookup table: full refresh with TRUNCATE + INSERT.
        TRUNCATE TABLE stg_program_ops.domains;

        INSERT INTO stg_program_ops.domains
            (
                [id],
                [name],
                [description],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[name],
            src.[description],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'domains',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_score_scales
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.score_scales
             into Stg_ProgramOps_DB.stg_program_ops.score_scales
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_score_scales
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'score_scales',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[name] AS [name],
            src.[min_score] AS [min_score],
            src.[max_score] AS [max_score],
            src.[description] AS [description],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[name]), CONVERT(NVARCHAR(MAX), src.[min_score]), CONVERT(NVARCHAR(MAX), src.[max_score]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.score_scales src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN name IS NULL THEN N'name missing; ' ELSE N'' END, CASE WHEN min_score IS NULL THEN N'min_score missing; ' ELSE N'' END, CASE WHEN max_score IS NULL THEN N'max_score missing; ' ELSE N'' END, CASE WHEN min_score > max_score THEN N'min_score > max_score; ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Small lookup table: full refresh with TRUNCATE + INSERT.
        TRUNCATE TABLE stg_program_ops.score_scales;

        INSERT INTO stg_program_ops.score_scales
            (
                [id],
                [name],
                [min_score],
                [max_score],
                [description],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[name],
            src.[min_score],
            src.[max_score],
            src.[description],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'score_scales',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_closure_reasons
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.closure_reasons
             into Stg_ProgramOps_DB.stg_program_ops.closure_reasons
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_closure_reasons
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'closure_reasons',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'closure_reasons',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[title] AS [title],
            src.[description] AS [description],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[title]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.closure_reasons src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN title IS NULL THEN N'title missing; ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Small lookup table: full refresh with TRUNCATE + INSERT.
        TRUNCATE TABLE stg_program_ops.closure_reasons;

        INSERT INTO stg_program_ops.closure_reasons
            (
                [id],
                [title],
                [description],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[title],
            src.[description],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'closure_reasons',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_absence_reasons
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.absence_reasons
             into Stg_ProgramOps_DB.stg_program_ops.absence_reasons
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_absence_reasons
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'absence_reasons',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'absence_reasons',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[title] AS [title],
            src.[description] AS [description],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[title]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.absence_reasons src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN title IS NULL THEN N'title missing; ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Small lookup table: full refresh with TRUNCATE + INSERT.
        TRUNCATE TABLE stg_program_ops.absence_reasons;

        INSERT INTO stg_program_ops.absence_reasons
            (
                [id],
                [title],
                [description],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[title],
            src.[description],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'absence_reasons',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_no_score_reasons
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.no_score_reasons
             into Stg_ProgramOps_DB.stg_program_ops.no_score_reasons
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_no_score_reasons
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'no_score_reasons',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[title] AS [title],
            src.[description] AS [description],
            src.[is_active] AS [is_active],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[title]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.no_score_reasons src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN title IS NULL THEN N'title missing; ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Small lookup table: full refresh with TRUNCATE + INSERT.
        TRUNCATE TABLE stg_program_ops.no_score_reasons;

        INSERT INTO stg_program_ops.no_score_reasons
            (
                [id],
                [title],
                [description],
                [is_active],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[title],
            src.[description],
            src.[is_active],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'no_score_reasons',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src;

        SET @rows_inserted = @@ROWCOUNT;
        SET @rows_updated = 0;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_task_templates
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.task_templates
             into Stg_ProgramOps_DB.stg_program_ops.task_templates
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_task_templates
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'task_templates',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[domain_id] AS [domain_id],
            src.[title] AS [title],
            src.[description] AS [description],
            src.[default_score_scale_id] AS [default_score_scale_id],
            src.[is_active] AS [is_active],
            src.[created_by] AS [created_by],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[domain_id]), CONVERT(NVARCHAR(MAX), src.[title]), CONVERT(NVARCHAR(MAX), src.[description]), CONVERT(NVARCHAR(MAX), src.[default_score_scale_id]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_by]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.task_templates src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END, CASE WHEN title IS NULL THEN N'title missing; ' ELSE N'' END, CASE WHEN domain_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.domains p WHERE p.id = s.domain_id) THEN N'domain_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.domains p WHERE p.id = s.domain_id); ' ELSE N'' END, CASE WHEN default_score_scale_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.score_scales p WHERE p.id = s.default_score_scale_id) THEN N'default_score_scale_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.score_scales p WHERE p.id = s.default_score_scale_id); ' ELSE N'' END, CASE WHEN created_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by) THEN N'created_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[domain_id] = src.[domain_id],
            tgt.[title] = src.[title],
            tgt.[description] = src.[description],
            tgt.[default_score_scale_id] = src.[default_score_scale_id],
            tgt.[is_active] = src.[is_active],
            tgt.[created_by] = src.[created_by],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'task_templates',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.task_templates AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.task_templates
            (
                [id],
                [domain_id],
                [title],
                [description],
                [default_score_scale_id],
                [is_active],
                [created_by],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[domain_id],
            src.[title],
            src.[description],
            src.[default_score_scale_id],
            src.[is_active],
            src.[created_by],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'task_templates',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.task_templates AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_center_daily_status
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.center_daily_status
             into Stg_ProgramOps_DB.stg_program_ops.center_daily_status
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_center_daily_status
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'center_daily_status',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'center_daily_status',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[center_id] AS [center_id],
            src.[date] AS [date],
            src.[status] AS [status],
            src.[closure_reason_id] AS [closure_reason_id],
            src.[note] AS [note],
            src.[created_by] AS [created_by],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[date]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[closure_reason_id]), CONVERT(NVARCHAR(MAX), src.[note]), CONVERT(NVARCHAR(MAX), src.[created_by]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.center_daily_status src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN [date] IS NULL THEN N'[date] missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN center_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id) THEN N'center_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id); ' ELSE N'' END, CASE WHEN closure_reason_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.closure_reasons p WHERE p.id = s.closure_reason_id) THEN N'closure_reason_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.closure_reasons p WHERE p.id = s.closure_reason_id); ' ELSE N'' END, CASE WHEN created_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by) THEN N'created_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[center_id] = src.[center_id],
            tgt.[date] = src.[date],
            tgt.[status] = src.[status],
            tgt.[closure_reason_id] = src.[closure_reason_id],
            tgt.[note] = src.[note],
            tgt.[created_by] = src.[created_by],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'center_daily_status',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.center_daily_status AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.center_daily_status
            (
                [id],
                [center_id],
                [date],
                [status],
                [closure_reason_id],
                [note],
                [created_by],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[center_id],
            src.[date],
            src.[status],
            src.[closure_reason_id],
            src.[note],
            src.[created_by],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'center_daily_status',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.center_daily_status AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_child_daily_status
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.child_daily_status
             into Stg_ProgramOps_DB.stg_program_ops.child_daily_status
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_child_daily_status
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'child_daily_status',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'child_daily_status',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[child_id] AS [child_id],
            src.[date] AS [date],
            src.[status] AS [status],
            src.[absence_reason_id] AS [absence_reason_id],
            src.[note] AS [note],
            src.[created_by] AS [created_by],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[date]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[absence_reason_id]), CONVERT(NVARCHAR(MAX), src.[note]), CONVERT(NVARCHAR(MAX), src.[created_by]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.child_daily_status src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN child_id IS NULL THEN N'child_id missing; ' ELSE N'' END, CASE WHEN [date] IS NULL THEN N'[date] missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN child_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id) THEN N'child_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id); ' ELSE N'' END, CASE WHEN absence_reason_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.absence_reasons p WHERE p.id = s.absence_reason_id) THEN N'absence_reason_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.absence_reasons p WHERE p.id = s.absence_reason_id); ' ELSE N'' END, CASE WHEN created_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by) THEN N'created_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[child_id] = src.[child_id],
            tgt.[date] = src.[date],
            tgt.[status] = src.[status],
            tgt.[absence_reason_id] = src.[absence_reason_id],
            tgt.[note] = src.[note],
            tgt.[created_by] = src.[created_by],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'child_daily_status',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.child_daily_status AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.child_daily_status
            (
                [id],
                [child_id],
                [date],
                [status],
                [absence_reason_id],
                [note],
                [created_by],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[child_id],
            src.[date],
            src.[status],
            src.[absence_reason_id],
            src.[note],
            src.[created_by],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'child_daily_status',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.child_daily_status AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_child_task_plans
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.child_task_plans
             into Stg_ProgramOps_DB.stg_program_ops.child_task_plans
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_child_task_plans
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'child_task_plans',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'child_task_plans',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[child_id] AS [child_id],
            src.[task_template_id] AS [task_template_id],
            src.[domain_id] AS [domain_id],
            src.[task_title] AS [task_title],
            src.[score_scale_id] AS [score_scale_id],
            src.[start_date] AS [start_date],
            src.[end_date] AS [end_date],
            src.[is_active] AS [is_active],
            src.[created_by] AS [created_by],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[task_template_id]), CONVERT(NVARCHAR(MAX), src.[domain_id]), CONVERT(NVARCHAR(MAX), src.[task_title]), CONVERT(NVARCHAR(MAX), src.[score_scale_id]), CONVERT(NVARCHAR(MAX), src.[start_date]), CONVERT(NVARCHAR(MAX), src.[end_date]), CONVERT(NVARCHAR(MAX), src.[is_active]), CONVERT(NVARCHAR(MAX), src.[created_by]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.child_task_plans src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN child_id IS NULL THEN N'child_id missing; ' ELSE N'' END, CASE WHEN domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END, CASE WHEN task_title IS NULL THEN N'task_title missing; ' ELSE N'' END, CASE WHEN score_scale_id IS NULL THEN N'score_scale_id missing; ' ELSE N'' END, CASE WHEN start_date IS NULL THEN N'start_date missing; ' ELSE N'' END, CASE WHEN end_date IS NOT NULL AND start_date IS NOT NULL AND start_date > end_date THEN N'end_date IS NOT NULL AND start_date IS NOT NULL AND start_date > end_date; ' ELSE N'' END, CASE WHEN child_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id) THEN N'child_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id); ' ELSE N'' END, CASE WHEN task_template_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.task_templates p WHERE p.id = s.task_template_id) THEN N'task_template_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.task_templates p WHERE p.id = s.task_template_id); ' ELSE N'' END, CASE WHEN domain_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.domains p WHERE p.id = s.domain_id) THEN N'domain_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.domains p WHERE p.id = s.domain_id); ' ELSE N'' END, CASE WHEN score_scale_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.score_scales p WHERE p.id = s.score_scale_id) THEN N'score_scale_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.score_scales p WHERE p.id = s.score_scale_id); ' ELSE N'' END, CASE WHEN created_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by) THEN N'created_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[child_id] = src.[child_id],
            tgt.[task_template_id] = src.[task_template_id],
            tgt.[domain_id] = src.[domain_id],
            tgt.[task_title] = src.[task_title],
            tgt.[score_scale_id] = src.[score_scale_id],
            tgt.[start_date] = src.[start_date],
            tgt.[end_date] = src.[end_date],
            tgt.[is_active] = src.[is_active],
            tgt.[created_by] = src.[created_by],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'child_task_plans',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.child_task_plans AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.child_task_plans
            (
                [id],
                [child_id],
                [task_template_id],
                [domain_id],
                [task_title],
                [score_scale_id],
                [start_date],
                [end_date],
                [is_active],
                [created_by],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[child_id],
            src.[task_template_id],
            src.[domain_id],
            src.[task_title],
            src.[score_scale_id],
            src.[start_date],
            src.[end_date],
            src.[is_active],
            src.[created_by],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'child_task_plans',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.child_task_plans AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_daily_task_assignments
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.daily_task_assignments
             into Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_daily_task_assignments
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'daily_task_assignments',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[child_id] AS [child_id],
            src.[date] AS [date],
            src.[child_task_plan_id] AS [child_task_plan_id],
            src.[task_template_id] AS [task_template_id],
            src.[domain_id] AS [domain_id],
            src.[task_title] AS [task_title],
            src.[score_scale_id] AS [score_scale_id],
            src.[planned_by] AS [planned_by],
            src.[status] AS [status],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[date]), CONVERT(NVARCHAR(MAX), src.[child_task_plan_id]), CONVERT(NVARCHAR(MAX), src.[task_template_id]), CONVERT(NVARCHAR(MAX), src.[domain_id]), CONVERT(NVARCHAR(MAX), src.[task_title]), CONVERT(NVARCHAR(MAX), src.[score_scale_id]), CONVERT(NVARCHAR(MAX), src.[planned_by]), CONVERT(NVARCHAR(MAX), src.[status]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.daily_task_assignments src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN child_id IS NULL THEN N'child_id missing; ' ELSE N'' END, CASE WHEN [date] IS NULL THEN N'[date] missing; ' ELSE N'' END, CASE WHEN domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END, CASE WHEN task_title IS NULL THEN N'task_title missing; ' ELSE N'' END, CASE WHEN score_scale_id IS NULL THEN N'score_scale_id missing; ' ELSE N'' END, CASE WHEN status IS NULL THEN N'status missing; ' ELSE N'' END, CASE WHEN child_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id) THEN N'child_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id); ' ELSE N'' END, CASE WHEN child_task_plan_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.child_task_plans p WHERE p.id = s.child_task_plan_id) THEN N'child_task_plan_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.child_task_plans p WHERE p.id = s.child_task_plan_id); ' ELSE N'' END, CASE WHEN task_template_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.task_templates p WHERE p.id = s.task_template_id) THEN N'task_template_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.task_templates p WHERE p.id = s.task_template_id); ' ELSE N'' END, CASE WHEN domain_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.domains p WHERE p.id = s.domain_id) THEN N'domain_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.domains p WHERE p.id = s.domain_id); ' ELSE N'' END, CASE WHEN score_scale_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.score_scales p WHERE p.id = s.score_scale_id) THEN N'score_scale_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.score_scales p WHERE p.id = s.score_scale_id); ' ELSE N'' END, CASE WHEN planned_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.planned_by) THEN N'planned_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.planned_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[child_id] = src.[child_id],
            tgt.[date] = src.[date],
            tgt.[child_task_plan_id] = src.[child_task_plan_id],
            tgt.[task_template_id] = src.[task_template_id],
            tgt.[domain_id] = src.[domain_id],
            tgt.[task_title] = src.[task_title],
            tgt.[score_scale_id] = src.[score_scale_id],
            tgt.[planned_by] = src.[planned_by],
            tgt.[status] = src.[status],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'daily_task_assignments',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.daily_task_assignments AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.daily_task_assignments
            (
                [id],
                [child_id],
                [date],
                [child_task_plan_id],
                [task_template_id],
                [domain_id],
                [task_title],
                [score_scale_id],
                [planned_by],
                [status],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[child_id],
            src.[date],
            src.[child_task_plan_id],
            src.[task_template_id],
            src.[domain_id],
            src.[task_title],
            src.[score_scale_id],
            src.[planned_by],
            src.[status],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'daily_task_assignments',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.daily_task_assignments AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_assessment_sessions
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.assessment_sessions
             into Stg_ProgramOps_DB.stg_program_ops.assessment_sessions
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_assessment_sessions
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'assessment_sessions',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'assessment_sessions',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[child_id] AS [child_id],
            src.[teacher_id] AS [teacher_id],
            src.[center_id] AS [center_id],
            src.[date] AS [date],
            src.[started_at] AS [started_at],
            src.[ended_at] AS [ended_at],
            src.[session_status] AS [session_status],
            src.[general_note] AS [general_note],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[teacher_id]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[date]), CONVERT(NVARCHAR(MAX), src.[started_at]), CONVERT(NVARCHAR(MAX), src.[ended_at]), CONVERT(NVARCHAR(MAX), src.[session_status]), CONVERT(NVARCHAR(MAX), src.[general_note]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.assessment_sessions src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN child_id IS NULL THEN N'child_id missing; ' ELSE N'' END, CASE WHEN teacher_id IS NULL THEN N'teacher_id missing; ' ELSE N'' END, CASE WHEN center_id IS NULL THEN N'center_id missing; ' ELSE N'' END, CASE WHEN [date] IS NULL THEN N'[date] missing; ' ELSE N'' END, CASE WHEN session_status IS NULL THEN N'session_status missing; ' ELSE N'' END, CASE WHEN ended_at IS NOT NULL AND started_at IS NOT NULL AND started_at > ended_at THEN N'ended_at IS NOT NULL AND started_at IS NOT NULL AND started_at > ended_at; ' ELSE N'' END, CASE WHEN child_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id) THEN N'child_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id); ' ELSE N'' END, CASE WHEN teacher_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id) THEN N'teacher_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id); ' ELSE N'' END, CASE WHEN center_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id) THEN N'center_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[child_id] = src.[child_id],
            tgt.[teacher_id] = src.[teacher_id],
            tgt.[center_id] = src.[center_id],
            tgt.[date] = src.[date],
            tgt.[started_at] = src.[started_at],
            tgt.[ended_at] = src.[ended_at],
            tgt.[session_status] = src.[session_status],
            tgt.[general_note] = src.[general_note],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'assessment_sessions',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.assessment_sessions AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.assessment_sessions
            (
                [id],
                [child_id],
                [teacher_id],
                [center_id],
                [date],
                [started_at],
                [ended_at],
                [session_status],
                [general_note],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[child_id],
            src.[teacher_id],
            src.[center_id],
            src.[date],
            src.[started_at],
            src.[ended_at],
            src.[session_status],
            src.[general_note],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'assessment_sessions',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.assessment_sessions AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_task_assessments
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.task_assessments
             into Stg_ProgramOps_DB.stg_program_ops.task_assessments
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_task_assessments
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'task_assessments',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[daily_task_assignment_id] AS [daily_task_assignment_id],
            src.[assessment_session_id] AS [assessment_session_id],
            src.[child_id] AS [child_id],
            src.[teacher_id] AS [teacher_id],
            src.[date] AS [date],
            src.[score] AS [score],
            src.[normalized_score] AS [normalized_score],
            src.[assessment_status] AS [assessment_status],
            src.[no_score_reason_id] AS [no_score_reason_id],
            src.[attempt_no] AS [attempt_no],
            src.[note] AS [note],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[daily_task_assignment_id]), CONVERT(NVARCHAR(MAX), src.[assessment_session_id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[teacher_id]), CONVERT(NVARCHAR(MAX), src.[date]), CONVERT(NVARCHAR(MAX), src.[score]), CONVERT(NVARCHAR(MAX), src.[normalized_score]), CONVERT(NVARCHAR(MAX), src.[assessment_status]), CONVERT(NVARCHAR(MAX), src.[no_score_reason_id]), CONVERT(NVARCHAR(MAX), src.[attempt_no]), CONVERT(NVARCHAR(MAX), src.[note]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.task_assessments src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN daily_task_assignment_id IS NULL THEN N'daily_task_assignment_id missing; ' ELSE N'' END, CASE WHEN assessment_session_id IS NULL THEN N'assessment_session_id missing; ' ELSE N'' END, CASE WHEN child_id IS NULL THEN N'child_id missing; ' ELSE N'' END, CASE WHEN teacher_id IS NULL THEN N'teacher_id missing; ' ELSE N'' END, CASE WHEN [date] IS NULL THEN N'[date] missing; ' ELSE N'' END, CASE WHEN assessment_status IS NULL THEN N'assessment_status missing; ' ELSE N'' END, CASE WHEN attempt_no IS NULL THEN N'attempt_no missing; ' ELSE N'' END, CASE WHEN attempt_no IS NOT NULL AND attempt_no < 1 THEN N'attempt_no IS NOT NULL AND attempt_no < 1; ' ELSE N'' END, CASE WHEN normalized_score IS NOT NULL AND (normalized_score < 0 OR normalized_score > 100) THEN N'normalized_score IS NOT NULL AND (normalized_score < 0 OR normalized_score > 100); ' ELSE N'' END, CASE WHEN daily_task_assignment_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.daily_task_assignments p WHERE p.id = s.daily_task_assignment_id) THEN N'daily_task_assignment_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.daily_task_assignments p WHERE p.id = s.daily_task_assig...; ' ELSE N'' END, CASE WHEN assessment_session_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.assessment_sessions p WHERE p.id = s.assessment_session_id) THEN N'assessment_session_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.assessment_sessions p WHERE p.id = s.assessment_session_id); ' ELSE N'' END, CASE WHEN child_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id) THEN N'child_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id); ' ELSE N'' END, CASE WHEN teacher_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id) THEN N'teacher_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id); ' ELSE N'' END, CASE WHEN no_score_reason_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.no_score_reasons p WHERE p.id = s.no_score_reason_id) THEN N'no_score_reason_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.no_score_reasons p WHERE p.id = s.no_score_reason_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[daily_task_assignment_id] = src.[daily_task_assignment_id],
            tgt.[assessment_session_id] = src.[assessment_session_id],
            tgt.[child_id] = src.[child_id],
            tgt.[teacher_id] = src.[teacher_id],
            tgt.[date] = src.[date],
            tgt.[score] = src.[score],
            tgt.[normalized_score] = src.[normalized_score],
            tgt.[assessment_status] = src.[assessment_status],
            tgt.[no_score_reason_id] = src.[no_score_reason_id],
            tgt.[attempt_no] = src.[attempt_no],
            tgt.[note] = src.[note],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'task_assessments',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.task_assessments AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.task_assessments
            (
                [id],
                [daily_task_assignment_id],
                [assessment_session_id],
                [child_id],
                [teacher_id],
                [date],
                [score],
                [normalized_score],
                [assessment_status],
                [no_score_reason_id],
                [attempt_no],
                [note],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[daily_task_assignment_id],
            src.[assessment_session_id],
            src.[child_id],
            src.[teacher_id],
            src.[date],
            src.[score],
            src.[normalized_score],
            src.[assessment_status],
            src.[no_score_reason_id],
            src.[attempt_no],
            src.[note],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'task_assessments',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.task_assessments AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_notes
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.notes
             into Stg_ProgramOps_DB.stg_program_ops.notes
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_notes
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'notes',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'notes',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[note_scope] AS [note_scope],
            src.[center_id] AS [center_id],
            src.[child_id] AS [child_id],
            src.[teacher_id] AS [teacher_id],
            src.[date] AS [date],
            src.[daily_task_assignment_id] AS [daily_task_assignment_id],
            src.[task_assessment_id] AS [task_assessment_id],
            src.[note_text] AS [note_text],
            src.[created_by] AS [created_by],
            src.[created_at] AS [created_at],
            src.[updated_at] AS [updated_at],
            COALESCE(updated_at, created_at) AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[note_scope]), CONVERT(NVARCHAR(MAX), src.[center_id]), CONVERT(NVARCHAR(MAX), src.[child_id]), CONVERT(NVARCHAR(MAX), src.[teacher_id]), CONVERT(NVARCHAR(MAX), src.[date]), CONVERT(NVARCHAR(MAX), src.[daily_task_assignment_id]), CONVERT(NVARCHAR(MAX), src.[task_assessment_id]), CONVERT(NVARCHAR(MAX), src.[note_text]), CONVERT(NVARCHAR(MAX), src.[created_by]), CONVERT(NVARCHAR(MAX), src.[created_at]), CONVERT(NVARCHAR(MAX), src.[updated_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.notes src
        WHERE COALESCE(updated_at, created_at) <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN note_scope IS NULL THEN N'note_scope missing; ' ELSE N'' END, CASE WHEN note_text IS NULL THEN N'note_text missing; ' ELSE N'' END, CASE WHEN center_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id) THEN N'center_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.centers p WHERE p.id = s.center_id); ' ELSE N'' END, CASE WHEN child_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id) THEN N'child_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.children p WHERE p.id = s.child_id); ' ELSE N'' END, CASE WHEN teacher_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id) THEN N'teacher_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.teachers p WHERE p.id = s.teacher_id); ' ELSE N'' END, CASE WHEN daily_task_assignment_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.daily_task_assignments p WHERE p.id = s.daily_task_assignment_id) THEN N'daily_task_assignment_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.daily_task_assignments p WHERE p.id = s.daily_task_assig...; ' ELSE N'' END, CASE WHEN task_assessment_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.task_assessments p WHERE p.id = s.task_assessment_id) THEN N'task_assessment_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.task_assessments p WHERE p.id = s.task_assessment_id); ' ELSE N'' END, CASE WHEN created_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by) THEN N'created_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[note_scope] = src.[note_scope],
            tgt.[center_id] = src.[center_id],
            tgt.[child_id] = src.[child_id],
            tgt.[teacher_id] = src.[teacher_id],
            tgt.[date] = src.[date],
            tgt.[daily_task_assignment_id] = src.[daily_task_assignment_id],
            tgt.[task_assessment_id] = src.[task_assessment_id],
            tgt.[note_text] = src.[note_text],
            tgt.[created_by] = src.[created_by],
            tgt.[created_at] = src.[created_at],
            tgt.[updated_at] = src.[updated_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'notes',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.notes AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.notes
            (
                [id],
                [note_scope],
                [center_id],
                [child_id],
                [teacher_id],
                [date],
                [daily_task_assignment_id],
                [task_assessment_id],
                [note_text],
                [created_by],
                [created_at],
                [updated_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[note_scope],
            src.[center_id],
            src.[child_id],
            src.[teacher_id],
            src.[date],
            src.[daily_task_assignment_id],
            src.[task_assessment_id],
            src.[note_text],
            src.[created_by],
            src.[created_at],
            src.[updated_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'notes',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.notes AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_note_batches
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.note_batches
             into Stg_ProgramOps_DB.stg_program_ops.note_batches
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_note_batches
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'note_batches',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'note_batches',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[created_by] AS [created_by],
            src.[note_scope] AS [note_scope],
            src.[note_text] AS [note_text],
            src.[created_at] AS [created_at],
            created_at AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[created_by]), CONVERT(NVARCHAR(MAX), src.[note_scope]), CONVERT(NVARCHAR(MAX), src.[note_text]), CONVERT(NVARCHAR(MAX), src.[created_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.note_batches src
        WHERE created_at <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN note_scope IS NULL THEN N'note_scope missing; ' ELSE N'' END, CASE WHEN note_text IS NULL THEN N'note_text missing; ' ELSE N'' END, CASE WHEN created_by IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by) THEN N'created_by invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.created_by); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[created_by] = src.[created_by],
            tgt.[note_scope] = src.[note_scope],
            tgt.[note_text] = src.[note_text],
            tgt.[created_at] = src.[created_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'note_batches',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.note_batches AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.note_batches
            (
                [id],
                [created_by],
                [note_scope],
                [note_text],
                [created_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[created_by],
            src.[note_scope],
            src.[note_text],
            src.[created_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'note_batches',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.note_batches AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_note_batch_items
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.note_batch_items
             into Stg_ProgramOps_DB.stg_program_ops.note_batch_items
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_note_batch_items
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'note_batch_items',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'note_batch_items',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[note_batch_id] AS [note_batch_id],
            src.[note_id] AS [note_id],
            SYSDATETIME() AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[note_batch_id]), CONVERT(NVARCHAR(MAX), src.[note_id]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.note_batch_items src
        WHERE EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.note_batches nb WHERE nb.id = src.note_batch_id AND nb.created_at <= @to_date)
          AND EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.notes n WHERE n.id = src.note_id AND COALESCE(n.updated_at, n.created_at) <= @to_date);

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN note_batch_id IS NULL THEN N'note_batch_id missing; ' ELSE N'' END, CASE WHEN note_id IS NULL THEN N'note_id missing; ' ELSE N'' END, CASE WHEN note_batch_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.note_batches p WHERE p.id = s.note_batch_id) THEN N'note_batch_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.note_batches p WHERE p.id = s.note_batch_id); ' ELSE N'' END, CASE WHEN note_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.notes p WHERE p.id = s.note_id) THEN N'note_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.notes p WHERE p.id = s.note_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[note_batch_id] = src.[note_batch_id],
            tgt.[note_id] = src.[note_id],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'note_batch_items',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.note_batch_items AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.note_batch_items
            (
                [id],
                [note_batch_id],
                [note_id],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[note_batch_id],
            src.[note_id],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'note_batch_items',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.note_batch_items AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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
  Procedure: etl_admin.usp_load_stg_program_ops_audit_logs
  Purpose  : Upsert validated Source_ProgramOps_DB.program_ops.audit_logs
             into Stg_ProgramOps_DB.stg_program_ops.audit_logs
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_stg_program_ops_audit_logs
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
                (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

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
            (@effective_batch_id, N'Source_ProgramOps_DB', N'program_ops', N'audit_logs',
             N'Stg_ProgramOps_DB', N'stg_program_ops', N'audit_logs',
             N'running', SYSDATETIME(), N'Starting source-to-staging load.');

        SET @load_log_id = SCOPE_IDENTITY();

        BEGIN TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
        IF OBJECT_ID('tempdb..#valid') IS NOT NULL DROP TABLE #valid;

        SELECT
            src.[id] AS [id],
            src.[user_id] AS [user_id],
            src.[entity_name] AS [entity_name],
            src.[entity_id] AS [entity_id],
            src.[action] AS [action],
            src.[old_value] AS [old_value],
            src.[new_value] AS [new_value],
            src.[created_at] AS [created_at],
            created_at AS source_updated_at,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', CONVERT(NVARCHAR(MAX), src.[id]), CONVERT(NVARCHAR(MAX), src.[user_id]), CONVERT(NVARCHAR(MAX), src.[entity_name]), CONVERT(NVARCHAR(MAX), src.[entity_id]), CONVERT(NVARCHAR(MAX), src.[action]), CONVERT(NVARCHAR(MAX), src.[old_value]), CONVERT(NVARCHAR(MAX), src.[new_value]), CONVERT(NVARCHAR(MAX), src.[created_at]))) AS row_hash
        INTO #src
        FROM Source_ProgramOps_DB.program_ops.audit_logs src
        WHERE created_at <= @to_date;

        SET @rows_read = @@ROWCOUNT;

        SELECT
            s.*,
            NULLIF(CONCAT(CASE WHEN id IS NULL THEN N'id missing; ' ELSE N'' END, CASE WHEN entity_name IS NULL THEN N'entity_name missing; ' ELSE N'' END, CASE WHEN entity_id IS NULL THEN N'entity_id missing; ' ELSE N'' END, CASE WHEN action IS NULL THEN N'action missing; ' ELSE N'' END, CASE WHEN user_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.user_id) THEN N'user_id invalid reference (SELECT 1 FROM Source_ProgramOps_DB.program_ops.users p WHERE p.id = s.user_id); ' ELSE N'' END), N'') AS validation_message
        INTO #valid
        FROM #src s;

        SET @rows_rejected = (
            SELECT COUNT(*)
            FROM #valid
            WHERE validation_message IS NOT NULL
        );

        DELETE FROM #valid
        WHERE validation_message IS NOT NULL;

        SELECT @rows_valid = COUNT(*) FROM #valid;
        -- Large/growing table: update existing changed rows, then insert new rows.
        UPDATE tgt
        SET
            tgt.[user_id] = src.[user_id],
            tgt.[entity_name] = src.[entity_name],
            tgt.[entity_id] = src.[entity_id],
            tgt.[action] = src.[action],
            tgt.[old_value] = src.[old_value],
            tgt.[new_value] = src.[new_value],
            tgt.[created_at] = src.[created_at],
            tgt.etl_batch_id = @effective_batch_id,
            tgt.source_system = N'PROGRAM_OPS',
            tgt.source_database = N'Source_ProgramOps_DB',
            tgt.source_schema = N'program_ops',
            tgt.source_table = N'audit_logs',
            tgt.extracted_at = @extract_time,
            tgt.source_updated_at = src.source_updated_at,
            tgt.row_hash = src.row_hash,
            tgt.is_valid = 1,
            tgt.validation_message = NULL
        FROM stg_program_ops.audit_logs AS tgt
        INNER JOIN #valid AS src
            ON tgt.[id] = src.[id]
        WHERE
            tgt.row_hash IS NULL
            OR src.row_hash IS NULL
            OR tgt.row_hash <> src.row_hash
            OR ISNULL(tgt.is_valid, 0) <> 1;

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO stg_program_ops.audit_logs
            (
                [id],
                [user_id],
                [entity_name],
                [entity_id],
                [action],
                [old_value],
                [new_value],
                [created_at],
                [etl_batch_id],
                [source_system],
                [source_database],
                [source_schema],
                [source_table],
                [extracted_at],
                [source_updated_at],
                [row_hash],
                [is_valid],
                [validation_message]
            )
        SELECT
            src.[id],
            src.[user_id],
            src.[entity_name],
            src.[entity_id],
            src.[action],
            src.[old_value],
            src.[new_value],
            src.[created_at],
            @effective_batch_id,
            N'PROGRAM_OPS',
            N'Source_ProgramOps_DB',
            N'program_ops',
            N'audit_logs',
            @extract_time,
            src.source_updated_at,
            src.row_hash,
            1,
            NULL
        FROM #valid AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM stg_program_ops.audit_logs AS tgt
            WHERE tgt.[id] = src.[id]
        );

        SET @rows_inserted = @@ROWCOUNT;

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
                N'; to_date: ', CONVERT(NVARCHAR(30), @to_date, 126)
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

        COMMIT TRANSACTION;
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

CREATE OR ALTER PROCEDURE etl_admin.usp_run_stg_program_ops_all
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
            (N'PROGRAM_OPS', N'STAGING', N'running', SYSDATETIME());

        SET @etl_batch_id = SCOPE_IDENTITY();

        /*
          Safe load order:
          parent/reference tables first, transactional/dependent tables later.
        */

        EXEC etl_admin.usp_load_stg_program_ops_centers @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_children @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_teachers @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_users @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_domains @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_score_scales @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_closure_reasons @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_absence_reasons @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_no_score_reasons @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_task_templates @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_center_daily_status @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_child_daily_status @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_child_task_plans @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_daily_task_assignments @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_assessment_sessions @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_task_assessments @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_notes @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_note_batches @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_note_batch_items @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_stg_program_ops_audit_logs @to_date = @to_date, @etl_batch_id = @etl_batch_id;

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

  EXEC etl_admin.usp_run_stg_program_ops_all
      @to_date = '2025-12-31 23:59:59';
=============================================================================*/

PRINT 'Program Ops source-to-staging ETL procedures created successfully.';
PRINT 'Main procedure: etl_admin.usp_run_stg_program_ops_all';
GO
