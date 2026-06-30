/*=============================================================================
  File: 28_P_fl_Etl_Procedure__program_stagging_to_dw_REWORKED.sql

  Purpose:
      First-load ETL procedures, reworked by requested ETL patterns.

  Main changes:
      - Type 1 dimensions use permanent etl_work tables + TRUNCATE + INSERT.
      - Type 2 dimensions load initial current versions.
      - Facts do not update target fact rows.
      - Snapshot fact uses a daily loop and appends missing daily rows.
      - Lifecycle fact is rebuilt using work tables and fact_daily snapshots.
      - No SQL Server local temporary tables are used.
=============================================================================*/

USE Charity_DW_DB;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_center
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_center;

        INSERT INTO etl_work.w_dim_center
            (center_id, center_name, city, address, center_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        SELECT
            s.id,
            LTRIM(RTRIM(s.name)),
            LTRIM(RTRIM(s.city)),
            LTRIM(RTRIM(s.address)),
            CASE WHEN ISNULL(s.is_active, 0) = 1 THEN N'active' ELSE N'inactive' END,
            @start_time,
            NULL,
            1,
            s.source_system,
            s.row_hash,
            SYSDATETIME(),
            SYSDATETIME()
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.centers
            WHERE is_valid = 1
              AND id IS NOT NULL
              AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
              AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.centers AS s
            ON s.stg_row_id = x.max_stg_row_id;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_center;

        TRUNCATE TABLE dw.dim_center;
        DBCC CHECKIDENT ('dw.dim_center', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_center ON;
        INSERT INTO dw.dim_center
            (center_key, center_id, center_name, city, address, center_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', N'Unknown', N'Unknown', N'unknown',
             CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'SYSTEM', NULL,
             SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_center OFF;

        INSERT INTO dw.dim_center
            (center_id, center_name, city, address, center_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        SELECT
            center_id, center_name, city, address, center_status,
            effective_from, effective_to, is_current, source_system, row_hash,
            created_at, updated_at
        FROM etl_work.w_dim_center;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 2 dimension. Target was truncated, unknown row inserted, then current versions inserted from etl_work.w_dim_center.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers',
             N'Charity_DW_DB', N'dw', N'dim_center', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers',
                 N'Charity_DW_DB', N'dw', N'dim_center', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_teacher
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_teacher;

        INSERT INTO etl_work.w_dim_teacher
            (teacher_id, first_name, last_name, full_name, center_id, center_name,
             employment_status, effective_from, effective_to, is_current,
             source_system, row_hash, created_at, updated_at)
        SELECT
            s.id,
            LTRIM(RTRIM(s.first_name)),
            LTRIM(RTRIM(s.last_name)),
            LTRIM(RTRIM(CONCAT(ISNULL(s.first_name, N''), N' ', ISNULL(s.last_name, N'')))),
            s.center_id,
            dc.center_name,
            CASE WHEN ISNULL(s.is_active, 0) = 1 THEN COALESCE(NULLIF(LTRIM(RTRIM(s.employment_status)), N''), N'active') ELSE N'inactive' END,
            @start_time,
            NULL,
            1,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.first_name, s.last_name, s.center_id, dc.center_name, s.employment_status, s.is_active)),
            SYSDATETIME(),
            SYSDATETIME()
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.teachers
            WHERE is_valid = 1
              AND id IS NOT NULL
              AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) >= @start_time
              AND COALESCE(source_updated_at, updated_at, created_at, extracted_at) <  @end_time
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.teachers AS s
            ON s.stg_row_id = x.max_stg_row_id
        LEFT JOIN dw.dim_center AS dc
            ON dc.center_id = s.center_id
           AND dc.is_current = 1;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_teacher;

        TRUNCATE TABLE dw.dim_teacher;
        DBCC CHECKIDENT ('dw.dim_teacher', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_teacher ON;
        INSERT INTO dw.dim_teacher
            (teacher_key, teacher_id, first_name, last_name, full_name,
             center_id, center_name, employment_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', N'Unknown', N'Unknown',
             -1, N'Unknown', N'unknown',
             CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'SYSTEM', NULL,
             SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_teacher OFF;

        INSERT INTO dw.dim_teacher
            (teacher_id, first_name, last_name, full_name,
             center_id, center_name, employment_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        SELECT
            teacher_id, first_name, last_name, full_name,
            center_id, center_name, employment_status,
            effective_from, effective_to, is_current, source_system, row_hash,
            created_at, updated_at
        FROM etl_work.w_dim_teacher;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 2 teacher dimension.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers',
             N'Charity_DW_DB', N'dw', N'dim_teacher', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers',
                 N'Charity_DW_DB', N'dw', N'dim_teacher', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_child
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_child;

        INSERT INTO etl_work.w_dim_child
            (child_id, first_name, last_name, full_name, birth_date, gender,
             center_id, status, enrollment_date, source_system, row_hash,
             created_at, updated_at)
        SELECT
            s.id,
            LTRIM(RTRIM(s.first_name)),
            LTRIM(RTRIM(s.last_name)),
            LTRIM(RTRIM(CONCAT(ISNULL(s.first_name, N''), N' ', ISNULL(s.last_name, N'')))),
            s.birth_date,
            LTRIM(RTRIM(s.gender)),
            s.center_id,
            LTRIM(RTRIM(s.status)),
            s.enrollment_date,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.first_name, s.last_name, s.birth_date, s.gender, s.center_id, s.status, s.enrollment_date)),
            SYSDATETIME(),
            SYSDATETIME()
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.children
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.children AS s
            ON s.stg_row_id = x.max_stg_row_id;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_child;

        TRUNCATE TABLE dw.dim_child;
        DBCC CHECKIDENT ('dw.dim_child', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_child ON;
        INSERT INTO dw.dim_child
            (child_key, child_id, first_name, last_name, full_name, birth_date,
             gender, center_id, status, enrollment_date, source_system, row_hash,
             created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', N'Unknown', N'Unknown', NULL,
             N'unknown', -1, N'unknown', NULL, N'SYSTEM', NULL,
             SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_child OFF;

        INSERT INTO dw.dim_child
            (child_id, first_name, last_name, full_name, birth_date,
             gender, center_id, status, enrollment_date, source_system, row_hash,
             created_at, updated_at)
        SELECT child_id, first_name, last_name, full_name, birth_date,
               gender, center_id, status, enrollment_date, source_system, row_hash,
               created_at, updated_at
        FROM etl_work.w_dim_child;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 1 child dimension using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'children',
             N'Charity_DW_DB', N'dw', N'dim_child', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'children',
                 N'Charity_DW_DB', N'dw', N'dim_child', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_domain
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_domain;

        INSERT INTO etl_work.w_dim_domain
            (domain_id, domain_name, domain_description, domain_status,
             source_system, row_hash, created_at, updated_at)
        SELECT
            s.id,
            LTRIM(RTRIM(s.name)),
            s.description,
            CASE WHEN ISNULL(s.is_active, 0) = 1 THEN N'active' ELSE N'inactive' END,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.name, s.description, s.is_active)),
            SYSDATETIME(),
            SYSDATETIME()
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.domains
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.domains AS s
            ON s.stg_row_id = x.max_stg_row_id;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_domain;

        TRUNCATE TABLE dw.dim_domain;
        DBCC CHECKIDENT ('dw.dim_domain', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_domain ON;
        INSERT INTO dw.dim_domain
            (domain_key, domain_id, domain_name, domain_description, domain_status,
             source_system, row_hash, created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', N'Unknown', N'unknown', N'SYSTEM', NULL, SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_domain OFF;

        INSERT INTO dw.dim_domain
            (domain_id, domain_name, domain_description, domain_status,
             source_system, row_hash, created_at, updated_at)
        SELECT domain_id, domain_name, domain_description, domain_status,
               source_system, row_hash, created_at, updated_at
        FROM etl_work.w_dim_domain;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 1 domain dimension using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains',
             N'Charity_DW_DB', N'dw', N'dim_domain', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains',
                 N'Charity_DW_DB', N'dw', N'dim_domain', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_score_scale
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_score_scale;

        INSERT INTO etl_work.w_dim_score_scale
            (score_scale_id, scale_name, min_score, max_score, scale_description,
             scale_status, source_system, row_hash, created_at, updated_at)
        SELECT
            s.id,
            LTRIM(RTRIM(s.name)),
            s.min_score,
            s.max_score,
            s.description,
            CASE WHEN ISNULL(s.is_active, 0) = 1 THEN N'active' ELSE N'inactive' END,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.name, s.min_score, s.max_score, s.description, s.is_active)),
            SYSDATETIME(),
            SYSDATETIME()
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.score_scales
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.score_scales AS s
            ON s.stg_row_id = x.max_stg_row_id;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_score_scale;

        TRUNCATE TABLE dw.dim_score_scale;
        DBCC CHECKIDENT ('dw.dim_score_scale', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_score_scale ON;
        INSERT INTO dw.dim_score_scale
            (score_scale_key, score_scale_id, scale_name, min_score, max_score,
             scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', NULL, NULL, N'Unknown', N'unknown', N'SYSTEM', NULL, SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_score_scale OFF;

        INSERT INTO dw.dim_score_scale
            (score_scale_id, scale_name, min_score, max_score,
             scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        SELECT score_scale_id, scale_name, min_score, max_score,
               scale_description, scale_status, source_system, row_hash, created_at, updated_at
        FROM etl_work.w_dim_score_scale;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 1 score scale dimension using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales',
             N'Charity_DW_DB', N'dw', N'dim_score_scale', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales',
                 N'Charity_DW_DB', N'dw', N'dim_score_scale', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_assessment_status
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_assessment_status;

        INSERT INTO etl_work.w_dim_assessment_status
            (assessment_status_code, assessment_status_title, assessment_status_category,
             is_successful_assessment, is_failure_assessment, source_system,
             created_at, updated_at)
        SELECT DISTINCT
            LOWER(LTRIM(RTRIM(assessment_status))) AS assessment_status_code,
            LTRIM(RTRIM(assessment_status)) AS assessment_status_title,
            CASE
                WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'scored', N'completed') THEN N'success'
                WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'not_scored', N'refused', N'absent', N'incomplete', N'cancelled') THEN N'failure'
                ELSE N'other'
            END,
            CASE WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'scored', N'completed') THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'not_scored', N'refused', N'absent', N'incomplete', N'cancelled') THEN 1 ELSE 0 END,
            N'PROGRAM_OPS',
            SYSDATETIME(),
            SYSDATETIME()
        FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments
        WHERE is_valid = 1
          AND NULLIF(LTRIM(RTRIM(assessment_status)), N'') IS NOT NULL;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_assessment_status;

        TRUNCATE TABLE dw.dim_assessment_status;
        DBCC CHECKIDENT ('dw.dim_assessment_status', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_assessment_status ON;
        INSERT INTO dw.dim_assessment_status
            (assessment_status_key, assessment_status_code, assessment_status_title,
             assessment_status_category, is_successful_assessment, is_failure_assessment,
             source_system, created_at, updated_at)
        VALUES
            (-1, N'unknown', N'Unknown', N'unknown', 0, 0, N'SYSTEM', SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_assessment_status OFF;

        INSERT INTO dw.dim_assessment_status
            (assessment_status_code, assessment_status_title,
             assessment_status_category, is_successful_assessment, is_failure_assessment,
             source_system, created_at, updated_at)
        SELECT assessment_status_code, assessment_status_title,
               assessment_status_category, is_successful_assessment, is_failure_assessment,
               source_system, created_at, updated_at
        FROM etl_work.w_dim_assessment_status;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load static/reference assessment status dimension using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments',
             N'Charity_DW_DB', N'dw', N'dim_assessment_status', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments',
                 N'Charity_DW_DB', N'dw', N'dim_assessment_status', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_no_score_reason
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_no_score_reason;

        INSERT INTO etl_work.w_dim_no_score_reason
            (no_score_reason_id, reason_title, reason_description, reason_category,
             is_child_related, is_teacher_related, is_center_related, is_system_related,
             source_system, row_hash, created_at, updated_at)
        SELECT
            s.id,
            LTRIM(RTRIM(s.title)),
            s.description,
            CASE
                WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%child%' OR LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%absent%' OR LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%refus%' THEN N'child'
                WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%teacher%' THEN N'teacher'
                WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%center%' OR LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%closed%' THEN N'center'
                WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%system%' THEN N'system'
                ELSE N'other'
            END,
            CASE WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%child%' OR LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%absent%' OR LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%refus%' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%teacher%' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%center%' OR LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%closed%' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(CONCAT(s.title, N' ', s.description)) LIKE N'%system%' THEN 1 ELSE 0 END,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.title, s.description, s.is_active)),
            SYSDATETIME(),
            SYSDATETIME()
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.no_score_reasons
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.no_score_reasons AS s
            ON s.stg_row_id = x.max_stg_row_id;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_no_score_reason;

        TRUNCATE TABLE dw.dim_no_score_reason;
        DBCC CHECKIDENT ('dw.dim_no_score_reason', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_no_score_reason ON;
        INSERT INTO dw.dim_no_score_reason
            (no_score_reason_key, no_score_reason_id, reason_title, reason_description,
             reason_category, is_child_related, is_teacher_related, is_center_related,
             is_system_related, source_system, row_hash, created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', N'Unknown', N'unknown', 0, 0, 0, 0, N'SYSTEM', NULL, SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_no_score_reason OFF;

        INSERT INTO dw.dim_no_score_reason
            (no_score_reason_id, reason_title, reason_description,
             reason_category, is_child_related, is_teacher_related, is_center_related,
             is_system_related, source_system, row_hash, created_at, updated_at)
        SELECT no_score_reason_id, reason_title, reason_description,
               reason_category, is_child_related, is_teacher_related, is_center_related,
               is_system_related, source_system, row_hash, created_at, updated_at
        FROM etl_work.w_dim_no_score_reason;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 1 no-score reason dimension using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons',
             N'Charity_DW_DB', N'dw', N'dim_no_score_reason', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons',
                 N'Charity_DW_DB', N'dw', N'dim_no_score_reason', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_task
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_dim_task;

        INSERT INTO etl_work.w_dim_task
            (task_template_id, task_title, domain_id, domain_name, is_template_based,
             task_description, task_status, source_system, row_hash, created_at, updated_at,
             natural_task_code)
        SELECT
            s.id,
            LTRIM(RTRIM(s.title)),
            s.domain_id,
            dd.domain_name,
            1,
            s.description,
            CASE WHEN ISNULL(s.is_active, 0) = 1 THEN N'active' ELSE N'inactive' END,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.title, s.domain_id, s.description, s.is_active)),
            SYSDATETIME(),
            SYSDATETIME(),
            CONCAT(N'TEMPLATE:', CONVERT(NVARCHAR(50), s.id))
        FROM (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.task_templates
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.task_templates AS s
            ON s.stg_row_id = x.max_stg_row_id
        LEFT JOIN dw.dim_domain AS dd
            ON dd.domain_id = s.domain_id;

        INSERT INTO etl_work.w_dim_task
            (task_template_id, task_title, domain_id, domain_name, is_template_based,
             task_description, task_status, source_system, row_hash, created_at, updated_at,
             natural_task_code)
        SELECT
            NULL,
            LTRIM(RTRIM(s.task_title)),
            s.domain_id,
            dd.domain_name,
            0,
            NULL,
            CASE WHEN ISNULL(s.is_active, 0) = 1 THEN N'active' ELSE N'inactive' END,
            s.source_system,
            HASHBYTES('SHA2_256', CONCAT_WS(N'|', N'CUSTOM', s.domain_id, LTRIM(RTRIM(s.task_title)), s.is_active)),
            SYSDATETIME(),
            SYSDATETIME(),
            CONCAT(N'CUSTOM:', CONVERT(NVARCHAR(50), s.domain_id), N':', LOWER(LTRIM(RTRIM(s.task_title))))
        FROM (
            SELECT domain_id, LTRIM(RTRIM(task_title)) AS task_title, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.child_task_plans
            WHERE is_valid = 1
              AND task_template_id IS NULL
              AND NULLIF(LTRIM(RTRIM(task_title)), N'') IS NOT NULL
            GROUP BY domain_id, LTRIM(RTRIM(task_title))
        ) AS x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.child_task_plans AS s
            ON s.stg_row_id = x.max_stg_row_id
        LEFT JOIN dw.dim_domain AS dd
            ON dd.domain_id = s.domain_id;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_dim_task;

        TRUNCATE TABLE dw.dim_task;
        DBCC CHECKIDENT ('dw.dim_task', RESEED, 0) WITH NO_INFOMSGS;

        SET IDENTITY_INSERT dw.dim_task ON;
        INSERT INTO dw.dim_task
            (task_key, task_template_id, task_title, domain_id, domain_name,
             is_template_based, task_description, task_status,
             source_system, row_hash, created_at, updated_at)
        VALUES
            (-1, -1, N'Unknown', -1, N'Unknown', 0, N'Unknown', N'unknown', N'SYSTEM', NULL, SYSDATETIME(), SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_task OFF;

        INSERT INTO dw.dim_task
            (task_template_id, task_title, domain_id, domain_name,
             is_template_based, task_description, task_status,
             source_system, row_hash, created_at, updated_at)
        SELECT task_template_id, task_title, domain_id, domain_name,
               is_template_based, task_description, task_status,
               source_system, row_hash, created_at, updated_at
        FROM etl_work.w_dim_task;

        SET @rows_inserted = @@ROWCOUNT + 1;
        SET @message = N'First-load Type 1 task dimension using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates',
             N'Charity_DW_DB', N'dw', N'dim_task', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates',
                 N'Charity_DW_DB', N'dw', N'dim_task', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_tran_student_task_progress
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_fact_tran_student_task_progress;

        ;WITH latest_dta AS (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ), dta AS (
            SELECT s.*
            FROM latest_dta AS x
            INNER JOIN Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS s
                ON s.stg_row_id = x.max_stg_row_id
        ), latest_users AS (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.users
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ), usr AS (
            SELECT s.*
            FROM latest_users AS x
            INNER JOIN Stg_ProgramOps_DB.stg_program_ops.users AS s
                ON s.stg_row_id = x.max_stg_row_id
        )
        INSERT INTO etl_work.w_fact_tran_student_task_progress
            (date_key, child_key, center_key, teacher_key, domain_key, task_key,
             score_scale_key, assessment_status_key, no_score_reason_key,
             attempt_no, raw_score, normalized_score,
             is_completed, is_planned, is_scored, is_not_scored, is_cancelled,
             is_incomplete, is_refused, is_absent, is_center_closed, is_assessed,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_child_task_plan_id, source_system)
        SELECT
            COALESCE(dd.TimeKey, -1),
            COALESCE(dc.child_key, -1),
            COALESCE(dcen.center_key, -1),
            COALESCE(dt.teacher_key, -1),
            COALESCE(ddom.domain_key, -1),
            COALESCE(dtask.task_key, -1),
            COALESCE(dss.score_scale_key, -1),
            -1,
            -1,
            NULL,
            NULL,
            NULL,
            CASE WHEN LOWER(LTRIM(RTRIM(dta.status))) = N'completed' THEN 1 ELSE 0 END,
            1,
            0,
            0,
            CASE WHEN LOWER(LTRIM(RTRIM(dta.status))) = N'cancelled' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(dta.status))) = N'incomplete' THEN 1 ELSE 0 END,
            0,
            CASE WHEN LOWER(LTRIM(RTRIM(cds.status))) = N'absent' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(cends.status))) = N'closed' THEN 1 ELSE 0 END,
            0,
            dta.id,
            NULL,
            NULL,
            dta.child_task_plan_id,
            dta.source_system
        FROM dta
        LEFT JOIN dw.dim_date AS dd
            ON dd.FullDateAlternateKey = dta.[date]
        LEFT JOIN dw.dim_child AS dc
            ON dc.child_id = dta.child_id
        OUTER APPLY (
            SELECT TOP (1) c.center_key
            FROM dw.dim_center AS c
            WHERE c.center_id = dc.center_id
              AND c.is_current = 1
            ORDER BY c.center_key DESC
        ) AS dcen
        LEFT JOIN usr
            ON usr.id = dta.planned_by
        OUTER APPLY (
            SELECT TOP (1) t.teacher_key
            FROM dw.dim_teacher AS t
            WHERE t.teacher_id = usr.teacher_id
              AND t.is_current = 1
            ORDER BY t.teacher_key DESC
        ) AS dt
        LEFT JOIN dw.dim_domain AS ddom
            ON ddom.domain_id = dta.domain_id
        OUTER APPLY (
            SELECT TOP (1) task_key
            FROM dw.dim_task AS task
            WHERE (task.task_template_id = dta.task_template_id)
               OR (dta.task_template_id IS NULL AND task.task_template_id IS NULL AND task.domain_id = dta.domain_id AND LOWER(LTRIM(RTRIM(task.task_title))) = LOWER(LTRIM(RTRIM(dta.task_title))))
            ORDER BY task.task_key DESC
        ) AS dtask
        LEFT JOIN dw.dim_score_scale AS dss
            ON dss.score_scale_id = dta.score_scale_id
        OUTER APPLY (
            SELECT TOP (1) status
            FROM Stg_ProgramOps_DB.stg_program_ops.child_daily_status AS cds0
            WHERE cds0.is_valid = 1
              AND cds0.child_id = dta.child_id
              AND cds0.[date] = dta.[date]
            ORDER BY cds0.stg_row_id DESC
        ) AS cds
        OUTER APPLY (
            SELECT TOP (1) status
            FROM Stg_ProgramOps_DB.stg_program_ops.center_daily_status AS cends0
            WHERE cends0.is_valid = 1
              AND cends0.center_id = dc.center_id
              AND cends0.[date] = dta.[date]
            ORDER BY cends0.stg_row_id DESC
        ) AS cends
        WHERE COALESCE(dta.source_updated_at, dta.updated_at, dta.created_at, dta.extracted_at) >= @start_time
          AND COALESCE(dta.source_updated_at, dta.updated_at, dta.created_at, dta.extracted_at) <  @end_time;

        ;WITH latest_ta AS (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ), ta AS (
            SELECT s.*
            FROM latest_ta AS x
            INNER JOIN Stg_ProgramOps_DB.stg_program_ops.task_assessments AS s
                ON s.stg_row_id = x.max_stg_row_id
        ), latest_dta AS (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ), dta AS (
            SELECT s.*
            FROM latest_dta AS x
            INNER JOIN Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS s
                ON s.stg_row_id = x.max_stg_row_id
        ), latest_sess AS (
            SELECT id, MAX(stg_row_id) AS max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.assessment_sessions
            WHERE is_valid = 1 AND id IS NOT NULL
            GROUP BY id
        ), sess AS (
            SELECT s.*
            FROM latest_sess AS x
            INNER JOIN Stg_ProgramOps_DB.stg_program_ops.assessment_sessions AS s
                ON s.stg_row_id = x.max_stg_row_id
        )
        INSERT INTO etl_work.w_fact_tran_student_task_progress
            (date_key, child_key, center_key, teacher_key, domain_key, task_key,
             score_scale_key, assessment_status_key, no_score_reason_key,
             attempt_no, raw_score, normalized_score,
             is_completed, is_planned, is_scored, is_not_scored, is_cancelled,
             is_incomplete, is_refused, is_absent, is_center_closed, is_assessed,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_child_task_plan_id, source_system)
        SELECT
            COALESCE(dd.TimeKey, -1),
            COALESCE(dc.child_key, -1),
            COALESCE(dcen.center_key, -1),
            COALESCE(dt.teacher_key, -1),
            COALESCE(ddom.domain_key, -1),
            COALESCE(dtask.task_key, -1),
            COALESCE(dss.score_scale_key, -1),
            COALESCE(das.assessment_status_key, -1),
            COALESCE(dnsr.no_score_reason_key, -1),
            ta.attempt_no,
            ta.score,
            CAST(CASE
                    WHEN ta.score IS NULL THEN NULL
                    WHEN ta.normalized_score IS NOT NULL THEN ta.normalized_score
                    WHEN ss_src.max_score IS NULL OR ss_src.max_score = 0 THEN NULL
                    ELSE (ta.score / ss_src.max_score) * 100.0
                 END AS DECIMAL(10,4)),
            CASE WHEN LOWER(LTRIM(RTRIM(ta.assessment_status))) IN (N'scored', N'completed') THEN 1 ELSE 0 END,
            1,
            CASE WHEN ta.score IS NOT NULL OR LOWER(LTRIM(RTRIM(ta.assessment_status))) = N'scored' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(ta.assessment_status))) = N'not_scored' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(ta.assessment_status))) = N'cancelled' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(ta.assessment_status))) = N'incomplete' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(ta.assessment_status))) = N'refused' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(cds.status))) = N'absent' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(LTRIM(RTRIM(cends.status))) = N'closed' THEN 1 ELSE 0 END,
            1,
            ta.daily_task_assignment_id,
            ta.id,
            ta.assessment_session_id,
            dta.child_task_plan_id,
            ta.source_system
        FROM ta
        LEFT JOIN dta
            ON dta.id = ta.daily_task_assignment_id
        LEFT JOIN sess
            ON sess.id = ta.assessment_session_id
        LEFT JOIN dw.dim_date AS dd
            ON dd.FullDateAlternateKey = ta.[date]
        LEFT JOIN dw.dim_child AS dc
            ON dc.child_id = ta.child_id
        OUTER APPLY (
            SELECT TOP (1) c.center_key
            FROM dw.dim_center AS c
            WHERE c.center_id = COALESCE(sess.center_id, dc.center_id)
              AND c.is_current = 1
            ORDER BY c.center_key DESC
        ) AS dcen
        OUTER APPLY (
            SELECT TOP (1) t.teacher_key
            FROM dw.dim_teacher AS t
            WHERE t.teacher_id = ta.teacher_id
              AND t.is_current = 1
            ORDER BY t.teacher_key DESC
        ) AS dt
        LEFT JOIN dw.dim_domain AS ddom
            ON ddom.domain_id = dta.domain_id
        OUTER APPLY (
            SELECT TOP (1) task_key
            FROM dw.dim_task AS task
            WHERE (task.task_template_id = dta.task_template_id)
               OR (dta.task_template_id IS NULL AND task.task_template_id IS NULL AND task.domain_id = dta.domain_id AND LOWER(LTRIM(RTRIM(task.task_title))) = LOWER(LTRIM(RTRIM(dta.task_title))))
            ORDER BY task.task_key DESC
        ) AS dtask
        LEFT JOIN dw.dim_score_scale AS dss
            ON dss.score_scale_id = dta.score_scale_id
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.score_scales AS ss_src
            ON ss_src.id = dta.score_scale_id
           AND ss_src.is_valid = 1
        LEFT JOIN dw.dim_assessment_status AS das
            ON das.assessment_status_code = LOWER(LTRIM(RTRIM(ta.assessment_status)))
        LEFT JOIN dw.dim_no_score_reason AS dnsr
            ON dnsr.no_score_reason_id = ta.no_score_reason_id
        OUTER APPLY (
            SELECT TOP (1) status
            FROM Stg_ProgramOps_DB.stg_program_ops.child_daily_status AS cds0
            WHERE cds0.is_valid = 1
              AND cds0.child_id = ta.child_id
              AND cds0.[date] = ta.[date]
            ORDER BY cds0.stg_row_id DESC
        ) AS cds
        OUTER APPLY (
            SELECT TOP (1) status
            FROM Stg_ProgramOps_DB.stg_program_ops.center_daily_status AS cends0
            WHERE cends0.is_valid = 1
              AND cends0.center_id = COALESCE(sess.center_id, dc.center_id)
              AND cends0.[date] = ta.[date]
            ORDER BY cends0.stg_row_id DESC
        ) AS cends
        WHERE COALESCE(ta.source_updated_at, ta.updated_at, ta.created_at, ta.extracted_at) >= @start_time
          AND COALESCE(ta.source_updated_at, ta.updated_at, ta.created_at, ta.extracted_at) <  @end_time;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_fact_tran_student_task_progress;

        TRUNCATE TABLE dw.fact_tran_student_task_progress;
        DBCC CHECKIDENT ('dw.fact_tran_student_task_progress', RESEED, 0) WITH NO_INFOMSGS;

        INSERT INTO dw.fact_tran_student_task_progress
            (date_key, child_key, center_key, teacher_key, domain_key, task_key,
             score_scale_key, assessment_status_key, no_score_reason_key,
             attempt_no, raw_score, normalized_score,
             is_completed, is_planned, is_scored, is_not_scored, is_cancelled,
             is_incomplete, is_refused, is_absent, is_center_closed, is_assessed,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_child_task_plan_id,
             source_system, etl_batch_id, loaded_at)
        SELECT
             date_key, child_key, center_key, teacher_key, domain_key, task_key,
             score_scale_key, assessment_status_key, no_score_reason_key,
             attempt_no, raw_score, normalized_score,
             is_completed, is_planned, is_scored, is_not_scored, is_cancelled,
             is_incomplete, is_refused, is_absent, is_center_closed, is_assessed,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_child_task_plan_id,
             source_system, @etl_batch_id, SYSDATETIME()
        FROM etl_work.w_fact_tran_student_task_progress;

        SET @rows_inserted = @@ROWCOUNT;
        SET @message = N'First-load transaction fact. Target truncated. Rows appended from etl_work.w_fact_tran_student_task_progress.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments/task_assessments',
             N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments/task_assessments',
                 N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_child_task_event
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_fact_child_task_event;

        INSERT INTO etl_work.w_fact_child_task_event
            (child_key, task_key, teacher_key, center_key, domain_key, date_key,
             event_type, event_status, raw_score, normalized_score,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_system)
        SELECT
            ft.child_key,
            ft.task_key,
            ft.teacher_key,
            ft.center_key,
            ft.domain_key,
            ft.date_key,
            CASE WHEN ft.source_task_assessment_id IS NOT NULL THEN N'ASSESSMENT' ELSE N'PLAN' END,
            CASE
                WHEN ft.source_task_assessment_id IS NOT NULL AND das.assessment_status_code IS NOT NULL THEN das.assessment_status_code
                WHEN ft.is_cancelled = 1 THEN N'CANCELLED'
                WHEN ft.is_absent = 1 THEN N'ABSENT'
                WHEN ft.is_refused = 1 THEN N'REFUSED'
                WHEN ft.is_incomplete = 1 THEN N'INCOMPLETE'
                WHEN ft.is_completed = 1 THEN N'COMPLETED'
                WHEN ft.is_scored = 1 THEN N'SCORED'
                WHEN ft.is_not_scored = 1 THEN N'NOT_SCORED'
                WHEN ft.is_assessed = 1 THEN N'ASSESSED'
                WHEN ft.is_planned = 1 THEN N'PLANNED'
                ELSE N'UNKNOWN'
            END,
            ft.raw_score,
            ft.normalized_score,
            ft.source_daily_task_assignment_id,
            ft.source_task_assessment_id,
            ft.source_assessment_session_id,
            ft.source_system
        FROM dw.fact_tran_student_task_progress AS ft
        LEFT JOIN dw.dim_assessment_status AS das
            ON das.assessment_status_key = ft.assessment_status_key
        WHERE ft.source_daily_task_assignment_id IS NOT NULL
           OR ft.source_task_assessment_id IS NOT NULL;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_fact_child_task_event;

        TRUNCATE TABLE dw.fact_child_task_event;
        DBCC CHECKIDENT ('dw.fact_child_task_event', RESEED, 0) WITH NO_INFOMSGS;

        INSERT INTO dw.fact_child_task_event
            (child_key, task_key, teacher_key, center_key, domain_key, date_key,
             event_type, event_status, raw_score, normalized_score,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_system, etl_batch_id, loaded_at)
        SELECT
            child_key, task_key, teacher_key, center_key, domain_key, date_key,
            event_type, event_status, raw_score, normalized_score,
            source_daily_task_assignment_id, source_task_assessment_id,
            source_assessment_session_id, source_system, @etl_batch_id, SYSDATETIME()
        FROM etl_work.w_fact_child_task_event;

        SET @rows_inserted = @@ROWCOUNT;
        SET @message = N'First-load factless/event fact. Target truncated and event relations inserted.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
             N'Charity_DW_DB', N'dw', N'fact_child_task_event', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
                 N'Charity_DW_DB', N'dw', N'fact_child_task_event', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_daily_student_task_progress
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        DECLARE @current_snapshot_date DATE = CONVERT(DATE, @start_time);
        DECLARE @end_snapshot_date DATE = CONVERT(DATE, @end_time);
        DECLARE @current_date_key INT;

        TRUNCATE TABLE dw.fact_daily_student_task_progress;
        DBCC CHECKIDENT ('dw.fact_daily_student_task_progress', RESEED, 0) WITH NO_INFOMSGS;

        WHILE @current_snapshot_date < @end_snapshot_date
        BEGIN
            TRUNCATE TABLE etl_work.w_fact_daily_student_task_progress;

            SELECT @current_date_key = TimeKey
            FROM dw.dim_date
            WHERE FullDateAlternateKey = @current_snapshot_date;

            IF @current_date_key IS NULL SET @current_date_key = -1;

            INSERT INTO etl_work.w_fact_daily_student_task_progress
                (date_key, child_key, center_key, teacher_key,
                 raw_score, min_score, max_score, normalized_score,
                 planned_task_count, assessment_count, completed_task_count,
                 scored_task_count, not_scored_task_count, source_system)
            SELECT
                @current_date_key,
                COALESCE(ft.child_key, -1),
                COALESCE(ft.center_key, -1),
                COALESCE(ft.teacher_key, -1),
                CAST(AVG(CASE WHEN ft.is_scored = 1 AND ft.raw_score IS NOT NULL THEN ft.raw_score END) AS DECIMAL(10,2)),
                CAST(MIN(CASE WHEN ft.is_scored = 1 THEN dss.min_score END) AS DECIMAL(10,2)),
                CAST(MAX(CASE WHEN ft.is_scored = 1 THEN dss.max_score END) AS DECIMAL(10,2)),
                CAST(AVG(CASE WHEN ft.is_scored = 1 AND ft.normalized_score IS NOT NULL THEN ft.normalized_score END) AS DECIMAL(10,4)),
                COUNT(DISTINCT CASE WHEN ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                COUNT(DISTINCT CASE WHEN ft.source_task_assessment_id IS NOT NULL THEN ft.source_task_assessment_id END),
                COUNT(DISTINCT CASE WHEN ft.is_completed = 1 AND ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                COUNT(DISTINCT CASE WHEN ft.is_scored = 1 AND ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                COUNT(DISTINCT CASE WHEN ft.is_not_scored = 1 AND ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                N'PROGRAM_OPS'
            FROM dw.fact_tran_student_task_progress AS ft
            LEFT JOIN dw.dim_date AS tx_date
                ON tx_date.TimeKey = ft.date_key
            LEFT JOIN dw.dim_score_scale AS dss
                ON dss.score_scale_key = ft.score_scale_key
            WHERE tx_date.FullDateAlternateKey <= @current_snapshot_date
               OR COALESCE(ft.date_key, -1) = -1
            GROUP BY COALESCE(ft.child_key, -1), COALESCE(ft.center_key, -1), COALESCE(ft.teacher_key, -1);

            SET @rows_read += @@ROWCOUNT;

            INSERT INTO dw.fact_daily_student_task_progress
                (date_key, child_key, center_key, teacher_key,
                 raw_score, min_score, max_score, normalized_score,
                 planned_task_count, assessment_count, completed_task_count,
                 scored_task_count, not_scored_task_count,
                 source_system, etl_batch_id, loaded_at)
            SELECT
                date_key, child_key, center_key, teacher_key,
                raw_score, min_score, max_score, normalized_score,
                planned_task_count, assessment_count, completed_task_count,
                scored_task_count, not_scored_task_count,
                source_system, @etl_batch_id, SYSDATETIME()
            FROM etl_work.w_fact_daily_student_task_progress;

            SET @rows_inserted += @@ROWCOUNT;
            SET @current_snapshot_date = DATEADD(DAY, 1, @current_snapshot_date);
        END;

        SET @message = N'First-load daily snapshot fact. Daily loop created one row per date_key + child_key + center_key + teacher_key.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
             N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
                 N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @etl_batch_id   INT,
        @created_by     NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
        @started_at     DATETIME2(0) = SYSDATETIME(),
        @rows_read      INT = 0,
        @rows_inserted  INT = 0,
        @rows_updated   INT = 0,
        @rows_rejected  INT = 0,
        @message        NVARCHAR(MAX) = NULL;

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch
            (source_system, target_layer, mart_name, batch_status,
             started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
             created_by)
        VALUES
            (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
             @started_at, 0, 0, 0, 0, @created_by);

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


        TRUNCATE TABLE etl_work.w_fact_child_snapshot_old;
        TRUNCATE TABLE etl_work.w_fact_child_snapshot_period;
        TRUNCATE TABLE etl_work.w_fact_child_snapshot_final;

        INSERT INTO etl_work.w_fact_child_snapshot_period
            (snapshot_date_key, child_key, center_key, teacher_key,
             planned_task_count, assessment_count, completed_task_count, scored_task_count,
             first_plan_date_key, last_plan_date_key,
             first_assessment_date_key, last_assessment_date_key,
             source_system)
        SELECT
            latest.max_date_key,
            latest.child_key,
            latest.center_key,
            latest.teacher_key,
            ds.planned_task_count,
            ds.assessment_count,
            ds.completed_task_count,
            ds.scored_task_count,
            dates.first_plan_date_key,
            dates.last_plan_date_key,
            dates.first_assessment_date_key,
            dates.last_assessment_date_key,
            N'PROGRAM_OPS'
        FROM (
            SELECT child_key, center_key, teacher_key, MAX(date_key) AS max_date_key
            FROM dw.fact_daily_student_task_progress
            WHERE date_key >= CONVERT(INT, CONVERT(CHAR(8), CONVERT(DATE, @start_time), 112))
              AND date_key <  CONVERT(INT, CONVERT(CHAR(8), CONVERT(DATE, @end_time), 112))
            GROUP BY child_key, center_key, teacher_key
        ) AS latest
        INNER JOIN dw.fact_daily_student_task_progress AS ds
            ON ds.child_key = latest.child_key
           AND ds.center_key = latest.center_key
           AND ds.teacher_key = latest.teacher_key
           AND ds.date_key = latest.max_date_key
        INNER JOIN (
            SELECT
                child_key, center_key, teacher_key,
                MIN(CASE WHEN planned_task_count > 0 THEN date_key END) AS first_plan_date_key,
                MAX(CASE WHEN planned_task_count > 0 THEN date_key END) AS last_plan_date_key,
                MIN(CASE WHEN assessment_count > 0 THEN date_key END) AS first_assessment_date_key,
                MAX(CASE WHEN assessment_count > 0 THEN date_key END) AS last_assessment_date_key
            FROM dw.fact_daily_student_task_progress
            GROUP BY child_key, center_key, teacher_key
        ) AS dates
            ON dates.child_key = latest.child_key
           AND dates.center_key = latest.center_key
           AND dates.teacher_key = latest.teacher_key;

        INSERT INTO etl_work.w_fact_child_snapshot_final
        SELECT * FROM etl_work.w_fact_child_snapshot_period;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_fact_child_snapshot_final;

        TRUNCATE TABLE dw.fact_child_snapshot_accumulation;
        DBCC CHECKIDENT ('dw.fact_child_snapshot_accumulation', RESEED, 0) WITH NO_INFOMSGS;

        INSERT INTO dw.fact_child_snapshot_accumulation
            (snapshot_date_key, child_key, center_key, teacher_key,
             planned_task_count, assessment_count, completed_task_count, scored_task_count,
             first_plan_date_key, last_plan_date_key,
             first_assessment_date_key, last_assessment_date_key,
             source_system, etl_batch_id, loaded_at)
        SELECT
             snapshot_date_key, child_key, center_key, teacher_key,
             planned_task_count, assessment_count, completed_task_count, scored_task_count,
             first_plan_date_key, last_plan_date_key,
             first_assessment_date_key, last_assessment_date_key,
             source_system, @etl_batch_id, SYSDATETIME()
        FROM etl_work.w_fact_child_snapshot_final;

        SET @rows_inserted = @@ROWCOUNT;
        SET @message = N'First-load lifecycle fact rebuilt from fact_daily_student_task_progress through etl_work tables.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress',
             N'Charity_DW_DB', N'dw', N'fact_child_snapshot_accumulation', N'succeeded',
             @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
             @started_at, SYSDATETIME(), @message);

        UPDATE etl_admin.etl_batch
        SET
            batch_status  = N'succeeded',
            ended_at      = SYSDATETIME(),
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            rows_updated  = @rows_updated,
            rows_rejected = @rows_rejected,
            error_message = NULL
        WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

        IF @etl_batch_id IS NOT NULL
        BEGIN
            INSERT INTO etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress',
                 N'Charity_DW_DB', N'dw', N'fact_child_snapshot_accumulation', N'failed',
                 @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
                 @started_at, SYSDATETIME(), @message);

            UPDATE etl_admin.etl_batch
            SET
                batch_status  = N'failed',
                ended_at      = SYSDATETIME(),
                rows_read     = @rows_read,
                rows_inserted = @rows_inserted,
                rows_updated  = @rows_updated,
                rows_rejected = @rows_rejected,
                error_message = @message
            WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_run_dw_mart1_first_load
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;

    EXEC etl_admin.usp_first_load_dw_dim_center @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_domain @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_score_scale @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_no_score_reason @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_assessment_status @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_child @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_teacher @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_dim_task @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_tran_student_task_progress @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_child_task_event @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_daily_student_task_progress @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation @start_time = @start_time, @end_time = @end_time;
END;
GO

PRINT 'Created reworked first-load DW ETL procedures.';
GO


-- /*
-- ===============================================================================
--  Project      : Charity Data Warehouse Project
--  Phase        : Phase 3 - DW ETL Procedures
--  File         : 13_etl_dw_dim_center_procedures.sql
--  DBMS         : Microsoft SQL Server

--  Purpose:
--    Load dw.dim_center from Stg_ProgramOps_DB.stg_program_ops.centers.

--  Dimension Type:
--    dw.dim_center is treated as an SCD Type 2 dimension because it contains:
--      - effective_from
--      - effective_to
--      - is_current

--  Rules followed:
--    - Two procedures are provided:
--        1) etl_admin.usp_first_load_dw_dim_center
--        2) etl_admin.usp_incremental_load_dw_dim_center
--    - Both procedures receive @start_time and @end_time.
--    - Half-open range is used:
--        @start_time <= source_updated_at < @end_time
--    - Unknown row center_key = -1 is always preserved.
--    - No MERGE is used.
--    - No window functions are used.
--    - No WHILE loop is used for this dimension.
--    - Logging is written to Charity_DW_DB.etl_admin tables.
--    - etl_batch stores only final procedure-level summary.
--    - etl_load_log stores step-level details.

--  Important:
--    First-load deletes existing non-unknown rows from dw.dim_center. Use it only
--    before fact loading or when the related facts will be rebuilt.
-- ===============================================================================
-- */

-- SET NOCOUNT ON;
-- GO

-- USE Charity_DW_DB;
-- GO

-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_center
--   Type     : First-load SCD Type 2 dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_center
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#center_candidates') IS NOT NULL DROP TABLE #center_candidates;
--         IF OBJECT_ID('tempdb..#source_center') IS NOT NULL DROP TABLE #source_center;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             s.id,
--             s.name,
--             s.city,
--             s.address,
--             s.is_active,
--             s.created_at,
--             s.updated_at,
--             s.source_updated_at,
--             s.source_system,
--             s.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN s.is_valid <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN s.id IS NULL THEN N'center id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(s.name)), N'') IS NULL THEN N'center name missing; ' ELSE N'' END,
--                 CASE WHEN s.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #center_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.centers AS s
--         WHERE s.source_updated_at >= @start_time
--           AND s.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #center_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             c.id AS center_id,
--             NULLIF(LTRIM(RTRIM(c.name)), N'') AS center_name,
--             NULLIF(LTRIM(RTRIM(c.city)), N'') AS city,
--             NULLIF(LTRIM(RTRIM(c.address)), N'') AS address,
--             CASE
--                 WHEN c.is_active = 1 THEN N'active'
--                 WHEN c.is_active = 0 THEN N'inactive'
--                 ELSE N'unknown'
--             END AS center_status,
--             COALESCE(c.created_at, c.source_updated_at, @start_time) AS effective_from,
--             c.source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(NULLIF(LTRIM(RTRIM(c.name)), N''), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(c.city)), N''), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(c.address)), N''), N'<NULL>'),
--                 CASE
--                     WHEN c.is_active = 1 THEN N'active'
--                     WHEN c.is_active = 0 THEN N'inactive'
--                     ELSE N'unknown'
--                 END
--             )) AS row_hash
--         INTO #source_center
--         FROM #center_candidates AS c
--         WHERE c.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging centers', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_center WHERE center_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_center ON;

--             INSERT INTO dw.dim_center
--                 (center_key, center_id, center_name, city, address, center_status,
--                  effective_from, effective_to, is_current, source_system,
--                  row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', NULL, NULL, N'unknown',
--                  CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'PROGRAM_OPS',
--                  NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_center OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown center row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted center_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 3: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_center
--         WHERE center_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT (N'dw.dim_center', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - First-load reset dim_center', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' existing non-unknown rows and reseeded identity to 0. Deleted rows are not counted in final business summary.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert initial current SCD2 versions.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_center
--             (center_id, center_name, city, address, center_status,
--              effective_from, effective_to, is_current, source_system,
--              row_hash, created_at, updated_at)
--         SELECT
--             src.center_id,
--             src.center_name,
--             src.city,
--             src.address,
--             src.center_status,
--             src.effective_from,
--             NULL AS effective_to,
--             1 AS is_current,
--             COALESCE(src.source_system, N'PROGRAM_OPS') AS source_system,
--             src.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #source_center AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load center versions', N'succeeded',
--              (SELECT COUNT(*) FROM #source_center), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one current SCD2 version per valid source center.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers',
--             N'Charity_DW_DB', N'dw', N'dim_center',
--             sl.load_status,
--             sl.rows_read,
--             sl.rows_inserted,
--             sl.rows_updated,
--             sl.rows_rejected,
--             sl.started_at,
--             sl.ended_at,
--             CONCAT(sl.step_name, N'. ', sl.message)
--         FROM @step_log AS sl;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_center OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers',
--                  N'Charity_DW_DB', N'dw', N'dim_center', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_teacher
--   Type     : First-load SCD Type 2 dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_teacher
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#teacher_candidates') IS NOT NULL DROP TABLE #teacher_candidates;
--         IF OBJECT_ID('tempdb..#source_teacher') IS NOT NULL DROP TABLE #source_teacher;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             t.id,
--             t.center_id,
--             t.first_name,
--             t.last_name,
--             t.employment_status,
--             t.is_active,
--             t.created_at,
--             t.updated_at,
--             t.source_updated_at,
--             t.source_system,
--             t.is_valid,
--             dc.center_name AS dw_center_name,
--             sc.name AS stg_center_name,
--             NULLIF(CONCAT(
--                 CASE WHEN t.is_valid <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN t.id IS NULL THEN N'teacher id missing; ' ELSE N'' END,
--                 CASE WHEN t.center_id IS NULL THEN N'center_id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(t.first_name)), N'') IS NULL THEN N'first_name missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(t.last_name)), N'') IS NULL THEN N'last_name missing; ' ELSE N'' END,
--                 CASE WHEN t.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #teacher_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.teachers AS t
--         OUTER APPLY (
--             SELECT TOP (1) d.center_name
--             FROM dw.dim_center AS d
--             WHERE d.center_id = t.center_id
--               AND d.is_current = 1
--               AND d.center_key <> -1
--             ORDER BY d.center_key DESC
--         ) AS dc
--         OUTER APPLY (
--             SELECT TOP (1) c.name
--             FROM Stg_ProgramOps_DB.stg_program_ops.centers AS c
--             WHERE c.id = t.center_id
--             ORDER BY c.stg_row_id DESC
--         ) AS sc
--         WHERE t.source_updated_at >= @start_time
--           AND t.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #teacher_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             tc.id AS teacher_id,
--             NULLIF(LTRIM(RTRIM(tc.first_name)), N'') AS first_name,
--             NULLIF(LTRIM(RTRIM(tc.last_name)), N'') AS last_name,
--             CONCAT(
--                 NULLIF(LTRIM(RTRIM(tc.first_name)), N''),
--                 N' ',
--                 NULLIF(LTRIM(RTRIM(tc.last_name)), N'')
--             ) AS full_name,
--             tc.center_id,
--             COALESCE(
--                 NULLIF(LTRIM(RTRIM(tc.dw_center_name)), N''),
--                 NULLIF(LTRIM(RTRIM(tc.stg_center_name)), N''),
--                 N'Unknown'
--             ) AS center_name,
--             CASE
--                 WHEN tc.is_active = 0 THEN N'inactive'
--                 WHEN NULLIF(LTRIM(RTRIM(tc.employment_status)), N'') IS NOT NULL THEN LOWER(NULLIF(LTRIM(RTRIM(tc.employment_status)), N''))
--                 WHEN tc.is_active = 1 THEN N'active'
--                 ELSE N'unknown'
--             END AS employment_status,
--             COALESCE(tc.created_at, tc.source_updated_at, @start_time) AS effective_from,
--             tc.source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(NULLIF(LTRIM(RTRIM(tc.first_name)), N''), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(tc.last_name)), N''), N'<NULL>'),
--                 ISNULL(CONCAT(
--                     NULLIF(LTRIM(RTRIM(tc.first_name)), N''),
--                     N' ',
--                     NULLIF(LTRIM(RTRIM(tc.last_name)), N'')
--                 ), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(MAX), tc.center_id), N'<NULL>'),
--                 ISNULL(COALESCE(
--                     NULLIF(LTRIM(RTRIM(tc.dw_center_name)), N''),
--                     NULLIF(LTRIM(RTRIM(tc.stg_center_name)), N''),
--                     N'Unknown'
--                 ), N'<NULL>'),
--                 CASE
--                     WHEN tc.is_active = 0 THEN N'inactive'
--                     WHEN NULLIF(LTRIM(RTRIM(tc.employment_status)), N'') IS NOT NULL THEN LOWER(NULLIF(LTRIM(RTRIM(tc.employment_status)), N''))
--                     WHEN tc.is_active = 1 THEN N'active'
--                     ELSE N'unknown'
--                 END
--             )) AS row_hash
--         INTO #source_teacher
--         FROM #teacher_candidates AS tc
--         WHERE tc.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging teachers', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= teacher source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_teacher WHERE teacher_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_teacher ON;

--             INSERT INTO dw.dim_teacher
--                 (teacher_key, teacher_id, first_name, last_name, full_name,
--                  center_id, center_name, employment_status,
--                  effective_from, effective_to, is_current, source_system,
--                  row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, NULL, NULL, N'Unknown',
--                  NULL, NULL, N'unknown',
--                  CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'PROGRAM_OPS',
--                  NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_teacher OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown teacher row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted teacher_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 3: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_teacher
--         WHERE teacher_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT (N'dw.dim_teacher', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - First-load reset dim_teacher', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' existing non-unknown rows and reseeded identity to 0. Deleted rows are not counted in final business summary.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert initial current SCD2 versions.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_teacher
--             (teacher_id, first_name, last_name, full_name,
--              center_id, center_name, employment_status,
--              effective_from, effective_to, is_current, source_system,
--              row_hash, created_at, updated_at)
--         SELECT
--             src.teacher_id,
--             src.first_name,
--             src.last_name,
--             src.full_name,
--             src.center_id,
--             src.center_name,
--             src.employment_status,
--             src.effective_from,
--             NULL AS effective_to,
--             1 AS is_current,
--             COALESCE(src.source_system, N'PROGRAM_OPS') AS source_system,
--             src.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #source_teacher AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load teacher versions', N'succeeded',
--              (SELECT COUNT(*) FROM #source_teacher), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one current SCD2 version per valid source teacher.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers',
--             N'Charity_DW_DB', N'dw', N'dim_teacher',
--             sl.load_status,
--             sl.rows_read,
--             sl.rows_inserted,
--             sl.rows_updated,
--             sl.rows_rejected,
--             sl.started_at,
--             sl.ended_at,
--             CONCAT(sl.step_name, N'. ', sl.message)
--         FROM @step_log AS sl;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_teacher OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers',
--                  N'Charity_DW_DB', N'dw', N'dim_teacher', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO



-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_child
--   Type     : First-load SCD Type 1 dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_child
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#child_candidates') IS NOT NULL DROP TABLE #child_candidates;
--         IF OBJECT_ID('tempdb..#source_child') IS NOT NULL DROP TABLE #source_child;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             c.id,
--             c.center_id,
--             c.first_name,
--             c.last_name,
--             c.birth_date,
--             c.gender,
--             c.enrollment_date,
--             c.status,
--             c.created_at,
--             c.updated_at,
--             c.source_updated_at,
--             c.source_system,
--             c.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN c.is_valid <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN c.id IS NULL THEN N'child id missing; ' ELSE N'' END,
--                 CASE WHEN c.center_id IS NULL THEN N'center_id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(c.first_name)), N'') IS NULL THEN N'first_name missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(c.last_name)), N'') IS NULL THEN N'last_name missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(c.status)), N'') IS NULL THEN N'status missing; ' ELSE N'' END,
--                 CASE WHEN c.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #child_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.children AS c
--         WHERE c.source_updated_at >= @start_time
--           AND c.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #child_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             cc.id AS child_id,
--             NULLIF(LTRIM(RTRIM(cc.first_name)), N'') AS first_name,
--             NULLIF(LTRIM(RTRIM(cc.last_name)), N'') AS last_name,
--             CONCAT(
--                 NULLIF(LTRIM(RTRIM(cc.first_name)), N''),
--                 N' ',
--                 NULLIF(LTRIM(RTRIM(cc.last_name)), N'')
--             ) AS full_name,
--             cc.birth_date,
--             NULLIF(LTRIM(RTRIM(cc.gender)), N'') AS gender,
--             cc.center_id,
--             LOWER(NULLIF(LTRIM(RTRIM(cc.status)), N'')) AS status,
--             cc.enrollment_date,
--             cc.source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(NULLIF(LTRIM(RTRIM(cc.first_name)), N''), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(cc.last_name)), N''), N'<NULL>'),
--                 ISNULL(CONCAT(
--                     NULLIF(LTRIM(RTRIM(cc.first_name)), N''),
--                     N' ',
--                     NULLIF(LTRIM(RTRIM(cc.last_name)), N'')
--                 ), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), cc.birth_date, 126), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(cc.gender)), N''), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(MAX), cc.center_id), N'<NULL>'),
--                 ISNULL(LOWER(NULLIF(LTRIM(RTRIM(cc.status)), N'')), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), cc.enrollment_date, 126), N'<NULL>')
--             )) AS row_hash
--         INTO #source_child
--         FROM #child_candidates AS cc
--         WHERE cc.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging children', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= child source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_child WHERE child_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_child ON;

--             INSERT INTO dw.dim_child
--                 (child_key, child_id, first_name, last_name, full_name,
--                  birth_date, gender, center_id, status, enrollment_date,
--                  source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, NULL, NULL, N'Unknown',
--                  NULL, NULL, NULL, N'unknown', NULL,
--                  N'PROGRAM_OPS', NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_child OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown child row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted child_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 3: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_child
--         WHERE child_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT (N'dw.dim_child', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - First-load reset dim_child', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' existing non-unknown rows and reseeded identity to 0. Deleted rows are not counted in final business summary.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert first-load SCD1 child rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_child
--             (child_id, first_name, last_name, full_name,
--              birth_date, gender, center_id, status, enrollment_date,
--              source_system, row_hash, created_at, updated_at)
--         SELECT
--             src.child_id,
--             src.first_name,
--             src.last_name,
--             src.full_name,
--             src.birth_date,
--             src.gender,
--             src.center_id,
--             src.status,
--             src.enrollment_date,
--             COALESCE(src.source_system, N'PROGRAM_OPS') AS source_system,
--             src.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #source_child AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load children', N'succeeded',
--              (SELECT COUNT(*) FROM #source_child), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one SCD1 dimension row per valid source child.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'children',
--             N'Charity_DW_DB', N'dw', N'dim_child',
--             sl.load_status,
--             sl.rows_read,
--             sl.rows_inserted,
--             sl.rows_updated,
--             sl.rows_rejected,
--             sl.started_at,
--             sl.ended_at,
--             CONCAT(sl.step_name, N'. ', sl.message)
--         FROM @step_log AS sl;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_child OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Stg_ProgramOps_DB', N'stg_program_ops', N'children',
--                  N'Charity_DW_DB', N'dw', N'dim_child', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO



-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_domain
--   Type     : First-load SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_domain
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#domain_candidates') IS NOT NULL DROP TABLE #domain_candidates;
--         IF OBJECT_ID('tempdb..#source_domain') IS NOT NULL DROP TABLE #source_domain;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             d.id,
--             d.name,
--             d.description,
--             d.is_active,
--             d.created_at,
--             d.updated_at,
--             d.source_updated_at,
--             d.source_system,
--             d.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(d.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN d.id IS NULL THEN N'domain id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(d.name)), N'') IS NULL THEN N'domain name missing; ' ELSE N'' END,
--                 CASE WHEN d.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #domain_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.domains AS d
--         WHERE d.source_updated_at >= @start_time
--           AND d.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #domain_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             dc.id AS domain_id,
--             NULLIF(LTRIM(RTRIM(dc.name)), N'') AS domain_name,
--             NULLIF(LTRIM(RTRIM(dc.description)), N'') AS domain_description,
--             CASE
--                 WHEN dc.is_active = 1 THEN N'active'
--                 WHEN dc.is_active = 0 THEN N'inactive'
--                 ELSE N'unknown'
--             END AS domain_status,
--             dc.source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(NULLIF(LTRIM(RTRIM(dc.name)), N''), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(dc.description)), N''), N'<NULL>'),
--                 ISNULL(CASE
--                     WHEN dc.is_active = 1 THEN N'active'
--                     WHEN dc.is_active = 0 THEN N'inactive'
--                     ELSE N'unknown'
--                 END, N'<NULL>')
--             )) AS row_hash
--         INTO #source_domain
--         FROM #domain_candidates AS dc
--         WHERE dc.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging domains', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= domain source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_domain WHERE domain_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_domain ON;

--             INSERT INTO dw.dim_domain
--                 (domain_key, domain_id, domain_name, domain_description, domain_status,
--                  source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', NULL, N'unknown',
--                  N'PROGRAM_OPS', NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_domain OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown domain row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted domain_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 3: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_domain
--         WHERE domain_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT (N'dw.dim_domain', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - First-load reset dim_domain', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' existing non-unknown rows and reseeded identity to 0. Deleted rows are not counted in final business summary.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert first-load SCD1 domain rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_domain
--             (domain_id, domain_name, domain_description, domain_status,
--              source_system, row_hash, created_at, updated_at)
--         SELECT
--             src.domain_id,
--             src.domain_name,
--             src.domain_description,
--             src.domain_status,
--             COALESCE(src.source_system, N'PROGRAM_OPS') AS source_system,
--             src.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #source_domain AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load domains', N'succeeded',
--              (SELECT COUNT(*) FROM #source_domain), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one SCD1/reference dimension row per valid source domain.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains',
--             N'Charity_DW_DB', N'dw', N'dim_domain',
--             sl.load_status,
--             sl.rows_read,
--             sl.rows_inserted,
--             sl.rows_updated,
--             sl.rows_rejected,
--             sl.started_at,
--             sl.ended_at,
--             CONCAT(sl.step_name, N'. ', sl.message)
--         FROM @step_log AS sl;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_domain OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains',
--                  N'Charity_DW_DB', N'dw', N'dim_domain', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO

-- /*
-- ===============================================================================
--  Project      : Charity Data Warehouse Project
--  Phase        : Phase 3 - DW ETL Procedures
--  File         : 17_etl_dw_dim_task_procedures.sql
--  DBMS         : Microsoft SQL Server

--  Purpose:
--    Load dw.dim_task from Stg_ProgramOps_DB.stg_program_ops task sources.

--  Dimension Type:
--    dw.dim_task is treated as an SCD Type 1 / reference dimension because it does
--    not contain:
--      - effective_from
--      - effective_to
--      - is_current
--      - version_number

--  Grain:
--    One row per analytical task business key.

--    1) Template-based task:
--         Natural key = task_template_id

--    2) Non-template/custom task:
--         Natural key = domain_id + normalized task_title
--         task_template_id remains NULL and is_template_based = 0.

--  Why multiple staging sources are used:
--    - stg_program_ops.task_templates contains official reusable task templates.
--    - stg_program_ops.child_task_plans and stg_program_ops.daily_task_assignments
--      can contain task titles with task_template_id IS NULL. These are custom
--      task titles and must also be loaded into dim_task so later fact loads do
--      not unnecessarily resolve to task_key = -1.

--  Rules followed:
--    - Two procedures are provided:
--        1) etl_admin.usp_first_load_dw_dim_task
--        2) etl_admin.usp_incremental_load_dw_dim_task
--    - Both procedures receive @start_time and @end_time.
--    - Half-open range is used:
--        @start_time <= source_time < @end_time
--    - Unknown row task_key = -1 is always preserved.
--    - No MERGE is used.
--    - No window functions are used.
--    - No WHILE loop is used for this dimension.
--    - Logging is written to Charity_DW_DB.etl_admin tables.
--    - etl_batch stores only final procedure-level summary.
--    - etl_load_log stores step-level details.

--  Important:
--    The hash used here is calculated from the actual DW descriptive attributes.
--    It does not reuse staging row_hash directly because dim_task is derived from
--    more than one staging source and also denormalizes domain_name from dim_domain.
-- ===============================================================================
-- */

-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_task
--   Type     : First-load SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_task
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id       INT,
--         @created_by         NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started  DATETIME2(0) = SYSDATETIME(),
--         @step_started       DATETIME2(0),
--         @rows_read          INT = 0,
--         @rows_inserted      INT = 0,
--         @rows_updated       INT = 0,
--         @rows_rejected      INT = 0,
--         @unknown_inserted   INT = 0,
--         @rows_deleted       INT = 0,
--         @error_message      NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#task_candidates') IS NOT NULL DROP TABLE #task_candidates;
--         IF OBJECT_ID('tempdb..#source_task') IS NOT NULL DROP TABLE #source_task;

--         /*---------------------------------------------------------------------
--           Step 1: Read source candidates from staging and validate them.
--                   First load uses the requested half-open source_updated_at range.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         CREATE TABLE #task_candidates
--         (
--             candidate_source      NVARCHAR(40)  NOT NULL,
--             task_template_id      INT           NULL,
--             task_title            NVARCHAR(300) NULL,
--             domain_id             INT           NULL,
--             task_description      NVARCHAR(MAX) NULL,
--             is_template_based     BIT           NULL,
--             task_status           NVARCHAR(30)  NULL,
--             source_system         NVARCHAR(100) NULL,
--             source_updated_at     DATETIME2(0)  NULL,
--             is_valid              BIT           NULL,
--             validation_message    NVARCHAR(MAX) NULL
--         );

--         INSERT INTO #task_candidates
--             (candidate_source, task_template_id, task_title, domain_id, task_description,
--              is_template_based, task_status, source_system, source_updated_at, is_valid,
--              validation_message)
--         SELECT
--             N'task_templates' AS candidate_source,
--             tt.id AS task_template_id,
--             NULLIF(LTRIM(RTRIM(tt.title)), N'') AS task_title,
--             tt.domain_id,
--             NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), tt.description))), N'') AS task_description,
--             CAST(1 AS BIT) AS is_template_based,
--             CASE
--                 WHEN tt.is_active = 1 THEN N'active'
--                 WHEN tt.is_active = 0 THEN N'inactive'
--                 ELSE N'unknown'
--             END AS task_status,
--             tt.source_system,
--             tt.source_updated_at,
--             tt.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(tt.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN tt.id IS NULL THEN N'task_template_id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(tt.title)), N'') IS NULL THEN N'task title missing; ' ELSE N'' END,
--                 CASE WHEN tt.domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END,
--                 CASE WHEN tt.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         FROM Stg_ProgramOps_DB.stg_program_ops.task_templates AS tt
--         WHERE tt.source_updated_at >= @start_time
--           AND tt.source_updated_at <  @end_time;

--         INSERT INTO #task_candidates
--             (candidate_source, task_template_id, task_title, domain_id, task_description,
--              is_template_based, task_status, source_system, source_updated_at, is_valid,
--              validation_message)
--         SELECT
--             N'child_task_plans' AS candidate_source,
--             NULL AS task_template_id,
--             NULLIF(LTRIM(RTRIM(ctp.task_title)), N'') AS task_title,
--             ctp.domain_id,
--             NULL AS task_description,
--             CAST(0 AS BIT) AS is_template_based,
--             CASE
--                 WHEN ctp.is_active = 1 THEN N'active'
--                 WHEN ctp.is_active = 0 THEN N'inactive'
--                 ELSE N'unknown'
--             END AS task_status,
--             ctp.source_system,
--             ctp.source_updated_at,
--             ctp.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(ctp.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(ctp.task_title)), N'') IS NULL THEN N'task title missing; ' ELSE N'' END,
--                 CASE WHEN ctp.domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END,
--                 CASE WHEN ctp.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         FROM Stg_ProgramOps_DB.stg_program_ops.child_task_plans AS ctp
--         WHERE ctp.task_template_id IS NULL
--           AND ctp.source_updated_at >= @start_time
--           AND ctp.source_updated_at <  @end_time;

--         INSERT INTO #task_candidates
--             (candidate_source, task_template_id, task_title, domain_id, task_description,
--              is_template_based, task_status, source_system, source_updated_at, is_valid,
--              validation_message)
--         SELECT
--             N'daily_task_assignments' AS candidate_source,
--             NULL AS task_template_id,
--             NULLIF(LTRIM(RTRIM(dta.task_title)), N'') AS task_title,
--             dta.domain_id,
--             NULL AS task_description,
--             CAST(0 AS BIT) AS is_template_based,
--             N'active' AS task_status,
--             dta.source_system,
--             dta.source_updated_at,
--             dta.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(dta.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(dta.task_title)), N'') IS NULL THEN N'task title missing; ' ELSE N'' END,
--                 CASE WHEN dta.domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END,
--                 CASE WHEN dta.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--         WHERE dta.task_template_id IS NULL
--           AND dta.source_updated_at >= @start_time
--           AND dta.source_updated_at <  @end_time;

--         SELECT @rows_read = COUNT(*)
--         FROM #task_candidates;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #task_candidates
--         WHERE validation_message IS NOT NULL;

--         /*---------------------------------------------------------------------
--           Step 2: Normalize and collapse candidates to the dim_task grain.
--         ---------------------------------------------------------------------*/
--         SELECT
--             tc.task_template_id,
--             tc.task_title,
--             tc.domain_id,
--             dd.domain_name,
--             tc.is_template_based,
--             MAX(CONVERT(NVARCHAR(MAX), tc.task_description)) AS task_description,
--             CASE
--                 WHEN MAX(CASE WHEN tc.task_status = N'active' THEN 1 ELSE 0 END) = 1 THEN N'active'
--                 WHEN MAX(CASE WHEN tc.task_status = N'inactive' THEN 1 ELSE 0 END) = 1 THEN N'inactive'
--                 ELSE N'unknown'
--             END AS task_status,
--             COALESCE(MAX(tc.source_system), N'PROGRAM_OPS') AS source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(CONVERT(NVARCHAR(30), tc.task_template_id), N'<NULL>'),
--                 ISNULL(tc.task_title, N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), tc.domain_id), N'<NULL>'),
--                 ISNULL(MAX(dd.domain_name), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tc.is_template_based), N'<NULL>'),
--                 ISNULL(MAX(CONVERT(NVARCHAR(MAX), tc.task_description)), N'<NULL>'),
--                 CASE
--                     WHEN MAX(CASE WHEN tc.task_status = N'active' THEN 1 ELSE 0 END) = 1 THEN N'active'
--                     WHEN MAX(CASE WHEN tc.task_status = N'inactive' THEN 1 ELSE 0 END) = 1 THEN N'inactive'
--                     ELSE N'unknown'
--                 END
--             )) AS row_hash
--         INTO #source_task
--         FROM #task_candidates AS tc
--         LEFT JOIN dw.dim_domain AS dd
--             ON dd.domain_id = tc.domain_id
--            AND dd.domain_key <> -1
--         WHERE tc.validation_message IS NULL
--         GROUP BY
--             tc.task_template_id,
--             tc.task_title,
--             tc.domain_id,
--             dd.domain_name,
--             tc.is_template_based;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read, validate, and normalize staging tasks', N'succeeded',
--              @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded task candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126),
--                     N' <= source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126),
--                     N'. Template tasks use task_template_id as natural key; custom tasks use domain_id + task_title.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 3: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_task WHERE task_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_task ON;

--             INSERT INTO dw.dim_task
--                 (task_key, task_template_id, task_title, domain_id, domain_name,
--                  is_template_based, task_description, task_status, source_system,
--                  row_hash, created_at, updated_at)
--             VALUES
--                 (-1, NULL, N'Unknown', NULL, NULL,
--                  0, NULL, N'unknown', N'PROGRAM_OPS',
--                  NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_task OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown task row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted task_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 4: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_task
--         WHERE task_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT ('dw.dim_task', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Reset dim_task for first load', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted, N' existing non-unknown rows and reseeded identity to 0.'));

--         /*---------------------------------------------------------------------
--           Step 5: Insert first-load rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_task
--             (task_template_id, task_title, domain_id, domain_name,
--              is_template_based, task_description, task_status, source_system,
--              row_hash, created_at, updated_at)
--         SELECT
--             st.task_template_id,
--             st.task_title,
--             st.domain_id,
--             st.domain_name,
--             st.is_template_based,
--             st.task_description,
--             st.task_status,
--             st.source_system,
--             st.row_hash,
--             SYSDATETIME(),
--             NULL
--         FROM #source_task AS st;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load dim_task rows', N'succeeded', 0, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Inserted normalized task dimension rows.');

--         COMMIT TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 6: Persist step-level logs.
--         ---------------------------------------------------------------------*/
--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB',
--             N'stg_program_ops',
--             N'task_templates, child_task_plans, daily_task_assignments',
--             N'Charity_DW_DB',
--             N'dw',
--             N'dim_task',
--             load_status,
--             rows_read,
--             rows_inserted,
--             rows_updated,
--             rows_rejected,
--             started_at,
--             ended_at,
--             message
--         FROM @step_log;

--         UPDATE etl_admin.etl_batch
--         SET batch_status  = N'succeeded',
--             ended_at       = SYSDATETIME(),
--             rows_read      = @rows_read,
--             rows_inserted  = @rows_inserted,
--             rows_updated   = @rows_updated,
--             rows_rejected  = @rows_rejected,
--             error_message  = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF XACT_STATE() <> 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_task OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops',
--                  N'task_templates, child_task_plans, daily_task_assignments',
--                  N'Charity_DW_DB', N'dw', N'dim_task', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET batch_status  = N'failed',
--                 ended_at       = SYSDATETIME(),
--                 rows_read      = @rows_read,
--                 rows_inserted  = @rows_inserted,
--                 rows_updated   = @rows_updated,
--                 rows_rejected  = @rows_rejected,
--                 error_message  = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO

-- /*
-- ===============================================================================
--  Project      : Charity Data Warehouse Project
--  Phase        : Phase 3 - DW ETL Procedures
--  File         : 18_etl_dw_dim_score_scale_procedures.sql
--  DBMS         : Microsoft SQL Server

--  Purpose:
--    Load dw.dim_score_scale from Stg_ProgramOps_DB.stg_program_ops.score_scales.

--  Dimension Type:
--    dw.dim_score_scale is treated as an SCD Type 1 / small reference dimension
--    because it does not contain:
--      - effective_from
--      - effective_to
--      - is_current
--      - version_number

--  Grain:
--    One row per score scale business key.
--    Attribute changes overwrite the existing dimension row.

--  SCD1 attributes handled:
--      - scale_name
--      - min_score
--      - max_score
--      - scale_description
--      - scale_status

--  Rules followed:
--    - Two procedures are provided:
--        1) etl_admin.usp_first_load_dw_dim_score_scale
--        2) etl_admin.usp_incremental_load_dw_dim_score_scale
--    - Both procedures receive @start_time and @end_time.
--    - Half-open range is used:
--        @start_time <= source_time < @end_time
--    - Unknown row score_scale_key = -1 is always preserved.
--    - No MERGE is used.
--    - No window functions are used.
--    - No WHILE loop is used for this dimension.
--    - Logging is written to Charity_DW_DB.etl_admin tables.
--    - etl_batch stores only final procedure-level summary.
--    - etl_load_log stores step-level details.

--  Important:
--    The hash used here is calculated from the DW business attributes only.
--    It does not reuse the staging row_hash directly because staging hashes may
--    include source-only technical columns such as created_at / updated_at that
--    are not stored as descriptive attributes in dim_score_scale.
-- ===============================================================================
-- */

-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_score_scale
--   Type     : First-load SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_score_scale
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#score_scale_candidates') IS NOT NULL DROP TABLE #score_scale_candidates;
--         IF OBJECT_ID('tempdb..#source_score_scale') IS NOT NULL DROP TABLE #source_score_scale;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             ss.id,
--             ss.name,
--             ss.min_score,
--             ss.max_score,
--             ss.description,
--             ss.is_active,
--             ss.created_at,
--             ss.updated_at,
--             ss.source_updated_at,
--             ss.source_system,
--             ss.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(ss.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN ss.id IS NULL THEN N'score scale id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(ss.name)), N'') IS NULL THEN N'score scale name missing; ' ELSE N'' END,
--                 CASE WHEN ss.min_score IS NULL THEN N'min_score missing; ' ELSE N'' END,
--                 CASE WHEN ss.max_score IS NULL THEN N'max_score missing; ' ELSE N'' END,
--                 CASE WHEN ss.min_score IS NOT NULL AND ss.max_score IS NOT NULL AND ss.min_score > ss.max_score THEN N'min_score greater than max_score; ' ELSE N'' END,
--                 CASE WHEN ss.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #score_scale_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.score_scales AS ss
--         WHERE ss.source_updated_at >= @start_time
--           AND ss.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #score_scale_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             ssc.id AS score_scale_id,
--             NULLIF(LTRIM(RTRIM(ssc.name)), N'') AS scale_name,
--             ssc.min_score,
--             ssc.max_score,
--             NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), ssc.description))), N'') AS scale_description,
--             CASE
--                 WHEN ssc.is_active = 1 THEN N'active'
--                 WHEN ssc.is_active = 0 THEN N'inactive'
--                 ELSE N'unknown'
--             END AS scale_status,
--             ssc.source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(NULLIF(LTRIM(RTRIM(ssc.name)), N''), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(50), ssc.min_score), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(50), ssc.max_score), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), ssc.description))), N''), N'<NULL>'),
--                 ISNULL(CASE
--                     WHEN ssc.is_active = 1 THEN N'active'
--                     WHEN ssc.is_active = 0 THEN N'inactive'
--                     ELSE N'unknown'
--                 END, N'<NULL>')
--             )) AS row_hash
--         INTO #source_score_scale
--         FROM #score_scale_candidates AS ssc
--         WHERE ssc.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging score scales', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= score_scales source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_score_scale WHERE score_scale_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_score_scale ON;

--             INSERT INTO dw.dim_score_scale
--                 (score_scale_key, score_scale_id, scale_name, min_score, max_score,
--                  scale_description, scale_status, source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', NULL, NULL,
--                  NULL, N'unknown', N'PROGRAM_OPS', NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_score_scale OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown score scale row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted score_scale_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 3: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_score_scale
--         WHERE score_scale_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT (N'dw.dim_score_scale', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - First-load reset dim_score_scale', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' existing non-unknown rows and reseeded identity to 0. Deleted rows are not counted in final business summary.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert first-load SCD1 score scale rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_score_scale
--             (score_scale_id, scale_name, min_score, max_score,
--              scale_description, scale_status, source_system, row_hash, created_at, updated_at)
--         SELECT
--             src.score_scale_id,
--             src.scale_name,
--             src.min_score,
--             src.max_score,
--             src.scale_description,
--             src.scale_status,
--             COALESCE(src.source_system, N'PROGRAM_OPS') AS source_system,
--             src.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #source_score_scale AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load score scales', N'succeeded',
--              (SELECT COUNT(*) FROM #source_score_scale), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one SCD1/reference dimension row per valid source score scale.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales',
--             N'Charity_DW_DB', N'dw', N'dim_score_scale',
--             sl.load_status,
--             sl.rows_read,
--             sl.rows_inserted,
--             sl.rows_updated,
--             sl.rows_rejected,
--             sl.started_at,
--             sl.ended_at,
--             CONCAT(sl.step_name, N'. ', sl.message)
--         FROM @step_log AS sl;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_score_scale OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales',
--                  N'Charity_DW_DB', N'dw', N'dim_score_scale', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*
-- ===============================================================================
--  Project      : Charity Data Warehouse Project
--  Phase        : Phase 3 - DW ETL Procedures
--  File         : 19_etl_dw_dim_assessment_status_procedures.sql
--  DBMS         : Microsoft SQL Server

--  Purpose:
--    Load dw.dim_assessment_status from distinct assessment_status values in
--    Stg_ProgramOps_DB.stg_program_ops.task_assessments.

--  Dimension Type:
--    dw.dim_assessment_status is treated as an SCD Type 1 / static reference
--    dimension because it does not contain:
--      - effective_from
--      - effective_to
--      - is_current
--      - version_number

--  Grain:
--    One row per normalized assessment status code.

--  Source:
--    There is no separate source lookup table for assessment status. The dimension
--    is built from distinct assessment_status values found in task assessments.

--  SCD1 attributes handled:
--      - assessment_status_title
--      - assessment_status_category
--      - is_successful_assessment
--      - is_failure_assessment

--  Rules followed:
--    - Two procedures are provided:
--        1) etl_admin.usp_first_load_dw_dim_assessment_status
--        2) etl_admin.usp_incremental_load_dw_dim_assessment_status
--    - Both procedures receive @start_time and @end_time.
--    - Half-open range is used:
--        @start_time <= source_time < @end_time
--    - Unknown row assessment_status_key = -1 is always preserved.
--    - No MERGE is used.
--    - No window functions are used.
--    - No WHILE loop is used for this dimension.
--    - Logging is written to Charity_DW_DB.etl_admin tables.
--    - etl_batch stores only final procedure-level summary.
--    - etl_load_log stores step-level details.

--  Important:
--    Since dw.dim_assessment_status has no row_hash column, change detection is
--    performed by comparing the actual descriptive attributes.
-- ===============================================================================
-- */

-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_assessment_status
--   Type     : First-load SCD Type 1 / static reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_assessment_status
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#assessment_status_candidates') IS NOT NULL DROP TABLE #assessment_status_candidates;
--         IF OBJECT_ID('tempdb..#source_assessment_status') IS NOT NULL DROP TABLE #source_assessment_status;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             ta.assessment_status,
--             ta.source_updated_at,
--             ta.source_system,
--             ta.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(ta.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(ta.assessment_status)), N'') IS NULL THEN N'assessment_status missing; ' ELSE N'' END,
--                 CASE WHEN ta.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #assessment_status_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments AS ta
--         WHERE ta.source_updated_at >= @start_time
--           AND ta.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #assessment_status_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) AS assessment_status_code,
--             MIN(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) AS assessment_status_title,
--             CASE
--                 WHEN LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) IN
--                      (N'completed', N'complete', N'scored', N'assessed', N'done',
--                       N'success', N'successful', N'pass', N'passed')
--                     THEN N'success'
--                 WHEN LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) IN
--                      (N'failed', N'failure', N'fail', N'not_scored', N'no_score',
--                       N'cancelled', N'canceled', N'incomplete', N'refused', N'absent',
--                       N'center_closed', N'teacher_absent', N'not_completed')
--                     THEN N'failure'
--                 WHEN LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) IN
--                      (N'planned', N'open', N'pending', N'started', N'in_progress')
--                     THEN N'in_progress'
--                 ELSE N'other'
--             END AS assessment_status_category,
--             CONVERT(BIT, CASE
--                 WHEN LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) IN
--                      (N'completed', N'complete', N'scored', N'assessed', N'done',
--                       N'success', N'successful', N'pass', N'passed')
--                     THEN 1
--                 ELSE 0
--             END) AS is_successful_assessment,
--             CONVERT(BIT, CASE
--                 WHEN LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N'')) IN
--                      (N'failed', N'failure', N'fail', N'not_scored', N'no_score',
--                       N'cancelled', N'canceled', N'incomplete', N'refused', N'absent',
--                       N'center_closed', N'teacher_absent', N'not_completed')
--                     THEN 1
--                 ELSE 0
--             END) AS is_failure_assessment,
--             MIN(COALESCE(ascn.source_system, N'PROGRAM_OPS')) AS source_system
--         INTO #source_assessment_status
--         FROM #assessment_status_candidates AS ascn
--         WHERE ascn.validation_message IS NULL
--         GROUP BY LOWER(NULLIF(LTRIM(RTRIM(ascn.assessment_status)), N''));

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging assessment statuses', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= task_assessments source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'. Built distinct normalized status codes from valid rows.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and is preserved.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_assessment_status WHERE assessment_status_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_assessment_status ON;

--             INSERT INTO dw.dim_assessment_status
--                 (assessment_status_key, assessment_status_code, assessment_status_title,
--                  assessment_status_category, is_successful_assessment, is_failure_assessment,
--                  source_system, created_at, updated_at)
--             VALUES
--                 (-1, N'unknown', N'Unknown', N'unknown', 0, 0,
--                  N'PROGRAM_OPS', SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_assessment_status OFF;
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown assessment status row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted assessment_status_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 3: First-load reset. Keep unknown row only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_assessment_status
--         WHERE assessment_status_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT (N'dw.dim_assessment_status', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - First-load reset dim_assessment_status', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' existing non-unknown rows and reseeded identity to 0. Deleted rows are not counted in final business summary.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert first-load SCD1/static reference rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_assessment_status
--             (assessment_status_code, assessment_status_title, assessment_status_category,
--              is_successful_assessment, is_failure_assessment,
--              source_system, created_at, updated_at)
--         SELECT
--             src.assessment_status_code,
--             src.assessment_status_title,
--             src.assessment_status_category,
--             src.is_successful_assessment,
--             src.is_failure_assessment,
--             COALESCE(src.source_system, N'PROGRAM_OPS') AS source_system,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #source_assessment_status AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load assessment statuses', N'succeeded',
--              (SELECT COUNT(*) FROM #source_assessment_status), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one SCD1/static reference dimension row per valid normalized assessment status code.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments',
--             N'Charity_DW_DB', N'dw', N'dim_assessment_status',
--             sl.load_status,
--             sl.rows_read,
--             sl.rows_inserted,
--             sl.rows_updated,
--             sl.rows_rejected,
--             sl.started_at,
--             sl.ended_at,
--             CONCAT(sl.step_name, N'. ', sl.message)
--         FROM @step_log AS sl;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0
--             ROLLBACK TRANSACTION;

--         BEGIN TRY
--             SET IDENTITY_INSERT dw.dim_assessment_status OFF;
--         END TRY
--         BEGIN CATCH
--         END CATCH;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments',
--                  N'Charity_DW_DB', N'dw', N'dim_assessment_status', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*
-- ===============================================================================
--  Project      : Charity Data Warehouse Project
--  Phase        : Phase 3 - DW ETL Procedures
--  File         : 20_etl_dw_dim_no_score_reason_procedures.sql
--  DBMS         : Microsoft SQL Server

--  Purpose:
--    Load dw.dim_no_score_reason from
--    Stg_ProgramOps_DB.stg_program_ops.no_score_reasons.

--  Dimension Type:
--    dw.dim_no_score_reason is treated as an SCD Type 1 / static reference
--    dimension because it does not contain:
--      - effective_from
--      - effective_to
--      - is_current
--      - version_number

--  Grain:
--    One row per no-score reason business key.
--    Attribute changes overwrite the existing dimension row.

--  Source Mapping Notes:
--    The operational source stores only id, title, description, and is_active.
--    The DW dimension also contains reason_category and related-party flags.
--    Because those columns are not explicit in the source, this ETL derives them
--    deterministically from title/description using conservative keyword rules.
--    If the source system later adds explicit category/flag columns, replace only
--    the derivation block in #source_no_score_reason.

--  Procedures:
--    1) etl_admin.usp_first_load_dw_dim_no_score_reason
--    2) etl_admin.usp_incremental_load_dw_dim_no_score_reason

--  Rules followed:
--    - Both procedures receive @start_time and @end_time.
--    - Half-open range is used:
--        @start_time <= source_updated_at < @end_time
--    - Unknown row no_score_reason_key = -1 is always preserved.
--    - No MERGE is used.
--    - No window functions are used.
--    - No WHILE loop is used for this dimension.
--    - Logging is written to Charity_DW_DB.etl_admin tables.
--    - etl_batch stores only final procedure-level summary.
--    - etl_load_log stores step-level details.
-- ===============================================================================
-- */
-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_dim_no_score_reason
--   Type     : First-load SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_no_score_reason
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id      INT,
--         @created_by        NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started DATETIME2(0) = SYSDATETIME(),
--         @step_started      DATETIME2(0),
--         @rows_read         INT = 0,
--         @rows_inserted     INT = 0,
--         @rows_updated      INT = 0,
--         @rows_rejected     INT = 0,
--         @unknown_inserted  INT = 0,
--         @rows_deleted      INT = 0,
--         @error_message     NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#no_score_reason_candidates') IS NOT NULL DROP TABLE #no_score_reason_candidates;
--         IF OBJECT_ID('tempdb..#source_no_score_reason') IS NOT NULL DROP TABLE #source_no_score_reason;

--         /*---------------------------------------------------------------------
--           Step 1: Read and validate source candidates from staging.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             nsr.id,
--             nsr.title,
--             nsr.description,
--             nsr.is_active,
--             nsr.created_at,
--             nsr.updated_at,
--             nsr.source_updated_at,
--             nsr.source_system,
--             nsr.is_valid,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(nsr.is_valid, 0) <> 1 THEN N'is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN nsr.id IS NULL THEN N'no_score_reason id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(nsr.title)), N'') IS NULL THEN N'reason title missing; ' ELSE N'' END,
--                 CASE WHEN nsr.source_updated_at IS NULL THEN N'source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         INTO #no_score_reason_candidates
--         FROM Stg_ProgramOps_DB.stg_program_ops.no_score_reasons AS nsr
--         WHERE nsr.source_updated_at >= @start_time
--           AND nsr.source_updated_at <  @end_time;

--         SET @rows_read = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #no_score_reason_candidates
--         WHERE validation_message IS NOT NULL;

--         SELECT
--             c.id AS no_score_reason_id,
--             NULLIF(LTRIM(RTRIM(c.title)), N'') AS reason_title,
--             NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), c.description))), N'') AS reason_description,
--             CASE
--                 WHEN c.is_active = 0 THEN N'inactive'
--                 WHEN t.reason_text LIKE N'%teacher%' OR t.reason_text LIKE N'%instructor%'
--                   OR t.reason_text LIKE N'%معلم%' OR t.reason_text LIKE N'%مربی%' THEN N'teacher'
--                 WHEN t.reason_text LIKE N'%center%' OR t.reason_text LIKE N'%centre%'
--                   OR t.reason_text LIKE N'%closed%' OR t.reason_text LIKE N'%closure%'
--                   OR t.reason_text LIKE N'%holiday%'
--                   OR t.reason_text LIKE N'%مرکز%' OR t.reason_text LIKE N'%تعطیل%' THEN N'center'
--                 WHEN t.reason_text LIKE N'%system%' OR t.reason_text LIKE N'%technical%'
--                   OR t.reason_text LIKE N'%device%' OR t.reason_text LIKE N'%network%'
--                   OR t.reason_text LIKE N'%سیستم%' OR t.reason_text LIKE N'%خطا%' OR t.reason_text LIKE N'%فنی%' THEN N'system'
--                 WHEN t.reason_text LIKE N'%child%' OR t.reason_text LIKE N'%student%'
--                   OR t.reason_text LIKE N'%absent%' OR t.reason_text LIKE N'%absence%'
--                   OR t.reason_text LIKE N'%refus%' OR t.reason_text LIKE N'%ill%'
--                   OR t.reason_text LIKE N'%کودک%' OR t.reason_text LIKE N'%دانش%' OR t.reason_text LIKE N'%غایب%' THEN N'child'
--                 ELSE N'other'
--             END AS reason_category,
--             CONVERT(BIT, CASE
--                 WHEN t.reason_text LIKE N'%child%' OR t.reason_text LIKE N'%student%'
--                   OR t.reason_text LIKE N'%absent%' OR t.reason_text LIKE N'%absence%'
--                   OR t.reason_text LIKE N'%refus%' OR t.reason_text LIKE N'%ill%'
--                   OR t.reason_text LIKE N'%کودک%' OR t.reason_text LIKE N'%دانش%' OR t.reason_text LIKE N'%غایب%'
--                 THEN 1 ELSE 0 END) AS is_child_related,
--             CONVERT(BIT, CASE
--                 WHEN t.reason_text LIKE N'%teacher%' OR t.reason_text LIKE N'%instructor%'
--                   OR t.reason_text LIKE N'%معلم%' OR t.reason_text LIKE N'%مربی%'
--                 THEN 1 ELSE 0 END) AS is_teacher_related,
--             CONVERT(BIT, CASE
--                 WHEN t.reason_text LIKE N'%center%' OR t.reason_text LIKE N'%centre%'
--                   OR t.reason_text LIKE N'%closed%' OR t.reason_text LIKE N'%closure%'
--                   OR t.reason_text LIKE N'%holiday%'
--                   OR t.reason_text LIKE N'%مرکز%' OR t.reason_text LIKE N'%تعطیل%'
--                 THEN 1 ELSE 0 END) AS is_center_related,
--             CONVERT(BIT, CASE
--                 WHEN t.reason_text LIKE N'%system%' OR t.reason_text LIKE N'%technical%'
--                   OR t.reason_text LIKE N'%device%' OR t.reason_text LIKE N'%network%'
--                   OR t.reason_text LIKE N'%سیستم%' OR t.reason_text LIKE N'%خطا%' OR t.reason_text LIKE N'%فنی%'
--                 THEN 1 ELSE 0 END) AS is_system_related,
--             c.source_system,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 ISNULL(NULLIF(LTRIM(RTRIM(c.title)), N''), N'<NULL>'),
--                 ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), c.description))), N''), N'<NULL>'),
--                 ISNULL(CASE
--                     WHEN c.is_active = 0 THEN N'inactive'
--                     WHEN t.reason_text LIKE N'%teacher%' OR t.reason_text LIKE N'%instructor%'
--                       OR t.reason_text LIKE N'%معلم%' OR t.reason_text LIKE N'%مربی%' THEN N'teacher'
--                     WHEN t.reason_text LIKE N'%center%' OR t.reason_text LIKE N'%centre%'
--                       OR t.reason_text LIKE N'%closed%' OR t.reason_text LIKE N'%closure%'
--                       OR t.reason_text LIKE N'%holiday%'
--                       OR t.reason_text LIKE N'%مرکز%' OR t.reason_text LIKE N'%تعطیل%' THEN N'center'
--                     WHEN t.reason_text LIKE N'%system%' OR t.reason_text LIKE N'%technical%'
--                       OR t.reason_text LIKE N'%device%' OR t.reason_text LIKE N'%network%'
--                       OR t.reason_text LIKE N'%سیستم%' OR t.reason_text LIKE N'%خطا%' OR t.reason_text LIKE N'%فنی%' THEN N'system'
--                     WHEN t.reason_text LIKE N'%child%' OR t.reason_text LIKE N'%student%'
--                       OR t.reason_text LIKE N'%absent%' OR t.reason_text LIKE N'%absence%'
--                       OR t.reason_text LIKE N'%refus%' OR t.reason_text LIKE N'%ill%'
--                       OR t.reason_text LIKE N'%کودک%' OR t.reason_text LIKE N'%دانش%' OR t.reason_text LIKE N'%غایب%' THEN N'child'
--                     ELSE N'other'
--                 END, N'<NULL>'),
--                 CONVERT(NVARCHAR(1), CASE
--                     WHEN t.reason_text LIKE N'%child%' OR t.reason_text LIKE N'%student%'
--                       OR t.reason_text LIKE N'%absent%' OR t.reason_text LIKE N'%absence%'
--                       OR t.reason_text LIKE N'%refus%' OR t.reason_text LIKE N'%ill%'
--                       OR t.reason_text LIKE N'%کودک%' OR t.reason_text LIKE N'%دانش%' OR t.reason_text LIKE N'%غایب%'
--                     THEN 1 ELSE 0 END),
--                 CONVERT(NVARCHAR(1), CASE
--                     WHEN t.reason_text LIKE N'%teacher%' OR t.reason_text LIKE N'%instructor%'
--                       OR t.reason_text LIKE N'%معلم%' OR t.reason_text LIKE N'%مربی%'
--                     THEN 1 ELSE 0 END),
--                 CONVERT(NVARCHAR(1), CASE
--                     WHEN t.reason_text LIKE N'%center%' OR t.reason_text LIKE N'%centre%'
--                       OR t.reason_text LIKE N'%closed%' OR t.reason_text LIKE N'%closure%'
--                       OR t.reason_text LIKE N'%holiday%'
--                       OR t.reason_text LIKE N'%مرکز%' OR t.reason_text LIKE N'%تعطیل%'
--                     THEN 1 ELSE 0 END),
--                 CONVERT(NVARCHAR(1), CASE
--                     WHEN t.reason_text LIKE N'%system%' OR t.reason_text LIKE N'%technical%'
--                       OR t.reason_text LIKE N'%device%' OR t.reason_text LIKE N'%network%'
--                       OR t.reason_text LIKE N'%سیستم%' OR t.reason_text LIKE N'%خطا%' OR t.reason_text LIKE N'%فنی%'
--                     THEN 1 ELSE 0 END)
--             )) AS row_hash
--         INTO #source_no_score_reason
--         FROM #no_score_reason_candidates AS c
--         CROSS APPLY
--         (
--             SELECT LOWER(CONCAT_WS(N' ',
--                 ISNULL(c.title, N''),
--                 ISNULL(CONVERT(NVARCHAR(MAX), c.description), N'')
--             )) AS reason_text
--         ) AS t
--         WHERE c.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Read and validate staging no-score reasons', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded source candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= no_score_reasons source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Clear first-load rows, preserve/recreate unknown row, reset identity.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.dim_no_score_reason
--         WHERE no_score_reason_key <> -1;

--         SET @rows_deleted = @@ROWCOUNT;

--         IF NOT EXISTS (SELECT 1 FROM dw.dim_no_score_reason WHERE no_score_reason_key = -1)
--         BEGIN
--             SET IDENTITY_INSERT dw.dim_no_score_reason ON;

--             INSERT INTO dw.dim_no_score_reason
--                 (no_score_reason_key, no_score_reason_id, reason_title, reason_description,
--                  reason_category, is_child_related, is_teacher_related, is_center_related,
--                  is_system_related, source_system, row_hash, created_at, updated_at)
--             VALUES
--                 (-1, -1, N'Unknown', NULL,
--                  N'unknown', 0, 0, 0,
--                  0, N'PROGRAM_OPS', NULL, SYSDATETIME(), NULL);

--             SET @unknown_inserted = @@ROWCOUNT;

--             SET IDENTITY_INSERT dw.dim_no_score_reason OFF;
--         END;

--         DBCC CHECKIDENT ('dw.dim_no_score_reason', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Reset first-load target and preserve unknown row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted, N' existing positive-key rows; preserved or recreated no_score_reason_key = -1.'));

--         /*---------------------------------------------------------------------
--           Step 3: Insert all valid source rows for first load.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_no_score_reason
--             (no_score_reason_id, reason_title, reason_description,
--              reason_category, is_child_related, is_teacher_related, is_center_related,
--              is_system_related, source_system, row_hash, created_at, updated_at)
--         SELECT
--             src.no_score_reason_id,
--             src.reason_title,
--             src.reason_description,
--             src.reason_category,
--             src.is_child_related,
--             src.is_teacher_related,
--             src.is_center_related,
--             src.is_system_related,
--             src.source_system,
--             src.row_hash,
--             SYSDATETIME(),
--             NULL
--         FROM #source_no_score_reason AS src
--         WHERE NOT EXISTS
--         (
--             SELECT 1
--             FROM dw.dim_no_score_reason AS tgt
--             WHERE tgt.no_score_reason_key <> -1
--               AND tgt.no_score_reason_id = src.no_score_reason_id
--         );

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Insert first-load no-score reason rows', N'succeeded',
--              (SELECT COUNT(*) FROM #source_no_score_reason), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted valid source no-score reason rows into dw.dim_no_score_reason.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons',
--             N'Charity_DW_DB', N'dw', N'dim_no_score_reason',
--             load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--             started_at, ended_at, CONCAT(step_name, N': ', ISNULL(message, N''))
--         FROM @step_log;

--         UPDATE etl_admin.etl_batch
--         SET batch_status  = N'succeeded',
--             ended_at       = SYSDATETIME(),
--             rows_read      = @rows_read,
--             rows_inserted  = @rows_inserted,
--             rows_updated   = @rows_updated,
--             rows_rejected  = @rows_rejected,
--             error_message  = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

--         SET @error_message = ERROR_MESSAGE();

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             SELECT
--                 @etl_batch_id,
--                 N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons',
--                 N'Charity_DW_DB', N'dw', N'dim_no_score_reason',
--                 load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--                 started_at, ended_at, CONCAT(step_name, N': ', ISNULL(message, N''))
--             FROM @step_log;

--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons',
--                  N'Charity_DW_DB', N'dw', N'dim_no_score_reason', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET batch_status  = N'failed',
--                 ended_at       = SYSDATETIME(),
--                 rows_read      = @rows_read,
--                 rows_inserted  = @rows_inserted,
--                 rows_updated   = @rows_updated,
--                 rows_rejected  = @rows_rejected,
--                 error_message  = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*=============================================================================
--   Procedure: etl_admin.usp_first_load_dw_fact_tran_student_task_progress
--   Type     : First-load transaction fact load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_tran_student_task_progress
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id       INT,
--         @created_by         NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started  DATETIME2(0) = SYSDATETIME(),
--         @step_started       DATETIME2(0),
--         @rows_read          INT = 0,
--         @rows_inserted      INT = 0,
--         @rows_updated       INT = 0,
--         @rows_rejected      INT = 0,
--         @rows_deleted       INT = 0,
--         @error_message      NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = SCOPE_IDENTITY();

--         IF OBJECT_ID('tempdb..#affected_daily_task_assignment') IS NOT NULL DROP TABLE #affected_daily_task_assignment;
--         IF OBJECT_ID('tempdb..#fact_candidates') IS NOT NULL DROP TABLE #fact_candidates;
--         IF OBJECT_ID('tempdb..#source_fact') IS NOT NULL DROP TABLE #source_fact;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected daily task assignments for first load.
--                   The first load can use a broad range, but it still honors the
--                   required half-open source_updated_at interval.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT DISTINCT dta.id AS daily_task_assignment_id
--         INTO #affected_daily_task_assignment
--         FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--         WHERE dta.source_updated_at >= @start_time
--           AND dta.source_updated_at <  @end_time

--         UNION

--         SELECT DISTINCT ta.daily_task_assignment_id
--         FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments AS ta
--         WHERE ta.daily_task_assignment_id IS NOT NULL
--           AND ta.source_updated_at >= @start_time
--           AND ta.source_updated_at <  @end_time

--         UNION

--         SELECT DISTINCT dta.id
--         FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.task_assessments AS ta
--             ON ta.daily_task_assignment_id = dta.id
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.assessment_sessions AS sess
--             ON sess.id = ta.assessment_session_id
--         WHERE sess.source_updated_at >= @start_time
--           AND sess.source_updated_at <  @end_time

--         UNION

--         SELECT DISTINCT dta.id
--         FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.child_task_plans AS ctp
--             ON ctp.id = dta.child_task_plan_id
--         WHERE ctp.source_updated_at >= @start_time
--           AND ctp.source_updated_at <  @end_time

--         UNION

--         SELECT DISTINCT dta.id
--         FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.child_daily_status AS cds
--             ON cds.child_id = dta.child_id
--            AND cds.[date] = dta.[date]
--         WHERE cds.source_updated_at >= @start_time
--           AND cds.source_updated_at <  @end_time

--         UNION

--         SELECT DISTINCT dta.id
--         FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.children AS ch
--             ON ch.id = dta.child_id
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.center_daily_status AS cends
--             ON cends.center_id = ch.center_id
--            AND cends.[date] = dta.[date]
--         WHERE cends.source_updated_at >= @start_time
--           AND cends.source_updated_at <  @end_time;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Detect affected daily task assignments', N'succeeded',
--              (SELECT COUNT(*) FROM #affected_daily_task_assignment), 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected assignments using half-open source_updated_at range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126),
--                     N' <= source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         /*---------------------------------------------------------------------
--           Step 2: Build fact candidates.
--                   Planned rows come from daily_task_assignments.
--                   Assessment rows come from task_assessments.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         CREATE TABLE #fact_candidates
--         (
--             event_kind                       NVARCHAR(30)  NOT NULL,
--             business_date                    DATE          NULL,
--             child_id                         INT           NULL,
--             center_id                        INT           NULL,
--             teacher_id                       INT           NULL,
--             domain_id                        INT           NULL,
--             task_template_id                 INT           NULL,
--             task_title                       NVARCHAR(300) NULL,
--             score_scale_id                   INT           NULL,
--             dta_status                       NVARCHAR(50)  NULL,
--             assessment_status                NVARCHAR(50)  NULL,
--             no_score_reason_id               INT           NULL,
--             attempt_no                       INT           NULL,
--             raw_score                        DECIMAL(10,2) NULL,
--             normalized_score_source          DECIMAL(10,4) NULL,
--             source_daily_task_assignment_id  BIGINT        NULL,
--             source_task_assessment_id        BIGINT        NULL,
--             source_assessment_session_id     BIGINT        NULL,
--             source_child_task_plan_id        BIGINT        NULL,
--             source_system                    NVARCHAR(100) NULL,
--             validation_message               NVARCHAR(MAX) NULL
--         );

--         INSERT INTO #fact_candidates
--             (event_kind, business_date, child_id, center_id, teacher_id, domain_id,
--              task_template_id, task_title, score_scale_id, dta_status, assessment_status,
--              no_score_reason_id, attempt_no, raw_score, normalized_score_source,
--              source_daily_task_assignment_id, source_task_assessment_id,
--              source_assessment_session_id, source_child_task_plan_id, source_system,
--              validation_message)
--         SELECT
--             N'planned' AS event_kind,
--             dta.[date] AS business_date,
--             dta.child_id,
--             ch.center_id,
--             usr.teacher_id,
--             dta.domain_id,
--             dta.task_template_id,
--             NULLIF(LTRIM(RTRIM(dta.task_title)), N'') AS task_title,
--             dta.score_scale_id,
--             NULLIF(LTRIM(RTRIM(dta.status)), N'') AS dta_status,
--             NULL AS assessment_status,
--             NULL AS no_score_reason_id,
--             NULL AS attempt_no,
--             NULL AS raw_score,
--             NULL AS normalized_score_source,
--             CONVERT(BIGINT, dta.id) AS source_daily_task_assignment_id,
--             NULL AS source_task_assessment_id,
--             NULL AS source_assessment_session_id,
--             CONVERT(BIGINT, dta.child_task_plan_id) AS source_child_task_plan_id,
--             COALESCE(dta.source_system, N'PROGRAM_OPS') AS source_system,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(dta.is_valid, 0) <> 1 THEN N'daily_task_assignment is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN dta.id IS NULL THEN N'daily_task_assignment id missing; ' ELSE N'' END,
--                 CASE WHEN dta.child_id IS NULL THEN N'child_id missing; ' ELSE N'' END,
--                 CASE WHEN dta.[date] IS NULL THEN N'assignment date missing; ' ELSE N'' END,
--                 CASE WHEN dta.domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(dta.task_title)), N'') IS NULL THEN N'task_title missing; ' ELSE N'' END,
--                 CASE WHEN dta.score_scale_id IS NULL THEN N'score_scale_id missing; ' ELSE N'' END,
--                 CASE WHEN dta.source_updated_at IS NULL THEN N'daily_task_assignment source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         FROM #affected_daily_task_assignment AS aff
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--             ON dta.id = aff.daily_task_assignment_id
--         LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.children AS ch
--             ON ch.id = dta.child_id
--         LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.users AS usr
--             ON usr.id = dta.planned_by;

--         INSERT INTO #fact_candidates
--             (event_kind, business_date, child_id, center_id, teacher_id, domain_id,
--              task_template_id, task_title, score_scale_id, dta_status, assessment_status,
--              no_score_reason_id, attempt_no, raw_score, normalized_score_source,
--              source_daily_task_assignment_id, source_task_assessment_id,
--              source_assessment_session_id, source_child_task_plan_id, source_system,
--              validation_message)
--         SELECT
--             N'assessment' AS event_kind,
--             COALESCE(ta.[date], dta.[date]) AS business_date,
--             COALESCE(ta.child_id, dta.child_id) AS child_id,
--             COALESCE(sess.center_id, ch.center_id) AS center_id,
--             COALESCE(ta.teacher_id, sess.teacher_id) AS teacher_id,
--             dta.domain_id,
--             dta.task_template_id,
--             NULLIF(LTRIM(RTRIM(dta.task_title)), N'') AS task_title,
--             dta.score_scale_id,
--             NULLIF(LTRIM(RTRIM(dta.status)), N'') AS dta_status,
--             NULLIF(LTRIM(RTRIM(ta.assessment_status)), N'') AS assessment_status,
--             ta.no_score_reason_id,
--             ta.attempt_no,
--             ta.score AS raw_score,
--             COALESCE(
--                 ta.normalized_score,
--                 CASE
--                     WHEN ta.score IS NOT NULL
--                      AND ss.min_score IS NOT NULL
--                      AND ss.max_score IS NOT NULL
--                      AND ss.max_score > ss.min_score
--                     THEN CONVERT(DECIMAL(10,4), ((ta.score - ss.min_score) / NULLIF(ss.max_score - ss.min_score, 0)) * 100.0)
--                     ELSE NULL
--                 END
--             ) AS normalized_score_source,
--             CONVERT(BIGINT, dta.id) AS source_daily_task_assignment_id,
--             CONVERT(BIGINT, ta.id) AS source_task_assessment_id,
--             CONVERT(BIGINT, ta.assessment_session_id) AS source_assessment_session_id,
--             CONVERT(BIGINT, dta.child_task_plan_id) AS source_child_task_plan_id,
--             COALESCE(ta.source_system, dta.source_system, N'PROGRAM_OPS') AS source_system,
--             NULLIF(CONCAT(
--                 CASE WHEN ISNULL(dta.is_valid, 0) <> 1 THEN N'daily_task_assignment is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN ISNULL(ta.is_valid, 0) <> 1 THEN N'task_assessment is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN sess.id IS NOT NULL AND ISNULL(sess.is_valid, 0) <> 1 THEN N'assessment_session is_valid is not 1; ' ELSE N'' END,
--                 CASE WHEN dta.id IS NULL THEN N'daily_task_assignment id missing; ' ELSE N'' END,
--                 CASE WHEN ta.id IS NULL THEN N'task_assessment id missing; ' ELSE N'' END,
--                 CASE WHEN COALESCE(ta.child_id, dta.child_id) IS NULL THEN N'child_id missing; ' ELSE N'' END,
--                 CASE WHEN COALESCE(ta.[date], dta.[date]) IS NULL THEN N'assessment date missing; ' ELSE N'' END,
--                 CASE WHEN dta.domain_id IS NULL THEN N'domain_id missing; ' ELSE N'' END,
--                 CASE WHEN NULLIF(LTRIM(RTRIM(dta.task_title)), N'') IS NULL THEN N'task_title missing; ' ELSE N'' END,
--                 CASE WHEN dta.score_scale_id IS NULL THEN N'score_scale_id missing; ' ELSE N'' END,
--                 CASE WHEN ta.source_updated_at IS NULL THEN N'task_assessment source_updated_at missing; ' ELSE N'' END
--             ), N'') AS validation_message
--         FROM #affected_daily_task_assignment AS aff
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
--             ON dta.id = aff.daily_task_assignment_id
--         INNER JOIN Stg_ProgramOps_DB.stg_program_ops.task_assessments AS ta
--             ON ta.daily_task_assignment_id = dta.id
--         LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.assessment_sessions AS sess
--             ON sess.id = ta.assessment_session_id
--         LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.children AS ch
--             ON ch.id = COALESCE(ta.child_id, dta.child_id)
--         LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.score_scales AS ss
--             ON ss.id = dta.score_scale_id;

--         SET @rows_read = (SELECT COUNT(*) FROM #fact_candidates);

--         SELECT @rows_rejected = COUNT(*)
--         FROM #fact_candidates
--         WHERE validation_message IS NOT NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Build and validate fact candidates', N'succeeded',
--              @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              N'Built planned and assessed transaction candidates. Rejections are only critically invalid source rows; missing dimension lookups are handled with key -1.');

--         /*---------------------------------------------------------------------
--           Step 3: Resolve dimension keys and calculate fact hash.
--                   Missing lookups use -1 and are not rejected.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             COALESCE(dd.TimeKey, -1) AS date_key,
--             COALESCE(dchild.child_key, -1) AS child_key,
--             COALESCE(dcenter.center_key, -1) AS center_key,
--             COALESCE(dteacher.teacher_key, -1) AS teacher_key,
--             COALESCE(ddomain.domain_key, -1) AS domain_key,
--             COALESCE(dtask.task_key, -1) AS task_key,
--             COALESCE(dscore.score_scale_key, -1) AS score_scale_key,
--             COALESCE(dstatus.assessment_status_key, -1) AS assessment_status_key,
--             COALESCE(dreason.no_score_reason_key, -1) AS no_score_reason_key,
--             fc.attempt_no,
--             fc.raw_score,
--             fc.normalized_score_source AS normalized_score,
--             CASE
--                 WHEN LOWER(ISNULL(fc.dta_status, N'')) IN (N'completed', N'complete', N'done')
--                   OR LOWER(ISNULL(fc.assessment_status, N'')) IN (N'completed', N'complete', N'done', N'scored', N'success', N'successful')
--                   OR (fc.event_kind = N'assessment' AND fc.raw_score IS NOT NULL)
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_completed,
--             CONVERT(BIT, 1) AS is_planned,
--             CASE
--                 WHEN fc.event_kind = N'assessment'
--                  AND fc.raw_score IS NOT NULL
--                  AND fc.no_score_reason_id IS NULL
--                  AND LOWER(ISNULL(fc.assessment_status, N'')) NOT IN (N'not_scored', N'no_score', N'unscored', N'refused', N'absent', N'cancelled', N'canceled', N'incomplete')
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_scored,
--             CASE
--                 WHEN fc.event_kind = N'assessment'
--                  AND (
--                         fc.raw_score IS NULL
--                      OR fc.no_score_reason_id IS NOT NULL
--                      OR LOWER(ISNULL(fc.assessment_status, N'')) IN (N'not_scored', N'no_score', N'unscored', N'refused', N'absent', N'cancelled', N'canceled', N'incomplete')
--                  )
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_not_scored,
--             CASE
--                 WHEN LOWER(ISNULL(fc.dta_status, N'')) LIKE N'%cancel%'
--                   OR LOWER(ISNULL(fc.assessment_status, N'')) LIKE N'%cancel%'
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_cancelled,
--             CASE
--                 WHEN LOWER(ISNULL(fc.dta_status, N'')) LIKE N'%incomplete%'
--                   OR LOWER(ISNULL(fc.assessment_status, N'')) LIKE N'%incomplete%'
--                   OR LOWER(ISNULL(fc.assessment_status, N'')) IN (N'open', N'partial')
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_incomplete,
--             CASE
--                 WHEN LOWER(ISNULL(fc.assessment_status, N'')) LIKE N'%refus%'
--                   OR LOWER(ISNULL(fc.dta_status, N'')) LIKE N'%refus%'
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_refused,
--             CASE
--                 WHEN child_status.status IS NOT NULL
--                  AND (
--                         LOWER(child_status.status) LIKE N'%absent%'
--                      OR LOWER(child_status.status) LIKE N'%absence%'
--                      OR child_status.absence_reason_id IS NOT NULL
--                  )
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_absent,
--             CASE
--                 WHEN center_status.status IS NOT NULL
--                  AND (
--                         LOWER(center_status.status) LIKE N'%closed%'
--                      OR LOWER(center_status.status) LIKE N'%closure%'
--                      OR center_status.closure_reason_id IS NOT NULL
--                  )
--                 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0)
--             END AS is_center_closed,
--             CASE WHEN fc.event_kind = N'assessment' THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0) END AS is_assessed,
--             fc.source_daily_task_assignment_id,
--             fc.source_task_assessment_id,
--             fc.source_assessment_session_id,
--             fc.source_child_task_plan_id,
--             fc.source_system,
--             @etl_batch_id AS etl_batch_id,
--             SYSDATETIME() AS loaded_at,
--             HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 CONVERT(NVARCHAR(30), COALESCE(dd.TimeKey, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dchild.child_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dcenter.center_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dteacher.teacher_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(ddomain.domain_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dtask.task_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dscore.score_scale_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dstatus.assessment_status_key, -1)),
--                 CONVERT(NVARCHAR(30), COALESCE(dreason.no_score_reason_key, -1)),
--                 ISNULL(CONVERT(NVARCHAR(30), fc.attempt_no), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(50), fc.raw_score), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(50), fc.normalized_score_source), N'<NULL>'),
--                 CASE
--                     WHEN LOWER(ISNULL(fc.dta_status, N'')) IN (N'completed', N'complete', N'done')
--                       OR LOWER(ISNULL(fc.assessment_status, N'')) IN (N'completed', N'complete', N'done', N'scored', N'success', N'successful')
--                       OR (fc.event_kind = N'assessment' AND fc.raw_score IS NOT NULL)
--                     THEN N'1' ELSE N'0'
--                 END,
--                 N'1',
--                 CASE
--                     WHEN fc.event_kind = N'assessment'
--                      AND fc.raw_score IS NOT NULL
--                      AND fc.no_score_reason_id IS NULL
--                      AND LOWER(ISNULL(fc.assessment_status, N'')) NOT IN (N'not_scored', N'no_score', N'unscored', N'refused', N'absent', N'cancelled', N'canceled', N'incomplete')
--                     THEN N'1' ELSE N'0'
--                 END,
--                 CASE
--                     WHEN fc.event_kind = N'assessment'
--                      AND (
--                             fc.raw_score IS NULL
--                          OR fc.no_score_reason_id IS NOT NULL
--                          OR LOWER(ISNULL(fc.assessment_status, N'')) IN (N'not_scored', N'no_score', N'unscored', N'refused', N'absent', N'cancelled', N'canceled', N'incomplete')
--                      )
--                     THEN N'1' ELSE N'0'
--                 END,
--                 CASE WHEN LOWER(ISNULL(fc.dta_status, N'')) LIKE N'%cancel%' OR LOWER(ISNULL(fc.assessment_status, N'')) LIKE N'%cancel%' THEN N'1' ELSE N'0' END,
--                 CASE WHEN LOWER(ISNULL(fc.dta_status, N'')) LIKE N'%incomplete%' OR LOWER(ISNULL(fc.assessment_status, N'')) LIKE N'%incomplete%' OR LOWER(ISNULL(fc.assessment_status, N'')) IN (N'open', N'partial') THEN N'1' ELSE N'0' END,
--                 CASE WHEN LOWER(ISNULL(fc.assessment_status, N'')) LIKE N'%refus%' OR LOWER(ISNULL(fc.dta_status, N'')) LIKE N'%refus%' THEN N'1' ELSE N'0' END,
--                 CASE WHEN child_status.status IS NOT NULL AND (LOWER(child_status.status) LIKE N'%absent%' OR LOWER(child_status.status) LIKE N'%absence%' OR child_status.absence_reason_id IS NOT NULL) THEN N'1' ELSE N'0' END,
--                 CASE WHEN center_status.status IS NOT NULL AND (LOWER(center_status.status) LIKE N'%closed%' OR LOWER(center_status.status) LIKE N'%closure%' OR center_status.closure_reason_id IS NOT NULL) THEN N'1' ELSE N'0' END,
--                 CASE WHEN fc.event_kind = N'assessment' THEN N'1' ELSE N'0' END,
--                 ISNULL(CONVERT(NVARCHAR(30), fc.source_daily_task_assignment_id), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), fc.source_task_assessment_id), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), fc.source_assessment_session_id), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), fc.source_child_task_plan_id), N'<NULL>'),
--                 ISNULL(fc.source_system, N'<NULL>')
--             )) AS fact_row_hash
--         INTO #source_fact
--         FROM #fact_candidates AS fc
--         LEFT JOIN dw.dim_date AS dd
--             ON dd.FullDateAlternateKey = fc.business_date
--         LEFT JOIN dw.dim_child AS dchild
--             ON dchild.child_id = fc.child_id
--            AND dchild.child_key <> -1
--         OUTER APPLY
--         (
--             SELECT TOP (1) dc.center_key
--             FROM dw.dim_center AS dc
--             WHERE dc.center_id = fc.center_id
--               AND dc.center_key <> -1
--               AND CONVERT(DATETIME2(0), fc.business_date) >= dc.effective_from
--               AND (dc.effective_to IS NULL OR CONVERT(DATETIME2(0), fc.business_date) < dc.effective_to)
--             ORDER BY dc.is_current DESC, dc.effective_from DESC, dc.center_key DESC
--         ) AS dcenter
--         OUTER APPLY
--         (
--             SELECT TOP (1) dt.teacher_key
--             FROM dw.dim_teacher AS dt
--             WHERE dt.teacher_id = fc.teacher_id
--               AND dt.teacher_key <> -1
--               AND CONVERT(DATETIME2(0), fc.business_date) >= dt.effective_from
--               AND (dt.effective_to IS NULL OR CONVERT(DATETIME2(0), fc.business_date) < dt.effective_to)
--             ORDER BY dt.is_current DESC, dt.effective_from DESC, dt.teacher_key DESC
--         ) AS dteacher
--         LEFT JOIN dw.dim_domain AS ddomain
--             ON ddomain.domain_id = fc.domain_id
--            AND ddomain.domain_key <> -1
--         OUTER APPLY
--         (
--             SELECT TOP (1) dtk.task_key
--             FROM dw.dim_task AS dtk
--             WHERE dtk.task_key <> -1
--               AND (
--                     (fc.task_template_id IS NOT NULL
--                      AND dtk.is_template_based = 1
--                      AND dtk.task_template_id = fc.task_template_id)
--                  OR (fc.task_template_id IS NULL
--                      AND ISNULL(dtk.is_template_based, 0) = 0
--                      AND dtk.task_template_id IS NULL
--                      AND dtk.domain_id = fc.domain_id
--                      AND NULLIF(LTRIM(RTRIM(dtk.task_title)), N'') = fc.task_title)
--               )
--             ORDER BY dtk.task_key DESC
--         ) AS dtask
--         LEFT JOIN dw.dim_score_scale AS dscore
--             ON dscore.score_scale_id = fc.score_scale_id
--            AND dscore.score_scale_key <> -1
--         LEFT JOIN dw.dim_assessment_status AS dstatus
--             ON dstatus.assessment_status_code = LOWER(NULLIF(LTRIM(RTRIM(fc.assessment_status)), N''))
--            AND dstatus.assessment_status_key <> -1
--         LEFT JOIN dw.dim_no_score_reason AS dreason
--             ON dreason.no_score_reason_id = fc.no_score_reason_id
--            AND dreason.no_score_reason_key <> -1
--         OUTER APPLY
--         (
--             SELECT TOP (1)
--                 cds.status,
--                 cds.absence_reason_id
--             FROM Stg_ProgramOps_DB.stg_program_ops.child_daily_status AS cds
--             WHERE cds.child_id = fc.child_id
--               AND cds.[date] = fc.business_date
--               AND cds.is_valid = 1
--             ORDER BY cds.source_updated_at DESC, cds.stg_row_id DESC
--         ) AS child_status
--         OUTER APPLY
--         (
--             SELECT TOP (1)
--                 cends.status,
--                 cends.closure_reason_id
--             FROM Stg_ProgramOps_DB.stg_program_ops.center_daily_status AS cends
--             WHERE cends.center_id = fc.center_id
--               AND cends.[date] = fc.business_date
--               AND cends.is_valid = 1
--             ORDER BY cends.source_updated_at DESC, cends.stg_row_id DESC
--         ) AS center_status
--         WHERE fc.validation_message IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Resolve dimension keys', N'succeeded',
--              (SELECT COUNT(*) FROM #source_fact), 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Resolved dimension keys. Missing lookups were converted to -1 and were not rejected.');

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 4: First-load reset of the transaction fact.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.fact_tran_student_task_progress;
--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT ('dw.fact_tran_student_task_progress', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Reset fact_tran_student_task_progress for first load', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted, N' existing fact rows and reseeded identity to 0.'));

--         /*---------------------------------------------------------------------
--           Step 5: Insert first-load fact rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.fact_tran_student_task_progress
--             (date_key, child_key, center_key, teacher_key, domain_key, task_key,
--              score_scale_key, assessment_status_key, no_score_reason_key,
--              attempt_no, raw_score, normalized_score, is_completed, is_planned,
--              is_scored, is_not_scored, is_cancelled, is_incomplete, is_refused,
--              is_absent, is_center_closed, is_assessed,
--              source_daily_task_assignment_id, source_task_assessment_id,
--              source_assessment_session_id, source_child_task_plan_id,
--              source_system, etl_batch_id, loaded_at)
--         SELECT
--             date_key, child_key, center_key, teacher_key, domain_key, task_key,
--             score_scale_key, assessment_status_key, no_score_reason_key,
--             attempt_no, raw_score, normalized_score, is_completed, is_planned,
--             is_scored, is_not_scored, is_cancelled, is_incomplete, is_refused,
--             is_absent, is_center_closed, is_assessed,
--             source_daily_task_assignment_id, source_task_assessment_id,
--             source_assessment_session_id, source_child_task_plan_id,
--             source_system, etl_batch_id, loaded_at
--         FROM #source_fact;

--         SET @rows_inserted = @@ROWCOUNT;
--         SET @rows_updated = 0;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert first-load fact rows', N'succeeded',
--              0, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted first-load rows into dw.fact_tran_student_task_progress.');

--         COMMIT TRANSACTION;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Stg_ProgramOps_DB',
--             N'stg_program_ops',
--             N'daily_task_assignments/task_assessments',
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_tran_student_task_progress',
--             load_status,
--             rows_read,
--             rows_inserted,
--             rows_updated,
--             rows_rejected,
--             started_at,
--             ended_at,
--             CONCAT(step_name, N' - ', ISNULL(message, N''))
--         FROM @step_log;

--         UPDATE etl_admin.etl_batch
--         SET batch_status  = N'succeeded',
--             ended_at       = SYSDATETIME(),
--             rows_read      = @rows_read,
--             rows_inserted  = @rows_inserted,
--             rows_updated   = @rows_updated,
--             rows_rejected  = @rows_rejected,
--             error_message  = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

--         SET @error_message = CONCAT(
--             ERROR_MESSAGE(), N' | Procedure: ', COALESCE(ERROR_PROCEDURE(), N'unknown'),
--             N' | Line: ', ERROR_LINE()
--         );

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops',
--                  N'daily_task_assignments/task_assessments', N'Charity_DW_DB', N'dw',
--                  N'fact_tran_student_task_progress', N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET batch_status  = N'failed',
--                 ended_at       = SYSDATETIME(),
--                 rows_read      = @rows_read,
--                 rows_inserted  = @rows_inserted,
--                 rows_updated   = @rows_updated,
--                 rows_rejected  = @rows_rejected,
--                 error_message  = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*=============================================================================
--   First-load procedure
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_daily_student_task_progress
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id                  INT,
--         @created_by                    NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started             DATETIME2(0) = SYSDATETIME(),
--         @step_started                  DATETIME2(0),
--         @rows_read                     INT = 0,
--         @rows_inserted                 INT = 0,
--         @rows_updated                  INT = 0,
--         @rows_rejected                 INT = 0,
--         @rows_deleted                  INT = 0,
--         @snapshot_candidate_rows       INT = 0,
--         @transaction_history_rows_read INT = 0,
--         @day_source_rows_read          INT = 0,
--         @day_snapshot_rows             INT = 0,
--         @day_rows_inserted             INT = 0,
--         @missing_date_count            INT = 0,
--         @loop_day_count                INT = 0,
--         @current_snapshot_date         DATE,
--         @end_snapshot_date             DATE,
--         @current_date_key              INT,
--         @error_message                 NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 1: First-load reset.

--           This is a fact table, so there is no unknown row to preserve.
--           First load is allowed to be aggressive.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.fact_daily_student_task_progress;
--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT ('dw.fact_daily_student_task_progress', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Reset first-load daily snapshot fact', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' rows and reseeded daily_student_task_progress_key to 0.'));

--         /*---------------------------------------------------------------------
--           Step 2: REQUIRED DAILY WHILE LOOP.

--           Grain:
--               one row per date_key + child_key + center_key + teacher_key

--           For each day in the half-open range:
--               @start_time date <= snapshot_date < @end_time date

--           The row is created from all transaction fact history as of that day:
--               transaction_date <= snapshot_date

--           This means if values are the same as the previous day, a new row is
--           still created for the new date_key.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SET @current_snapshot_date = CONVERT(DATE, @start_time);
--         SET @end_snapshot_date     = CONVERT(DATE, @end_time);

--         WHILE @current_snapshot_date < @end_snapshot_date
--         BEGIN
--             SET @current_date_key = NULL;
--             SET @day_source_rows_read = 0;
--             SET @day_snapshot_rows = 0;
--             SET @day_rows_inserted = 0;

--             SELECT TOP (1)
--                 @current_date_key = TimeKey
--             FROM dw.dim_date
--             WHERE FullDateAlternateKey = @current_snapshot_date;

--             IF @current_date_key IS NULL
--             BEGIN
--                 SET @current_date_key = -1;
--                 SET @missing_date_count += 1;
--             END;

--             IF OBJECT_ID('tempdb..#day_snapshot') IS NOT NULL
--                 DROP TABLE #day_snapshot;

--             SELECT
--                 @day_source_rows_read = COUNT(1)
--             FROM dw.fact_tran_student_task_progress AS ft
--             LEFT JOIN dw.dim_date AS tx_date
--                 ON tx_date.TimeKey = ft.date_key
--             WHERE
--                 tx_date.FullDateAlternateKey <= @current_snapshot_date
--                 OR
--                 (
--                     COALESCE(ft.date_key, -1) = -1
--                     AND ft.loaded_at < DATEADD(DAY, 1, CAST(@current_snapshot_date AS DATETIME2(0)))
--                 );

--             SELECT
--                 @current_date_key AS date_key,
--                 agg.child_key,
--                 agg.center_key,
--                 agg.teacher_key,
--                 agg.raw_score,
--                 agg.min_score,
--                 agg.max_score,
--                 agg.normalized_score,
--                 agg.planned_task_count,
--                 agg.assessment_count,
--                 agg.completed_task_count,
--                 agg.scored_task_count,
--                 agg.not_scored_task_count,
--                 CAST(N'PROGRAM_OPS' AS NVARCHAR(100)) AS source_system
--             INTO #day_snapshot
--             FROM
--             (
--                 SELECT
--                     COALESCE(ft.child_key, -1)   AS child_key,
--                     COALESCE(ft.center_key, -1)  AS center_key,
--                     COALESCE(ft.teacher_key, -1) AS teacher_key,

--                     CAST(AVG(CASE
--                                  WHEN ft.is_scored = 1 AND ft.raw_score IS NOT NULL
--                                  THEN ft.raw_score
--                              END) AS DECIMAL(10,2)) AS raw_score,

--                     CAST(MIN(CASE
--                                  WHEN ft.is_scored = 1
--                                  THEN dss.min_score
--                              END) AS DECIMAL(10,2)) AS min_score,

--                     CAST(MAX(CASE
--                                  WHEN ft.is_scored = 1
--                                  THEN dss.max_score
--                              END) AS DECIMAL(10,2)) AS max_score,

--                     CAST(AVG(CASE
--                                  WHEN ft.is_scored = 1 AND ft.normalized_score IS NOT NULL
--                                  THEN ft.normalized_score
--                              END) AS DECIMAL(10,4)) AS normalized_score,

--                     COUNT(DISTINCT CASE
--                                        WHEN ft.source_daily_task_assignment_id IS NOT NULL
--                                        THEN ft.source_daily_task_assignment_id
--                                    END) AS planned_task_count,

--                     COUNT(DISTINCT CASE
--                                        WHEN ft.source_task_assessment_id IS NOT NULL
--                                        THEN ft.source_task_assessment_id
--                                    END) AS assessment_count,

--                     COUNT(DISTINCT CASE
--                                        WHEN ft.is_completed = 1
--                                             AND ft.source_daily_task_assignment_id IS NOT NULL
--                                        THEN ft.source_daily_task_assignment_id
--                                    END) AS completed_task_count,

--                     COUNT(DISTINCT CASE
--                                        WHEN ft.is_scored = 1
--                                             AND ft.source_daily_task_assignment_id IS NOT NULL
--                                        THEN ft.source_daily_task_assignment_id
--                                    END) AS scored_task_count,

--                     COUNT(DISTINCT CASE
--                                        WHEN ft.is_not_scored = 1
--                                             AND ft.source_daily_task_assignment_id IS NOT NULL
--                                        THEN ft.source_daily_task_assignment_id
--                                    END) AS not_scored_task_count
--                 FROM dw.fact_tran_student_task_progress AS ft
--                 LEFT JOIN dw.dim_date AS tx_date
--                     ON tx_date.TimeKey = ft.date_key
--                 LEFT JOIN dw.dim_score_scale AS dss
--                     ON dss.score_scale_key = ft.score_scale_key
--                 WHERE
--                     tx_date.FullDateAlternateKey <= @current_snapshot_date
--                     OR
--                     (
--                         COALESCE(ft.date_key, -1) = -1
--                         AND ft.loaded_at < DATEADD(DAY, 1, CAST(@current_snapshot_date AS DATETIME2(0)))
--                     )
--                 GROUP BY
--                     COALESCE(ft.child_key, -1),
--                     COALESCE(ft.center_key, -1),
--                     COALESCE(ft.teacher_key, -1)
--             ) AS agg;

--             SET @day_snapshot_rows = @@ROWCOUNT;
--             SET @transaction_history_rows_read += @day_source_rows_read;
--             SET @snapshot_candidate_rows += @day_snapshot_rows;

--             INSERT INTO dw.fact_daily_student_task_progress
--                 (date_key, child_key, center_key, teacher_key,
--                  raw_score, min_score, max_score, normalized_score,
--                  planned_task_count, assessment_count, completed_task_count,
--                  scored_task_count, not_scored_task_count,
--                  source_system, etl_batch_id, loaded_at)
--             SELECT
--                 src.date_key,
--                 src.child_key,
--                 src.center_key,
--                 src.teacher_key,
--                 src.raw_score,
--                 src.min_score,
--                 src.max_score,
--                 src.normalized_score,
--                 src.planned_task_count,
--                 src.assessment_count,
--                 src.completed_task_count,
--                 src.scored_task_count,
--                 src.not_scored_task_count,
--                 src.source_system,
--                 @etl_batch_id,
--                 SYSDATETIME()
--             FROM #day_snapshot AS src;

--             SET @day_rows_inserted = @@ROWCOUNT;
--             SET @rows_inserted += @day_rows_inserted;
--             SET @loop_day_count += 1;

--             SET @current_snapshot_date = DATEADD(DAY, 1, @current_snapshot_date);
--         END;

--         SET @rows_read = @snapshot_candidate_rows;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Build daily snapshots using WHILE loop', N'succeeded',
--              @rows_read, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'WHILE loop days processed: ', @loop_day_count,
--                     N'. Transaction history rows scanned across days: ', @transaction_history_rows_read,
--                     N'. Missing dim_date rows resolved to date_key = -1: ', @missing_date_count,
--                     N'. Grain: date_key + child_key + center_key + teacher_key.'));

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_tran_student_task_progress',
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_daily_student_task_progress',
--             load_status,
--             rows_read,
--             rows_inserted,
--             rows_updated,
--             rows_rejected,
--             started_at,
--             ended_at,
--             message
--         FROM @step_log;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;

--         COMMIT TRANSACTION;
--     END TRY
--     BEGIN CATCH
--         SET @error_message = CONCAT(
--             N'Error ', ERROR_NUMBER(),
--             N' at line ', ERROR_LINE(),
--             N': ', ERROR_MESSAGE()
--         );

--         IF XACT_STATE() <> 0
--             ROLLBACK TRANSACTION;

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
--                  N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress',
--                  N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*=============================================================================
--   First-load procedure
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id              INT,
--         @created_by                NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started         DATETIME2(0) = SYSDATETIME(),
--         @step_started              DATETIME2(0),
--         @rows_read                 INT = 0,
--         @rows_inserted             INT = 0,
--         @rows_updated              INT = 0,
--         @rows_rejected             INT = 0,
--         @rows_deleted              INT = 0,
--         @source_transaction_rows   INT = 0,
--         @lifecycle_candidate_rows  INT = 0,
--         @snapshot_date             DATE,
--         @snapshot_date_key         INT,
--         @error_message             NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 1: Reset first-load target table.

--           This is a fact table, so there is no unknown row to preserve.
--           First-load may be aggressive.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.fact_child_snapshot_accumulation;
--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT ('dw.fact_child_snapshot_accumulation', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Reset first-load child lifecycle fact', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' rows and reseeded child_snapshot_key to 0.'));

--         /*---------------------------------------------------------------------
--           Step 2: Resolve snapshot_date_key.

--           Because the ETL range is half-open, the snapshot represents the state
--           as of the last instant before @end_time.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SET @snapshot_date = CONVERT(DATE, DATEADD(SECOND, -1, @end_time));

--         SELECT TOP (1)
--             @snapshot_date_key = TimeKey
--         FROM dw.dim_date
--         WHERE FullDateAlternateKey = @snapshot_date;

--         IF @snapshot_date_key IS NULL
--             SET @snapshot_date_key = -1;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Resolve lifecycle snapshot date key', N'succeeded',
--              1, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Snapshot date = ', CONVERT(NVARCHAR(10), @snapshot_date, 120),
--                     N', snapshot_date_key = ', @snapshot_date_key,
--                     N'. If dim_date is missing, -1 is used.'));

--         /*---------------------------------------------------------------------
--           Step 3: Build lifecycle state from transaction fact.

--           First-load source window:
--               transaction_date >= @start_time date
--               transaction_date <  @end_time date

--           Grain:
--               child_key + center_key + teacher_key

--           Note:
--               Missing dimension keys are already represented as -1 by the
--               transaction fact ETL.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF OBJECT_ID('tempdb..#lifecycle_snapshot') IS NOT NULL
--             DROP TABLE #lifecycle_snapshot;

--         SELECT
--             @source_transaction_rows = COUNT(1)
--         FROM dw.fact_tran_student_task_progress AS ft
--         LEFT JOIN dw.dim_date AS tx_date
--             ON tx_date.TimeKey = ft.date_key
--         WHERE
--             (
--                 tx_date.FullDateAlternateKey >= CONVERT(DATE, @start_time)
--                 AND tx_date.FullDateAlternateKey <  CONVERT(DATE, @end_time)
--             )
--             OR
--             (
--                 COALESCE(ft.date_key, -1) = -1
--                 AND ft.loaded_at >= @start_time
--                 AND ft.loaded_at <  @end_time
--             );

--         SELECT
--             @snapshot_date_key AS snapshot_date_key,
--             agg.child_key,
--             agg.center_key,
--             agg.teacher_key,
--             agg.planned_task_count,
--             agg.assessment_count,
--             agg.completed_task_count,
--             agg.scored_task_count,
--             agg.first_plan_date_key,
--             agg.last_plan_date_key,
--             agg.first_assessment_date_key,
--             agg.last_assessment_date_key,
--             CAST(N'PROGRAM_OPS' AS NVARCHAR(100)) AS source_system
--         INTO #lifecycle_snapshot
--         FROM
--         (
--             SELECT
--                 COALESCE(ft.child_key, -1)   AS child_key,
--                 COALESCE(ft.center_key, -1)  AS center_key,
--                 COALESCE(ft.teacher_key, -1) AS teacher_key,

--                 COUNT(DISTINCT CASE
--                                    WHEN ft.source_daily_task_assignment_id IS NOT NULL
--                                    THEN ft.source_daily_task_assignment_id
--                                END) AS planned_task_count,

--                 COUNT(DISTINCT CASE
--                                    WHEN ft.source_task_assessment_id IS NOT NULL
--                                    THEN ft.source_task_assessment_id
--                                END) AS assessment_count,

--                 COUNT(DISTINCT CASE
--                                    WHEN ft.is_completed = 1
--                                         AND ft.source_daily_task_assignment_id IS NOT NULL
--                                    THEN ft.source_daily_task_assignment_id
--                                END) AS completed_task_count,

--                 COUNT(DISTINCT CASE
--                                    WHEN ft.is_scored = 1
--                                         AND ft.source_daily_task_assignment_id IS NOT NULL
--                                    THEN ft.source_daily_task_assignment_id
--                                END) AS scored_task_count,

--                 MIN(CASE
--                         WHEN ft.source_daily_task_assignment_id IS NOT NULL
--                         THEN COALESCE(ft.date_key, -1)
--                     END) AS first_plan_date_key,

--                 MAX(CASE
--                         WHEN ft.source_daily_task_assignment_id IS NOT NULL
--                         THEN COALESCE(ft.date_key, -1)
--                     END) AS last_plan_date_key,

--                 MIN(CASE
--                         WHEN ft.source_task_assessment_id IS NOT NULL
--                         THEN COALESCE(ft.date_key, -1)
--                     END) AS first_assessment_date_key,

--                 MAX(CASE
--                         WHEN ft.source_task_assessment_id IS NOT NULL
--                         THEN COALESCE(ft.date_key, -1)
--                     END) AS last_assessment_date_key
--             FROM dw.fact_tran_student_task_progress AS ft
--             LEFT JOIN dw.dim_date AS tx_date
--                 ON tx_date.TimeKey = ft.date_key
--             WHERE
--                 (
--                     tx_date.FullDateAlternateKey >= CONVERT(DATE, @start_time)
--                     AND tx_date.FullDateAlternateKey <  CONVERT(DATE, @end_time)
--                 )
--                 OR
--                 (
--                     COALESCE(ft.date_key, -1) = -1
--                     AND ft.loaded_at >= @start_time
--                     AND ft.loaded_at <  @end_time
--                 )
--             GROUP BY
--                 COALESCE(ft.child_key, -1),
--                 COALESCE(ft.center_key, -1),
--                 COALESCE(ft.teacher_key, -1)
--         ) AS agg;

--         SET @lifecycle_candidate_rows = @@ROWCOUNT;
--         SET @rows_read = @lifecycle_candidate_rows;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Build first-load lifecycle candidates', N'succeeded',
--              @lifecycle_candidate_rows, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Source transaction rows read: ', @source_transaction_rows,
--                     N'. Lifecycle grain rows prepared: ', @lifecycle_candidate_rows,
--                     N'. Grain: child_key + center_key + teacher_key.'));

--         /*---------------------------------------------------------------------
--           Step 4: Insert lifecycle rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.fact_child_snapshot_accumulation
--             (snapshot_date_key, child_key, center_key, teacher_key,
--              planned_task_count, assessment_count, completed_task_count, scored_task_count,
--              first_plan_date_key, last_plan_date_key,
--              first_assessment_date_key, last_assessment_date_key,
--              source_system, etl_batch_id, loaded_at)
--         SELECT
--             src.snapshot_date_key,
--             src.child_key,
--             src.center_key,
--             src.teacher_key,
--             src.planned_task_count,
--             src.assessment_count,
--             src.completed_task_count,
--             src.scored_task_count,
--             src.first_plan_date_key,
--             src.last_plan_date_key,
--             src.first_assessment_date_key,
--             src.last_assessment_date_key,
--             src.source_system,
--             @etl_batch_id,
--             SYSDATETIME()
--         FROM #lifecycle_snapshot AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert first-load lifecycle rows', N'succeeded',
--              @lifecycle_candidate_rows, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted child lifecycle accumulation rows.');

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_tran_student_task_progress',
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_child_snapshot_accumulation',
--             load_status,
--             rows_read,
--             rows_inserted,
--             rows_updated,
--             rows_rejected,
--             started_at,
--             ended_at,
--             message
--         FROM @step_log;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;

--         COMMIT TRANSACTION;
--     END TRY
--     BEGIN CATCH
--         SET @error_message = CONCAT(
--             N'Error ', ERROR_NUMBER(),
--             N' at line ', ERROR_LINE(),
--             N': ', ERROR_MESSAGE()
--         );

--         IF XACT_STATE() <> 0
--             ROLLBACK TRANSACTION;

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
--                  N'Charity_DW_DB', N'dw', N'fact_child_snapshot_accumulation',
--                  N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- /*=============================================================================
--   First-load procedure
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_child_task_event
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id            INT,
--         @created_by              NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started       DATETIME2(0) = SYSDATETIME(),
--         @step_started            DATETIME2(0),
--         @rows_read               INT = 0,
--         @rows_inserted           INT = 0,
--         @rows_updated            INT = 0,
--         @rows_rejected           INT = 0,
--         @rows_deleted            INT = 0,
--         @source_rows_read        INT = 0,
--         @candidate_rows          INT = 0,
--         @error_message           NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_name      NVARCHAR(200) NOT NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 1: Reset target event fact for first load.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         DELETE FROM dw.fact_child_task_event;
--         SET @rows_deleted = @@ROWCOUNT;

--         DBCC CHECKIDENT ('dw.fact_child_task_event', RESEED, 0) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Reset first-load child task event fact', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Deleted ', @rows_deleted,
--                     N' rows and reseeded child_task_event_key to 0.'));

--         /*---------------------------------------------------------------------
--           Step 2: Build event candidates from the transaction fact.

--           First-load source range:
--               transaction date >= @start_time date
--               transaction date <  @end_time date

--           Rows with unresolved date_key = -1 are included by transaction
--           loaded_at so they are still captured by the requested ETL window.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         IF OBJECT_ID('tempdb..#event_candidate') IS NOT NULL
--             DROP TABLE #event_candidate;

--         SELECT
--             @source_rows_read = COUNT(1)
--         FROM dw.fact_tran_student_task_progress AS ft
--         LEFT JOIN dw.dim_date AS tx_date
--             ON tx_date.TimeKey = ft.date_key
--         WHERE
--             (
--                 tx_date.FullDateAlternateKey >= CONVERT(DATE, @start_time)
--                 AND tx_date.FullDateAlternateKey <  CONVERT(DATE, @end_time)
--             )
--             OR
--             (
--                 COALESCE(ft.date_key, -1) = -1
--                 AND ft.loaded_at >= @start_time
--                 AND ft.loaded_at <  @end_time
--             );

--         SELECT
--             COALESCE(ft.child_key, -1)   AS child_key,
--             COALESCE(ft.task_key, -1)    AS task_key,
--             COALESCE(ft.teacher_key, -1) AS teacher_key,
--             COALESCE(ft.center_key, -1)  AS center_key,
--             COALESCE(ft.domain_key, -1)  AS domain_key,
--             COALESCE(ft.date_key, -1)    AS date_key,

--             CASE
--                 WHEN ft.source_task_assessment_id IS NOT NULL
--                     THEN CAST(N'ASSESSMENT' AS NVARCHAR(50))
--                 WHEN ft.source_daily_task_assignment_id IS NOT NULL
--                     THEN CAST(N'PLAN' AS NVARCHAR(50))
--                 ELSE CAST(N'UNKNOWN' AS NVARCHAR(50))
--             END AS event_type,

--             CAST(
--                 CASE
--                     WHEN ft.source_task_assessment_id IS NOT NULL
--                          AND NULLIF(LTRIM(RTRIM(das.assessment_status_code)), N'') IS NOT NULL
--                         THEN LEFT(LTRIM(RTRIM(das.assessment_status_code)), 50)
--                     WHEN ft.is_cancelled = 1
--                         THEN N'CANCELLED'
--                     WHEN ft.is_absent = 1
--                         THEN N'ABSENT'
--                     WHEN ft.is_refused = 1
--                         THEN N'REFUSED'
--                     WHEN ft.is_incomplete = 1
--                         THEN N'INCOMPLETE'
--                     WHEN ft.is_completed = 1
--                         THEN N'COMPLETED'
--                     WHEN ft.is_scored = 1
--                         THEN N'SCORED'
--                     WHEN ft.is_not_scored = 1
--                         THEN N'NOT_SCORED'
--                     WHEN ft.is_assessed = 1
--                         THEN N'ASSESSED'
--                     WHEN ft.is_planned = 1
--                         THEN N'PLANNED'
--                     ELSE N'UNKNOWN'
--                 END
--             AS NVARCHAR(50)) AS event_status,

--             ft.raw_score,
--             ft.normalized_score,
--             ft.source_daily_task_assignment_id,
--             ft.source_task_assessment_id,
--             ft.source_assessment_session_id,
--             CAST(N'PROGRAM_OPS' AS NVARCHAR(100)) AS source_system
--         INTO #event_candidate
--         FROM dw.fact_tran_student_task_progress AS ft
--         LEFT JOIN dw.dim_date AS tx_date
--             ON tx_date.TimeKey = ft.date_key
--         LEFT JOIN dw.dim_assessment_status AS das
--             ON das.assessment_status_key = ft.assessment_status_key
--         WHERE
--             (
--                 (
--                     tx_date.FullDateAlternateKey >= CONVERT(DATE, @start_time)
--                     AND tx_date.FullDateAlternateKey <  CONVERT(DATE, @end_time)
--                 )
--                 OR
--                 (
--                     COALESCE(ft.date_key, -1) = -1
--                     AND ft.loaded_at >= @start_time
--                     AND ft.loaded_at <  @end_time
--                 )
--             )
--             AND
--             (
--                 ft.source_daily_task_assignment_id IS NOT NULL
--                 OR ft.source_task_assessment_id IS NOT NULL
--             );

--         SET @candidate_rows = @@ROWCOUNT;

--         SELECT
--             @rows_rejected = COUNT(1)
--         FROM dw.fact_tran_student_task_progress AS ft
--         LEFT JOIN dw.dim_date AS tx_date
--             ON tx_date.TimeKey = ft.date_key
--         WHERE
--             (
--                 (
--                     tx_date.FullDateAlternateKey >= CONVERT(DATE, @start_time)
--                     AND tx_date.FullDateAlternateKey <  CONVERT(DATE, @end_time)
--                 )
--                 OR
--                 (
--                     COALESCE(ft.date_key, -1) = -1
--                     AND ft.loaded_at >= @start_time
--                     AND ft.loaded_at <  @end_time
--                 )
--             )
--             AND ft.source_daily_task_assignment_id IS NULL
--             AND ft.source_task_assessment_id IS NULL;

--         SET @rows_read = @candidate_rows;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Build first-load child task event candidates', N'succeeded',
--              @candidate_rows, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Source transaction rows read: ', @source_rows_read,
--                     N'. Candidate event rows: ', @candidate_rows,
--                     N'. Rejected rows without source event IDs: ', @rows_rejected,
--                     N'. Grain: source event ID + event_type.'));

--         /*---------------------------------------------------------------------
--           Step 3: Insert event rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.fact_child_task_event
--             (child_key, task_key, teacher_key, center_key, domain_key, date_key,
--              event_type, event_status, raw_score, normalized_score,
--              source_daily_task_assignment_id, source_task_assessment_id,
--              source_assessment_session_id, source_system, etl_batch_id, loaded_at)
--         SELECT
--             src.child_key,
--             src.task_key,
--             src.teacher_key,
--             src.center_key,
--             src.domain_key,
--             src.date_key,
--             src.event_type,
--             src.event_status,
--             src.raw_score,
--             src.normalized_score,
--             src.source_daily_task_assignment_id,
--             src.source_task_assessment_id,
--             src.source_assessment_session_id,
--             src.source_system,
--             @etl_batch_id,
--             SYSDATETIME()
--         FROM #event_candidate AS src;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Insert first-load child task event rows', N'succeeded',
--              @candidate_rows, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted first-load event fact rows.');

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_tran_student_task_progress',
--             N'Charity_DW_DB',
--             N'dw',
--             N'fact_child_task_event',
--             load_status,
--             rows_read,
--             rows_inserted,
--             rows_updated,
--             rows_rejected,
--             started_at,
--             ended_at,
--             message
--         FROM @step_log;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;

--         COMMIT TRANSACTION;
--     END TRY
--     BEGIN CATCH
--         SET @error_message = CONCAT(
--             N'Error ', ERROR_NUMBER(),
--             N' at line ', ERROR_LINE(),
--             N': ', ERROR_MESSAGE()
--         );

--         IF XACT_STATE() <> 0
--             ROLLBACK TRANSACTION;

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress',
--                  N'Charity_DW_DB', N'dw', N'fact_child_task_event',
--                  N'failed',
--                  @rows_read, @rows_inserted, @rows_updated, @rows_rejected,
--                  @procedure_started, SYSDATETIME(), @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO


-- CREATE OR ALTER PROCEDURE etl_admin.usp_run_dw_mart1_first_load
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id             INT,
--         @created_by               NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_orchestrator'),
--         @procedure_started        DATETIME2(0) = SYSDATETIME(),
--         @step_started             DATETIME2(0),
--         @before_child_batch_id    INT,
--         @child_batch_id           INT,
--         @child_status             NVARCHAR(50),
--         @child_rows_read          INT,
--         @child_rows_inserted      INT,
--         @child_rows_updated       INT,
--         @child_rows_rejected      INT,
--         @rows_read                INT = 0,
--         @rows_inserted            INT = 0,
--         @rows_updated             INT = 0,
--         @rows_rejected            INT = 0,
--         @error_message            NVARCHAR(MAX);

--     DECLARE @step_log TABLE
--     (
--         step_no        INT           NOT NULL,
--         step_name      NVARCHAR(200) NOT NULL,
--         target_table   NVARCHAR(128) NOT NULL,
--         child_batch_id INT           NULL,
--         load_status    NVARCHAR(50)  NOT NULL,
--         rows_read      INT           NULL,
--         rows_inserted  INT           NULL,
--         rows_updated   INT           NULL,
--         rows_rejected  INT           NULL,
--         started_at     DATETIME2(0)  NOT NULL,
--         ended_at       DATETIME2(0)  NOT NULL,
--         message        NVARCHAR(MAX) NULL
--     );

--     IF @start_time IS NULL OR @end_time IS NULL
--     BEGIN
--         RAISERROR('@start_time and @end_time are required.', 16, 1);
--         RETURN;
--     END;

--     IF @start_time >= @end_time
--     BEGIN
--         RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
--         RETURN;
--     END;

--     BEGIN TRY
--         INSERT INTO etl_admin.etl_batch
--             (source_system, target_layer, mart_name, batch_status,
--              started_at, rows_read, rows_inserted, rows_updated, rows_rejected,
--              created_by)
--         VALUES
--             (N'PROGRAM_OPS', N'DW', N'MART1_DW_ONLY_FIRST_LOAD', N'running',
--              @procedure_started, 0, 0, 0, 0, @created_by);

--         SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());


--         /* Step 01: dim_center */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_center
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_center did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (1,
--              N'01 - usp_first_load_dw_dim_center',
--              N'dim_center',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_center. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 02: dim_domain */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_domain
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_domain did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (2,
--              N'02 - usp_first_load_dw_dim_domain',
--              N'dim_domain',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_domain. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 03: dim_score_scale */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_score_scale
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_score_scale did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (3,
--              N'03 - usp_first_load_dw_dim_score_scale',
--              N'dim_score_scale',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_score_scale. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 04: dim_no_score_reason */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_no_score_reason
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_no_score_reason did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (4,
--              N'04 - usp_first_load_dw_dim_no_score_reason',
--              N'dim_no_score_reason',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_no_score_reason. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 05: dim_assessment_status */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_assessment_status
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_assessment_status did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (5,
--              N'05 - usp_first_load_dw_dim_assessment_status',
--              N'dim_assessment_status',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_assessment_status. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 06: dim_child */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_child
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_child did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (6,
--              N'06 - usp_first_load_dw_dim_child',
--              N'dim_child',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_child. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 07: dim_teacher */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_teacher
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_teacher did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (7,
--              N'07 - usp_first_load_dw_dim_teacher',
--              N'dim_teacher',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_teacher. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 08: dim_task */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_dim_task
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_dim_task did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (8,
--              N'08 - usp_first_load_dw_dim_task',
--              N'dim_task',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_dim_task. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 09: fact_tran_student_task_progress */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_fact_tran_student_task_progress
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_fact_tran_student_task_progress did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (9,
--              N'09 - usp_first_load_dw_fact_tran_student_task_progress',
--              N'fact_tran_student_task_progress',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_fact_tran_student_task_progress. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 10: fact_child_task_event */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_fact_child_task_event
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_fact_child_task_event did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (10,
--              N'10 - usp_first_load_dw_fact_child_task_event',
--              N'fact_child_task_event',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_fact_child_task_event. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 11: fact_daily_student_task_progress */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_fact_daily_student_task_progress
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_fact_daily_student_task_progress did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (11,
--              N'11 - usp_first_load_dw_fact_daily_student_task_progress',
--              N'fact_daily_student_task_progress',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_fact_daily_student_task_progress. Child etl_batch_id = ', @child_batch_id, N'.'));

--         /* Step 12: fact_child_snapshot_accumulation */
--         SET @step_started = SYSDATETIME();
--         SET @child_batch_id = NULL;
--         SET @child_status = NULL;
--         SET @child_rows_read = 0;
--         SET @child_rows_inserted = 0;
--         SET @child_rows_updated = 0;
--         SET @child_rows_rejected = 0;

--         SELECT @before_child_batch_id = ISNULL(MAX(etl_batch_id), 0)
--         FROM etl_admin.etl_batch;

--         EXEC etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation
--             @start_time = @start_time,
--             @end_time   = @end_time;

--         SELECT TOP (1)
--             @child_batch_id       = etl_batch_id,
--             @child_status         = batch_status,
--             @child_rows_read      = ISNULL(rows_read, 0),
--             @child_rows_inserted  = ISNULL(rows_inserted, 0),
--             @child_rows_updated   = ISNULL(rows_updated, 0),
--             @child_rows_rejected  = ISNULL(rows_rejected, 0)
--         FROM etl_admin.etl_batch
--         WHERE etl_batch_id > @before_child_batch_id
--         ORDER BY etl_batch_id DESC;

--         IF ISNULL(@child_status, N'failed') <> N'succeeded'
--         BEGIN
--             RAISERROR('Child procedure etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (12,
--              N'12 - usp_first_load_dw_fact_child_snapshot_accumulation',
--              N'fact_child_snapshot_accumulation',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_first_load_dw_fact_child_snapshot_accumulation. Child etl_batch_id = ', @child_batch_id, N'.'));

--         SELECT
--             @rows_read     = ISNULL(SUM(ISNULL(rows_read, 0)), 0),
--             @rows_inserted = ISNULL(SUM(ISNULL(rows_inserted, 0)), 0),
--             @rows_updated  = ISNULL(SUM(ISNULL(rows_updated, 0)), 0),
--             @rows_rejected = ISNULL(SUM(ISNULL(rows_rejected, 0)), 0)
--         FROM @step_log;

--         INSERT INTO etl_admin.etl_load_log
--             (etl_batch_id, source_database, source_schema, source_table,
--              target_database, target_schema, target_table, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         SELECT
--             @etl_batch_id,
--             N'Charity_DW_DB',
--             N'etl_admin',
--             N'usp_run_dw_mart1_first_load',
--             N'Charity_DW_DB',
--             N'dw',
--             target_table,
--             load_status,
--             rows_read,
--             rows_inserted,
--             rows_updated,
--             rows_rejected,
--             started_at,
--             ended_at,
--             CONCAT(
--                 N'[',
--                 RIGHT(N'00' + CONVERT(NVARCHAR(10), step_no), 2),
--                 N'] ',
--                 step_name,
--                 N'. ',
--                 ISNULL(message, N'')
--             )
--         FROM @step_log
--         ORDER BY step_no;

--         UPDATE etl_admin.etl_batch
--         SET
--             batch_status  = N'succeeded',
--             ended_at      = SYSDATETIME(),
--             rows_read     = @rows_read,
--             rows_inserted = @rows_inserted,
--             rows_updated  = @rows_updated,
--             rows_rejected = @rows_rejected,
--             error_message = NULL
--         WHERE etl_batch_id = @etl_batch_id;
--     END TRY
--     BEGIN CATCH
--         SET @error_message = CONCAT(
--             N'Error ', ERROR_NUMBER(),
--             N' at line ', ERROR_LINE(),
--             N': ', ERROR_MESSAGE()
--         );

--         IF @etl_batch_id IS NOT NULL
--         BEGIN
--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             SELECT
--                 @etl_batch_id,
--                 N'Charity_DW_DB',
--                 N'etl_admin',
--                 N'usp_run_dw_mart1_first_load',
--                 N'Charity_DW_DB',
--                 N'dw',
--                 target_table,
--                 load_status,
--                 rows_read,
--                 rows_inserted,
--                 rows_updated,
--                 rows_rejected,
--                 started_at,
--                 ended_at,
--                 CONCAT(
--                     N'[',
--                     RIGHT(N'00' + CONVERT(NVARCHAR(10), step_no), 2),
--                     N'] ',
--                     step_name,
--                     N'. ',
--                     ISNULL(message, N'')
--                 )
--             FROM @step_log
--             ORDER BY step_no;

--             INSERT INTO etl_admin.etl_load_log
--                 (etl_batch_id, source_database, source_schema, source_table,
--                  target_database, target_schema, target_table, load_status,
--                  rows_read, rows_inserted, rows_updated, rows_rejected,
--                  started_at, ended_at, message)
--             VALUES
--                 (@etl_batch_id,
--                  N'Charity_DW_DB',
--                  N'etl_admin',
--                  N'usp_run_dw_mart1_first_load',
--                  N'Charity_DW_DB',
--                  N'etl_admin',
--                  N'etl_batch',
--                  N'failed',
--                  @rows_read,
--                  @rows_inserted,
--                  @rows_updated,
--                  @rows_rejected,
--                  @procedure_started,
--                  SYSDATETIME(),
--                  @error_message);

--             UPDATE etl_admin.etl_batch
--             SET
--                 batch_status  = N'failed',
--                 ended_at      = SYSDATETIME(),
--                 rows_read     = @rows_read,
--                 rows_inserted = @rows_inserted,
--                 rows_updated  = @rows_updated,
--                 rows_rejected = @rows_rejected,
--                 error_message = @error_message
--             WHERE etl_batch_id = @etl_batch_id;
--         END;

--         THROW;
--     END CATCH;
-- END;
-- GO
