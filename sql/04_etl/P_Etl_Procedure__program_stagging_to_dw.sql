/*=============================================================================
  File: 29_P_Etl_Procedure__program_stagging_to_dw_REWORKED.sql

  Purpose:
      Normal/daily ETL procedures, reworked by requested ETL patterns.

  Main changes:
      - Type 1 dimensions use permanent etl_work tables + TRUNCATE + INSERT.
      - Type 2 dimensions close old current row and insert new current row.
      - Transaction/event facts are append-only and avoid duplicates.
      - Daily snapshot fact appends missing daily rows using a daily loop.
      - Lifecycle fact is rebuilt using old lifecycle + new snapshot period work tables.
      - No SQL Server local temporary tables are used.
=============================================================================*/

USE Charity_DW_DB;
GO


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_center
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
            @end_time,
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

        UPDATE tgt
        SET
            tgt.is_current = 0,
            tgt.effective_to = @end_time,
            tgt.updated_at = SYSDATETIME()
        FROM dw.dim_center AS tgt
        INNER JOIN etl_work.w_dim_center AS src
            ON src.center_id = tgt.center_id
        WHERE tgt.center_key <> -1
          AND tgt.is_current = 1
          AND ISNULL(CONVERT(VARBINARY(32), tgt.row_hash), 0x) <> ISNULL(CONVERT(VARBINARY(32), src.row_hash), 0x);

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO dw.dim_center
            (center_id, center_name, city, address, center_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        SELECT
            src.center_id, src.center_name, src.city, src.address, src.center_status,
            src.effective_from, src.effective_to, src.is_current, src.source_system, src.row_hash,
            src.created_at, src.updated_at
        FROM etl_work.w_dim_center AS src
        LEFT JOIN dw.dim_center AS cur
            ON cur.center_id = src.center_id
           AND cur.is_current = 1
           AND ISNULL(CONVERT(VARBINARY(32), cur.row_hash), 0x) = ISNULL(CONVERT(VARBINARY(32), src.row_hash), 0x)
        WHERE cur.center_key IS NULL;

        SET @rows_inserted = @@ROWCOUNT;
        SET @message = N'Incremental Type 2 center dimension. Changed current rows were closed and new current versions inserted.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_teacher
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
            @end_time,
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

        UPDATE tgt
        SET
            tgt.is_current = 0,
            tgt.effective_to = @end_time,
            tgt.updated_at = SYSDATETIME()
        FROM dw.dim_teacher AS tgt
        INNER JOIN etl_work.w_dim_teacher AS src
            ON src.teacher_id = tgt.teacher_id
        WHERE tgt.teacher_key <> -1
          AND tgt.is_current = 1
          AND ISNULL(CONVERT(VARBINARY(32), tgt.row_hash), 0x) <> ISNULL(CONVERT(VARBINARY(32), src.row_hash), 0x);

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO dw.dim_teacher
            (teacher_id, first_name, last_name, full_name,
             center_id, center_name, employment_status,
             effective_from, effective_to, is_current, source_system, row_hash,
             created_at, updated_at)
        SELECT
            src.teacher_id, src.first_name, src.last_name, src.full_name,
            src.center_id, src.center_name, src.employment_status,
            src.effective_from, src.effective_to, src.is_current, src.source_system, src.row_hash,
            src.created_at, src.updated_at
        FROM etl_work.w_dim_teacher AS src
        LEFT JOIN dw.dim_teacher AS cur
            ON cur.teacher_id = src.teacher_id
           AND cur.is_current = 1
           AND ISNULL(CONVERT(VARBINARY(32), cur.row_hash), 0x) = ISNULL(CONVERT(VARBINARY(32), src.row_hash), 0x)
        WHERE cur.teacher_key IS NULL;

        SET @rows_inserted = @@ROWCOUNT;
        SET @message = N'Incremental Type 2 teacher dimension. Changed current rows were closed and new current versions inserted.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_child
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
            (child_key, child_id, first_name, last_name, full_name, birth_date, gender,
             center_id, status, enrollment_date, source_system, row_hash,
             created_at, updated_at)
        SELECT
            old.child_key,
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
            ON s.stg_row_id = x.max_stg_row_id
        LEFT JOIN dw.dim_child AS old
            ON old.child_id = s.id
           AND old.child_key <> -1;

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

        INSERT INTO dw.dim_child
            (child_key, child_id, first_name, last_name, full_name, birth_date,
             gender, center_id, status, enrollment_date, source_system, row_hash,
             created_at, updated_at)
        SELECT child_key, child_id, first_name, last_name, full_name, birth_date,
               gender, center_id, status, enrollment_date, source_system, row_hash,
               created_at, updated_at
        FROM etl_work.w_dim_child
        WHERE child_key IS NOT NULL;
        SET IDENTITY_INSERT dw.dim_child OFF;

        INSERT INTO dw.dim_child
            (child_id, first_name, last_name, full_name, birth_date,
             gender, center_id, status, enrollment_date, source_system, row_hash,
             created_at, updated_at)
        SELECT child_id, first_name, last_name, full_name, birth_date,
               gender, center_id, status, enrollment_date, source_system, row_hash,
               created_at, updated_at
        FROM etl_work.w_dim_child
        WHERE child_key IS NULL;

        SET @rows_inserted = @rows_read + 1;
        SET @message = N'Incremental Type 1 child dimension. Dimension rebuilt using truncate and insert while preserving existing surrogate keys.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_domain
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
            (domain_key, domain_id, domain_name, domain_description, domain_status,
             source_system, row_hash, created_at, updated_at)
        SELECT old.domain_key, s.id, LTRIM(RTRIM(s.name)), s.description,
               CASE WHEN ISNULL(s.is_active,0)=1 THEN N'active' ELSE N'inactive' END,
               s.source_system,
               HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.name, s.description, s.is_active)),
               SYSDATETIME(), SYSDATETIME()
        FROM (SELECT id, MAX(stg_row_id) max_stg_row_id FROM Stg_ProgramOps_DB.stg_program_ops.domains WHERE is_valid=1 AND id IS NOT NULL GROUP BY id) x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.domains s ON s.stg_row_id=x.max_stg_row_id
        LEFT JOIN dw.dim_domain old ON old.domain_id=s.id AND old.domain_key<>-1;

        SELECT @rows_read=COUNT(*) FROM etl_work.w_dim_domain;
        TRUNCATE TABLE dw.dim_domain;
        DBCC CHECKIDENT ('dw.dim_domain', RESEED, 0) WITH NO_INFOMSGS;
        SET IDENTITY_INSERT dw.dim_domain ON;
        INSERT INTO dw.dim_domain (domain_key, domain_id, domain_name, domain_description, domain_status, source_system, row_hash, created_at, updated_at)
        VALUES (-1,-1,N'Unknown',N'Unknown',N'unknown',N'SYSTEM',NULL,SYSDATETIME(),SYSDATETIME());
        INSERT INTO dw.dim_domain (domain_key, domain_id, domain_name, domain_description, domain_status, source_system, row_hash, created_at, updated_at)
        SELECT domain_key, domain_id, domain_name, domain_description, domain_status, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_domain WHERE domain_key IS NOT NULL;
        SET IDENTITY_INSERT dw.dim_domain OFF;
        INSERT INTO dw.dim_domain (domain_id, domain_name, domain_description, domain_status, source_system, row_hash, created_at, updated_at)
        SELECT domain_id, domain_name, domain_description, domain_status, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_domain WHERE domain_key IS NULL;
        SET @rows_inserted=@rows_read+1;
        SET @message=N'Incremental Type 1 domain dimension rebuilt using truncate and insert.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_score_scale
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
            (score_scale_key, score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        SELECT old.score_scale_key, s.id, LTRIM(RTRIM(s.name)), s.min_score, s.max_score, s.description,
               CASE WHEN ISNULL(s.is_active,0)=1 THEN N'active' ELSE N'inactive' END,
               s.source_system,
               HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.name, s.min_score, s.max_score, s.description, s.is_active)),
               SYSDATETIME(), SYSDATETIME()
        FROM (SELECT id, MAX(stg_row_id) max_stg_row_id FROM Stg_ProgramOps_DB.stg_program_ops.score_scales WHERE is_valid=1 AND id IS NOT NULL GROUP BY id) x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.score_scales s ON s.stg_row_id=x.max_stg_row_id
        LEFT JOIN dw.dim_score_scale old ON old.score_scale_id=s.id AND old.score_scale_key<>-1;
        SELECT @rows_read=COUNT(*) FROM etl_work.w_dim_score_scale;
        TRUNCATE TABLE dw.dim_score_scale;
        DBCC CHECKIDENT ('dw.dim_score_scale', RESEED, 0) WITH NO_INFOMSGS;
        SET IDENTITY_INSERT dw.dim_score_scale ON;
        INSERT INTO dw.dim_score_scale (score_scale_key, score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        VALUES (-1,-1,N'Unknown',NULL,NULL,N'Unknown',N'unknown',N'SYSTEM',NULL,SYSDATETIME(),SYSDATETIME());
        INSERT INTO dw.dim_score_scale (score_scale_key, score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        SELECT score_scale_key, score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_score_scale WHERE score_scale_key IS NOT NULL;
        SET IDENTITY_INSERT dw.dim_score_scale OFF;
        INSERT INTO dw.dim_score_scale (score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        SELECT score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_score_scale WHERE score_scale_key IS NULL;
        SET @rows_inserted=@rows_read+1;
        SET @message=N'Incremental Type 1 score scale dimension rebuilt using truncate and insert.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_assessment_status
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
            (assessment_status_key, assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at)
        SELECT old.assessment_status_key, src.code, src.title, src.category, src.is_success, src.is_failure, N'PROGRAM_OPS', SYSDATETIME(), SYSDATETIME()
        FROM (
            SELECT DISTINCT
                LOWER(LTRIM(RTRIM(assessment_status))) AS code,
                LTRIM(RTRIM(assessment_status)) AS title,
                CASE WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'scored',N'completed') THEN N'success'
                     WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'not_scored',N'refused',N'absent',N'incomplete',N'cancelled') THEN N'failure'
                     ELSE N'other' END AS category,
                CASE WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'scored',N'completed') THEN 1 ELSE 0 END AS is_success,
                CASE WHEN LOWER(LTRIM(RTRIM(assessment_status))) IN (N'not_scored',N'refused',N'absent',N'incomplete',N'cancelled') THEN 1 ELSE 0 END AS is_failure
            FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments
            WHERE is_valid=1 AND NULLIF(LTRIM(RTRIM(assessment_status)),N'') IS NOT NULL
        ) src
        LEFT JOIN dw.dim_assessment_status old ON old.assessment_status_code=src.code AND old.assessment_status_key<>-1;
        SELECT @rows_read=COUNT(*) FROM etl_work.w_dim_assessment_status;
        TRUNCATE TABLE dw.dim_assessment_status;
        DBCC CHECKIDENT ('dw.dim_assessment_status', RESEED, 0) WITH NO_INFOMSGS;
        SET IDENTITY_INSERT dw.dim_assessment_status ON;
        INSERT INTO dw.dim_assessment_status (assessment_status_key, assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at)
        VALUES (-1,N'unknown',N'Unknown',N'unknown',0,0,N'SYSTEM',SYSDATETIME(),SYSDATETIME());
        INSERT INTO dw.dim_assessment_status (assessment_status_key, assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at)
        SELECT assessment_status_key, assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at FROM etl_work.w_dim_assessment_status WHERE assessment_status_key IS NOT NULL;
        SET IDENTITY_INSERT dw.dim_assessment_status OFF;
        INSERT INTO dw.dim_assessment_status (assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at)
        SELECT assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at FROM etl_work.w_dim_assessment_status WHERE assessment_status_key IS NULL;
        SET @rows_inserted=@rows_read+1;
        SET @message=N'Incremental static assessment status dimension rebuilt using truncate and insert.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_no_score_reason
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
            (no_score_reason_key, no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, row_hash, created_at, updated_at)
        SELECT old.no_score_reason_key, s.id, LTRIM(RTRIM(s.title)), s.description,
               CASE WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%child%' OR LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%absent%' OR LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%refus%' THEN N'child'
                    WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%teacher%' THEN N'teacher'
                    WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%center%' OR LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%closed%' THEN N'center'
                    WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%system%' THEN N'system'
                    ELSE N'other' END,
               CASE WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%child%' OR LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%absent%' OR LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%refus%' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%teacher%' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%center%' OR LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%closed%' THEN 1 ELSE 0 END,
               CASE WHEN LOWER(CONCAT(s.title,N' ',s.description)) LIKE N'%system%' THEN 1 ELSE 0 END,
               s.source_system,
               HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.title, s.description, s.is_active)),
               SYSDATETIME(), SYSDATETIME()
        FROM (SELECT id, MAX(stg_row_id) max_stg_row_id FROM Stg_ProgramOps_DB.stg_program_ops.no_score_reasons WHERE is_valid=1 AND id IS NOT NULL GROUP BY id) x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.no_score_reasons s ON s.stg_row_id=x.max_stg_row_id
        LEFT JOIN dw.dim_no_score_reason old ON old.no_score_reason_id=s.id AND old.no_score_reason_key<>-1;
        SELECT @rows_read=COUNT(*) FROM etl_work.w_dim_no_score_reason;
        TRUNCATE TABLE dw.dim_no_score_reason;
        DBCC CHECKIDENT ('dw.dim_no_score_reason', RESEED, 0) WITH NO_INFOMSGS;
        SET IDENTITY_INSERT dw.dim_no_score_reason ON;
        INSERT INTO dw.dim_no_score_reason (no_score_reason_key, no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, row_hash, created_at, updated_at)
        VALUES (-1,-1,N'Unknown',N'Unknown',N'unknown',0,0,0,0,N'SYSTEM',NULL,SYSDATETIME(),SYSDATETIME());
        INSERT INTO dw.dim_no_score_reason (no_score_reason_key, no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, row_hash, created_at, updated_at)
        SELECT no_score_reason_key, no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_no_score_reason WHERE no_score_reason_key IS NOT NULL;
        SET IDENTITY_INSERT dw.dim_no_score_reason OFF;
        INSERT INTO dw.dim_no_score_reason (no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, row_hash, created_at, updated_at)
        SELECT no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_no_score_reason WHERE no_score_reason_key IS NULL;
        SET @rows_inserted=@rows_read+1;
        SET @message=N'Incremental Type 1 no-score reason dimension rebuilt using truncate and insert.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_task
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
            (task_key, task_template_id, task_title, domain_id, domain_name, is_template_based,
             task_description, task_status, source_system, row_hash, created_at, updated_at, natural_task_code)
        SELECT old.task_key, s.id, LTRIM(RTRIM(s.title)), s.domain_id, dd.domain_name, 1,
               s.description, CASE WHEN ISNULL(s.is_active,0)=1 THEN N'active' ELSE N'inactive' END,
               s.source_system,
               HASHBYTES('SHA2_256', CONCAT_WS(N'|', s.id, s.title, s.domain_id, s.description, s.is_active)),
               SYSDATETIME(), SYSDATETIME(), CONCAT(N'TEMPLATE:', CONVERT(NVARCHAR(50), s.id))
        FROM (SELECT id, MAX(stg_row_id) max_stg_row_id FROM Stg_ProgramOps_DB.stg_program_ops.task_templates WHERE is_valid=1 AND id IS NOT NULL GROUP BY id) x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.task_templates s ON s.stg_row_id=x.max_stg_row_id
        LEFT JOIN dw.dim_domain dd ON dd.domain_id=s.domain_id
        LEFT JOIN dw.dim_task old ON old.task_template_id=s.id AND old.task_key<>-1;

        INSERT INTO etl_work.w_dim_task
            (task_key, task_template_id, task_title, domain_id, domain_name, is_template_based,
             task_description, task_status, source_system, row_hash, created_at, updated_at, natural_task_code)
        SELECT old.task_key, NULL, LTRIM(RTRIM(s.task_title)), s.domain_id, dd.domain_name, 0,
               NULL, CASE WHEN ISNULL(s.is_active,0)=1 THEN N'active' ELSE N'inactive' END,
               s.source_system,
               HASHBYTES('SHA2_256', CONCAT_WS(N'|', N'CUSTOM', s.domain_id, LTRIM(RTRIM(s.task_title)), s.is_active)),
               SYSDATETIME(), SYSDATETIME(), CONCAT(N'CUSTOM:', CONVERT(NVARCHAR(50), s.domain_id), N':', LOWER(LTRIM(RTRIM(s.task_title))))
        FROM (
            SELECT domain_id, LTRIM(RTRIM(task_title)) task_title, MAX(stg_row_id) max_stg_row_id
            FROM Stg_ProgramOps_DB.stg_program_ops.child_task_plans
            WHERE is_valid=1 AND task_template_id IS NULL AND NULLIF(LTRIM(RTRIM(task_title)),N'') IS NOT NULL
            GROUP BY domain_id, LTRIM(RTRIM(task_title))
        ) x
        INNER JOIN Stg_ProgramOps_DB.stg_program_ops.child_task_plans s ON s.stg_row_id=x.max_stg_row_id
        LEFT JOIN dw.dim_domain dd ON dd.domain_id=s.domain_id
        LEFT JOIN dw.dim_task old ON old.task_template_id IS NULL AND old.domain_id=s.domain_id AND LOWER(LTRIM(RTRIM(old.task_title)))=LOWER(LTRIM(RTRIM(s.task_title))) AND old.task_key<>-1;

        SELECT @rows_read=COUNT(*) FROM etl_work.w_dim_task;
        TRUNCATE TABLE dw.dim_task;
        DBCC CHECKIDENT ('dw.dim_task', RESEED, 0) WITH NO_INFOMSGS;
        SET IDENTITY_INSERT dw.dim_task ON;
        INSERT INTO dw.dim_task (task_key, task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, row_hash, created_at, updated_at)
        VALUES (-1,-1,N'Unknown',-1,N'Unknown',0,N'Unknown',N'unknown',N'SYSTEM',NULL,SYSDATETIME(),SYSDATETIME());
        INSERT INTO dw.dim_task (task_key, task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, row_hash, created_at, updated_at)
        SELECT task_key, task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_task WHERE task_key IS NOT NULL;
        SET IDENTITY_INSERT dw.dim_task OFF;
        INSERT INTO dw.dim_task (task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, row_hash, created_at, updated_at)
        SELECT task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, row_hash, created_at, updated_at FROM etl_work.w_dim_task WHERE task_key IS NULL;
        SET @rows_inserted=@rows_read+1;
        SET @message=N'Incremental Type 1 task dimension rebuilt using truncate and insert.';


        INSERT INTO etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table, load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates/child_task_plans',
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
                (@etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates/child_task_plans',
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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress
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
             src.date_key, src.child_key, src.center_key, src.teacher_key, src.domain_key, src.task_key,
             src.score_scale_key, src.assessment_status_key, src.no_score_reason_key,
             src.attempt_no, src.raw_score, src.normalized_score,
             src.is_completed, src.is_planned, src.is_scored, src.is_not_scored, src.is_cancelled,
             src.is_incomplete, src.is_refused, src.is_absent, src.is_center_closed, src.is_assessed,
             src.source_daily_task_assignment_id, src.source_task_assessment_id,
             src.source_assessment_session_id, src.source_child_task_plan_id,
             src.source_system, @etl_batch_id, SYSDATETIME()
        FROM etl_work.w_fact_tran_student_task_progress AS src
        LEFT JOIN dw.fact_tran_student_task_progress AS tgt
            ON ISNULL(tgt.source_daily_task_assignment_id, -1) = ISNULL(src.source_daily_task_assignment_id, -1)
           AND ISNULL(tgt.source_task_assessment_id, -1) = ISNULL(src.source_task_assessment_id, -1)
        WHERE tgt.student_task_progress_key IS NULL;

        SET @rows_inserted = @@ROWCOUNT;
        SET @message = N'Incremental transaction fact append-only. Existing source event keys are skipped to avoid duplicates.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_child_task_event
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
            ft.child_key, ft.task_key, ft.teacher_key, ft.center_key, ft.domain_key, ft.date_key,
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
            ft.raw_score, ft.normalized_score,
            ft.source_daily_task_assignment_id, ft.source_task_assessment_id,
            ft.source_assessment_session_id, ft.source_system
        FROM dw.fact_tran_student_task_progress AS ft
        LEFT JOIN dw.dim_assessment_status AS das ON das.assessment_status_key=ft.assessment_status_key
        WHERE ft.source_daily_task_assignment_id IS NOT NULL OR ft.source_task_assessment_id IS NOT NULL;

        SELECT @rows_read = COUNT(*) FROM etl_work.w_fact_child_task_event;

        INSERT INTO dw.fact_child_task_event
            (child_key, task_key, teacher_key, center_key, domain_key, date_key,
             event_type, event_status, raw_score, normalized_score,
             source_daily_task_assignment_id, source_task_assessment_id,
             source_assessment_session_id, source_system, etl_batch_id, loaded_at)
        SELECT src.child_key, src.task_key, src.teacher_key, src.center_key, src.domain_key, src.date_key,
               src.event_type, src.event_status, src.raw_score, src.normalized_score,
               src.source_daily_task_assignment_id, src.source_task_assessment_id,
               src.source_assessment_session_id, src.source_system, @etl_batch_id, SYSDATETIME()
        FROM etl_work.w_fact_child_task_event AS src
        LEFT JOIN dw.fact_child_task_event AS tgt
            ON ISNULL(tgt.event_type,N'')=ISNULL(src.event_type,N'')
           AND ISNULL(tgt.source_daily_task_assignment_id,-1)=ISNULL(src.source_daily_task_assignment_id,-1)
           AND ISNULL(tgt.source_task_assessment_id,-1)=ISNULL(src.source_task_assessment_id,-1)
        WHERE tgt.child_task_event_key IS NULL;

        SET @rows_inserted=@@ROWCOUNT;
        SET @message=N'Incremental factless/event fact append-only. Existing relationships are skipped.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress
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


        DECLARE @current_snapshot_date DATE;
        DECLARE @end_snapshot_date DATE = CONVERT(DATE, @end_time);
        DECLARE @current_date_key INT;
        DECLARE @earliest_affected_date DATE;

        SELECT @earliest_affected_date = MIN(dd.FullDateAlternateKey)
        FROM dw.fact_tran_student_task_progress AS ft
        LEFT JOIN dw.dim_date AS dd ON dd.TimeKey = ft.date_key
        WHERE ft.etl_batch_id = (
            SELECT MAX(etl_batch_id)
            FROM dw.fact_tran_student_task_progress
        );

        SET @current_snapshot_date = CASE
            WHEN @earliest_affected_date IS NOT NULL AND @earliest_affected_date < CONVERT(DATE, @start_time)
            THEN @earliest_affected_date
            ELSE CONVERT(DATE, @start_time)
        END;

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
                COALESCE(ft.child_key, -1), COALESCE(ft.center_key, -1), COALESCE(ft.teacher_key, -1),
                CAST(AVG(CASE WHEN ft.is_scored=1 AND ft.raw_score IS NOT NULL THEN ft.raw_score END) AS DECIMAL(10,2)),
                CAST(MIN(CASE WHEN ft.is_scored=1 THEN dss.min_score END) AS DECIMAL(10,2)),
                CAST(MAX(CASE WHEN ft.is_scored=1 THEN dss.max_score END) AS DECIMAL(10,2)),
                CAST(AVG(CASE WHEN ft.is_scored=1 AND ft.normalized_score IS NOT NULL THEN ft.normalized_score END) AS DECIMAL(10,4)),
                COUNT(DISTINCT CASE WHEN ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                COUNT(DISTINCT CASE WHEN ft.source_task_assessment_id IS NOT NULL THEN ft.source_task_assessment_id END),
                COUNT(DISTINCT CASE WHEN ft.is_completed=1 AND ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                COUNT(DISTINCT CASE WHEN ft.is_scored=1 AND ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                COUNT(DISTINCT CASE WHEN ft.is_not_scored=1 AND ft.source_daily_task_assignment_id IS NOT NULL THEN ft.source_daily_task_assignment_id END),
                N'PROGRAM_OPS'
            FROM dw.fact_tran_student_task_progress ft
            LEFT JOIN dw.dim_date tx_date ON tx_date.TimeKey=ft.date_key
            LEFT JOIN dw.dim_score_scale dss ON dss.score_scale_key=ft.score_scale_key
            WHERE tx_date.FullDateAlternateKey <= @current_snapshot_date
               OR COALESCE(ft.date_key,-1)=-1
            GROUP BY COALESCE(ft.child_key,-1), COALESCE(ft.center_key,-1), COALESCE(ft.teacher_key,-1);

            SET @rows_read += @@ROWCOUNT;

            INSERT INTO dw.fact_daily_student_task_progress
                (date_key, child_key, center_key, teacher_key,
                 raw_score, min_score, max_score, normalized_score,
                 planned_task_count, assessment_count, completed_task_count,
                 scored_task_count, not_scored_task_count,
                 source_system, etl_batch_id, loaded_at)
            SELECT src.date_key, src.child_key, src.center_key, src.teacher_key,
                   src.raw_score, src.min_score, src.max_score, src.normalized_score,
                   src.planned_task_count, src.assessment_count, src.completed_task_count,
                   src.scored_task_count, src.not_scored_task_count,
                   src.source_system, @etl_batch_id, SYSDATETIME()
            FROM etl_work.w_fact_daily_student_task_progress src
            LEFT JOIN dw.fact_daily_student_task_progress tgt
                ON tgt.date_key=src.date_key
               AND tgt.child_key=src.child_key
               AND tgt.center_key=src.center_key
               AND tgt.teacher_key=src.teacher_key
            WHERE tgt.daily_student_task_progress_key IS NULL;

            SET @rows_inserted += @@ROWCOUNT;
            SET @current_snapshot_date = DATEADD(DAY,1,@current_snapshot_date);
        END;

        SET @message=N'Incremental daily snapshot append-only. Daily loop inserts missing date_key + child_key + center_key + teacher_key rows and never updates previous rows.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation
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


        DECLARE @start_date_key INT = CONVERT(INT, CONVERT(CHAR(8), CONVERT(DATE, @start_time), 112));
        DECLARE @end_date_key INT = CONVERT(INT, CONVERT(CHAR(8), CONVERT(DATE, @end_time), 112));

        TRUNCATE TABLE etl_work.w_fact_child_snapshot_old;
        TRUNCATE TABLE etl_work.w_fact_child_snapshot_period;
        TRUNCATE TABLE etl_work.w_fact_child_snapshot_final;

        INSERT INTO etl_work.w_fact_child_snapshot_old
            (snapshot_date_key, child_key, center_key, teacher_key,
             planned_task_count, assessment_count, completed_task_count, scored_task_count,
             first_plan_date_key, last_plan_date_key,
             first_assessment_date_key, last_assessment_date_key,
             source_system)
        SELECT snapshot_date_key, child_key, center_key, teacher_key,
               planned_task_count, assessment_count, completed_task_count, scored_task_count,
               first_plan_date_key, last_plan_date_key,
               first_assessment_date_key, last_assessment_date_key,
               source_system
        FROM dw.fact_child_snapshot_accumulation;

        INSERT INTO etl_work.w_fact_child_snapshot_period
            (snapshot_date_key, child_key, center_key, teacher_key,
             planned_task_count, assessment_count, completed_task_count, scored_task_count,
             first_plan_date_key, last_plan_date_key,
             first_assessment_date_key, last_assessment_date_key,
             source_system)
        SELECT latest.max_date_key, latest.child_key, latest.center_key, latest.teacher_key,
               ds.planned_task_count, ds.assessment_count, ds.completed_task_count, ds.scored_task_count,
               dates.first_plan_date_key, dates.last_plan_date_key,
               dates.first_assessment_date_key, dates.last_assessment_date_key,
               N'PROGRAM_OPS'
        FROM (
            SELECT child_key, center_key, teacher_key, MAX(date_key) AS max_date_key
            FROM dw.fact_daily_student_task_progress
            WHERE date_key >= @start_date_key AND date_key < @end_date_key
            GROUP BY child_key, center_key, teacher_key
        ) latest
        INNER JOIN dw.fact_daily_student_task_progress ds
            ON ds.child_key=latest.child_key AND ds.center_key=latest.center_key AND ds.teacher_key=latest.teacher_key AND ds.date_key=latest.max_date_key
        INNER JOIN (
            SELECT child_key, center_key, teacher_key,
                   MIN(CASE WHEN planned_task_count>0 THEN date_key END) AS first_plan_date_key,
                   MAX(CASE WHEN planned_task_count>0 THEN date_key END) AS last_plan_date_key,
                   MIN(CASE WHEN assessment_count>0 THEN date_key END) AS first_assessment_date_key,
                   MAX(CASE WHEN assessment_count>0 THEN date_key END) AS last_assessment_date_key
            FROM dw.fact_daily_student_task_progress
            GROUP BY child_key, center_key, teacher_key
        ) dates
            ON dates.child_key=latest.child_key AND dates.center_key=latest.center_key AND dates.teacher_key=latest.teacher_key;

        INSERT INTO etl_work.w_fact_child_snapshot_final
        SELECT * FROM etl_work.w_fact_child_snapshot_period;

        INSERT INTO etl_work.w_fact_child_snapshot_final
        SELECT old.*
        FROM etl_work.w_fact_child_snapshot_old old
        LEFT JOIN etl_work.w_fact_child_snapshot_period p
            ON p.child_key=old.child_key AND p.center_key=old.center_key AND p.teacher_key=old.teacher_key
        WHERE p.child_key IS NULL;

        SELECT @rows_read=COUNT(*) FROM etl_work.w_fact_child_snapshot_final;

        TRUNCATE TABLE dw.fact_child_snapshot_accumulation;
        DBCC CHECKIDENT ('dw.fact_child_snapshot_accumulation', RESEED, 0) WITH NO_INFOMSGS;

        INSERT INTO dw.fact_child_snapshot_accumulation
            (snapshot_date_key, child_key, center_key, teacher_key,
             planned_task_count, assessment_count, completed_task_count, scored_task_count,
             first_plan_date_key, last_plan_date_key,
             first_assessment_date_key, last_assessment_date_key,
             source_system, etl_batch_id, loaded_at)
        SELECT snapshot_date_key, child_key, center_key, teacher_key,
               planned_task_count, assessment_count, completed_task_count, scored_task_count,
               first_plan_date_key, last_plan_date_key,
               first_assessment_date_key, last_assessment_date_key,
               source_system, @etl_batch_id, SYSDATETIME()
        FROM etl_work.w_fact_child_snapshot_final;

        SET @rows_inserted=@@ROWCOUNT;
        SET @message=N'Incremental lifecycle fact rebuilt through etl_work tables: old lifecycle plus current period snapshot result, then target truncated and reinserted.';


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


CREATE OR ALTER PROCEDURE etl_admin.usp_run_dw_mart1_daily_incremental
    @start_time DATETIME2(0),
    @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;

    EXEC etl_admin.usp_incremental_load_dw_dim_center @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_domain @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_score_scale @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_no_score_reason @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_assessment_status @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_child @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_teacher @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_dim_task @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_fact_child_task_event @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress @start_time = @start_time, @end_time = @end_time;
    EXEC etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation @start_time = @start_time, @end_time = @end_time;
END;
GO

PRINT 'Created reworked incremental DW ETL procedures.';
GO


-- /*=============================================================================
--   Procedure: etl_admin.usp_incremental_load_dw_dim_center
--   Type     : Normal incremental SCD Type 2 dimension load
-- =============================================================================*/
-- SET NOCOUNT ON;
-- GO

-- USE Charity_DW_DB;
-- GO

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_center
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
--         @max_center_key    INT = 0,
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
--         IF OBJECT_ID('tempdb..#changed_center') IS NOT NULL DROP TABLE #changed_center;
--         IF OBJECT_ID('tempdb..#new_center') IS NOT NULL DROP TABLE #new_center;
--         IF OBJECT_ID('tempdb..#insert_center') IS NOT NULL DROP TABLE #insert_center;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected source centers from staging.
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
--             COALESCE(c.created_at, c.source_updated_at, @start_time) AS new_effective_from,
--             COALESCE(c.source_updated_at, c.updated_at, c.created_at, @start_time) AS change_effective_from,
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
--             (N'01 - Detect affected staging centers', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected centers using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row and align identity.
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

--         SELECT @max_center_key = ISNULL(MAX(center_key), 0)
--         FROM dw.dim_center
--         WHERE center_key > 0;

--         DBCC CHECKIDENT (N'dw.dim_center', RESEED, @max_center_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown row and align identity', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Preserved or inserted center_key = -1. Identity reseeded to current MAX(center_key): ', @max_center_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 3: Detect changed and new centers.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             src.center_id,
--             src.center_name,
--             src.city,
--             src.address,
--             src.center_status,
--             CASE
--                 WHEN src.change_effective_from > ISNULL(dim.effective_from, CONVERT(DATETIME2(0), '19000101'))
--                     THEN src.change_effective_from
--                 ELSE DATEADD(SECOND, 1, ISNULL(dim.effective_from, src.change_effective_from))
--             END AS effective_from,
--             src.source_system,
--             src.row_hash,
--             dim.center_key AS old_center_key
--         INTO #changed_center
--         FROM #source_center AS src
--         INNER JOIN dw.dim_center AS dim
--             ON dim.center_id = src.center_id
--            AND dim.is_current = 1
--            AND dim.center_key <> -1
--         WHERE dim.row_hash IS NULL
--            OR dim.row_hash <> src.row_hash;

--         SELECT
--             src.center_id,
--             src.center_name,
--             src.city,
--             src.address,
--             src.center_status,
--             src.new_effective_from AS effective_from,
--             src.source_system,
--             src.row_hash
--         INTO #new_center
--         FROM #source_center AS src
--         LEFT JOIN dw.dim_center AS dim
--             ON dim.center_id = src.center_id
--            AND dim.is_current = 1
--            AND dim.center_key <> -1
--         WHERE dim.center_key IS NULL;

--         SELECT
--             center_id,
--             center_name,
--             city,
--             address,
--             center_status,
--             effective_from,
--             source_system,
--             row_hash
--         INTO #insert_center
--         FROM #new_center
--         UNION ALL
--         SELECT
--             center_id,
--             center_name,
--             city,
--             address,
--             center_status,
--             effective_from,
--             source_system,
--             row_hash
--         FROM #changed_center;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Detect new and changed centers', N'succeeded',
--              (SELECT COUNT(*) FROM #source_center),
--              (SELECT COUNT(*) FROM #new_center),
--              (SELECT COUNT(*) FROM #changed_center),
--              0,
--              @step_started, SYSDATETIME(),
--              N'Detected new centers and changed current SCD2 center versions by comparing business-attribute hash values.');

--         /*---------------------------------------------------------------------
--           Step 4: Close old current versions for changed centers.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE dim
--         SET
--             dim.is_current   = 0,
--             dim.effective_to = ch.effective_from,
--             dim.updated_at   = SYSDATETIME()
--         FROM dw.dim_center AS dim
--         INNER JOIN #changed_center AS ch
--             ON ch.old_center_key = dim.center_key
--         WHERE dim.center_key <> -1
--           AND dim.is_current = 1;

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Close old current SCD2 versions', N'succeeded',
--              (SELECT COUNT(*) FROM #changed_center), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Closed old current center versions by setting is_current = 0 and effective_to = change effective date.');

--         /*---------------------------------------------------------------------
--           Step 5: Insert new current versions.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_center
--             (center_id, center_name, city, address, center_status,
--              effective_from, effective_to, is_current, source_system,
--              row_hash, created_at, updated_at)
--         SELECT
--             ins.center_id,
--             ins.center_name,
--             ins.city,
--             ins.address,
--             ins.center_status,
--             ins.effective_from,
--             NULL AS effective_to,
--             1 AS is_current,
--             COALESCE(ins.source_system, N'PROGRAM_OPS') AS source_system,
--             ins.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #insert_center AS ins;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new current SCD2 versions', N'succeeded',
--              (SELECT COUNT(*) FROM #insert_center), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted current versions for new centers and changed centers.');

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
--   Procedure: etl_admin.usp_incremental_load_dw_dim_teacher
--   Type     : Normal incremental SCD Type 2 dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_teacher
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
--         @max_teacher_key   INT = 0,
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
--         IF OBJECT_ID('tempdb..#changed_teacher') IS NOT NULL DROP TABLE #changed_teacher;
--         IF OBJECT_ID('tempdb..#new_teacher') IS NOT NULL DROP TABLE #new_teacher;
--         IF OBJECT_ID('tempdb..#insert_teacher') IS NOT NULL DROP TABLE #insert_teacher;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected source teachers from staging.

--           A teacher is affected when:
--             1) the teacher row changed in the requested half-open window; or
--             2) the related center row changed in the requested half-open window,
--                because dim_teacher stores a denormalized center_name attribute.
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
--             sc.source_updated_at AS center_source_updated_at,
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
--             SELECT TOP (1) c.name, c.source_updated_at
--             FROM Stg_ProgramOps_DB.stg_program_ops.centers AS c
--             WHERE c.id = t.center_id
--             ORDER BY c.stg_row_id DESC
--         ) AS sc
--         WHERE (t.source_updated_at >= @start_time AND t.source_updated_at < @end_time)
--            OR (sc.source_updated_at >= @start_time AND sc.source_updated_at < @end_time);

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
--             COALESCE(tc.created_at, tc.source_updated_at, @start_time) AS new_effective_from,
--             CASE
--                 WHEN tc.center_source_updated_at >= @start_time
--                  AND tc.center_source_updated_at <  @end_time
--                  AND (tc.source_updated_at IS NULL OR tc.center_source_updated_at > tc.source_updated_at)
--                     THEN tc.center_source_updated_at
--                 ELSE COALESCE(tc.source_updated_at, tc.updated_at, tc.created_at, @start_time)
--             END AS change_effective_from,
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
--             (N'01 - Detect affected staging teachers', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected teachers using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126),
--                     N' <= teacher/center source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row and align identity.
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

--         SELECT @max_teacher_key = ISNULL(MAX(teacher_key), 0)
--         FROM dw.dim_teacher
--         WHERE teacher_key > 0;

--         DBCC CHECKIDENT (N'dw.dim_teacher', RESEED, @max_teacher_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown row and align identity', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Preserved or inserted teacher_key = -1. Identity reseeded to current MAX(teacher_key): ', @max_teacher_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 3: Detect changed and new teachers.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             src.teacher_id,
--             src.first_name,
--             src.last_name,
--             src.full_name,
--             src.center_id,
--             src.center_name,
--             src.employment_status,
--             CASE
--                 WHEN src.change_effective_from > ISNULL(dim.effective_from, CONVERT(DATETIME2(0), '19000101'))
--                     THEN src.change_effective_from
--                 ELSE DATEADD(SECOND, 1, ISNULL(dim.effective_from, src.change_effective_from))
--             END AS effective_from,
--             src.source_system,
--             src.row_hash,
--             dim.teacher_key AS old_teacher_key
--         INTO #changed_teacher
--         FROM #source_teacher AS src
--         INNER JOIN dw.dim_teacher AS dim
--             ON dim.teacher_id = src.teacher_id
--            AND dim.is_current = 1
--            AND dim.teacher_key <> -1
--         WHERE dim.row_hash IS NULL
--            OR dim.row_hash <> src.row_hash;

--         SELECT
--             src.teacher_id,
--             src.first_name,
--             src.last_name,
--             src.full_name,
--             src.center_id,
--             src.center_name,
--             src.employment_status,
--             src.new_effective_from AS effective_from,
--             src.source_system,
--             src.row_hash
--         INTO #new_teacher
--         FROM #source_teacher AS src
--         LEFT JOIN dw.dim_teacher AS dim
--             ON dim.teacher_id = src.teacher_id
--            AND dim.is_current = 1
--            AND dim.teacher_key <> -1
--         WHERE dim.teacher_key IS NULL;

--         SELECT
--             teacher_id,
--             first_name,
--             last_name,
--             full_name,
--             center_id,
--             center_name,
--             employment_status,
--             effective_from,
--             source_system,
--             row_hash
--         INTO #insert_teacher
--         FROM #new_teacher
--         UNION ALL
--         SELECT
--             teacher_id,
--             first_name,
--             last_name,
--             full_name,
--             center_id,
--             center_name,
--             employment_status,
--             effective_from,
--             source_system,
--             row_hash
--         FROM #changed_teacher;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Detect new and changed teachers', N'succeeded',
--              (SELECT COUNT(*) FROM #source_teacher),
--              (SELECT COUNT(*) FROM #new_teacher),
--              (SELECT COUNT(*) FROM #changed_teacher),
--              0,
--              @step_started, SYSDATETIME(),
--              N'Detected new teachers and changed current SCD2 teacher versions by comparing business-attribute hash values.');

--         /*---------------------------------------------------------------------
--           Step 4: Close old current versions for changed teachers.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE dim
--         SET
--             dim.is_current   = 0,
--             dim.effective_to = ch.effective_from,
--             dim.updated_at   = SYSDATETIME()
--         FROM dw.dim_teacher AS dim
--         INNER JOIN #changed_teacher AS ch
--             ON ch.old_teacher_key = dim.teacher_key
--         WHERE dim.teacher_key <> -1
--           AND dim.is_current = 1;

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Close old current SCD2 versions', N'succeeded',
--              (SELECT COUNT(*) FROM #changed_teacher), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Closed old current teacher versions by setting is_current = 0 and effective_to = change effective date.');

--         /*---------------------------------------------------------------------
--           Step 5: Insert new current versions.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_teacher
--             (teacher_id, first_name, last_name, full_name,
--              center_id, center_name, employment_status,
--              effective_from, effective_to, is_current, source_system,
--              row_hash, created_at, updated_at)
--         SELECT
--             ins.teacher_id,
--             ins.first_name,
--             ins.last_name,
--             ins.full_name,
--             ins.center_id,
--             ins.center_name,
--             ins.employment_status,
--             ins.effective_from,
--             NULL AS effective_to,
--             1 AS is_current,
--             COALESCE(ins.source_system, N'PROGRAM_OPS') AS source_system,
--             ins.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #insert_teacher AS ins;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new current SCD2 versions', N'succeeded',
--              (SELECT COUNT(*) FROM #insert_teacher), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted current versions for new teachers and changed teachers.');

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
--   Procedure: etl_admin.usp_incremental_load_dw_dim_child
--   Type     : Normal incremental SCD Type 1 dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_child
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
--         @max_child_key     INT = 0,
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
--         IF OBJECT_ID('tempdb..#changed_child') IS NOT NULL DROP TABLE #changed_child;
--         IF OBJECT_ID('tempdb..#new_child') IS NOT NULL DROP TABLE #new_child;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected source children from staging.
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
--             (N'01 - Detect affected staging children', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected children using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= child source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row and align identity.
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

--         SELECT @max_child_key = ISNULL(MAX(child_key), 0)
--         FROM dw.dim_child
--         WHERE child_key > 0;

--         DBCC CHECKIDENT (N'dw.dim_child', RESEED, @max_child_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown row and align identity', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Preserved or inserted child_key = -1. Identity reseeded to current MAX(child_key): ', @max_child_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 3: Detect changed and new child rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

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
--             src.source_system,
--             src.row_hash,
--             dim.child_key AS existing_child_key
--         INTO #changed_child
--         FROM #source_child AS src
--         INNER JOIN dw.dim_child AS dim
--             ON dim.child_id = src.child_id
--            AND dim.child_key <> -1
--         WHERE dim.row_hash IS NULL
--            OR dim.row_hash <> src.row_hash;

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
--             src.source_system,
--             src.row_hash
--         INTO #new_child
--         FROM #source_child AS src
--         LEFT JOIN dw.dim_child AS dim
--             ON dim.child_id = src.child_id
--            AND dim.child_key <> -1
--         WHERE dim.child_key IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Detect new and changed children', N'succeeded',
--              (SELECT COUNT(*) FROM #source_child),
--              (SELECT COUNT(*) FROM #new_child),
--              (SELECT COUNT(*) FROM #changed_child),
--              0,
--              @step_started, SYSDATETIME(),
--              N'Detected new children and changed SCD1 rows by comparing DW-attribute hash values.');

--         /*---------------------------------------------------------------------
--           Step 4: Update changed SCD1 child rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE dim
--         SET
--             dim.first_name      = ch.first_name,
--             dim.last_name       = ch.last_name,
--             dim.full_name       = ch.full_name,
--             dim.birth_date      = ch.birth_date,
--             dim.gender          = ch.gender,
--             dim.center_id       = ch.center_id,
--             dim.status          = ch.status,
--             dim.enrollment_date = ch.enrollment_date,
--             dim.source_system   = COALESCE(ch.source_system, N'PROGRAM_OPS'),
--             dim.row_hash        = ch.row_hash,
--             dim.updated_at      = SYSDATETIME()
--         FROM dw.dim_child AS dim
--         INNER JOIN #changed_child AS ch
--             ON ch.existing_child_key = dim.child_key
--         WHERE dim.child_key <> -1;

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Update changed SCD1 children', N'succeeded',
--              (SELECT COUNT(*) FROM #changed_child), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated changed child attributes in place because dim_child is SCD Type 1.');

--         /*---------------------------------------------------------------------
--           Step 5: Insert new child rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_child
--             (child_id, first_name, last_name, full_name,
--              birth_date, gender, center_id, status, enrollment_date,
--              source_system, row_hash, created_at, updated_at)
--         SELECT
--             nc.child_id,
--             nc.first_name,
--             nc.last_name,
--             nc.full_name,
--             nc.birth_date,
--             nc.gender,
--             nc.center_id,
--             nc.status,
--             nc.enrollment_date,
--             COALESCE(nc.source_system, N'PROGRAM_OPS') AS source_system,
--             nc.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #new_child AS nc;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new children', N'succeeded',
--              (SELECT COUNT(*) FROM #new_child), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new child dimension rows.');

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
--   Procedure: etl_admin.usp_incremental_load_dw_dim_domain
--   Type     : Normal incremental SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_domain
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
--         @max_domain_key    INT = 0,
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
--         IF OBJECT_ID('tempdb..#changed_domain') IS NOT NULL DROP TABLE #changed_domain;
--         IF OBJECT_ID('tempdb..#new_domain') IS NOT NULL DROP TABLE #new_domain;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected source domains from staging.
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
--             (N'01 - Detect affected staging domains', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected domains using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= domain source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row and align identity.
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

--         SELECT @max_domain_key = ISNULL(MAX(domain_key), 0)
--         FROM dw.dim_domain
--         WHERE domain_key > 0;

--         DBCC CHECKIDENT (N'dw.dim_domain', RESEED, @max_domain_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown row and align identity', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Preserved or inserted domain_key = -1. Identity reseeded to current MAX(domain_key): ', @max_domain_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 3: Detect changed and new domain rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             src.domain_id,
--             src.domain_name,
--             src.domain_description,
--             src.domain_status,
--             src.source_system,
--             src.row_hash,
--             dim.domain_key AS existing_domain_key
--         INTO #changed_domain
--         FROM #source_domain AS src
--         INNER JOIN dw.dim_domain AS dim
--             ON dim.domain_id = src.domain_id
--            AND dim.domain_key <> -1
--         WHERE dim.row_hash IS NULL
--            OR dim.row_hash <> src.row_hash;

--         SELECT
--             src.domain_id,
--             src.domain_name,
--             src.domain_description,
--             src.domain_status,
--             src.source_system,
--             src.row_hash
--         INTO #new_domain
--         FROM #source_domain AS src
--         LEFT JOIN dw.dim_domain AS dim
--             ON dim.domain_id = src.domain_id
--            AND dim.domain_key <> -1
--         WHERE dim.domain_key IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Detect new and changed domains', N'succeeded',
--              (SELECT COUNT(*) FROM #source_domain),
--              (SELECT COUNT(*) FROM #new_domain),
--              (SELECT COUNT(*) FROM #changed_domain),
--              0,
--              @step_started, SYSDATETIME(),
--              N'Detected new domains and changed SCD1 rows by comparing DW-attribute hash values.');

--         /*---------------------------------------------------------------------
--           Step 4: Update changed SCD1 domain rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE dim
--         SET
--             dim.domain_name        = ch.domain_name,
--             dim.domain_description = ch.domain_description,
--             dim.domain_status      = ch.domain_status,
--             dim.source_system      = COALESCE(ch.source_system, N'PROGRAM_OPS'),
--             dim.row_hash           = ch.row_hash,
--             dim.updated_at         = SYSDATETIME()
--         FROM dw.dim_domain AS dim
--         INNER JOIN #changed_domain AS ch
--             ON ch.existing_domain_key = dim.domain_key
--         WHERE dim.domain_key <> -1;

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Update changed SCD1 domains', N'succeeded',
--              (SELECT COUNT(*) FROM #changed_domain), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated changed domain attributes in place because dim_domain is SCD Type 1/reference.');

--         /*---------------------------------------------------------------------
--           Step 5: Insert new domain rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_domain
--             (domain_id, domain_name, domain_description, domain_status,
--              source_system, row_hash, created_at, updated_at)
--         SELECT
--             nd.domain_id,
--             nd.domain_name,
--             nd.domain_description,
--             nd.domain_status,
--             COALESCE(nd.source_system, N'PROGRAM_OPS') AS source_system,
--             nd.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #new_domain AS nd;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new domains', N'succeeded',
--              (SELECT COUNT(*) FROM #new_domain), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new domain dimension rows.');

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

-- /*=============================================================================
--   Procedure: etl_admin.usp_incremental_load_dw_dim_task
--   Type     : Normal / incremental SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_task
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
--         @max_key            INT = 0,
--         @identity_aligned   INT = 0,
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

--         IF OBJECT_ID('tempdb..#affected_domain_ids') IS NOT NULL DROP TABLE #affected_domain_ids;
--         IF OBJECT_ID('tempdb..#task_candidates') IS NOT NULL DROP TABLE #task_candidates;
--         IF OBJECT_ID('tempdb..#source_task') IS NOT NULL DROP TABLE #source_task;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected domain ids.
--                   dim_task denormalizes domain_name, so if a domain changes,
--                   related tasks must be considered affected too.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT DISTINCT d.id AS domain_id
--         INTO #affected_domain_ids
--         FROM Stg_ProgramOps_DB.stg_program_ops.domains AS d
--         WHERE d.is_valid = 1
--           AND d.id IS NOT NULL
--           AND d.source_updated_at >= @start_time
--           AND d.source_updated_at <  @end_time;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Detect affected domains for task denormalization', N'succeeded',
--              @@ROWCOUNT, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Detected domains whose changed domain_name may require dim_task updates.');

--         /*---------------------------------------------------------------------
--           Step 2: Read affected task candidates from staging and validate them.
--                   Candidates are affected either by their own source_updated_at
--                   or by a changed domain_id.
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
--         WHERE (
--                   tt.source_updated_at >= @start_time
--               AND tt.source_updated_at <  @end_time
--               )
--            OR EXISTS (
--                   SELECT 1
--                   FROM #affected_domain_ids AS ad
--                   WHERE ad.domain_id = tt.domain_id
--               );

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
--           AND (
--                   (
--                       ctp.source_updated_at >= @start_time
--                   AND ctp.source_updated_at <  @end_time
--                   )
--                OR EXISTS (
--                       SELECT 1
--                       FROM #affected_domain_ids AS ad
--                       WHERE ad.domain_id = ctp.domain_id
--                   )
--               );

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
--           AND (
--                   (
--                       dta.source_updated_at >= @start_time
--                   AND dta.source_updated_at <  @end_time
--                   )
--                OR EXISTS (
--                       SELECT 1
--                       FROM #affected_domain_ids AS ad
--                       WHERE ad.domain_id = dta.domain_id
--                   )
--               );

--         SELECT @rows_read = COUNT(*)
--         FROM #task_candidates;

--         SELECT @rows_rejected = COUNT(*)
--         FROM #task_candidates
--         WHERE validation_message IS NOT NULL;

--         /*---------------------------------------------------------------------
--           Step 3: Normalize and collapse candidates to the dim_task grain.
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
--             (N'02 - Read, validate, and normalize affected staging tasks', N'succeeded',
--              @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected task candidates using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126),
--                     N' <= source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126),
--                     N', plus tasks related to changed domains.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 4: Ensure unknown row exists and is preserved.
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
--             (N'03 - Ensure unknown task row', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(), N'Preserved or inserted task_key = -1.');

--         /*---------------------------------------------------------------------
--           Step 5: Align identity with current max positive task_key.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT @max_key = ISNULL(MAX(CASE WHEN task_key > 0 THEN task_key ELSE 0 END), 0)
--         FROM dw.dim_task;

--         DBCC CHECKIDENT ('dw.dim_task', RESEED, @max_key) WITH NO_INFOMSGS;
--         SET @identity_aligned = 1;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Align dim_task identity', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Reseeded dim_task identity to current max positive key: ', @max_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 6: Update changed SCD Type 1 rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE dt
--         SET
--             dt.task_title        = st.task_title,
--             dt.domain_id         = st.domain_id,
--             dt.domain_name       = st.domain_name,
--             dt.is_template_based = st.is_template_based,
--             dt.task_description  = st.task_description,
--             dt.task_status       = st.task_status,
--             dt.source_system     = st.source_system,
--             dt.row_hash          = st.row_hash,
--             dt.updated_at        = SYSDATETIME()
--         FROM dw.dim_task AS dt
--         INNER JOIN #source_task AS st
--             ON (
--                     st.task_template_id IS NOT NULL
--                 AND dt.task_template_id = st.task_template_id
--                )
--             OR (
--                     st.task_template_id IS NULL
--                 AND dt.task_template_id IS NULL
--                 AND ISNULL(dt.domain_id, -2147483648) = ISNULL(st.domain_id, -2147483648)
--                 AND ISNULL(NULLIF(LTRIM(RTRIM(dt.task_title)), N''), N'<NULL>') = ISNULL(st.task_title, N'<NULL>')
--                )
--         WHERE dt.task_key <> -1
--           AND (
--                  dt.row_hash IS NULL
--               OR st.row_hash IS NULL
--               OR dt.row_hash <> st.row_hash
--           );

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Update changed dim_task rows', N'succeeded', 0, 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated existing SCD Type 1 rows where the calculated DW hash changed.');

--         /*---------------------------------------------------------------------
--           Step 7: Insert new task dimension rows.
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
--         FROM #source_task AS st
--         WHERE NOT EXISTS
--         (
--             SELECT 1
--             FROM dw.dim_task AS dt
--             WHERE dt.task_key <> -1
--               AND (
--                     (
--                         st.task_template_id IS NOT NULL
--                     AND dt.task_template_id = st.task_template_id
--                     )
--                  OR (
--                         st.task_template_id IS NULL
--                     AND dt.task_template_id IS NULL
--                     AND ISNULL(dt.domain_id, -2147483648) = ISNULL(st.domain_id, -2147483648)
--                     AND ISNULL(NULLIF(LTRIM(RTRIM(dt.task_title)), N''), N'<NULL>') = ISNULL(st.task_title, N'<NULL>')
--                     )
--                   )
--         );

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'06 - Insert new dim_task rows', N'succeeded', 0, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted task dimension rows that did not already exist at the task natural grain.');

--         COMMIT TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 8: Persist step-level logs.
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



-- /*=============================================================================
--   Procedure: etl_admin.usp_incremental_load_dw_dim_score_scale
--   Type     : Normal incremental SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_score_scale
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id          INT,
--         @created_by            NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started     DATETIME2(0) = SYSDATETIME(),
--         @step_started          DATETIME2(0),
--         @rows_read             INT = 0,
--         @rows_inserted         INT = 0,
--         @rows_updated          INT = 0,
--         @rows_rejected         INT = 0,
--         @unknown_inserted      INT = 0,
--         @max_score_scale_key   INT = 0,
--         @error_message         NVARCHAR(MAX);

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
--         IF OBJECT_ID('tempdb..#changed_score_scale') IS NOT NULL DROP TABLE #changed_score_scale;
--         IF OBJECT_ID('tempdb..#new_score_scale') IS NOT NULL DROP TABLE #new_score_scale;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected source score scales from staging.
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
--             (N'01 - Detect affected staging score scales', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected score scales using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= score_scales source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row and align identity.
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

--         SELECT @max_score_scale_key = ISNULL(MAX(score_scale_key), 0)
--         FROM dw.dim_score_scale
--         WHERE score_scale_key > 0;

--         DBCC CHECKIDENT (N'dw.dim_score_scale', RESEED, @max_score_scale_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown row and align identity', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Preserved or inserted score_scale_key = -1. Identity reseeded to current MAX(score_scale_key): ', @max_score_scale_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 3: Detect changed and new score scale rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT
--             src.score_scale_id,
--             src.scale_name,
--             src.min_score,
--             src.max_score,
--             src.scale_description,
--             src.scale_status,
--             src.source_system,
--             src.row_hash,
--             dim.score_scale_key AS existing_score_scale_key
--         INTO #changed_score_scale
--         FROM #source_score_scale AS src
--         INNER JOIN dw.dim_score_scale AS dim
--             ON dim.score_scale_id = src.score_scale_id
--            AND dim.score_scale_key <> -1
--         WHERE dim.row_hash IS NULL
--            OR dim.row_hash <> src.row_hash;

--         SELECT
--             src.score_scale_id,
--             src.scale_name,
--             src.min_score,
--             src.max_score,
--             src.scale_description,
--             src.scale_status,
--             src.source_system,
--             src.row_hash
--         INTO #new_score_scale
--         FROM #source_score_scale AS src
--         LEFT JOIN dw.dim_score_scale AS dim
--             ON dim.score_scale_id = src.score_scale_id
--            AND dim.score_scale_key <> -1
--         WHERE dim.score_scale_key IS NULL;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Detect new and changed score scales', N'succeeded',
--              (SELECT COUNT(*) FROM #source_score_scale),
--              (SELECT COUNT(*) FROM #new_score_scale),
--              (SELECT COUNT(*) FROM #changed_score_scale),
--              0,
--              @step_started, SYSDATETIME(),
--              N'Detected new score scales and changed SCD1 rows by comparing DW-attribute hash values.');

--         /*---------------------------------------------------------------------
--           Step 4: Update changed SCD1 score scale rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE dim
--         SET
--             dim.scale_name        = ch.scale_name,
--             dim.min_score         = ch.min_score,
--             dim.max_score         = ch.max_score,
--             dim.scale_description = ch.scale_description,
--             dim.scale_status      = ch.scale_status,
--             dim.source_system     = COALESCE(ch.source_system, N'PROGRAM_OPS'),
--             dim.row_hash          = ch.row_hash,
--             dim.updated_at        = SYSDATETIME()
--         FROM dw.dim_score_scale AS dim
--         INNER JOIN #changed_score_scale AS ch
--             ON ch.existing_score_scale_key = dim.score_scale_key
--         WHERE dim.score_scale_key <> -1;

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Update changed SCD1 score scales', N'succeeded',
--              (SELECT COUNT(*) FROM #changed_score_scale), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated changed score scale attributes in place because dim_score_scale is SCD Type 1/reference.');

--         /*---------------------------------------------------------------------
--           Step 5: Insert new score scale rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         INSERT INTO dw.dim_score_scale
--             (score_scale_id, scale_name, min_score, max_score,
--              scale_description, scale_status, source_system, row_hash, created_at, updated_at)
--         SELECT
--             ns.score_scale_id,
--             ns.scale_name,
--             ns.min_score,
--             ns.max_score,
--             ns.scale_description,
--             ns.scale_status,
--             COALESCE(ns.source_system, N'PROGRAM_OPS') AS source_system,
--             ns.row_hash,
--             SYSDATETIME() AS created_at,
--             NULL AS updated_at
--         FROM #new_score_scale AS ns;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new score scales', N'succeeded',
--              (SELECT COUNT(*) FROM #new_score_scale), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new score scale dimension rows.');

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

-- /*=============================================================================
--   Procedure: etl_admin.usp_incremental_load_dw_dim_assessment_status
--   Type     : Normal incremental SCD Type 1 / static reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_assessment_status
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
--         @max_status_key    INT = 0,
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
--           Step 1: Read and validate affected assessment status candidates.
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
--             (N'01 - Read and validate affected assessment statuses', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Loaded affected source candidates using half-open range: ',
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
--           Step 3: Align identity with current max key before incremental inserts.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT @max_status_key = ISNULL(MAX(assessment_status_key), 0)
--         FROM dw.dim_assessment_status
--         WHERE assessment_status_key > 0;

--         DBCC CHECKIDENT (N'dw.dim_assessment_status', RESEED, @max_status_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Align dim_assessment_status identity', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Reseeded identity to current maximum positive assessment_status_key = ', @max_status_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 4: Update changed SCD1/static reference rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE tgt
--         SET
--             tgt.assessment_status_title = src.assessment_status_title,
--             tgt.assessment_status_category = src.assessment_status_category,
--             tgt.is_successful_assessment = src.is_successful_assessment,
--             tgt.is_failure_assessment = src.is_failure_assessment,
--             tgt.source_system = COALESCE(src.source_system, N'PROGRAM_OPS'),
--             tgt.updated_at = SYSDATETIME()
--         FROM dw.dim_assessment_status AS tgt
--         INNER JOIN #source_assessment_status AS src
--             ON tgt.assessment_status_code = src.assessment_status_code
--         WHERE tgt.assessment_status_key <> -1
--           AND (
--                 ISNULL(tgt.assessment_status_title, N'') <> ISNULL(src.assessment_status_title, N'')
--              OR ISNULL(tgt.assessment_status_category, N'') <> ISNULL(src.assessment_status_category, N'')
--              OR ISNULL(tgt.is_successful_assessment, 0) <> ISNULL(src.is_successful_assessment, 0)
--              OR ISNULL(tgt.is_failure_assessment, 0) <> ISNULL(src.is_failure_assessment, 0)
--              OR ISNULL(tgt.source_system, N'') <> ISNULL(COALESCE(src.source_system, N'PROGRAM_OPS'), N'')
--           );

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Update changed assessment statuses', N'succeeded',
--              (SELECT COUNT(*) FROM #source_assessment_status), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated existing SCD1/static reference rows where descriptive attributes changed.');

--         /*---------------------------------------------------------------------
--           Step 5: Insert new SCD1/static reference rows.
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
--         FROM #source_assessment_status AS src
--         LEFT JOIN dw.dim_assessment_status AS tgt
--             ON tgt.assessment_status_code = src.assessment_status_code
--         WHERE tgt.assessment_status_key IS NULL;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new assessment statuses', N'succeeded',
--              (SELECT COUNT(*) FROM #source_assessment_status), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted one new SCD1/static reference row per new normalized assessment status code.');

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

-- /*=============================================================================
--   Procedure: etl_admin.usp_incremental_load_dw_dim_no_score_reason
--   Type     : Incremental SCD Type 1 reference dimension load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_dim_no_score_reason
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
--         @current_identity   INT = 0,
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

--         IF OBJECT_ID('tempdb..#no_score_reason_candidates') IS NOT NULL DROP TABLE #no_score_reason_candidates;
--         IF OBJECT_ID('tempdb..#source_no_score_reason') IS NOT NULL DROP TABLE #source_no_score_reason;

--         /*---------------------------------------------------------------------
--           Step 1: Detect affected source records in the half-open time window.
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
--             (N'01 - Detect and validate affected no-score reasons', N'succeeded', @rows_read, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Detected affected source rows using half-open range: ',
--                     CONVERT(NVARCHAR(30), @start_time, 126), N' <= no_score_reasons source_updated_at < ',
--                     CONVERT(NVARCHAR(30), @end_time, 126), N'.'));

--         BEGIN TRANSACTION;

--         /*---------------------------------------------------------------------
--           Step 2: Ensure unknown row exists and align identity with max key.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

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

--         SELECT @current_identity = ISNULL(MAX(no_score_reason_key), 0)
--         FROM dw.dim_no_score_reason
--         WHERE no_score_reason_key > 0;

--         DBCC CHECKIDENT ('dw.dim_no_score_reason', RESEED, @current_identity) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Ensure unknown row and align identity', N'succeeded', 0, @unknown_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Ensured no_score_reason_key = -1 exists. Identity reseeded to ', @current_identity, N'.'));

--         /*---------------------------------------------------------------------
--           Step 3: Update changed Type 1 rows.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE tgt
--         SET
--             tgt.reason_title       = src.reason_title,
--             tgt.reason_description = src.reason_description,
--             tgt.reason_category    = src.reason_category,
--             tgt.is_child_related   = src.is_child_related,
--             tgt.is_teacher_related = src.is_teacher_related,
--             tgt.is_center_related  = src.is_center_related,
--             tgt.is_system_related  = src.is_system_related,
--             tgt.source_system      = src.source_system,
--             tgt.row_hash           = src.row_hash,
--             tgt.updated_at         = SYSDATETIME()
--         FROM dw.dim_no_score_reason AS tgt
--         INNER JOIN #source_no_score_reason AS src
--             ON tgt.no_score_reason_id = src.no_score_reason_id
--         WHERE tgt.no_score_reason_key <> -1
--           AND (
--                 (tgt.row_hash IS NULL AND src.row_hash IS NOT NULL)
--              OR (tgt.row_hash IS NOT NULL AND src.row_hash IS NULL)
--              OR (tgt.row_hash <> src.row_hash)
--           );

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Update changed no-score reason rows', N'succeeded',
--              (SELECT COUNT(*) FROM #source_no_score_reason), 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated existing SCD Type 1 rows where the DW attribute hash changed.');

--         /*---------------------------------------------------------------------
--           Step 4: Insert new Type 1 rows.
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
--         LEFT JOIN dw.dim_no_score_reason AS tgt
--             ON tgt.no_score_reason_id = src.no_score_reason_id
--            AND tgt.no_score_reason_key <> -1
--         WHERE tgt.no_score_reason_key IS NULL;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Insert new no-score reason rows', N'succeeded',
--              (SELECT COUNT(*) FROM #source_no_score_reason), @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new source no-score reasons that did not already exist in dw.dim_no_score_reason.');

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
--   Procedure: etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress
--   Type     : Incremental transaction fact load
-- =============================================================================*/

-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress
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
--         @max_existing_key   BIGINT,
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
--           Step 1: Detect affected daily task assignments.
--                   Incremental load updates only these affected business records.
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
--           Step 2: Build fact candidates for affected assignments.
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
--              N'Built planned and assessed transaction candidates for affected assignments.');

--         /*---------------------------------------------------------------------
--           Step 3: Resolve dimension keys and calculate source fact hash.
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
--           Step 4: Align identity with MAX(key) before incremental inserts.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         SELECT @max_existing_key = ISNULL(MAX(student_task_progress_key), 0)
--         FROM dw.fact_tran_student_task_progress;

--         DBCC CHECKIDENT ('dw.fact_tran_student_task_progress', RESEED, @max_existing_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Align fact identity', N'succeeded', 0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Reseeded fact identity to current MAX(student_task_progress_key) = ', @max_existing_key, N'.'));

--         /*---------------------------------------------------------------------
--           Step 5: Update changed fact rows only.
--         ---------------------------------------------------------------------*/
--         SET @step_started = SYSDATETIME();

--         UPDATE tgt
--         SET
--             tgt.date_key                        = src.date_key,
--             tgt.child_key                       = src.child_key,
--             tgt.center_key                      = src.center_key,
--             tgt.teacher_key                     = src.teacher_key,
--             tgt.domain_key                      = src.domain_key,
--             tgt.task_key                        = src.task_key,
--             tgt.score_scale_key                 = src.score_scale_key,
--             tgt.assessment_status_key           = src.assessment_status_key,
--             tgt.no_score_reason_key             = src.no_score_reason_key,
--             tgt.attempt_no                      = src.attempt_no,
--             tgt.raw_score                       = src.raw_score,
--             tgt.normalized_score                = src.normalized_score,
--             tgt.is_completed                    = src.is_completed,
--             tgt.is_planned                      = src.is_planned,
--             tgt.is_scored                       = src.is_scored,
--             tgt.is_not_scored                   = src.is_not_scored,
--             tgt.is_cancelled                    = src.is_cancelled,
--             tgt.is_incomplete                   = src.is_incomplete,
--             tgt.is_refused                      = src.is_refused,
--             tgt.is_absent                       = src.is_absent,
--             tgt.is_center_closed                = src.is_center_closed,
--             tgt.is_assessed                     = src.is_assessed,
--             tgt.source_assessment_session_id    = src.source_assessment_session_id,
--             tgt.source_child_task_plan_id       = src.source_child_task_plan_id,
--             tgt.source_system                   = src.source_system,
--             tgt.etl_batch_id                    = @etl_batch_id,
--             tgt.loaded_at                       = SYSDATETIME()
--         FROM dw.fact_tran_student_task_progress AS tgt
--         INNER JOIN #source_fact AS src
--             ON tgt.source_daily_task_assignment_id = src.source_daily_task_assignment_id
--            AND ISNULL(tgt.source_task_assessment_id, -1) = ISNULL(src.source_task_assessment_id, -1)
--         CROSS APPLY
--         (
--             SELECT HASHBYTES('SHA2_256', CONCAT_WS(N'|',
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.date_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.child_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.center_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.teacher_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.domain_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.task_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.score_scale_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.assessment_status_key, -1)),
--                 CONVERT(NVARCHAR(30), ISNULL(tgt.no_score_reason_key, -1)),
--                 ISNULL(CONVERT(NVARCHAR(30), tgt.attempt_no), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(50), tgt.raw_score), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(50), tgt.normalized_score), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_completed), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_planned), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_scored), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_not_scored), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_cancelled), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_incomplete), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_refused), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_absent), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_center_closed), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(1), tgt.is_assessed), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), tgt.source_daily_task_assignment_id), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), tgt.source_task_assessment_id), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), tgt.source_assessment_session_id), N'<NULL>'),
--                 ISNULL(CONVERT(NVARCHAR(30), tgt.source_child_task_plan_id), N'<NULL>'),
--                 ISNULL(tgt.source_system, N'<NULL>')
--             )) AS fact_row_hash
--         ) AS tgt_hash
--         WHERE tgt_hash.fact_row_hash <> src.fact_row_hash;

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Update changed fact rows', N'succeeded',
--              0, 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated existing transaction fact rows whose calculated hash changed.');

--         /*---------------------------------------------------------------------
--           Step 6: Insert new fact rows.
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
--             src.date_key, src.child_key, src.center_key, src.teacher_key, src.domain_key, src.task_key,
--             src.score_scale_key, src.assessment_status_key, src.no_score_reason_key,
--             src.attempt_no, src.raw_score, src.normalized_score, src.is_completed, src.is_planned,
--             src.is_scored, src.is_not_scored, src.is_cancelled, src.is_incomplete, src.is_refused,
--             src.is_absent, src.is_center_closed, src.is_assessed,
--             src.source_daily_task_assignment_id, src.source_task_assessment_id,
--             src.source_assessment_session_id, src.source_child_task_plan_id,
--             src.source_system, @etl_batch_id, SYSDATETIME()
--         FROM #source_fact AS src
--         WHERE NOT EXISTS
--         (
--             SELECT 1
--             FROM dw.fact_tran_student_task_progress AS tgt
--             WHERE tgt.source_daily_task_assignment_id = src.source_daily_task_assignment_id
--               AND ISNULL(tgt.source_task_assessment_id, -1) = ISNULL(src.source_task_assessment_id, -1)
--         );

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'06 - Insert new fact rows', N'succeeded',
--              0, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new transaction fact rows for affected source records.');

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
--   2) FIX: fact_daily_student_task_progress incremental
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id                  INT,
--         @source_fact_batch_id          INT,
--         @created_by                    NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started             DATETIME2(0) = SYSDATETIME(),
--         @step_started                  DATETIME2(0),
--         @rows_read                     INT = 0,
--         @rows_inserted                 INT = 0,
--         @rows_updated                  INT = 0,
--         @rows_rejected                 INT = 0,
--         @snapshot_candidate_rows       INT = 0,
--         @transaction_history_rows_read INT = 0,
--         @day_source_rows_read          INT = 0,
--         @day_snapshot_rows             INT = 0,
--         @day_rows_inserted             INT = 0,
--         @day_rows_updated              INT = 0,
--         @affected_transaction_rows     INT = 0,
--         @missing_date_count            INT = 0,
--         @loop_day_count                INT = 0,
--         @requested_start_date          DATE,
--         @current_snapshot_date         DATE,
--         @end_snapshot_date             DATE,
--         @earliest_affected_date        DATE,
--         @current_date_key              INT,
--         @max_existing_key              BIGINT = 0,
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

--         /* Step 1: Align identity. */
--         SET @step_started = SYSDATETIME();

--         SELECT @max_existing_key = ISNULL(MAX(daily_student_task_progress_key), 0)
--         FROM dw.fact_daily_student_task_progress;

--         DBCC CHECKIDENT ('dw.fact_daily_student_task_progress', RESEED, @max_existing_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Align daily snapshot fact identity', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Reseeded daily_student_task_progress_key to ', @max_existing_key, N'.'));

--         /* Step 2: Resolve latest successful transaction fact batch. */
--         SET @step_started = SYSDATETIME();

--         SELECT TOP (1)
--             @source_fact_batch_id = etl_batch_id
--         FROM etl_admin.etl_load_log
--         WHERE target_database = N'Charity_DW_DB'
--           AND target_schema   = N'dw'
--           AND target_table    = N'fact_tran_student_task_progress'
--           AND load_status     = N'succeeded'
--           AND etl_batch_id    < @etl_batch_id
--         ORDER BY ended_at DESC, etl_batch_id DESC;

--         IF @source_fact_batch_id IS NULL
--         BEGIN
--             RAISERROR('Cannot find latest successful fact_tran_student_task_progress batch for downstream daily snapshot fact.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Resolve upstream transaction fact batch', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Using fact_tran_student_task_progress etl_batch_id = ', @source_fact_batch_id,
--                     N' as affected transaction set.'));

--         /* Step 3: Detect affected snapshot start date from affected transaction batch. */
--         SET @step_started = SYSDATETIME();

--         SELECT
--             @affected_transaction_rows = COUNT(1),
--             @earliest_affected_date = MIN(tx_date.FullDateAlternateKey)
--         FROM dw.fact_tran_student_task_progress AS ft
--         LEFT JOIN dw.dim_date AS tx_date
--             ON tx_date.TimeKey = ft.date_key
--         WHERE ft.etl_batch_id = @source_fact_batch_id;

--         SET @requested_start_date = CONVERT(DATE, @start_time);

--         SET @current_snapshot_date =
--             CASE
--                 WHEN @earliest_affected_date IS NOT NULL
--                      AND @earliest_affected_date < @requested_start_date
--                 THEN @earliest_affected_date
--                 ELSE @requested_start_date
--             END;

--         SET @end_snapshot_date = CONVERT(DATE, @end_time);

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Detect affected snapshot dates', N'succeeded',
--              @affected_transaction_rows, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Affected transaction rows in upstream batch: ', @affected_transaction_rows,
--                     N'. Daily snapshot loop starts at ',
--                     CONVERT(NVARCHAR(10), @current_snapshot_date, 120),
--                     N' and ends before ',
--                     CONVERT(NVARCHAR(10), @end_snapshot_date, 120),
--                     N'.'));

--         /* Step 4: Daily snapshot loop. */
--         SET @step_started = SYSDATETIME();

--         WHILE @current_snapshot_date < @end_snapshot_date
--         BEGIN
--             SET @current_date_key = NULL;
--             SET @day_source_rows_read = 0;
--             SET @day_snapshot_rows = 0;
--             SET @day_rows_inserted = 0;
--             SET @day_rows_updated = 0;

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

--             SELECT @day_source_rows_read = COUNT(1)
--             FROM dw.fact_tran_student_task_progress AS ft
--             LEFT JOIN dw.dim_date AS tx_date
--                 ON tx_date.TimeKey = ft.date_key
--             WHERE
--                 tx_date.FullDateAlternateKey <= @current_snapshot_date
--                 OR COALESCE(ft.date_key, -1) = -1;

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
--                     OR COALESCE(ft.date_key, -1) = -1
--                 GROUP BY
--                     COALESCE(ft.child_key, -1),
--                     COALESCE(ft.center_key, -1),
--                     COALESCE(ft.teacher_key, -1)
--             ) AS agg;

--             SET @day_snapshot_rows = @@ROWCOUNT;
--             SET @transaction_history_rows_read += @day_source_rows_read;
--             SET @snapshot_candidate_rows += @day_snapshot_rows;

--             UPDATE tgt
--             SET
--                 tgt.raw_score             = src.raw_score,
--                 tgt.min_score             = src.min_score,
--                 tgt.max_score             = src.max_score,
--                 tgt.normalized_score      = src.normalized_score,
--                 tgt.planned_task_count    = src.planned_task_count,
--                 tgt.assessment_count      = src.assessment_count,
--                 tgt.completed_task_count  = src.completed_task_count,
--                 tgt.scored_task_count     = src.scored_task_count,
--                 tgt.not_scored_task_count = src.not_scored_task_count,
--                 tgt.source_system         = src.source_system,
--                 tgt.etl_batch_id          = @etl_batch_id,
--                 tgt.loaded_at             = SYSDATETIME()
--             FROM dw.fact_daily_student_task_progress AS tgt
--             INNER JOIN #day_snapshot AS src
--                 ON COALESCE(tgt.date_key, -1)    = COALESCE(src.date_key, -1)
--                AND COALESCE(tgt.child_key, -1)   = COALESCE(src.child_key, -1)
--                AND COALESCE(tgt.center_key, -1)  = COALESCE(src.center_key, -1)
--                AND COALESCE(tgt.teacher_key, -1) = COALESCE(src.teacher_key, -1)
--             WHERE
--                 (tgt.raw_score <> src.raw_score OR (tgt.raw_score IS NULL AND src.raw_score IS NOT NULL) OR (tgt.raw_score IS NOT NULL AND src.raw_score IS NULL))
--                 OR (tgt.min_score <> src.min_score OR (tgt.min_score IS NULL AND src.min_score IS NOT NULL) OR (tgt.min_score IS NOT NULL AND src.min_score IS NULL))
--                 OR (tgt.max_score <> src.max_score OR (tgt.max_score IS NULL AND src.max_score IS NOT NULL) OR (tgt.max_score IS NOT NULL AND src.max_score IS NULL))
--                 OR (tgt.normalized_score <> src.normalized_score OR (tgt.normalized_score IS NULL AND src.normalized_score IS NOT NULL) OR (tgt.normalized_score IS NOT NULL AND src.normalized_score IS NULL))
--                 OR ISNULL(tgt.planned_task_count, -1)    <> ISNULL(src.planned_task_count, -1)
--                 OR ISNULL(tgt.assessment_count, -1)      <> ISNULL(src.assessment_count, -1)
--                 OR ISNULL(tgt.completed_task_count, -1)  <> ISNULL(src.completed_task_count, -1)
--                 OR ISNULL(tgt.scored_task_count, -1)     <> ISNULL(src.scored_task_count, -1)
--                 OR ISNULL(tgt.not_scored_task_count, -1) <> ISNULL(src.not_scored_task_count, -1);

--             SET @day_rows_updated = @@ROWCOUNT;
--             SET @rows_updated += @day_rows_updated;

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
--             FROM #day_snapshot AS src
--             LEFT JOIN dw.fact_daily_student_task_progress AS tgt
--                 ON COALESCE(tgt.date_key, -1)    = COALESCE(src.date_key, -1)
--                AND COALESCE(tgt.child_key, -1)   = COALESCE(src.child_key, -1)
--                AND COALESCE(tgt.center_key, -1)  = COALESCE(src.center_key, -1)
--                AND COALESCE(tgt.teacher_key, -1) = COALESCE(src.teacher_key, -1)
--             WHERE tgt.daily_student_task_progress_key IS NULL;

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
--             (N'04 - Upsert daily snapshots using daily loop', N'succeeded',
--              @rows_read, @rows_inserted, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Days processed: ', @loop_day_count,
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
--         SET @error_message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

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
--   3) FIX: fact_child_snapshot_accumulation incremental
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id              INT,
--         @source_fact_batch_id      INT,
--         @created_by                NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started         DATETIME2(0) = SYSDATETIME(),
--         @step_started              DATETIME2(0),
--         @rows_read                 INT = 0,
--         @rows_inserted             INT = 0,
--         @rows_updated              INT = 0,
--         @rows_rejected             INT = 0,
--         @affected_transaction_rows INT = 0,
--         @affected_lifecycle_rows   INT = 0,
--         @lifecycle_candidate_rows  INT = 0,
--         @snapshot_date             DATE,
--         @snapshot_date_key         INT,
--         @max_existing_key          BIGINT = 0,
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

--         /* Step 1: Align identity. */
--         SET @step_started = SYSDATETIME();

--         SELECT @max_existing_key = ISNULL(MAX(child_snapshot_key), 0)
--         FROM dw.fact_child_snapshot_accumulation;

--         DBCC CHECKIDENT ('dw.fact_child_snapshot_accumulation', RESEED, @max_existing_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Align child lifecycle fact identity', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Reseeded child_snapshot_key to ', @max_existing_key, N'.'));

--         /* Step 2: Resolve snapshot date key. */
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
--                     N', snapshot_date_key = ', @snapshot_date_key, N'.'));

--         /* Step 3: Resolve latest successful transaction fact batch. */
--         SET @step_started = SYSDATETIME();

--         SELECT TOP (1)
--             @source_fact_batch_id = etl_batch_id
--         FROM etl_admin.etl_load_log
--         WHERE target_database = N'Charity_DW_DB'
--           AND target_schema   = N'dw'
--           AND target_table    = N'fact_tran_student_task_progress'
--           AND load_status     = N'succeeded'
--           AND etl_batch_id    < @etl_batch_id
--         ORDER BY ended_at DESC, etl_batch_id DESC;

--         IF @source_fact_batch_id IS NULL
--         BEGIN
--             RAISERROR('Cannot find latest successful fact_tran_student_task_progress batch for downstream lifecycle fact.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Resolve upstream transaction fact batch', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Using fact_tran_student_task_progress etl_batch_id = ', @source_fact_batch_id,
--                     N' as affected transaction set.'));

--         /* Step 4: Detect affected lifecycles from upstream transaction fact batch. */
--         SET @step_started = SYSDATETIME();

--         IF OBJECT_ID('tempdb..#affected_lifecycle') IS NOT NULL
--             DROP TABLE #affected_lifecycle;

--         SELECT
--             COALESCE(ft.child_key, -1)   AS child_key,
--             COALESCE(ft.center_key, -1)  AS center_key,
--             COALESCE(ft.teacher_key, -1) AS teacher_key
--         INTO #affected_lifecycle
--         FROM dw.fact_tran_student_task_progress AS ft
--         WHERE ft.etl_batch_id = @source_fact_batch_id
--         GROUP BY
--             COALESCE(ft.child_key, -1),
--             COALESCE(ft.center_key, -1),
--             COALESCE(ft.teacher_key, -1);

--         SET @affected_lifecycle_rows = @@ROWCOUNT;

--         SELECT @affected_transaction_rows = COUNT(1)
--         FROM dw.fact_tran_student_task_progress AS ft
--         WHERE ft.etl_batch_id = @source_fact_batch_id;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Detect affected child lifecycles', N'succeeded',
--              @affected_transaction_rows, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Affected transaction rows in upstream batch: ', @affected_transaction_rows,
--                     N'. Affected lifecycle grain rows: ', @affected_lifecycle_rows,
--                     N'.'));

--         /* Step 5: Recalculate affected lifecycle rows from full history. */
--         SET @step_started = SYSDATETIME();

--         IF OBJECT_ID('tempdb..#lifecycle_snapshot') IS NOT NULL
--             DROP TABLE #lifecycle_snapshot;

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
--             INNER JOIN #affected_lifecycle AS affected
--                 ON COALESCE(ft.child_key, -1)   = affected.child_key
--                AND COALESCE(ft.center_key, -1)  = affected.center_key
--                AND COALESCE(ft.teacher_key, -1) = affected.teacher_key
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
--             (N'05 - Recalculate affected lifecycles from full history', N'succeeded',
--              @lifecycle_candidate_rows, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Recalculated affected lifecycle rows from full transaction history.');

--         /* Step 6: Update changed lifecycle rows. */
--         SET @step_started = SYSDATETIME();

--         UPDATE tgt
--         SET
--             tgt.snapshot_date_key          = src.snapshot_date_key,
--             tgt.planned_task_count        = src.planned_task_count,
--             tgt.assessment_count          = src.assessment_count,
--             tgt.completed_task_count      = src.completed_task_count,
--             tgt.scored_task_count         = src.scored_task_count,
--             tgt.first_plan_date_key       = src.first_plan_date_key,
--             tgt.last_plan_date_key        = src.last_plan_date_key,
--             tgt.first_assessment_date_key = src.first_assessment_date_key,
--             tgt.last_assessment_date_key  = src.last_assessment_date_key,
--             tgt.source_system             = src.source_system,
--             tgt.etl_batch_id              = @etl_batch_id,
--             tgt.loaded_at                 = SYSDATETIME()
--         FROM dw.fact_child_snapshot_accumulation AS tgt
--         INNER JOIN #lifecycle_snapshot AS src
--             ON COALESCE(tgt.child_key, -1)   = COALESCE(src.child_key, -1)
--            AND COALESCE(tgt.center_key, -1)  = COALESCE(src.center_key, -1)
--            AND COALESCE(tgt.teacher_key, -1) = COALESCE(src.teacher_key, -1)
--         WHERE
--             ISNULL(tgt.snapshot_date_key, -1)            <> ISNULL(src.snapshot_date_key, -1)
--             OR ISNULL(tgt.planned_task_count, -1)        <> ISNULL(src.planned_task_count, -1)
--             OR ISNULL(tgt.assessment_count, -1)          <> ISNULL(src.assessment_count, -1)
--             OR ISNULL(tgt.completed_task_count, -1)      <> ISNULL(src.completed_task_count, -1)
--             OR ISNULL(tgt.scored_task_count, -1)         <> ISNULL(src.scored_task_count, -1)
--             OR ISNULL(tgt.first_plan_date_key, -1)       <> ISNULL(src.first_plan_date_key, -1)
--             OR ISNULL(tgt.last_plan_date_key, -1)        <> ISNULL(src.last_plan_date_key, -1)
--             OR ISNULL(tgt.first_assessment_date_key, -1) <> ISNULL(src.first_assessment_date_key, -1)
--             OR ISNULL(tgt.last_assessment_date_key, -1)  <> ISNULL(src.last_assessment_date_key, -1);

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'06 - Update changed lifecycle rows', N'succeeded',
--              @lifecycle_candidate_rows, 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated existing accumulating snapshot rows where lifecycle state changed.');

--         /* Step 7: Insert new lifecycle rows. */
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
--         FROM #lifecycle_snapshot AS src
--         LEFT JOIN dw.fact_child_snapshot_accumulation AS tgt
--             ON COALESCE(tgt.child_key, -1)   = COALESCE(src.child_key, -1)
--            AND COALESCE(tgt.center_key, -1)  = COALESCE(src.center_key, -1)
--            AND COALESCE(tgt.teacher_key, -1) = COALESCE(src.teacher_key, -1)
--         WHERE tgt.child_snapshot_key IS NULL;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'07 - Insert new lifecycle rows', N'succeeded',
--              @lifecycle_candidate_rows, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new accumulating snapshot lifecycle rows.');

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
--         SET @error_message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

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
--   1) FIX: fact_child_task_event incremental
-- =============================================================================*/
-- CREATE OR ALTER PROCEDURE etl_admin.usp_incremental_load_dw_fact_child_task_event
--     @start_time DATETIME2(0),
--     @end_time   DATETIME2(0)
-- AS
-- BEGIN
--     SET NOCOUNT ON;
--     SET XACT_ABORT ON;

--     DECLARE
--         @etl_batch_id              INT,
--         @source_fact_batch_id      INT,
--         @created_by                NVARCHAR(128) = COALESCE(SUSER_SNAME(), SYSTEM_USER, N'dw_etl'),
--         @procedure_started         DATETIME2(0) = SYSDATETIME(),
--         @step_started              DATETIME2(0),
--         @rows_read                 INT = 0,
--         @rows_inserted             INT = 0,
--         @rows_updated              INT = 0,
--         @rows_rejected             INT = 0,
--         @source_rows_read          INT = 0,
--         @candidate_rows            INT = 0,
--         @max_existing_key          BIGINT = 0,
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

--         /* Step 1: Align identity. */
--         SET @step_started = SYSDATETIME();

--         SELECT @max_existing_key = ISNULL(MAX(child_task_event_key), 0)
--         FROM dw.fact_child_task_event;

--         DBCC CHECKIDENT ('dw.fact_child_task_event', RESEED, @max_existing_key) WITH NO_INFOMSGS;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'01 - Align child task event identity', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Reseeded child_task_event_key to ', @max_existing_key, N'.'));

--         /* Step 2: Resolve latest successful transaction fact batch. */
--         SET @step_started = SYSDATETIME();

--         SELECT TOP (1)
--             @source_fact_batch_id = etl_batch_id
--         FROM etl_admin.etl_load_log
--         WHERE target_database = N'Charity_DW_DB'
--           AND target_schema   = N'dw'
--           AND target_table    = N'fact_tran_student_task_progress'
--           AND load_status     = N'succeeded'
--           AND etl_batch_id    < @etl_batch_id
--         ORDER BY ended_at DESC, etl_batch_id DESC;

--         IF @source_fact_batch_id IS NULL
--         BEGIN
--             RAISERROR('Cannot find latest successful fact_tran_student_task_progress batch for downstream event fact.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'02 - Resolve upstream transaction fact batch', N'succeeded',
--              0, 0, 0, 0,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Using fact_tran_student_task_progress etl_batch_id = ', @source_fact_batch_id,
--                     N' as affected transaction set.'));

--         /* Step 3: Build affected event candidates from transaction fact batch. */
--         SET @step_started = SYSDATETIME();

--         IF OBJECT_ID('tempdb..#event_candidate') IS NOT NULL
--             DROP TABLE #event_candidate;

--         SELECT @source_rows_read = COUNT(1)
--         FROM dw.fact_tran_student_task_progress AS ft
--         WHERE ft.etl_batch_id = @source_fact_batch_id;

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
--         LEFT JOIN dw.dim_assessment_status AS das
--             ON das.assessment_status_key = ft.assessment_status_key
--         WHERE ft.etl_batch_id = @source_fact_batch_id
--           AND
--           (
--               ft.source_daily_task_assignment_id IS NOT NULL
--               OR ft.source_task_assessment_id IS NOT NULL
--           );

--         SET @candidate_rows = @@ROWCOUNT;

--         SELECT @rows_rejected = COUNT(1)
--         FROM dw.fact_tran_student_task_progress AS ft
--         WHERE ft.etl_batch_id = @source_fact_batch_id
--           AND ft.source_daily_task_assignment_id IS NULL
--           AND ft.source_task_assessment_id IS NULL;

--         SET @rows_read = @candidate_rows;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'03 - Build affected child task event candidates', N'succeeded',
--              @candidate_rows, 0, 0, @rows_rejected,
--              @step_started, SYSDATETIME(),
--              CONCAT(N'Source transaction rows from upstream batch: ', @source_rows_read,
--                     N'. Candidate event rows: ', @candidate_rows,
--                     N'. Rejected rows without source event IDs: ', @rows_rejected,
--                     N'.'));

--         /* Step 4: Update changed event rows. */
--         SET @step_started = SYSDATETIME();

--         UPDATE tgt
--         SET
--             tgt.child_key                    = src.child_key,
--             tgt.task_key                     = src.task_key,
--             tgt.teacher_key                  = src.teacher_key,
--             tgt.center_key                   = src.center_key,
--             tgt.domain_key                   = src.domain_key,
--             tgt.date_key                     = src.date_key,
--             tgt.event_status                 = src.event_status,
--             tgt.raw_score                    = src.raw_score,
--             tgt.normalized_score             = src.normalized_score,
--             tgt.source_assessment_session_id = src.source_assessment_session_id,
--             tgt.source_system                = src.source_system,
--             tgt.etl_batch_id                 = @etl_batch_id,
--             tgt.loaded_at                    = SYSDATETIME()
--         FROM dw.fact_child_task_event AS tgt
--         INNER JOIN #event_candidate AS src
--             ON ISNULL(tgt.event_type, N'') = ISNULL(src.event_type, N'')
--            AND
--            (
--                 (
--                     src.event_type = N'PLAN'
--                     AND tgt.source_task_assessment_id IS NULL
--                     AND tgt.source_daily_task_assignment_id = src.source_daily_task_assignment_id
--                 )
--                 OR
--                 (
--                     src.event_type = N'ASSESSMENT'
--                     AND tgt.source_task_assessment_id = src.source_task_assessment_id
--                 )
--            )
--         WHERE
--             ISNULL(tgt.child_key, -1)        <> ISNULL(src.child_key, -1)
--             OR ISNULL(tgt.task_key, -1)      <> ISNULL(src.task_key, -1)
--             OR ISNULL(tgt.teacher_key, -1)   <> ISNULL(src.teacher_key, -1)
--             OR ISNULL(tgt.center_key, -1)    <> ISNULL(src.center_key, -1)
--             OR ISNULL(tgt.domain_key, -1)    <> ISNULL(src.domain_key, -1)
--             OR ISNULL(tgt.date_key, -1)      <> ISNULL(src.date_key, -1)
--             OR ISNULL(tgt.event_status, N'') <> ISNULL(src.event_status, N'')
--             OR
--             (
--                 tgt.raw_score <> src.raw_score
--                 OR (tgt.raw_score IS NULL AND src.raw_score IS NOT NULL)
--                 OR (tgt.raw_score IS NOT NULL AND src.raw_score IS NULL)
--             )
--             OR
--             (
--                 tgt.normalized_score <> src.normalized_score
--                 OR (tgt.normalized_score IS NULL AND src.normalized_score IS NOT NULL)
--                 OR (tgt.normalized_score IS NOT NULL AND src.normalized_score IS NULL)
--             )
--             OR ISNULL(tgt.source_assessment_session_id, -1) <> ISNULL(src.source_assessment_session_id, -1)
--             OR ISNULL(tgt.source_system, N'') <> ISNULL(src.source_system, N'');

--         SET @rows_updated = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'04 - Update changed child task event rows', N'succeeded',
--              @candidate_rows, 0, @rows_updated, 0,
--              @step_started, SYSDATETIME(),
--              N'Updated changed existing event rows.');

--         /* Step 5: Insert new event rows. */
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
--         FROM #event_candidate AS src
--         LEFT JOIN dw.fact_child_task_event AS tgt
--             ON ISNULL(tgt.event_type, N'') = ISNULL(src.event_type, N'')
--            AND
--            (
--                 (
--                     src.event_type = N'PLAN'
--                     AND tgt.source_task_assessment_id IS NULL
--                     AND tgt.source_daily_task_assignment_id = src.source_daily_task_assignment_id
--                 )
--                 OR
--                 (
--                     src.event_type = N'ASSESSMENT'
--                     AND tgt.source_task_assessment_id = src.source_task_assessment_id
--                 )
--            )
--         WHERE tgt.child_task_event_key IS NULL;

--         SET @rows_inserted = @@ROWCOUNT;

--         INSERT INTO @step_log
--             (step_name, load_status, rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (N'05 - Insert new child task event rows', N'succeeded',
--              @candidate_rows, @rows_inserted, 0, 0,
--              @step_started, SYSDATETIME(),
--              N'Inserted new event rows.');

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
--         SET @error_message = CONCAT(N'Error ', ERROR_NUMBER(), N' at line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

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





-- CREATE OR ALTER PROCEDURE etl_admin.usp_run_dw_mart1_daily_incremental
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
--             (N'PROGRAM_OPS', N'DW', N'MART1_DW_ONLY_DAILY_INCREMENTAL', N'running',
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

--         EXEC etl_admin.usp_incremental_load_dw_dim_center
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_center did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (1,
--              N'01 - usp_incremental_load_dw_dim_center',
--              N'dim_center',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_center. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_domain
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_domain did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (2,
--              N'02 - usp_incremental_load_dw_dim_domain',
--              N'dim_domain',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_domain. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_score_scale
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_score_scale did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (3,
--              N'03 - usp_incremental_load_dw_dim_score_scale',
--              N'dim_score_scale',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_score_scale. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_no_score_reason
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_no_score_reason did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (4,
--              N'04 - usp_incremental_load_dw_dim_no_score_reason',
--              N'dim_no_score_reason',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_no_score_reason. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_assessment_status
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_assessment_status did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (5,
--              N'05 - usp_incremental_load_dw_dim_assessment_status',
--              N'dim_assessment_status',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_assessment_status. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_child
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_child did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (6,
--              N'06 - usp_incremental_load_dw_dim_child',
--              N'dim_child',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_child. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_teacher
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_teacher did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (7,
--              N'07 - usp_incremental_load_dw_dim_teacher',
--              N'dim_teacher',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_teacher. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_dim_task
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_dim_task did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (8,
--              N'08 - usp_incremental_load_dw_dim_task',
--              N'dim_task',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_dim_task. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (9,
--              N'09 - usp_incremental_load_dw_fact_tran_student_task_progress',
--              N'fact_tran_student_task_progress',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_fact_tran_student_task_progress. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_fact_child_task_event
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_fact_child_task_event did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (10,
--              N'10 - usp_incremental_load_dw_fact_child_task_event',
--              N'fact_child_task_event',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_fact_child_task_event. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (11,
--              N'11 - usp_incremental_load_dw_fact_daily_student_task_progress',
--              N'fact_daily_student_task_progress',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_fact_daily_student_task_progress. Child etl_batch_id = ', @child_batch_id, N'.'));

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

--         EXEC etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation
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
--             RAISERROR('Child procedure etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation did not succeed.', 16, 1);
--         END;

--         INSERT INTO @step_log
--             (step_no, step_name, target_table, child_batch_id, load_status,
--              rows_read, rows_inserted, rows_updated, rows_rejected,
--              started_at, ended_at, message)
--         VALUES
--             (12,
--              N'12 - usp_incremental_load_dw_fact_child_snapshot_accumulation',
--              N'fact_child_snapshot_accumulation',
--              @child_batch_id,
--              @child_status,
--              @child_rows_read,
--              @child_rows_inserted,
--              @child_rows_updated,
--              @child_rows_rejected,
--              @step_started,
--              SYSDATETIME(),
--              CONCAT(N'Executed etl_admin.usp_incremental_load_dw_fact_child_snapshot_accumulation. Child etl_batch_id = ', @child_batch_id, N'.'));

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
--             N'usp_run_dw_mart1_daily_incremental',
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
--                 N'usp_run_dw_mart1_daily_incremental',
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
--                  N'usp_run_dw_mart1_daily_incremental',
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

