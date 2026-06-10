/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 4 - DW ETL Layer
 File         : 14_create_dw_mart1_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Load MART 1 warehouse dimensions and facts from Stg_ProgramOps_DB.

 Performance rules used here:
   - Uses separate truncate/insert loading; no row modification statements in ETL bodies.
   - Full-refresh loads use TRUNCATE TABLE.
   - Large fact loads are staged in #temp tables with narrow columns.
   - Big joins are split into small lookup maps first, then applied to fact cores.
   - Fact table inserts are one INSERT ... SELECT segment per fact table.
   - Any active nonclustered/columnstore indexes on target big tables are dropped before truncate.
   - Deleted source business records are preserved as DELETE events from audit_logs.
   - Each procedure writes an ETL log row.

 Prerequisites:
   - Run 05_create_stg_program_ops_db.sql
   - Run 06_create_stg_program_ops_tables.sql
   - Run 09_create_etl_program_ops_to_staging_procedures.sql and execute staging load
   - Run 11_create_dw_db.sql
   - Run 12_create_dw_mart1_tables.sql
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Charity_DW_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END
GO

/*=============================================================================
  Helper: write one load log row
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_dw_mart1_write_load_log
    @etl_batch_id       INT,
    @source_database    NVARCHAR(128),
    @source_schema      NVARCHAR(128),
    @source_table       NVARCHAR(128),
    @target_schema      NVARCHAR(128),
    @target_table       NVARCHAR(128),
    @load_status        NVARCHAR(50),
    @rows_read          INT,
    @rows_inserted      INT,
    @rows_rejected      INT,
    @started_at         DATETIME2(0),
    @message            NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl_admin.etl_load_log (
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
    VALUES (
        @etl_batch_id,
        @source_database,
        @source_schema,
        @source_table,
        DB_NAME(),
        @target_schema,
        @target_table,
        @load_status,
        @rows_read,
        @rows_inserted,
        0,
        @rows_rejected,
        @started_at,
        SYSDATETIME(),
        @message
    );
END;
GO

/*=============================================================================
  Helper: drop all user-created indexes before large truncate/load
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_dw_mart1_drop_table_indexes
    @schema_name SYSNAME,
    @table_name  SYSNAME
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX) = N'';

    SELECT @sql = @sql +
        N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';' + CHAR(10)
    FROM sys.indexes AS i
    INNER JOIN sys.tables AS t
        ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    WHERE s.name = @schema_name
      AND t.name = @table_name
      AND i.index_id > 0
      AND i.is_primary_key = 0
      AND i.is_unique_constraint = 0
      AND i.name IS NOT NULL;

    IF @sql <> N''
    BEGIN
        EXEC sys.sp_executesql @sql;
    END
END;
GO

/*=============================================================================
  Dimension: Date
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_date
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#source_dates') IS NOT NULL DROP TABLE #source_dates;
        IF OBJECT_ID(N'tempdb..#date_bounds') IS NOT NULL DROP TABLE #date_bounds;
        IF OBJECT_ID(N'tempdb..#date_sequence') IS NOT NULL DROP TABLE #date_sequence;

        SELECT DISTINCT source_date
        INTO #source_dates
        FROM (
            SELECT [date] AS source_date FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments WHERE is_valid = 1 AND [date] IS NOT NULL
            UNION ALL SELECT [date] FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments WHERE is_valid = 1 AND [date] IS NOT NULL
            UNION ALL SELECT [date] FROM Stg_ProgramOps_DB.stg_program_ops.assessment_sessions WHERE is_valid = 1 AND [date] IS NOT NULL
            UNION ALL SELECT [date] FROM Stg_ProgramOps_DB.stg_program_ops.child_daily_status WHERE is_valid = 1 AND [date] IS NOT NULL
            UNION ALL SELECT [date] FROM Stg_ProgramOps_DB.stg_program_ops.center_daily_status WHERE is_valid = 1 AND [date] IS NOT NULL
            UNION ALL SELECT start_date FROM Stg_ProgramOps_DB.stg_program_ops.child_task_plans WHERE is_valid = 1 AND start_date IS NOT NULL
            UNION ALL SELECT end_date FROM Stg_ProgramOps_DB.stg_program_ops.child_task_plans WHERE is_valid = 1 AND end_date IS NOT NULL
            UNION ALL SELECT enrollment_date FROM Stg_ProgramOps_DB.stg_program_ops.children WHERE is_valid = 1 AND enrollment_date IS NOT NULL
            UNION ALL SELECT birth_date FROM Stg_ProgramOps_DB.stg_program_ops.children WHERE is_valid = 1 AND birth_date IS NOT NULL
        ) AS d;

        SELECT @rows_read = COUNT(*) FROM #source_dates;

        SELECT MIN(source_date) AS min_date, MAX(source_date) AS max_date
        INTO #date_bounds
        FROM #source_dates;

        ;WITH n AS (
            SELECT 0 AS i
            UNION ALL
            SELECT i + 1
            FROM n
            CROSS JOIN #date_bounds AS b
            WHERE DATEADD(DAY, i + 1, b.min_date) <= b.max_date
        )
        SELECT DATEADD(DAY, i, b.min_date) AS full_date
        INTO #date_sequence
        FROM n
        CROSS JOIN #date_bounds AS b
        WHERE b.min_date IS NOT NULL
        OPTION (MAXRECURSION 0);

        TRUNCATE TABLE dw.dim_date;

        INSERT INTO dw.dim_date (
            date_key, full_date, [day], day_name, day_of_week, day_of_year,
            week_of_year, [month], month_name, [quarter], [year], is_weekend, created_at
        )
        VALUES (-1, CONVERT(DATE, '19000101'), 1, N'Unknown', 0, 0, 0, 0, N'Unknown', 0, 1900, 0, SYSDATETIME());

        INSERT INTO dw.dim_date (
            date_key, full_date, [day], day_name, day_of_week, day_of_year,
            week_of_year, [month], month_name, [quarter], [year], is_weekend, created_at
        )
        SELECT
            CONVERT(INT, CONVERT(CHAR(8), ds.full_date, 112)) AS date_key,
            ds.full_date,
            DATEPART(DAY, ds.full_date),
            DATENAME(WEEKDAY, ds.full_date),
            DATEPART(WEEKDAY, ds.full_date),
            DATEPART(DAYOFYEAR, ds.full_date),
            DATEPART(WEEK, ds.full_date),
            DATEPART(MONTH, ds.full_date),
            DATENAME(MONTH, ds.full_date),
            DATEPART(QUARTER, ds.full_date),
            DATEPART(YEAR, ds.full_date),
            CASE WHEN DATEPART(WEEKDAY, ds.full_date) IN (1, 7) THEN 1 ELSE 0 END,
            SYSDATETIME()
        FROM #date_sequence AS ds;

        SET @rows_inserted = @@ROWCOUNT + 1;

        EXEC etl_admin.usp_dw_mart1_write_load_log
            @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'date_sources',
            N'dw', N'dim_date', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at,
            N'Date dimension full-refresh loaded.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log
            @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'date_sources',
            N'dw', N'dim_date', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Center
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_center
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        SELECT @rows_read = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.centers;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.centers WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_center';
        TRUNCATE TABLE dw.dim_center;

        SET IDENTITY_INSERT dw.dim_center ON;
        INSERT INTO dw.dim_center (center_key, center_id, center_name, city, address, center_status, effective_from, effective_to, is_current, source_system, created_at)
        VALUES (-1, -1, N'Unknown', NULL, NULL, N'unknown', CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_center OFF;

        INSERT INTO dw.dim_center (
            center_id, center_name, city, address, center_status,
            effective_from, effective_to, is_current, source_system, row_hash, created_at, updated_at
        )
        SELECT
            c.id,
            c.name,
            c.city,
            c.address,
            CASE WHEN c.is_active = 1 THEN N'active' WHEN c.is_active = 0 THEN N'deleted_or_inactive' ELSE N'unknown' END,
            COALESCE(c.created_at, c.source_updated_at, c.extracted_at),
            CASE WHEN c.is_active = 0 THEN COALESCE(c.updated_at, c.source_updated_at, c.extracted_at) ELSE NULL END,
            CASE WHEN c.is_active = 1 THEN 1 ELSE 0 END,
            c.source_system,
            c.row_hash,
            SYSDATETIME(),
            c.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.centers AS c
        WHERE c.is_valid = 1
          AND c.id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers', N'dw', N'dim_center', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Center dimension loaded with inactive/deleted rows retained.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers', N'dw', N'dim_center', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Teacher
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_teacher
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#center_map_for_teacher') IS NOT NULL DROP TABLE #center_map_for_teacher;

        SELECT c.id AS center_id, c.name AS center_name
        INTO #center_map_for_teacher
        FROM Stg_ProgramOps_DB.stg_program_ops.centers AS c
        WHERE c.is_valid = 1
          AND c.id IS NOT NULL;

        CREATE INDEX IX_tmp_center_map_for_teacher ON #center_map_for_teacher(center_id);

        SELECT @rows_read = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.teachers;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.teachers WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_teacher';
        TRUNCATE TABLE dw.dim_teacher;

        SET IDENTITY_INSERT dw.dim_teacher ON;
        INSERT INTO dw.dim_teacher (teacher_key, teacher_id, first_name, last_name, full_name, center_id, center_name, employment_status, effective_from, effective_to, is_current, source_system, created_at)
        VALUES (-1, -1, NULL, NULL, N'Unknown', NULL, NULL, N'unknown', CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_teacher OFF;

        INSERT INTO dw.dim_teacher (
            teacher_id, first_name, last_name, full_name, center_id, center_name,
            employment_status, effective_from, effective_to, is_current, source_system, row_hash, created_at, updated_at
        )
        SELECT
            t.id,
            t.first_name,
            t.last_name,
            LTRIM(RTRIM(CONCAT(COALESCE(t.first_name, N''), N' ', COALESCE(t.last_name, N'')))),
            t.center_id,
            cm.center_name,
            CASE WHEN t.is_active = 0 THEN N'deleted_or_inactive' ELSE COALESCE(t.employment_status, N'unknown') END,
            COALESCE(t.created_at, t.source_updated_at, t.extracted_at),
            CASE WHEN t.is_active = 0 THEN COALESCE(t.updated_at, t.source_updated_at, t.extracted_at) ELSE NULL END,
            CASE WHEN t.is_active = 1 THEN 1 ELSE 0 END,
            t.source_system,
            t.row_hash,
            SYSDATETIME(),
            t.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.teachers AS t
        LEFT JOIN #center_map_for_teacher AS cm
            ON t.center_id = cm.center_id
        WHERE t.is_valid = 1
          AND t.id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers', N'dw', N'dim_teacher', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Teacher dimension loaded.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers', N'dw', N'dim_teacher', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Child
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_child
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        SELECT @rows_read = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.children;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.children WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_child';
        TRUNCATE TABLE dw.dim_child;

        SET IDENTITY_INSERT dw.dim_child ON;
        INSERT INTO dw.dim_child (child_key, child_id, first_name, last_name, full_name, birth_date, gender, center_id, status, enrollment_date, source_system, created_at)
        VALUES (-1, -1, NULL, NULL, N'Unknown', NULL, NULL, NULL, N'unknown', NULL, N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_child OFF;

        INSERT INTO dw.dim_child (
            child_id, first_name, last_name, full_name, birth_date, gender, center_id,
            status, enrollment_date, source_system, row_hash, created_at, updated_at
        )
        SELECT
            c.id,
            c.first_name,
            c.last_name,
            LTRIM(RTRIM(CONCAT(COALESCE(c.first_name, N''), N' ', COALESCE(c.last_name, N'')))),
            c.birth_date,
            c.gender,
            c.center_id,
            COALESCE(c.status, N'unknown'),
            c.enrollment_date,
            c.source_system,
            c.row_hash,
            SYSDATETIME(),
            c.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.children AS c
        WHERE c.is_valid = 1
          AND c.id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'children', N'dw', N'dim_child', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Child dimension loaded.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'children', N'dw', N'dim_child', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Domain
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_domain
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        SELECT @rows_read = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.domains;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.domains WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_domain';
        TRUNCATE TABLE dw.dim_domain;

        SET IDENTITY_INSERT dw.dim_domain ON;
        INSERT INTO dw.dim_domain (domain_key, domain_id, domain_name, domain_description, domain_status, source_system, created_at)
        VALUES (-1, -1, N'Unknown', NULL, N'unknown', N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_domain OFF;

        INSERT INTO dw.dim_domain (domain_id, domain_name, domain_description, domain_status, source_system, row_hash, created_at, updated_at)
        SELECT
            d.id,
            d.name,
            d.description,
            CASE WHEN d.is_active = 1 THEN N'active' WHEN d.is_active = 0 THEN N'deleted_or_inactive' ELSE N'unknown' END,
            d.source_system,
            d.row_hash,
            SYSDATETIME(),
            d.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.domains AS d
        WHERE d.is_valid = 1
          AND d.id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains', N'dw', N'dim_domain', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Domain dimension loaded.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains', N'dw', N'dim_domain', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Task
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_task
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#domain_names') IS NOT NULL DROP TABLE #domain_names;
        IF OBJECT_ID(N'tempdb..#task_candidates') IS NOT NULL DROP TABLE #task_candidates;

        SELECT d.id AS domain_id, d.name AS domain_name
        INTO #domain_names
        FROM Stg_ProgramOps_DB.stg_program_ops.domains AS d
        WHERE d.is_valid = 1 AND d.id IS NOT NULL;

        CREATE INDEX IX_tmp_domain_names ON #domain_names(domain_id);

        SELECT
            tt.id AS task_template_id,
            tt.title AS task_title,
            tt.domain_id,
            dn.domain_name,
            CONVERT(BIT, 1) AS is_template_based,
            tt.description AS task_description,
            CASE WHEN tt.is_active = 1 THEN N'active' WHEN tt.is_active = 0 THEN N'deleted_or_inactive' ELSE N'unknown' END AS task_status,
            tt.source_system,
            tt.row_hash,
            tt.created_at,
            tt.updated_at
        INTO #task_candidates
        FROM Stg_ProgramOps_DB.stg_program_ops.task_templates AS tt
        LEFT JOIN #domain_names AS dn
            ON tt.domain_id = dn.domain_id
        WHERE tt.is_valid = 1
          AND tt.id IS NOT NULL
        UNION ALL
        SELECT DISTINCT
            dta.task_template_id,
            dta.task_title,
            dta.domain_id,
            dn.domain_name,
            CASE WHEN dta.task_template_id IS NULL THEN CONVERT(BIT, 0) ELSE CONVERT(BIT, 1) END,
            NULL,
            COALESCE(dta.status, N'unknown'),
            dta.source_system,
            dta.row_hash,
            dta.created_at,
            dta.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
        LEFT JOIN #domain_names AS dn
            ON dta.domain_id = dn.domain_id
        WHERE dta.is_valid = 1
          AND dta.task_title IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM Stg_ProgramOps_DB.stg_program_ops.task_templates AS tt
              WHERE tt.is_valid = 1
                AND tt.id = dta.task_template_id
          )
        UNION ALL
        SELECT DISTINCT
            ctp.task_template_id,
            ctp.task_title,
            ctp.domain_id,
            dn.domain_name,
            CASE WHEN ctp.task_template_id IS NULL THEN CONVERT(BIT, 0) ELSE CONVERT(BIT, 1) END,
            ctp.task_title,
            CASE WHEN ctp.is_active = 1 THEN N'active' WHEN ctp.is_active = 0 THEN N'deleted_or_inactive' ELSE N'unknown' END,
            ctp.source_system,
            ctp.row_hash,
            ctp.created_at,
            ctp.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.child_task_plans AS ctp
        LEFT JOIN #domain_names AS dn
            ON ctp.domain_id = dn.domain_id
        WHERE ctp.is_valid = 1
          AND ctp.task_title IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM Stg_ProgramOps_DB.stg_program_ops.task_templates AS tt
              WHERE tt.is_valid = 1
                AND tt.id = ctp.task_template_id
          );

        SELECT @rows_read = COUNT(*) FROM #task_candidates;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.task_templates WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_task';
        TRUNCATE TABLE dw.dim_task;

        SET IDENTITY_INSERT dw.dim_task ON;
        INSERT INTO dw.dim_task (task_key, task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, created_at)
        VALUES (-1, NULL, N'Unknown', NULL, NULL, 0, NULL, N'unknown', N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_task OFF;

        INSERT INTO dw.dim_task (
            task_template_id, task_title, domain_id, domain_name, is_template_based,
            task_description, task_status, source_system, row_hash, created_at, updated_at
        )
        SELECT
            tc.task_template_id,
            tc.task_title,
            tc.domain_id,
            tc.domain_name,
            tc.is_template_based,
            tc.task_description,
            tc.task_status,
            tc.source_system,
            HASHBYTES('SHA2_256', CONCAT(COALESCE(CONVERT(NVARCHAR(50), tc.task_template_id), N''), N'|', COALESCE(tc.task_title, N''), N'|', COALESCE(CONVERT(NVARCHAR(50), tc.domain_id), N''))),
            SYSDATETIME(),
            MAX(tc.updated_at)
        FROM #task_candidates AS tc
        GROUP BY
            tc.task_template_id, tc.task_title, tc.domain_id, tc.domain_name,
            tc.is_template_based, tc.task_description, tc.task_status, tc.source_system;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates/daily_task_assignments/child_task_plans', N'dw', N'dim_task', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Task dimension loaded from templates and non-template task titles.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates/daily_task_assignments/child_task_plans', N'dw', N'dim_task', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Score Scale
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_score_scale
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        SELECT @rows_read = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.score_scales;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.score_scales WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_score_scale';
        TRUNCATE TABLE dw.dim_score_scale;

        SET IDENTITY_INSERT dw.dim_score_scale ON;
        INSERT INTO dw.dim_score_scale (score_scale_key, score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, created_at)
        VALUES (-1, -1, N'Unknown', NULL, NULL, NULL, N'unknown', N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_score_scale OFF;

        INSERT INTO dw.dim_score_scale (score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, row_hash, created_at, updated_at)
        SELECT
            ss.id,
            ss.name,
            ss.min_score,
            ss.max_score,
            ss.description,
            CASE WHEN ss.is_active = 1 THEN N'active' WHEN ss.is_active = 0 THEN N'deleted_or_inactive' ELSE N'unknown' END,
            ss.source_system,
            ss.row_hash,
            SYSDATETIME(),
            ss.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.score_scales AS ss
        WHERE ss.is_valid = 1
          AND ss.id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales', N'dw', N'dim_score_scale', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Score scale dimension loaded.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales', N'dw', N'dim_score_scale', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Assessment Status
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_assessment_status
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#statuses') IS NOT NULL DROP TABLE #statuses;

        SELECT DISTINCT LOWER(LTRIM(RTRIM(assessment_status))) AS assessment_status_code
        INTO #statuses
        FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments
        WHERE is_valid = 1
          AND assessment_status IS NOT NULL
        UNION
        SELECT DISTINCT LOWER(LTRIM(RTRIM(status)))
        FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments
        WHERE is_valid = 1
          AND status IS NOT NULL
        UNION
        SELECT DISTINCT LOWER(LTRIM(RTRIM(session_status)))
        FROM Stg_ProgramOps_DB.stg_program_ops.assessment_sessions
        WHERE is_valid = 1
          AND session_status IS NOT NULL;

        SELECT @rows_read = COUNT(*) FROM #statuses;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_assessment_status';
        TRUNCATE TABLE dw.dim_assessment_status;

        SET IDENTITY_INSERT dw.dim_assessment_status ON;
        INSERT INTO dw.dim_assessment_status (assessment_status_key, assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at)
        VALUES (-1, N'unknown', N'Unknown', N'unknown', 0, 0, N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_assessment_status OFF;

        INSERT INTO dw.dim_assessment_status (
            assessment_status_code, assessment_status_title, assessment_status_category,
            is_successful_assessment, is_failure_assessment, source_system, created_at, updated_at
        )
        SELECT
            s.assessment_status_code,
            UPPER(s.assessment_status_code),
            CASE
                WHEN s.assessment_status_code IN (N'scored', N'completed', N'done', N'assessed') THEN N'success'
                WHEN s.assessment_status_code IN (N'not_scored', N'no_score', N'refused', N'absent', N'cancelled', N'canceled', N'incomplete', N'failed') THEN N'failure'
                ELSE N'other'
            END,
            CASE WHEN s.assessment_status_code IN (N'scored', N'completed', N'done', N'assessed') THEN 1 ELSE 0 END,
            CASE WHEN s.assessment_status_code IN (N'not_scored', N'no_score', N'refused', N'absent', N'cancelled', N'canceled', N'incomplete', N'failed') THEN 1 ELSE 0 END,
            N'PROGRAM_OPS',
            SYSDATETIME(),
            SYSDATETIME()
        FROM #statuses AS s;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments/daily_task_assignments/assessment_sessions', N'dw', N'dim_assessment_status', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Assessment status dimension loaded from distinct operational statuses.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments/daily_task_assignments/assessment_sessions', N'dw', N'dim_assessment_status', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: No Score Reason
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_no_score_reason
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        SELECT @rows_read = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.no_score_reasons;
        SELECT @rows_rejected = COUNT(*) FROM Stg_ProgramOps_DB.stg_program_ops.no_score_reasons WHERE is_valid = 0 OR id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_no_score_reason';
        TRUNCATE TABLE dw.dim_no_score_reason;

        SET IDENTITY_INSERT dw.dim_no_score_reason ON;
        INSERT INTO dw.dim_no_score_reason (no_score_reason_key, no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, created_at)
        VALUES (-1, -1, N'Unknown', NULL, N'unknown', 0, 0, 0, 0, N'PROGRAM_OPS', SYSDATETIME());
        SET IDENTITY_INSERT dw.dim_no_score_reason OFF;

        INSERT INTO dw.dim_no_score_reason (
            no_score_reason_id, reason_title, reason_description, reason_category,
            is_child_related, is_teacher_related, is_center_related, is_system_related,
            source_system, row_hash, created_at, updated_at
        )
        SELECT
            nsr.id,
            nsr.title,
            nsr.description,
            CASE
                WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%refus%' OR nsr.title LIKE N'%امتناع%' THEN N'child'
                WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%absen%' OR nsr.title LIKE N'%غیبت%' THEN N'child'
                WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%teacher%' OR nsr.title LIKE N'%معلم%' THEN N'teacher'
                WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%center%' OR nsr.title LIKE N'%مرکز%' THEN N'center'
                WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%system%' OR nsr.title LIKE N'%سیستم%' THEN N'system'
                ELSE N'other'
            END,
            CASE WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%refus%' OR LOWER(COALESCE(nsr.title, N'')) LIKE N'%absen%' OR nsr.title LIKE N'%امتناع%' OR nsr.title LIKE N'%غیبت%' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%teacher%' OR nsr.title LIKE N'%معلم%' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%center%' OR nsr.title LIKE N'%مرکز%' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%system%' OR nsr.title LIKE N'%سیستم%' THEN 1 ELSE 0 END,
            nsr.source_system,
            nsr.row_hash,
            SYSDATETIME(),
            nsr.updated_at
        FROM Stg_ProgramOps_DB.stg_program_ops.no_score_reasons AS nsr
        WHERE nsr.is_valid = 1
          AND nsr.id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT + 1;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons', N'dw', N'dim_no_score_reason', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'No-score reason dimension loaded.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons', N'dw', N'dim_no_score_reason', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Transaction Student Task Progress
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_tran_student_task_progress
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#dim_child') IS NOT NULL DROP TABLE #dim_child;
        IF OBJECT_ID(N'tempdb..#dim_center') IS NOT NULL DROP TABLE #dim_center;
        IF OBJECT_ID(N'tempdb..#dim_teacher') IS NOT NULL DROP TABLE #dim_teacher;
        IF OBJECT_ID(N'tempdb..#dim_domain') IS NOT NULL DROP TABLE #dim_domain;
        IF OBJECT_ID(N'tempdb..#dim_task_template') IS NOT NULL DROP TABLE #dim_task_template;
        IF OBJECT_ID(N'tempdb..#dim_task_title') IS NOT NULL DROP TABLE #dim_task_title;
        IF OBJECT_ID(N'tempdb..#dim_score_scale') IS NOT NULL DROP TABLE #dim_score_scale;
        IF OBJECT_ID(N'tempdb..#dim_status') IS NOT NULL DROP TABLE #dim_status;
        IF OBJECT_ID(N'tempdb..#dim_no_score_reason') IS NOT NULL DROP TABLE #dim_no_score_reason;
        IF OBJECT_ID(N'tempdb..#center_closed') IS NOT NULL DROP TABLE #center_closed;
        IF OBJECT_ID(N'tempdb..#child_absent') IS NOT NULL DROP TABLE #child_absent;
        IF OBJECT_ID(N'tempdb..#assignment_core') IS NOT NULL DROP TABLE #assignment_core;
        IF OBJECT_ID(N'tempdb..#assessment_orphan_core') IS NOT NULL DROP TABLE #assessment_orphan_core;
        IF OBJECT_ID(N'tempdb..#fact_core') IS NOT NULL DROP TABLE #fact_core;

        SELECT child_id, MIN(child_key) AS child_key, MIN(center_id) AS center_id
        INTO #dim_child
        FROM dw.dim_child
        GROUP BY child_id;
        CREATE INDEX IX_tmp_dim_child ON #dim_child(child_id);

        SELECT center_id, MIN(center_key) AS center_key
        INTO #dim_center
        FROM dw.dim_center
        GROUP BY center_id;
        CREATE INDEX IX_tmp_dim_center ON #dim_center(center_id);

        SELECT teacher_id, MIN(teacher_key) AS teacher_key
        INTO #dim_teacher
        FROM dw.dim_teacher
        GROUP BY teacher_id;
        CREATE INDEX IX_tmp_dim_teacher ON #dim_teacher(teacher_id);

        SELECT domain_id, MIN(domain_key) AS domain_key
        INTO #dim_domain
        FROM dw.dim_domain
        GROUP BY domain_id;
        CREATE INDEX IX_tmp_dim_domain ON #dim_domain(domain_id);

        SELECT task_template_id, MIN(task_key) AS task_key
        INTO #dim_task_template
        FROM dw.dim_task
        WHERE task_template_id IS NOT NULL
        GROUP BY task_template_id;
        CREATE INDEX IX_tmp_dim_task_template ON #dim_task_template(task_template_id);

        SELECT task_title, domain_id, MIN(task_key) AS task_key
        INTO #dim_task_title
        FROM dw.dim_task
        GROUP BY task_title, domain_id;
        CREATE INDEX IX_tmp_dim_task_title ON #dim_task_title(task_title, domain_id);

        SELECT score_scale_id, MIN(score_scale_key) AS score_scale_key
        INTO #dim_score_scale
        FROM dw.dim_score_scale
        GROUP BY score_scale_id;
        CREATE INDEX IX_tmp_dim_score_scale ON #dim_score_scale(score_scale_id);

        SELECT assessment_status_code, MIN(assessment_status_key) AS assessment_status_key
        INTO #dim_status
        FROM dw.dim_assessment_status
        GROUP BY assessment_status_code;
        CREATE INDEX IX_tmp_dim_status ON #dim_status(assessment_status_code);

        SELECT no_score_reason_id, MIN(no_score_reason_key) AS no_score_reason_key
        INTO #dim_no_score_reason
        FROM dw.dim_no_score_reason
        GROUP BY no_score_reason_id;
        CREATE INDEX IX_tmp_dim_nsr ON #dim_no_score_reason(no_score_reason_id);

        SELECT center_id, [date], CONVERT(BIT, 1) AS is_center_closed
        INTO #center_closed
        FROM Stg_ProgramOps_DB.stg_program_ops.center_daily_status
        WHERE is_valid = 1
          AND (LOWER(COALESCE(status, N'')) IN (N'closed', N'closure') OR closure_reason_id IS NOT NULL);
        CREATE INDEX IX_tmp_center_closed ON #center_closed(center_id, [date]);

        SELECT child_id, [date], CONVERT(BIT, 1) AS is_absent
        INTO #child_absent
        FROM Stg_ProgramOps_DB.stg_program_ops.child_daily_status
        WHERE is_valid = 1
          AND (LOWER(COALESCE(status, N'')) IN (N'absent', N'absence') OR absence_reason_id IS NOT NULL);
        CREATE INDEX IX_tmp_child_absent ON #child_absent(child_id, [date]);

        SELECT
            dta.id AS source_daily_task_assignment_id,
            ta.id AS source_task_assessment_id,
            ta.assessment_session_id AS source_assessment_session_id,
            dta.child_task_plan_id AS source_child_task_plan_id,
            COALESCE(ta.[date], dta.[date]) AS fact_date,
            COALESCE(ta.child_id, dta.child_id) AS child_id,
            COALESCE(ta.teacher_id, ases.teacher_id) AS teacher_id,
            COALESCE(ases.center_id, dc.center_id) AS center_id,
            COALESCE(dta.domain_id, ctp.domain_id) AS domain_id,
            COALESCE(dta.task_template_id, ctp.task_template_id) AS task_template_id,
            COALESCE(dta.task_title, ctp.task_title) AS task_title,
            COALESCE(dta.score_scale_id, ctp.score_scale_id) AS score_scale_id,
            LOWER(COALESCE(ta.assessment_status, dta.status, ases.session_status, N'unknown')) AS assessment_status_code,
            ta.no_score_reason_id,
            ta.attempt_no,
            ta.score AS raw_score,
            ta.normalized_score,
            dta.status AS assignment_status,
            ta.assessment_status,
            ases.session_status,
            CONVERT(BIT, 1) AS is_planned,
            CASE WHEN ta.id IS NULL THEN CONVERT(BIT, 0) ELSE CONVERT(BIT, 1) END AS is_assessed,
            dta.source_system
        INTO #assignment_core
        FROM Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.task_assessments AS ta
            ON dta.id = ta.daily_task_assignment_id
           AND ta.is_valid = 1
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.assessment_sessions AS ases
            ON ta.assessment_session_id = ases.id
           AND ases.is_valid = 1
        LEFT JOIN #dim_child AS dc
            ON COALESCE(ta.child_id, dta.child_id) = dc.child_id
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.child_task_plans AS ctp
            ON dta.child_task_plan_id = ctp.id
           AND ctp.is_valid = 1
        WHERE dta.is_valid = 1;

        CREATE INDEX IX_tmp_assignment_core_child_date ON #assignment_core(child_id, fact_date);
        CREATE INDEX IX_tmp_assignment_core_task ON #assignment_core(task_template_id, task_title, domain_id);

        SELECT
            NULL AS source_daily_task_assignment_id,
            ta.id AS source_task_assessment_id,
            ta.assessment_session_id AS source_assessment_session_id,
            NULL AS source_child_task_plan_id,
            ta.[date] AS fact_date,
            ta.child_id,
            COALESCE(ta.teacher_id, ases.teacher_id) AS teacher_id,
            COALESCE(ases.center_id, dc.center_id) AS center_id,
            dta.domain_id,
            dta.task_template_id,
            dta.task_title,
            dta.score_scale_id,
            LOWER(COALESCE(ta.assessment_status, ases.session_status, N'unknown')) AS assessment_status_code,
            ta.no_score_reason_id,
            ta.attempt_no,
            ta.score AS raw_score,
            ta.normalized_score,
            dta.status AS assignment_status,
            ta.assessment_status,
            ases.session_status,
            CONVERT(BIT, 0) AS is_planned,
            CONVERT(BIT, 1) AS is_assessed,
            ta.source_system
        INTO #assessment_orphan_core
        FROM Stg_ProgramOps_DB.stg_program_ops.task_assessments AS ta
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.daily_task_assignments AS dta
            ON ta.daily_task_assignment_id = dta.id
           AND dta.is_valid = 1
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.assessment_sessions AS ases
            ON ta.assessment_session_id = ases.id
           AND ases.is_valid = 1
        LEFT JOIN #dim_child AS dc
            ON ta.child_id = dc.child_id
        WHERE ta.is_valid = 1
          AND dta.id IS NULL;

        SELECT *
        INTO #fact_core
        FROM #assignment_core
        UNION ALL
        SELECT * FROM #assessment_orphan_core;

        CREATE INDEX IX_tmp_fact_core_child_date ON #fact_core(child_id, fact_date);
        CREATE INDEX IX_tmp_fact_core_lookup ON #fact_core(center_id, teacher_id, domain_id, score_scale_id, no_score_reason_id);

        SELECT @rows_read = COUNT(*) FROM #fact_core;
        SELECT @rows_rejected = COUNT(*) FROM #fact_core WHERE fact_date IS NULL OR child_id IS NULL;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'fact_tran_student_task_progress';
        TRUNCATE TABLE dw.fact_tran_student_task_progress;

        INSERT INTO dw.fact_tran_student_task_progress (
            date_key, child_key, center_key, teacher_key, domain_key, task_key,
            score_scale_key, assessment_status_key, no_score_reason_key,
            attempt_no, raw_score, normalized_score,
            is_completed, is_planned, is_scored, is_not_scored, is_cancelled,
            is_incomplete, is_refused, is_absent, is_center_closed, is_assessed,
            source_daily_task_assignment_id, source_task_assessment_id, source_assessment_session_id,
            source_child_task_plan_id, source_system, etl_batch_id, loaded_at
        )
        SELECT
            COALESCE(dd.date_key, -1),
            COALESCE(dc.child_key, -1),
            COALESCE(dcent.center_key, -1),
            COALESCE(dt.teacher_key, -1),
            COALESCE(ddom.domain_key, -1),
            COALESCE(dtt.task_key, dtitle.task_key, -1),
            COALESCE(dss.score_scale_key, -1),
            COALESCE(ds.assessment_status_key, -1),
            COALESCE(dnsr.no_score_reason_key, -1),
            fc.attempt_no,
            fc.raw_score,
            fc.normalized_score,
            CASE WHEN LOWER(COALESCE(fc.assignment_status, N'')) IN (N'completed', N'done') OR LOWER(COALESCE(fc.assessment_status, N'')) IN (N'scored', N'completed', N'done') THEN 1 ELSE 0 END,
            fc.is_planned,
            CASE WHEN fc.raw_score IS NOT NULL OR LOWER(COALESCE(fc.assessment_status, N'')) = N'scored' THEN 1 ELSE 0 END,
            CASE WHEN fc.raw_score IS NULL AND (fc.no_score_reason_id IS NOT NULL OR LOWER(COALESCE(fc.assessment_status, N'')) IN (N'not_scored', N'no_score')) THEN 1 ELSE 0 END,
            CASE WHEN LOWER(COALESCE(fc.assignment_status, N'')) IN (N'cancelled', N'canceled') OR LOWER(COALESCE(fc.session_status, N'')) IN (N'cancelled', N'canceled') THEN 1 ELSE 0 END,
            CASE WHEN LOWER(COALESCE(fc.assignment_status, N'')) = N'incomplete' OR LOWER(COALESCE(fc.assessment_status, N'')) = N'incomplete' THEN 1 ELSE 0 END,
            CASE WHEN LOWER(COALESCE(nsr.title, N'')) LIKE N'%refus%' OR nsr.title LIKE N'%امتناع%' THEN 1 ELSE 0 END,
            COALESCE(ca.is_absent, 0),
            COALESCE(cc.is_center_closed, 0),
            fc.is_assessed,
            fc.source_daily_task_assignment_id,
            fc.source_task_assessment_id,
            fc.source_assessment_session_id,
            fc.source_child_task_plan_id,
            COALESCE(fc.source_system, N'PROGRAM_OPS'),
            @etl_batch_id,
            SYSDATETIME()
        FROM #fact_core AS fc
        LEFT JOIN dw.dim_date AS dd
            ON fc.fact_date = dd.full_date
        LEFT JOIN #dim_child AS dc
            ON fc.child_id = dc.child_id
        LEFT JOIN #dim_center AS dcent
            ON fc.center_id = dcent.center_id
        LEFT JOIN #dim_teacher AS dt
            ON fc.teacher_id = dt.teacher_id
        LEFT JOIN #dim_domain AS ddom
            ON fc.domain_id = ddom.domain_id
        LEFT JOIN #dim_task_template AS dtt
            ON fc.task_template_id = dtt.task_template_id
        LEFT JOIN #dim_task_title AS dtitle
            ON fc.task_title = dtitle.task_title
           AND ISNULL(fc.domain_id, -999999) = ISNULL(dtitle.domain_id, -999999)
        LEFT JOIN #dim_score_scale AS dss
            ON fc.score_scale_id = dss.score_scale_id
        LEFT JOIN #dim_status AS ds
            ON fc.assessment_status_code = ds.assessment_status_code
        LEFT JOIN #dim_no_score_reason AS dnsr
            ON fc.no_score_reason_id = dnsr.no_score_reason_id
        LEFT JOIN Stg_ProgramOps_DB.stg_program_ops.no_score_reasons AS nsr
            ON fc.no_score_reason_id = nsr.id
           AND nsr.is_valid = 1
        LEFT JOIN #child_absent AS ca
            ON fc.child_id = ca.child_id
           AND fc.fact_date = ca.[date]
        LEFT JOIN #center_closed AS cc
            ON fc.center_id = cc.center_id
           AND fc.fact_date = cc.[date]
        WHERE fc.fact_date IS NOT NULL
          AND fc.child_id IS NOT NULL;

        SET @rows_inserted = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments/task_assessments', N'dw', N'fact_tran_student_task_progress', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Transaction fact loaded in one insert segment after temp staging.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments/task_assessments', N'dw', N'fact_tran_student_task_progress', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Daily Student Task Progress
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_daily_student_task_progress
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#daily_core') IS NOT NULL DROP TABLE #daily_core;
        IF OBJECT_ID(N'tempdb..#daily_teacher') IS NOT NULL DROP TABLE #daily_teacher;

        SELECT
            date_key, child_key, center_key,
            MIN(NULLIF(teacher_key, -1)) AS teacher_key,
            AVG(raw_score) AS raw_score,
            AVG(normalized_score) AS normalized_score,
            COUNT(*) AS planned_task_count,
            SUM(CASE WHEN is_assessed = 1 THEN 1 ELSE 0 END) AS assessment_count,
            SUM(CASE WHEN is_completed = 1 THEN 1 ELSE 0 END) AS completed_task_count,
            SUM(CASE WHEN is_scored = 1 THEN 1 ELSE 0 END) AS scored_task_count,
            SUM(CASE WHEN is_not_scored = 1 THEN 1 ELSE 0 END) AS not_scored_task_count
        INTO #daily_core
        FROM dw.fact_tran_student_task_progress
        WHERE etl_batch_id = @etl_batch_id OR @etl_batch_id IS NULL
        GROUP BY date_key, child_key, center_key;

        CREATE INDEX IX_tmp_daily_core_keys ON #daily_core(date_key, child_key, center_key);

        SELECT @rows_read = COUNT(*) FROM #daily_core;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'fact_daily_student_task_progress';
        TRUNCATE TABLE dw.fact_daily_student_task_progress;

        INSERT INTO dw.fact_daily_student_task_progress (
            date_key, child_key, center_key, teacher_key,
            raw_score, min_score, max_score, normalized_score,
            planned_task_count, assessment_count, completed_task_count,
            scored_task_count, not_scored_task_count,
            source_system, etl_batch_id, loaded_at
        )
        SELECT
            dc.date_key,
            dc.child_key,
            dc.center_key,
            COALESCE(dc.teacher_key, -1),
            dc.raw_score,
            NULL,
            NULL,
            dc.normalized_score,
            dc.planned_task_count,
            dc.assessment_count,
            dc.completed_task_count,
            dc.scored_task_count,
            dc.not_scored_task_count,
            N'PROGRAM_OPS',
            @etl_batch_id,
            SYSDATETIME()
        FROM #daily_core AS dc;

        SET @rows_inserted = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress', N'dw', N'fact_daily_student_task_progress', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Daily aggregate fact loaded in one insert segment.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress', N'dw', N'fact_daily_student_task_progress', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Child Snapshot Accumulation
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_child_snapshot_accumulation
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#snapshot_core') IS NOT NULL DROP TABLE #snapshot_core;

        SELECT
            MAX(date_key) AS snapshot_date_key,
            child_key,
            MIN(center_key) AS center_key,
            MIN(NULLIF(teacher_key, -1)) AS teacher_key,
            SUM(planned_task_count) AS planned_task_count,
            SUM(assessment_count) AS assessment_count,
            SUM(completed_task_count) AS completed_task_count,
            SUM(scored_task_count) AS scored_task_count,
            MIN(CASE WHEN planned_task_count > 0 THEN date_key END) AS first_plan_date_key,
            MAX(CASE WHEN planned_task_count > 0 THEN date_key END) AS last_plan_date_key,
            MIN(CASE WHEN assessment_count > 0 THEN date_key END) AS first_assessment_date_key,
            MAX(CASE WHEN assessment_count > 0 THEN date_key END) AS last_assessment_date_key
        INTO #snapshot_core
        FROM dw.fact_daily_student_task_progress
        WHERE etl_batch_id = @etl_batch_id OR @etl_batch_id IS NULL
        GROUP BY child_key;

        CREATE INDEX IX_tmp_snapshot_core_child ON #snapshot_core(child_key);

        SELECT @rows_read = COUNT(*) FROM #snapshot_core;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'fact_child_snapshot_accumulation';
        TRUNCATE TABLE dw.fact_child_snapshot_accumulation;

        INSERT INTO dw.fact_child_snapshot_accumulation (
            snapshot_date_key, child_key, center_key, teacher_key,
            planned_task_count, assessment_count, completed_task_count, scored_task_count,
            first_plan_date_key, last_plan_date_key, first_assessment_date_key, last_assessment_date_key,
            source_system, etl_batch_id, loaded_at
        )
        SELECT
            COALESCE(sc.snapshot_date_key, -1),
            sc.child_key,
            COALESCE(sc.center_key, -1),
            COALESCE(sc.teacher_key, -1),
            sc.planned_task_count,
            sc.assessment_count,
            sc.completed_task_count,
            sc.scored_task_count,
            COALESCE(sc.first_plan_date_key, -1),
            COALESCE(sc.last_plan_date_key, -1),
            COALESCE(sc.first_assessment_date_key, -1),
            COALESCE(sc.last_assessment_date_key, -1),
            N'PROGRAM_OPS',
            @etl_batch_id,
            SYSDATETIME()
        FROM #snapshot_core AS sc;

        SET @rows_inserted = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress', N'dw', N'fact_child_snapshot_accumulation', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Child snapshot fact loaded in one insert segment.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress', N'dw', N'fact_child_snapshot_accumulation', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Child Task Event, including deleted business events
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_child_task_event
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#event_core') IS NOT NULL DROP TABLE #event_core;

        SELECT
            child_key,
            task_key,
            teacher_key,
            center_key,
            domain_key,
            date_key,
            N'planned' AS event_type,
            CASE WHEN is_planned = 1 THEN N'planned' ELSE N'unplanned' END AS event_status,
            raw_score,
            normalized_score,
            source_daily_task_assignment_id,
            source_task_assessment_id,
            source_assessment_session_id,
            source_system
        INTO #event_core
        FROM dw.fact_tran_student_task_progress
        WHERE source_daily_task_assignment_id IS NOT NULL
          AND (etl_batch_id = @etl_batch_id OR @etl_batch_id IS NULL)
        UNION ALL
        SELECT
            child_key,
            task_key,
            teacher_key,
            center_key,
            domain_key,
            date_key,
            N'assessed' AS event_type,
            CASE WHEN is_scored = 1 THEN N'scored' WHEN is_not_scored = 1 THEN N'not_scored' ELSE N'assessed' END AS event_status,
            raw_score,
            normalized_score,
            source_daily_task_assignment_id,
            source_task_assessment_id,
            source_assessment_session_id,
            source_system
        FROM dw.fact_tran_student_task_progress
        WHERE source_task_assessment_id IS NOT NULL
          AND (etl_batch_id = @etl_batch_id OR @etl_batch_id IS NULL)
        UNION ALL
        SELECT
            -1 AS child_key,
            -1 AS task_key,
            -1 AS teacher_key,
            -1 AS center_key,
            -1 AS domain_key,
            COALESCE(dd.date_key, -1) AS date_key,
            N'deleted' AS event_type,
            LOWER(al.entity_name) AS event_status,
            NULL AS raw_score,
            NULL AS normalized_score,
            CASE WHEN LOWER(al.entity_name) = N'daily_task_assignments' THEN al.entity_id ELSE NULL END,
            CASE WHEN LOWER(al.entity_name) = N'task_assessments' THEN al.entity_id ELSE NULL END,
            CASE WHEN LOWER(al.entity_name) = N'assessment_sessions' THEN al.entity_id ELSE NULL END,
            al.source_system
        FROM Stg_ProgramOps_DB.stg_program_ops.audit_logs AS al
        LEFT JOIN dw.dim_date AS dd
            ON CONVERT(DATE, al.created_at) = dd.full_date
        WHERE al.is_valid = 1
          AND LOWER(COALESCE(al.action, N'')) IN (N'delete', N'deleted', N'remove', N'removed')
          AND LOWER(COALESCE(al.entity_name, N'')) IN (N'daily_task_assignments', N'task_assessments', N'assessment_sessions', N'child_task_plans', N'children', N'teachers', N'centers');

        CREATE INDEX IX_tmp_event_core_keys ON #event_core(date_key, child_key, event_type);

        SELECT @rows_read = COUNT(*) FROM #event_core;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'fact_child_task_event';
        TRUNCATE TABLE dw.fact_child_task_event;

        INSERT INTO dw.fact_child_task_event (
            child_key, task_key, teacher_key, center_key, domain_key, date_key,
            event_type, event_status, raw_score, normalized_score,
            source_daily_task_assignment_id, source_task_assessment_id, source_assessment_session_id,
            source_system, etl_batch_id, loaded_at
        )
        SELECT
            child_key,
            task_key,
            teacher_key,
            center_key,
            domain_key,
            date_key,
            event_type,
            event_status,
            raw_score,
            normalized_score,
            source_daily_task_assignment_id,
            source_task_assessment_id,
            source_assessment_session_id,
            source_system,
            @etl_batch_id,
            SYSDATETIME()
        FROM #event_core;

        SET @rows_inserted = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB/Stg_ProgramOps_DB', N'dw/stg_program_ops', N'fact_tran_student_task_progress/audit_logs', N'dw', N'fact_child_task_event', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Child task event fact loaded in one insert segment including deleted business events.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB/Stg_ProgramOps_DB', N'dw/stg_program_ops', N'fact_tran_student_task_progress/audit_logs', N'dw', N'fact_child_task_event', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Main Runner: MART 1 DW ETL
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_run_dw_mart1_all
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @etl_batch_id INT;

    BEGIN TRY
        INSERT INTO etl_admin.etl_batch (
            source_system, target_layer, mart_name, batch_status,
            started_at, rows_read, rows_inserted, rows_updated, rows_rejected, error_message
        )
        VALUES (
            N'PROGRAM_OPS', N'DW', N'MART1_STUDENT_TASK_PROGRESS', N'running',
            @started_at, 0, 0, 0, 0, NULL
        );

        SET @etl_batch_id = SCOPE_IDENTITY();

        EXEC etl_admin.usp_load_dw_mart1_dim_date @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_center @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_teacher @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_child @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_domain @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_task @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_score_scale @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_assessment_status @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_no_score_reason @etl_batch_id;

        EXEC etl_admin.usp_load_dw_mart1_fact_tran_student_task_progress @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_fact_daily_student_task_progress @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_fact_child_snapshot_accumulation @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_fact_child_task_event @etl_batch_id;

        EXEC etl_admin.usp_dw_mart1_write_load_log
            @etl_batch_id,
            N'Stg_ProgramOps_DB',
            N'stg_program_ops',
            N'MART1_ALL',
            N'dw',
            N'MART1_ALL',
            N'succeeded',
            NULL,
            NULL,
            NULL,
            @started_at,
            N'MART 1 DW ETL runner completed. Batch final status is recorded in this log row because the ETL standard for this phase forbids row modification statements.';
    END TRY
    BEGIN CATCH
        EXEC etl_admin.usp_dw_mart1_write_load_log
            @etl_batch_id,
            N'Stg_ProgramOps_DB',
            N'stg_program_ops',
            N'MART1_ALL',
            N'dw',
            N'MART1_ALL',
            N'failed',
            NULL,
            NULL,
            NULL,
            @started_at,
            ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

PRINT 'MART 1 DW ETL procedures created. Run EXEC etl_admin.usp_run_dw_mart1_all after staging load.';
GO
