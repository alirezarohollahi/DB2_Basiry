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
  Helper: write detailed ETL step log row for every temp/target insertion
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_dw_mart1_write_step_log
    @etl_batch_id       INT,
    @step_name          NVARCHAR(128),
    @target_table       NVARCHAR(128),
    @rows_inserted      INT,
    @description        NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @step_started_at DATETIME2(0) = SYSDATETIME();

    EXEC etl_admin.usp_dw_mart1_write_load_log
        @etl_batch_id       = @etl_batch_id,
        @source_database    = N'ETL_RUNTIME',
        @source_schema      = N'tempdb_or_dw',
        @source_table       = @step_name,
        @target_schema      = N'dw',
        @target_table       = @target_table,
        @load_status        = N'step_succeeded',
        @rows_read          = @rows_inserted,
        @rows_inserted      = @rows_inserted,
        @rows_rejected      = 0,
        @started_at         = @step_started_at,
        @message            = @description;
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
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));

    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#dim_date_sample') IS NOT NULL DROP TABLE #dim_date_sample;

        CREATE TABLE #dim_date_sample (
            TimeKey INT NULL,
            FullDateAlternateKey DATE NULL,
            PersianFullDateAlternateKey NVARCHAR(10) NULL,
            DayNumberOfWeek TINYINT NULL,
            PersianDayNumberOfWeek TINYINT NULL,
            EnglishDayNameOfWeek NVARCHAR(20) NULL,
            PersianDayNameOfWeek NVARCHAR(20) NULL,
            DayNumberOfMonth TINYINT NULL,
            PersianDayNumberOfMonth TINYINT NULL,
            DayNumberOfYear SMALLINT NULL,
            PersianDayNumberOfYear SMALLINT NULL,
            WeekNumberOfYear TINYINT NULL,
            PersianWeekNumberOfYear TINYINT NULL,
            EnglishMonthName NVARCHAR(20) NULL,
            PersianMonthName NVARCHAR(20) NULL,
            MonthNumberOfYear TINYINT NULL,
            PersianMonthNumberOfYear TINYINT NULL,
            CalendarQuarter TINYINT NULL,
            PersianCalendarQuarter TINYINT NULL,
            CalendarYear SMALLINT NULL,
            PersianCalendarYear SMALLINT NULL,
            CalendarSemester TINYINT NULL,
            PersianCalendarSemester TINYINT NULL
        );

        INSERT INTO #dim_date_sample (
            TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey, DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek, DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear, WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName, MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter, CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
        )
        VALUES
            (20120904, CONVERT(DATE, '20120904'), N'1391-06-14', 3, 4, N'Tuesday', N'سه شنبه', 4, 14, 248, 169, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120905, CONVERT(DATE, '20120905'), N'1391-06-15', 4, 5, N'Wednesday', N'چهار شنبه', 5, 15, 249, 170, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120906, CONVERT(DATE, '20120906'), N'1391-06-16', 5, 6, N'Thursday', N'پنج شنبه', 6, 16, 250, 171, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120907, CONVERT(DATE, '20120907'), N'1391-06-17', 6, 7, N'Friday', N'جمعه', 7, 17, 251, 172, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120908, CONVERT(DATE, '20120908'), N'1391-06-18', 7, 1, N'Saturday', N'شنبه', 8, 18, 252, 173, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120909, CONVERT(DATE, '20120909'), N'1391-06-19', 1, 2, N'Sunday', N'یک شنبه', 9, 19, 253, 174, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120910, CONVERT(DATE, '20120910'), N'1391-06-20', 2, 3, N'Monday', N'دو شنبه', 10, 20, 254, 175, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120911, CONVERT(DATE, '20120911'), N'1391-06-21', 3, 4, N'Tuesday', N'سه شنبه', 11, 21, 255, 176, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120912, CONVERT(DATE, '20120912'), N'1391-06-22', 4, 5, N'Wednesday', N'چهار شنبه', 12, 22, 256, 177, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120913, CONVERT(DATE, '20120913'), N'1391-06-23', 5, 6, N'Thursday', N'پنج شنبه', 13, 23, 257, 178, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120914, CONVERT(DATE, '20120914'), N'1391-06-24', 6, 7, N'Friday', N'جمعه', 14, 24, 258, 179, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120915, CONVERT(DATE, '20120915'), N'1391-06-25', 7, 1, N'Saturday', N'شنبه', 15, 25, 259, 180, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120916, CONVERT(DATE, '20120916'), N'1391-06-26', 1, 2, N'Sunday', N'یک شنبه', 16, 26, 260, 181, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120917, CONVERT(DATE, '20120917'), N'1391-06-27', 2, 3, N'Monday', N'دو شنبه', 17, 27, 261, 182, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120918, CONVERT(DATE, '20120918'), N'1391-06-28', 3, 4, N'Tuesday', N'سه شنبه', 18, 28, 262, 183, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120919, CONVERT(DATE, '20120919'), N'1391-06-29', 4, 5, N'Wednesday', N'چهار شنبه', 19, 29, 263, 184, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120920, CONVERT(DATE, '20120920'), N'1391-06-30', 5, 6, N'Thursday', N'پنج شنبه', 20, 30, 264, 185, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120921, CONVERT(DATE, '20120921'), N'1391-06-31', 6, 7, N'Friday', N'جمعه', 21, 31, 265, 186, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120922, CONVERT(DATE, '20120922'), N'1391-07-01', 7, 1, N'Saturday', N'شنبه', 22, 1, 266, 187, 38, 27, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120923, CONVERT(DATE, '20120923'), N'1391-07-02', 1, 2, N'Sunday', N'یک شنبه', 23, 2, 267, 188, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120924, CONVERT(DATE, '20120924'), N'1391-07-03', 2, 3, N'Monday', N'دو شنبه', 24, 3, 268, 189, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120925, CONVERT(DATE, '20120925'), N'1391-07-04', 3, 4, N'Tuesday', N'سه شنبه', 25, 4, 269, 190, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120926, CONVERT(DATE, '20120926'), N'1391-07-05', 4, 5, N'Wednesday', N'چهار شنبه', 26, 5, 270, 191, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120927, CONVERT(DATE, '20120927'), N'1391-07-06', 5, 6, N'Thursday', N'پنج شنبه', 27, 6, 271, 192, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120928, CONVERT(DATE, '20120928'), N'1391-07-07', 6, 7, N'Friday', N'جمعه', 28, 7, 272, 193, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120929, CONVERT(DATE, '20120929'), N'1391-07-08', 7, 1, N'Saturday', N'شنبه', 29, 8, 273, 194, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20120930, CONVERT(DATE, '20120930'), N'1391-07-09', 1, 2, N'Sunday', N'یک شنبه', 30, 9, 274, 195, 40, 29, N'September', N'مهر', 9, 7, 3, 3, 2012, 1391, 2, 2),
            (20121001, CONVERT(DATE, '20121001'), N'1391-07-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 275, 196, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121002, CONVERT(DATE, '20121002'), N'1391-07-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 276, 197, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121003, CONVERT(DATE, '20121003'), N'1391-07-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 277, 198, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121004, CONVERT(DATE, '20121004'), N'1391-07-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 278, 199, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121005, CONVERT(DATE, '20121005'), N'1391-07-14', 6, 7, N'Friday', N'جمعه', 5, 14, 279, 200, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121006, CONVERT(DATE, '20121006'), N'1391-07-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 280, 201, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121007, CONVERT(DATE, '20121007'), N'1391-07-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 281, 202, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121008, CONVERT(DATE, '20121008'), N'1391-07-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 282, 203, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121009, CONVERT(DATE, '20121009'), N'1391-07-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 283, 204, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121010, CONVERT(DATE, '20121010'), N'1391-07-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 284, 205, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121011, CONVERT(DATE, '20121011'), N'1391-07-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 285, 206, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121012, CONVERT(DATE, '20121012'), N'1391-07-21', 6, 7, N'Friday', N'جمعه', 12, 21, 286, 207, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121013, CONVERT(DATE, '20121013'), N'1391-07-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 287, 208, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121014, CONVERT(DATE, '20121014'), N'1391-07-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 288, 209, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121015, CONVERT(DATE, '20121015'), N'1391-07-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 289, 210, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121016, CONVERT(DATE, '20121016'), N'1391-07-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 290, 211, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121017, CONVERT(DATE, '20121017'), N'1391-07-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 291, 212, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121018, CONVERT(DATE, '20121018'), N'1391-07-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 292, 213, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121019, CONVERT(DATE, '20121019'), N'1391-07-28', 6, 7, N'Friday', N'جمعه', 19, 28, 293, 214, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121020, CONVERT(DATE, '20121020'), N'1391-07-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 294, 215, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121021, CONVERT(DATE, '20121021'), N'1391-07-30', 1, 2, N'Sunday', N'یک شنبه', 21, 30, 295, 216, 43, 32, N'October', N'مهر', 10, 7, 4, 3, 2012, 1391, 2, 2),
            (20121022, CONVERT(DATE, '20121022'), N'1391-08-01', 2, 3, N'Monday', N'دو شنبه', 22, 1, 296, 217, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121023, CONVERT(DATE, '20121023'), N'1391-08-02', 3, 4, N'Tuesday', N'سه شنبه', 23, 2, 297, 218, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121024, CONVERT(DATE, '20121024'), N'1391-08-03', 4, 5, N'Wednesday', N'چهار شنبه', 24, 3, 298, 219, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121025, CONVERT(DATE, '20121025'), N'1391-08-04', 5, 6, N'Thursday', N'پنج شنبه', 25, 4, 299, 220, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121026, CONVERT(DATE, '20121026'), N'1391-08-05', 6, 7, N'Friday', N'جمعه', 26, 5, 300, 221, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121027, CONVERT(DATE, '20121027'), N'1391-08-06', 7, 1, N'Saturday', N'شنبه', 27, 6, 301, 222, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121028, CONVERT(DATE, '20121028'), N'1391-08-07', 1, 2, N'Sunday', N'یک شنبه', 28, 7, 302, 223, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121029, CONVERT(DATE, '20121029'), N'1391-08-08', 2, 3, N'Monday', N'دو شنبه', 29, 8, 303, 224, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121030, CONVERT(DATE, '20121030'), N'1391-08-09', 3, 4, N'Tuesday', N'سه شنبه', 30, 9, 304, 225, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121031, CONVERT(DATE, '20121031'), N'1391-08-10', 4, 5, N'Wednesday', N'چهار شنبه', 31, 10, 305, 226, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2012, 1391, 2, 2),
            (20121101, CONVERT(DATE, '20121101'), N'1391-08-11', 5, 6, N'Thursday', N'پنج شنبه', 1, 11, 306, 227, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121102, CONVERT(DATE, '20121102'), N'1391-08-12', 6, 7, N'Friday', N'جمعه', 2, 12, 307, 228, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121103, CONVERT(DATE, '20121103'), N'1391-08-13', 7, 1, N'Saturday', N'شنبه', 3, 13, 308, 229, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121104, CONVERT(DATE, '20121104'), N'1391-08-14', 1, 2, N'Sunday', N'یک شنبه', 4, 14, 309, 230, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121105, CONVERT(DATE, '20121105'), N'1391-08-15', 2, 3, N'Monday', N'دو شنبه', 5, 15, 310, 231, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121106, CONVERT(DATE, '20121106'), N'1391-08-16', 3, 4, N'Tuesday', N'سه شنبه', 6, 16, 311, 232, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121107, CONVERT(DATE, '20121107'), N'1391-08-17', 4, 5, N'Wednesday', N'چهار شنبه', 7, 17, 312, 233, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121108, CONVERT(DATE, '20121108'), N'1391-08-18', 5, 6, N'Thursday', N'پنج شنبه', 8, 18, 313, 234, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121109, CONVERT(DATE, '20121109'), N'1391-08-19', 6, 7, N'Friday', N'جمعه', 9, 19, 314, 235, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121110, CONVERT(DATE, '20121110'), N'1391-08-20', 7, 1, N'Saturday', N'شنبه', 10, 20, 315, 236, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121111, CONVERT(DATE, '20121111'), N'1391-08-21', 1, 2, N'Sunday', N'یک شنبه', 11, 21, 316, 237, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121112, CONVERT(DATE, '20121112'), N'1391-08-22', 2, 3, N'Monday', N'دو شنبه', 12, 22, 317, 238, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121113, CONVERT(DATE, '20121113'), N'1391-08-23', 3, 4, N'Tuesday', N'سه شنبه', 13, 23, 318, 239, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121114, CONVERT(DATE, '20121114'), N'1391-08-24', 4, 5, N'Wednesday', N'چهار شنبه', 14, 24, 319, 240, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121115, CONVERT(DATE, '20121115'), N'1391-08-25', 5, 6, N'Thursday', N'پنج شنبه', 15, 25, 320, 241, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121116, CONVERT(DATE, '20121116'), N'1391-08-26', 6, 7, N'Friday', N'جمعه', 16, 26, 321, 242, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121117, CONVERT(DATE, '20121117'), N'1391-08-27', 7, 1, N'Saturday', N'شنبه', 17, 27, 322, 243, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121118, CONVERT(DATE, '20121118'), N'1391-08-28', 1, 2, N'Sunday', N'یک شنبه', 18, 28, 323, 244, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121119, CONVERT(DATE, '20121119'), N'1391-08-29', 2, 3, N'Monday', N'دو شنبه', 19, 29, 324, 245, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121120, CONVERT(DATE, '20121120'), N'1391-08-30', 3, 4, N'Tuesday', N'سه شنبه', 20, 30, 325, 246, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2012, 1391, 2, 2),
            (20121121, CONVERT(DATE, '20121121'), N'1391-09-01', 4, 5, N'Wednesday', N'چهار شنبه', 21, 1, 326, 247, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121122, CONVERT(DATE, '20121122'), N'1391-09-02', 5, 6, N'Thursday', N'پنج شنبه', 22, 2, 327, 248, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121123, CONVERT(DATE, '20121123'), N'1391-09-03', 6, 7, N'Friday', N'جمعه', 23, 3, 328, 249, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121124, CONVERT(DATE, '20121124'), N'1391-09-04', 7, 1, N'Saturday', N'شنبه', 24, 4, 329, 250, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121125, CONVERT(DATE, '20121125'), N'1391-09-05', 1, 2, N'Sunday', N'یک شنبه', 25, 5, 330, 251, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121126, CONVERT(DATE, '20121126'), N'1391-09-06', 2, 3, N'Monday', N'دو شنبه', 26, 6, 331, 252, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121127, CONVERT(DATE, '20121127'), N'1391-09-07', 3, 4, N'Tuesday', N'سه شنبه', 27, 7, 332, 253, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121128, CONVERT(DATE, '20121128'), N'1391-09-08', 4, 5, N'Wednesday', N'چهار شنبه', 28, 8, 333, 254, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121129, CONVERT(DATE, '20121129'), N'1391-09-09', 5, 6, N'Thursday', N'پنج شنبه', 29, 9, 334, 255, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121130, CONVERT(DATE, '20121130'), N'1391-09-10', 6, 7, N'Friday', N'جمعه', 30, 10, 335, 256, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2012, 1391, 2, 2),
            (20121201, CONVERT(DATE, '20121201'), N'1391-09-11', 7, 1, N'Saturday', N'شنبه', 1, 11, 336, 257, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121202, CONVERT(DATE, '20121202'), N'1391-09-12', 1, 2, N'Sunday', N'یک شنبه', 2, 12, 337, 258, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121203, CONVERT(DATE, '20121203'), N'1391-09-13', 2, 3, N'Monday', N'دو شنبه', 3, 13, 338, 259, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121204, CONVERT(DATE, '20121204'), N'1391-09-14', 3, 4, N'Tuesday', N'سه شنبه', 4, 14, 339, 260, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121205, CONVERT(DATE, '20121205'), N'1391-09-15', 4, 5, N'Wednesday', N'چهار شنبه', 5, 15, 340, 261, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121206, CONVERT(DATE, '20121206'), N'1391-09-16', 5, 6, N'Thursday', N'پنج شنبه', 6, 16, 341, 262, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121207, CONVERT(DATE, '20121207'), N'1391-09-17', 6, 7, N'Friday', N'جمعه', 7, 17, 342, 263, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121208, CONVERT(DATE, '20121208'), N'1391-09-18', 7, 1, N'Saturday', N'شنبه', 8, 18, 343, 264, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121209, CONVERT(DATE, '20121209'), N'1391-09-19', 1, 2, N'Sunday', N'یک شنبه', 9, 19, 344, 265, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121210, CONVERT(DATE, '20121210'), N'1391-09-20', 2, 3, N'Monday', N'دو شنبه', 10, 20, 345, 266, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121211, CONVERT(DATE, '20121211'), N'1391-09-21', 3, 4, N'Tuesday', N'سه شنبه', 11, 21, 346, 267, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121212, CONVERT(DATE, '20121212'), N'1391-09-22', 4, 5, N'Wednesday', N'چهار شنبه', 12, 22, 347, 268, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121213, CONVERT(DATE, '20121213'), N'1391-09-23', 5, 6, N'Thursday', N'پنج شنبه', 13, 23, 348, 269, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121214, CONVERT(DATE, '20121214'), N'1391-09-24', 6, 7, N'Friday', N'جمعه', 14, 24, 349, 270, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121215, CONVERT(DATE, '20121215'), N'1391-09-25', 7, 1, N'Saturday', N'شنبه', 15, 25, 350, 271, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121216, CONVERT(DATE, '20121216'), N'1391-09-26', 1, 2, N'Sunday', N'یک شنبه', 16, 26, 351, 272, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121217, CONVERT(DATE, '20121217'), N'1391-09-27', 2, 3, N'Monday', N'دو شنبه', 17, 27, 352, 273, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121218, CONVERT(DATE, '20121218'), N'1391-09-28', 3, 4, N'Tuesday', N'سه شنبه', 18, 28, 353, 274, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121219, CONVERT(DATE, '20121219'), N'1391-09-29', 4, 5, N'Wednesday', N'چهار شنبه', 19, 29, 354, 275, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121220, CONVERT(DATE, '20121220'), N'1391-09-30', 5, 6, N'Thursday', N'پنج شنبه', 20, 30, 355, 276, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2012, 1391, 2, 2),
            (20121221, CONVERT(DATE, '20121221'), N'1391-10-01', 6, 7, N'Friday', N'جمعه', 21, 1, 356, 277, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121222, CONVERT(DATE, '20121222'), N'1391-10-02', 7, 1, N'Saturday', N'شنبه', 22, 2, 357, 278, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121223, CONVERT(DATE, '20121223'), N'1391-10-03', 1, 2, N'Sunday', N'یک شنبه', 23, 3, 358, 279, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121224, CONVERT(DATE, '20121224'), N'1391-10-04', 2, 3, N'Monday', N'دو شنبه', 24, 4, 359, 280, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121225, CONVERT(DATE, '20121225'), N'1391-10-05', 3, 4, N'Tuesday', N'سه شنبه', 25, 5, 360, 281, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121226, CONVERT(DATE, '20121226'), N'1391-10-06', 4, 5, N'Wednesday', N'چهار شنبه', 26, 6, 361, 282, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121227, CONVERT(DATE, '20121227'), N'1391-10-07', 5, 6, N'Thursday', N'پنج شنبه', 27, 7, 362, 283, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121228, CONVERT(DATE, '20121228'), N'1391-10-08', 6, 7, N'Friday', N'جمعه', 28, 8, 363, 284, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121229, CONVERT(DATE, '20121229'), N'1391-10-09', 7, 1, N'Saturday', N'شنبه', 29, 9, 364, 285, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121230, CONVERT(DATE, '20121230'), N'1391-10-10', 1, 2, N'Sunday', N'یک شنبه', 30, 10, 365, 286, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20121231, CONVERT(DATE, '20121231'), N'1391-10-11', 2, 3, N'Monday', N'دو شنبه', 31, 11, 366, 287, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2012, 1391, 2, 2),
            (20130101, CONVERT(DATE, '20130101'), N'1391-10-12', 3, 4, N'Tuesday', N'سه شنبه', 1, 12, 1, 288, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130102, CONVERT(DATE, '20130102'), N'1391-10-13', 4, 5, N'Wednesday', N'چهار شنبه', 2, 13, 2, 289, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130103, CONVERT(DATE, '20130103'), N'1391-10-14', 5, 6, N'Thursday', N'پنج شنبه', 3, 14, 3, 290, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130104, CONVERT(DATE, '20130104'), N'1391-10-15', 6, 7, N'Friday', N'جمعه', 4, 15, 4, 291, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130105, CONVERT(DATE, '20130105'), N'1391-10-16', 7, 1, N'Saturday', N'شنبه', 5, 16, 5, 292, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130106, CONVERT(DATE, '20130106'), N'1391-10-17', 1, 2, N'Sunday', N'یک شنبه', 6, 17, 6, 293, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130107, CONVERT(DATE, '20130107'), N'1391-10-18', 2, 3, N'Monday', N'دو شنبه', 7, 18, 7, 294, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130108, CONVERT(DATE, '20130108'), N'1391-10-19', 3, 4, N'Tuesday', N'سه شنبه', 8, 19, 8, 295, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130109, CONVERT(DATE, '20130109'), N'1391-10-20', 4, 5, N'Wednesday', N'چهار شنبه', 9, 20, 9, 296, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130110, CONVERT(DATE, '20130110'), N'1391-10-21', 5, 6, N'Thursday', N'پنج شنبه', 10, 21, 10, 297, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130111, CONVERT(DATE, '20130111'), N'1391-10-22', 6, 7, N'Friday', N'جمعه', 11, 22, 11, 298, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130112, CONVERT(DATE, '20130112'), N'1391-10-23', 7, 1, N'Saturday', N'شنبه', 12, 23, 12, 299, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130113, CONVERT(DATE, '20130113'), N'1391-10-24', 1, 2, N'Sunday', N'یک شنبه', 13, 24, 13, 300, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130114, CONVERT(DATE, '20130114'), N'1391-10-25', 2, 3, N'Monday', N'دو شنبه', 14, 25, 14, 301, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130115, CONVERT(DATE, '20130115'), N'1391-10-26', 3, 4, N'Tuesday', N'سه شنبه', 15, 26, 15, 302, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130116, CONVERT(DATE, '20130116'), N'1391-10-27', 4, 5, N'Wednesday', N'چهار شنبه', 16, 27, 16, 303, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130117, CONVERT(DATE, '20130117'), N'1391-10-28', 5, 6, N'Thursday', N'پنج شنبه', 17, 28, 17, 304, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130118, CONVERT(DATE, '20130118'), N'1391-10-29', 6, 7, N'Friday', N'جمعه', 18, 29, 18, 305, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130119, CONVERT(DATE, '20130119'), N'1391-10-30', 7, 1, N'Saturday', N'شنبه', 19, 30, 19, 306, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2013, 1391, 1, 2),
            (20130120, CONVERT(DATE, '20130120'), N'1391-11-01', 1, 2, N'Sunday', N'یک شنبه', 20, 1, 20, 307, 3, 44, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130121, CONVERT(DATE, '20130121'), N'1391-11-02', 2, 3, N'Monday', N'دو شنبه', 21, 2, 21, 308, 3, 44, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130122, CONVERT(DATE, '20130122'), N'1391-11-03', 3, 4, N'Tuesday', N'سه شنبه', 22, 3, 22, 309, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130123, CONVERT(DATE, '20130123'), N'1391-11-04', 4, 5, N'Wednesday', N'چهار شنبه', 23, 4, 23, 310, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130124, CONVERT(DATE, '20130124'), N'1391-11-05', 5, 6, N'Thursday', N'پنج شنبه', 24, 5, 24, 311, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130125, CONVERT(DATE, '20130125'), N'1391-11-06', 6, 7, N'Friday', N'جمعه', 25, 6, 25, 312, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130126, CONVERT(DATE, '20130126'), N'1391-11-07', 7, 1, N'Saturday', N'شنبه', 26, 7, 26, 313, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130127, CONVERT(DATE, '20130127'), N'1391-11-08', 1, 2, N'Sunday', N'یک شنبه', 27, 8, 27, 314, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130128, CONVERT(DATE, '20130128'), N'1391-11-09', 2, 3, N'Monday', N'دو شنبه', 28, 9, 28, 315, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130129, CONVERT(DATE, '20130129'), N'1391-11-10', 3, 4, N'Tuesday', N'سه شنبه', 29, 10, 29, 316, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130130, CONVERT(DATE, '20130130'), N'1391-11-11', 4, 5, N'Wednesday', N'چهار شنبه', 30, 11, 30, 317, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130131, CONVERT(DATE, '20130131'), N'1391-11-12', 5, 6, N'Thursday', N'پنج شنبه', 31, 12, 31, 318, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2013, 1391, 1, 2),
            (20130201, CONVERT(DATE, '20130201'), N'1391-11-13', 6, 7, N'Friday', N'جمعه', 1, 13, 32, 319, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130202, CONVERT(DATE, '20130202'), N'1391-11-14', 7, 1, N'Saturday', N'شنبه', 2, 14, 33, 320, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130203, CONVERT(DATE, '20130203'), N'1391-11-15', 1, 2, N'Sunday', N'یک شنبه', 3, 15, 34, 321, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130204, CONVERT(DATE, '20130204'), N'1391-11-16', 2, 3, N'Monday', N'دو شنبه', 4, 16, 35, 322, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130205, CONVERT(DATE, '20130205'), N'1391-11-17', 3, 4, N'Tuesday', N'سه شنبه', 5, 17, 36, 323, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130206, CONVERT(DATE, '20130206'), N'1391-11-18', 4, 5, N'Wednesday', N'چهار شنبه', 6, 18, 37, 324, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130207, CONVERT(DATE, '20130207'), N'1391-11-19', 5, 6, N'Thursday', N'پنج شنبه', 7, 19, 38, 325, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130208, CONVERT(DATE, '20130208'), N'1391-11-20', 6, 7, N'Friday', N'جمعه', 8, 20, 39, 326, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130209, CONVERT(DATE, '20130209'), N'1391-11-21', 7, 1, N'Saturday', N'شنبه', 9, 21, 40, 327, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130210, CONVERT(DATE, '20130210'), N'1391-11-22', 1, 2, N'Sunday', N'یک شنبه', 10, 22, 41, 328, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130211, CONVERT(DATE, '20130211'), N'1391-11-23', 2, 3, N'Monday', N'دو شنبه', 11, 23, 42, 329, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130212, CONVERT(DATE, '20130212'), N'1391-11-24', 3, 4, N'Tuesday', N'سه شنبه', 12, 24, 43, 330, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130213, CONVERT(DATE, '20130213'), N'1391-11-25', 4, 5, N'Wednesday', N'چهار شنبه', 13, 25, 44, 331, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130214, CONVERT(DATE, '20130214'), N'1391-11-26', 5, 6, N'Thursday', N'پنج شنبه', 14, 26, 45, 332, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130215, CONVERT(DATE, '20130215'), N'1391-11-27', 6, 7, N'Friday', N'جمعه', 15, 27, 46, 333, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130216, CONVERT(DATE, '20130216'), N'1391-11-28', 7, 1, N'Saturday', N'شنبه', 16, 28, 47, 334, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130217, CONVERT(DATE, '20130217'), N'1391-11-29', 1, 2, N'Sunday', N'یک شنبه', 17, 29, 48, 335, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130218, CONVERT(DATE, '20130218'), N'1391-11-30', 2, 3, N'Monday', N'دو شنبه', 18, 30, 49, 336, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2013, 1391, 1, 2),
            (20130219, CONVERT(DATE, '20130219'), N'1391-12-01', 3, 4, N'Tuesday', N'سه شنبه', 19, 1, 50, 337, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130220, CONVERT(DATE, '20130220'), N'1391-12-02', 4, 5, N'Wednesday', N'چهار شنبه', 20, 2, 51, 338, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130221, CONVERT(DATE, '20130221'), N'1391-12-03', 5, 6, N'Thursday', N'پنج شنبه', 21, 3, 52, 339, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130222, CONVERT(DATE, '20130222'), N'1391-12-04', 6, 7, N'Friday', N'جمعه', 22, 4, 53, 340, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130223, CONVERT(DATE, '20130223'), N'1391-12-05', 7, 1, N'Saturday', N'شنبه', 23, 5, 54, 341, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130224, CONVERT(DATE, '20130224'), N'1391-12-06', 1, 2, N'Sunday', N'یک شنبه', 24, 6, 55, 342, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130225, CONVERT(DATE, '20130225'), N'1391-12-07', 2, 3, N'Monday', N'دو شنبه', 25, 7, 56, 343, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130226, CONVERT(DATE, '20130226'), N'1391-12-08', 3, 4, N'Tuesday', N'سه شنبه', 26, 8, 57, 344, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130227, CONVERT(DATE, '20130227'), N'1391-12-09', 4, 5, N'Wednesday', N'چهار شنبه', 27, 9, 58, 345, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130228, CONVERT(DATE, '20130228'), N'1391-12-10', 5, 6, N'Thursday', N'پنج شنبه', 28, 10, 59, 346, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2013, 1391, 1, 2),
            (20130301, CONVERT(DATE, '20130301'), N'1391-12-11', 6, 7, N'Friday', N'جمعه', 1, 11, 60, 347, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130302, CONVERT(DATE, '20130302'), N'1391-12-12', 7, 1, N'Saturday', N'شنبه', 2, 12, 61, 348, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130303, CONVERT(DATE, '20130303'), N'1391-12-13', 1, 2, N'Sunday', N'یک شنبه', 3, 13, 62, 349, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130304, CONVERT(DATE, '20130304'), N'1391-12-14', 2, 3, N'Monday', N'دو شنبه', 4, 14, 63, 350, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130305, CONVERT(DATE, '20130305'), N'1391-12-15', 3, 4, N'Tuesday', N'سه شنبه', 5, 15, 64, 351, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130306, CONVERT(DATE, '20130306'), N'1391-12-16', 4, 5, N'Wednesday', N'چهار شنبه', 6, 16, 65, 352, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130307, CONVERT(DATE, '20130307'), N'1391-12-17', 5, 6, N'Thursday', N'پنج شنبه', 7, 17, 66, 353, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130308, CONVERT(DATE, '20130308'), N'1391-12-18', 6, 7, N'Friday', N'جمعه', 8, 18, 67, 354, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130309, CONVERT(DATE, '20130309'), N'1391-12-19', 7, 1, N'Saturday', N'شنبه', 9, 19, 68, 355, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130310, CONVERT(DATE, '20130310'), N'1391-12-20', 1, 2, N'Sunday', N'یک شنبه', 10, 20, 69, 356, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130311, CONVERT(DATE, '20130311'), N'1391-12-21', 2, 3, N'Monday', N'دو شنبه', 11, 21, 70, 357, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130312, CONVERT(DATE, '20130312'), N'1391-12-22', 3, 4, N'Tuesday', N'سه شنبه', 12, 22, 71, 358, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130313, CONVERT(DATE, '20130313'), N'1391-12-23', 4, 5, N'Wednesday', N'چهار شنبه', 13, 23, 72, 359, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130314, CONVERT(DATE, '20130314'), N'1391-12-24', 5, 6, N'Thursday', N'پنج شنبه', 14, 24, 73, 360, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130315, CONVERT(DATE, '20130315'), N'1391-12-25', 6, 7, N'Friday', N'جمعه', 15, 25, 74, 361, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130316, CONVERT(DATE, '20130316'), N'1391-12-26', 7, 1, N'Saturday', N'شنبه', 16, 26, 75, 362, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130317, CONVERT(DATE, '20130317'), N'1391-12-27', 1, 2, N'Sunday', N'یک شنبه', 17, 27, 76, 363, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130318, CONVERT(DATE, '20130318'), N'1391-12-28', 2, 3, N'Monday', N'دو شنبه', 18, 28, 77, 364, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130319, CONVERT(DATE, '20130319'), N'1391-12-29', 3, 4, N'Tuesday', N'سه شنبه', 19, 29, 78, 365, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130320, CONVERT(DATE, '20130320'), N'1391-12-30', 4, 5, N'Wednesday', N'چهار شنبه', 20, 30, 79, 366, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2013, 1391, 1, 2),
            (20130321, CONVERT(DATE, '20130321'), N'1392-01-01', 5, 6, N'Thursday', N'پنج شنبه', 21, 1, 80, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130322, CONVERT(DATE, '20130322'), N'1392-01-02', 6, 7, N'Friday', N'جمعه', 22, 2, 81, 2, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130323, CONVERT(DATE, '20130323'), N'1392-01-03', 7, 1, N'Saturday', N'شنبه', 23, 3, 82, 3, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130324, CONVERT(DATE, '20130324'), N'1392-01-04', 1, 2, N'Sunday', N'یک شنبه', 24, 4, 83, 4, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130325, CONVERT(DATE, '20130325'), N'1392-01-05', 2, 3, N'Monday', N'دو شنبه', 25, 5, 84, 5, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130326, CONVERT(DATE, '20130326'), N'1392-01-06', 3, 4, N'Tuesday', N'سه شنبه', 26, 6, 85, 6, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130327, CONVERT(DATE, '20130327'), N'1392-01-07', 4, 5, N'Wednesday', N'چهار شنبه', 27, 7, 86, 7, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130328, CONVERT(DATE, '20130328'), N'1392-01-08', 5, 6, N'Thursday', N'پنج شنبه', 28, 8, 87, 8, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130329, CONVERT(DATE, '20130329'), N'1392-01-09', 6, 7, N'Friday', N'جمعه', 29, 9, 88, 9, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130330, CONVERT(DATE, '20130330'), N'1392-01-10', 7, 1, N'Saturday', N'شنبه', 30, 10, 89, 10, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130331, CONVERT(DATE, '20130331'), N'1392-01-11', 1, 2, N'Sunday', N'یک شنبه', 31, 11, 90, 11, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2013, 1392, 1, 1),
            (20130401, CONVERT(DATE, '20130401'), N'1392-01-12', 2, 3, N'Monday', N'دو شنبه', 1, 12, 91, 12, 13, 2, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130402, CONVERT(DATE, '20130402'), N'1392-01-13', 3, 4, N'Tuesday', N'سه شنبه', 2, 13, 92, 13, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130403, CONVERT(DATE, '20130403'), N'1392-01-14', 4, 5, N'Wednesday', N'چهار شنبه', 3, 14, 93, 14, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130404, CONVERT(DATE, '20130404'), N'1392-01-15', 5, 6, N'Thursday', N'پنج شنبه', 4, 15, 94, 15, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130405, CONVERT(DATE, '20130405'), N'1392-01-16', 6, 7, N'Friday', N'جمعه', 5, 16, 95, 16, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130406, CONVERT(DATE, '20130406'), N'1392-01-17', 7, 1, N'Saturday', N'شنبه', 6, 17, 96, 17, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130407, CONVERT(DATE, '20130407'), N'1392-01-18', 1, 2, N'Sunday', N'یک شنبه', 7, 18, 97, 18, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130408, CONVERT(DATE, '20130408'), N'1392-01-19', 2, 3, N'Monday', N'دو شنبه', 8, 19, 98, 19, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130409, CONVERT(DATE, '20130409'), N'1392-01-20', 3, 4, N'Tuesday', N'سه شنبه', 9, 20, 99, 20, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130410, CONVERT(DATE, '20130410'), N'1392-01-21', 4, 5, N'Wednesday', N'چهار شنبه', 10, 21, 100, 21, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130411, CONVERT(DATE, '20130411'), N'1392-01-22', 5, 6, N'Thursday', N'پنج شنبه', 11, 22, 101, 22, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130412, CONVERT(DATE, '20130412'), N'1392-01-23', 6, 7, N'Friday', N'جمعه', 12, 23, 102, 23, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130413, CONVERT(DATE, '20130413'), N'1392-01-24', 7, 1, N'Saturday', N'شنبه', 13, 24, 103, 24, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130414, CONVERT(DATE, '20130414'), N'1392-01-25', 1, 2, N'Sunday', N'یک شنبه', 14, 25, 104, 25, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130415, CONVERT(DATE, '20130415'), N'1392-01-26', 2, 3, N'Monday', N'دو شنبه', 15, 26, 105, 26, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130416, CONVERT(DATE, '20130416'), N'1392-01-27', 3, 4, N'Tuesday', N'سه شنبه', 16, 27, 106, 27, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130417, CONVERT(DATE, '20130417'), N'1392-01-28', 4, 5, N'Wednesday', N'چهار شنبه', 17, 28, 107, 28, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130418, CONVERT(DATE, '20130418'), N'1392-01-29', 5, 6, N'Thursday', N'پنج شنبه', 18, 29, 108, 29, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130419, CONVERT(DATE, '20130419'), N'1392-01-30', 6, 7, N'Friday', N'جمعه', 19, 30, 109, 30, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130420, CONVERT(DATE, '20130420'), N'1392-01-31', 7, 1, N'Saturday', N'شنبه', 20, 31, 110, 31, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2013, 1392, 1, 1),
            (20130421, CONVERT(DATE, '20130421'), N'1392-02-01', 1, 2, N'Sunday', N'یک شنبه', 21, 1, 111, 32, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130422, CONVERT(DATE, '20130422'), N'1392-02-02', 2, 3, N'Monday', N'دو شنبه', 22, 2, 112, 33, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130423, CONVERT(DATE, '20130423'), N'1392-02-03', 3, 4, N'Tuesday', N'سه شنبه', 23, 3, 113, 34, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130424, CONVERT(DATE, '20130424'), N'1392-02-04', 4, 5, N'Wednesday', N'چهار شنبه', 24, 4, 114, 35, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130425, CONVERT(DATE, '20130425'), N'1392-02-05', 5, 6, N'Thursday', N'پنج شنبه', 25, 5, 115, 36, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130426, CONVERT(DATE, '20130426'), N'1392-02-06', 6, 7, N'Friday', N'جمعه', 26, 6, 116, 37, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130427, CONVERT(DATE, '20130427'), N'1392-02-07', 7, 1, N'Saturday', N'شنبه', 27, 7, 117, 38, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130428, CONVERT(DATE, '20130428'), N'1392-02-08', 1, 2, N'Sunday', N'یک شنبه', 28, 8, 118, 39, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130429, CONVERT(DATE, '20130429'), N'1392-02-09', 2, 3, N'Monday', N'دو شنبه', 29, 9, 119, 40, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130430, CONVERT(DATE, '20130430'), N'1392-02-10', 3, 4, N'Tuesday', N'سه شنبه', 30, 10, 120, 41, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2013, 1392, 1, 1),
            (20130501, CONVERT(DATE, '20130501'), N'1392-02-11', 4, 5, N'Wednesday', N'چهار شنبه', 1, 11, 121, 42, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130502, CONVERT(DATE, '20130502'), N'1392-02-12', 5, 6, N'Thursday', N'پنج شنبه', 2, 12, 122, 43, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130503, CONVERT(DATE, '20130503'), N'1392-02-13', 6, 7, N'Friday', N'جمعه', 3, 13, 123, 44, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130504, CONVERT(DATE, '20130504'), N'1392-02-14', 7, 1, N'Saturday', N'شنبه', 4, 14, 124, 45, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130505, CONVERT(DATE, '20130505'), N'1392-02-15', 1, 2, N'Sunday', N'یک شنبه', 5, 15, 125, 46, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130506, CONVERT(DATE, '20130506'), N'1392-02-16', 2, 3, N'Monday', N'دو شنبه', 6, 16, 126, 47, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130507, CONVERT(DATE, '20130507'), N'1392-02-17', 3, 4, N'Tuesday', N'سه شنبه', 7, 17, 127, 48, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130508, CONVERT(DATE, '20130508'), N'1392-02-18', 4, 5, N'Wednesday', N'چهار شنبه', 8, 18, 128, 49, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130509, CONVERT(DATE, '20130509'), N'1392-02-19', 5, 6, N'Thursday', N'پنج شنبه', 9, 19, 129, 50, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130510, CONVERT(DATE, '20130510'), N'1392-02-20', 6, 7, N'Friday', N'جمعه', 10, 20, 130, 51, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130511, CONVERT(DATE, '20130511'), N'1392-02-21', 7, 1, N'Saturday', N'شنبه', 11, 21, 131, 52, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130512, CONVERT(DATE, '20130512'), N'1392-02-22', 1, 2, N'Sunday', N'یک شنبه', 12, 22, 132, 53, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130513, CONVERT(DATE, '20130513'), N'1392-02-23', 2, 3, N'Monday', N'دو شنبه', 13, 23, 133, 54, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130514, CONVERT(DATE, '20130514'), N'1392-02-24', 3, 4, N'Tuesday', N'سه شنبه', 14, 24, 134, 55, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130515, CONVERT(DATE, '20130515'), N'1392-02-25', 4, 5, N'Wednesday', N'چهار شنبه', 15, 25, 135, 56, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130516, CONVERT(DATE, '20130516'), N'1392-02-26', 5, 6, N'Thursday', N'پنج شنبه', 16, 26, 136, 57, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130517, CONVERT(DATE, '20130517'), N'1392-02-27', 6, 7, N'Friday', N'جمعه', 17, 27, 137, 58, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130518, CONVERT(DATE, '20130518'), N'1392-02-28', 7, 1, N'Saturday', N'شنبه', 18, 28, 138, 59, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130519, CONVERT(DATE, '20130519'), N'1392-02-29', 1, 2, N'Sunday', N'یک شنبه', 19, 29, 139, 60, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130520, CONVERT(DATE, '20130520'), N'1392-02-30', 2, 3, N'Monday', N'دو شنبه', 20, 30, 140, 61, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130521, CONVERT(DATE, '20130521'), N'1392-02-31', 3, 4, N'Tuesday', N'سه شنبه', 21, 31, 141, 62, 21, 10, N'May', N'اردیبهشت', 5, 2, 2, 1, 2013, 1392, 1, 1),
            (20130522, CONVERT(DATE, '20130522'), N'1392-03-01', 4, 5, N'Wednesday', N'چهار شنبه', 22, 1, 142, 63, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130523, CONVERT(DATE, '20130523'), N'1392-03-02', 5, 6, N'Thursday', N'پنج شنبه', 23, 2, 143, 64, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130524, CONVERT(DATE, '20130524'), N'1392-03-03', 6, 7, N'Friday', N'جمعه', 24, 3, 144, 65, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130525, CONVERT(DATE, '20130525'), N'1392-03-04', 7, 1, N'Saturday', N'شنبه', 25, 4, 145, 66, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130526, CONVERT(DATE, '20130526'), N'1392-03-05', 1, 2, N'Sunday', N'یک شنبه', 26, 5, 146, 67, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130527, CONVERT(DATE, '20130527'), N'1392-03-06', 2, 3, N'Monday', N'دو شنبه', 27, 6, 147, 68, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130528, CONVERT(DATE, '20130528'), N'1392-03-07', 3, 4, N'Tuesday', N'سه شنبه', 28, 7, 148, 69, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130529, CONVERT(DATE, '20130529'), N'1392-03-08', 4, 5, N'Wednesday', N'چهار شنبه', 29, 8, 149, 70, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130530, CONVERT(DATE, '20130530'), N'1392-03-09', 5, 6, N'Thursday', N'پنج شنبه', 30, 9, 150, 71, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130531, CONVERT(DATE, '20130531'), N'1392-03-10', 6, 7, N'Friday', N'جمعه', 31, 10, 151, 72, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2013, 1392, 1, 1),
            (20130601, CONVERT(DATE, '20130601'), N'1392-03-11', 7, 1, N'Saturday', N'شنبه', 1, 11, 152, 73, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130602, CONVERT(DATE, '20130602'), N'1392-03-12', 1, 2, N'Sunday', N'یک شنبه', 2, 12, 153, 74, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130603, CONVERT(DATE, '20130603'), N'1392-03-13', 2, 3, N'Monday', N'دو شنبه', 3, 13, 154, 75, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130604, CONVERT(DATE, '20130604'), N'1392-03-14', 3, 4, N'Tuesday', N'سه شنبه', 4, 14, 155, 76, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130605, CONVERT(DATE, '20130605'), N'1392-03-15', 4, 5, N'Wednesday', N'چهار شنبه', 5, 15, 156, 77, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130606, CONVERT(DATE, '20130606'), N'1392-03-16', 5, 6, N'Thursday', N'پنج شنبه', 6, 16, 157, 78, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130607, CONVERT(DATE, '20130607'), N'1392-03-17', 6, 7, N'Friday', N'جمعه', 7, 17, 158, 79, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130608, CONVERT(DATE, '20130608'), N'1392-03-18', 7, 1, N'Saturday', N'شنبه', 8, 18, 159, 80, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130609, CONVERT(DATE, '20130609'), N'1392-03-19', 1, 2, N'Sunday', N'یک شنبه', 9, 19, 160, 81, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130610, CONVERT(DATE, '20130610'), N'1392-03-20', 2, 3, N'Monday', N'دو شنبه', 10, 20, 161, 82, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130611, CONVERT(DATE, '20130611'), N'1392-03-21', 3, 4, N'Tuesday', N'سه شنبه', 11, 21, 162, 83, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130612, CONVERT(DATE, '20130612'), N'1392-03-22', 4, 5, N'Wednesday', N'چهار شنبه', 12, 22, 163, 84, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130613, CONVERT(DATE, '20130613'), N'1392-03-23', 5, 6, N'Thursday', N'پنج شنبه', 13, 23, 164, 85, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130614, CONVERT(DATE, '20130614'), N'1392-03-24', 6, 7, N'Friday', N'جمعه', 14, 24, 165, 86, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130615, CONVERT(DATE, '20130615'), N'1392-03-25', 7, 1, N'Saturday', N'شنبه', 15, 25, 166, 87, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130616, CONVERT(DATE, '20130616'), N'1392-03-26', 1, 2, N'Sunday', N'یک شنبه', 16, 26, 167, 88, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130617, CONVERT(DATE, '20130617'), N'1392-03-27', 2, 3, N'Monday', N'دو شنبه', 17, 27, 168, 89, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130618, CONVERT(DATE, '20130618'), N'1392-03-28', 3, 4, N'Tuesday', N'سه شنبه', 18, 28, 169, 90, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130619, CONVERT(DATE, '20130619'), N'1392-03-29', 4, 5, N'Wednesday', N'چهار شنبه', 19, 29, 170, 91, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130620, CONVERT(DATE, '20130620'), N'1392-03-30', 5, 6, N'Thursday', N'پنج شنبه', 20, 30, 171, 92, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130621, CONVERT(DATE, '20130621'), N'1392-03-31', 6, 7, N'Friday', N'جمعه', 21, 31, 172, 93, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2013, 1392, 1, 1),
            (20130622, CONVERT(DATE, '20130622'), N'1392-04-01', 7, 1, N'Saturday', N'شنبه', 22, 1, 173, 94, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130623, CONVERT(DATE, '20130623'), N'1392-04-02', 1, 2, N'Sunday', N'یک شنبه', 23, 2, 174, 95, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130624, CONVERT(DATE, '20130624'), N'1392-04-03', 2, 3, N'Monday', N'دو شنبه', 24, 3, 175, 96, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130625, CONVERT(DATE, '20130625'), N'1392-04-04', 3, 4, N'Tuesday', N'سه شنبه', 25, 4, 176, 97, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130626, CONVERT(DATE, '20130626'), N'1392-04-05', 4, 5, N'Wednesday', N'چهار شنبه', 26, 5, 177, 98, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130627, CONVERT(DATE, '20130627'), N'1392-04-06', 5, 6, N'Thursday', N'پنج شنبه', 27, 6, 178, 99, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130628, CONVERT(DATE, '20130628'), N'1392-04-07', 6, 7, N'Friday', N'جمعه', 28, 7, 179, 100, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130629, CONVERT(DATE, '20130629'), N'1392-04-08', 7, 1, N'Saturday', N'شنبه', 29, 8, 180, 101, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130630, CONVERT(DATE, '20130630'), N'1392-04-09', 1, 2, N'Sunday', N'یک شنبه', 30, 9, 181, 102, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2013, 1392, 1, 1),
            (20130701, CONVERT(DATE, '20130701'), N'1392-04-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 182, 103, 26, 15, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130702, CONVERT(DATE, '20130702'), N'1392-04-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 183, 104, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130703, CONVERT(DATE, '20130703'), N'1392-04-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 184, 105, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130704, CONVERT(DATE, '20130704'), N'1392-04-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 185, 106, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130705, CONVERT(DATE, '20130705'), N'1392-04-14', 6, 7, N'Friday', N'جمعه', 5, 14, 186, 107, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130706, CONVERT(DATE, '20130706'), N'1392-04-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 187, 108, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130707, CONVERT(DATE, '20130707'), N'1392-04-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 188, 109, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130708, CONVERT(DATE, '20130708'), N'1392-04-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 189, 110, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130709, CONVERT(DATE, '20130709'), N'1392-04-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 190, 111, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130710, CONVERT(DATE, '20130710'), N'1392-04-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 191, 112, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130711, CONVERT(DATE, '20130711'), N'1392-04-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 192, 113, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130712, CONVERT(DATE, '20130712'), N'1392-04-21', 6, 7, N'Friday', N'جمعه', 12, 21, 193, 114, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130713, CONVERT(DATE, '20130713'), N'1392-04-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 194, 115, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130714, CONVERT(DATE, '20130714'), N'1392-04-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 195, 116, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130715, CONVERT(DATE, '20130715'), N'1392-04-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 196, 117, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130716, CONVERT(DATE, '20130716'), N'1392-04-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 197, 118, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130717, CONVERT(DATE, '20130717'), N'1392-04-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 198, 119, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130718, CONVERT(DATE, '20130718'), N'1392-04-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 199, 120, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130719, CONVERT(DATE, '20130719'), N'1392-04-28', 6, 7, N'Friday', N'جمعه', 19, 28, 200, 121, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130720, CONVERT(DATE, '20130720'), N'1392-04-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 201, 122, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130721, CONVERT(DATE, '20130721'), N'1392-04-30', 1, 2, N'Sunday', N'یک شنبه', 21, 30, 202, 123, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130722, CONVERT(DATE, '20130722'), N'1392-04-31', 2, 3, N'Monday', N'دو شنبه', 22, 31, 203, 124, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2013, 1392, 2, 1),
            (20130723, CONVERT(DATE, '20130723'), N'1392-05-01', 3, 4, N'Tuesday', N'سه شنبه', 23, 1, 204, 125, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130724, CONVERT(DATE, '20130724'), N'1392-05-02', 4, 5, N'Wednesday', N'چهار شنبه', 24, 2, 205, 126, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130725, CONVERT(DATE, '20130725'), N'1392-05-03', 5, 6, N'Thursday', N'پنج شنبه', 25, 3, 206, 127, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130726, CONVERT(DATE, '20130726'), N'1392-05-04', 6, 7, N'Friday', N'جمعه', 26, 4, 207, 128, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130727, CONVERT(DATE, '20130727'), N'1392-05-05', 7, 1, N'Saturday', N'شنبه', 27, 5, 208, 129, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130728, CONVERT(DATE, '20130728'), N'1392-05-06', 1, 2, N'Sunday', N'یک شنبه', 28, 6, 209, 130, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130729, CONVERT(DATE, '20130729'), N'1392-05-07', 2, 3, N'Monday', N'دو شنبه', 29, 7, 210, 131, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130730, CONVERT(DATE, '20130730'), N'1392-05-08', 3, 4, N'Tuesday', N'سه شنبه', 30, 8, 211, 132, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130731, CONVERT(DATE, '20130731'), N'1392-05-09', 4, 5, N'Wednesday', N'چهار شنبه', 31, 9, 212, 133, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2013, 1392, 2, 1),
            (20130801, CONVERT(DATE, '20130801'), N'1392-05-10', 5, 6, N'Thursday', N'پنج شنبه', 1, 10, 213, 134, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130802, CONVERT(DATE, '20130802'), N'1392-05-11', 6, 7, N'Friday', N'جمعه', 2, 11, 214, 135, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130803, CONVERT(DATE, '20130803'), N'1392-05-12', 7, 1, N'Saturday', N'شنبه', 3, 12, 215, 136, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130804, CONVERT(DATE, '20130804'), N'1392-05-13', 1, 2, N'Sunday', N'یک شنبه', 4, 13, 216, 137, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130805, CONVERT(DATE, '20130805'), N'1392-05-14', 2, 3, N'Monday', N'دو شنبه', 5, 14, 217, 138, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130806, CONVERT(DATE, '20130806'), N'1392-05-15', 3, 4, N'Tuesday', N'سه شنبه', 6, 15, 218, 139, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130807, CONVERT(DATE, '20130807'), N'1392-05-16', 4, 5, N'Wednesday', N'چهار شنبه', 7, 16, 219, 140, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130808, CONVERT(DATE, '20130808'), N'1392-05-17', 5, 6, N'Thursday', N'پنج شنبه', 8, 17, 220, 141, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130809, CONVERT(DATE, '20130809'), N'1392-05-18', 6, 7, N'Friday', N'جمعه', 9, 18, 221, 142, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130810, CONVERT(DATE, '20130810'), N'1392-05-19', 7, 1, N'Saturday', N'شنبه', 10, 19, 222, 143, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130811, CONVERT(DATE, '20130811'), N'1392-05-20', 1, 2, N'Sunday', N'یک شنبه', 11, 20, 223, 144, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130812, CONVERT(DATE, '20130812'), N'1392-05-21', 2, 3, N'Monday', N'دو شنبه', 12, 21, 224, 145, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130813, CONVERT(DATE, '20130813'), N'1392-05-22', 3, 4, N'Tuesday', N'سه شنبه', 13, 22, 225, 146, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130814, CONVERT(DATE, '20130814'), N'1392-05-23', 4, 5, N'Wednesday', N'چهار شنبه', 14, 23, 226, 147, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130815, CONVERT(DATE, '20130815'), N'1392-05-24', 5, 6, N'Thursday', N'پنج شنبه', 15, 24, 227, 148, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130816, CONVERT(DATE, '20130816'), N'1392-05-25', 6, 7, N'Friday', N'جمعه', 16, 25, 228, 149, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130817, CONVERT(DATE, '20130817'), N'1392-05-26', 7, 1, N'Saturday', N'شنبه', 17, 26, 229, 150, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130818, CONVERT(DATE, '20130818'), N'1392-05-27', 1, 2, N'Sunday', N'یک شنبه', 18, 27, 230, 151, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130819, CONVERT(DATE, '20130819'), N'1392-05-28', 2, 3, N'Monday', N'دو شنبه', 19, 28, 231, 152, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130820, CONVERT(DATE, '20130820'), N'1392-05-29', 3, 4, N'Tuesday', N'سه شنبه', 20, 29, 232, 153, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130821, CONVERT(DATE, '20130821'), N'1392-05-30', 4, 5, N'Wednesday', N'چهار شنبه', 21, 30, 233, 154, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130822, CONVERT(DATE, '20130822'), N'1392-05-31', 5, 6, N'Thursday', N'پنج شنبه', 22, 31, 234, 155, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2013, 1392, 2, 1),
            (20130823, CONVERT(DATE, '20130823'), N'1392-06-01', 6, 7, N'Friday', N'جمعه', 23, 1, 235, 156, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130824, CONVERT(DATE, '20130824'), N'1392-06-02', 7, 1, N'Saturday', N'شنبه', 24, 2, 236, 157, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130825, CONVERT(DATE, '20130825'), N'1392-06-03', 1, 2, N'Sunday', N'یک شنبه', 25, 3, 237, 158, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130826, CONVERT(DATE, '20130826'), N'1392-06-04', 2, 3, N'Monday', N'دو شنبه', 26, 4, 238, 159, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130827, CONVERT(DATE, '20130827'), N'1392-06-05', 3, 4, N'Tuesday', N'سه شنبه', 27, 5, 239, 160, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130828, CONVERT(DATE, '20130828'), N'1392-06-06', 4, 5, N'Wednesday', N'چهار شنبه', 28, 6, 240, 161, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130829, CONVERT(DATE, '20130829'), N'1392-06-07', 5, 6, N'Thursday', N'پنج شنبه', 29, 7, 241, 162, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130830, CONVERT(DATE, '20130830'), N'1392-06-08', 6, 7, N'Friday', N'جمعه', 30, 8, 242, 163, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130831, CONVERT(DATE, '20130831'), N'1392-06-09', 7, 1, N'Saturday', N'شنبه', 31, 9, 243, 164, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2013, 1392, 2, 1),
            (20130901, CONVERT(DATE, '20130901'), N'1392-06-10', 1, 2, N'Sunday', N'یک شنبه', 1, 10, 244, 165, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130902, CONVERT(DATE, '20130902'), N'1392-06-11', 2, 3, N'Monday', N'دو شنبه', 2, 11, 245, 166, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130903, CONVERT(DATE, '20130903'), N'1392-06-12', 3, 4, N'Tuesday', N'سه شنبه', 3, 12, 246, 167, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130904, CONVERT(DATE, '20130904'), N'1392-06-13', 4, 5, N'Wednesday', N'چهار شنبه', 4, 13, 247, 168, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130905, CONVERT(DATE, '20130905'), N'1392-06-14', 5, 6, N'Thursday', N'پنج شنبه', 5, 14, 248, 169, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130906, CONVERT(DATE, '20130906'), N'1392-06-15', 6, 7, N'Friday', N'جمعه', 6, 15, 249, 170, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130907, CONVERT(DATE, '20130907'), N'1392-06-16', 7, 1, N'Saturday', N'شنبه', 7, 16, 250, 171, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130908, CONVERT(DATE, '20130908'), N'1392-06-17', 1, 2, N'Sunday', N'یک شنبه', 8, 17, 251, 172, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130909, CONVERT(DATE, '20130909'), N'1392-06-18', 2, 3, N'Monday', N'دو شنبه', 9, 18, 252, 173, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130910, CONVERT(DATE, '20130910'), N'1392-06-19', 3, 4, N'Tuesday', N'سه شنبه', 10, 19, 253, 174, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130911, CONVERT(DATE, '20130911'), N'1392-06-20', 4, 5, N'Wednesday', N'چهار شنبه', 11, 20, 254, 175, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130912, CONVERT(DATE, '20130912'), N'1392-06-21', 5, 6, N'Thursday', N'پنج شنبه', 12, 21, 255, 176, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130913, CONVERT(DATE, '20130913'), N'1392-06-22', 6, 7, N'Friday', N'جمعه', 13, 22, 256, 177, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130914, CONVERT(DATE, '20130914'), N'1392-06-23', 7, 1, N'Saturday', N'شنبه', 14, 23, 257, 178, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130915, CONVERT(DATE, '20130915'), N'1392-06-24', 1, 2, N'Sunday', N'یک شنبه', 15, 24, 258, 179, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130916, CONVERT(DATE, '20130916'), N'1392-06-25', 2, 3, N'Monday', N'دو شنبه', 16, 25, 259, 180, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130917, CONVERT(DATE, '20130917'), N'1392-06-26', 3, 4, N'Tuesday', N'سه شنبه', 17, 26, 260, 181, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130918, CONVERT(DATE, '20130918'), N'1392-06-27', 4, 5, N'Wednesday', N'چهار شنبه', 18, 27, 261, 182, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130919, CONVERT(DATE, '20130919'), N'1392-06-28', 5, 6, N'Thursday', N'پنج شنبه', 19, 28, 262, 183, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130920, CONVERT(DATE, '20130920'), N'1392-06-29', 6, 7, N'Friday', N'جمعه', 20, 29, 263, 184, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130921, CONVERT(DATE, '20130921'), N'1392-06-30', 7, 1, N'Saturday', N'شنبه', 21, 30, 264, 185, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130922, CONVERT(DATE, '20130922'), N'1392-06-31', 1, 2, N'Sunday', N'یک شنبه', 22, 31, 265, 186, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2013, 1392, 2, 1),
            (20130923, CONVERT(DATE, '20130923'), N'1392-07-01', 2, 3, N'Monday', N'دو شنبه', 23, 1, 266, 187, 38, 27, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130924, CONVERT(DATE, '20130924'), N'1392-07-02', 3, 4, N'Tuesday', N'سه شنبه', 24, 2, 267, 188, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130925, CONVERT(DATE, '20130925'), N'1392-07-03', 4, 5, N'Wednesday', N'چهار شنبه', 25, 3, 268, 189, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130926, CONVERT(DATE, '20130926'), N'1392-07-04', 5, 6, N'Thursday', N'پنج شنبه', 26, 4, 269, 190, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130927, CONVERT(DATE, '20130927'), N'1392-07-05', 6, 7, N'Friday', N'جمعه', 27, 5, 270, 191, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130928, CONVERT(DATE, '20130928'), N'1392-07-06', 7, 1, N'Saturday', N'شنبه', 28, 6, 271, 192, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130929, CONVERT(DATE, '20130929'), N'1392-07-07', 1, 2, N'Sunday', N'یک شنبه', 29, 7, 272, 193, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20130930, CONVERT(DATE, '20130930'), N'1392-07-08', 2, 3, N'Monday', N'دو شنبه', 30, 8, 273, 194, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2013, 1392, 2, 2),
            (20131001, CONVERT(DATE, '20131001'), N'1392-07-09', 3, 4, N'Tuesday', N'سه شنبه', 1, 9, 274, 195, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131002, CONVERT(DATE, '20131002'), N'1392-07-10', 4, 5, N'Wednesday', N'چهار شنبه', 2, 10, 275, 196, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131003, CONVERT(DATE, '20131003'), N'1392-07-11', 5, 6, N'Thursday', N'پنج شنبه', 3, 11, 276, 197, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131004, CONVERT(DATE, '20131004'), N'1392-07-12', 6, 7, N'Friday', N'جمعه', 4, 12, 277, 198, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131005, CONVERT(DATE, '20131005'), N'1392-07-13', 7, 1, N'Saturday', N'شنبه', 5, 13, 278, 199, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131006, CONVERT(DATE, '20131006'), N'1392-07-14', 1, 2, N'Sunday', N'یک شنبه', 6, 14, 279, 200, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131007, CONVERT(DATE, '20131007'), N'1392-07-15', 2, 3, N'Monday', N'دو شنبه', 7, 15, 280, 201, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131008, CONVERT(DATE, '20131008'), N'1392-07-16', 3, 4, N'Tuesday', N'سه شنبه', 8, 16, 281, 202, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_date_sample', N'dim_date', @step_rows, N'Inserted rows from Dim_Date.txt chunk into temp table #dim_date_sample.';

        INSERT INTO #dim_date_sample (
            TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey, DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek, DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear, WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName, MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter, CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
        )
        VALUES
            (20131009, CONVERT(DATE, '20131009'), N'1392-07-17', 4, 5, N'Wednesday', N'چهار شنبه', 9, 17, 282, 203, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131010, CONVERT(DATE, '20131010'), N'1392-07-18', 5, 6, N'Thursday', N'پنج شنبه', 10, 18, 283, 204, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131011, CONVERT(DATE, '20131011'), N'1392-07-19', 6, 7, N'Friday', N'جمعه', 11, 19, 284, 205, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131012, CONVERT(DATE, '20131012'), N'1392-07-20', 7, 1, N'Saturday', N'شنبه', 12, 20, 285, 206, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131013, CONVERT(DATE, '20131013'), N'1392-07-21', 1, 2, N'Sunday', N'یک شنبه', 13, 21, 286, 207, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131014, CONVERT(DATE, '20131014'), N'1392-07-22', 2, 3, N'Monday', N'دو شنبه', 14, 22, 287, 208, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131015, CONVERT(DATE, '20131015'), N'1392-07-23', 3, 4, N'Tuesday', N'سه شنبه', 15, 23, 288, 209, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131016, CONVERT(DATE, '20131016'), N'1392-07-24', 4, 5, N'Wednesday', N'چهار شنبه', 16, 24, 289, 210, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131017, CONVERT(DATE, '20131017'), N'1392-07-25', 5, 6, N'Thursday', N'پنج شنبه', 17, 25, 290, 211, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131018, CONVERT(DATE, '20131018'), N'1392-07-26', 6, 7, N'Friday', N'جمعه', 18, 26, 291, 212, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131019, CONVERT(DATE, '20131019'), N'1392-07-27', 7, 1, N'Saturday', N'شنبه', 19, 27, 292, 213, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131020, CONVERT(DATE, '20131020'), N'1392-07-28', 1, 2, N'Sunday', N'یک شنبه', 20, 28, 293, 214, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131021, CONVERT(DATE, '20131021'), N'1392-07-29', 2, 3, N'Monday', N'دو شنبه', 21, 29, 294, 215, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131022, CONVERT(DATE, '20131022'), N'1392-07-30', 3, 4, N'Tuesday', N'سه شنبه', 22, 30, 295, 216, 43, 32, N'October', N'مهر', 10, 7, 4, 3, 2013, 1392, 2, 2),
            (20131023, CONVERT(DATE, '20131023'), N'1392-08-01', 4, 5, N'Wednesday', N'چهار شنبه', 23, 1, 296, 217, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131024, CONVERT(DATE, '20131024'), N'1392-08-02', 5, 6, N'Thursday', N'پنج شنبه', 24, 2, 297, 218, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131025, CONVERT(DATE, '20131025'), N'1392-08-03', 6, 7, N'Friday', N'جمعه', 25, 3, 298, 219, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131026, CONVERT(DATE, '20131026'), N'1392-08-04', 7, 1, N'Saturday', N'شنبه', 26, 4, 299, 220, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131027, CONVERT(DATE, '20131027'), N'1392-08-05', 1, 2, N'Sunday', N'یک شنبه', 27, 5, 300, 221, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131028, CONVERT(DATE, '20131028'), N'1392-08-06', 2, 3, N'Monday', N'دو شنبه', 28, 6, 301, 222, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131029, CONVERT(DATE, '20131029'), N'1392-08-07', 3, 4, N'Tuesday', N'سه شنبه', 29, 7, 302, 223, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131030, CONVERT(DATE, '20131030'), N'1392-08-08', 4, 5, N'Wednesday', N'چهار شنبه', 30, 8, 303, 224, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131031, CONVERT(DATE, '20131031'), N'1392-08-09', 5, 6, N'Thursday', N'پنج شنبه', 31, 9, 304, 225, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2013, 1392, 2, 2),
            (20131101, CONVERT(DATE, '20131101'), N'1392-08-10', 6, 7, N'Friday', N'جمعه', 1, 10, 305, 226, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131102, CONVERT(DATE, '20131102'), N'1392-08-11', 7, 1, N'Saturday', N'شنبه', 2, 11, 306, 227, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131103, CONVERT(DATE, '20131103'), N'1392-08-12', 1, 2, N'Sunday', N'یک شنبه', 3, 12, 307, 228, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131104, CONVERT(DATE, '20131104'), N'1392-08-13', 2, 3, N'Monday', N'دو شنبه', 4, 13, 308, 229, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131105, CONVERT(DATE, '20131105'), N'1392-08-14', 3, 4, N'Tuesday', N'سه شنبه', 5, 14, 309, 230, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131106, CONVERT(DATE, '20131106'), N'1392-08-15', 4, 5, N'Wednesday', N'چهار شنبه', 6, 15, 310, 231, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131107, CONVERT(DATE, '20131107'), N'1392-08-16', 5, 6, N'Thursday', N'پنج شنبه', 7, 16, 311, 232, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131108, CONVERT(DATE, '20131108'), N'1392-08-17', 6, 7, N'Friday', N'جمعه', 8, 17, 312, 233, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131109, CONVERT(DATE, '20131109'), N'1392-08-18', 7, 1, N'Saturday', N'شنبه', 9, 18, 313, 234, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131110, CONVERT(DATE, '20131110'), N'1392-08-19', 1, 2, N'Sunday', N'یک شنبه', 10, 19, 314, 235, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131111, CONVERT(DATE, '20131111'), N'1392-08-20', 2, 3, N'Monday', N'دو شنبه', 11, 20, 315, 236, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131112, CONVERT(DATE, '20131112'), N'1392-08-21', 3, 4, N'Tuesday', N'سه شنبه', 12, 21, 316, 237, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131113, CONVERT(DATE, '20131113'), N'1392-08-22', 4, 5, N'Wednesday', N'چهار شنبه', 13, 22, 317, 238, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131114, CONVERT(DATE, '20131114'), N'1392-08-23', 5, 6, N'Thursday', N'پنج شنبه', 14, 23, 318, 239, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131115, CONVERT(DATE, '20131115'), N'1392-08-24', 6, 7, N'Friday', N'جمعه', 15, 24, 319, 240, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131116, CONVERT(DATE, '20131116'), N'1392-08-25', 7, 1, N'Saturday', N'شنبه', 16, 25, 320, 241, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131117, CONVERT(DATE, '20131117'), N'1392-08-26', 1, 2, N'Sunday', N'یک شنبه', 17, 26, 321, 242, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131118, CONVERT(DATE, '20131118'), N'1392-08-27', 2, 3, N'Monday', N'دو شنبه', 18, 27, 322, 243, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131119, CONVERT(DATE, '20131119'), N'1392-08-28', 3, 4, N'Tuesday', N'سه شنبه', 19, 28, 323, 244, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131120, CONVERT(DATE, '20131120'), N'1392-08-29', 4, 5, N'Wednesday', N'چهار شنبه', 20, 29, 324, 245, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131121, CONVERT(DATE, '20131121'), N'1392-08-30', 5, 6, N'Thursday', N'پنج شنبه', 21, 30, 325, 246, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2013, 1392, 2, 2),
            (20131122, CONVERT(DATE, '20131122'), N'1392-09-01', 6, 7, N'Friday', N'جمعه', 22, 1, 326, 247, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131123, CONVERT(DATE, '20131123'), N'1392-09-02', 7, 1, N'Saturday', N'شنبه', 23, 2, 327, 248, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131124, CONVERT(DATE, '20131124'), N'1392-09-03', 1, 2, N'Sunday', N'یک شنبه', 24, 3, 328, 249, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131125, CONVERT(DATE, '20131125'), N'1392-09-04', 2, 3, N'Monday', N'دو شنبه', 25, 4, 329, 250, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131126, CONVERT(DATE, '20131126'), N'1392-09-05', 3, 4, N'Tuesday', N'سه شنبه', 26, 5, 330, 251, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131127, CONVERT(DATE, '20131127'), N'1392-09-06', 4, 5, N'Wednesday', N'چهار شنبه', 27, 6, 331, 252, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131128, CONVERT(DATE, '20131128'), N'1392-09-07', 5, 6, N'Thursday', N'پنج شنبه', 28, 7, 332, 253, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131129, CONVERT(DATE, '20131129'), N'1392-09-08', 6, 7, N'Friday', N'جمعه', 29, 8, 333, 254, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131130, CONVERT(DATE, '20131130'), N'1392-09-09', 7, 1, N'Saturday', N'شنبه', 30, 9, 334, 255, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2013, 1392, 2, 2),
            (20131201, CONVERT(DATE, '20131201'), N'1392-09-10', 1, 2, N'Sunday', N'یک شنبه', 1, 10, 335, 256, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131202, CONVERT(DATE, '20131202'), N'1392-09-11', 2, 3, N'Monday', N'دو شنبه', 2, 11, 336, 257, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131203, CONVERT(DATE, '20131203'), N'1392-09-12', 3, 4, N'Tuesday', N'سه شنبه', 3, 12, 337, 258, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131204, CONVERT(DATE, '20131204'), N'1392-09-13', 4, 5, N'Wednesday', N'چهار شنبه', 4, 13, 338, 259, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131205, CONVERT(DATE, '20131205'), N'1392-09-14', 5, 6, N'Thursday', N'پنج شنبه', 5, 14, 339, 260, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131206, CONVERT(DATE, '20131206'), N'1392-09-15', 6, 7, N'Friday', N'جمعه', 6, 15, 340, 261, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131207, CONVERT(DATE, '20131207'), N'1392-09-16', 7, 1, N'Saturday', N'شنبه', 7, 16, 341, 262, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131208, CONVERT(DATE, '20131208'), N'1392-09-17', 1, 2, N'Sunday', N'یک شنبه', 8, 17, 342, 263, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131209, CONVERT(DATE, '20131209'), N'1392-09-18', 2, 3, N'Monday', N'دو شنبه', 9, 18, 343, 264, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131210, CONVERT(DATE, '20131210'), N'1392-09-19', 3, 4, N'Tuesday', N'سه شنبه', 10, 19, 344, 265, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131211, CONVERT(DATE, '20131211'), N'1392-09-20', 4, 5, N'Wednesday', N'چهار شنبه', 11, 20, 345, 266, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131212, CONVERT(DATE, '20131212'), N'1392-09-21', 5, 6, N'Thursday', N'پنج شنبه', 12, 21, 346, 267, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131213, CONVERT(DATE, '20131213'), N'1392-09-22', 6, 7, N'Friday', N'جمعه', 13, 22, 347, 268, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131214, CONVERT(DATE, '20131214'), N'1392-09-23', 7, 1, N'Saturday', N'شنبه', 14, 23, 348, 269, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131215, CONVERT(DATE, '20131215'), N'1392-09-24', 1, 2, N'Sunday', N'یک شنبه', 15, 24, 349, 270, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131216, CONVERT(DATE, '20131216'), N'1392-09-25', 2, 3, N'Monday', N'دو شنبه', 16, 25, 350, 271, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131217, CONVERT(DATE, '20131217'), N'1392-09-26', 3, 4, N'Tuesday', N'سه شنبه', 17, 26, 351, 272, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131218, CONVERT(DATE, '20131218'), N'1392-09-27', 4, 5, N'Wednesday', N'چهار شنبه', 18, 27, 352, 273, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131219, CONVERT(DATE, '20131219'), N'1392-09-28', 5, 6, N'Thursday', N'پنج شنبه', 19, 28, 353, 274, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131220, CONVERT(DATE, '20131220'), N'1392-09-29', 6, 7, N'Friday', N'جمعه', 20, 29, 354, 275, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131221, CONVERT(DATE, '20131221'), N'1392-09-30', 7, 1, N'Saturday', N'شنبه', 21, 30, 355, 276, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2013, 1392, 2, 2),
            (20131222, CONVERT(DATE, '20131222'), N'1392-10-01', 1, 2, N'Sunday', N'یک شنبه', 22, 1, 356, 277, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131223, CONVERT(DATE, '20131223'), N'1392-10-02', 2, 3, N'Monday', N'دو شنبه', 23, 2, 357, 278, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131224, CONVERT(DATE, '20131224'), N'1392-10-03', 3, 4, N'Tuesday', N'سه شنبه', 24, 3, 358, 279, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131225, CONVERT(DATE, '20131225'), N'1392-10-04', 4, 5, N'Wednesday', N'چهار شنبه', 25, 4, 359, 280, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131226, CONVERT(DATE, '20131226'), N'1392-10-05', 5, 6, N'Thursday', N'پنج شنبه', 26, 5, 360, 281, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131227, CONVERT(DATE, '20131227'), N'1392-10-06', 6, 7, N'Friday', N'جمعه', 27, 6, 361, 282, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131228, CONVERT(DATE, '20131228'), N'1392-10-07', 7, 1, N'Saturday', N'شنبه', 28, 7, 362, 283, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131229, CONVERT(DATE, '20131229'), N'1392-10-08', 1, 2, N'Sunday', N'یک شنبه', 29, 8, 363, 284, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131230, CONVERT(DATE, '20131230'), N'1392-10-09', 2, 3, N'Monday', N'دو شنبه', 30, 9, 364, 285, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20131231, CONVERT(DATE, '20131231'), N'1392-10-10', 3, 4, N'Tuesday', N'سه شنبه', 31, 10, 365, 286, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2013, 1392, 2, 2),
            (20140101, CONVERT(DATE, '20140101'), N'1392-10-11', 4, 5, N'Wednesday', N'چهار شنبه', 1, 11, 1, 287, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140102, CONVERT(DATE, '20140102'), N'1392-10-12', 5, 6, N'Thursday', N'پنج شنبه', 2, 12, 2, 288, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140103, CONVERT(DATE, '20140103'), N'1392-10-13', 6, 7, N'Friday', N'جمعه', 3, 13, 3, 289, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140104, CONVERT(DATE, '20140104'), N'1392-10-14', 7, 1, N'Saturday', N'شنبه', 4, 14, 4, 290, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140105, CONVERT(DATE, '20140105'), N'1392-10-15', 1, 2, N'Sunday', N'یک شنبه', 5, 15, 5, 291, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140106, CONVERT(DATE, '20140106'), N'1392-10-16', 2, 3, N'Monday', N'دو شنبه', 6, 16, 6, 292, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140107, CONVERT(DATE, '20140107'), N'1392-10-17', 3, 4, N'Tuesday', N'سه شنبه', 7, 17, 7, 293, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140108, CONVERT(DATE, '20140108'), N'1392-10-18', 4, 5, N'Wednesday', N'چهار شنبه', 8, 18, 8, 294, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140109, CONVERT(DATE, '20140109'), N'1392-10-19', 5, 6, N'Thursday', N'پنج شنبه', 9, 19, 9, 295, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140110, CONVERT(DATE, '20140110'), N'1392-10-20', 6, 7, N'Friday', N'جمعه', 10, 20, 10, 296, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140111, CONVERT(DATE, '20140111'), N'1392-10-21', 7, 1, N'Saturday', N'شنبه', 11, 21, 11, 297, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140112, CONVERT(DATE, '20140112'), N'1392-10-22', 1, 2, N'Sunday', N'یک شنبه', 12, 22, 12, 298, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140113, CONVERT(DATE, '20140113'), N'1392-10-23', 2, 3, N'Monday', N'دو شنبه', 13, 23, 13, 299, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140114, CONVERT(DATE, '20140114'), N'1392-10-24', 3, 4, N'Tuesday', N'سه شنبه', 14, 24, 14, 300, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140115, CONVERT(DATE, '20140115'), N'1392-10-25', 4, 5, N'Wednesday', N'چهار شنبه', 15, 25, 15, 301, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140116, CONVERT(DATE, '20140116'), N'1392-10-26', 5, 6, N'Thursday', N'پنج شنبه', 16, 26, 16, 302, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140117, CONVERT(DATE, '20140117'), N'1392-10-27', 6, 7, N'Friday', N'جمعه', 17, 27, 17, 303, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140118, CONVERT(DATE, '20140118'), N'1392-10-28', 7, 1, N'Saturday', N'شنبه', 18, 28, 18, 304, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140119, CONVERT(DATE, '20140119'), N'1392-10-29', 1, 2, N'Sunday', N'یک شنبه', 19, 29, 19, 305, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140120, CONVERT(DATE, '20140120'), N'1392-10-30', 2, 3, N'Monday', N'دو شنبه', 20, 30, 20, 306, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2014, 1392, 1, 2),
            (20140121, CONVERT(DATE, '20140121'), N'1392-11-01', 3, 4, N'Tuesday', N'سه شنبه', 21, 1, 21, 307, 3, 44, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140122, CONVERT(DATE, '20140122'), N'1392-11-02', 4, 5, N'Wednesday', N'چهار شنبه', 22, 2, 22, 308, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140123, CONVERT(DATE, '20140123'), N'1392-11-03', 5, 6, N'Thursday', N'پنج شنبه', 23, 3, 23, 309, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140124, CONVERT(DATE, '20140124'), N'1392-11-04', 6, 7, N'Friday', N'جمعه', 24, 4, 24, 310, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140125, CONVERT(DATE, '20140125'), N'1392-11-05', 7, 1, N'Saturday', N'شنبه', 25, 5, 25, 311, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140126, CONVERT(DATE, '20140126'), N'1392-11-06', 1, 2, N'Sunday', N'یک شنبه', 26, 6, 26, 312, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140127, CONVERT(DATE, '20140127'), N'1392-11-07', 2, 3, N'Monday', N'دو شنبه', 27, 7, 27, 313, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140128, CONVERT(DATE, '20140128'), N'1392-11-08', 3, 4, N'Tuesday', N'سه شنبه', 28, 8, 28, 314, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140129, CONVERT(DATE, '20140129'), N'1392-11-09', 4, 5, N'Wednesday', N'چهار شنبه', 29, 9, 29, 315, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140130, CONVERT(DATE, '20140130'), N'1392-11-10', 5, 6, N'Thursday', N'پنج شنبه', 30, 10, 30, 316, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140131, CONVERT(DATE, '20140131'), N'1392-11-11', 6, 7, N'Friday', N'جمعه', 31, 11, 31, 317, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2014, 1392, 1, 2),
            (20140201, CONVERT(DATE, '20140201'), N'1392-11-12', 7, 1, N'Saturday', N'شنبه', 1, 12, 32, 318, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140202, CONVERT(DATE, '20140202'), N'1392-11-13', 1, 2, N'Sunday', N'یک شنبه', 2, 13, 33, 319, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140203, CONVERT(DATE, '20140203'), N'1392-11-14', 2, 3, N'Monday', N'دو شنبه', 3, 14, 34, 320, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140204, CONVERT(DATE, '20140204'), N'1392-11-15', 3, 4, N'Tuesday', N'سه شنبه', 4, 15, 35, 321, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140205, CONVERT(DATE, '20140205'), N'1392-11-16', 4, 5, N'Wednesday', N'چهار شنبه', 5, 16, 36, 322, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140206, CONVERT(DATE, '20140206'), N'1392-11-17', 5, 6, N'Thursday', N'پنج شنبه', 6, 17, 37, 323, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140207, CONVERT(DATE, '20140207'), N'1392-11-18', 6, 7, N'Friday', N'جمعه', 7, 18, 38, 324, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140208, CONVERT(DATE, '20140208'), N'1392-11-19', 7, 1, N'Saturday', N'شنبه', 8, 19, 39, 325, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140209, CONVERT(DATE, '20140209'), N'1392-11-20', 1, 2, N'Sunday', N'یک شنبه', 9, 20, 40, 326, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140210, CONVERT(DATE, '20140210'), N'1392-11-21', 2, 3, N'Monday', N'دو شنبه', 10, 21, 41, 327, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140211, CONVERT(DATE, '20140211'), N'1392-11-22', 3, 4, N'Tuesday', N'سه شنبه', 11, 22, 42, 328, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140212, CONVERT(DATE, '20140212'), N'1392-11-23', 4, 5, N'Wednesday', N'چهار شنبه', 12, 23, 43, 329, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140213, CONVERT(DATE, '20140213'), N'1392-11-24', 5, 6, N'Thursday', N'پنج شنبه', 13, 24, 44, 330, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140214, CONVERT(DATE, '20140214'), N'1392-11-25', 6, 7, N'Friday', N'جمعه', 14, 25, 45, 331, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140215, CONVERT(DATE, '20140215'), N'1392-11-26', 7, 1, N'Saturday', N'شنبه', 15, 26, 46, 332, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140216, CONVERT(DATE, '20140216'), N'1392-11-27', 1, 2, N'Sunday', N'یک شنبه', 16, 27, 47, 333, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140217, CONVERT(DATE, '20140217'), N'1392-11-28', 2, 3, N'Monday', N'دو شنبه', 17, 28, 48, 334, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140218, CONVERT(DATE, '20140218'), N'1392-11-29', 3, 4, N'Tuesday', N'سه شنبه', 18, 29, 49, 335, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140219, CONVERT(DATE, '20140219'), N'1392-11-30', 4, 5, N'Wednesday', N'چهار شنبه', 19, 30, 50, 336, 8, 49, N'February', N'بهمن', 2, 11, 1, 4, 2014, 1392, 1, 2),
            (20140220, CONVERT(DATE, '20140220'), N'1392-12-01', 5, 6, N'Thursday', N'پنج شنبه', 20, 1, 51, 337, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140221, CONVERT(DATE, '20140221'), N'1392-12-02', 6, 7, N'Friday', N'جمعه', 21, 2, 52, 338, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140222, CONVERT(DATE, '20140222'), N'1392-12-03', 7, 1, N'Saturday', N'شنبه', 22, 3, 53, 339, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140223, CONVERT(DATE, '20140223'), N'1392-12-04', 1, 2, N'Sunday', N'یک شنبه', 23, 4, 54, 340, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140224, CONVERT(DATE, '20140224'), N'1392-12-05', 2, 3, N'Monday', N'دو شنبه', 24, 5, 55, 341, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140225, CONVERT(DATE, '20140225'), N'1392-12-06', 3, 4, N'Tuesday', N'سه شنبه', 25, 6, 56, 342, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140226, CONVERT(DATE, '20140226'), N'1392-12-07', 4, 5, N'Wednesday', N'چهار شنبه', 26, 7, 57, 343, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140227, CONVERT(DATE, '20140227'), N'1392-12-08', 5, 6, N'Thursday', N'پنج شنبه', 27, 8, 58, 344, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140228, CONVERT(DATE, '20140228'), N'1392-12-09', 6, 7, N'Friday', N'جمعه', 28, 9, 59, 345, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2014, 1392, 1, 2),
            (20140301, CONVERT(DATE, '20140301'), N'1392-12-10', 7, 1, N'Saturday', N'شنبه', 1, 10, 60, 346, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140302, CONVERT(DATE, '20140302'), N'1392-12-11', 1, 2, N'Sunday', N'یک شنبه', 2, 11, 61, 347, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140303, CONVERT(DATE, '20140303'), N'1392-12-12', 2, 3, N'Monday', N'دو شنبه', 3, 12, 62, 348, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140304, CONVERT(DATE, '20140304'), N'1392-12-13', 3, 4, N'Tuesday', N'سه شنبه', 4, 13, 63, 349, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140305, CONVERT(DATE, '20140305'), N'1392-12-14', 4, 5, N'Wednesday', N'چهار شنبه', 5, 14, 64, 350, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140306, CONVERT(DATE, '20140306'), N'1392-12-15', 5, 6, N'Thursday', N'پنج شنبه', 6, 15, 65, 351, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140307, CONVERT(DATE, '20140307'), N'1392-12-16', 6, 7, N'Friday', N'جمعه', 7, 16, 66, 352, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140308, CONVERT(DATE, '20140308'), N'1392-12-17', 7, 1, N'Saturday', N'شنبه', 8, 17, 67, 353, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140309, CONVERT(DATE, '20140309'), N'1392-12-18', 1, 2, N'Sunday', N'یک شنبه', 9, 18, 68, 354, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140310, CONVERT(DATE, '20140310'), N'1392-12-19', 2, 3, N'Monday', N'دو شنبه', 10, 19, 69, 355, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140311, CONVERT(DATE, '20140311'), N'1392-12-20', 3, 4, N'Tuesday', N'سه شنبه', 11, 20, 70, 356, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140312, CONVERT(DATE, '20140312'), N'1392-12-21', 4, 5, N'Wednesday', N'چهار شنبه', 12, 21, 71, 357, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140313, CONVERT(DATE, '20140313'), N'1392-12-22', 5, 6, N'Thursday', N'پنج شنبه', 13, 22, 72, 358, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140314, CONVERT(DATE, '20140314'), N'1392-12-23', 6, 7, N'Friday', N'جمعه', 14, 23, 73, 359, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140315, CONVERT(DATE, '20140315'), N'1392-12-24', 7, 1, N'Saturday', N'شنبه', 15, 24, 74, 360, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140316, CONVERT(DATE, '20140316'), N'1392-12-25', 1, 2, N'Sunday', N'یک شنبه', 16, 25, 75, 361, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140317, CONVERT(DATE, '20140317'), N'1392-12-26', 2, 3, N'Monday', N'دو شنبه', 17, 26, 76, 362, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140318, CONVERT(DATE, '20140318'), N'1392-12-27', 3, 4, N'Tuesday', N'سه شنبه', 18, 27, 77, 363, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140319, CONVERT(DATE, '20140319'), N'1392-12-28', 4, 5, N'Wednesday', N'چهار شنبه', 19, 28, 78, 364, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140320, CONVERT(DATE, '20140320'), N'1392-12-29', 5, 6, N'Thursday', N'پنج شنبه', 20, 29, 79, 365, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2014, 1392, 1, 2),
            (20140321, CONVERT(DATE, '20140321'), N'1393-01-01', 6, 7, N'Friday', N'جمعه', 21, 1, 80, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140322, CONVERT(DATE, '20140322'), N'1393-01-02', 7, 1, N'Saturday', N'شنبه', 22, 2, 81, 2, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140323, CONVERT(DATE, '20140323'), N'1393-01-03', 1, 2, N'Sunday', N'یک شنبه', 23, 3, 82, 3, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140324, CONVERT(DATE, '20140324'), N'1393-01-04', 2, 3, N'Monday', N'دو شنبه', 24, 4, 83, 4, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140325, CONVERT(DATE, '20140325'), N'1393-01-05', 3, 4, N'Tuesday', N'سه شنبه', 25, 5, 84, 5, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140326, CONVERT(DATE, '20140326'), N'1393-01-06', 4, 5, N'Wednesday', N'چهار شنبه', 26, 6, 85, 6, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140327, CONVERT(DATE, '20140327'), N'1393-01-07', 5, 6, N'Thursday', N'پنج شنبه', 27, 7, 86, 7, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140328, CONVERT(DATE, '20140328'), N'1393-01-08', 6, 7, N'Friday', N'جمعه', 28, 8, 87, 8, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140329, CONVERT(DATE, '20140329'), N'1393-01-09', 7, 1, N'Saturday', N'شنبه', 29, 9, 88, 9, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140330, CONVERT(DATE, '20140330'), N'1393-01-10', 1, 2, N'Sunday', N'یک شنبه', 30, 10, 89, 10, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140331, CONVERT(DATE, '20140331'), N'1393-01-11', 2, 3, N'Monday', N'دو شنبه', 31, 11, 90, 11, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2014, 1393, 1, 1),
            (20140401, CONVERT(DATE, '20140401'), N'1393-01-12', 3, 4, N'Tuesday', N'سه شنبه', 1, 12, 91, 12, 13, 2, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140402, CONVERT(DATE, '20140402'), N'1393-01-13', 4, 5, N'Wednesday', N'چهار شنبه', 2, 13, 92, 13, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140403, CONVERT(DATE, '20140403'), N'1393-01-14', 5, 6, N'Thursday', N'پنج شنبه', 3, 14, 93, 14, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140404, CONVERT(DATE, '20140404'), N'1393-01-15', 6, 7, N'Friday', N'جمعه', 4, 15, 94, 15, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140405, CONVERT(DATE, '20140405'), N'1393-01-16', 7, 1, N'Saturday', N'شنبه', 5, 16, 95, 16, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140406, CONVERT(DATE, '20140406'), N'1393-01-17', 1, 2, N'Sunday', N'یک شنبه', 6, 17, 96, 17, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140407, CONVERT(DATE, '20140407'), N'1393-01-18', 2, 3, N'Monday', N'دو شنبه', 7, 18, 97, 18, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140408, CONVERT(DATE, '20140408'), N'1393-01-19', 3, 4, N'Tuesday', N'سه شنبه', 8, 19, 98, 19, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140409, CONVERT(DATE, '20140409'), N'1393-01-20', 4, 5, N'Wednesday', N'چهار شنبه', 9, 20, 99, 20, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140410, CONVERT(DATE, '20140410'), N'1393-01-21', 5, 6, N'Thursday', N'پنج شنبه', 10, 21, 100, 21, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140411, CONVERT(DATE, '20140411'), N'1393-01-22', 6, 7, N'Friday', N'جمعه', 11, 22, 101, 22, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140412, CONVERT(DATE, '20140412'), N'1393-01-23', 7, 1, N'Saturday', N'شنبه', 12, 23, 102, 23, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140413, CONVERT(DATE, '20140413'), N'1393-01-24', 1, 2, N'Sunday', N'یک شنبه', 13, 24, 103, 24, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140414, CONVERT(DATE, '20140414'), N'1393-01-25', 2, 3, N'Monday', N'دو شنبه', 14, 25, 104, 25, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140415, CONVERT(DATE, '20140415'), N'1393-01-26', 3, 4, N'Tuesday', N'سه شنبه', 15, 26, 105, 26, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140416, CONVERT(DATE, '20140416'), N'1393-01-27', 4, 5, N'Wednesday', N'چهار شنبه', 16, 27, 106, 27, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140417, CONVERT(DATE, '20140417'), N'1393-01-28', 5, 6, N'Thursday', N'پنج شنبه', 17, 28, 107, 28, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140418, CONVERT(DATE, '20140418'), N'1393-01-29', 6, 7, N'Friday', N'جمعه', 18, 29, 108, 29, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140419, CONVERT(DATE, '20140419'), N'1393-01-30', 7, 1, N'Saturday', N'شنبه', 19, 30, 109, 30, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140420, CONVERT(DATE, '20140420'), N'1393-01-31', 1, 2, N'Sunday', N'یک شنبه', 20, 31, 110, 31, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2014, 1393, 1, 1),
            (20140421, CONVERT(DATE, '20140421'), N'1393-02-01', 2, 3, N'Monday', N'دو شنبه', 21, 1, 111, 32, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140422, CONVERT(DATE, '20140422'), N'1393-02-02', 3, 4, N'Tuesday', N'سه شنبه', 22, 2, 112, 33, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140423, CONVERT(DATE, '20140423'), N'1393-02-03', 4, 5, N'Wednesday', N'چهار شنبه', 23, 3, 113, 34, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140424, CONVERT(DATE, '20140424'), N'1393-02-04', 5, 6, N'Thursday', N'پنج شنبه', 24, 4, 114, 35, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140425, CONVERT(DATE, '20140425'), N'1393-02-05', 6, 7, N'Friday', N'جمعه', 25, 5, 115, 36, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140426, CONVERT(DATE, '20140426'), N'1393-02-06', 7, 1, N'Saturday', N'شنبه', 26, 6, 116, 37, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140427, CONVERT(DATE, '20140427'), N'1393-02-07', 1, 2, N'Sunday', N'یک شنبه', 27, 7, 117, 38, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140428, CONVERT(DATE, '20140428'), N'1393-02-08', 2, 3, N'Monday', N'دو شنبه', 28, 8, 118, 39, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140429, CONVERT(DATE, '20140429'), N'1393-02-09', 3, 4, N'Tuesday', N'سه شنبه', 29, 9, 119, 40, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140430, CONVERT(DATE, '20140430'), N'1393-02-10', 4, 5, N'Wednesday', N'چهار شنبه', 30, 10, 120, 41, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2014, 1393, 1, 1),
            (20140501, CONVERT(DATE, '20140501'), N'1393-02-11', 5, 6, N'Thursday', N'پنج شنبه', 1, 11, 121, 42, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140502, CONVERT(DATE, '20140502'), N'1393-02-12', 6, 7, N'Friday', N'جمعه', 2, 12, 122, 43, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140503, CONVERT(DATE, '20140503'), N'1393-02-13', 7, 1, N'Saturday', N'شنبه', 3, 13, 123, 44, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140504, CONVERT(DATE, '20140504'), N'1393-02-14', 1, 2, N'Sunday', N'یک شنبه', 4, 14, 124, 45, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140505, CONVERT(DATE, '20140505'), N'1393-02-15', 2, 3, N'Monday', N'دو شنبه', 5, 15, 125, 46, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140506, CONVERT(DATE, '20140506'), N'1393-02-16', 3, 4, N'Tuesday', N'سه شنبه', 6, 16, 126, 47, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140507, CONVERT(DATE, '20140507'), N'1393-02-17', 4, 5, N'Wednesday', N'چهار شنبه', 7, 17, 127, 48, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140508, CONVERT(DATE, '20140508'), N'1393-02-18', 5, 6, N'Thursday', N'پنج شنبه', 8, 18, 128, 49, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140509, CONVERT(DATE, '20140509'), N'1393-02-19', 6, 7, N'Friday', N'جمعه', 9, 19, 129, 50, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140510, CONVERT(DATE, '20140510'), N'1393-02-20', 7, 1, N'Saturday', N'شنبه', 10, 20, 130, 51, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140511, CONVERT(DATE, '20140511'), N'1393-02-21', 1, 2, N'Sunday', N'یک شنبه', 11, 21, 131, 52, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140512, CONVERT(DATE, '20140512'), N'1393-02-22', 2, 3, N'Monday', N'دو شنبه', 12, 22, 132, 53, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140513, CONVERT(DATE, '20140513'), N'1393-02-23', 3, 4, N'Tuesday', N'سه شنبه', 13, 23, 133, 54, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140514, CONVERT(DATE, '20140514'), N'1393-02-24', 4, 5, N'Wednesday', N'چهار شنبه', 14, 24, 134, 55, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140515, CONVERT(DATE, '20140515'), N'1393-02-25', 5, 6, N'Thursday', N'پنج شنبه', 15, 25, 135, 56, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140516, CONVERT(DATE, '20140516'), N'1393-02-26', 6, 7, N'Friday', N'جمعه', 16, 26, 136, 57, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140517, CONVERT(DATE, '20140517'), N'1393-02-27', 7, 1, N'Saturday', N'شنبه', 17, 27, 137, 58, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140518, CONVERT(DATE, '20140518'), N'1393-02-28', 1, 2, N'Sunday', N'یک شنبه', 18, 28, 138, 59, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140519, CONVERT(DATE, '20140519'), N'1393-02-29', 2, 3, N'Monday', N'دو شنبه', 19, 29, 139, 60, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140520, CONVERT(DATE, '20140520'), N'1393-02-30', 3, 4, N'Tuesday', N'سه شنبه', 20, 30, 140, 61, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140521, CONVERT(DATE, '20140521'), N'1393-02-31', 4, 5, N'Wednesday', N'چهار شنبه', 21, 31, 141, 62, 21, 10, N'May', N'اردیبهشت', 5, 2, 2, 1, 2014, 1393, 1, 1),
            (20140522, CONVERT(DATE, '20140522'), N'1393-03-01', 5, 6, N'Thursday', N'پنج شنبه', 22, 1, 142, 63, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140523, CONVERT(DATE, '20140523'), N'1393-03-02', 6, 7, N'Friday', N'جمعه', 23, 2, 143, 64, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140524, CONVERT(DATE, '20140524'), N'1393-03-03', 7, 1, N'Saturday', N'شنبه', 24, 3, 144, 65, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140525, CONVERT(DATE, '20140525'), N'1393-03-04', 1, 2, N'Sunday', N'یک شنبه', 25, 4, 145, 66, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140526, CONVERT(DATE, '20140526'), N'1393-03-05', 2, 3, N'Monday', N'دو شنبه', 26, 5, 146, 67, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140527, CONVERT(DATE, '20140527'), N'1393-03-06', 3, 4, N'Tuesday', N'سه شنبه', 27, 6, 147, 68, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140528, CONVERT(DATE, '20140528'), N'1393-03-07', 4, 5, N'Wednesday', N'چهار شنبه', 28, 7, 148, 69, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140529, CONVERT(DATE, '20140529'), N'1393-03-08', 5, 6, N'Thursday', N'پنج شنبه', 29, 8, 149, 70, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140530, CONVERT(DATE, '20140530'), N'1393-03-09', 6, 7, N'Friday', N'جمعه', 30, 9, 150, 71, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140531, CONVERT(DATE, '20140531'), N'1393-03-10', 7, 1, N'Saturday', N'شنبه', 31, 10, 151, 72, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2014, 1393, 1, 1),
            (20140601, CONVERT(DATE, '20140601'), N'1393-03-11', 1, 2, N'Sunday', N'یک شنبه', 1, 11, 152, 73, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140602, CONVERT(DATE, '20140602'), N'1393-03-12', 2, 3, N'Monday', N'دو شنبه', 2, 12, 153, 74, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140603, CONVERT(DATE, '20140603'), N'1393-03-13', 3, 4, N'Tuesday', N'سه شنبه', 3, 13, 154, 75, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140604, CONVERT(DATE, '20140604'), N'1393-03-14', 4, 5, N'Wednesday', N'چهار شنبه', 4, 14, 155, 76, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140605, CONVERT(DATE, '20140605'), N'1393-03-15', 5, 6, N'Thursday', N'پنج شنبه', 5, 15, 156, 77, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140606, CONVERT(DATE, '20140606'), N'1393-03-16', 6, 7, N'Friday', N'جمعه', 6, 16, 157, 78, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140607, CONVERT(DATE, '20140607'), N'1393-03-17', 7, 1, N'Saturday', N'شنبه', 7, 17, 158, 79, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140608, CONVERT(DATE, '20140608'), N'1393-03-18', 1, 2, N'Sunday', N'یک شنبه', 8, 18, 159, 80, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140609, CONVERT(DATE, '20140609'), N'1393-03-19', 2, 3, N'Monday', N'دو شنبه', 9, 19, 160, 81, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140610, CONVERT(DATE, '20140610'), N'1393-03-20', 3, 4, N'Tuesday', N'سه شنبه', 10, 20, 161, 82, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140611, CONVERT(DATE, '20140611'), N'1393-03-21', 4, 5, N'Wednesday', N'چهار شنبه', 11, 21, 162, 83, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140612, CONVERT(DATE, '20140612'), N'1393-03-22', 5, 6, N'Thursday', N'پنج شنبه', 12, 22, 163, 84, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140613, CONVERT(DATE, '20140613'), N'1393-03-23', 6, 7, N'Friday', N'جمعه', 13, 23, 164, 85, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140614, CONVERT(DATE, '20140614'), N'1393-03-24', 7, 1, N'Saturday', N'شنبه', 14, 24, 165, 86, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140615, CONVERT(DATE, '20140615'), N'1393-03-25', 1, 2, N'Sunday', N'یک شنبه', 15, 25, 166, 87, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140616, CONVERT(DATE, '20140616'), N'1393-03-26', 2, 3, N'Monday', N'دو شنبه', 16, 26, 167, 88, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140617, CONVERT(DATE, '20140617'), N'1393-03-27', 3, 4, N'Tuesday', N'سه شنبه', 17, 27, 168, 89, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140618, CONVERT(DATE, '20140618'), N'1393-03-28', 4, 5, N'Wednesday', N'چهار شنبه', 18, 28, 169, 90, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140619, CONVERT(DATE, '20140619'), N'1393-03-29', 5, 6, N'Thursday', N'پنج شنبه', 19, 29, 170, 91, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140620, CONVERT(DATE, '20140620'), N'1393-03-30', 6, 7, N'Friday', N'جمعه', 20, 30, 171, 92, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140621, CONVERT(DATE, '20140621'), N'1393-03-31', 7, 1, N'Saturday', N'شنبه', 21, 31, 172, 93, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2014, 1393, 1, 1),
            (20140622, CONVERT(DATE, '20140622'), N'1393-04-01', 1, 2, N'Sunday', N'یک شنبه', 22, 1, 173, 94, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140623, CONVERT(DATE, '20140623'), N'1393-04-02', 2, 3, N'Monday', N'دو شنبه', 23, 2, 174, 95, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140624, CONVERT(DATE, '20140624'), N'1393-04-03', 3, 4, N'Tuesday', N'سه شنبه', 24, 3, 175, 96, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140625, CONVERT(DATE, '20140625'), N'1393-04-04', 4, 5, N'Wednesday', N'چهار شنبه', 25, 4, 176, 97, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140626, CONVERT(DATE, '20140626'), N'1393-04-05', 5, 6, N'Thursday', N'پنج شنبه', 26, 5, 177, 98, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140627, CONVERT(DATE, '20140627'), N'1393-04-06', 6, 7, N'Friday', N'جمعه', 27, 6, 178, 99, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140628, CONVERT(DATE, '20140628'), N'1393-04-07', 7, 1, N'Saturday', N'شنبه', 28, 7, 179, 100, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140629, CONVERT(DATE, '20140629'), N'1393-04-08', 1, 2, N'Sunday', N'یک شنبه', 29, 8, 180, 101, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140630, CONVERT(DATE, '20140630'), N'1393-04-09', 2, 3, N'Monday', N'دو شنبه', 30, 9, 181, 102, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2014, 1393, 1, 1),
            (20140701, CONVERT(DATE, '20140701'), N'1393-04-10', 3, 4, N'Tuesday', N'سه شنبه', 1, 10, 182, 103, 26, 15, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140702, CONVERT(DATE, '20140702'), N'1393-04-11', 4, 5, N'Wednesday', N'چهار شنبه', 2, 11, 183, 104, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140703, CONVERT(DATE, '20140703'), N'1393-04-12', 5, 6, N'Thursday', N'پنج شنبه', 3, 12, 184, 105, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140704, CONVERT(DATE, '20140704'), N'1393-04-13', 6, 7, N'Friday', N'جمعه', 4, 13, 185, 106, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140705, CONVERT(DATE, '20140705'), N'1393-04-14', 7, 1, N'Saturday', N'شنبه', 5, 14, 186, 107, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140706, CONVERT(DATE, '20140706'), N'1393-04-15', 1, 2, N'Sunday', N'یک شنبه', 6, 15, 187, 108, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140707, CONVERT(DATE, '20140707'), N'1393-04-16', 2, 3, N'Monday', N'دو شنبه', 7, 16, 188, 109, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140708, CONVERT(DATE, '20140708'), N'1393-04-17', 3, 4, N'Tuesday', N'سه شنبه', 8, 17, 189, 110, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140709, CONVERT(DATE, '20140709'), N'1393-04-18', 4, 5, N'Wednesday', N'چهار شنبه', 9, 18, 190, 111, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140710, CONVERT(DATE, '20140710'), N'1393-04-19', 5, 6, N'Thursday', N'پنج شنبه', 10, 19, 191, 112, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140711, CONVERT(DATE, '20140711'), N'1393-04-20', 6, 7, N'Friday', N'جمعه', 11, 20, 192, 113, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140712, CONVERT(DATE, '20140712'), N'1393-04-21', 7, 1, N'Saturday', N'شنبه', 12, 21, 193, 114, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140713, CONVERT(DATE, '20140713'), N'1393-04-22', 1, 2, N'Sunday', N'یک شنبه', 13, 22, 194, 115, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140714, CONVERT(DATE, '20140714'), N'1393-04-23', 2, 3, N'Monday', N'دو شنبه', 14, 23, 195, 116, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140715, CONVERT(DATE, '20140715'), N'1393-04-24', 3, 4, N'Tuesday', N'سه شنبه', 15, 24, 196, 117, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140716, CONVERT(DATE, '20140716'), N'1393-04-25', 4, 5, N'Wednesday', N'چهار شنبه', 16, 25, 197, 118, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140717, CONVERT(DATE, '20140717'), N'1393-04-26', 5, 6, N'Thursday', N'پنج شنبه', 17, 26, 198, 119, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140718, CONVERT(DATE, '20140718'), N'1393-04-27', 6, 7, N'Friday', N'جمعه', 18, 27, 199, 120, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140719, CONVERT(DATE, '20140719'), N'1393-04-28', 7, 1, N'Saturday', N'شنبه', 19, 28, 200, 121, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140720, CONVERT(DATE, '20140720'), N'1393-04-29', 1, 2, N'Sunday', N'یک شنبه', 20, 29, 201, 122, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140721, CONVERT(DATE, '20140721'), N'1393-04-30', 2, 3, N'Monday', N'دو شنبه', 21, 30, 202, 123, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140722, CONVERT(DATE, '20140722'), N'1393-04-31', 3, 4, N'Tuesday', N'سه شنبه', 22, 31, 203, 124, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2014, 1393, 2, 1),
            (20140723, CONVERT(DATE, '20140723'), N'1393-05-01', 4, 5, N'Wednesday', N'چهار شنبه', 23, 1, 204, 125, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140724, CONVERT(DATE, '20140724'), N'1393-05-02', 5, 6, N'Thursday', N'پنج شنبه', 24, 2, 205, 126, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140725, CONVERT(DATE, '20140725'), N'1393-05-03', 6, 7, N'Friday', N'جمعه', 25, 3, 206, 127, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140726, CONVERT(DATE, '20140726'), N'1393-05-04', 7, 1, N'Saturday', N'شنبه', 26, 4, 207, 128, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140727, CONVERT(DATE, '20140727'), N'1393-05-05', 1, 2, N'Sunday', N'یک شنبه', 27, 5, 208, 129, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140728, CONVERT(DATE, '20140728'), N'1393-05-06', 2, 3, N'Monday', N'دو شنبه', 28, 6, 209, 130, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140729, CONVERT(DATE, '20140729'), N'1393-05-07', 3, 4, N'Tuesday', N'سه شنبه', 29, 7, 210, 131, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140730, CONVERT(DATE, '20140730'), N'1393-05-08', 4, 5, N'Wednesday', N'چهار شنبه', 30, 8, 211, 132, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140731, CONVERT(DATE, '20140731'), N'1393-05-09', 5, 6, N'Thursday', N'پنج شنبه', 31, 9, 212, 133, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2014, 1393, 2, 1),
            (20140801, CONVERT(DATE, '20140801'), N'1393-05-10', 6, 7, N'Friday', N'جمعه', 1, 10, 213, 134, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140802, CONVERT(DATE, '20140802'), N'1393-05-11', 7, 1, N'Saturday', N'شنبه', 2, 11, 214, 135, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140803, CONVERT(DATE, '20140803'), N'1393-05-12', 1, 2, N'Sunday', N'یک شنبه', 3, 12, 215, 136, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140804, CONVERT(DATE, '20140804'), N'1393-05-13', 2, 3, N'Monday', N'دو شنبه', 4, 13, 216, 137, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140805, CONVERT(DATE, '20140805'), N'1393-05-14', 3, 4, N'Tuesday', N'سه شنبه', 5, 14, 217, 138, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140806, CONVERT(DATE, '20140806'), N'1393-05-15', 4, 5, N'Wednesday', N'چهار شنبه', 6, 15, 218, 139, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140807, CONVERT(DATE, '20140807'), N'1393-05-16', 5, 6, N'Thursday', N'پنج شنبه', 7, 16, 219, 140, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140808, CONVERT(DATE, '20140808'), N'1393-05-17', 6, 7, N'Friday', N'جمعه', 8, 17, 220, 141, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140809, CONVERT(DATE, '20140809'), N'1393-05-18', 7, 1, N'Saturday', N'شنبه', 9, 18, 221, 142, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140810, CONVERT(DATE, '20140810'), N'1393-05-19', 1, 2, N'Sunday', N'یک شنبه', 10, 19, 222, 143, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140811, CONVERT(DATE, '20140811'), N'1393-05-20', 2, 3, N'Monday', N'دو شنبه', 11, 20, 223, 144, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140812, CONVERT(DATE, '20140812'), N'1393-05-21', 3, 4, N'Tuesday', N'سه شنبه', 12, 21, 224, 145, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140813, CONVERT(DATE, '20140813'), N'1393-05-22', 4, 5, N'Wednesday', N'چهار شنبه', 13, 22, 225, 146, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140814, CONVERT(DATE, '20140814'), N'1393-05-23', 5, 6, N'Thursday', N'پنج شنبه', 14, 23, 226, 147, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140815, CONVERT(DATE, '20140815'), N'1393-05-24', 6, 7, N'Friday', N'جمعه', 15, 24, 227, 148, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140816, CONVERT(DATE, '20140816'), N'1393-05-25', 7, 1, N'Saturday', N'شنبه', 16, 25, 228, 149, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140817, CONVERT(DATE, '20140817'), N'1393-05-26', 1, 2, N'Sunday', N'یک شنبه', 17, 26, 229, 150, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140818, CONVERT(DATE, '20140818'), N'1393-05-27', 2, 3, N'Monday', N'دو شنبه', 18, 27, 230, 151, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140819, CONVERT(DATE, '20140819'), N'1393-05-28', 3, 4, N'Tuesday', N'سه شنبه', 19, 28, 231, 152, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140820, CONVERT(DATE, '20140820'), N'1393-05-29', 4, 5, N'Wednesday', N'چهار شنبه', 20, 29, 232, 153, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140821, CONVERT(DATE, '20140821'), N'1393-05-30', 5, 6, N'Thursday', N'پنج شنبه', 21, 30, 233, 154, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140822, CONVERT(DATE, '20140822'), N'1393-05-31', 6, 7, N'Friday', N'جمعه', 22, 31, 234, 155, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2014, 1393, 2, 1),
            (20140823, CONVERT(DATE, '20140823'), N'1393-06-01', 7, 1, N'Saturday', N'شنبه', 23, 1, 235, 156, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140824, CONVERT(DATE, '20140824'), N'1393-06-02', 1, 2, N'Sunday', N'یک شنبه', 24, 2, 236, 157, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140825, CONVERT(DATE, '20140825'), N'1393-06-03', 2, 3, N'Monday', N'دو شنبه', 25, 3, 237, 158, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140826, CONVERT(DATE, '20140826'), N'1393-06-04', 3, 4, N'Tuesday', N'سه شنبه', 26, 4, 238, 159, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140827, CONVERT(DATE, '20140827'), N'1393-06-05', 4, 5, N'Wednesday', N'چهار شنبه', 27, 5, 239, 160, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140828, CONVERT(DATE, '20140828'), N'1393-06-06', 5, 6, N'Thursday', N'پنج شنبه', 28, 6, 240, 161, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140829, CONVERT(DATE, '20140829'), N'1393-06-07', 6, 7, N'Friday', N'جمعه', 29, 7, 241, 162, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140830, CONVERT(DATE, '20140830'), N'1393-06-08', 7, 1, N'Saturday', N'شنبه', 30, 8, 242, 163, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140831, CONVERT(DATE, '20140831'), N'1393-06-09', 1, 2, N'Sunday', N'یک شنبه', 31, 9, 243, 164, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2014, 1393, 2, 1),
            (20140901, CONVERT(DATE, '20140901'), N'1393-06-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 244, 165, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140902, CONVERT(DATE, '20140902'), N'1393-06-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 245, 166, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140903, CONVERT(DATE, '20140903'), N'1393-06-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 246, 167, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140904, CONVERT(DATE, '20140904'), N'1393-06-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 247, 168, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140905, CONVERT(DATE, '20140905'), N'1393-06-14', 6, 7, N'Friday', N'جمعه', 5, 14, 248, 169, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140906, CONVERT(DATE, '20140906'), N'1393-06-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 249, 170, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140907, CONVERT(DATE, '20140907'), N'1393-06-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 250, 171, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140908, CONVERT(DATE, '20140908'), N'1393-06-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 251, 172, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140909, CONVERT(DATE, '20140909'), N'1393-06-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 252, 173, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140910, CONVERT(DATE, '20140910'), N'1393-06-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 253, 174, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140911, CONVERT(DATE, '20140911'), N'1393-06-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 254, 175, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140912, CONVERT(DATE, '20140912'), N'1393-06-21', 6, 7, N'Friday', N'جمعه', 12, 21, 255, 176, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140913, CONVERT(DATE, '20140913'), N'1393-06-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 256, 177, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140914, CONVERT(DATE, '20140914'), N'1393-06-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 257, 178, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140915, CONVERT(DATE, '20140915'), N'1393-06-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 258, 179, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140916, CONVERT(DATE, '20140916'), N'1393-06-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 259, 180, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140917, CONVERT(DATE, '20140917'), N'1393-06-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 260, 181, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140918, CONVERT(DATE, '20140918'), N'1393-06-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 261, 182, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140919, CONVERT(DATE, '20140919'), N'1393-06-28', 6, 7, N'Friday', N'جمعه', 19, 28, 262, 183, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140920, CONVERT(DATE, '20140920'), N'1393-06-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 263, 184, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140921, CONVERT(DATE, '20140921'), N'1393-06-30', 1, 2, N'Sunday', N'یک شنبه', 21, 30, 264, 185, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140922, CONVERT(DATE, '20140922'), N'1393-06-31', 2, 3, N'Monday', N'دو شنبه', 22, 31, 265, 186, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2014, 1393, 2, 1),
            (20140923, CONVERT(DATE, '20140923'), N'1393-07-01', 3, 4, N'Tuesday', N'سه شنبه', 23, 1, 266, 187, 38, 27, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140924, CONVERT(DATE, '20140924'), N'1393-07-02', 4, 5, N'Wednesday', N'چهار شنبه', 24, 2, 267, 188, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140925, CONVERT(DATE, '20140925'), N'1393-07-03', 5, 6, N'Thursday', N'پنج شنبه', 25, 3, 268, 189, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140926, CONVERT(DATE, '20140926'), N'1393-07-04', 6, 7, N'Friday', N'جمعه', 26, 4, 269, 190, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140927, CONVERT(DATE, '20140927'), N'1393-07-05', 7, 1, N'Saturday', N'شنبه', 27, 5, 270, 191, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140928, CONVERT(DATE, '20140928'), N'1393-07-06', 1, 2, N'Sunday', N'یک شنبه', 28, 6, 271, 192, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140929, CONVERT(DATE, '20140929'), N'1393-07-07', 2, 3, N'Monday', N'دو شنبه', 29, 7, 272, 193, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20140930, CONVERT(DATE, '20140930'), N'1393-07-08', 3, 4, N'Tuesday', N'سه شنبه', 30, 8, 273, 194, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2014, 1393, 2, 2),
            (20141001, CONVERT(DATE, '20141001'), N'1393-07-09', 4, 5, N'Wednesday', N'چهار شنبه', 1, 9, 274, 195, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141002, CONVERT(DATE, '20141002'), N'1393-07-10', 5, 6, N'Thursday', N'پنج شنبه', 2, 10, 275, 196, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141003, CONVERT(DATE, '20141003'), N'1393-07-11', 6, 7, N'Friday', N'جمعه', 3, 11, 276, 197, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141004, CONVERT(DATE, '20141004'), N'1393-07-12', 7, 1, N'Saturday', N'شنبه', 4, 12, 277, 198, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141005, CONVERT(DATE, '20141005'), N'1393-07-13', 1, 2, N'Sunday', N'یک شنبه', 5, 13, 278, 199, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141006, CONVERT(DATE, '20141006'), N'1393-07-14', 2, 3, N'Monday', N'دو شنبه', 6, 14, 279, 200, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141007, CONVERT(DATE, '20141007'), N'1393-07-15', 3, 4, N'Tuesday', N'سه شنبه', 7, 15, 280, 201, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141008, CONVERT(DATE, '20141008'), N'1393-07-16', 4, 5, N'Wednesday', N'چهار شنبه', 8, 16, 281, 202, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141009, CONVERT(DATE, '20141009'), N'1393-07-17', 5, 6, N'Thursday', N'پنج شنبه', 9, 17, 282, 203, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141010, CONVERT(DATE, '20141010'), N'1393-07-18', 6, 7, N'Friday', N'جمعه', 10, 18, 283, 204, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141011, CONVERT(DATE, '20141011'), N'1393-07-19', 7, 1, N'Saturday', N'شنبه', 11, 19, 284, 205, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141012, CONVERT(DATE, '20141012'), N'1393-07-20', 1, 2, N'Sunday', N'یک شنبه', 12, 20, 285, 206, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141013, CONVERT(DATE, '20141013'), N'1393-07-21', 2, 3, N'Monday', N'دو شنبه', 13, 21, 286, 207, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141014, CONVERT(DATE, '20141014'), N'1393-07-22', 3, 4, N'Tuesday', N'سه شنبه', 14, 22, 287, 208, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141015, CONVERT(DATE, '20141015'), N'1393-07-23', 4, 5, N'Wednesday', N'چهار شنبه', 15, 23, 288, 209, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141016, CONVERT(DATE, '20141016'), N'1393-07-24', 5, 6, N'Thursday', N'پنج شنبه', 16, 24, 289, 210, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141017, CONVERT(DATE, '20141017'), N'1393-07-25', 6, 7, N'Friday', N'جمعه', 17, 25, 290, 211, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141018, CONVERT(DATE, '20141018'), N'1393-07-26', 7, 1, N'Saturday', N'شنبه', 18, 26, 291, 212, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141019, CONVERT(DATE, '20141019'), N'1393-07-27', 1, 2, N'Sunday', N'یک شنبه', 19, 27, 292, 213, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141020, CONVERT(DATE, '20141020'), N'1393-07-28', 2, 3, N'Monday', N'دو شنبه', 20, 28, 293, 214, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141021, CONVERT(DATE, '20141021'), N'1393-07-29', 3, 4, N'Tuesday', N'سه شنبه', 21, 29, 294, 215, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141022, CONVERT(DATE, '20141022'), N'1393-07-30', 4, 5, N'Wednesday', N'چهار شنبه', 22, 30, 295, 216, 43, 32, N'October', N'مهر', 10, 7, 4, 3, 2014, 1393, 2, 2),
            (20141023, CONVERT(DATE, '20141023'), N'1393-08-01', 5, 6, N'Thursday', N'پنج شنبه', 23, 1, 296, 217, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141024, CONVERT(DATE, '20141024'), N'1393-08-02', 6, 7, N'Friday', N'جمعه', 24, 2, 297, 218, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141025, CONVERT(DATE, '20141025'), N'1393-08-03', 7, 1, N'Saturday', N'شنبه', 25, 3, 298, 219, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141026, CONVERT(DATE, '20141026'), N'1393-08-04', 1, 2, N'Sunday', N'یک شنبه', 26, 4, 299, 220, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141027, CONVERT(DATE, '20141027'), N'1393-08-05', 2, 3, N'Monday', N'دو شنبه', 27, 5, 300, 221, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141028, CONVERT(DATE, '20141028'), N'1393-08-06', 3, 4, N'Tuesday', N'سه شنبه', 28, 6, 301, 222, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141029, CONVERT(DATE, '20141029'), N'1393-08-07', 4, 5, N'Wednesday', N'چهار شنبه', 29, 7, 302, 223, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141030, CONVERT(DATE, '20141030'), N'1393-08-08', 5, 6, N'Thursday', N'پنج شنبه', 30, 8, 303, 224, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141031, CONVERT(DATE, '20141031'), N'1393-08-09', 6, 7, N'Friday', N'جمعه', 31, 9, 304, 225, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2014, 1393, 2, 2),
            (20141101, CONVERT(DATE, '20141101'), N'1393-08-10', 7, 1, N'Saturday', N'شنبه', 1, 10, 305, 226, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141102, CONVERT(DATE, '20141102'), N'1393-08-11', 1, 2, N'Sunday', N'یک شنبه', 2, 11, 306, 227, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141103, CONVERT(DATE, '20141103'), N'1393-08-12', 2, 3, N'Monday', N'دو شنبه', 3, 12, 307, 228, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141104, CONVERT(DATE, '20141104'), N'1393-08-13', 3, 4, N'Tuesday', N'سه شنبه', 4, 13, 308, 229, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141105, CONVERT(DATE, '20141105'), N'1393-08-14', 4, 5, N'Wednesday', N'چهار شنبه', 5, 14, 309, 230, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141106, CONVERT(DATE, '20141106'), N'1393-08-15', 5, 6, N'Thursday', N'پنج شنبه', 6, 15, 310, 231, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141107, CONVERT(DATE, '20141107'), N'1393-08-16', 6, 7, N'Friday', N'جمعه', 7, 16, 311, 232, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141108, CONVERT(DATE, '20141108'), N'1393-08-17', 7, 1, N'Saturday', N'شنبه', 8, 17, 312, 233, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141109, CONVERT(DATE, '20141109'), N'1393-08-18', 1, 2, N'Sunday', N'یک شنبه', 9, 18, 313, 234, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141110, CONVERT(DATE, '20141110'), N'1393-08-19', 2, 3, N'Monday', N'دو شنبه', 10, 19, 314, 235, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141111, CONVERT(DATE, '20141111'), N'1393-08-20', 3, 4, N'Tuesday', N'سه شنبه', 11, 20, 315, 236, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141112, CONVERT(DATE, '20141112'), N'1393-08-21', 4, 5, N'Wednesday', N'چهار شنبه', 12, 21, 316, 237, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_date_sample', N'dim_date', @step_rows, N'Inserted rows from Dim_Date.txt chunk into temp table #dim_date_sample.';

        INSERT INTO #dim_date_sample (
            TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey, DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek, DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear, WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName, MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter, CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
        )
        VALUES
            (20141113, CONVERT(DATE, '20141113'), N'1393-08-22', 5, 6, N'Thursday', N'پنج شنبه', 13, 22, 317, 238, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141114, CONVERT(DATE, '20141114'), N'1393-08-23', 6, 7, N'Friday', N'جمعه', 14, 23, 318, 239, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141115, CONVERT(DATE, '20141115'), N'1393-08-24', 7, 1, N'Saturday', N'شنبه', 15, 24, 319, 240, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141116, CONVERT(DATE, '20141116'), N'1393-08-25', 1, 2, N'Sunday', N'یک شنبه', 16, 25, 320, 241, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141117, CONVERT(DATE, '20141117'), N'1393-08-26', 2, 3, N'Monday', N'دو شنبه', 17, 26, 321, 242, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141118, CONVERT(DATE, '20141118'), N'1393-08-27', 3, 4, N'Tuesday', N'سه شنبه', 18, 27, 322, 243, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141119, CONVERT(DATE, '20141119'), N'1393-08-28', 4, 5, N'Wednesday', N'چهار شنبه', 19, 28, 323, 244, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141120, CONVERT(DATE, '20141120'), N'1393-08-29', 5, 6, N'Thursday', N'پنج شنبه', 20, 29, 324, 245, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141121, CONVERT(DATE, '20141121'), N'1393-08-30', 6, 7, N'Friday', N'جمعه', 21, 30, 325, 246, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2014, 1393, 2, 2),
            (20141122, CONVERT(DATE, '20141122'), N'1393-09-01', 7, 1, N'Saturday', N'شنبه', 22, 1, 326, 247, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141123, CONVERT(DATE, '20141123'), N'1393-09-02', 1, 2, N'Sunday', N'یک شنبه', 23, 2, 327, 248, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141124, CONVERT(DATE, '20141124'), N'1393-09-03', 2, 3, N'Monday', N'دو شنبه', 24, 3, 328, 249, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141125, CONVERT(DATE, '20141125'), N'1393-09-04', 3, 4, N'Tuesday', N'سه شنبه', 25, 4, 329, 250, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141126, CONVERT(DATE, '20141126'), N'1393-09-05', 4, 5, N'Wednesday', N'چهار شنبه', 26, 5, 330, 251, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141127, CONVERT(DATE, '20141127'), N'1393-09-06', 5, 6, N'Thursday', N'پنج شنبه', 27, 6, 331, 252, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141128, CONVERT(DATE, '20141128'), N'1393-09-07', 6, 7, N'Friday', N'جمعه', 28, 7, 332, 253, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141129, CONVERT(DATE, '20141129'), N'1393-09-08', 7, 1, N'Saturday', N'شنبه', 29, 8, 333, 254, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141130, CONVERT(DATE, '20141130'), N'1393-09-09', 1, 2, N'Sunday', N'یک شنبه', 30, 9, 334, 255, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2014, 1393, 2, 2),
            (20141201, CONVERT(DATE, '20141201'), N'1393-09-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 335, 256, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141202, CONVERT(DATE, '20141202'), N'1393-09-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 336, 257, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141203, CONVERT(DATE, '20141203'), N'1393-09-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 337, 258, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141204, CONVERT(DATE, '20141204'), N'1393-09-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 338, 259, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141205, CONVERT(DATE, '20141205'), N'1393-09-14', 6, 7, N'Friday', N'جمعه', 5, 14, 339, 260, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141206, CONVERT(DATE, '20141206'), N'1393-09-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 340, 261, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141207, CONVERT(DATE, '20141207'), N'1393-09-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 341, 262, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141208, CONVERT(DATE, '20141208'), N'1393-09-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 342, 263, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141209, CONVERT(DATE, '20141209'), N'1393-09-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 343, 264, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141210, CONVERT(DATE, '20141210'), N'1393-09-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 344, 265, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141211, CONVERT(DATE, '20141211'), N'1393-09-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 345, 266, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141212, CONVERT(DATE, '20141212'), N'1393-09-21', 6, 7, N'Friday', N'جمعه', 12, 21, 346, 267, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141213, CONVERT(DATE, '20141213'), N'1393-09-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 347, 268, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141214, CONVERT(DATE, '20141214'), N'1393-09-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 348, 269, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141215, CONVERT(DATE, '20141215'), N'1393-09-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 349, 270, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141216, CONVERT(DATE, '20141216'), N'1393-09-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 350, 271, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141217, CONVERT(DATE, '20141217'), N'1393-09-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 351, 272, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141218, CONVERT(DATE, '20141218'), N'1393-09-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 352, 273, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141219, CONVERT(DATE, '20141219'), N'1393-09-28', 6, 7, N'Friday', N'جمعه', 19, 28, 353, 274, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141220, CONVERT(DATE, '20141220'), N'1393-09-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 354, 275, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141221, CONVERT(DATE, '20141221'), N'1393-09-30', 1, 2, N'Sunday', N'یک شنبه', 21, 30, 355, 276, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2014, 1393, 2, 2),
            (20141222, CONVERT(DATE, '20141222'), N'1393-10-01', 2, 3, N'Monday', N'دو شنبه', 22, 1, 356, 277, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141223, CONVERT(DATE, '20141223'), N'1393-10-02', 3, 4, N'Tuesday', N'سه شنبه', 23, 2, 357, 278, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141224, CONVERT(DATE, '20141224'), N'1393-10-03', 4, 5, N'Wednesday', N'چهار شنبه', 24, 3, 358, 279, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141225, CONVERT(DATE, '20141225'), N'1393-10-04', 5, 6, N'Thursday', N'پنج شنبه', 25, 4, 359, 280, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141226, CONVERT(DATE, '20141226'), N'1393-10-05', 6, 7, N'Friday', N'جمعه', 26, 5, 360, 281, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141227, CONVERT(DATE, '20141227'), N'1393-10-06', 7, 1, N'Saturday', N'شنبه', 27, 6, 361, 282, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141228, CONVERT(DATE, '20141228'), N'1393-10-07', 1, 2, N'Sunday', N'یک شنبه', 28, 7, 362, 283, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141229, CONVERT(DATE, '20141229'), N'1393-10-08', 2, 3, N'Monday', N'دو شنبه', 29, 8, 363, 284, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141230, CONVERT(DATE, '20141230'), N'1393-10-09', 3, 4, N'Tuesday', N'سه شنبه', 30, 9, 364, 285, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20141231, CONVERT(DATE, '20141231'), N'1393-10-10', 4, 5, N'Wednesday', N'چهار شنبه', 31, 10, 365, 286, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2014, 1393, 2, 2),
            (20150101, CONVERT(DATE, '20150101'), N'1393-10-11', 5, 6, N'Thursday', N'پنج شنبه', 1, 11, 1, 287, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2015, 1393, 1, 2),
            (19050623, CONVERT(DATE, '19050623'), N'0000-00-00', 7, 1, N'Saturday', N'شنبه', 21, 1, 21, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 1905, 0, 1, 1),
            (20090321, CONVERT(DATE, '20090321'), N'1388-01-01', 7, 1, N'Saturday', N'شنبه', 21, 1, 80, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090322, CONVERT(DATE, '20090322'), N'1388-01-02', 1, 2, N'Sunday', N'یک شنبه', 22, 2, 81, 2, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090323, CONVERT(DATE, '20090323'), N'1388-01-03', 2, 3, N'Monday', N'دو شنبه', 23, 3, 82, 3, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090324, CONVERT(DATE, '20090324'), N'1388-01-04', 3, 4, N'Tuesday', N'سه شنبه', 24, 4, 83, 4, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090325, CONVERT(DATE, '20090325'), N'1388-01-05', 4, 5, N'Wednesday', N'چهار شنبه', 25, 5, 84, 5, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090326, CONVERT(DATE, '20090326'), N'1388-01-06', 5, 6, N'Thursday', N'پنج شنبه', 26, 6, 85, 6, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090327, CONVERT(DATE, '20090327'), N'1388-01-07', 6, 7, N'Friday', N'جمعه', 27, 7, 86, 7, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090328, CONVERT(DATE, '20090328'), N'1388-01-08', 7, 1, N'Saturday', N'شنبه', 28, 8, 87, 8, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090329, CONVERT(DATE, '20090329'), N'1388-01-09', 1, 2, N'Sunday', N'یک شنبه', 29, 9, 88, 9, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090330, CONVERT(DATE, '20090330'), N'1388-01-10', 2, 3, N'Monday', N'دو شنبه', 30, 10, 89, 10, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090331, CONVERT(DATE, '20090331'), N'1388-01-11', 3, 4, N'Tuesday', N'سه شنبه', 31, 11, 90, 11, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2009, 1388, 1, 1),
            (20090401, CONVERT(DATE, '20090401'), N'1388-01-12', 4, 5, N'Wednesday', N'چهار شنبه', 1, 12, 91, 12, 13, 2, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090402, CONVERT(DATE, '20090402'), N'1388-01-13', 5, 6, N'Thursday', N'پنج شنبه', 2, 13, 92, 13, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090403, CONVERT(DATE, '20090403'), N'1388-01-14', 6, 7, N'Friday', N'جمعه', 3, 14, 93, 14, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090404, CONVERT(DATE, '20090404'), N'1388-01-15', 7, 1, N'Saturday', N'شنبه', 4, 15, 94, 15, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090405, CONVERT(DATE, '20090405'), N'1388-01-16', 1, 2, N'Sunday', N'یک شنبه', 5, 16, 95, 16, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090406, CONVERT(DATE, '20090406'), N'1388-01-17', 2, 3, N'Monday', N'دو شنبه', 6, 17, 96, 17, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090407, CONVERT(DATE, '20090407'), N'1388-01-18', 3, 4, N'Tuesday', N'سه شنبه', 7, 18, 97, 18, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090408, CONVERT(DATE, '20090408'), N'1388-01-19', 4, 5, N'Wednesday', N'چهار شنبه', 8, 19, 98, 19, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090409, CONVERT(DATE, '20090409'), N'1388-01-20', 5, 6, N'Thursday', N'پنج شنبه', 9, 20, 99, 20, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090410, CONVERT(DATE, '20090410'), N'1388-01-21', 6, 7, N'Friday', N'جمعه', 10, 21, 100, 21, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090411, CONVERT(DATE, '20090411'), N'1388-01-22', 7, 1, N'Saturday', N'شنبه', 11, 22, 101, 22, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090412, CONVERT(DATE, '20090412'), N'1388-01-23', 1, 2, N'Sunday', N'یک شنبه', 12, 23, 102, 23, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090413, CONVERT(DATE, '20090413'), N'1388-01-24', 2, 3, N'Monday', N'دو شنبه', 13, 24, 103, 24, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090414, CONVERT(DATE, '20090414'), N'1388-01-25', 3, 4, N'Tuesday', N'سه شنبه', 14, 25, 104, 25, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090415, CONVERT(DATE, '20090415'), N'1388-01-26', 4, 5, N'Wednesday', N'چهار شنبه', 15, 26, 105, 26, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090416, CONVERT(DATE, '20090416'), N'1388-01-27', 5, 6, N'Thursday', N'پنج شنبه', 16, 27, 106, 27, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090417, CONVERT(DATE, '20090417'), N'1388-01-28', 6, 7, N'Friday', N'جمعه', 17, 28, 107, 28, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090418, CONVERT(DATE, '20090418'), N'1388-01-29', 7, 1, N'Saturday', N'شنبه', 18, 29, 108, 29, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090419, CONVERT(DATE, '20090419'), N'1388-01-30', 1, 2, N'Sunday', N'یک شنبه', 19, 30, 109, 30, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090420, CONVERT(DATE, '20090420'), N'1388-01-31', 2, 3, N'Monday', N'دو شنبه', 20, 31, 110, 31, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2009, 1388, 1, 1),
            (20090421, CONVERT(DATE, '20090421'), N'1388-02-01', 3, 4, N'Tuesday', N'سه شنبه', 21, 1, 111, 32, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090422, CONVERT(DATE, '20090422'), N'1388-02-02', 4, 5, N'Wednesday', N'چهار شنبه', 22, 2, 112, 33, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090423, CONVERT(DATE, '20090423'), N'1388-02-03', 5, 6, N'Thursday', N'پنج شنبه', 23, 3, 113, 34, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090424, CONVERT(DATE, '20090424'), N'1388-02-04', 6, 7, N'Friday', N'جمعه', 24, 4, 114, 35, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090425, CONVERT(DATE, '20090425'), N'1388-02-05', 7, 1, N'Saturday', N'شنبه', 25, 5, 115, 36, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090426, CONVERT(DATE, '20090426'), N'1388-02-06', 1, 2, N'Sunday', N'یک شنبه', 26, 6, 116, 37, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090427, CONVERT(DATE, '20090427'), N'1388-02-07', 2, 3, N'Monday', N'دو شنبه', 27, 7, 117, 38, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090428, CONVERT(DATE, '20090428'), N'1388-02-08', 3, 4, N'Tuesday', N'سه شنبه', 28, 8, 118, 39, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090429, CONVERT(DATE, '20090429'), N'1388-02-09', 4, 5, N'Wednesday', N'چهار شنبه', 29, 9, 119, 40, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090430, CONVERT(DATE, '20090430'), N'1388-02-10', 5, 6, N'Thursday', N'پنج شنبه', 30, 10, 120, 41, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2009, 1388, 1, 1),
            (20090501, CONVERT(DATE, '20090501'), N'1388-02-11', 6, 7, N'Friday', N'جمعه', 1, 11, 121, 42, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090502, CONVERT(DATE, '20090502'), N'1388-02-12', 7, 1, N'Saturday', N'شنبه', 2, 12, 122, 43, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090503, CONVERT(DATE, '20090503'), N'1388-02-13', 1, 2, N'Sunday', N'یک شنبه', 3, 13, 123, 44, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090504, CONVERT(DATE, '20090504'), N'1388-02-14', 2, 3, N'Monday', N'دو شنبه', 4, 14, 124, 45, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090505, CONVERT(DATE, '20090505'), N'1388-02-15', 3, 4, N'Tuesday', N'سه شنبه', 5, 15, 125, 46, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090506, CONVERT(DATE, '20090506'), N'1388-02-16', 4, 5, N'Wednesday', N'چهار شنبه', 6, 16, 126, 47, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090507, CONVERT(DATE, '20090507'), N'1388-02-17', 5, 6, N'Thursday', N'پنج شنبه', 7, 17, 127, 48, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090508, CONVERT(DATE, '20090508'), N'1388-02-18', 6, 7, N'Friday', N'جمعه', 8, 18, 128, 49, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090509, CONVERT(DATE, '20090509'), N'1388-02-19', 7, 1, N'Saturday', N'شنبه', 9, 19, 129, 50, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090510, CONVERT(DATE, '20090510'), N'1388-02-20', 1, 2, N'Sunday', N'یک شنبه', 10, 20, 130, 51, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090511, CONVERT(DATE, '20090511'), N'1388-02-21', 2, 3, N'Monday', N'دو شنبه', 11, 21, 131, 52, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090512, CONVERT(DATE, '20090512'), N'1388-02-22', 3, 4, N'Tuesday', N'سه شنبه', 12, 22, 132, 53, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090513, CONVERT(DATE, '20090513'), N'1388-02-23', 4, 5, N'Wednesday', N'چهار شنبه', 13, 23, 133, 54, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090514, CONVERT(DATE, '20090514'), N'1388-02-24', 5, 6, N'Thursday', N'پنج شنبه', 14, 24, 134, 55, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090515, CONVERT(DATE, '20090515'), N'1388-02-25', 6, 7, N'Friday', N'جمعه', 15, 25, 135, 56, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090516, CONVERT(DATE, '20090516'), N'1388-02-26', 7, 1, N'Saturday', N'شنبه', 16, 26, 136, 57, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090517, CONVERT(DATE, '20090517'), N'1388-02-27', 1, 2, N'Sunday', N'یک شنبه', 17, 27, 137, 58, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090518, CONVERT(DATE, '20090518'), N'1388-02-28', 2, 3, N'Monday', N'دو شنبه', 18, 28, 138, 59, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090519, CONVERT(DATE, '20090519'), N'1388-02-29', 3, 4, N'Tuesday', N'سه شنبه', 19, 29, 139, 60, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090520, CONVERT(DATE, '20090520'), N'1388-02-30', 4, 5, N'Wednesday', N'چهار شنبه', 20, 30, 140, 61, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090521, CONVERT(DATE, '20090521'), N'1388-02-31', 5, 6, N'Thursday', N'پنج شنبه', 21, 31, 141, 62, 21, 10, N'May', N'اردیبهشت', 5, 2, 2, 1, 2009, 1388, 1, 1),
            (20090522, CONVERT(DATE, '20090522'), N'1388-03-01', 6, 7, N'Friday', N'جمعه', 22, 1, 142, 63, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090523, CONVERT(DATE, '20090523'), N'1388-03-02', 7, 1, N'Saturday', N'شنبه', 23, 2, 143, 64, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090524, CONVERT(DATE, '20090524'), N'1388-03-03', 1, 2, N'Sunday', N'یک شنبه', 24, 3, 144, 65, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090525, CONVERT(DATE, '20090525'), N'1388-03-04', 2, 3, N'Monday', N'دو شنبه', 25, 4, 145, 66, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090526, CONVERT(DATE, '20090526'), N'1388-03-05', 3, 4, N'Tuesday', N'سه شنبه', 26, 5, 146, 67, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090527, CONVERT(DATE, '20090527'), N'1388-03-06', 4, 5, N'Wednesday', N'چهار شنبه', 27, 6, 147, 68, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090528, CONVERT(DATE, '20090528'), N'1388-03-07', 5, 6, N'Thursday', N'پنج شنبه', 28, 7, 148, 69, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090529, CONVERT(DATE, '20090529'), N'1388-03-08', 6, 7, N'Friday', N'جمعه', 29, 8, 149, 70, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090530, CONVERT(DATE, '20090530'), N'1388-03-09', 7, 1, N'Saturday', N'شنبه', 30, 9, 150, 71, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090531, CONVERT(DATE, '20090531'), N'1388-03-10', 1, 2, N'Sunday', N'یک شنبه', 31, 10, 151, 72, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2009, 1388, 1, 1),
            (20090601, CONVERT(DATE, '20090601'), N'1388-03-11', 2, 3, N'Monday', N'دو شنبه', 1, 11, 152, 73, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090602, CONVERT(DATE, '20090602'), N'1388-03-12', 3, 4, N'Tuesday', N'سه شنبه', 2, 12, 153, 74, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090603, CONVERT(DATE, '20090603'), N'1388-03-13', 4, 5, N'Wednesday', N'چهار شنبه', 3, 13, 154, 75, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090604, CONVERT(DATE, '20090604'), N'1388-03-14', 5, 6, N'Thursday', N'پنج شنبه', 4, 14, 155, 76, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090605, CONVERT(DATE, '20090605'), N'1388-03-15', 6, 7, N'Friday', N'جمعه', 5, 15, 156, 77, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090606, CONVERT(DATE, '20090606'), N'1388-03-16', 7, 1, N'Saturday', N'شنبه', 6, 16, 157, 78, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090607, CONVERT(DATE, '20090607'), N'1388-03-17', 1, 2, N'Sunday', N'یک شنبه', 7, 17, 158, 79, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090608, CONVERT(DATE, '20090608'), N'1388-03-18', 2, 3, N'Monday', N'دو شنبه', 8, 18, 159, 80, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090609, CONVERT(DATE, '20090609'), N'1388-03-19', 3, 4, N'Tuesday', N'سه شنبه', 9, 19, 160, 81, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090610, CONVERT(DATE, '20090610'), N'1388-03-20', 4, 5, N'Wednesday', N'چهار شنبه', 10, 20, 161, 82, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090611, CONVERT(DATE, '20090611'), N'1388-03-21', 5, 6, N'Thursday', N'پنج شنبه', 11, 21, 162, 83, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090612, CONVERT(DATE, '20090612'), N'1388-03-22', 6, 7, N'Friday', N'جمعه', 12, 22, 163, 84, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090613, CONVERT(DATE, '20090613'), N'1388-03-23', 7, 1, N'Saturday', N'شنبه', 13, 23, 164, 85, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090614, CONVERT(DATE, '20090614'), N'1388-03-24', 1, 2, N'Sunday', N'یک شنبه', 14, 24, 165, 86, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090615, CONVERT(DATE, '20090615'), N'1388-03-25', 2, 3, N'Monday', N'دو شنبه', 15, 25, 166, 87, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090616, CONVERT(DATE, '20090616'), N'1388-03-26', 3, 4, N'Tuesday', N'سه شنبه', 16, 26, 167, 88, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090617, CONVERT(DATE, '20090617'), N'1388-03-27', 4, 5, N'Wednesday', N'چهار شنبه', 17, 27, 168, 89, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090618, CONVERT(DATE, '20090618'), N'1388-03-28', 5, 6, N'Thursday', N'پنج شنبه', 18, 28, 169, 90, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090619, CONVERT(DATE, '20090619'), N'1388-03-29', 6, 7, N'Friday', N'جمعه', 19, 29, 170, 91, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090620, CONVERT(DATE, '20090620'), N'1388-03-30', 7, 1, N'Saturday', N'شنبه', 20, 30, 171, 92, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090621, CONVERT(DATE, '20090621'), N'1388-03-31', 1, 2, N'Sunday', N'یک شنبه', 21, 31, 172, 93, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2009, 1388, 1, 1),
            (20090622, CONVERT(DATE, '20090622'), N'1388-04-01', 2, 3, N'Monday', N'دو شنبه', 22, 1, 173, 94, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090623, CONVERT(DATE, '20090623'), N'1388-04-02', 3, 4, N'Tuesday', N'سه شنبه', 23, 2, 174, 95, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090624, CONVERT(DATE, '20090624'), N'1388-04-03', 4, 5, N'Wednesday', N'چهار شنبه', 24, 3, 175, 96, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090625, CONVERT(DATE, '20090625'), N'1388-04-04', 5, 6, N'Thursday', N'پنج شنبه', 25, 4, 176, 97, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090626, CONVERT(DATE, '20090626'), N'1388-04-05', 6, 7, N'Friday', N'جمعه', 26, 5, 177, 98, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090627, CONVERT(DATE, '20090627'), N'1388-04-06', 7, 1, N'Saturday', N'شنبه', 27, 6, 178, 99, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090628, CONVERT(DATE, '20090628'), N'1388-04-07', 1, 2, N'Sunday', N'یک شنبه', 28, 7, 179, 100, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090629, CONVERT(DATE, '20090629'), N'1388-04-08', 2, 3, N'Monday', N'دو شنبه', 29, 8, 180, 101, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090630, CONVERT(DATE, '20090630'), N'1388-04-09', 3, 4, N'Tuesday', N'سه شنبه', 30, 9, 181, 102, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2009, 1388, 1, 1),
            (20090701, CONVERT(DATE, '20090701'), N'1388-04-10', 4, 5, N'Wednesday', N'چهار شنبه', 1, 10, 182, 103, 26, 15, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090702, CONVERT(DATE, '20090702'), N'1388-04-11', 5, 6, N'Thursday', N'پنج شنبه', 2, 11, 183, 104, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090703, CONVERT(DATE, '20090703'), N'1388-04-12', 6, 7, N'Friday', N'جمعه', 3, 12, 184, 105, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090704, CONVERT(DATE, '20090704'), N'1388-04-13', 7, 1, N'Saturday', N'شنبه', 4, 13, 185, 106, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090705, CONVERT(DATE, '20090705'), N'1388-04-14', 1, 2, N'Sunday', N'یک شنبه', 5, 14, 186, 107, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090706, CONVERT(DATE, '20090706'), N'1388-04-15', 2, 3, N'Monday', N'دو شنبه', 6, 15, 187, 108, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090707, CONVERT(DATE, '20090707'), N'1388-04-16', 3, 4, N'Tuesday', N'سه شنبه', 7, 16, 188, 109, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090708, CONVERT(DATE, '20090708'), N'1388-04-17', 4, 5, N'Wednesday', N'چهار شنبه', 8, 17, 189, 110, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090709, CONVERT(DATE, '20090709'), N'1388-04-18', 5, 6, N'Thursday', N'پنج شنبه', 9, 18, 190, 111, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090710, CONVERT(DATE, '20090710'), N'1388-04-19', 6, 7, N'Friday', N'جمعه', 10, 19, 191, 112, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090711, CONVERT(DATE, '20090711'), N'1388-04-20', 7, 1, N'Saturday', N'شنبه', 11, 20, 192, 113, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090712, CONVERT(DATE, '20090712'), N'1388-04-21', 1, 2, N'Sunday', N'یک شنبه', 12, 21, 193, 114, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090713, CONVERT(DATE, '20090713'), N'1388-04-22', 2, 3, N'Monday', N'دو شنبه', 13, 22, 194, 115, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090714, CONVERT(DATE, '20090714'), N'1388-04-23', 3, 4, N'Tuesday', N'سه شنبه', 14, 23, 195, 116, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090715, CONVERT(DATE, '20090715'), N'1388-04-24', 4, 5, N'Wednesday', N'چهار شنبه', 15, 24, 196, 117, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090716, CONVERT(DATE, '20090716'), N'1388-04-25', 5, 6, N'Thursday', N'پنج شنبه', 16, 25, 197, 118, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090717, CONVERT(DATE, '20090717'), N'1388-04-26', 6, 7, N'Friday', N'جمعه', 17, 26, 198, 119, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090718, CONVERT(DATE, '20090718'), N'1388-04-27', 7, 1, N'Saturday', N'شنبه', 18, 27, 199, 120, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090719, CONVERT(DATE, '20090719'), N'1388-04-28', 1, 2, N'Sunday', N'یک شنبه', 19, 28, 200, 121, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090720, CONVERT(DATE, '20090720'), N'1388-04-29', 2, 3, N'Monday', N'دو شنبه', 20, 29, 201, 122, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090721, CONVERT(DATE, '20090721'), N'1388-04-30', 3, 4, N'Tuesday', N'سه شنبه', 21, 30, 202, 123, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090722, CONVERT(DATE, '20090722'), N'1388-04-31', 4, 5, N'Wednesday', N'چهار شنبه', 22, 31, 203, 124, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2009, 1388, 2, 1),
            (20090723, CONVERT(DATE, '20090723'), N'1388-05-01', 5, 6, N'Thursday', N'پنج شنبه', 23, 1, 204, 125, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090724, CONVERT(DATE, '20090724'), N'1388-05-02', 6, 7, N'Friday', N'جمعه', 24, 2, 205, 126, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090725, CONVERT(DATE, '20090725'), N'1388-05-03', 7, 1, N'Saturday', N'شنبه', 25, 3, 206, 127, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090726, CONVERT(DATE, '20090726'), N'1388-05-04', 1, 2, N'Sunday', N'یک شنبه', 26, 4, 207, 128, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090727, CONVERT(DATE, '20090727'), N'1388-05-05', 2, 3, N'Monday', N'دو شنبه', 27, 5, 208, 129, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090728, CONVERT(DATE, '20090728'), N'1388-05-06', 3, 4, N'Tuesday', N'سه شنبه', 28, 6, 209, 130, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090729, CONVERT(DATE, '20090729'), N'1388-05-07', 4, 5, N'Wednesday', N'چهار شنبه', 29, 7, 210, 131, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090730, CONVERT(DATE, '20090730'), N'1388-05-08', 5, 6, N'Thursday', N'پنج شنبه', 30, 8, 211, 132, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090731, CONVERT(DATE, '20090731'), N'1388-05-09', 6, 7, N'Friday', N'جمعه', 31, 9, 212, 133, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2009, 1388, 2, 1),
            (20090801, CONVERT(DATE, '20090801'), N'1388-05-10', 7, 1, N'Saturday', N'شنبه', 1, 10, 213, 134, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090802, CONVERT(DATE, '20090802'), N'1388-05-11', 1, 2, N'Sunday', N'یک شنبه', 2, 11, 214, 135, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090803, CONVERT(DATE, '20090803'), N'1388-05-12', 2, 3, N'Monday', N'دو شنبه', 3, 12, 215, 136, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090804, CONVERT(DATE, '20090804'), N'1388-05-13', 3, 4, N'Tuesday', N'سه شنبه', 4, 13, 216, 137, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090805, CONVERT(DATE, '20090805'), N'1388-05-14', 4, 5, N'Wednesday', N'چهار شنبه', 5, 14, 217, 138, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090806, CONVERT(DATE, '20090806'), N'1388-05-15', 5, 6, N'Thursday', N'پنج شنبه', 6, 15, 218, 139, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090807, CONVERT(DATE, '20090807'), N'1388-05-16', 6, 7, N'Friday', N'جمعه', 7, 16, 219, 140, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090808, CONVERT(DATE, '20090808'), N'1388-05-17', 7, 1, N'Saturday', N'شنبه', 8, 17, 220, 141, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090809, CONVERT(DATE, '20090809'), N'1388-05-18', 1, 2, N'Sunday', N'یک شنبه', 9, 18, 221, 142, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090810, CONVERT(DATE, '20090810'), N'1388-05-19', 2, 3, N'Monday', N'دو شنبه', 10, 19, 222, 143, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090811, CONVERT(DATE, '20090811'), N'1388-05-20', 3, 4, N'Tuesday', N'سه شنبه', 11, 20, 223, 144, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090812, CONVERT(DATE, '20090812'), N'1388-05-21', 4, 5, N'Wednesday', N'چهار شنبه', 12, 21, 224, 145, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090813, CONVERT(DATE, '20090813'), N'1388-05-22', 5, 6, N'Thursday', N'پنج شنبه', 13, 22, 225, 146, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090814, CONVERT(DATE, '20090814'), N'1388-05-23', 6, 7, N'Friday', N'جمعه', 14, 23, 226, 147, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090815, CONVERT(DATE, '20090815'), N'1388-05-24', 7, 1, N'Saturday', N'شنبه', 15, 24, 227, 148, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090816, CONVERT(DATE, '20090816'), N'1388-05-25', 1, 2, N'Sunday', N'یک شنبه', 16, 25, 228, 149, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090817, CONVERT(DATE, '20090817'), N'1388-05-26', 2, 3, N'Monday', N'دو شنبه', 17, 26, 229, 150, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090818, CONVERT(DATE, '20090818'), N'1388-05-27', 3, 4, N'Tuesday', N'سه شنبه', 18, 27, 230, 151, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090819, CONVERT(DATE, '20090819'), N'1388-05-28', 4, 5, N'Wednesday', N'چهار شنبه', 19, 28, 231, 152, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090820, CONVERT(DATE, '20090820'), N'1388-05-29', 5, 6, N'Thursday', N'پنج شنبه', 20, 29, 232, 153, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090821, CONVERT(DATE, '20090821'), N'1388-05-30', 6, 7, N'Friday', N'جمعه', 21, 30, 233, 154, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090822, CONVERT(DATE, '20090822'), N'1388-05-31', 7, 1, N'Saturday', N'شنبه', 22, 31, 234, 155, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2009, 1388, 2, 1),
            (20090823, CONVERT(DATE, '20090823'), N'1388-06-01', 1, 2, N'Sunday', N'یک شنبه', 23, 1, 235, 156, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090824, CONVERT(DATE, '20090824'), N'1388-06-02', 2, 3, N'Monday', N'دو شنبه', 24, 2, 236, 157, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090825, CONVERT(DATE, '20090825'), N'1388-06-03', 3, 4, N'Tuesday', N'سه شنبه', 25, 3, 237, 158, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090826, CONVERT(DATE, '20090826'), N'1388-06-04', 4, 5, N'Wednesday', N'چهار شنبه', 26, 4, 238, 159, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090827, CONVERT(DATE, '20090827'), N'1388-06-05', 5, 6, N'Thursday', N'پنج شنبه', 27, 5, 239, 160, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090828, CONVERT(DATE, '20090828'), N'1388-06-06', 6, 7, N'Friday', N'جمعه', 28, 6, 240, 161, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090829, CONVERT(DATE, '20090829'), N'1388-06-07', 7, 1, N'Saturday', N'شنبه', 29, 7, 241, 162, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090830, CONVERT(DATE, '20090830'), N'1388-06-08', 1, 2, N'Sunday', N'یک شنبه', 30, 8, 242, 163, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090831, CONVERT(DATE, '20090831'), N'1388-06-09', 2, 3, N'Monday', N'دو شنبه', 31, 9, 243, 164, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2009, 1388, 2, 1),
            (20090901, CONVERT(DATE, '20090901'), N'1388-06-10', 3, 4, N'Tuesday', N'سه شنبه', 1, 10, 244, 165, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090902, CONVERT(DATE, '20090902'), N'1388-06-11', 4, 5, N'Wednesday', N'چهار شنبه', 2, 11, 245, 166, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090903, CONVERT(DATE, '20090903'), N'1388-06-12', 5, 6, N'Thursday', N'پنج شنبه', 3, 12, 246, 167, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090904, CONVERT(DATE, '20090904'), N'1388-06-13', 6, 7, N'Friday', N'جمعه', 4, 13, 247, 168, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090905, CONVERT(DATE, '20090905'), N'1388-06-14', 7, 1, N'Saturday', N'شنبه', 5, 14, 248, 169, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090906, CONVERT(DATE, '20090906'), N'1388-06-15', 1, 2, N'Sunday', N'یک شنبه', 6, 15, 249, 170, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090907, CONVERT(DATE, '20090907'), N'1388-06-16', 2, 3, N'Monday', N'دو شنبه', 7, 16, 250, 171, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090908, CONVERT(DATE, '20090908'), N'1388-06-17', 3, 4, N'Tuesday', N'سه شنبه', 8, 17, 251, 172, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090909, CONVERT(DATE, '20090909'), N'1388-06-18', 4, 5, N'Wednesday', N'چهار شنبه', 9, 18, 252, 173, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090910, CONVERT(DATE, '20090910'), N'1388-06-19', 5, 6, N'Thursday', N'پنج شنبه', 10, 19, 253, 174, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090911, CONVERT(DATE, '20090911'), N'1388-06-20', 6, 7, N'Friday', N'جمعه', 11, 20, 254, 175, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090912, CONVERT(DATE, '20090912'), N'1388-06-21', 7, 1, N'Saturday', N'شنبه', 12, 21, 255, 176, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090913, CONVERT(DATE, '20090913'), N'1388-06-22', 1, 2, N'Sunday', N'یک شنبه', 13, 22, 256, 177, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090914, CONVERT(DATE, '20090914'), N'1388-06-23', 2, 3, N'Monday', N'دو شنبه', 14, 23, 257, 178, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090915, CONVERT(DATE, '20090915'), N'1388-06-24', 3, 4, N'Tuesday', N'سه شنبه', 15, 24, 258, 179, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090916, CONVERT(DATE, '20090916'), N'1388-06-25', 4, 5, N'Wednesday', N'چهار شنبه', 16, 25, 259, 180, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090917, CONVERT(DATE, '20090917'), N'1388-06-26', 5, 6, N'Thursday', N'پنج شنبه', 17, 26, 260, 181, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090918, CONVERT(DATE, '20090918'), N'1388-06-27', 6, 7, N'Friday', N'جمعه', 18, 27, 261, 182, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090919, CONVERT(DATE, '20090919'), N'1388-06-28', 7, 1, N'Saturday', N'شنبه', 19, 28, 262, 183, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090920, CONVERT(DATE, '20090920'), N'1388-06-29', 1, 2, N'Sunday', N'یک شنبه', 20, 29, 263, 184, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090921, CONVERT(DATE, '20090921'), N'1388-06-30', 2, 3, N'Monday', N'دو شنبه', 21, 30, 264, 185, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090922, CONVERT(DATE, '20090922'), N'1388-06-31', 3, 4, N'Tuesday', N'سه شنبه', 22, 31, 265, 186, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2009, 1388, 2, 1),
            (20090923, CONVERT(DATE, '20090923'), N'1388-07-01', 4, 5, N'Wednesday', N'چهار شنبه', 23, 1, 266, 187, 38, 27, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090924, CONVERT(DATE, '20090924'), N'1388-07-02', 5, 6, N'Thursday', N'پنج شنبه', 24, 2, 267, 188, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090925, CONVERT(DATE, '20090925'), N'1388-07-03', 6, 7, N'Friday', N'جمعه', 25, 3, 268, 189, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090926, CONVERT(DATE, '20090926'), N'1388-07-04', 7, 1, N'Saturday', N'شنبه', 26, 4, 269, 190, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090927, CONVERT(DATE, '20090927'), N'1388-07-05', 1, 2, N'Sunday', N'یک شنبه', 27, 5, 270, 191, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090928, CONVERT(DATE, '20090928'), N'1388-07-06', 2, 3, N'Monday', N'دو شنبه', 28, 6, 271, 192, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090929, CONVERT(DATE, '20090929'), N'1388-07-07', 3, 4, N'Tuesday', N'سه شنبه', 29, 7, 272, 193, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20090930, CONVERT(DATE, '20090930'), N'1388-07-08', 4, 5, N'Wednesday', N'چهار شنبه', 30, 8, 273, 194, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2009, 1388, 2, 2),
            (20091001, CONVERT(DATE, '20091001'), N'1388-07-09', 5, 6, N'Thursday', N'پنج شنبه', 1, 9, 274, 195, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091002, CONVERT(DATE, '20091002'), N'1388-07-10', 6, 7, N'Friday', N'جمعه', 2, 10, 275, 196, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091003, CONVERT(DATE, '20091003'), N'1388-07-11', 7, 1, N'Saturday', N'شنبه', 3, 11, 276, 197, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091004, CONVERT(DATE, '20091004'), N'1388-07-12', 1, 2, N'Sunday', N'یک شنبه', 4, 12, 277, 198, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091005, CONVERT(DATE, '20091005'), N'1388-07-13', 2, 3, N'Monday', N'دو شنبه', 5, 13, 278, 199, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091006, CONVERT(DATE, '20091006'), N'1388-07-14', 3, 4, N'Tuesday', N'سه شنبه', 6, 14, 279, 200, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091007, CONVERT(DATE, '20091007'), N'1388-07-15', 4, 5, N'Wednesday', N'چهار شنبه', 7, 15, 280, 201, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091008, CONVERT(DATE, '20091008'), N'1388-07-16', 5, 6, N'Thursday', N'پنج شنبه', 8, 16, 281, 202, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091009, CONVERT(DATE, '20091009'), N'1388-07-17', 6, 7, N'Friday', N'جمعه', 9, 17, 282, 203, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091010, CONVERT(DATE, '20091010'), N'1388-07-18', 7, 1, N'Saturday', N'شنبه', 10, 18, 283, 204, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091011, CONVERT(DATE, '20091011'), N'1388-07-19', 1, 2, N'Sunday', N'یک شنبه', 11, 19, 284, 205, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091012, CONVERT(DATE, '20091012'), N'1388-07-20', 2, 3, N'Monday', N'دو شنبه', 12, 20, 285, 206, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091013, CONVERT(DATE, '20091013'), N'1388-07-21', 3, 4, N'Tuesday', N'سه شنبه', 13, 21, 286, 207, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091014, CONVERT(DATE, '20091014'), N'1388-07-22', 4, 5, N'Wednesday', N'چهار شنبه', 14, 22, 287, 208, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091015, CONVERT(DATE, '20091015'), N'1388-07-23', 5, 6, N'Thursday', N'پنج شنبه', 15, 23, 288, 209, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091016, CONVERT(DATE, '20091016'), N'1388-07-24', 6, 7, N'Friday', N'جمعه', 16, 24, 289, 210, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091017, CONVERT(DATE, '20091017'), N'1388-07-25', 7, 1, N'Saturday', N'شنبه', 17, 25, 290, 211, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091018, CONVERT(DATE, '20091018'), N'1388-07-26', 1, 2, N'Sunday', N'یک شنبه', 18, 26, 291, 212, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091019, CONVERT(DATE, '20091019'), N'1388-07-27', 2, 3, N'Monday', N'دو شنبه', 19, 27, 292, 213, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091020, CONVERT(DATE, '20091020'), N'1388-07-28', 3, 4, N'Tuesday', N'سه شنبه', 20, 28, 293, 214, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091021, CONVERT(DATE, '20091021'), N'1388-07-29', 4, 5, N'Wednesday', N'چهار شنبه', 21, 29, 294, 215, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091022, CONVERT(DATE, '20091022'), N'1388-07-30', 5, 6, N'Thursday', N'پنج شنبه', 22, 30, 295, 216, 43, 32, N'October', N'مهر', 10, 7, 4, 3, 2009, 1388, 2, 2),
            (20091023, CONVERT(DATE, '20091023'), N'1388-08-01', 6, 7, N'Friday', N'جمعه', 23, 1, 296, 217, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091024, CONVERT(DATE, '20091024'), N'1388-08-02', 7, 1, N'Saturday', N'شنبه', 24, 2, 297, 218, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091025, CONVERT(DATE, '20091025'), N'1388-08-03', 1, 2, N'Sunday', N'یک شنبه', 25, 3, 298, 219, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091026, CONVERT(DATE, '20091026'), N'1388-08-04', 2, 3, N'Monday', N'دو شنبه', 26, 4, 299, 220, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091027, CONVERT(DATE, '20091027'), N'1388-08-05', 3, 4, N'Tuesday', N'سه شنبه', 27, 5, 300, 221, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091028, CONVERT(DATE, '20091028'), N'1388-08-06', 4, 5, N'Wednesday', N'چهار شنبه', 28, 6, 301, 222, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091029, CONVERT(DATE, '20091029'), N'1388-08-07', 5, 6, N'Thursday', N'پنج شنبه', 29, 7, 302, 223, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091030, CONVERT(DATE, '20091030'), N'1388-08-08', 6, 7, N'Friday', N'جمعه', 30, 8, 303, 224, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091031, CONVERT(DATE, '20091031'), N'1388-08-09', 7, 1, N'Saturday', N'شنبه', 31, 9, 304, 225, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2009, 1388, 2, 2),
            (20091101, CONVERT(DATE, '20091101'), N'1388-08-10', 1, 2, N'Sunday', N'یک شنبه', 1, 10, 305, 226, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091102, CONVERT(DATE, '20091102'), N'1388-08-11', 2, 3, N'Monday', N'دو شنبه', 2, 11, 306, 227, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091103, CONVERT(DATE, '20091103'), N'1388-08-12', 3, 4, N'Tuesday', N'سه شنبه', 3, 12, 307, 228, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091104, CONVERT(DATE, '20091104'), N'1388-08-13', 4, 5, N'Wednesday', N'چهار شنبه', 4, 13, 308, 229, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091105, CONVERT(DATE, '20091105'), N'1388-08-14', 5, 6, N'Thursday', N'پنج شنبه', 5, 14, 309, 230, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091106, CONVERT(DATE, '20091106'), N'1388-08-15', 6, 7, N'Friday', N'جمعه', 6, 15, 310, 231, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091107, CONVERT(DATE, '20091107'), N'1388-08-16', 7, 1, N'Saturday', N'شنبه', 7, 16, 311, 232, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091108, CONVERT(DATE, '20091108'), N'1388-08-17', 1, 2, N'Sunday', N'یک شنبه', 8, 17, 312, 233, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091109, CONVERT(DATE, '20091109'), N'1388-08-18', 2, 3, N'Monday', N'دو شنبه', 9, 18, 313, 234, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091110, CONVERT(DATE, '20091110'), N'1388-08-19', 3, 4, N'Tuesday', N'سه شنبه', 10, 19, 314, 235, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091111, CONVERT(DATE, '20091111'), N'1388-08-20', 4, 5, N'Wednesday', N'چهار شنبه', 11, 20, 315, 236, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091112, CONVERT(DATE, '20091112'), N'1388-08-21', 5, 6, N'Thursday', N'پنج شنبه', 12, 21, 316, 237, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091113, CONVERT(DATE, '20091113'), N'1388-08-22', 6, 7, N'Friday', N'جمعه', 13, 22, 317, 238, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091114, CONVERT(DATE, '20091114'), N'1388-08-23', 7, 1, N'Saturday', N'شنبه', 14, 23, 318, 239, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091115, CONVERT(DATE, '20091115'), N'1388-08-24', 1, 2, N'Sunday', N'یک شنبه', 15, 24, 319, 240, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091116, CONVERT(DATE, '20091116'), N'1388-08-25', 2, 3, N'Monday', N'دو شنبه', 16, 25, 320, 241, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091117, CONVERT(DATE, '20091117'), N'1388-08-26', 3, 4, N'Tuesday', N'سه شنبه', 17, 26, 321, 242, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091118, CONVERT(DATE, '20091118'), N'1388-08-27', 4, 5, N'Wednesday', N'چهار شنبه', 18, 27, 322, 243, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091119, CONVERT(DATE, '20091119'), N'1388-08-28', 5, 6, N'Thursday', N'پنج شنبه', 19, 28, 323, 244, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091120, CONVERT(DATE, '20091120'), N'1388-08-29', 6, 7, N'Friday', N'جمعه', 20, 29, 324, 245, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091121, CONVERT(DATE, '20091121'), N'1388-08-30', 7, 1, N'Saturday', N'شنبه', 21, 30, 325, 246, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2009, 1388, 2, 2),
            (20091122, CONVERT(DATE, '20091122'), N'1388-09-01', 1, 2, N'Sunday', N'یک شنبه', 22, 1, 326, 247, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091123, CONVERT(DATE, '20091123'), N'1388-09-02', 2, 3, N'Monday', N'دو شنبه', 23, 2, 327, 248, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091124, CONVERT(DATE, '20091124'), N'1388-09-03', 3, 4, N'Tuesday', N'سه شنبه', 24, 3, 328, 249, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091125, CONVERT(DATE, '20091125'), N'1388-09-04', 4, 5, N'Wednesday', N'چهار شنبه', 25, 4, 329, 250, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091126, CONVERT(DATE, '20091126'), N'1388-09-05', 5, 6, N'Thursday', N'پنج شنبه', 26, 5, 330, 251, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091127, CONVERT(DATE, '20091127'), N'1388-09-06', 6, 7, N'Friday', N'جمعه', 27, 6, 331, 252, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091128, CONVERT(DATE, '20091128'), N'1388-09-07', 7, 1, N'Saturday', N'شنبه', 28, 7, 332, 253, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091129, CONVERT(DATE, '20091129'), N'1388-09-08', 1, 2, N'Sunday', N'یک شنبه', 29, 8, 333, 254, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091130, CONVERT(DATE, '20091130'), N'1388-09-09', 2, 3, N'Monday', N'دو شنبه', 30, 9, 334, 255, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2009, 1388, 2, 2),
            (20091201, CONVERT(DATE, '20091201'), N'1388-09-10', 3, 4, N'Tuesday', N'سه شنبه', 1, 10, 335, 256, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091202, CONVERT(DATE, '20091202'), N'1388-09-11', 4, 5, N'Wednesday', N'چهار شنبه', 2, 11, 336, 257, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091203, CONVERT(DATE, '20091203'), N'1388-09-12', 5, 6, N'Thursday', N'پنج شنبه', 3, 12, 337, 258, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091204, CONVERT(DATE, '20091204'), N'1388-09-13', 6, 7, N'Friday', N'جمعه', 4, 13, 338, 259, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091205, CONVERT(DATE, '20091205'), N'1388-09-14', 7, 1, N'Saturday', N'شنبه', 5, 14, 339, 260, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091206, CONVERT(DATE, '20091206'), N'1388-09-15', 1, 2, N'Sunday', N'یک شنبه', 6, 15, 340, 261, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091207, CONVERT(DATE, '20091207'), N'1388-09-16', 2, 3, N'Monday', N'دو شنبه', 7, 16, 341, 262, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091208, CONVERT(DATE, '20091208'), N'1388-09-17', 3, 4, N'Tuesday', N'سه شنبه', 8, 17, 342, 263, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091209, CONVERT(DATE, '20091209'), N'1388-09-18', 4, 5, N'Wednesday', N'چهار شنبه', 9, 18, 343, 264, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091210, CONVERT(DATE, '20091210'), N'1388-09-19', 5, 6, N'Thursday', N'پنج شنبه', 10, 19, 344, 265, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091211, CONVERT(DATE, '20091211'), N'1388-09-20', 6, 7, N'Friday', N'جمعه', 11, 20, 345, 266, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091212, CONVERT(DATE, '20091212'), N'1388-09-21', 7, 1, N'Saturday', N'شنبه', 12, 21, 346, 267, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091213, CONVERT(DATE, '20091213'), N'1388-09-22', 1, 2, N'Sunday', N'یک شنبه', 13, 22, 347, 268, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091214, CONVERT(DATE, '20091214'), N'1388-09-23', 2, 3, N'Monday', N'دو شنبه', 14, 23, 348, 269, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091215, CONVERT(DATE, '20091215'), N'1388-09-24', 3, 4, N'Tuesday', N'سه شنبه', 15, 24, 349, 270, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091216, CONVERT(DATE, '20091216'), N'1388-09-25', 4, 5, N'Wednesday', N'چهار شنبه', 16, 25, 350, 271, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091217, CONVERT(DATE, '20091217'), N'1388-09-26', 5, 6, N'Thursday', N'پنج شنبه', 17, 26, 351, 272, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091218, CONVERT(DATE, '20091218'), N'1388-09-27', 6, 7, N'Friday', N'جمعه', 18, 27, 352, 273, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091219, CONVERT(DATE, '20091219'), N'1388-09-28', 7, 1, N'Saturday', N'شنبه', 19, 28, 353, 274, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091220, CONVERT(DATE, '20091220'), N'1388-09-29', 1, 2, N'Sunday', N'یک شنبه', 20, 29, 354, 275, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091221, CONVERT(DATE, '20091221'), N'1388-09-30', 2, 3, N'Monday', N'دو شنبه', 21, 30, 355, 276, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2009, 1388, 2, 2),
            (20091222, CONVERT(DATE, '20091222'), N'1388-10-01', 3, 4, N'Tuesday', N'سه شنبه', 22, 1, 356, 277, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091223, CONVERT(DATE, '20091223'), N'1388-10-02', 4, 5, N'Wednesday', N'چهار شنبه', 23, 2, 357, 278, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091224, CONVERT(DATE, '20091224'), N'1388-10-03', 5, 6, N'Thursday', N'پنج شنبه', 24, 3, 358, 279, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091225, CONVERT(DATE, '20091225'), N'1388-10-04', 6, 7, N'Friday', N'جمعه', 25, 4, 359, 280, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091226, CONVERT(DATE, '20091226'), N'1388-10-05', 7, 1, N'Saturday', N'شنبه', 26, 5, 360, 281, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091227, CONVERT(DATE, '20091227'), N'1388-10-06', 1, 2, N'Sunday', N'یک شنبه', 27, 6, 361, 282, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091228, CONVERT(DATE, '20091228'), N'1388-10-07', 2, 3, N'Monday', N'دو شنبه', 28, 7, 362, 283, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091229, CONVERT(DATE, '20091229'), N'1388-10-08', 3, 4, N'Tuesday', N'سه شنبه', 29, 8, 363, 284, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091230, CONVERT(DATE, '20091230'), N'1388-10-09', 4, 5, N'Wednesday', N'چهار شنبه', 30, 9, 364, 285, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20091231, CONVERT(DATE, '20091231'), N'1388-10-10', 5, 6, N'Thursday', N'پنج شنبه', 31, 10, 365, 286, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2009, 1388, 2, 2),
            (20100101, CONVERT(DATE, '20100101'), N'1388-10-11', 6, 7, N'Friday', N'جمعه', 1, 11, 1, 287, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100102, CONVERT(DATE, '20100102'), N'1388-10-12', 7, 1, N'Saturday', N'شنبه', 2, 12, 2, 288, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100103, CONVERT(DATE, '20100103'), N'1388-10-13', 1, 2, N'Sunday', N'یک شنبه', 3, 13, 3, 289, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100104, CONVERT(DATE, '20100104'), N'1388-10-14', 2, 3, N'Monday', N'دو شنبه', 4, 14, 4, 290, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100105, CONVERT(DATE, '20100105'), N'1388-10-15', 3, 4, N'Tuesday', N'سه شنبه', 5, 15, 5, 291, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100106, CONVERT(DATE, '20100106'), N'1388-10-16', 4, 5, N'Wednesday', N'چهار شنبه', 6, 16, 6, 292, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100107, CONVERT(DATE, '20100107'), N'1388-10-17', 5, 6, N'Thursday', N'پنج شنبه', 7, 17, 7, 293, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100108, CONVERT(DATE, '20100108'), N'1388-10-18', 6, 7, N'Friday', N'جمعه', 8, 18, 8, 294, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100109, CONVERT(DATE, '20100109'), N'1388-10-19', 7, 1, N'Saturday', N'شنبه', 9, 19, 9, 295, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100110, CONVERT(DATE, '20100110'), N'1388-10-20', 1, 2, N'Sunday', N'یک شنبه', 10, 20, 10, 296, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100111, CONVERT(DATE, '20100111'), N'1388-10-21', 2, 3, N'Monday', N'دو شنبه', 11, 21, 11, 297, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100112, CONVERT(DATE, '20100112'), N'1388-10-22', 3, 4, N'Tuesday', N'سه شنبه', 12, 22, 12, 298, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100113, CONVERT(DATE, '20100113'), N'1388-10-23', 4, 5, N'Wednesday', N'چهار شنبه', 13, 23, 13, 299, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100114, CONVERT(DATE, '20100114'), N'1388-10-24', 5, 6, N'Thursday', N'پنج شنبه', 14, 24, 14, 300, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100115, CONVERT(DATE, '20100115'), N'1388-10-25', 6, 7, N'Friday', N'جمعه', 15, 25, 15, 301, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100116, CONVERT(DATE, '20100116'), N'1388-10-26', 7, 1, N'Saturday', N'شنبه', 16, 26, 16, 302, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100117, CONVERT(DATE, '20100117'), N'1388-10-27', 1, 2, N'Sunday', N'یک شنبه', 17, 27, 17, 303, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100118, CONVERT(DATE, '20100118'), N'1388-10-28', 2, 3, N'Monday', N'دو شنبه', 18, 28, 18, 304, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100119, CONVERT(DATE, '20100119'), N'1388-10-29', 3, 4, N'Tuesday', N'سه شنبه', 19, 29, 19, 305, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100120, CONVERT(DATE, '20100120'), N'1388-10-30', 4, 5, N'Wednesday', N'چهار شنبه', 20, 30, 20, 306, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2010, 1388, 1, 2),
            (20100121, CONVERT(DATE, '20100121'), N'1388-11-01', 5, 6, N'Thursday', N'پنج شنبه', 21, 1, 21, 307, 3, 44, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100122, CONVERT(DATE, '20100122'), N'1388-11-02', 6, 7, N'Friday', N'جمعه', 22, 2, 22, 308, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100123, CONVERT(DATE, '20100123'), N'1388-11-03', 7, 1, N'Saturday', N'شنبه', 23, 3, 23, 309, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100124, CONVERT(DATE, '20100124'), N'1388-11-04', 1, 2, N'Sunday', N'یک شنبه', 24, 4, 24, 310, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100125, CONVERT(DATE, '20100125'), N'1388-11-05', 2, 3, N'Monday', N'دو شنبه', 25, 5, 25, 311, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100126, CONVERT(DATE, '20100126'), N'1388-11-06', 3, 4, N'Tuesday', N'سه شنبه', 26, 6, 26, 312, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100127, CONVERT(DATE, '20100127'), N'1388-11-07', 4, 5, N'Wednesday', N'چهار شنبه', 27, 7, 27, 313, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100128, CONVERT(DATE, '20100128'), N'1388-11-08', 5, 6, N'Thursday', N'پنج شنبه', 28, 8, 28, 314, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100129, CONVERT(DATE, '20100129'), N'1388-11-09', 6, 7, N'Friday', N'جمعه', 29, 9, 29, 315, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100130, CONVERT(DATE, '20100130'), N'1388-11-10', 7, 1, N'Saturday', N'شنبه', 30, 10, 30, 316, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100131, CONVERT(DATE, '20100131'), N'1388-11-11', 1, 2, N'Sunday', N'یک شنبه', 31, 11, 31, 317, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2010, 1388, 1, 2),
            (20100201, CONVERT(DATE, '20100201'), N'1388-11-12', 2, 3, N'Monday', N'دو شنبه', 1, 12, 32, 318, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100202, CONVERT(DATE, '20100202'), N'1388-11-13', 3, 4, N'Tuesday', N'سه شنبه', 2, 13, 33, 319, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100203, CONVERT(DATE, '20100203'), N'1388-11-14', 4, 5, N'Wednesday', N'چهار شنبه', 3, 14, 34, 320, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100204, CONVERT(DATE, '20100204'), N'1388-11-15', 5, 6, N'Thursday', N'پنج شنبه', 4, 15, 35, 321, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100205, CONVERT(DATE, '20100205'), N'1388-11-16', 6, 7, N'Friday', N'جمعه', 5, 16, 36, 322, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100206, CONVERT(DATE, '20100206'), N'1388-11-17', 7, 1, N'Saturday', N'شنبه', 6, 17, 37, 323, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100207, CONVERT(DATE, '20100207'), N'1388-11-18', 1, 2, N'Sunday', N'یک شنبه', 7, 18, 38, 324, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100208, CONVERT(DATE, '20100208'), N'1388-11-19', 2, 3, N'Monday', N'دو شنبه', 8, 19, 39, 325, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100209, CONVERT(DATE, '20100209'), N'1388-11-20', 3, 4, N'Tuesday', N'سه شنبه', 9, 20, 40, 326, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100210, CONVERT(DATE, '20100210'), N'1388-11-21', 4, 5, N'Wednesday', N'چهار شنبه', 10, 21, 41, 327, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100211, CONVERT(DATE, '20100211'), N'1388-11-22', 5, 6, N'Thursday', N'پنج شنبه', 11, 22, 42, 328, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100212, CONVERT(DATE, '20100212'), N'1388-11-23', 6, 7, N'Friday', N'جمعه', 12, 23, 43, 329, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100213, CONVERT(DATE, '20100213'), N'1388-11-24', 7, 1, N'Saturday', N'شنبه', 13, 24, 44, 330, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100214, CONVERT(DATE, '20100214'), N'1388-11-25', 1, 2, N'Sunday', N'یک شنبه', 14, 25, 45, 331, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100215, CONVERT(DATE, '20100215'), N'1388-11-26', 2, 3, N'Monday', N'دو شنبه', 15, 26, 46, 332, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100216, CONVERT(DATE, '20100216'), N'1388-11-27', 3, 4, N'Tuesday', N'سه شنبه', 16, 27, 47, 333, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100217, CONVERT(DATE, '20100217'), N'1388-11-28', 4, 5, N'Wednesday', N'چهار شنبه', 17, 28, 48, 334, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100218, CONVERT(DATE, '20100218'), N'1388-11-29', 5, 6, N'Thursday', N'پنج شنبه', 18, 29, 49, 335, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100219, CONVERT(DATE, '20100219'), N'1388-11-30', 6, 7, N'Friday', N'جمعه', 19, 30, 50, 336, 8, 49, N'February', N'بهمن', 2, 11, 1, 4, 2010, 1388, 1, 2),
            (20100220, CONVERT(DATE, '20100220'), N'1388-12-01', 7, 1, N'Saturday', N'شنبه', 20, 1, 51, 337, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100221, CONVERT(DATE, '20100221'), N'1388-12-02', 1, 2, N'Sunday', N'یک شنبه', 21, 2, 52, 338, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100222, CONVERT(DATE, '20100222'), N'1388-12-03', 2, 3, N'Monday', N'دو شنبه', 22, 3, 53, 339, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100223, CONVERT(DATE, '20100223'), N'1388-12-04', 3, 4, N'Tuesday', N'سه شنبه', 23, 4, 54, 340, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100224, CONVERT(DATE, '20100224'), N'1388-12-05', 4, 5, N'Wednesday', N'چهار شنبه', 24, 5, 55, 341, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100225, CONVERT(DATE, '20100225'), N'1388-12-06', 5, 6, N'Thursday', N'پنج شنبه', 25, 6, 56, 342, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100226, CONVERT(DATE, '20100226'), N'1388-12-07', 6, 7, N'Friday', N'جمعه', 26, 7, 57, 343, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100227, CONVERT(DATE, '20100227'), N'1388-12-08', 7, 1, N'Saturday', N'شنبه', 27, 8, 58, 344, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100228, CONVERT(DATE, '20100228'), N'1388-12-09', 1, 2, N'Sunday', N'یک شنبه', 28, 9, 59, 345, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2010, 1388, 1, 2),
            (20100301, CONVERT(DATE, '20100301'), N'1388-12-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 60, 346, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100302, CONVERT(DATE, '20100302'), N'1388-12-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 61, 347, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100303, CONVERT(DATE, '20100303'), N'1388-12-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 62, 348, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100304, CONVERT(DATE, '20100304'), N'1388-12-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 63, 349, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_date_sample', N'dim_date', @step_rows, N'Inserted rows from Dim_Date.txt chunk into temp table #dim_date_sample.';

        INSERT INTO #dim_date_sample (
            TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey, DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek, DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear, WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName, MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter, CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
        )
        VALUES
            (20100305, CONVERT(DATE, '20100305'), N'1388-12-14', 6, 7, N'Friday', N'جمعه', 5, 14, 64, 350, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100306, CONVERT(DATE, '20100306'), N'1388-12-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 65, 351, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100307, CONVERT(DATE, '20100307'), N'1388-12-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 66, 352, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100308, CONVERT(DATE, '20100308'), N'1388-12-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 67, 353, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100309, CONVERT(DATE, '20100309'), N'1388-12-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 68, 354, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100310, CONVERT(DATE, '20100310'), N'1388-12-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 69, 355, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100311, CONVERT(DATE, '20100311'), N'1388-12-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 70, 356, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100312, CONVERT(DATE, '20100312'), N'1388-12-21', 6, 7, N'Friday', N'جمعه', 12, 21, 71, 357, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100313, CONVERT(DATE, '20100313'), N'1388-12-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 72, 358, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100314, CONVERT(DATE, '20100314'), N'1388-12-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 73, 359, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100315, CONVERT(DATE, '20100315'), N'1388-12-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 74, 360, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100316, CONVERT(DATE, '20100316'), N'1388-12-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 75, 361, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100317, CONVERT(DATE, '20100317'), N'1388-12-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 76, 362, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100318, CONVERT(DATE, '20100318'), N'1388-12-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 77, 363, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100319, CONVERT(DATE, '20100319'), N'1388-12-28', 6, 7, N'Friday', N'جمعه', 19, 28, 78, 364, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100320, CONVERT(DATE, '20100320'), N'1388-12-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 79, 365, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2010, 1388, 1, 2),
            (20100321, CONVERT(DATE, '20100321'), N'1389-01-01', 1, 2, N'Sunday', N'یک شنبه', 21, 1, 80, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100322, CONVERT(DATE, '20100322'), N'1389-01-02', 2, 3, N'Monday', N'دو شنبه', 22, 2, 81, 2, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100323, CONVERT(DATE, '20100323'), N'1389-01-03', 3, 4, N'Tuesday', N'سه شنبه', 23, 3, 82, 3, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100324, CONVERT(DATE, '20100324'), N'1389-01-04', 4, 5, N'Wednesday', N'چهار شنبه', 24, 4, 83, 4, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100325, CONVERT(DATE, '20100325'), N'1389-01-05', 5, 6, N'Thursday', N'پنج شنبه', 25, 5, 84, 5, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100326, CONVERT(DATE, '20100326'), N'1389-01-06', 6, 7, N'Friday', N'جمعه', 26, 6, 85, 6, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100327, CONVERT(DATE, '20100327'), N'1389-01-07', 7, 1, N'Saturday', N'شنبه', 27, 7, 86, 7, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100328, CONVERT(DATE, '20100328'), N'1389-01-08', 1, 2, N'Sunday', N'یک شنبه', 28, 8, 87, 8, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100329, CONVERT(DATE, '20100329'), N'1389-01-09', 2, 3, N'Monday', N'دو شنبه', 29, 9, 88, 9, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100330, CONVERT(DATE, '20100330'), N'1389-01-10', 3, 4, N'Tuesday', N'سه شنبه', 30, 10, 89, 10, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100331, CONVERT(DATE, '20100331'), N'1389-01-11', 4, 5, N'Wednesday', N'چهار شنبه', 31, 11, 90, 11, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2010, 1389, 1, 1),
            (20100401, CONVERT(DATE, '20100401'), N'1389-01-12', 5, 6, N'Thursday', N'پنج شنبه', 1, 12, 91, 12, 13, 2, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100402, CONVERT(DATE, '20100402'), N'1389-01-13', 6, 7, N'Friday', N'جمعه', 2, 13, 92, 13, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100403, CONVERT(DATE, '20100403'), N'1389-01-14', 7, 1, N'Saturday', N'شنبه', 3, 14, 93, 14, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100404, CONVERT(DATE, '20100404'), N'1389-01-15', 1, 2, N'Sunday', N'یک شنبه', 4, 15, 94, 15, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100405, CONVERT(DATE, '20100405'), N'1389-01-16', 2, 3, N'Monday', N'دو شنبه', 5, 16, 95, 16, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100406, CONVERT(DATE, '20100406'), N'1389-01-17', 3, 4, N'Tuesday', N'سه شنبه', 6, 17, 96, 17, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100407, CONVERT(DATE, '20100407'), N'1389-01-18', 4, 5, N'Wednesday', N'چهار شنبه', 7, 18, 97, 18, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100408, CONVERT(DATE, '20100408'), N'1389-01-19', 5, 6, N'Thursday', N'پنج شنبه', 8, 19, 98, 19, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100409, CONVERT(DATE, '20100409'), N'1389-01-20', 6, 7, N'Friday', N'جمعه', 9, 20, 99, 20, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100410, CONVERT(DATE, '20100410'), N'1389-01-21', 7, 1, N'Saturday', N'شنبه', 10, 21, 100, 21, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100411, CONVERT(DATE, '20100411'), N'1389-01-22', 1, 2, N'Sunday', N'یک شنبه', 11, 22, 101, 22, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100412, CONVERT(DATE, '20100412'), N'1389-01-23', 2, 3, N'Monday', N'دو شنبه', 12, 23, 102, 23, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100413, CONVERT(DATE, '20100413'), N'1389-01-24', 3, 4, N'Tuesday', N'سه شنبه', 13, 24, 103, 24, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100414, CONVERT(DATE, '20100414'), N'1389-01-25', 4, 5, N'Wednesday', N'چهار شنبه', 14, 25, 104, 25, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100415, CONVERT(DATE, '20100415'), N'1389-01-26', 5, 6, N'Thursday', N'پنج شنبه', 15, 26, 105, 26, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100416, CONVERT(DATE, '20100416'), N'1389-01-27', 6, 7, N'Friday', N'جمعه', 16, 27, 106, 27, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100417, CONVERT(DATE, '20100417'), N'1389-01-28', 7, 1, N'Saturday', N'شنبه', 17, 28, 107, 28, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100418, CONVERT(DATE, '20100418'), N'1389-01-29', 1, 2, N'Sunday', N'یک شنبه', 18, 29, 108, 29, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100419, CONVERT(DATE, '20100419'), N'1389-01-30', 2, 3, N'Monday', N'دو شنبه', 19, 30, 109, 30, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100420, CONVERT(DATE, '20100420'), N'1389-01-31', 3, 4, N'Tuesday', N'سه شنبه', 20, 31, 110, 31, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2010, 1389, 1, 1),
            (20100421, CONVERT(DATE, '20100421'), N'1389-02-01', 4, 5, N'Wednesday', N'چهار شنبه', 21, 1, 111, 32, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100422, CONVERT(DATE, '20100422'), N'1389-02-02', 5, 6, N'Thursday', N'پنج شنبه', 22, 2, 112, 33, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100423, CONVERT(DATE, '20100423'), N'1389-02-03', 6, 7, N'Friday', N'جمعه', 23, 3, 113, 34, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100424, CONVERT(DATE, '20100424'), N'1389-02-04', 7, 1, N'Saturday', N'شنبه', 24, 4, 114, 35, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100425, CONVERT(DATE, '20100425'), N'1389-02-05', 1, 2, N'Sunday', N'یک شنبه', 25, 5, 115, 36, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100426, CONVERT(DATE, '20100426'), N'1389-02-06', 2, 3, N'Monday', N'دو شنبه', 26, 6, 116, 37, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100427, CONVERT(DATE, '20100427'), N'1389-02-07', 3, 4, N'Tuesday', N'سه شنبه', 27, 7, 117, 38, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100428, CONVERT(DATE, '20100428'), N'1389-02-08', 4, 5, N'Wednesday', N'چهار شنبه', 28, 8, 118, 39, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100429, CONVERT(DATE, '20100429'), N'1389-02-09', 5, 6, N'Thursday', N'پنج شنبه', 29, 9, 119, 40, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100430, CONVERT(DATE, '20100430'), N'1389-02-10', 6, 7, N'Friday', N'جمعه', 30, 10, 120, 41, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2010, 1389, 1, 1),
            (20100501, CONVERT(DATE, '20100501'), N'1389-02-11', 7, 1, N'Saturday', N'شنبه', 1, 11, 121, 42, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100502, CONVERT(DATE, '20100502'), N'1389-02-12', 1, 2, N'Sunday', N'یک شنبه', 2, 12, 122, 43, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100503, CONVERT(DATE, '20100503'), N'1389-02-13', 2, 3, N'Monday', N'دو شنبه', 3, 13, 123, 44, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100504, CONVERT(DATE, '20100504'), N'1389-02-14', 3, 4, N'Tuesday', N'سه شنبه', 4, 14, 124, 45, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100505, CONVERT(DATE, '20100505'), N'1389-02-15', 4, 5, N'Wednesday', N'چهار شنبه', 5, 15, 125, 46, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100506, CONVERT(DATE, '20100506'), N'1389-02-16', 5, 6, N'Thursday', N'پنج شنبه', 6, 16, 126, 47, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100507, CONVERT(DATE, '20100507'), N'1389-02-17', 6, 7, N'Friday', N'جمعه', 7, 17, 127, 48, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100508, CONVERT(DATE, '20100508'), N'1389-02-18', 7, 1, N'Saturday', N'شنبه', 8, 18, 128, 49, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100509, CONVERT(DATE, '20100509'), N'1389-02-19', 1, 2, N'Sunday', N'یک شنبه', 9, 19, 129, 50, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100510, CONVERT(DATE, '20100510'), N'1389-02-20', 2, 3, N'Monday', N'دو شنبه', 10, 20, 130, 51, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100511, CONVERT(DATE, '20100511'), N'1389-02-21', 3, 4, N'Tuesday', N'سه شنبه', 11, 21, 131, 52, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100512, CONVERT(DATE, '20100512'), N'1389-02-22', 4, 5, N'Wednesday', N'چهار شنبه', 12, 22, 132, 53, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100513, CONVERT(DATE, '20100513'), N'1389-02-23', 5, 6, N'Thursday', N'پنج شنبه', 13, 23, 133, 54, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100514, CONVERT(DATE, '20100514'), N'1389-02-24', 6, 7, N'Friday', N'جمعه', 14, 24, 134, 55, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100515, CONVERT(DATE, '20100515'), N'1389-02-25', 7, 1, N'Saturday', N'شنبه', 15, 25, 135, 56, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100516, CONVERT(DATE, '20100516'), N'1389-02-26', 1, 2, N'Sunday', N'یک شنبه', 16, 26, 136, 57, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100517, CONVERT(DATE, '20100517'), N'1389-02-27', 2, 3, N'Monday', N'دو شنبه', 17, 27, 137, 58, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100518, CONVERT(DATE, '20100518'), N'1389-02-28', 3, 4, N'Tuesday', N'سه شنبه', 18, 28, 138, 59, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100519, CONVERT(DATE, '20100519'), N'1389-02-29', 4, 5, N'Wednesday', N'چهار شنبه', 19, 29, 139, 60, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100520, CONVERT(DATE, '20100520'), N'1389-02-30', 5, 6, N'Thursday', N'پنج شنبه', 20, 30, 140, 61, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100521, CONVERT(DATE, '20100521'), N'1389-02-31', 6, 7, N'Friday', N'جمعه', 21, 31, 141, 62, 21, 10, N'May', N'اردیبهشت', 5, 2, 2, 1, 2010, 1389, 1, 1),
            (20100522, CONVERT(DATE, '20100522'), N'1389-03-01', 7, 1, N'Saturday', N'شنبه', 22, 1, 142, 63, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100523, CONVERT(DATE, '20100523'), N'1389-03-02', 1, 2, N'Sunday', N'یک شنبه', 23, 2, 143, 64, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100524, CONVERT(DATE, '20100524'), N'1389-03-03', 2, 3, N'Monday', N'دو شنبه', 24, 3, 144, 65, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100525, CONVERT(DATE, '20100525'), N'1389-03-04', 3, 4, N'Tuesday', N'سه شنبه', 25, 4, 145, 66, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100526, CONVERT(DATE, '20100526'), N'1389-03-05', 4, 5, N'Wednesday', N'چهار شنبه', 26, 5, 146, 67, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100527, CONVERT(DATE, '20100527'), N'1389-03-06', 5, 6, N'Thursday', N'پنج شنبه', 27, 6, 147, 68, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100528, CONVERT(DATE, '20100528'), N'1389-03-07', 6, 7, N'Friday', N'جمعه', 28, 7, 148, 69, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100529, CONVERT(DATE, '20100529'), N'1389-03-08', 7, 1, N'Saturday', N'شنبه', 29, 8, 149, 70, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100530, CONVERT(DATE, '20100530'), N'1389-03-09', 1, 2, N'Sunday', N'یک شنبه', 30, 9, 150, 71, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100531, CONVERT(DATE, '20100531'), N'1389-03-10', 2, 3, N'Monday', N'دو شنبه', 31, 10, 151, 72, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2010, 1389, 1, 1),
            (20100601, CONVERT(DATE, '20100601'), N'1389-03-11', 3, 4, N'Tuesday', N'سه شنبه', 1, 11, 152, 73, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100602, CONVERT(DATE, '20100602'), N'1389-03-12', 4, 5, N'Wednesday', N'چهار شنبه', 2, 12, 153, 74, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100603, CONVERT(DATE, '20100603'), N'1389-03-13', 5, 6, N'Thursday', N'پنج شنبه', 3, 13, 154, 75, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100604, CONVERT(DATE, '20100604'), N'1389-03-14', 6, 7, N'Friday', N'جمعه', 4, 14, 155, 76, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100605, CONVERT(DATE, '20100605'), N'1389-03-15', 7, 1, N'Saturday', N'شنبه', 5, 15, 156, 77, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100606, CONVERT(DATE, '20100606'), N'1389-03-16', 1, 2, N'Sunday', N'یک شنبه', 6, 16, 157, 78, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100607, CONVERT(DATE, '20100607'), N'1389-03-17', 2, 3, N'Monday', N'دو شنبه', 7, 17, 158, 79, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100608, CONVERT(DATE, '20100608'), N'1389-03-18', 3, 4, N'Tuesday', N'سه شنبه', 8, 18, 159, 80, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100609, CONVERT(DATE, '20100609'), N'1389-03-19', 4, 5, N'Wednesday', N'چهار شنبه', 9, 19, 160, 81, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100610, CONVERT(DATE, '20100610'), N'1389-03-20', 5, 6, N'Thursday', N'پنج شنبه', 10, 20, 161, 82, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100611, CONVERT(DATE, '20100611'), N'1389-03-21', 6, 7, N'Friday', N'جمعه', 11, 21, 162, 83, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100612, CONVERT(DATE, '20100612'), N'1389-03-22', 7, 1, N'Saturday', N'شنبه', 12, 22, 163, 84, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100613, CONVERT(DATE, '20100613'), N'1389-03-23', 1, 2, N'Sunday', N'یک شنبه', 13, 23, 164, 85, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100614, CONVERT(DATE, '20100614'), N'1389-03-24', 2, 3, N'Monday', N'دو شنبه', 14, 24, 165, 86, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100615, CONVERT(DATE, '20100615'), N'1389-03-25', 3, 4, N'Tuesday', N'سه شنبه', 15, 25, 166, 87, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100616, CONVERT(DATE, '20100616'), N'1389-03-26', 4, 5, N'Wednesday', N'چهار شنبه', 16, 26, 167, 88, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100617, CONVERT(DATE, '20100617'), N'1389-03-27', 5, 6, N'Thursday', N'پنج شنبه', 17, 27, 168, 89, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100618, CONVERT(DATE, '20100618'), N'1389-03-28', 6, 7, N'Friday', N'جمعه', 18, 28, 169, 90, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100619, CONVERT(DATE, '20100619'), N'1389-03-29', 7, 1, N'Saturday', N'شنبه', 19, 29, 170, 91, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100620, CONVERT(DATE, '20100620'), N'1389-03-30', 1, 2, N'Sunday', N'یک شنبه', 20, 30, 171, 92, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100621, CONVERT(DATE, '20100621'), N'1389-03-31', 2, 3, N'Monday', N'دو شنبه', 21, 31, 172, 93, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2010, 1389, 1, 1),
            (20100622, CONVERT(DATE, '20100622'), N'1389-04-01', 3, 4, N'Tuesday', N'سه شنبه', 22, 1, 173, 94, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100623, CONVERT(DATE, '20100623'), N'1389-04-02', 4, 5, N'Wednesday', N'چهار شنبه', 23, 2, 174, 95, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100624, CONVERT(DATE, '20100624'), N'1389-04-03', 5, 6, N'Thursday', N'پنج شنبه', 24, 3, 175, 96, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100625, CONVERT(DATE, '20100625'), N'1389-04-04', 6, 7, N'Friday', N'جمعه', 25, 4, 176, 97, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100626, CONVERT(DATE, '20100626'), N'1389-04-05', 7, 1, N'Saturday', N'شنبه', 26, 5, 177, 98, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100627, CONVERT(DATE, '20100627'), N'1389-04-06', 1, 2, N'Sunday', N'یک شنبه', 27, 6, 178, 99, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100628, CONVERT(DATE, '20100628'), N'1389-04-07', 2, 3, N'Monday', N'دو شنبه', 28, 7, 179, 100, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100629, CONVERT(DATE, '20100629'), N'1389-04-08', 3, 4, N'Tuesday', N'سه شنبه', 29, 8, 180, 101, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100630, CONVERT(DATE, '20100630'), N'1389-04-09', 4, 5, N'Wednesday', N'چهار شنبه', 30, 9, 181, 102, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2010, 1389, 1, 1),
            (20100701, CONVERT(DATE, '20100701'), N'1389-04-10', 5, 6, N'Thursday', N'پنج شنبه', 1, 10, 182, 103, 26, 15, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100702, CONVERT(DATE, '20100702'), N'1389-04-11', 6, 7, N'Friday', N'جمعه', 2, 11, 183, 104, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100703, CONVERT(DATE, '20100703'), N'1389-04-12', 7, 1, N'Saturday', N'شنبه', 3, 12, 184, 105, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100704, CONVERT(DATE, '20100704'), N'1389-04-13', 1, 2, N'Sunday', N'یک شنبه', 4, 13, 185, 106, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100705, CONVERT(DATE, '20100705'), N'1389-04-14', 2, 3, N'Monday', N'دو شنبه', 5, 14, 186, 107, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100706, CONVERT(DATE, '20100706'), N'1389-04-15', 3, 4, N'Tuesday', N'سه شنبه', 6, 15, 187, 108, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100707, CONVERT(DATE, '20100707'), N'1389-04-16', 4, 5, N'Wednesday', N'چهار شنبه', 7, 16, 188, 109, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100708, CONVERT(DATE, '20100708'), N'1389-04-17', 5, 6, N'Thursday', N'پنج شنبه', 8, 17, 189, 110, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100709, CONVERT(DATE, '20100709'), N'1389-04-18', 6, 7, N'Friday', N'جمعه', 9, 18, 190, 111, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100710, CONVERT(DATE, '20100710'), N'1389-04-19', 7, 1, N'Saturday', N'شنبه', 10, 19, 191, 112, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100711, CONVERT(DATE, '20100711'), N'1389-04-20', 1, 2, N'Sunday', N'یک شنبه', 11, 20, 192, 113, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100712, CONVERT(DATE, '20100712'), N'1389-04-21', 2, 3, N'Monday', N'دو شنبه', 12, 21, 193, 114, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100713, CONVERT(DATE, '20100713'), N'1389-04-22', 3, 4, N'Tuesday', N'سه شنبه', 13, 22, 194, 115, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100714, CONVERT(DATE, '20100714'), N'1389-04-23', 4, 5, N'Wednesday', N'چهار شنبه', 14, 23, 195, 116, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100715, CONVERT(DATE, '20100715'), N'1389-04-24', 5, 6, N'Thursday', N'پنج شنبه', 15, 24, 196, 117, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100716, CONVERT(DATE, '20100716'), N'1389-04-25', 6, 7, N'Friday', N'جمعه', 16, 25, 197, 118, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100717, CONVERT(DATE, '20100717'), N'1389-04-26', 7, 1, N'Saturday', N'شنبه', 17, 26, 198, 119, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100718, CONVERT(DATE, '20100718'), N'1389-04-27', 1, 2, N'Sunday', N'یک شنبه', 18, 27, 199, 120, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100719, CONVERT(DATE, '20100719'), N'1389-04-28', 2, 3, N'Monday', N'دو شنبه', 19, 28, 200, 121, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100720, CONVERT(DATE, '20100720'), N'1389-04-29', 3, 4, N'Tuesday', N'سه شنبه', 20, 29, 201, 122, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100721, CONVERT(DATE, '20100721'), N'1389-04-30', 4, 5, N'Wednesday', N'چهار شنبه', 21, 30, 202, 123, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100722, CONVERT(DATE, '20100722'), N'1389-04-31', 5, 6, N'Thursday', N'پنج شنبه', 22, 31, 203, 124, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2010, 1389, 2, 1),
            (20100723, CONVERT(DATE, '20100723'), N'1389-05-01', 6, 7, N'Friday', N'جمعه', 23, 1, 204, 125, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100724, CONVERT(DATE, '20100724'), N'1389-05-02', 7, 1, N'Saturday', N'شنبه', 24, 2, 205, 126, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100725, CONVERT(DATE, '20100725'), N'1389-05-03', 1, 2, N'Sunday', N'یک شنبه', 25, 3, 206, 127, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100726, CONVERT(DATE, '20100726'), N'1389-05-04', 2, 3, N'Monday', N'دو شنبه', 26, 4, 207, 128, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100727, CONVERT(DATE, '20100727'), N'1389-05-05', 3, 4, N'Tuesday', N'سه شنبه', 27, 5, 208, 129, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100728, CONVERT(DATE, '20100728'), N'1389-05-06', 4, 5, N'Wednesday', N'چهار شنبه', 28, 6, 209, 130, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100729, CONVERT(DATE, '20100729'), N'1389-05-07', 5, 6, N'Thursday', N'پنج شنبه', 29, 7, 210, 131, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100730, CONVERT(DATE, '20100730'), N'1389-05-08', 6, 7, N'Friday', N'جمعه', 30, 8, 211, 132, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100731, CONVERT(DATE, '20100731'), N'1389-05-09', 7, 1, N'Saturday', N'شنبه', 31, 9, 212, 133, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2010, 1389, 2, 1),
            (20100801, CONVERT(DATE, '20100801'), N'1389-05-10', 1, 2, N'Sunday', N'یک شنبه', 1, 10, 213, 134, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100802, CONVERT(DATE, '20100802'), N'1389-05-11', 2, 3, N'Monday', N'دو شنبه', 2, 11, 214, 135, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100803, CONVERT(DATE, '20100803'), N'1389-05-12', 3, 4, N'Tuesday', N'سه شنبه', 3, 12, 215, 136, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100804, CONVERT(DATE, '20100804'), N'1389-05-13', 4, 5, N'Wednesday', N'چهار شنبه', 4, 13, 216, 137, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100805, CONVERT(DATE, '20100805'), N'1389-05-14', 5, 6, N'Thursday', N'پنج شنبه', 5, 14, 217, 138, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100806, CONVERT(DATE, '20100806'), N'1389-05-15', 6, 7, N'Friday', N'جمعه', 6, 15, 218, 139, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100807, CONVERT(DATE, '20100807'), N'1389-05-16', 7, 1, N'Saturday', N'شنبه', 7, 16, 219, 140, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100808, CONVERT(DATE, '20100808'), N'1389-05-17', 1, 2, N'Sunday', N'یک شنبه', 8, 17, 220, 141, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100809, CONVERT(DATE, '20100809'), N'1389-05-18', 2, 3, N'Monday', N'دو شنبه', 9, 18, 221, 142, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100810, CONVERT(DATE, '20100810'), N'1389-05-19', 3, 4, N'Tuesday', N'سه شنبه', 10, 19, 222, 143, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100811, CONVERT(DATE, '20100811'), N'1389-05-20', 4, 5, N'Wednesday', N'چهار شنبه', 11, 20, 223, 144, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100812, CONVERT(DATE, '20100812'), N'1389-05-21', 5, 6, N'Thursday', N'پنج شنبه', 12, 21, 224, 145, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100813, CONVERT(DATE, '20100813'), N'1389-05-22', 6, 7, N'Friday', N'جمعه', 13, 22, 225, 146, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100814, CONVERT(DATE, '20100814'), N'1389-05-23', 7, 1, N'Saturday', N'شنبه', 14, 23, 226, 147, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100815, CONVERT(DATE, '20100815'), N'1389-05-24', 1, 2, N'Sunday', N'یک شنبه', 15, 24, 227, 148, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100816, CONVERT(DATE, '20100816'), N'1389-05-25', 2, 3, N'Monday', N'دو شنبه', 16, 25, 228, 149, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100817, CONVERT(DATE, '20100817'), N'1389-05-26', 3, 4, N'Tuesday', N'سه شنبه', 17, 26, 229, 150, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100818, CONVERT(DATE, '20100818'), N'1389-05-27', 4, 5, N'Wednesday', N'چهار شنبه', 18, 27, 230, 151, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100819, CONVERT(DATE, '20100819'), N'1389-05-28', 5, 6, N'Thursday', N'پنج شنبه', 19, 28, 231, 152, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100820, CONVERT(DATE, '20100820'), N'1389-05-29', 6, 7, N'Friday', N'جمعه', 20, 29, 232, 153, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100821, CONVERT(DATE, '20100821'), N'1389-05-30', 7, 1, N'Saturday', N'شنبه', 21, 30, 233, 154, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100822, CONVERT(DATE, '20100822'), N'1389-05-31', 1, 2, N'Sunday', N'یک شنبه', 22, 31, 234, 155, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2010, 1389, 2, 1),
            (20100823, CONVERT(DATE, '20100823'), N'1389-06-01', 2, 3, N'Monday', N'دو شنبه', 23, 1, 235, 156, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100824, CONVERT(DATE, '20100824'), N'1389-06-02', 3, 4, N'Tuesday', N'سه شنبه', 24, 2, 236, 157, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100825, CONVERT(DATE, '20100825'), N'1389-06-03', 4, 5, N'Wednesday', N'چهار شنبه', 25, 3, 237, 158, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100826, CONVERT(DATE, '20100826'), N'1389-06-04', 5, 6, N'Thursday', N'پنج شنبه', 26, 4, 238, 159, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100827, CONVERT(DATE, '20100827'), N'1389-06-05', 6, 7, N'Friday', N'جمعه', 27, 5, 239, 160, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100828, CONVERT(DATE, '20100828'), N'1389-06-06', 7, 1, N'Saturday', N'شنبه', 28, 6, 240, 161, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100829, CONVERT(DATE, '20100829'), N'1389-06-07', 1, 2, N'Sunday', N'یک شنبه', 29, 7, 241, 162, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100830, CONVERT(DATE, '20100830'), N'1389-06-08', 2, 3, N'Monday', N'دو شنبه', 30, 8, 242, 163, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100831, CONVERT(DATE, '20100831'), N'1389-06-09', 3, 4, N'Tuesday', N'سه شنبه', 31, 9, 243, 164, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2010, 1389, 2, 1),
            (20100901, CONVERT(DATE, '20100901'), N'1389-06-10', 4, 5, N'Wednesday', N'چهار شنبه', 1, 10, 244, 165, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100902, CONVERT(DATE, '20100902'), N'1389-06-11', 5, 6, N'Thursday', N'پنج شنبه', 2, 11, 245, 166, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100903, CONVERT(DATE, '20100903'), N'1389-06-12', 6, 7, N'Friday', N'جمعه', 3, 12, 246, 167, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100904, CONVERT(DATE, '20100904'), N'1389-06-13', 7, 1, N'Saturday', N'شنبه', 4, 13, 247, 168, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100905, CONVERT(DATE, '20100905'), N'1389-06-14', 1, 2, N'Sunday', N'یک شنبه', 5, 14, 248, 169, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100906, CONVERT(DATE, '20100906'), N'1389-06-15', 2, 3, N'Monday', N'دو شنبه', 6, 15, 249, 170, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100907, CONVERT(DATE, '20100907'), N'1389-06-16', 3, 4, N'Tuesday', N'سه شنبه', 7, 16, 250, 171, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100908, CONVERT(DATE, '20100908'), N'1389-06-17', 4, 5, N'Wednesday', N'چهار شنبه', 8, 17, 251, 172, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100909, CONVERT(DATE, '20100909'), N'1389-06-18', 5, 6, N'Thursday', N'پنج شنبه', 9, 18, 252, 173, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100910, CONVERT(DATE, '20100910'), N'1389-06-19', 6, 7, N'Friday', N'جمعه', 10, 19, 253, 174, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100911, CONVERT(DATE, '20100911'), N'1389-06-20', 7, 1, N'Saturday', N'شنبه', 11, 20, 254, 175, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100912, CONVERT(DATE, '20100912'), N'1389-06-21', 1, 2, N'Sunday', N'یک شنبه', 12, 21, 255, 176, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100913, CONVERT(DATE, '20100913'), N'1389-06-22', 2, 3, N'Monday', N'دو شنبه', 13, 22, 256, 177, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100914, CONVERT(DATE, '20100914'), N'1389-06-23', 3, 4, N'Tuesday', N'سه شنبه', 14, 23, 257, 178, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100915, CONVERT(DATE, '20100915'), N'1389-06-24', 4, 5, N'Wednesday', N'چهار شنبه', 15, 24, 258, 179, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100916, CONVERT(DATE, '20100916'), N'1389-06-25', 5, 6, N'Thursday', N'پنج شنبه', 16, 25, 259, 180, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100917, CONVERT(DATE, '20100917'), N'1389-06-26', 6, 7, N'Friday', N'جمعه', 17, 26, 260, 181, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100918, CONVERT(DATE, '20100918'), N'1389-06-27', 7, 1, N'Saturday', N'شنبه', 18, 27, 261, 182, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100919, CONVERT(DATE, '20100919'), N'1389-06-28', 1, 2, N'Sunday', N'یک شنبه', 19, 28, 262, 183, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100920, CONVERT(DATE, '20100920'), N'1389-06-29', 2, 3, N'Monday', N'دو شنبه', 20, 29, 263, 184, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100921, CONVERT(DATE, '20100921'), N'1389-06-30', 3, 4, N'Tuesday', N'سه شنبه', 21, 30, 264, 185, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100922, CONVERT(DATE, '20100922'), N'1389-06-31', 4, 5, N'Wednesday', N'چهار شنبه', 22, 31, 265, 186, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2010, 1389, 2, 1),
            (20100923, CONVERT(DATE, '20100923'), N'1389-07-01', 5, 6, N'Thursday', N'پنج شنبه', 23, 1, 266, 187, 38, 27, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100924, CONVERT(DATE, '20100924'), N'1389-07-02', 6, 7, N'Friday', N'جمعه', 24, 2, 267, 188, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100925, CONVERT(DATE, '20100925'), N'1389-07-03', 7, 1, N'Saturday', N'شنبه', 25, 3, 268, 189, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100926, CONVERT(DATE, '20100926'), N'1389-07-04', 1, 2, N'Sunday', N'یک شنبه', 26, 4, 269, 190, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100927, CONVERT(DATE, '20100927'), N'1389-07-05', 2, 3, N'Monday', N'دو شنبه', 27, 5, 270, 191, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100928, CONVERT(DATE, '20100928'), N'1389-07-06', 3, 4, N'Tuesday', N'سه شنبه', 28, 6, 271, 192, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100929, CONVERT(DATE, '20100929'), N'1389-07-07', 4, 5, N'Wednesday', N'چهار شنبه', 29, 7, 272, 193, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20100930, CONVERT(DATE, '20100930'), N'1389-07-08', 5, 6, N'Thursday', N'پنج شنبه', 30, 8, 273, 194, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2010, 1389, 2, 2),
            (20101001, CONVERT(DATE, '20101001'), N'1389-07-09', 6, 7, N'Friday', N'جمعه', 1, 9, 274, 195, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101002, CONVERT(DATE, '20101002'), N'1389-07-10', 7, 1, N'Saturday', N'شنبه', 2, 10, 275, 196, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101003, CONVERT(DATE, '20101003'), N'1389-07-11', 1, 2, N'Sunday', N'یک شنبه', 3, 11, 276, 197, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101004, CONVERT(DATE, '20101004'), N'1389-07-12', 2, 3, N'Monday', N'دو شنبه', 4, 12, 277, 198, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101005, CONVERT(DATE, '20101005'), N'1389-07-13', 3, 4, N'Tuesday', N'سه شنبه', 5, 13, 278, 199, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101006, CONVERT(DATE, '20101006'), N'1389-07-14', 4, 5, N'Wednesday', N'چهار شنبه', 6, 14, 279, 200, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101007, CONVERT(DATE, '20101007'), N'1389-07-15', 5, 6, N'Thursday', N'پنج شنبه', 7, 15, 280, 201, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101008, CONVERT(DATE, '20101008'), N'1389-07-16', 6, 7, N'Friday', N'جمعه', 8, 16, 281, 202, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101009, CONVERT(DATE, '20101009'), N'1389-07-17', 7, 1, N'Saturday', N'شنبه', 9, 17, 282, 203, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101010, CONVERT(DATE, '20101010'), N'1389-07-18', 1, 2, N'Sunday', N'یک شنبه', 10, 18, 283, 204, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101011, CONVERT(DATE, '20101011'), N'1389-07-19', 2, 3, N'Monday', N'دو شنبه', 11, 19, 284, 205, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101012, CONVERT(DATE, '20101012'), N'1389-07-20', 3, 4, N'Tuesday', N'سه شنبه', 12, 20, 285, 206, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101013, CONVERT(DATE, '20101013'), N'1389-07-21', 4, 5, N'Wednesday', N'چهار شنبه', 13, 21, 286, 207, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101014, CONVERT(DATE, '20101014'), N'1389-07-22', 5, 6, N'Thursday', N'پنج شنبه', 14, 22, 287, 208, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101015, CONVERT(DATE, '20101015'), N'1389-07-23', 6, 7, N'Friday', N'جمعه', 15, 23, 288, 209, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101016, CONVERT(DATE, '20101016'), N'1389-07-24', 7, 1, N'Saturday', N'شنبه', 16, 24, 289, 210, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101017, CONVERT(DATE, '20101017'), N'1389-07-25', 1, 2, N'Sunday', N'یک شنبه', 17, 25, 290, 211, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101018, CONVERT(DATE, '20101018'), N'1389-07-26', 2, 3, N'Monday', N'دو شنبه', 18, 26, 291, 212, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101019, CONVERT(DATE, '20101019'), N'1389-07-27', 3, 4, N'Tuesday', N'سه شنبه', 19, 27, 292, 213, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101020, CONVERT(DATE, '20101020'), N'1389-07-28', 4, 5, N'Wednesday', N'چهار شنبه', 20, 28, 293, 214, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101021, CONVERT(DATE, '20101021'), N'1389-07-29', 5, 6, N'Thursday', N'پنج شنبه', 21, 29, 294, 215, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101022, CONVERT(DATE, '20101022'), N'1389-07-30', 6, 7, N'Friday', N'جمعه', 22, 30, 295, 216, 43, 32, N'October', N'مهر', 10, 7, 4, 3, 2010, 1389, 2, 2),
            (20101023, CONVERT(DATE, '20101023'), N'1389-08-01', 7, 1, N'Saturday', N'شنبه', 23, 1, 296, 217, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101024, CONVERT(DATE, '20101024'), N'1389-08-02', 1, 2, N'Sunday', N'یک شنبه', 24, 2, 297, 218, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101025, CONVERT(DATE, '20101025'), N'1389-08-03', 2, 3, N'Monday', N'دو شنبه', 25, 3, 298, 219, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101026, CONVERT(DATE, '20101026'), N'1389-08-04', 3, 4, N'Tuesday', N'سه شنبه', 26, 4, 299, 220, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101027, CONVERT(DATE, '20101027'), N'1389-08-05', 4, 5, N'Wednesday', N'چهار شنبه', 27, 5, 300, 221, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101028, CONVERT(DATE, '20101028'), N'1389-08-06', 5, 6, N'Thursday', N'پنج شنبه', 28, 6, 301, 222, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101029, CONVERT(DATE, '20101029'), N'1389-08-07', 6, 7, N'Friday', N'جمعه', 29, 7, 302, 223, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101030, CONVERT(DATE, '20101030'), N'1389-08-08', 7, 1, N'Saturday', N'شنبه', 30, 8, 303, 224, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101031, CONVERT(DATE, '20101031'), N'1389-08-09', 1, 2, N'Sunday', N'یک شنبه', 31, 9, 304, 225, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2010, 1389, 2, 2),
            (20101101, CONVERT(DATE, '20101101'), N'1389-08-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 305, 226, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101102, CONVERT(DATE, '20101102'), N'1389-08-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 306, 227, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101103, CONVERT(DATE, '20101103'), N'1389-08-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 307, 228, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101104, CONVERT(DATE, '20101104'), N'1389-08-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 308, 229, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101105, CONVERT(DATE, '20101105'), N'1389-08-14', 6, 7, N'Friday', N'جمعه', 5, 14, 309, 230, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101106, CONVERT(DATE, '20101106'), N'1389-08-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 310, 231, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101107, CONVERT(DATE, '20101107'), N'1389-08-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 311, 232, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101108, CONVERT(DATE, '20101108'), N'1389-08-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 312, 233, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101109, CONVERT(DATE, '20101109'), N'1389-08-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 313, 234, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101110, CONVERT(DATE, '20101110'), N'1389-08-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 314, 235, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101111, CONVERT(DATE, '20101111'), N'1389-08-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 315, 236, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101112, CONVERT(DATE, '20101112'), N'1389-08-21', 6, 7, N'Friday', N'جمعه', 12, 21, 316, 237, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101113, CONVERT(DATE, '20101113'), N'1389-08-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 317, 238, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101114, CONVERT(DATE, '20101114'), N'1389-08-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 318, 239, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101115, CONVERT(DATE, '20101115'), N'1389-08-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 319, 240, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101116, CONVERT(DATE, '20101116'), N'1389-08-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 320, 241, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101117, CONVERT(DATE, '20101117'), N'1389-08-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 321, 242, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101118, CONVERT(DATE, '20101118'), N'1389-08-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 322, 243, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101119, CONVERT(DATE, '20101119'), N'1389-08-28', 6, 7, N'Friday', N'جمعه', 19, 28, 323, 244, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101120, CONVERT(DATE, '20101120'), N'1389-08-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 324, 245, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101121, CONVERT(DATE, '20101121'), N'1389-08-30', 1, 2, N'Sunday', N'یک شنبه', 21, 30, 325, 246, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2010, 1389, 2, 2),
            (20101122, CONVERT(DATE, '20101122'), N'1389-09-01', 2, 3, N'Monday', N'دو شنبه', 22, 1, 326, 247, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101123, CONVERT(DATE, '20101123'), N'1389-09-02', 3, 4, N'Tuesday', N'سه شنبه', 23, 2, 327, 248, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101124, CONVERT(DATE, '20101124'), N'1389-09-03', 4, 5, N'Wednesday', N'چهار شنبه', 24, 3, 328, 249, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101125, CONVERT(DATE, '20101125'), N'1389-09-04', 5, 6, N'Thursday', N'پنج شنبه', 25, 4, 329, 250, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101126, CONVERT(DATE, '20101126'), N'1389-09-05', 6, 7, N'Friday', N'جمعه', 26, 5, 330, 251, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101127, CONVERT(DATE, '20101127'), N'1389-09-06', 7, 1, N'Saturday', N'شنبه', 27, 6, 331, 252, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101128, CONVERT(DATE, '20101128'), N'1389-09-07', 1, 2, N'Sunday', N'یک شنبه', 28, 7, 332, 253, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101129, CONVERT(DATE, '20101129'), N'1389-09-08', 2, 3, N'Monday', N'دو شنبه', 29, 8, 333, 254, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101130, CONVERT(DATE, '20101130'), N'1389-09-09', 3, 4, N'Tuesday', N'سه شنبه', 30, 9, 334, 255, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2010, 1389, 2, 2),
            (20101201, CONVERT(DATE, '20101201'), N'1389-09-10', 4, 5, N'Wednesday', N'چهار شنبه', 1, 10, 335, 256, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101202, CONVERT(DATE, '20101202'), N'1389-09-11', 5, 6, N'Thursday', N'پنج شنبه', 2, 11, 336, 257, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101203, CONVERT(DATE, '20101203'), N'1389-09-12', 6, 7, N'Friday', N'جمعه', 3, 12, 337, 258, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101204, CONVERT(DATE, '20101204'), N'1389-09-13', 7, 1, N'Saturday', N'شنبه', 4, 13, 338, 259, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101205, CONVERT(DATE, '20101205'), N'1389-09-14', 1, 2, N'Sunday', N'یک شنبه', 5, 14, 339, 260, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101206, CONVERT(DATE, '20101206'), N'1389-09-15', 2, 3, N'Monday', N'دو شنبه', 6, 15, 340, 261, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101207, CONVERT(DATE, '20101207'), N'1389-09-16', 3, 4, N'Tuesday', N'سه شنبه', 7, 16, 341, 262, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101208, CONVERT(DATE, '20101208'), N'1389-09-17', 4, 5, N'Wednesday', N'چهار شنبه', 8, 17, 342, 263, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101209, CONVERT(DATE, '20101209'), N'1389-09-18', 5, 6, N'Thursday', N'پنج شنبه', 9, 18, 343, 264, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101210, CONVERT(DATE, '20101210'), N'1389-09-19', 6, 7, N'Friday', N'جمعه', 10, 19, 344, 265, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101211, CONVERT(DATE, '20101211'), N'1389-09-20', 7, 1, N'Saturday', N'شنبه', 11, 20, 345, 266, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101212, CONVERT(DATE, '20101212'), N'1389-09-21', 1, 2, N'Sunday', N'یک شنبه', 12, 21, 346, 267, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101213, CONVERT(DATE, '20101213'), N'1389-09-22', 2, 3, N'Monday', N'دو شنبه', 13, 22, 347, 268, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101214, CONVERT(DATE, '20101214'), N'1389-09-23', 3, 4, N'Tuesday', N'سه شنبه', 14, 23, 348, 269, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101215, CONVERT(DATE, '20101215'), N'1389-09-24', 4, 5, N'Wednesday', N'چهار شنبه', 15, 24, 349, 270, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101216, CONVERT(DATE, '20101216'), N'1389-09-25', 5, 6, N'Thursday', N'پنج شنبه', 16, 25, 350, 271, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101217, CONVERT(DATE, '20101217'), N'1389-09-26', 6, 7, N'Friday', N'جمعه', 17, 26, 351, 272, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101218, CONVERT(DATE, '20101218'), N'1389-09-27', 7, 1, N'Saturday', N'شنبه', 18, 27, 352, 273, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101219, CONVERT(DATE, '20101219'), N'1389-09-28', 1, 2, N'Sunday', N'یک شنبه', 19, 28, 353, 274, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101220, CONVERT(DATE, '20101220'), N'1389-09-29', 2, 3, N'Monday', N'دو شنبه', 20, 29, 354, 275, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101221, CONVERT(DATE, '20101221'), N'1389-09-30', 3, 4, N'Tuesday', N'سه شنبه', 21, 30, 355, 276, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2010, 1389, 2, 2),
            (20101222, CONVERT(DATE, '20101222'), N'1389-10-01', 4, 5, N'Wednesday', N'چهار شنبه', 22, 1, 356, 277, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101223, CONVERT(DATE, '20101223'), N'1389-10-02', 5, 6, N'Thursday', N'پنج شنبه', 23, 2, 357, 278, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101224, CONVERT(DATE, '20101224'), N'1389-10-03', 6, 7, N'Friday', N'جمعه', 24, 3, 358, 279, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101225, CONVERT(DATE, '20101225'), N'1389-10-04', 7, 1, N'Saturday', N'شنبه', 25, 4, 359, 280, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101226, CONVERT(DATE, '20101226'), N'1389-10-05', 1, 2, N'Sunday', N'یک شنبه', 26, 5, 360, 281, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101227, CONVERT(DATE, '20101227'), N'1389-10-06', 2, 3, N'Monday', N'دو شنبه', 27, 6, 361, 282, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101228, CONVERT(DATE, '20101228'), N'1389-10-07', 3, 4, N'Tuesday', N'سه شنبه', 28, 7, 362, 283, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101229, CONVERT(DATE, '20101229'), N'1389-10-08', 4, 5, N'Wednesday', N'چهار شنبه', 29, 8, 363, 284, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101230, CONVERT(DATE, '20101230'), N'1389-10-09', 5, 6, N'Thursday', N'پنج شنبه', 30, 9, 364, 285, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20101231, CONVERT(DATE, '20101231'), N'1389-10-10', 6, 7, N'Friday', N'جمعه', 31, 10, 365, 286, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2010, 1389, 2, 2),
            (20110101, CONVERT(DATE, '20110101'), N'1389-10-11', 7, 1, N'Saturday', N'شنبه', 1, 11, 1, 287, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110102, CONVERT(DATE, '20110102'), N'1389-10-12', 1, 2, N'Sunday', N'یک شنبه', 2, 12, 2, 288, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110103, CONVERT(DATE, '20110103'), N'1389-10-13', 2, 3, N'Monday', N'دو شنبه', 3, 13, 3, 289, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110104, CONVERT(DATE, '20110104'), N'1389-10-14', 3, 4, N'Tuesday', N'سه شنبه', 4, 14, 4, 290, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110105, CONVERT(DATE, '20110105'), N'1389-10-15', 4, 5, N'Wednesday', N'چهار شنبه', 5, 15, 5, 291, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110106, CONVERT(DATE, '20110106'), N'1389-10-16', 5, 6, N'Thursday', N'پنج شنبه', 6, 16, 6, 292, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110107, CONVERT(DATE, '20110107'), N'1389-10-17', 6, 7, N'Friday', N'جمعه', 7, 17, 7, 293, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110108, CONVERT(DATE, '20110108'), N'1389-10-18', 7, 1, N'Saturday', N'شنبه', 8, 18, 8, 294, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110109, CONVERT(DATE, '20110109'), N'1389-10-19', 1, 2, N'Sunday', N'یک شنبه', 9, 19, 9, 295, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110110, CONVERT(DATE, '20110110'), N'1389-10-20', 2, 3, N'Monday', N'دو شنبه', 10, 20, 10, 296, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110111, CONVERT(DATE, '20110111'), N'1389-10-21', 3, 4, N'Tuesday', N'سه شنبه', 11, 21, 11, 297, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110112, CONVERT(DATE, '20110112'), N'1389-10-22', 4, 5, N'Wednesday', N'چهار شنبه', 12, 22, 12, 298, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110113, CONVERT(DATE, '20110113'), N'1389-10-23', 5, 6, N'Thursday', N'پنج شنبه', 13, 23, 13, 299, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110114, CONVERT(DATE, '20110114'), N'1389-10-24', 6, 7, N'Friday', N'جمعه', 14, 24, 14, 300, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110115, CONVERT(DATE, '20110115'), N'1389-10-25', 7, 1, N'Saturday', N'شنبه', 15, 25, 15, 301, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110116, CONVERT(DATE, '20110116'), N'1389-10-26', 1, 2, N'Sunday', N'یک شنبه', 16, 26, 16, 302, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110117, CONVERT(DATE, '20110117'), N'1389-10-27', 2, 3, N'Monday', N'دو شنبه', 17, 27, 17, 303, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110118, CONVERT(DATE, '20110118'), N'1389-10-28', 3, 4, N'Tuesday', N'سه شنبه', 18, 28, 18, 304, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110119, CONVERT(DATE, '20110119'), N'1389-10-29', 4, 5, N'Wednesday', N'چهار شنبه', 19, 29, 19, 305, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110120, CONVERT(DATE, '20110120'), N'1389-10-30', 5, 6, N'Thursday', N'پنج شنبه', 20, 30, 20, 306, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2011, 1389, 1, 2),
            (20110121, CONVERT(DATE, '20110121'), N'1389-11-01', 6, 7, N'Friday', N'جمعه', 21, 1, 21, 307, 3, 44, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110122, CONVERT(DATE, '20110122'), N'1389-11-02', 7, 1, N'Saturday', N'شنبه', 22, 2, 22, 308, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110123, CONVERT(DATE, '20110123'), N'1389-11-03', 1, 2, N'Sunday', N'یک شنبه', 23, 3, 23, 309, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110124, CONVERT(DATE, '20110124'), N'1389-11-04', 2, 3, N'Monday', N'دو شنبه', 24, 4, 24, 310, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110125, CONVERT(DATE, '20110125'), N'1389-11-05', 3, 4, N'Tuesday', N'سه شنبه', 25, 5, 25, 311, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110126, CONVERT(DATE, '20110126'), N'1389-11-06', 4, 5, N'Wednesday', N'چهار شنبه', 26, 6, 26, 312, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110127, CONVERT(DATE, '20110127'), N'1389-11-07', 5, 6, N'Thursday', N'پنج شنبه', 27, 7, 27, 313, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110128, CONVERT(DATE, '20110128'), N'1389-11-08', 6, 7, N'Friday', N'جمعه', 28, 8, 28, 314, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110129, CONVERT(DATE, '20110129'), N'1389-11-09', 7, 1, N'Saturday', N'شنبه', 29, 9, 29, 315, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110130, CONVERT(DATE, '20110130'), N'1389-11-10', 1, 2, N'Sunday', N'یک شنبه', 30, 10, 30, 316, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110131, CONVERT(DATE, '20110131'), N'1389-11-11', 2, 3, N'Monday', N'دو شنبه', 31, 11, 31, 317, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2011, 1389, 1, 2),
            (20110201, CONVERT(DATE, '20110201'), N'1389-11-12', 3, 4, N'Tuesday', N'سه شنبه', 1, 12, 32, 318, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110202, CONVERT(DATE, '20110202'), N'1389-11-13', 4, 5, N'Wednesday', N'چهار شنبه', 2, 13, 33, 319, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110203, CONVERT(DATE, '20110203'), N'1389-11-14', 5, 6, N'Thursday', N'پنج شنبه', 3, 14, 34, 320, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110204, CONVERT(DATE, '20110204'), N'1389-11-15', 6, 7, N'Friday', N'جمعه', 4, 15, 35, 321, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110205, CONVERT(DATE, '20110205'), N'1389-11-16', 7, 1, N'Saturday', N'شنبه', 5, 16, 36, 322, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110206, CONVERT(DATE, '20110206'), N'1389-11-17', 1, 2, N'Sunday', N'یک شنبه', 6, 17, 37, 323, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110207, CONVERT(DATE, '20110207'), N'1389-11-18', 2, 3, N'Monday', N'دو شنبه', 7, 18, 38, 324, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110208, CONVERT(DATE, '20110208'), N'1389-11-19', 3, 4, N'Tuesday', N'سه شنبه', 8, 19, 39, 325, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110209, CONVERT(DATE, '20110209'), N'1389-11-20', 4, 5, N'Wednesday', N'چهار شنبه', 9, 20, 40, 326, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110210, CONVERT(DATE, '20110210'), N'1389-11-21', 5, 6, N'Thursday', N'پنج شنبه', 10, 21, 41, 327, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110211, CONVERT(DATE, '20110211'), N'1389-11-22', 6, 7, N'Friday', N'جمعه', 11, 22, 42, 328, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110212, CONVERT(DATE, '20110212'), N'1389-11-23', 7, 1, N'Saturday', N'شنبه', 12, 23, 43, 329, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110213, CONVERT(DATE, '20110213'), N'1389-11-24', 1, 2, N'Sunday', N'یک شنبه', 13, 24, 44, 330, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110214, CONVERT(DATE, '20110214'), N'1389-11-25', 2, 3, N'Monday', N'دو شنبه', 14, 25, 45, 331, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110215, CONVERT(DATE, '20110215'), N'1389-11-26', 3, 4, N'Tuesday', N'سه شنبه', 15, 26, 46, 332, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110216, CONVERT(DATE, '20110216'), N'1389-11-27', 4, 5, N'Wednesday', N'چهار شنبه', 16, 27, 47, 333, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110217, CONVERT(DATE, '20110217'), N'1389-11-28', 5, 6, N'Thursday', N'پنج شنبه', 17, 28, 48, 334, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110218, CONVERT(DATE, '20110218'), N'1389-11-29', 6, 7, N'Friday', N'جمعه', 18, 29, 49, 335, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110219, CONVERT(DATE, '20110219'), N'1389-11-30', 7, 1, N'Saturday', N'شنبه', 19, 30, 50, 336, 8, 49, N'February', N'بهمن', 2, 11, 1, 4, 2011, 1389, 1, 2),
            (20110220, CONVERT(DATE, '20110220'), N'1389-12-01', 1, 2, N'Sunday', N'یک شنبه', 20, 1, 51, 337, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110221, CONVERT(DATE, '20110221'), N'1389-12-02', 2, 3, N'Monday', N'دو شنبه', 21, 2, 52, 338, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110222, CONVERT(DATE, '20110222'), N'1389-12-03', 3, 4, N'Tuesday', N'سه شنبه', 22, 3, 53, 339, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110223, CONVERT(DATE, '20110223'), N'1389-12-04', 4, 5, N'Wednesday', N'چهار شنبه', 23, 4, 54, 340, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110224, CONVERT(DATE, '20110224'), N'1389-12-05', 5, 6, N'Thursday', N'پنج شنبه', 24, 5, 55, 341, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110225, CONVERT(DATE, '20110225'), N'1389-12-06', 6, 7, N'Friday', N'جمعه', 25, 6, 56, 342, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110226, CONVERT(DATE, '20110226'), N'1389-12-07', 7, 1, N'Saturday', N'شنبه', 26, 7, 57, 343, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110227, CONVERT(DATE, '20110227'), N'1389-12-08', 1, 2, N'Sunday', N'یک شنبه', 27, 8, 58, 344, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110228, CONVERT(DATE, '20110228'), N'1389-12-09', 2, 3, N'Monday', N'دو شنبه', 28, 9, 59, 345, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2011, 1389, 1, 2),
            (20110301, CONVERT(DATE, '20110301'), N'1389-12-10', 3, 4, N'Tuesday', N'سه شنبه', 1, 10, 60, 346, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110302, CONVERT(DATE, '20110302'), N'1389-12-11', 4, 5, N'Wednesday', N'چهار شنبه', 2, 11, 61, 347, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110303, CONVERT(DATE, '20110303'), N'1389-12-12', 5, 6, N'Thursday', N'پنج شنبه', 3, 12, 62, 348, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110304, CONVERT(DATE, '20110304'), N'1389-12-13', 6, 7, N'Friday', N'جمعه', 4, 13, 63, 349, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110305, CONVERT(DATE, '20110305'), N'1389-12-14', 7, 1, N'Saturday', N'شنبه', 5, 14, 64, 350, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110306, CONVERT(DATE, '20110306'), N'1389-12-15', 1, 2, N'Sunday', N'یک شنبه', 6, 15, 65, 351, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110307, CONVERT(DATE, '20110307'), N'1389-12-16', 2, 3, N'Monday', N'دو شنبه', 7, 16, 66, 352, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110308, CONVERT(DATE, '20110308'), N'1389-12-17', 3, 4, N'Tuesday', N'سه شنبه', 8, 17, 67, 353, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110309, CONVERT(DATE, '20110309'), N'1389-12-18', 4, 5, N'Wednesday', N'چهار شنبه', 9, 18, 68, 354, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110310, CONVERT(DATE, '20110310'), N'1389-12-19', 5, 6, N'Thursday', N'پنج شنبه', 10, 19, 69, 355, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110311, CONVERT(DATE, '20110311'), N'1389-12-20', 6, 7, N'Friday', N'جمعه', 11, 20, 70, 356, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110312, CONVERT(DATE, '20110312'), N'1389-12-21', 7, 1, N'Saturday', N'شنبه', 12, 21, 71, 357, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110313, CONVERT(DATE, '20110313'), N'1389-12-22', 1, 2, N'Sunday', N'یک شنبه', 13, 22, 72, 358, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110314, CONVERT(DATE, '20110314'), N'1389-12-23', 2, 3, N'Monday', N'دو شنبه', 14, 23, 73, 359, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110315, CONVERT(DATE, '20110315'), N'1389-12-24', 3, 4, N'Tuesday', N'سه شنبه', 15, 24, 74, 360, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110316, CONVERT(DATE, '20110316'), N'1389-12-25', 4, 5, N'Wednesday', N'چهار شنبه', 16, 25, 75, 361, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110317, CONVERT(DATE, '20110317'), N'1389-12-26', 5, 6, N'Thursday', N'پنج شنبه', 17, 26, 76, 362, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110318, CONVERT(DATE, '20110318'), N'1389-12-27', 6, 7, N'Friday', N'جمعه', 18, 27, 77, 363, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110319, CONVERT(DATE, '20110319'), N'1389-12-28', 7, 1, N'Saturday', N'شنبه', 19, 28, 78, 364, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110320, CONVERT(DATE, '20110320'), N'1389-12-29', 1, 2, N'Sunday', N'یک شنبه', 20, 29, 79, 365, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2011, 1389, 1, 2),
            (20110321, CONVERT(DATE, '20110321'), N'1390-01-01', 2, 3, N'Monday', N'دو شنبه', 21, 1, 80, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110322, CONVERT(DATE, '20110322'), N'1390-01-02', 3, 4, N'Tuesday', N'سه شنبه', 22, 2, 81, 2, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110323, CONVERT(DATE, '20110323'), N'1390-01-03', 4, 5, N'Wednesday', N'چهار شنبه', 23, 3, 82, 3, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110324, CONVERT(DATE, '20110324'), N'1390-01-04', 5, 6, N'Thursday', N'پنج شنبه', 24, 4, 83, 4, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110325, CONVERT(DATE, '20110325'), N'1390-01-05', 6, 7, N'Friday', N'جمعه', 25, 5, 84, 5, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110326, CONVERT(DATE, '20110326'), N'1390-01-06', 7, 1, N'Saturday', N'شنبه', 26, 6, 85, 6, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110327, CONVERT(DATE, '20110327'), N'1390-01-07', 1, 2, N'Sunday', N'یک شنبه', 27, 7, 86, 7, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110328, CONVERT(DATE, '20110328'), N'1390-01-08', 2, 3, N'Monday', N'دو شنبه', 28, 8, 87, 8, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110329, CONVERT(DATE, '20110329'), N'1390-01-09', 3, 4, N'Tuesday', N'سه شنبه', 29, 9, 88, 9, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110330, CONVERT(DATE, '20110330'), N'1390-01-10', 4, 5, N'Wednesday', N'چهار شنبه', 30, 10, 89, 10, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110331, CONVERT(DATE, '20110331'), N'1390-01-11', 5, 6, N'Thursday', N'پنج شنبه', 31, 11, 90, 11, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2011, 1390, 1, 1),
            (20110401, CONVERT(DATE, '20110401'), N'1390-01-12', 6, 7, N'Friday', N'جمعه', 1, 12, 91, 12, 13, 2, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110402, CONVERT(DATE, '20110402'), N'1390-01-13', 7, 1, N'Saturday', N'شنبه', 2, 13, 92, 13, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110403, CONVERT(DATE, '20110403'), N'1390-01-14', 1, 2, N'Sunday', N'یک شنبه', 3, 14, 93, 14, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110404, CONVERT(DATE, '20110404'), N'1390-01-15', 2, 3, N'Monday', N'دو شنبه', 4, 15, 94, 15, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110405, CONVERT(DATE, '20110405'), N'1390-01-16', 3, 4, N'Tuesday', N'سه شنبه', 5, 16, 95, 16, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110406, CONVERT(DATE, '20110406'), N'1390-01-17', 4, 5, N'Wednesday', N'چهار شنبه', 6, 17, 96, 17, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110407, CONVERT(DATE, '20110407'), N'1390-01-18', 5, 6, N'Thursday', N'پنج شنبه', 7, 18, 97, 18, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110408, CONVERT(DATE, '20110408'), N'1390-01-19', 6, 7, N'Friday', N'جمعه', 8, 19, 98, 19, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_date_sample', N'dim_date', @step_rows, N'Inserted rows from Dim_Date.txt chunk into temp table #dim_date_sample.';

        INSERT INTO #dim_date_sample (
            TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey, DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek, DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear, WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName, MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter, CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
        )
        VALUES
            (20110409, CONVERT(DATE, '20110409'), N'1390-01-20', 7, 1, N'Saturday', N'شنبه', 9, 20, 99, 20, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110410, CONVERT(DATE, '20110410'), N'1390-01-21', 1, 2, N'Sunday', N'یک شنبه', 10, 21, 100, 21, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110411, CONVERT(DATE, '20110411'), N'1390-01-22', 2, 3, N'Monday', N'دو شنبه', 11, 22, 101, 22, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110412, CONVERT(DATE, '20110412'), N'1390-01-23', 3, 4, N'Tuesday', N'سه شنبه', 12, 23, 102, 23, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110413, CONVERT(DATE, '20110413'), N'1390-01-24', 4, 5, N'Wednesday', N'چهار شنبه', 13, 24, 103, 24, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110414, CONVERT(DATE, '20110414'), N'1390-01-25', 5, 6, N'Thursday', N'پنج شنبه', 14, 25, 104, 25, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110415, CONVERT(DATE, '20110415'), N'1390-01-26', 6, 7, N'Friday', N'جمعه', 15, 26, 105, 26, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110416, CONVERT(DATE, '20110416'), N'1390-01-27', 7, 1, N'Saturday', N'شنبه', 16, 27, 106, 27, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110417, CONVERT(DATE, '20110417'), N'1390-01-28', 1, 2, N'Sunday', N'یک شنبه', 17, 28, 107, 28, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110418, CONVERT(DATE, '20110418'), N'1390-01-29', 2, 3, N'Monday', N'دو شنبه', 18, 29, 108, 29, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110419, CONVERT(DATE, '20110419'), N'1390-01-30', 3, 4, N'Tuesday', N'سه شنبه', 19, 30, 109, 30, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110420, CONVERT(DATE, '20110420'), N'1390-01-31', 4, 5, N'Wednesday', N'چهار شنبه', 20, 31, 110, 31, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2011, 1390, 1, 1),
            (20110421, CONVERT(DATE, '20110421'), N'1390-02-01', 5, 6, N'Thursday', N'پنج شنبه', 21, 1, 111, 32, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110422, CONVERT(DATE, '20110422'), N'1390-02-02', 6, 7, N'Friday', N'جمعه', 22, 2, 112, 33, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110423, CONVERT(DATE, '20110423'), N'1390-02-03', 7, 1, N'Saturday', N'شنبه', 23, 3, 113, 34, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110424, CONVERT(DATE, '20110424'), N'1390-02-04', 1, 2, N'Sunday', N'یک شنبه', 24, 4, 114, 35, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110425, CONVERT(DATE, '20110425'), N'1390-02-05', 2, 3, N'Monday', N'دو شنبه', 25, 5, 115, 36, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110426, CONVERT(DATE, '20110426'), N'1390-02-06', 3, 4, N'Tuesday', N'سه شنبه', 26, 6, 116, 37, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110427, CONVERT(DATE, '20110427'), N'1390-02-07', 4, 5, N'Wednesday', N'چهار شنبه', 27, 7, 117, 38, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110428, CONVERT(DATE, '20110428'), N'1390-02-08', 5, 6, N'Thursday', N'پنج شنبه', 28, 8, 118, 39, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110429, CONVERT(DATE, '20110429'), N'1390-02-09', 6, 7, N'Friday', N'جمعه', 29, 9, 119, 40, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110430, CONVERT(DATE, '20110430'), N'1390-02-10', 7, 1, N'Saturday', N'شنبه', 30, 10, 120, 41, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2011, 1390, 1, 1),
            (20110501, CONVERT(DATE, '20110501'), N'1390-02-11', 1, 2, N'Sunday', N'یک شنبه', 1, 11, 121, 42, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110502, CONVERT(DATE, '20110502'), N'1390-02-12', 2, 3, N'Monday', N'دو شنبه', 2, 12, 122, 43, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110503, CONVERT(DATE, '20110503'), N'1390-02-13', 3, 4, N'Tuesday', N'سه شنبه', 3, 13, 123, 44, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110504, CONVERT(DATE, '20110504'), N'1390-02-14', 4, 5, N'Wednesday', N'چهار شنبه', 4, 14, 124, 45, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110505, CONVERT(DATE, '20110505'), N'1390-02-15', 5, 6, N'Thursday', N'پنج شنبه', 5, 15, 125, 46, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110506, CONVERT(DATE, '20110506'), N'1390-02-16', 6, 7, N'Friday', N'جمعه', 6, 16, 126, 47, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110507, CONVERT(DATE, '20110507'), N'1390-02-17', 7, 1, N'Saturday', N'شنبه', 7, 17, 127, 48, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110508, CONVERT(DATE, '20110508'), N'1390-02-18', 1, 2, N'Sunday', N'یک شنبه', 8, 18, 128, 49, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110509, CONVERT(DATE, '20110509'), N'1390-02-19', 2, 3, N'Monday', N'دو شنبه', 9, 19, 129, 50, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110510, CONVERT(DATE, '20110510'), N'1390-02-20', 3, 4, N'Tuesday', N'سه شنبه', 10, 20, 130, 51, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110511, CONVERT(DATE, '20110511'), N'1390-02-21', 4, 5, N'Wednesday', N'چهار شنبه', 11, 21, 131, 52, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110512, CONVERT(DATE, '20110512'), N'1390-02-22', 5, 6, N'Thursday', N'پنج شنبه', 12, 22, 132, 53, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110513, CONVERT(DATE, '20110513'), N'1390-02-23', 6, 7, N'Friday', N'جمعه', 13, 23, 133, 54, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110514, CONVERT(DATE, '20110514'), N'1390-02-24', 7, 1, N'Saturday', N'شنبه', 14, 24, 134, 55, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110515, CONVERT(DATE, '20110515'), N'1390-02-25', 1, 2, N'Sunday', N'یک شنبه', 15, 25, 135, 56, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110516, CONVERT(DATE, '20110516'), N'1390-02-26', 2, 3, N'Monday', N'دو شنبه', 16, 26, 136, 57, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110517, CONVERT(DATE, '20110517'), N'1390-02-27', 3, 4, N'Tuesday', N'سه شنبه', 17, 27, 137, 58, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110518, CONVERT(DATE, '20110518'), N'1390-02-28', 4, 5, N'Wednesday', N'چهار شنبه', 18, 28, 138, 59, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110519, CONVERT(DATE, '20110519'), N'1390-02-29', 5, 6, N'Thursday', N'پنج شنبه', 19, 29, 139, 60, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110520, CONVERT(DATE, '20110520'), N'1390-02-30', 6, 7, N'Friday', N'جمعه', 20, 30, 140, 61, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110521, CONVERT(DATE, '20110521'), N'1390-02-31', 7, 1, N'Saturday', N'شنبه', 21, 31, 141, 62, 21, 10, N'May', N'اردیبهشت', 5, 2, 2, 1, 2011, 1390, 1, 1),
            (20110522, CONVERT(DATE, '20110522'), N'1390-03-01', 1, 2, N'Sunday', N'یک شنبه', 22, 1, 142, 63, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110523, CONVERT(DATE, '20110523'), N'1390-03-02', 2, 3, N'Monday', N'دو شنبه', 23, 2, 143, 64, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110524, CONVERT(DATE, '20110524'), N'1390-03-03', 3, 4, N'Tuesday', N'سه شنبه', 24, 3, 144, 65, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110525, CONVERT(DATE, '20110525'), N'1390-03-04', 4, 5, N'Wednesday', N'چهار شنبه', 25, 4, 145, 66, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110526, CONVERT(DATE, '20110526'), N'1390-03-05', 5, 6, N'Thursday', N'پنج شنبه', 26, 5, 146, 67, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110527, CONVERT(DATE, '20110527'), N'1390-03-06', 6, 7, N'Friday', N'جمعه', 27, 6, 147, 68, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110528, CONVERT(DATE, '20110528'), N'1390-03-07', 7, 1, N'Saturday', N'شنبه', 28, 7, 148, 69, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110529, CONVERT(DATE, '20110529'), N'1390-03-08', 1, 2, N'Sunday', N'یک شنبه', 29, 8, 149, 70, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110530, CONVERT(DATE, '20110530'), N'1390-03-09', 2, 3, N'Monday', N'دو شنبه', 30, 9, 150, 71, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110531, CONVERT(DATE, '20110531'), N'1390-03-10', 3, 4, N'Tuesday', N'سه شنبه', 31, 10, 151, 72, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2011, 1390, 1, 1),
            (20110601, CONVERT(DATE, '20110601'), N'1390-03-11', 4, 5, N'Wednesday', N'چهار شنبه', 1, 11, 152, 73, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110602, CONVERT(DATE, '20110602'), N'1390-03-12', 5, 6, N'Thursday', N'پنج شنبه', 2, 12, 153, 74, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110603, CONVERT(DATE, '20110603'), N'1390-03-13', 6, 7, N'Friday', N'جمعه', 3, 13, 154, 75, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110604, CONVERT(DATE, '20110604'), N'1390-03-14', 7, 1, N'Saturday', N'شنبه', 4, 14, 155, 76, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110605, CONVERT(DATE, '20110605'), N'1390-03-15', 1, 2, N'Sunday', N'یک شنبه', 5, 15, 156, 77, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110606, CONVERT(DATE, '20110606'), N'1390-03-16', 2, 3, N'Monday', N'دو شنبه', 6, 16, 157, 78, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110607, CONVERT(DATE, '20110607'), N'1390-03-17', 3, 4, N'Tuesday', N'سه شنبه', 7, 17, 158, 79, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110608, CONVERT(DATE, '20110608'), N'1390-03-18', 4, 5, N'Wednesday', N'چهار شنبه', 8, 18, 159, 80, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110609, CONVERT(DATE, '20110609'), N'1390-03-19', 5, 6, N'Thursday', N'پنج شنبه', 9, 19, 160, 81, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110610, CONVERT(DATE, '20110610'), N'1390-03-20', 6, 7, N'Friday', N'جمعه', 10, 20, 161, 82, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110611, CONVERT(DATE, '20110611'), N'1390-03-21', 7, 1, N'Saturday', N'شنبه', 11, 21, 162, 83, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110612, CONVERT(DATE, '20110612'), N'1390-03-22', 1, 2, N'Sunday', N'یک شنبه', 12, 22, 163, 84, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110613, CONVERT(DATE, '20110613'), N'1390-03-23', 2, 3, N'Monday', N'دو شنبه', 13, 23, 164, 85, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110614, CONVERT(DATE, '20110614'), N'1390-03-24', 3, 4, N'Tuesday', N'سه شنبه', 14, 24, 165, 86, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110615, CONVERT(DATE, '20110615'), N'1390-03-25', 4, 5, N'Wednesday', N'چهار شنبه', 15, 25, 166, 87, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110616, CONVERT(DATE, '20110616'), N'1390-03-26', 5, 6, N'Thursday', N'پنج شنبه', 16, 26, 167, 88, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110617, CONVERT(DATE, '20110617'), N'1390-03-27', 6, 7, N'Friday', N'جمعه', 17, 27, 168, 89, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110618, CONVERT(DATE, '20110618'), N'1390-03-28', 7, 1, N'Saturday', N'شنبه', 18, 28, 169, 90, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110619, CONVERT(DATE, '20110619'), N'1390-03-29', 1, 2, N'Sunday', N'یک شنبه', 19, 29, 170, 91, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110620, CONVERT(DATE, '20110620'), N'1390-03-30', 2, 3, N'Monday', N'دو شنبه', 20, 30, 171, 92, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110621, CONVERT(DATE, '20110621'), N'1390-03-31', 3, 4, N'Tuesday', N'سه شنبه', 21, 31, 172, 93, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2011, 1390, 1, 1),
            (20110622, CONVERT(DATE, '20110622'), N'1390-04-01', 4, 5, N'Wednesday', N'چهار شنبه', 22, 1, 173, 94, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110623, CONVERT(DATE, '20110623'), N'1390-04-02', 5, 6, N'Thursday', N'پنج شنبه', 23, 2, 174, 95, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110624, CONVERT(DATE, '20110624'), N'1390-04-03', 6, 7, N'Friday', N'جمعه', 24, 3, 175, 96, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110625, CONVERT(DATE, '20110625'), N'1390-04-04', 7, 1, N'Saturday', N'شنبه', 25, 4, 176, 97, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110626, CONVERT(DATE, '20110626'), N'1390-04-05', 1, 2, N'Sunday', N'یک شنبه', 26, 5, 177, 98, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110627, CONVERT(DATE, '20110627'), N'1390-04-06', 2, 3, N'Monday', N'دو شنبه', 27, 6, 178, 99, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110628, CONVERT(DATE, '20110628'), N'1390-04-07', 3, 4, N'Tuesday', N'سه شنبه', 28, 7, 179, 100, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110629, CONVERT(DATE, '20110629'), N'1390-04-08', 4, 5, N'Wednesday', N'چهار شنبه', 29, 8, 180, 101, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110630, CONVERT(DATE, '20110630'), N'1390-04-09', 5, 6, N'Thursday', N'پنج شنبه', 30, 9, 181, 102, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2011, 1390, 1, 1),
            (20110701, CONVERT(DATE, '20110701'), N'1390-04-10', 6, 7, N'Friday', N'جمعه', 1, 10, 182, 103, 26, 15, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110702, CONVERT(DATE, '20110702'), N'1390-04-11', 7, 1, N'Saturday', N'شنبه', 2, 11, 183, 104, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110703, CONVERT(DATE, '20110703'), N'1390-04-12', 1, 2, N'Sunday', N'یک شنبه', 3, 12, 184, 105, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110704, CONVERT(DATE, '20110704'), N'1390-04-13', 2, 3, N'Monday', N'دو شنبه', 4, 13, 185, 106, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110705, CONVERT(DATE, '20110705'), N'1390-04-14', 3, 4, N'Tuesday', N'سه شنبه', 5, 14, 186, 107, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110706, CONVERT(DATE, '20110706'), N'1390-04-15', 4, 5, N'Wednesday', N'چهار شنبه', 6, 15, 187, 108, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110707, CONVERT(DATE, '20110707'), N'1390-04-16', 5, 6, N'Thursday', N'پنج شنبه', 7, 16, 188, 109, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110708, CONVERT(DATE, '20110708'), N'1390-04-17', 6, 7, N'Friday', N'جمعه', 8, 17, 189, 110, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110709, CONVERT(DATE, '20110709'), N'1390-04-18', 7, 1, N'Saturday', N'شنبه', 9, 18, 190, 111, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110710, CONVERT(DATE, '20110710'), N'1390-04-19', 1, 2, N'Sunday', N'یک شنبه', 10, 19, 191, 112, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110711, CONVERT(DATE, '20110711'), N'1390-04-20', 2, 3, N'Monday', N'دو شنبه', 11, 20, 192, 113, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110712, CONVERT(DATE, '20110712'), N'1390-04-21', 3, 4, N'Tuesday', N'سه شنبه', 12, 21, 193, 114, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110713, CONVERT(DATE, '20110713'), N'1390-04-22', 4, 5, N'Wednesday', N'چهار شنبه', 13, 22, 194, 115, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110714, CONVERT(DATE, '20110714'), N'1390-04-23', 5, 6, N'Thursday', N'پنج شنبه', 14, 23, 195, 116, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110715, CONVERT(DATE, '20110715'), N'1390-04-24', 6, 7, N'Friday', N'جمعه', 15, 24, 196, 117, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110716, CONVERT(DATE, '20110716'), N'1390-04-25', 7, 1, N'Saturday', N'شنبه', 16, 25, 197, 118, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110717, CONVERT(DATE, '20110717'), N'1390-04-26', 1, 2, N'Sunday', N'یک شنبه', 17, 26, 198, 119, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110718, CONVERT(DATE, '20110718'), N'1390-04-27', 2, 3, N'Monday', N'دو شنبه', 18, 27, 199, 120, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110719, CONVERT(DATE, '20110719'), N'1390-04-28', 3, 4, N'Tuesday', N'سه شنبه', 19, 28, 200, 121, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110720, CONVERT(DATE, '20110720'), N'1390-04-29', 4, 5, N'Wednesday', N'چهار شنبه', 20, 29, 201, 122, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110721, CONVERT(DATE, '20110721'), N'1390-04-30', 5, 6, N'Thursday', N'پنج شنبه', 21, 30, 202, 123, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110722, CONVERT(DATE, '20110722'), N'1390-04-31', 6, 7, N'Friday', N'جمعه', 22, 31, 203, 124, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2011, 1390, 2, 1),
            (20110723, CONVERT(DATE, '20110723'), N'1390-05-01', 7, 1, N'Saturday', N'شنبه', 23, 1, 204, 125, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110724, CONVERT(DATE, '20110724'), N'1390-05-02', 1, 2, N'Sunday', N'یک شنبه', 24, 2, 205, 126, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110725, CONVERT(DATE, '20110725'), N'1390-05-03', 2, 3, N'Monday', N'دو شنبه', 25, 3, 206, 127, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110726, CONVERT(DATE, '20110726'), N'1390-05-04', 3, 4, N'Tuesday', N'سه شنبه', 26, 4, 207, 128, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110727, CONVERT(DATE, '20110727'), N'1390-05-05', 4, 5, N'Wednesday', N'چهار شنبه', 27, 5, 208, 129, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110728, CONVERT(DATE, '20110728'), N'1390-05-06', 5, 6, N'Thursday', N'پنج شنبه', 28, 6, 209, 130, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110729, CONVERT(DATE, '20110729'), N'1390-05-07', 6, 7, N'Friday', N'جمعه', 29, 7, 210, 131, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110730, CONVERT(DATE, '20110730'), N'1390-05-08', 7, 1, N'Saturday', N'شنبه', 30, 8, 211, 132, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110731, CONVERT(DATE, '20110731'), N'1390-05-09', 1, 2, N'Sunday', N'یک شنبه', 31, 9, 212, 133, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2011, 1390, 2, 1),
            (20110801, CONVERT(DATE, '20110801'), N'1390-05-10', 2, 3, N'Monday', N'دو شنبه', 1, 10, 213, 134, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110802, CONVERT(DATE, '20110802'), N'1390-05-11', 3, 4, N'Tuesday', N'سه شنبه', 2, 11, 214, 135, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110803, CONVERT(DATE, '20110803'), N'1390-05-12', 4, 5, N'Wednesday', N'چهار شنبه', 3, 12, 215, 136, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110804, CONVERT(DATE, '20110804'), N'1390-05-13', 5, 6, N'Thursday', N'پنج شنبه', 4, 13, 216, 137, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110805, CONVERT(DATE, '20110805'), N'1390-05-14', 6, 7, N'Friday', N'جمعه', 5, 14, 217, 138, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110806, CONVERT(DATE, '20110806'), N'1390-05-15', 7, 1, N'Saturday', N'شنبه', 6, 15, 218, 139, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110807, CONVERT(DATE, '20110807'), N'1390-05-16', 1, 2, N'Sunday', N'یک شنبه', 7, 16, 219, 140, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110808, CONVERT(DATE, '20110808'), N'1390-05-17', 2, 3, N'Monday', N'دو شنبه', 8, 17, 220, 141, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110809, CONVERT(DATE, '20110809'), N'1390-05-18', 3, 4, N'Tuesday', N'سه شنبه', 9, 18, 221, 142, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110810, CONVERT(DATE, '20110810'), N'1390-05-19', 4, 5, N'Wednesday', N'چهار شنبه', 10, 19, 222, 143, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110811, CONVERT(DATE, '20110811'), N'1390-05-20', 5, 6, N'Thursday', N'پنج شنبه', 11, 20, 223, 144, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110812, CONVERT(DATE, '20110812'), N'1390-05-21', 6, 7, N'Friday', N'جمعه', 12, 21, 224, 145, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110813, CONVERT(DATE, '20110813'), N'1390-05-22', 7, 1, N'Saturday', N'شنبه', 13, 22, 225, 146, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110814, CONVERT(DATE, '20110814'), N'1390-05-23', 1, 2, N'Sunday', N'یک شنبه', 14, 23, 226, 147, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110815, CONVERT(DATE, '20110815'), N'1390-05-24', 2, 3, N'Monday', N'دو شنبه', 15, 24, 227, 148, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110816, CONVERT(DATE, '20110816'), N'1390-05-25', 3, 4, N'Tuesday', N'سه شنبه', 16, 25, 228, 149, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110817, CONVERT(DATE, '20110817'), N'1390-05-26', 4, 5, N'Wednesday', N'چهار شنبه', 17, 26, 229, 150, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110818, CONVERT(DATE, '20110818'), N'1390-05-27', 5, 6, N'Thursday', N'پنج شنبه', 18, 27, 230, 151, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110819, CONVERT(DATE, '20110819'), N'1390-05-28', 6, 7, N'Friday', N'جمعه', 19, 28, 231, 152, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110820, CONVERT(DATE, '20110820'), N'1390-05-29', 7, 1, N'Saturday', N'شنبه', 20, 29, 232, 153, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110821, CONVERT(DATE, '20110821'), N'1390-05-30', 1, 2, N'Sunday', N'یک شنبه', 21, 30, 233, 154, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110822, CONVERT(DATE, '20110822'), N'1390-05-31', 2, 3, N'Monday', N'دو شنبه', 22, 31, 234, 155, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2011, 1390, 2, 1),
            (20110823, CONVERT(DATE, '20110823'), N'1390-06-01', 3, 4, N'Tuesday', N'سه شنبه', 23, 1, 235, 156, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110824, CONVERT(DATE, '20110824'), N'1390-06-02', 4, 5, N'Wednesday', N'چهار شنبه', 24, 2, 236, 157, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110825, CONVERT(DATE, '20110825'), N'1390-06-03', 5, 6, N'Thursday', N'پنج شنبه', 25, 3, 237, 158, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110826, CONVERT(DATE, '20110826'), N'1390-06-04', 6, 7, N'Friday', N'جمعه', 26, 4, 238, 159, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110827, CONVERT(DATE, '20110827'), N'1390-06-05', 7, 1, N'Saturday', N'شنبه', 27, 5, 239, 160, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110828, CONVERT(DATE, '20110828'), N'1390-06-06', 1, 2, N'Sunday', N'یک شنبه', 28, 6, 240, 161, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110829, CONVERT(DATE, '20110829'), N'1390-06-07', 2, 3, N'Monday', N'دو شنبه', 29, 7, 241, 162, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110830, CONVERT(DATE, '20110830'), N'1390-06-08', 3, 4, N'Tuesday', N'سه شنبه', 30, 8, 242, 163, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110831, CONVERT(DATE, '20110831'), N'1390-06-09', 4, 5, N'Wednesday', N'چهار شنبه', 31, 9, 243, 164, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2011, 1390, 2, 1),
            (20110901, CONVERT(DATE, '20110901'), N'1390-06-10', 5, 6, N'Thursday', N'پنج شنبه', 1, 10, 244, 165, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110902, CONVERT(DATE, '20110902'), N'1390-06-11', 6, 7, N'Friday', N'جمعه', 2, 11, 245, 166, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110903, CONVERT(DATE, '20110903'), N'1390-06-12', 7, 1, N'Saturday', N'شنبه', 3, 12, 246, 167, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110904, CONVERT(DATE, '20110904'), N'1390-06-13', 1, 2, N'Sunday', N'یک شنبه', 4, 13, 247, 168, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110905, CONVERT(DATE, '20110905'), N'1390-06-14', 2, 3, N'Monday', N'دو شنبه', 5, 14, 248, 169, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110906, CONVERT(DATE, '20110906'), N'1390-06-15', 3, 4, N'Tuesday', N'سه شنبه', 6, 15, 249, 170, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110907, CONVERT(DATE, '20110907'), N'1390-06-16', 4, 5, N'Wednesday', N'چهار شنبه', 7, 16, 250, 171, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110908, CONVERT(DATE, '20110908'), N'1390-06-17', 5, 6, N'Thursday', N'پنج شنبه', 8, 17, 251, 172, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110909, CONVERT(DATE, '20110909'), N'1390-06-18', 6, 7, N'Friday', N'جمعه', 9, 18, 252, 173, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110910, CONVERT(DATE, '20110910'), N'1390-06-19', 7, 1, N'Saturday', N'شنبه', 10, 19, 253, 174, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110911, CONVERT(DATE, '20110911'), N'1390-06-20', 1, 2, N'Sunday', N'یک شنبه', 11, 20, 254, 175, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110912, CONVERT(DATE, '20110912'), N'1390-06-21', 2, 3, N'Monday', N'دو شنبه', 12, 21, 255, 176, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110913, CONVERT(DATE, '20110913'), N'1390-06-22', 3, 4, N'Tuesday', N'سه شنبه', 13, 22, 256, 177, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110914, CONVERT(DATE, '20110914'), N'1390-06-23', 4, 5, N'Wednesday', N'چهار شنبه', 14, 23, 257, 178, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110915, CONVERT(DATE, '20110915'), N'1390-06-24', 5, 6, N'Thursday', N'پنج شنبه', 15, 24, 258, 179, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110916, CONVERT(DATE, '20110916'), N'1390-06-25', 6, 7, N'Friday', N'جمعه', 16, 25, 259, 180, 37, 26, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110917, CONVERT(DATE, '20110917'), N'1390-06-26', 7, 1, N'Saturday', N'شنبه', 17, 26, 260, 181, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110918, CONVERT(DATE, '20110918'), N'1390-06-27', 1, 2, N'Sunday', N'یک شنبه', 18, 27, 261, 182, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110919, CONVERT(DATE, '20110919'), N'1390-06-28', 2, 3, N'Monday', N'دو شنبه', 19, 28, 262, 183, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110920, CONVERT(DATE, '20110920'), N'1390-06-29', 3, 4, N'Tuesday', N'سه شنبه', 20, 29, 263, 184, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110921, CONVERT(DATE, '20110921'), N'1390-06-30', 4, 5, N'Wednesday', N'چهار شنبه', 21, 30, 264, 185, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110922, CONVERT(DATE, '20110922'), N'1390-06-31', 5, 6, N'Thursday', N'پنج شنبه', 22, 31, 265, 186, 38, 27, N'September', N'شهریور', 9, 6, 3, 2, 2011, 1390, 2, 1),
            (20110923, CONVERT(DATE, '20110923'), N'1390-07-01', 6, 7, N'Friday', N'جمعه', 23, 1, 266, 187, 38, 27, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110924, CONVERT(DATE, '20110924'), N'1390-07-02', 7, 1, N'Saturday', N'شنبه', 24, 2, 267, 188, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110925, CONVERT(DATE, '20110925'), N'1390-07-03', 1, 2, N'Sunday', N'یک شنبه', 25, 3, 268, 189, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110926, CONVERT(DATE, '20110926'), N'1390-07-04', 2, 3, N'Monday', N'دو شنبه', 26, 4, 269, 190, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110927, CONVERT(DATE, '20110927'), N'1390-07-05', 3, 4, N'Tuesday', N'سه شنبه', 27, 5, 270, 191, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110928, CONVERT(DATE, '20110928'), N'1390-07-06', 4, 5, N'Wednesday', N'چهار شنبه', 28, 6, 271, 192, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110929, CONVERT(DATE, '20110929'), N'1390-07-07', 5, 6, N'Thursday', N'پنج شنبه', 29, 7, 272, 193, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20110930, CONVERT(DATE, '20110930'), N'1390-07-08', 6, 7, N'Friday', N'جمعه', 30, 8, 273, 194, 39, 28, N'September', N'مهر', 9, 7, 3, 3, 2011, 1390, 2, 2),
            (20111001, CONVERT(DATE, '20111001'), N'1390-07-09', 7, 1, N'Saturday', N'شنبه', 1, 9, 274, 195, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111002, CONVERT(DATE, '20111002'), N'1390-07-10', 1, 2, N'Sunday', N'یک شنبه', 2, 10, 275, 196, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111003, CONVERT(DATE, '20111003'), N'1390-07-11', 2, 3, N'Monday', N'دو شنبه', 3, 11, 276, 197, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111004, CONVERT(DATE, '20111004'), N'1390-07-12', 3, 4, N'Tuesday', N'سه شنبه', 4, 12, 277, 198, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111005, CONVERT(DATE, '20111005'), N'1390-07-13', 4, 5, N'Wednesday', N'چهار شنبه', 5, 13, 278, 199, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111006, CONVERT(DATE, '20111006'), N'1390-07-14', 5, 6, N'Thursday', N'پنج شنبه', 6, 14, 279, 200, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111007, CONVERT(DATE, '20111007'), N'1390-07-15', 6, 7, N'Friday', N'جمعه', 7, 15, 280, 201, 40, 29, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111008, CONVERT(DATE, '20111008'), N'1390-07-16', 7, 1, N'Saturday', N'شنبه', 8, 16, 281, 202, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111009, CONVERT(DATE, '20111009'), N'1390-07-17', 1, 2, N'Sunday', N'یک شنبه', 9, 17, 282, 203, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111010, CONVERT(DATE, '20111010'), N'1390-07-18', 2, 3, N'Monday', N'دو شنبه', 10, 18, 283, 204, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111011, CONVERT(DATE, '20111011'), N'1390-07-19', 3, 4, N'Tuesday', N'سه شنبه', 11, 19, 284, 205, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111012, CONVERT(DATE, '20111012'), N'1390-07-20', 4, 5, N'Wednesday', N'چهار شنبه', 12, 20, 285, 206, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111013, CONVERT(DATE, '20111013'), N'1390-07-21', 5, 6, N'Thursday', N'پنج شنبه', 13, 21, 286, 207, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111014, CONVERT(DATE, '20111014'), N'1390-07-22', 6, 7, N'Friday', N'جمعه', 14, 22, 287, 208, 41, 30, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111015, CONVERT(DATE, '20111015'), N'1390-07-23', 7, 1, N'Saturday', N'شنبه', 15, 23, 288, 209, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111016, CONVERT(DATE, '20111016'), N'1390-07-24', 1, 2, N'Sunday', N'یک شنبه', 16, 24, 289, 210, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111017, CONVERT(DATE, '20111017'), N'1390-07-25', 2, 3, N'Monday', N'دو شنبه', 17, 25, 290, 211, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111018, CONVERT(DATE, '20111018'), N'1390-07-26', 3, 4, N'Tuesday', N'سه شنبه', 18, 26, 291, 212, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111019, CONVERT(DATE, '20111019'), N'1390-07-27', 4, 5, N'Wednesday', N'چهار شنبه', 19, 27, 292, 213, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111020, CONVERT(DATE, '20111020'), N'1390-07-28', 5, 6, N'Thursday', N'پنج شنبه', 20, 28, 293, 214, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111021, CONVERT(DATE, '20111021'), N'1390-07-29', 6, 7, N'Friday', N'جمعه', 21, 29, 294, 215, 42, 31, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111022, CONVERT(DATE, '20111022'), N'1390-07-30', 7, 1, N'Saturday', N'شنبه', 22, 30, 295, 216, 43, 32, N'October', N'مهر', 10, 7, 4, 3, 2011, 1390, 2, 2),
            (20111023, CONVERT(DATE, '20111023'), N'1390-08-01', 1, 2, N'Sunday', N'یک شنبه', 23, 1, 296, 217, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111024, CONVERT(DATE, '20111024'), N'1390-08-02', 2, 3, N'Monday', N'دو شنبه', 24, 2, 297, 218, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111025, CONVERT(DATE, '20111025'), N'1390-08-03', 3, 4, N'Tuesday', N'سه شنبه', 25, 3, 298, 219, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111026, CONVERT(DATE, '20111026'), N'1390-08-04', 4, 5, N'Wednesday', N'چهار شنبه', 26, 4, 299, 220, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111027, CONVERT(DATE, '20111027'), N'1390-08-05', 5, 6, N'Thursday', N'پنج شنبه', 27, 5, 300, 221, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111028, CONVERT(DATE, '20111028'), N'1390-08-06', 6, 7, N'Friday', N'جمعه', 28, 6, 301, 222, 43, 32, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111029, CONVERT(DATE, '20111029'), N'1390-08-07', 7, 1, N'Saturday', N'شنبه', 29, 7, 302, 223, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111030, CONVERT(DATE, '20111030'), N'1390-08-08', 1, 2, N'Sunday', N'یک شنبه', 30, 8, 303, 224, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111031, CONVERT(DATE, '20111031'), N'1390-08-09', 2, 3, N'Monday', N'دو شنبه', 31, 9, 304, 225, 44, 33, N'October', N'آبان', 10, 8, 4, 3, 2011, 1390, 2, 2),
            (20111101, CONVERT(DATE, '20111101'), N'1390-08-10', 3, 4, N'Tuesday', N'سه شنبه', 1, 10, 305, 226, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111102, CONVERT(DATE, '20111102'), N'1390-08-11', 4, 5, N'Wednesday', N'چهار شنبه', 2, 11, 306, 227, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111103, CONVERT(DATE, '20111103'), N'1390-08-12', 5, 6, N'Thursday', N'پنج شنبه', 3, 12, 307, 228, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111104, CONVERT(DATE, '20111104'), N'1390-08-13', 6, 7, N'Friday', N'جمعه', 4, 13, 308, 229, 44, 33, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111105, CONVERT(DATE, '20111105'), N'1390-08-14', 7, 1, N'Saturday', N'شنبه', 5, 14, 309, 230, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111106, CONVERT(DATE, '20111106'), N'1390-08-15', 1, 2, N'Sunday', N'یک شنبه', 6, 15, 310, 231, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111107, CONVERT(DATE, '20111107'), N'1390-08-16', 2, 3, N'Monday', N'دو شنبه', 7, 16, 311, 232, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111108, CONVERT(DATE, '20111108'), N'1390-08-17', 3, 4, N'Tuesday', N'سه شنبه', 8, 17, 312, 233, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111109, CONVERT(DATE, '20111109'), N'1390-08-18', 4, 5, N'Wednesday', N'چهار شنبه', 9, 18, 313, 234, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111110, CONVERT(DATE, '20111110'), N'1390-08-19', 5, 6, N'Thursday', N'پنج شنبه', 10, 19, 314, 235, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111111, CONVERT(DATE, '20111111'), N'1390-08-20', 6, 7, N'Friday', N'جمعه', 11, 20, 315, 236, 45, 34, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111112, CONVERT(DATE, '20111112'), N'1390-08-21', 7, 1, N'Saturday', N'شنبه', 12, 21, 316, 237, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111113, CONVERT(DATE, '20111113'), N'1390-08-22', 1, 2, N'Sunday', N'یک شنبه', 13, 22, 317, 238, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111114, CONVERT(DATE, '20111114'), N'1390-08-23', 2, 3, N'Monday', N'دو شنبه', 14, 23, 318, 239, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111115, CONVERT(DATE, '20111115'), N'1390-08-24', 3, 4, N'Tuesday', N'سه شنبه', 15, 24, 319, 240, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111116, CONVERT(DATE, '20111116'), N'1390-08-25', 4, 5, N'Wednesday', N'چهار شنبه', 16, 25, 320, 241, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111117, CONVERT(DATE, '20111117'), N'1390-08-26', 5, 6, N'Thursday', N'پنج شنبه', 17, 26, 321, 242, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111118, CONVERT(DATE, '20111118'), N'1390-08-27', 6, 7, N'Friday', N'جمعه', 18, 27, 322, 243, 46, 35, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111119, CONVERT(DATE, '20111119'), N'1390-08-28', 7, 1, N'Saturday', N'شنبه', 19, 28, 323, 244, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111120, CONVERT(DATE, '20111120'), N'1390-08-29', 1, 2, N'Sunday', N'یک شنبه', 20, 29, 324, 245, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111121, CONVERT(DATE, '20111121'), N'1390-08-30', 2, 3, N'Monday', N'دو شنبه', 21, 30, 325, 246, 47, 36, N'November', N'آبان', 11, 8, 4, 3, 2011, 1390, 2, 2),
            (20111122, CONVERT(DATE, '20111122'), N'1390-09-01', 3, 4, N'Tuesday', N'سه شنبه', 22, 1, 326, 247, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111123, CONVERT(DATE, '20111123'), N'1390-09-02', 4, 5, N'Wednesday', N'چهار شنبه', 23, 2, 327, 248, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111124, CONVERT(DATE, '20111124'), N'1390-09-03', 5, 6, N'Thursday', N'پنج شنبه', 24, 3, 328, 249, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111125, CONVERT(DATE, '20111125'), N'1390-09-04', 6, 7, N'Friday', N'جمعه', 25, 4, 329, 250, 47, 36, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111126, CONVERT(DATE, '20111126'), N'1390-09-05', 7, 1, N'Saturday', N'شنبه', 26, 5, 330, 251, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111127, CONVERT(DATE, '20111127'), N'1390-09-06', 1, 2, N'Sunday', N'یک شنبه', 27, 6, 331, 252, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111128, CONVERT(DATE, '20111128'), N'1390-09-07', 2, 3, N'Monday', N'دو شنبه', 28, 7, 332, 253, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111129, CONVERT(DATE, '20111129'), N'1390-09-08', 3, 4, N'Tuesday', N'سه شنبه', 29, 8, 333, 254, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111130, CONVERT(DATE, '20111130'), N'1390-09-09', 4, 5, N'Wednesday', N'چهار شنبه', 30, 9, 334, 255, 48, 37, N'November', N'آذر', 11, 9, 4, 3, 2011, 1390, 2, 2),
            (20111201, CONVERT(DATE, '20111201'), N'1390-09-10', 5, 6, N'Thursday', N'پنج شنبه', 1, 10, 335, 256, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111202, CONVERT(DATE, '20111202'), N'1390-09-11', 6, 7, N'Friday', N'جمعه', 2, 11, 336, 257, 48, 37, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111203, CONVERT(DATE, '20111203'), N'1390-09-12', 7, 1, N'Saturday', N'شنبه', 3, 12, 337, 258, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111204, CONVERT(DATE, '20111204'), N'1390-09-13', 1, 2, N'Sunday', N'یک شنبه', 4, 13, 338, 259, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111205, CONVERT(DATE, '20111205'), N'1390-09-14', 2, 3, N'Monday', N'دو شنبه', 5, 14, 339, 260, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111206, CONVERT(DATE, '20111206'), N'1390-09-15', 3, 4, N'Tuesday', N'سه شنبه', 6, 15, 340, 261, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111207, CONVERT(DATE, '20111207'), N'1390-09-16', 4, 5, N'Wednesday', N'چهار شنبه', 7, 16, 341, 262, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111208, CONVERT(DATE, '20111208'), N'1390-09-17', 5, 6, N'Thursday', N'پنج شنبه', 8, 17, 342, 263, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111209, CONVERT(DATE, '20111209'), N'1390-09-18', 6, 7, N'Friday', N'جمعه', 9, 18, 343, 264, 49, 38, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111210, CONVERT(DATE, '20111210'), N'1390-09-19', 7, 1, N'Saturday', N'شنبه', 10, 19, 344, 265, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111211, CONVERT(DATE, '20111211'), N'1390-09-20', 1, 2, N'Sunday', N'یک شنبه', 11, 20, 345, 266, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111212, CONVERT(DATE, '20111212'), N'1390-09-21', 2, 3, N'Monday', N'دو شنبه', 12, 21, 346, 267, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111213, CONVERT(DATE, '20111213'), N'1390-09-22', 3, 4, N'Tuesday', N'سه شنبه', 13, 22, 347, 268, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111214, CONVERT(DATE, '20111214'), N'1390-09-23', 4, 5, N'Wednesday', N'چهار شنبه', 14, 23, 348, 269, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111215, CONVERT(DATE, '20111215'), N'1390-09-24', 5, 6, N'Thursday', N'پنج شنبه', 15, 24, 349, 270, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111216, CONVERT(DATE, '20111216'), N'1390-09-25', 6, 7, N'Friday', N'جمعه', 16, 25, 350, 271, 50, 39, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111217, CONVERT(DATE, '20111217'), N'1390-09-26', 7, 1, N'Saturday', N'شنبه', 17, 26, 351, 272, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111218, CONVERT(DATE, '20111218'), N'1390-09-27', 1, 2, N'Sunday', N'یک شنبه', 18, 27, 352, 273, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111219, CONVERT(DATE, '20111219'), N'1390-09-28', 2, 3, N'Monday', N'دو شنبه', 19, 28, 353, 274, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111220, CONVERT(DATE, '20111220'), N'1390-09-29', 3, 4, N'Tuesday', N'سه شنبه', 20, 29, 354, 275, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111221, CONVERT(DATE, '20111221'), N'1390-09-30', 4, 5, N'Wednesday', N'چهار شنبه', 21, 30, 355, 276, 51, 40, N'December', N'آذر', 12, 9, 4, 3, 2011, 1390, 2, 2),
            (20111222, CONVERT(DATE, '20111222'), N'1390-10-01', 5, 6, N'Thursday', N'پنج شنبه', 22, 1, 356, 277, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111223, CONVERT(DATE, '20111223'), N'1390-10-02', 6, 7, N'Friday', N'جمعه', 23, 2, 357, 278, 51, 40, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111224, CONVERT(DATE, '20111224'), N'1390-10-03', 7, 1, N'Saturday', N'شنبه', 24, 3, 358, 279, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111225, CONVERT(DATE, '20111225'), N'1390-10-04', 1, 2, N'Sunday', N'یک شنبه', 25, 4, 359, 280, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111226, CONVERT(DATE, '20111226'), N'1390-10-05', 2, 3, N'Monday', N'دو شنبه', 26, 5, 360, 281, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111227, CONVERT(DATE, '20111227'), N'1390-10-06', 3, 4, N'Tuesday', N'سه شنبه', 27, 6, 361, 282, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111228, CONVERT(DATE, '20111228'), N'1390-10-07', 4, 5, N'Wednesday', N'چهار شنبه', 28, 7, 362, 283, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111229, CONVERT(DATE, '20111229'), N'1390-10-08', 5, 6, N'Thursday', N'پنج شنبه', 29, 8, 363, 284, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111230, CONVERT(DATE, '20111230'), N'1390-10-09', 6, 7, N'Friday', N'جمعه', 30, 9, 364, 285, 52, 41, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20111231, CONVERT(DATE, '20111231'), N'1390-10-10', 7, 1, N'Saturday', N'شنبه', 31, 10, 365, 286, 53, 42, N'December', N'دی', 12, 10, 4, 4, 2011, 1390, 2, 2),
            (20120101, CONVERT(DATE, '20120101'), N'1390-10-11', 1, 2, N'Sunday', N'یک شنبه', 1, 11, 1, 287, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120102, CONVERT(DATE, '20120102'), N'1390-10-12', 2, 3, N'Monday', N'دو شنبه', 2, 12, 2, 288, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120103, CONVERT(DATE, '20120103'), N'1390-10-13', 3, 4, N'Tuesday', N'سه شنبه', 3, 13, 3, 289, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120104, CONVERT(DATE, '20120104'), N'1390-10-14', 4, 5, N'Wednesday', N'چهار شنبه', 4, 14, 4, 290, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120105, CONVERT(DATE, '20120105'), N'1390-10-15', 5, 6, N'Thursday', N'پنج شنبه', 5, 15, 5, 291, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120106, CONVERT(DATE, '20120106'), N'1390-10-16', 6, 7, N'Friday', N'جمعه', 6, 16, 6, 292, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120107, CONVERT(DATE, '20120107'), N'1390-10-17', 7, 1, N'Saturday', N'شنبه', 7, 17, 7, 293, 1, 42, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120108, CONVERT(DATE, '20120108'), N'1390-10-18', 1, 2, N'Sunday', N'یک شنبه', 8, 18, 8, 294, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120109, CONVERT(DATE, '20120109'), N'1390-10-19', 2, 3, N'Monday', N'دو شنبه', 9, 19, 9, 295, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120110, CONVERT(DATE, '20120110'), N'1390-10-20', 3, 4, N'Tuesday', N'سه شنبه', 10, 20, 10, 296, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120111, CONVERT(DATE, '20120111'), N'1390-10-21', 4, 5, N'Wednesday', N'چهار شنبه', 11, 21, 11, 297, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120112, CONVERT(DATE, '20120112'), N'1390-10-22', 5, 6, N'Thursday', N'پنج شنبه', 12, 22, 12, 298, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120113, CONVERT(DATE, '20120113'), N'1390-10-23', 6, 7, N'Friday', N'جمعه', 13, 23, 13, 299, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120114, CONVERT(DATE, '20120114'), N'1390-10-24', 7, 1, N'Saturday', N'شنبه', 14, 24, 14, 300, 2, 43, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120115, CONVERT(DATE, '20120115'), N'1390-10-25', 1, 2, N'Sunday', N'یک شنبه', 15, 25, 15, 301, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120116, CONVERT(DATE, '20120116'), N'1390-10-26', 2, 3, N'Monday', N'دو شنبه', 16, 26, 16, 302, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120117, CONVERT(DATE, '20120117'), N'1390-10-27', 3, 4, N'Tuesday', N'سه شنبه', 17, 27, 17, 303, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120118, CONVERT(DATE, '20120118'), N'1390-10-28', 4, 5, N'Wednesday', N'چهار شنبه', 18, 28, 18, 304, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120119, CONVERT(DATE, '20120119'), N'1390-10-29', 5, 6, N'Thursday', N'پنج شنبه', 19, 29, 19, 305, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120120, CONVERT(DATE, '20120120'), N'1390-10-30', 6, 7, N'Friday', N'جمعه', 20, 30, 20, 306, 3, 44, N'January', N'دی', 1, 10, 1, 4, 2012, 1390, 1, 2),
            (20120121, CONVERT(DATE, '20120121'), N'1390-11-01', 7, 1, N'Saturday', N'شنبه', 21, 1, 21, 307, 3, 44, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120122, CONVERT(DATE, '20120122'), N'1390-11-02', 1, 2, N'Sunday', N'یک شنبه', 22, 2, 22, 308, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120123, CONVERT(DATE, '20120123'), N'1390-11-03', 2, 3, N'Monday', N'دو شنبه', 23, 3, 23, 309, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120124, CONVERT(DATE, '20120124'), N'1390-11-04', 3, 4, N'Tuesday', N'سه شنبه', 24, 4, 24, 310, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120125, CONVERT(DATE, '20120125'), N'1390-11-05', 4, 5, N'Wednesday', N'چهار شنبه', 25, 5, 25, 311, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120126, CONVERT(DATE, '20120126'), N'1390-11-06', 5, 6, N'Thursday', N'پنج شنبه', 26, 6, 26, 312, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120127, CONVERT(DATE, '20120127'), N'1390-11-07', 6, 7, N'Friday', N'جمعه', 27, 7, 27, 313, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120128, CONVERT(DATE, '20120128'), N'1390-11-08', 7, 1, N'Saturday', N'شنبه', 28, 8, 28, 314, 4, 45, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120129, CONVERT(DATE, '20120129'), N'1390-11-09', 1, 2, N'Sunday', N'یک شنبه', 29, 9, 29, 315, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120130, CONVERT(DATE, '20120130'), N'1390-11-10', 2, 3, N'Monday', N'دو شنبه', 30, 10, 30, 316, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120131, CONVERT(DATE, '20120131'), N'1390-11-11', 3, 4, N'Tuesday', N'سه شنبه', 31, 11, 31, 317, 5, 46, N'January', N'بهمن', 1, 11, 1, 4, 2012, 1390, 1, 2),
            (20120201, CONVERT(DATE, '20120201'), N'1390-11-12', 4, 5, N'Wednesday', N'چهار شنبه', 1, 12, 32, 318, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120202, CONVERT(DATE, '20120202'), N'1390-11-13', 5, 6, N'Thursday', N'پنج شنبه', 2, 13, 33, 319, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120203, CONVERT(DATE, '20120203'), N'1390-11-14', 6, 7, N'Friday', N'جمعه', 3, 14, 34, 320, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120204, CONVERT(DATE, '20120204'), N'1390-11-15', 7, 1, N'Saturday', N'شنبه', 4, 15, 35, 321, 5, 46, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120205, CONVERT(DATE, '20120205'), N'1390-11-16', 1, 2, N'Sunday', N'یک شنبه', 5, 16, 36, 322, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120206, CONVERT(DATE, '20120206'), N'1390-11-17', 2, 3, N'Monday', N'دو شنبه', 6, 17, 37, 323, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120207, CONVERT(DATE, '20120207'), N'1390-11-18', 3, 4, N'Tuesday', N'سه شنبه', 7, 18, 38, 324, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120208, CONVERT(DATE, '20120208'), N'1390-11-19', 4, 5, N'Wednesday', N'چهار شنبه', 8, 19, 39, 325, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120209, CONVERT(DATE, '20120209'), N'1390-11-20', 5, 6, N'Thursday', N'پنج شنبه', 9, 20, 40, 326, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120210, CONVERT(DATE, '20120210'), N'1390-11-21', 6, 7, N'Friday', N'جمعه', 10, 21, 41, 327, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120211, CONVERT(DATE, '20120211'), N'1390-11-22', 7, 1, N'Saturday', N'شنبه', 11, 22, 42, 328, 6, 47, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120212, CONVERT(DATE, '20120212'), N'1390-11-23', 1, 2, N'Sunday', N'یک شنبه', 12, 23, 43, 329, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120213, CONVERT(DATE, '20120213'), N'1390-11-24', 2, 3, N'Monday', N'دو شنبه', 13, 24, 44, 330, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120214, CONVERT(DATE, '20120214'), N'1390-11-25', 3, 4, N'Tuesday', N'سه شنبه', 14, 25, 45, 331, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120215, CONVERT(DATE, '20120215'), N'1390-11-26', 4, 5, N'Wednesday', N'چهار شنبه', 15, 26, 46, 332, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120216, CONVERT(DATE, '20120216'), N'1390-11-27', 5, 6, N'Thursday', N'پنج شنبه', 16, 27, 47, 333, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120217, CONVERT(DATE, '20120217'), N'1390-11-28', 6, 7, N'Friday', N'جمعه', 17, 28, 48, 334, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120218, CONVERT(DATE, '20120218'), N'1390-11-29', 7, 1, N'Saturday', N'شنبه', 18, 29, 49, 335, 7, 48, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120219, CONVERT(DATE, '20120219'), N'1390-11-30', 1, 2, N'Sunday', N'یک شنبه', 19, 30, 50, 336, 8, 49, N'February', N'بهمن', 2, 11, 1, 4, 2012, 1390, 1, 2),
            (20120220, CONVERT(DATE, '20120220'), N'1390-12-01', 2, 3, N'Monday', N'دو شنبه', 20, 1, 51, 337, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120221, CONVERT(DATE, '20120221'), N'1390-12-02', 3, 4, N'Tuesday', N'سه شنبه', 21, 2, 52, 338, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120222, CONVERT(DATE, '20120222'), N'1390-12-03', 4, 5, N'Wednesday', N'چهار شنبه', 22, 3, 53, 339, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120223, CONVERT(DATE, '20120223'), N'1390-12-04', 5, 6, N'Thursday', N'پنج شنبه', 23, 4, 54, 340, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120224, CONVERT(DATE, '20120224'), N'1390-12-05', 6, 7, N'Friday', N'جمعه', 24, 5, 55, 341, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120225, CONVERT(DATE, '20120225'), N'1390-12-06', 7, 1, N'Saturday', N'شنبه', 25, 6, 56, 342, 8, 49, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120226, CONVERT(DATE, '20120226'), N'1390-12-07', 1, 2, N'Sunday', N'یک شنبه', 26, 7, 57, 343, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120227, CONVERT(DATE, '20120227'), N'1390-12-08', 2, 3, N'Monday', N'دو شنبه', 27, 8, 58, 344, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120228, CONVERT(DATE, '20120228'), N'1390-12-09', 3, 4, N'Tuesday', N'سه شنبه', 28, 9, 59, 345, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120229, CONVERT(DATE, '20120229'), N'1390-12-10', 4, 5, N'Wednesday', N'چهار شنبه', 29, 10, 60, 346, 9, 50, N'February', N'اسفند', 2, 12, 1, 4, 2012, 1390, 1, 2),
            (20120301, CONVERT(DATE, '20120301'), N'1390-12-11', 5, 6, N'Thursday', N'پنج شنبه', 1, 11, 61, 347, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120302, CONVERT(DATE, '20120302'), N'1390-12-12', 6, 7, N'Friday', N'جمعه', 2, 12, 62, 348, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120303, CONVERT(DATE, '20120303'), N'1390-12-13', 7, 1, N'Saturday', N'شنبه', 3, 13, 63, 349, 9, 50, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120304, CONVERT(DATE, '20120304'), N'1390-12-14', 1, 2, N'Sunday', N'یک شنبه', 4, 14, 64, 350, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120305, CONVERT(DATE, '20120305'), N'1390-12-15', 2, 3, N'Monday', N'دو شنبه', 5, 15, 65, 351, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120306, CONVERT(DATE, '20120306'), N'1390-12-16', 3, 4, N'Tuesday', N'سه شنبه', 6, 16, 66, 352, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120307, CONVERT(DATE, '20120307'), N'1390-12-17', 4, 5, N'Wednesday', N'چهار شنبه', 7, 17, 67, 353, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120308, CONVERT(DATE, '20120308'), N'1390-12-18', 5, 6, N'Thursday', N'پنج شنبه', 8, 18, 68, 354, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120309, CONVERT(DATE, '20120309'), N'1390-12-19', 6, 7, N'Friday', N'جمعه', 9, 19, 69, 355, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120310, CONVERT(DATE, '20120310'), N'1390-12-20', 7, 1, N'Saturday', N'شنبه', 10, 20, 70, 356, 10, 51, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120311, CONVERT(DATE, '20120311'), N'1390-12-21', 1, 2, N'Sunday', N'یک شنبه', 11, 21, 71, 357, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120312, CONVERT(DATE, '20120312'), N'1390-12-22', 2, 3, N'Monday', N'دو شنبه', 12, 22, 72, 358, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120313, CONVERT(DATE, '20120313'), N'1390-12-23', 3, 4, N'Tuesday', N'سه شنبه', 13, 23, 73, 359, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120314, CONVERT(DATE, '20120314'), N'1390-12-24', 4, 5, N'Wednesday', N'چهار شنبه', 14, 24, 74, 360, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120315, CONVERT(DATE, '20120315'), N'1390-12-25', 5, 6, N'Thursday', N'پنج شنبه', 15, 25, 75, 361, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120316, CONVERT(DATE, '20120316'), N'1390-12-26', 6, 7, N'Friday', N'جمعه', 16, 26, 76, 362, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120317, CONVERT(DATE, '20120317'), N'1390-12-27', 7, 1, N'Saturday', N'شنبه', 17, 27, 77, 363, 11, 52, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120318, CONVERT(DATE, '20120318'), N'1390-12-28', 1, 2, N'Sunday', N'یک شنبه', 18, 28, 78, 364, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120319, CONVERT(DATE, '20120319'), N'1390-12-29', 2, 3, N'Monday', N'دو شنبه', 19, 29, 79, 365, 12, 53, N'March', N'اسفند', 3, 12, 1, 4, 2012, 1390, 1, 2),
            (20120320, CONVERT(DATE, '20120320'), N'1391-01-01', 3, 4, N'Tuesday', N'سه شنبه', 20, 1, 80, 1, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120321, CONVERT(DATE, '20120321'), N'1391-01-02', 4, 5, N'Wednesday', N'چهار شنبه', 21, 2, 81, 2, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120322, CONVERT(DATE, '20120322'), N'1391-01-03', 5, 6, N'Thursday', N'پنج شنبه', 22, 3, 82, 3, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120323, CONVERT(DATE, '20120323'), N'1391-01-04', 6, 7, N'Friday', N'جمعه', 23, 4, 83, 4, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120324, CONVERT(DATE, '20120324'), N'1391-01-05', 7, 1, N'Saturday', N'شنبه', 24, 5, 84, 5, 12, 1, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120325, CONVERT(DATE, '20120325'), N'1391-01-06', 1, 2, N'Sunday', N'یک شنبه', 25, 6, 85, 6, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120326, CONVERT(DATE, '20120326'), N'1391-01-07', 2, 3, N'Monday', N'دو شنبه', 26, 7, 86, 7, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120327, CONVERT(DATE, '20120327'), N'1391-01-08', 3, 4, N'Tuesday', N'سه شنبه', 27, 8, 87, 8, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120328, CONVERT(DATE, '20120328'), N'1391-01-09', 4, 5, N'Wednesday', N'چهار شنبه', 28, 9, 88, 9, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120329, CONVERT(DATE, '20120329'), N'1391-01-10', 5, 6, N'Thursday', N'پنج شنبه', 29, 10, 89, 10, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120330, CONVERT(DATE, '20120330'), N'1391-01-11', 6, 7, N'Friday', N'جمعه', 30, 11, 90, 11, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120331, CONVERT(DATE, '20120331'), N'1391-01-12', 7, 1, N'Saturday', N'شنبه', 31, 12, 91, 12, 13, 2, N'March', N'فروردین', 3, 1, 1, 1, 2012, 1391, 1, 1),
            (20120401, CONVERT(DATE, '20120401'), N'1391-01-13', 1, 2, N'Sunday', N'یک شنبه', 1, 13, 92, 13, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120402, CONVERT(DATE, '20120402'), N'1391-01-14', 2, 3, N'Monday', N'دو شنبه', 2, 14, 93, 14, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120403, CONVERT(DATE, '20120403'), N'1391-01-15', 3, 4, N'Tuesday', N'سه شنبه', 3, 15, 94, 15, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120404, CONVERT(DATE, '20120404'), N'1391-01-16', 4, 5, N'Wednesday', N'چهار شنبه', 4, 16, 95, 16, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120405, CONVERT(DATE, '20120405'), N'1391-01-17', 5, 6, N'Thursday', N'پنج شنبه', 5, 17, 96, 17, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120406, CONVERT(DATE, '20120406'), N'1391-01-18', 6, 7, N'Friday', N'جمعه', 6, 18, 97, 18, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120407, CONVERT(DATE, '20120407'), N'1391-01-19', 7, 1, N'Saturday', N'شنبه', 7, 19, 98, 19, 14, 3, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120408, CONVERT(DATE, '20120408'), N'1391-01-20', 1, 2, N'Sunday', N'یک شنبه', 8, 20, 99, 20, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120409, CONVERT(DATE, '20120409'), N'1391-01-21', 2, 3, N'Monday', N'دو شنبه', 9, 21, 100, 21, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120410, CONVERT(DATE, '20120410'), N'1391-01-22', 3, 4, N'Tuesday', N'سه شنبه', 10, 22, 101, 22, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120411, CONVERT(DATE, '20120411'), N'1391-01-23', 4, 5, N'Wednesday', N'چهار شنبه', 11, 23, 102, 23, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120412, CONVERT(DATE, '20120412'), N'1391-01-24', 5, 6, N'Thursday', N'پنج شنبه', 12, 24, 103, 24, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120413, CONVERT(DATE, '20120413'), N'1391-01-25', 6, 7, N'Friday', N'جمعه', 13, 25, 104, 25, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120414, CONVERT(DATE, '20120414'), N'1391-01-26', 7, 1, N'Saturday', N'شنبه', 14, 26, 105, 26, 15, 4, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120415, CONVERT(DATE, '20120415'), N'1391-01-27', 1, 2, N'Sunday', N'یک شنبه', 15, 27, 106, 27, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120416, CONVERT(DATE, '20120416'), N'1391-01-28', 2, 3, N'Monday', N'دو شنبه', 16, 28, 107, 28, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120417, CONVERT(DATE, '20120417'), N'1391-01-29', 3, 4, N'Tuesday', N'سه شنبه', 17, 29, 108, 29, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120418, CONVERT(DATE, '20120418'), N'1391-01-30', 4, 5, N'Wednesday', N'چهار شنبه', 18, 30, 109, 30, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120419, CONVERT(DATE, '20120419'), N'1391-01-31', 5, 6, N'Thursday', N'پنج شنبه', 19, 31, 110, 31, 16, 5, N'April', N'فروردین', 4, 1, 2, 1, 2012, 1391, 1, 1),
            (20120420, CONVERT(DATE, '20120420'), N'1391-02-01', 6, 7, N'Friday', N'جمعه', 20, 1, 111, 32, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120421, CONVERT(DATE, '20120421'), N'1391-02-02', 7, 1, N'Saturday', N'شنبه', 21, 2, 112, 33, 16, 5, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120422, CONVERT(DATE, '20120422'), N'1391-02-03', 1, 2, N'Sunday', N'یک شنبه', 22, 3, 113, 34, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120423, CONVERT(DATE, '20120423'), N'1391-02-04', 2, 3, N'Monday', N'دو شنبه', 23, 4, 114, 35, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120424, CONVERT(DATE, '20120424'), N'1391-02-05', 3, 4, N'Tuesday', N'سه شنبه', 24, 5, 115, 36, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120425, CONVERT(DATE, '20120425'), N'1391-02-06', 4, 5, N'Wednesday', N'چهار شنبه', 25, 6, 116, 37, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120426, CONVERT(DATE, '20120426'), N'1391-02-07', 5, 6, N'Thursday', N'پنج شنبه', 26, 7, 117, 38, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120427, CONVERT(DATE, '20120427'), N'1391-02-08', 6, 7, N'Friday', N'جمعه', 27, 8, 118, 39, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120428, CONVERT(DATE, '20120428'), N'1391-02-09', 7, 1, N'Saturday', N'شنبه', 28, 9, 119, 40, 17, 6, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120429, CONVERT(DATE, '20120429'), N'1391-02-10', 1, 2, N'Sunday', N'یک شنبه', 29, 10, 120, 41, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120430, CONVERT(DATE, '20120430'), N'1391-02-11', 2, 3, N'Monday', N'دو شنبه', 30, 11, 121, 42, 18, 7, N'April', N'اردیبهشت', 4, 2, 2, 1, 2012, 1391, 1, 1),
            (20120501, CONVERT(DATE, '20120501'), N'1391-02-12', 3, 4, N'Tuesday', N'سه شنبه', 1, 12, 122, 43, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120502, CONVERT(DATE, '20120502'), N'1391-02-13', 4, 5, N'Wednesday', N'چهار شنبه', 2, 13, 123, 44, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120503, CONVERT(DATE, '20120503'), N'1391-02-14', 5, 6, N'Thursday', N'پنج شنبه', 3, 14, 124, 45, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120504, CONVERT(DATE, '20120504'), N'1391-02-15', 6, 7, N'Friday', N'جمعه', 4, 15, 125, 46, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120505, CONVERT(DATE, '20120505'), N'1391-02-16', 7, 1, N'Saturday', N'شنبه', 5, 16, 126, 47, 18, 7, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120506, CONVERT(DATE, '20120506'), N'1391-02-17', 1, 2, N'Sunday', N'یک شنبه', 6, 17, 127, 48, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120507, CONVERT(DATE, '20120507'), N'1391-02-18', 2, 3, N'Monday', N'دو شنبه', 7, 18, 128, 49, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120508, CONVERT(DATE, '20120508'), N'1391-02-19', 3, 4, N'Tuesday', N'سه شنبه', 8, 19, 129, 50, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120509, CONVERT(DATE, '20120509'), N'1391-02-20', 4, 5, N'Wednesday', N'چهار شنبه', 9, 20, 130, 51, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120510, CONVERT(DATE, '20120510'), N'1391-02-21', 5, 6, N'Thursday', N'پنج شنبه', 10, 21, 131, 52, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120511, CONVERT(DATE, '20120511'), N'1391-02-22', 6, 7, N'Friday', N'جمعه', 11, 22, 132, 53, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120512, CONVERT(DATE, '20120512'), N'1391-02-23', 7, 1, N'Saturday', N'شنبه', 12, 23, 133, 54, 19, 8, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_date_sample', N'dim_date', @step_rows, N'Inserted rows from Dim_Date.txt chunk into temp table #dim_date_sample.';

        INSERT INTO #dim_date_sample (
            TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey, DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek, DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear, WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName, MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter, CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
        )
        VALUES
            (20120513, CONVERT(DATE, '20120513'), N'1391-02-24', 1, 2, N'Sunday', N'یک شنبه', 13, 24, 134, 55, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120514, CONVERT(DATE, '20120514'), N'1391-02-25', 2, 3, N'Monday', N'دو شنبه', 14, 25, 135, 56, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120515, CONVERT(DATE, '20120515'), N'1391-02-26', 3, 4, N'Tuesday', N'سه شنبه', 15, 26, 136, 57, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120516, CONVERT(DATE, '20120516'), N'1391-02-27', 4, 5, N'Wednesday', N'چهار شنبه', 16, 27, 137, 58, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120517, CONVERT(DATE, '20120517'), N'1391-02-28', 5, 6, N'Thursday', N'پنج شنبه', 17, 28, 138, 59, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120518, CONVERT(DATE, '20120518'), N'1391-02-29', 6, 7, N'Friday', N'جمعه', 18, 29, 139, 60, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120519, CONVERT(DATE, '20120519'), N'1391-02-30', 7, 1, N'Saturday', N'شنبه', 19, 30, 140, 61, 20, 9, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120520, CONVERT(DATE, '20120520'), N'1391-02-31', 1, 2, N'Sunday', N'یک شنبه', 20, 31, 141, 62, 21, 10, N'May', N'اردیبهشت', 5, 2, 2, 1, 2012, 1391, 1, 1),
            (20120521, CONVERT(DATE, '20120521'), N'1391-03-01', 2, 3, N'Monday', N'دو شنبه', 21, 1, 142, 63, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120522, CONVERT(DATE, '20120522'), N'1391-03-02', 3, 4, N'Tuesday', N'سه شنبه', 22, 2, 143, 64, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120523, CONVERT(DATE, '20120523'), N'1391-03-03', 4, 5, N'Wednesday', N'چهار شنبه', 23, 3, 144, 65, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120524, CONVERT(DATE, '20120524'), N'1391-03-04', 5, 6, N'Thursday', N'پنج شنبه', 24, 4, 145, 66, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120525, CONVERT(DATE, '20120525'), N'1391-03-05', 6, 7, N'Friday', N'جمعه', 25, 5, 146, 67, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120526, CONVERT(DATE, '20120526'), N'1391-03-06', 7, 1, N'Saturday', N'شنبه', 26, 6, 147, 68, 21, 10, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120527, CONVERT(DATE, '20120527'), N'1391-03-07', 1, 2, N'Sunday', N'یک شنبه', 27, 7, 148, 69, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120528, CONVERT(DATE, '20120528'), N'1391-03-08', 2, 3, N'Monday', N'دو شنبه', 28, 8, 149, 70, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120529, CONVERT(DATE, '20120529'), N'1391-03-09', 3, 4, N'Tuesday', N'سه شنبه', 29, 9, 150, 71, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120530, CONVERT(DATE, '20120530'), N'1391-03-10', 4, 5, N'Wednesday', N'چهار شنبه', 30, 10, 151, 72, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120531, CONVERT(DATE, '20120531'), N'1391-03-11', 5, 6, N'Thursday', N'پنج شنبه', 31, 11, 152, 73, 22, 11, N'May', N'خرداد', 5, 3, 2, 1, 2012, 1391, 1, 1),
            (20120601, CONVERT(DATE, '20120601'), N'1391-03-12', 6, 7, N'Friday', N'جمعه', 1, 12, 153, 74, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120602, CONVERT(DATE, '20120602'), N'1391-03-13', 7, 1, N'Saturday', N'شنبه', 2, 13, 154, 75, 22, 11, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120603, CONVERT(DATE, '20120603'), N'1391-03-14', 1, 2, N'Sunday', N'یک شنبه', 3, 14, 155, 76, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120604, CONVERT(DATE, '20120604'), N'1391-03-15', 2, 3, N'Monday', N'دو شنبه', 4, 15, 156, 77, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120605, CONVERT(DATE, '20120605'), N'1391-03-16', 3, 4, N'Tuesday', N'سه شنبه', 5, 16, 157, 78, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120606, CONVERT(DATE, '20120606'), N'1391-03-17', 4, 5, N'Wednesday', N'چهار شنبه', 6, 17, 158, 79, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120607, CONVERT(DATE, '20120607'), N'1391-03-18', 5, 6, N'Thursday', N'پنج شنبه', 7, 18, 159, 80, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120608, CONVERT(DATE, '20120608'), N'1391-03-19', 6, 7, N'Friday', N'جمعه', 8, 19, 160, 81, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120609, CONVERT(DATE, '20120609'), N'1391-03-20', 7, 1, N'Saturday', N'شنبه', 9, 20, 161, 82, 23, 12, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120610, CONVERT(DATE, '20120610'), N'1391-03-21', 1, 2, N'Sunday', N'یک شنبه', 10, 21, 162, 83, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120611, CONVERT(DATE, '20120611'), N'1391-03-22', 2, 3, N'Monday', N'دو شنبه', 11, 22, 163, 84, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120612, CONVERT(DATE, '20120612'), N'1391-03-23', 3, 4, N'Tuesday', N'سه شنبه', 12, 23, 164, 85, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120613, CONVERT(DATE, '20120613'), N'1391-03-24', 4, 5, N'Wednesday', N'چهار شنبه', 13, 24, 165, 86, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120614, CONVERT(DATE, '20120614'), N'1391-03-25', 5, 6, N'Thursday', N'پنج شنبه', 14, 25, 166, 87, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120615, CONVERT(DATE, '20120615'), N'1391-03-26', 6, 7, N'Friday', N'جمعه', 15, 26, 167, 88, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120616, CONVERT(DATE, '20120616'), N'1391-03-27', 7, 1, N'Saturday', N'شنبه', 16, 27, 168, 89, 24, 13, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120617, CONVERT(DATE, '20120617'), N'1391-03-28', 1, 2, N'Sunday', N'یک شنبه', 17, 28, 169, 90, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120618, CONVERT(DATE, '20120618'), N'1391-03-29', 2, 3, N'Monday', N'دو شنبه', 18, 29, 170, 91, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120619, CONVERT(DATE, '20120619'), N'1391-03-30', 3, 4, N'Tuesday', N'سه شنبه', 19, 30, 171, 92, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120620, CONVERT(DATE, '20120620'), N'1391-03-31', 4, 5, N'Wednesday', N'چهار شنبه', 20, 31, 172, 93, 25, 14, N'Jun', N'خرداد', 6, 3, 2, 1, 2012, 1391, 1, 1),
            (20120621, CONVERT(DATE, '20120621'), N'1391-04-01', 5, 6, N'Thursday', N'پنج شنبه', 21, 1, 173, 94, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120622, CONVERT(DATE, '20120622'), N'1391-04-02', 6, 7, N'Friday', N'جمعه', 22, 2, 174, 95, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120623, CONVERT(DATE, '20120623'), N'1391-04-03', 7, 1, N'Saturday', N'شنبه', 23, 3, 175, 96, 25, 14, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120624, CONVERT(DATE, '20120624'), N'1391-04-04', 1, 2, N'Sunday', N'یک شنبه', 24, 4, 176, 97, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120625, CONVERT(DATE, '20120625'), N'1391-04-05', 2, 3, N'Monday', N'دو شنبه', 25, 5, 177, 98, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120626, CONVERT(DATE, '20120626'), N'1391-04-06', 3, 4, N'Tuesday', N'سه شنبه', 26, 6, 178, 99, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120627, CONVERT(DATE, '20120627'), N'1391-04-07', 4, 5, N'Wednesday', N'چهار شنبه', 27, 7, 179, 100, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120628, CONVERT(DATE, '20120628'), N'1391-04-08', 5, 6, N'Thursday', N'پنج شنبه', 28, 8, 180, 101, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120629, CONVERT(DATE, '20120629'), N'1391-04-09', 6, 7, N'Friday', N'جمعه', 29, 9, 181, 102, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120630, CONVERT(DATE, '20120630'), N'1391-04-10', 7, 1, N'Saturday', N'شنبه', 30, 10, 182, 103, 26, 15, N'Jun', N'تیر', 6, 4, 2, 2, 2012, 1391, 1, 1),
            (20120701, CONVERT(DATE, '20120701'), N'1391-04-11', 1, 2, N'Sunday', N'یک شنبه', 1, 11, 183, 104, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120702, CONVERT(DATE, '20120702'), N'1391-04-12', 2, 3, N'Monday', N'دو شنبه', 2, 12, 184, 105, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120703, CONVERT(DATE, '20120703'), N'1391-04-13', 3, 4, N'Tuesday', N'سه شنبه', 3, 13, 185, 106, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120704, CONVERT(DATE, '20120704'), N'1391-04-14', 4, 5, N'Wednesday', N'چهار شنبه', 4, 14, 186, 107, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120705, CONVERT(DATE, '20120705'), N'1391-04-15', 5, 6, N'Thursday', N'پنج شنبه', 5, 15, 187, 108, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120706, CONVERT(DATE, '20120706'), N'1391-04-16', 6, 7, N'Friday', N'جمعه', 6, 16, 188, 109, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120707, CONVERT(DATE, '20120707'), N'1391-04-17', 7, 1, N'Saturday', N'شنبه', 7, 17, 189, 110, 27, 16, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120708, CONVERT(DATE, '20120708'), N'1391-04-18', 1, 2, N'Sunday', N'یک شنبه', 8, 18, 190, 111, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120709, CONVERT(DATE, '20120709'), N'1391-04-19', 2, 3, N'Monday', N'دو شنبه', 9, 19, 191, 112, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120710, CONVERT(DATE, '20120710'), N'1391-04-20', 3, 4, N'Tuesday', N'سه شنبه', 10, 20, 192, 113, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120711, CONVERT(DATE, '20120711'), N'1391-04-21', 4, 5, N'Wednesday', N'چهار شنبه', 11, 21, 193, 114, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120712, CONVERT(DATE, '20120712'), N'1391-04-22', 5, 6, N'Thursday', N'پنج شنبه', 12, 22, 194, 115, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120713, CONVERT(DATE, '20120713'), N'1391-04-23', 6, 7, N'Friday', N'جمعه', 13, 23, 195, 116, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120714, CONVERT(DATE, '20120714'), N'1391-04-24', 7, 1, N'Saturday', N'شنبه', 14, 24, 196, 117, 28, 17, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120715, CONVERT(DATE, '20120715'), N'1391-04-25', 1, 2, N'Sunday', N'یک شنبه', 15, 25, 197, 118, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120716, CONVERT(DATE, '20120716'), N'1391-04-26', 2, 3, N'Monday', N'دو شنبه', 16, 26, 198, 119, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120717, CONVERT(DATE, '20120717'), N'1391-04-27', 3, 4, N'Tuesday', N'سه شنبه', 17, 27, 199, 120, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120718, CONVERT(DATE, '20120718'), N'1391-04-28', 4, 5, N'Wednesday', N'چهار شنبه', 18, 28, 200, 121, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120719, CONVERT(DATE, '20120719'), N'1391-04-29', 5, 6, N'Thursday', N'پنج شنبه', 19, 29, 201, 122, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120720, CONVERT(DATE, '20120720'), N'1391-04-30', 6, 7, N'Friday', N'جمعه', 20, 30, 202, 123, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120721, CONVERT(DATE, '20120721'), N'1391-04-31', 7, 1, N'Saturday', N'شنبه', 21, 31, 203, 124, 29, 18, N'July', N'تیر', 7, 4, 3, 2, 2012, 1391, 2, 1),
            (20120722, CONVERT(DATE, '20120722'), N'1391-05-01', 1, 2, N'Sunday', N'یک شنبه', 22, 1, 204, 125, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120723, CONVERT(DATE, '20120723'), N'1391-05-02', 2, 3, N'Monday', N'دو شنبه', 23, 2, 205, 126, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120724, CONVERT(DATE, '20120724'), N'1391-05-03', 3, 4, N'Tuesday', N'سه شنبه', 24, 3, 206, 127, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120725, CONVERT(DATE, '20120725'), N'1391-05-04', 4, 5, N'Wednesday', N'چهار شنبه', 25, 4, 207, 128, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120726, CONVERT(DATE, '20120726'), N'1391-05-05', 5, 6, N'Thursday', N'پنج شنبه', 26, 5, 208, 129, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120727, CONVERT(DATE, '20120727'), N'1391-05-06', 6, 7, N'Friday', N'جمعه', 27, 6, 209, 130, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120728, CONVERT(DATE, '20120728'), N'1391-05-07', 7, 1, N'Saturday', N'شنبه', 28, 7, 210, 131, 30, 19, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120729, CONVERT(DATE, '20120729'), N'1391-05-08', 1, 2, N'Sunday', N'یک شنبه', 29, 8, 211, 132, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120730, CONVERT(DATE, '20120730'), N'1391-05-09', 2, 3, N'Monday', N'دو شنبه', 30, 9, 212, 133, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120731, CONVERT(DATE, '20120731'), N'1391-05-10', 3, 4, N'Tuesday', N'سه شنبه', 31, 10, 213, 134, 31, 20, N'July', N'مرداد', 7, 5, 3, 2, 2012, 1391, 2, 1),
            (20120801, CONVERT(DATE, '20120801'), N'1391-05-11', 4, 5, N'Wednesday', N'چهار شنبه', 1, 11, 214, 135, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120802, CONVERT(DATE, '20120802'), N'1391-05-12', 5, 6, N'Thursday', N'پنج شنبه', 2, 12, 215, 136, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120803, CONVERT(DATE, '20120803'), N'1391-05-13', 6, 7, N'Friday', N'جمعه', 3, 13, 216, 137, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120804, CONVERT(DATE, '20120804'), N'1391-05-14', 7, 1, N'Saturday', N'شنبه', 4, 14, 217, 138, 31, 20, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120805, CONVERT(DATE, '20120805'), N'1391-05-15', 1, 2, N'Sunday', N'یک شنبه', 5, 15, 218, 139, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120806, CONVERT(DATE, '20120806'), N'1391-05-16', 2, 3, N'Monday', N'دو شنبه', 6, 16, 219, 140, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120807, CONVERT(DATE, '20120807'), N'1391-05-17', 3, 4, N'Tuesday', N'سه شنبه', 7, 17, 220, 141, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120808, CONVERT(DATE, '20120808'), N'1391-05-18', 4, 5, N'Wednesday', N'چهار شنبه', 8, 18, 221, 142, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120809, CONVERT(DATE, '20120809'), N'1391-05-19', 5, 6, N'Thursday', N'پنج شنبه', 9, 19, 222, 143, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120810, CONVERT(DATE, '20120810'), N'1391-05-20', 6, 7, N'Friday', N'جمعه', 10, 20, 223, 144, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120811, CONVERT(DATE, '20120811'), N'1391-05-21', 7, 1, N'Saturday', N'شنبه', 11, 21, 224, 145, 32, 21, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120812, CONVERT(DATE, '20120812'), N'1391-05-22', 1, 2, N'Sunday', N'یک شنبه', 12, 22, 225, 146, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120813, CONVERT(DATE, '20120813'), N'1391-05-23', 2, 3, N'Monday', N'دو شنبه', 13, 23, 226, 147, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120814, CONVERT(DATE, '20120814'), N'1391-05-24', 3, 4, N'Tuesday', N'سه شنبه', 14, 24, 227, 148, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120815, CONVERT(DATE, '20120815'), N'1391-05-25', 4, 5, N'Wednesday', N'چهار شنبه', 15, 25, 228, 149, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120816, CONVERT(DATE, '20120816'), N'1391-05-26', 5, 6, N'Thursday', N'پنج شنبه', 16, 26, 229, 150, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120817, CONVERT(DATE, '20120817'), N'1391-05-27', 6, 7, N'Friday', N'جمعه', 17, 27, 230, 151, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120818, CONVERT(DATE, '20120818'), N'1391-05-28', 7, 1, N'Saturday', N'شنبه', 18, 28, 231, 152, 33, 22, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120819, CONVERT(DATE, '20120819'), N'1391-05-29', 1, 2, N'Sunday', N'یک شنبه', 19, 29, 232, 153, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120820, CONVERT(DATE, '20120820'), N'1391-05-30', 2, 3, N'Monday', N'دو شنبه', 20, 30, 233, 154, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120821, CONVERT(DATE, '20120821'), N'1391-05-31', 3, 4, N'Tuesday', N'سه شنبه', 21, 31, 234, 155, 34, 23, N'August', N'مرداد', 8, 5, 3, 2, 2012, 1391, 2, 1),
            (20120822, CONVERT(DATE, '20120822'), N'1391-06-01', 4, 5, N'Wednesday', N'چهار شنبه', 22, 1, 235, 156, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120823, CONVERT(DATE, '20120823'), N'1391-06-02', 5, 6, N'Thursday', N'پنج شنبه', 23, 2, 236, 157, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120824, CONVERT(DATE, '20120824'), N'1391-06-03', 6, 7, N'Friday', N'جمعه', 24, 3, 237, 158, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120825, CONVERT(DATE, '20120825'), N'1391-06-04', 7, 1, N'Saturday', N'شنبه', 25, 4, 238, 159, 34, 23, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120826, CONVERT(DATE, '20120826'), N'1391-06-05', 1, 2, N'Sunday', N'یک شنبه', 26, 5, 239, 160, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120827, CONVERT(DATE, '20120827'), N'1391-06-06', 2, 3, N'Monday', N'دو شنبه', 27, 6, 240, 161, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120828, CONVERT(DATE, '20120828'), N'1391-06-07', 3, 4, N'Tuesday', N'سه شنبه', 28, 7, 241, 162, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120829, CONVERT(DATE, '20120829'), N'1391-06-08', 4, 5, N'Wednesday', N'چهار شنبه', 29, 8, 242, 163, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120830, CONVERT(DATE, '20120830'), N'1391-06-09', 5, 6, N'Thursday', N'پنج شنبه', 30, 9, 243, 164, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120831, CONVERT(DATE, '20120831'), N'1391-06-10', 6, 7, N'Friday', N'جمعه', 31, 10, 244, 165, 35, 24, N'August', N'شهریور', 8, 6, 3, 2, 2012, 1391, 2, 1),
            (20120901, CONVERT(DATE, '20120901'), N'1391-06-11', 7, 1, N'Saturday', N'شنبه', 1, 11, 245, 166, 35, 24, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120902, CONVERT(DATE, '20120902'), N'1391-06-12', 1, 2, N'Sunday', N'یک شنبه', 2, 12, 246, 167, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1),
            (20120903, CONVERT(DATE, '20120903'), N'1391-06-13', 2, 3, N'Monday', N'دو شنبه', 3, 13, 247, 168, 36, 25, N'September', N'شهریور', 9, 6, 3, 2, 2012, 1391, 2, 1);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_date_sample', N'dim_date', @step_rows, N'Inserted rows from Dim_Date.txt chunk into temp table #dim_date_sample.';

        SELECT @rows_read = COUNT(*) FROM #dim_date_sample;

        EXEC etl_admin.usp_dw_mart1_drop_table_indexes N'dw', N'dim_date';
        TRUNCATE TABLE dw.dim_date;

        INSERT INTO dw.dim_date (
            TimeKey,
            FullDateAlternateKey,
            PersianFullDateAlternateKey,
            DayNumberOfWeek,
            PersianDayNumberOfWeek,
            EnglishDayNameOfWeek,
            PersianDayNameOfWeek,
            DayNumberOfMonth,
            PersianDayNumberOfMonth,
            DayNumberOfYear,
            PersianDayNumberOfYear,
            WeekNumberOfYear,
            PersianWeekNumberOfYear,
            EnglishMonthName,
            PersianMonthName,
            MonthNumberOfYear,
            PersianMonthNumberOfYear,
            CalendarQuarter,
            PersianCalendarQuarter,
            CalendarYear,
            PersianCalendarYear,
            CalendarSemester,
            PersianCalendarSemester
        )
        VALUES (
            -1, CONVERT(DATE, '19000101'), N'نامشخص', 0, 0, N'Unknown', N'نامشخص', 0, 0, 0, 0, 0, 0, N'Unknown', N'نامشخص', 0, 0, 0, 0, 1900, 0, 0, 0
        );

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_date_unknown_insert', N'dim_date', @step_rows, N'Inserted technical unknown row into dw.dim_date.';

        INSERT INTO dw.dim_date (
            TimeKey,
            FullDateAlternateKey,
            PersianFullDateAlternateKey,
            DayNumberOfWeek,
            PersianDayNumberOfWeek,
            EnglishDayNameOfWeek,
            PersianDayNameOfWeek,
            DayNumberOfMonth,
            PersianDayNumberOfMonth,
            DayNumberOfYear,
            PersianDayNumberOfYear,
            WeekNumberOfYear,
            PersianWeekNumberOfYear,
            EnglishMonthName,
            PersianMonthName,
            MonthNumberOfYear,
            PersianMonthNumberOfYear,
            CalendarQuarter,
            PersianCalendarQuarter,
            CalendarYear,
            PersianCalendarYear,
            CalendarSemester,
            PersianCalendarSemester
        )
        SELECT
            TimeKey,
            FullDateAlternateKey,
            PersianFullDateAlternateKey,
            DayNumberOfWeek,
            PersianDayNumberOfWeek,
            EnglishDayNameOfWeek,
            PersianDayNameOfWeek,
            DayNumberOfMonth,
            PersianDayNumberOfMonth,
            DayNumberOfYear,
            PersianDayNumberOfYear,
            WeekNumberOfYear,
            PersianWeekNumberOfYear,
            EnglishMonthName,
            PersianMonthName,
            MonthNumberOfYear,
            PersianMonthNumberOfYear,
            CalendarQuarter,
            PersianCalendarQuarter,
            CalendarYear,
            PersianCalendarYear,
            CalendarSemester,
            PersianCalendarSemester
        FROM #dim_date_sample;

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;

        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_date_sample_insert', N'dim_date', @step_rows, N'Inserted all date rows from Dim_Date.txt sample into dw.dim_date. No generated date range is used.';

        EXEC etl_admin.usp_dw_mart1_write_load_log
            @etl_batch_id, N'Dim_Date.txt', N'file_sample', N'Dim_Date',
            N'dw', N'dim_date', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at,
            N'Date dimension loaded from the provided Dim_Date.txt sample only; no ETL-generated date sequence is used.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log
            @etl_batch_id, N'Dim_Date.txt', N'file_sample', N'Dim_Date',
            N'dw', N'dim_date', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Center
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_center
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_center_business_insert', N'dim_center', @step_rows, N'Inserted business rows into dw.dim_center.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers', N'dw', N'dim_center', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Center dimension loaded with inactive/deleted rows retained.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'centers', N'dw', N'dim_center', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Teacher
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_teacher
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#center_map_for_teacher') IS NOT NULL DROP TABLE #center_map_for_teacher;

        SELECT c.id AS center_id, c.name AS center_name
        INTO #center_map_for_teacher
        FROM Stg_ProgramOps_DB.stg_program_ops.centers AS c
        WHERE c.is_valid = 1
          AND c.id IS NOT NULL;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#center_map_for_teacher', N'dim_teacher', @step_rows, N'Inserted rows into temp table #center_map_for_teacher.';

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_teacher_business_insert', N'dim_teacher', @step_rows, N'Inserted business rows into dw.dim_teacher.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers', N'dw', N'dim_teacher', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Teacher dimension loaded.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'teachers', N'dw', N'dim_teacher', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Child
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_child
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_child_business_insert', N'dim_child', @step_rows, N'Inserted business rows into dw.dim_child.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'children', N'dw', N'dim_child', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Child dimension loaded.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'children', N'dw', N'dim_child', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Domain
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_domain
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_domain_business_insert', N'dim_domain', @step_rows, N'Inserted business rows into dw.dim_domain.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains', N'dw', N'dim_domain', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Domain dimension loaded.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'domains', N'dw', N'dim_domain', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Task
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_task
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#domain_names') IS NOT NULL DROP TABLE #domain_names;
        IF OBJECT_ID(N'tempdb..#task_candidates') IS NOT NULL DROP TABLE #task_candidates;

        SELECT d.id AS domain_id, d.name AS domain_name
        INTO #domain_names
        FROM Stg_ProgramOps_DB.stg_program_ops.domains AS d
        WHERE d.is_valid = 1 AND d.id IS NOT NULL;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#domain_names', N'dim_task', @step_rows, N'Inserted rows into temp table #domain_names.';

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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#task_candidates', N'dim_task', @step_rows, N'Inserted rows into temp table #task_candidates.';

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_task_business_insert', N'dim_task', @step_rows, N'Inserted business rows into dw.dim_task.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates/daily_task_assignments/child_task_plans', N'dw', N'dim_task', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Task dimension loaded from templates and non-template task titles.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_templates/daily_task_assignments/child_task_plans', N'dw', N'dim_task', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Score Scale
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_score_scale
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_score_scale_business_insert', N'dim_score_scale', @step_rows, N'Inserted business rows into dw.dim_score_scale.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales', N'dw', N'dim_score_scale', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Score scale dimension loaded.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'score_scales', N'dw', N'dim_score_scale', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: Assessment Status
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_assessment_status
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#statuses', N'dim_assessment_status', @step_rows, N'Inserted rows into temp table #statuses.';

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_assessment_status_business_insert', N'dim_assessment_status', @step_rows, N'Inserted business rows into dw.dim_assessment_status.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments/daily_task_assignments/assessment_sessions', N'dw', N'dim_assessment_status', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Assessment status dimension loaded from distinct operational statuses.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'task_assessments/daily_task_assignments/assessment_sessions', N'dw', N'dim_assessment_status', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Dimension: No Score Reason
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_dim_no_score_reason
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows + 1;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.dim_no_score_reason_business_insert', N'dim_no_score_reason', @step_rows, N'Inserted business rows into dw.dim_no_score_reason.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons', N'dw', N'dim_no_score_reason', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'No-score reason dimension loaded.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'no_score_reasons', N'dw', N'dim_no_score_reason', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Transaction Student Task Progress
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_tran_student_task_progress
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_child', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_child.';
        CREATE INDEX IX_tmp_dim_child ON #dim_child(child_id);

        SELECT center_id, MIN(center_key) AS center_key
        INTO #dim_center
        FROM dw.dim_center
        GROUP BY center_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_center', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_center.';
        CREATE INDEX IX_tmp_dim_center ON #dim_center(center_id);

        SELECT teacher_id, MIN(teacher_key) AS teacher_key
        INTO #dim_teacher
        FROM dw.dim_teacher
        GROUP BY teacher_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_teacher', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_teacher.';
        CREATE INDEX IX_tmp_dim_teacher ON #dim_teacher(teacher_id);

        SELECT domain_id, MIN(domain_key) AS domain_key
        INTO #dim_domain
        FROM dw.dim_domain
        GROUP BY domain_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_domain', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_domain.';
        CREATE INDEX IX_tmp_dim_domain ON #dim_domain(domain_id);

        SELECT task_template_id, MIN(task_key) AS task_key
        INTO #dim_task_template
        FROM dw.dim_task
        WHERE task_template_id IS NOT NULL
        GROUP BY task_template_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_task_template', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_task_template.';
        CREATE INDEX IX_tmp_dim_task_template ON #dim_task_template(task_template_id);

        SELECT task_title, domain_id, MIN(task_key) AS task_key
        INTO #dim_task_title
        FROM dw.dim_task
        GROUP BY task_title, domain_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_task_title', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_task_title.';
        CREATE INDEX IX_tmp_dim_task_title ON #dim_task_title(task_title, domain_id);

        SELECT score_scale_id, MIN(score_scale_key) AS score_scale_key
        INTO #dim_score_scale
        FROM dw.dim_score_scale
        GROUP BY score_scale_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_score_scale', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_score_scale.';
        CREATE INDEX IX_tmp_dim_score_scale ON #dim_score_scale(score_scale_id);

        SELECT assessment_status_code, MIN(assessment_status_key) AS assessment_status_key
        INTO #dim_status
        FROM dw.dim_assessment_status
        GROUP BY assessment_status_code;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_status', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_status.';
        CREATE INDEX IX_tmp_dim_status ON #dim_status(assessment_status_code);

        SELECT no_score_reason_id, MIN(no_score_reason_key) AS no_score_reason_key
        INTO #dim_no_score_reason
        FROM dw.dim_no_score_reason
        GROUP BY no_score_reason_id;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#dim_no_score_reason', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #dim_no_score_reason.';
        CREATE INDEX IX_tmp_dim_nsr ON #dim_no_score_reason(no_score_reason_id);

        SELECT center_id, [date], CONVERT(BIT, 1) AS is_center_closed
        INTO #center_closed
        FROM Stg_ProgramOps_DB.stg_program_ops.center_daily_status
        WHERE is_valid = 1
          AND (LOWER(COALESCE(status, N'')) IN (N'closed', N'closure') OR closure_reason_id IS NOT NULL);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#center_closed', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #center_closed.';
        CREATE INDEX IX_tmp_center_closed ON #center_closed(center_id, [date]);

        SELECT child_id, [date], CONVERT(BIT, 1) AS is_absent
        INTO #child_absent
        FROM Stg_ProgramOps_DB.stg_program_ops.child_daily_status
        WHERE is_valid = 1
          AND (LOWER(COALESCE(status, N'')) IN (N'absent', N'absence') OR absence_reason_id IS NOT NULL);

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#child_absent', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #child_absent.';
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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#assignment_core', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #assignment_core.';

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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#assessment_orphan_core', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #assessment_orphan_core.';

        SELECT *
        INTO #fact_core
        FROM #assignment_core
        UNION ALL
        SELECT * FROM #assessment_orphan_core;

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#fact_core', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into temp table #fact_core.';

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
            COALESCE(dd.TimeKey, -1),
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
            ON fc.fact_date = dd.FullDateAlternateKey
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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.fact_tran_student_task_progress_insert', N'fact_tran_student_task_progress', @step_rows, N'Inserted rows into dw.fact_tran_student_task_progress.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments/task_assessments', N'dw', N'fact_tran_student_task_progress', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Transaction fact loaded in one insert segment after temp staging.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Stg_ProgramOps_DB', N'stg_program_ops', N'daily_task_assignments/task_assessments', N'dw', N'fact_tran_student_task_progress', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Daily Student Task Progress
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_daily_student_task_progress
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#daily_core', N'fact_daily_student_task_progress', @step_rows, N'Inserted rows into temp table #daily_core.';

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.fact_daily_student_task_progress_insert', N'fact_daily_student_task_progress', @step_rows, N'Inserted rows into dw.fact_daily_student_task_progress.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress', N'dw', N'fact_daily_student_task_progress', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Daily aggregate fact loaded in one insert segment.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_tran_student_task_progress', N'dw', N'fact_daily_student_task_progress', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Child Snapshot Accumulation
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_child_snapshot_accumulation
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#snapshot_core', N'fact_child_snapshot_accumulation', @step_rows, N'Inserted rows into temp table #snapshot_core.';

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.fact_child_snapshot_accumulation_insert', N'fact_child_snapshot_accumulation', @step_rows, N'Inserted rows into dw.fact_child_snapshot_accumulation.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress', N'dw', N'fact_child_snapshot_accumulation', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Child snapshot fact loaded in one insert segment.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_daily_student_task_progress', N'dw', N'fact_child_snapshot_accumulation', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Fact: Child Task Event, including deleted business events
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_load_dw_mart1_fact_child_task_event
    @from_date DATE,
    @to_date DATE,
    @etl_batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL procedures.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

    DECLARE @from_datetime DATETIME2(0) = CONVERT(DATETIME2(0), @from_date);
    DECLARE @to_datetime_exclusive DATETIME2(0) = DATEADD(DAY, 1, CONVERT(DATETIME2(0), @to_date));
    DECLARE @started_at DATETIME2(0) = SYSDATETIME();
    DECLARE @rows_read INT = 0, @rows_inserted INT = 0, @rows_rejected INT = 0;
    DECLARE @step_rows INT = 0;

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
            COALESCE(dd.TimeKey, -1) AS date_key,
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
            ON CONVERT(DATE, al.created_at) = dd.FullDateAlternateKey
        WHERE al.is_valid = 1
          AND LOWER(COALESCE(al.action, N'')) IN (N'delete', N'deleted', N'remove', N'removed')
          AND LOWER(COALESCE(al.entity_name, N'')) IN (N'daily_task_assignments', N'task_assessments', N'assessment_sessions', N'child_task_plans', N'children', N'teachers', N'centers');

        SET @step_rows = @@ROWCOUNT;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'#event_core', N'fact_child_task_event', @step_rows, N'Inserted rows into temp table #event_core.';

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

        SET @step_rows = @@ROWCOUNT;
        SET @rows_inserted = @step_rows;
        EXEC etl_admin.usp_dw_mart1_write_step_log @etl_batch_id, N'dw.fact_child_task_event_insert', N'fact_child_task_event', @step_rows, N'Inserted rows into dw.fact_child_task_event.';
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB/Stg_ProgramOps_DB', N'dw/stg_program_ops', N'fact_tran_student_task_progress/audit_logs', N'dw', N'fact_child_task_event', N'succeeded', @rows_read, @rows_inserted, @rows_rejected, @started_at, N'Child task event fact loaded in one insert segment including deleted business events.';
    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        EXEC etl_admin.usp_dw_mart1_write_load_log @etl_batch_id, N'Charity_DW_DB/Stg_ProgramOps_DB', N'dw/stg_program_ops', N'fact_tran_student_task_progress/audit_logs', N'dw', N'fact_child_task_event', N'failed', @rows_read, @rows_inserted, @rows_rejected, @started_at, @error_message;
        THROW;
    END CATCH
END;
GO

/*=============================================================================
  Main Runner: MART 1 DW ETL
=============================================================================*/

CREATE OR ALTER PROCEDURE etl_admin.usp_run_dw_mart1_all
    @from_date DATE,
    @to_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    IF @from_date IS NULL OR @to_date IS NULL
        THROW 51001, 'from_date and to_date are required for MART 1 DW ETL runner.', 1;

    IF @to_date < @from_date
        THROW 51002, 'to_date must be greater than or equal to from_date.', 1;

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

        EXEC etl_admin.usp_load_dw_mart1_dim_date @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_center @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_teacher @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_child @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_domain @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_task @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_score_scale @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_assessment_status @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_dim_no_score_reason @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;

        EXEC etl_admin.usp_load_dw_mart1_fact_tran_student_task_progress @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_fact_daily_student_task_progress @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_fact_child_snapshot_accumulation @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;
        EXEC etl_admin.usp_load_dw_mart1_fact_child_task_event @from_date = @from_date, @to_date = @to_date, @etl_batch_id = @etl_batch_id;

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
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
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
            @error_message;
        THROW;
    END CATCH
END;
GO

PRINT 'MART 1 DW ETL procedures created. Run EXEC etl_admin.usp_run_dw_mart1_all @from_date = ''YYYY-MM-DD'', @to_date = ''YYYY-MM-DD'' after staging load.';
GO
