/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 3 - Data Warehouse Layer
 File         : 12_create_dw_mart1_tables.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create MART 1 Data Warehouse tables for Student / Child Task Progress.

 Performance style requested:
   - No PRIMARY KEY constraints
   - No FOREIGN KEY constraints
   - No UNIQUE constraints
   - No CHECK constraints
   - No DEFAULT constraints
   - No MERGE logic here
   - Tables are created as HEAP tables for faster ETL loading

 Notes:
   - Surrogate key columns are still kept as IDENTITY columns.
   - Unknown dimension rows still use key = -1.
   - ETL procedures should handle data validation before loading DW tables.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Charity_DW_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw')
BEGIN
    EXEC(N'CREATE SCHEMA dw');
END
GO

/*=============================================================================
  Drop Existing MART 1 Tables - Dependency Order
=============================================================================*/

DROP TABLE IF EXISTS dw.fact_child_task_event;
DROP TABLE IF EXISTS dw.fact_child_snapshot_accumulation;
DROP TABLE IF EXISTS dw.fact_daily_student_task_progress;
DROP TABLE IF EXISTS dw.fact_tran_student_task_progress;

DROP TABLE IF EXISTS dw.dim_no_score_reason;
DROP TABLE IF EXISTS dw.dim_assessment_status;
DROP TABLE IF EXISTS dw.dim_score_scale;
DROP TABLE IF EXISTS dw.dim_task;
DROP TABLE IF EXISTS dw.dim_domain;
DROP TABLE IF EXISTS dw.dim_child;
DROP TABLE IF EXISTS dw.dim_teacher;
DROP TABLE IF EXISTS dw.dim_center;
DROP TABLE IF EXISTS dw.dim_date;
GO

/*=============================================================================
  Shared Dimension: Date
=============================================================================*/

CREATE TABLE dw.dim_date (
    TimeKey                      INT NULL,
    FullDateAlternateKey         DATE NULL,
    PersianFullDateAlternateKey  NVARCHAR(10) NULL,
    DayNumberOfWeek             TINYINT NULL,
    PersianDayNumberOfWeek      TINYINT NULL,
    EnglishDayNameOfWeek        NVARCHAR(20) NULL,
    PersianDayNameOfWeek        NVARCHAR(20) NULL,
    DayNumberOfMonth            TINYINT NULL,
    PersianDayNumberOfMonth     TINYINT NULL,
    DayNumberOfYear             SMALLINT NULL,
    PersianDayNumberOfYear      SMALLINT NULL,
    WeekNumberOfYear            TINYINT NULL,
    PersianWeekNumberOfYear     TINYINT NULL,
    EnglishMonthName            NVARCHAR(20) NULL,
    PersianMonthName            NVARCHAR(20) NULL,
    MonthNumberOfYear           TINYINT NULL,
    PersianMonthNumberOfYear    TINYINT NULL,
    CalendarQuarter             TINYINT NULL,
    PersianCalendarQuarter      TINYINT NULL,
    CalendarYear                SMALLINT NULL,
    PersianCalendarYear         SMALLINT NULL,
    CalendarSemester            TINYINT NULL,
    PersianCalendarSemester     TINYINT NULL
);
GO

/*=============================================================================
  MART 1 Dimensions
=============================================================================*/

CREATE TABLE dw.dim_center (
    center_key           INT IDENTITY(1,1) NOT NULL,
    center_id            INT NULL,
    center_name          NVARCHAR(200) NULL,
    city                 NVARCHAR(100) NULL,
    address              NVARCHAR(500) NULL,
    center_status        NVARCHAR(30) NULL,
    effective_from       DATETIME2(0) NULL,
    effective_to         DATETIME2(0) NULL,
    is_current           BIT NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_teacher (
    teacher_key          INT IDENTITY(1,1) NOT NULL,
    teacher_id           INT NULL,
    first_name           NVARCHAR(100) NULL,
    last_name            NVARCHAR(100) NULL,
    full_name            NVARCHAR(220) NULL,
    center_id            INT NULL,
    center_name          NVARCHAR(200) NULL,
    employment_status    NVARCHAR(50) NULL,
    effective_from       DATETIME2(0) NULL,
    effective_to         DATETIME2(0) NULL,
    is_current           BIT NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_child (
    child_key            INT IDENTITY(1,1) NOT NULL,
    child_id             INT NULL,
    first_name           NVARCHAR(100) NULL,
    last_name            NVARCHAR(100) NULL,
    full_name            NVARCHAR(220) NULL,
    birth_date           DATE NULL,
    gender               NVARCHAR(20) NULL,
    center_id            INT NULL,
    status               NVARCHAR(50) NULL,
    enrollment_date      DATE NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_domain (
    domain_key           INT IDENTITY(1,1) NOT NULL,
    domain_id            INT NULL,
    domain_name          NVARCHAR(200) NULL,
    domain_description   NVARCHAR(MAX) NULL,
    domain_status        NVARCHAR(30) NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_task (
    task_key             INT IDENTITY(1,1) NOT NULL,
    task_template_id     INT NULL,
    task_title           NVARCHAR(300) NULL,
    domain_id            INT NULL,
    domain_name          NVARCHAR(200) NULL,
    is_template_based    BIT NULL,
    task_description     NVARCHAR(MAX) NULL,
    task_status          NVARCHAR(30) NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_score_scale (
    score_scale_key      INT IDENTITY(1,1) NOT NULL,
    score_scale_id       INT NULL,
    scale_name           NVARCHAR(100) NULL,
    min_score            DECIMAL(10,2) NULL,
    max_score            DECIMAL(10,2) NULL,
    scale_description    NVARCHAR(MAX) NULL,
    scale_status         NVARCHAR(30) NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_assessment_status (
    assessment_status_key       INT IDENTITY(1,1) NOT NULL,
    assessment_status_code      NVARCHAR(50) NULL,
    assessment_status_title     NVARCHAR(100) NULL,
    assessment_status_category  NVARCHAR(50) NULL,
    is_successful_assessment    BIT NULL,
    is_failure_assessment       BIT NULL,
    source_system               NVARCHAR(100) NULL,
    created_at                  DATETIME2(0) NULL,
    updated_at                  DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_no_score_reason (
    no_score_reason_key  INT IDENTITY(1,1) NOT NULL,
    no_score_reason_id   INT NULL,
    reason_title         NVARCHAR(200) NULL,
    reason_description   NVARCHAR(MAX) NULL,
    reason_category      NVARCHAR(100) NULL,
    is_child_related     BIT NULL,
    is_teacher_related   BIT NULL,
    is_center_related    BIT NULL,
    is_system_related    BIT NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

/*=============================================================================
  Unknown Dimension Rows
=============================================================================*/

INSERT INTO dw.dim_date (
    TimeKey, FullDateAlternateKey, PersianFullDateAlternateKey,
    DayNumberOfWeek, PersianDayNumberOfWeek, EnglishDayNameOfWeek, PersianDayNameOfWeek,
    DayNumberOfMonth, PersianDayNumberOfMonth, DayNumberOfYear, PersianDayNumberOfYear,
    WeekNumberOfYear, PersianWeekNumberOfYear, EnglishMonthName, PersianMonthName,
    MonthNumberOfYear, PersianMonthNumberOfYear, CalendarQuarter, PersianCalendarQuarter,
    CalendarYear, PersianCalendarYear, CalendarSemester, PersianCalendarSemester
)
VALUES (
    -1, CONVERT(DATE, '19000101'), N'نامشخص',
    0, 0, N'Unknown', N'نامشخص',
    0, 0, 0, 0,
    0, 0, N'Unknown', N'نامشخص',
    0, 0, 0, 0,
    1900, 0, 0, 0
);
GO

SET IDENTITY_INSERT dw.dim_center ON;
INSERT INTO dw.dim_center (center_key, center_id, center_name, city, address, center_status, effective_from, effective_to, is_current, source_system, created_at)
VALUES (-1, -1, N'Unknown', NULL, NULL, N'unknown', CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_center OFF;
GO

SET IDENTITY_INSERT dw.dim_teacher ON;
INSERT INTO dw.dim_teacher (teacher_key, teacher_id, first_name, last_name, full_name, center_id, center_name, employment_status, effective_from, effective_to, is_current, source_system, created_at)
VALUES (-1, -1, NULL, NULL, N'Unknown', NULL, NULL, N'unknown', CONVERT(DATETIME2(0), '19000101'), NULL, 1, N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_teacher OFF;
GO

SET IDENTITY_INSERT dw.dim_child ON;
INSERT INTO dw.dim_child (child_key, child_id, first_name, last_name, full_name, birth_date, gender, center_id, status, enrollment_date, source_system, created_at)
VALUES (-1, -1, NULL, NULL, N'Unknown', NULL, NULL, NULL, N'unknown', NULL, N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_child OFF;
GO

SET IDENTITY_INSERT dw.dim_domain ON;
INSERT INTO dw.dim_domain (domain_key, domain_id, domain_name, domain_description, domain_status, source_system, created_at)
VALUES (-1, -1, N'Unknown', NULL, N'unknown', N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_domain OFF;
GO

SET IDENTITY_INSERT dw.dim_task ON;
INSERT INTO dw.dim_task (task_key, task_template_id, task_title, domain_id, domain_name, is_template_based, task_description, task_status, source_system, created_at)
VALUES (-1, NULL, N'Unknown', NULL, NULL, 0, NULL, N'unknown', N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_task OFF;
GO

SET IDENTITY_INSERT dw.dim_score_scale ON;
INSERT INTO dw.dim_score_scale (score_scale_key, score_scale_id, scale_name, min_score, max_score, scale_description, scale_status, source_system, created_at)
VALUES (-1, -1, N'Unknown', NULL, NULL, NULL, N'unknown', N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_score_scale OFF;
GO

SET IDENTITY_INSERT dw.dim_assessment_status ON;
INSERT INTO dw.dim_assessment_status (assessment_status_key, assessment_status_code, assessment_status_title, assessment_status_category, is_successful_assessment, is_failure_assessment, source_system, created_at)
VALUES (-1, N'unknown', N'Unknown', N'unknown', 0, 0, N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_assessment_status OFF;
GO

SET IDENTITY_INSERT dw.dim_no_score_reason ON;
INSERT INTO dw.dim_no_score_reason (no_score_reason_key, no_score_reason_id, reason_title, reason_description, reason_category, is_child_related, is_teacher_related, is_center_related, is_system_related, source_system, created_at)
VALUES (-1, -1, N'Unknown', NULL, N'unknown', 0, 0, 0, 0, N'PROGRAM_OPS', SYSDATETIME());
SET IDENTITY_INSERT dw.dim_no_score_reason OFF;
GO

/*=============================================================================
  MART 1 Fact Tables
=============================================================================*/

CREATE TABLE dw.fact_tran_student_task_progress (
    student_task_progress_key       BIGINT IDENTITY(1,1) NOT NULL,
    date_key                        INT NULL,
    child_key                       INT NULL,
    center_key                      INT NULL,
    teacher_key                     INT NULL,
    domain_key                      INT NULL,
    task_key                        INT NULL,
    score_scale_key                 INT NULL,
    assessment_status_key           INT NULL,
    no_score_reason_key             INT NULL,

    attempt_no                      INT NULL,
    raw_score                       DECIMAL(10,2) NULL,
    normalized_score                DECIMAL(10,4) NULL,
    is_completed                    BIT NULL,
    is_planned                      BIT NULL,
    is_scored                       BIT NULL,
    is_not_scored                   BIT NULL,
    is_cancelled                    BIT NULL,
    is_incomplete                   BIT NULL,
    is_refused                      BIT NULL,
    is_absent                       BIT NULL,
    is_center_closed                BIT NULL,
    is_assessed                     BIT NULL,

    source_daily_task_assignment_id BIGINT NULL,
    source_task_assessment_id       BIGINT NULL,
    source_assessment_session_id    BIGINT NULL,
    source_child_task_plan_id       BIGINT NULL,
    source_system                   NVARCHAR(100) NULL,
    etl_batch_id                    INT NULL,
    loaded_at                       DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_daily_student_task_progress (
    daily_student_task_progress_key BIGINT IDENTITY(1,1) NOT NULL,
    date_key                        INT NULL,
    child_key                       INT NULL,
    center_key                      INT NULL,
    teacher_key                     INT NULL,

    raw_score                       DECIMAL(10,2) NULL,
    min_score                       DECIMAL(10,2) NULL,
    max_score                       DECIMAL(10,2) NULL,
    normalized_score                DECIMAL(10,4) NULL,
    planned_task_count              INT NULL,
    assessment_count                INT NULL,
    completed_task_count            INT NULL,
    scored_task_count               INT NULL,
    not_scored_task_count           INT NULL,

    source_system                   NVARCHAR(100) NULL,
    etl_batch_id                    INT NULL,
    loaded_at                       DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_child_snapshot_accumulation (
    child_snapshot_key              BIGINT IDENTITY(1,1) NOT NULL,
    snapshot_date_key               INT NULL,
    child_key                       INT NULL,
    center_key                      INT NULL,
    teacher_key                     INT NULL,

    planned_task_count              INT NULL,
    assessment_count                INT NULL,
    completed_task_count            INT NULL,
    scored_task_count               INT NULL,

    first_plan_date_key             INT NULL,
    last_plan_date_key              INT NULL,
    first_assessment_date_key       INT NULL,
    last_assessment_date_key        INT NULL,
    source_system                   NVARCHAR(100) NULL,
    etl_batch_id                    INT NULL,
    loaded_at                       DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_child_task_event (
    child_task_event_key            BIGINT IDENTITY(1,1) NOT NULL,
    child_key                       INT NULL,
    task_key                        INT NULL,
    teacher_key                     INT NULL,
    center_key                      INT NULL,
    domain_key                      INT NULL,
    date_key                        INT NULL,

    event_type                      NVARCHAR(50) NULL,
    event_status                    NVARCHAR(50) NULL,
    raw_score                       DECIMAL(10,2) NULL,
    normalized_score                DECIMAL(10,4) NULL,
    source_daily_task_assignment_id BIGINT NULL,
    source_task_assessment_id       BIGINT NULL,
    source_assessment_session_id    BIGINT NULL,
    source_system                   NVARCHAR(100) NULL,
    etl_batch_id                    INT NULL,
    loaded_at                       DATETIME2(0) NULL
);
GO

/*=============================================================================
  Optional Query Performance Indexes

  For maximum ETL load speed, keep these disabled/commented during large loads.
  After loading, you can add indexes based on reporting queries.
=============================================================================*/

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_tran_student_task_progress
--     ON dw.fact_tran_student_task_progress;
-- GO

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_daily_student_task_progress
--     ON dw.fact_daily_student_task_progress;
-- GO

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_child_snapshot_accumulation
--     ON dw.fact_child_snapshot_accumulation;
-- GO

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_child_task_event
--     ON dw.fact_child_task_event;
-- GO

PRINT 'MART 1 DW tables created without PK/FK/UQ/CHECK/DEFAULT constraints.';
GO
