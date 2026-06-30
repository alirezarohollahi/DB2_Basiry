
/*=============================================================================
  File: 27_create_dw_etl_work_tables.sql

  Purpose:
      Create permanent DW work tables used by ETL procedures instead of SQL
      Server local temporary tables.

  Important:
      These tables are operational staging/work tables inside Charity_DW_DB.
      ETL procedures TRUNCATE and reuse them.

  Concurrency note:
      These work tables are designed for one MART1 ETL job at a time.
      Do not run first-load and incremental jobs concurrently.
=============================================================================*/

USE Charity_DW_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_work')
BEGIN
    EXEC(N'CREATE SCHEMA etl_work');
END;
GO

DROP TABLE IF EXISTS etl_work.w_dim_center;
CREATE TABLE etl_work.w_dim_center (
    center_id       INT NULL,
    center_name     NVARCHAR(200) NULL,
    city            NVARCHAR(100) NULL,
    address         NVARCHAR(500) NULL,
    center_status   NVARCHAR(30) NULL,
    effective_from  DATETIME2(0) NULL,
    effective_to    DATETIME2(0) NULL,
    is_current      BIT NULL,
    source_system   NVARCHAR(100) NULL,
    row_hash        VARBINARY(32) NULL,
    created_at      DATETIME2(0) NULL,
    updated_at      DATETIME2(0) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_dim_teacher;
CREATE TABLE etl_work.w_dim_teacher (
    teacher_id          INT NULL,
    first_name          NVARCHAR(100) NULL,
    last_name           NVARCHAR(100) NULL,
    full_name           NVARCHAR(220) NULL,
    center_id           INT NULL,
    center_name         NVARCHAR(200) NULL,
    employment_status   NVARCHAR(50) NULL,
    effective_from      DATETIME2(0) NULL,
    effective_to        DATETIME2(0) NULL,
    is_current          BIT NULL,
    source_system       NVARCHAR(100) NULL,
    row_hash            VARBINARY(32) NULL,
    created_at          DATETIME2(0) NULL,
    updated_at          DATETIME2(0) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_dim_child;
CREATE TABLE etl_work.w_dim_child (
    child_key       INT NULL,
    child_id        INT NULL,
    first_name      NVARCHAR(100) NULL,
    last_name       NVARCHAR(100) NULL,
    full_name       NVARCHAR(220) NULL,
    birth_date      DATE NULL,
    gender          NVARCHAR(20) NULL,
    center_id       INT NULL,
    status          NVARCHAR(50) NULL,
    enrollment_date DATE NULL,
    source_system   NVARCHAR(100) NULL,
    row_hash        VARBINARY(32) NULL,
    created_at      DATETIME2(0) NULL,
    updated_at      DATETIME2(0) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_dim_domain;
CREATE TABLE etl_work.w_dim_domain (
    domain_key          INT NULL,
    domain_id           INT NULL,
    domain_name         NVARCHAR(200) NULL,
    domain_description  NVARCHAR(MAX) NULL,
    domain_status       NVARCHAR(30) NULL,
    source_system       NVARCHAR(100) NULL,
    row_hash            VARBINARY(32) NULL,
    created_at          DATETIME2(0) NULL,
    updated_at          DATETIME2(0) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_dim_task;
CREATE TABLE etl_work.w_dim_task (
    task_key            INT NULL,
    task_template_id    INT NULL,
    task_title          NVARCHAR(300) NULL,
    domain_id           INT NULL,
    domain_name         NVARCHAR(200) NULL,
    is_template_based   BIT NULL,
    task_description    NVARCHAR(MAX) NULL,
    task_status         NVARCHAR(30) NULL,
    source_system       NVARCHAR(100) NULL,
    row_hash            VARBINARY(32) NULL,
    created_at          DATETIME2(0) NULL,
    updated_at          DATETIME2(0) NULL,
    natural_task_code   NVARCHAR(500) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_dim_score_scale;
CREATE TABLE etl_work.w_dim_score_scale (
    score_scale_key         INT NULL,
    score_scale_id          INT NULL,
    scale_name              NVARCHAR(100) NULL,
    min_score               DECIMAL(10,2) NULL,
    max_score               DECIMAL(10,2) NULL,
    scale_description       NVARCHAR(MAX) NULL,
    scale_status            NVARCHAR(30) NULL,
    source_system           NVARCHAR(100) NULL,
    row_hash                VARBINARY(32) NULL,
    created_at              DATETIME2(0) NULL,
    updated_at              DATETIME2(0) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_dim_assessment_status;
CREATE TABLE etl_work.w_dim_assessment_status (
    assessment_status_key       INT NULL,
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

DROP TABLE IF EXISTS etl_work.w_dim_no_score_reason;
CREATE TABLE etl_work.w_dim_no_score_reason (
    no_score_reason_key     INT NULL,
    no_score_reason_id      INT NULL,
    reason_title            NVARCHAR(200) NULL,
    reason_description      NVARCHAR(MAX) NULL,
    reason_category         NVARCHAR(100) NULL,
    is_child_related        BIT NULL,
    is_teacher_related      BIT NULL,
    is_center_related       BIT NULL,
    is_system_related       BIT NULL,
    source_system           NVARCHAR(100) NULL,
    row_hash                VARBINARY(32) NULL,
    created_at              DATETIME2(0) NULL,
    updated_at              DATETIME2(0) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_fact_tran_student_task_progress;
CREATE TABLE etl_work.w_fact_tran_student_task_progress (
    date_key                         INT NULL,
    child_key                        INT NULL,
    center_key                       INT NULL,
    teacher_key                      INT NULL,
    domain_key                       INT NULL,
    task_key                         INT NULL,
    score_scale_key                  INT NULL,
    assessment_status_key            INT NULL,
    no_score_reason_key              INT NULL,
    attempt_no                       INT NULL,
    raw_score                        DECIMAL(10,2) NULL,
    normalized_score                 DECIMAL(10,4) NULL,
    is_completed                     BIT NULL,
    is_planned                       BIT NULL,
    is_scored                        BIT NULL,
    is_not_scored                    BIT NULL,
    is_cancelled                     BIT NULL,
    is_incomplete                    BIT NULL,
    is_refused                       BIT NULL,
    is_absent                        BIT NULL,
    is_center_closed                 BIT NULL,
    is_assessed                      BIT NULL,
    source_daily_task_assignment_id  BIGINT NULL,
    source_task_assessment_id        BIGINT NULL,
    source_assessment_session_id     BIGINT NULL,
    source_child_task_plan_id        BIGINT NULL,
    source_system                    NVARCHAR(100) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_fact_child_task_event;
CREATE TABLE etl_work.w_fact_child_task_event (
    child_key                        INT NULL,
    task_key                         INT NULL,
    teacher_key                      INT NULL,
    center_key                       INT NULL,
    domain_key                       INT NULL,
    date_key                         INT NULL,
    event_type                       NVARCHAR(50) NULL,
    event_status                     NVARCHAR(50) NULL,
    raw_score                        DECIMAL(10,2) NULL,
    normalized_score                 DECIMAL(10,4) NULL,
    source_daily_task_assignment_id  BIGINT NULL,
    source_task_assessment_id        BIGINT NULL,
    source_assessment_session_id     BIGINT NULL,
    source_system                    NVARCHAR(100) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_fact_daily_student_task_progress;
CREATE TABLE etl_work.w_fact_daily_student_task_progress (
    date_key                 INT NULL,
    child_key                INT NULL,
    center_key               INT NULL,
    teacher_key              INT NULL,
    raw_score                DECIMAL(10,2) NULL,
    min_score                DECIMAL(10,2) NULL,
    max_score                DECIMAL(10,2) NULL,
    normalized_score         DECIMAL(10,4) NULL,
    planned_task_count       INT NULL,
    assessment_count         INT NULL,
    completed_task_count     INT NULL,
    scored_task_count        INT NULL,
    not_scored_task_count    INT NULL,
    source_system            NVARCHAR(100) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_fact_child_snapshot_old;
CREATE TABLE etl_work.w_fact_child_snapshot_old (
    snapshot_date_key          INT NULL,
    child_key                  INT NULL,
    center_key                 INT NULL,
    teacher_key                INT NULL,
    planned_task_count         INT NULL,
    assessment_count           INT NULL,
    completed_task_count       INT NULL,
    scored_task_count          INT NULL,
    first_plan_date_key        INT NULL,
    last_plan_date_key         INT NULL,
    first_assessment_date_key  INT NULL,
    last_assessment_date_key   INT NULL,
    source_system              NVARCHAR(100) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_fact_child_snapshot_period;
CREATE TABLE etl_work.w_fact_child_snapshot_period (
    snapshot_date_key          INT NULL,
    child_key                  INT NULL,
    center_key                 INT NULL,
    teacher_key                INT NULL,
    planned_task_count         INT NULL,
    assessment_count           INT NULL,
    completed_task_count       INT NULL,
    scored_task_count          INT NULL,
    first_plan_date_key        INT NULL,
    last_plan_date_key         INT NULL,
    first_assessment_date_key  INT NULL,
    last_assessment_date_key   INT NULL,
    source_system              NVARCHAR(100) NULL
);
GO

DROP TABLE IF EXISTS etl_work.w_fact_child_snapshot_final;
CREATE TABLE etl_work.w_fact_child_snapshot_final (
    snapshot_date_key          INT NULL,
    child_key                  INT NULL,
    center_key                 INT NULL,
    teacher_key                INT NULL,
    planned_task_count         INT NULL,
    assessment_count           INT NULL,
    completed_task_count       INT NULL,
    scored_task_count          INT NULL,
    first_plan_date_key        INT NULL,
    last_plan_date_key         INT NULL,
    first_assessment_date_key  INT NULL,
    last_assessment_date_key   INT NULL,
    source_system              NVARCHAR(100) NULL
);
GO

PRINT 'Created Charity_DW_DB.etl_work permanent work tables.';
GO
