/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 1 - Operational Source Databases
 File         : 01_create_source_program_ops_db.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create the operational source database for Program Operations:
   centers, children, teachers, users, domains, task templates, score scales,
   daily statuses, task plans, task assignments, assessment sessions,
   task assessments, notes, note batches, and audit logs.

 Naming decision:
   Database : Source_ProgramOps_DB
   Schema   : program_ops

 Why this name:
   This source is wider than only child/teacher data. It represents the program
   operations of the charity/education/therapy system.

 Notes:
   - This script is development-friendly and re-runnable.
   - All operational source tables are created under schema program_ops.
   - This is a normalized OLTP-style source database.
   - It will later feed staging and then the dimensional warehouse.
===============================================================================
*/

SET NOCOUNT ON;
GO

/*=============================================================================
  1. Create Database
=============================================================================*/

IF DB_ID(N'Source_ProgramOps_DB') IS NULL
BEGIN
    CREATE DATABASE Source_ProgramOps_DB;
END
GO

USE Source_ProgramOps_DB;
GO

/*=============================================================================
  2. Create Schema
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'program_ops'
)
BEGIN
    EXEC(N'CREATE SCHEMA program_ops');
END
GO

/*=============================================================================
  3. Drop Existing Tables
     This makes the script re-runnable during development.
=============================================================================*/

DROP TABLE IF EXISTS program_ops.audit_logs;
DROP TABLE IF EXISTS program_ops.note_batch_items;
DROP TABLE IF EXISTS program_ops.note_batches;
DROP TABLE IF EXISTS program_ops.notes;
DROP TABLE IF EXISTS program_ops.task_assessments;
DROP TABLE IF EXISTS program_ops.assessment_sessions;
DROP TABLE IF EXISTS program_ops.daily_task_assignments;
DROP TABLE IF EXISTS program_ops.child_task_plans;
DROP TABLE IF EXISTS program_ops.child_daily_status;
DROP TABLE IF EXISTS program_ops.absence_reasons;
DROP TABLE IF EXISTS program_ops.center_daily_status;
DROP TABLE IF EXISTS program_ops.closure_reasons;
DROP TABLE IF EXISTS program_ops.task_templates;
DROP TABLE IF EXISTS program_ops.score_scales;
DROP TABLE IF EXISTS program_ops.domains;
DROP TABLE IF EXISTS program_ops.users;
DROP TABLE IF EXISTS program_ops.teachers;
DROP TABLE IF EXISTS program_ops.children;
DROP TABLE IF EXISTS program_ops.centers;
DROP TABLE IF EXISTS program_ops.no_score_reasons;
GO

/*=============================================================================
  4. Core Master Tables
=============================================================================*/

CREATE TABLE program_ops.centers (
    id              INT IDENTITY(1,1) NOT NULL,
    name            NVARCHAR(200) NOT NULL,
    city            NVARCHAR(100) NULL,
    address         NVARCHAR(500) NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_centers_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_centers_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_centers PRIMARY KEY CLUSTERED (id)
);
GO

CREATE TABLE program_ops.children (
    id                  INT IDENTITY(1,1) NOT NULL,
    center_id           INT NOT NULL,
    first_name          NVARCHAR(100) NOT NULL,
    last_name           NVARCHAR(100) NOT NULL,
    national_code       NVARCHAR(20) NULL,
    birth_date          DATE NULL,
    gender              NVARCHAR(20) NULL,
    enrollment_date     DATE NULL,
    status              NVARCHAR(50) NOT NULL CONSTRAINT DF_program_children_status DEFAULT (N'active'),
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_program_children_created_at DEFAULT (SYSDATETIME()),
    updated_at          DATETIME2(0) NULL,

    CONSTRAINT PK_program_children PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_children_centers
        FOREIGN KEY (center_id) REFERENCES program_ops.centers(id),
    CONSTRAINT UQ_program_children_national_code UNIQUE (national_code)
);
GO

CREATE TABLE program_ops.teachers (
    id                  INT IDENTITY(1,1) NOT NULL,
    center_id           INT NOT NULL,
    first_name          NVARCHAR(100) NOT NULL,
    last_name           NVARCHAR(100) NOT NULL,
    phone               NVARCHAR(30) NULL,
    email               NVARCHAR(255) NULL,
    employment_status   NVARCHAR(50) NOT NULL CONSTRAINT DF_program_teachers_employment_status DEFAULT (N'active'),
    is_active           BIT NOT NULL CONSTRAINT DF_program_teachers_is_active DEFAULT (1),
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_program_teachers_created_at DEFAULT (SYSDATETIME()),
    updated_at          DATETIME2(0) NULL,

    CONSTRAINT PK_program_teachers PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_teachers_centers
        FOREIGN KEY (center_id) REFERENCES program_ops.centers(id),
    CONSTRAINT UQ_program_teachers_email UNIQUE (email)
);
GO

CREATE TABLE program_ops.users (
    id              INT IDENTITY(1,1) NOT NULL,
    username        NVARCHAR(100) NOT NULL,
    password_hash   NVARCHAR(500) NOT NULL,
    role            NVARCHAR(50) NOT NULL,
    teacher_id      INT NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_users_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_users_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_users PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_program_users_username UNIQUE (username),
    CONSTRAINT FK_program_users_teachers
        FOREIGN KEY (teacher_id) REFERENCES program_ops.teachers(id)
);
GO

/*=============================================================================
  5. Assessment Definition Tables
=============================================================================*/

CREATE TABLE program_ops.domains (
    id              INT IDENTITY(1,1) NOT NULL,
    name            NVARCHAR(200) NOT NULL,
    description     NVARCHAR(1000) NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_domains_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_domains_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_domains PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_program_domains_name UNIQUE (name)
);
GO

CREATE TABLE program_ops.score_scales (
    id              INT IDENTITY(1,1) NOT NULL,
    name            NVARCHAR(100) NOT NULL,
    min_score       DECIMAL(10,2) NOT NULL,
    max_score       DECIMAL(10,2) NOT NULL,
    description     NVARCHAR(1000) NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_score_scales_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_score_scales_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_score_scales PRIMARY KEY CLUSTERED (id),
    CONSTRAINT CK_program_score_scales_range CHECK (min_score <= max_score),
    CONSTRAINT UQ_program_score_scales_name UNIQUE (name)
);
GO

CREATE TABLE program_ops.task_templates (
    id                      INT IDENTITY(1,1) NOT NULL,
    domain_id               INT NOT NULL,
    title                   NVARCHAR(300) NOT NULL,
    description             NVARCHAR(2000) NULL,
    default_score_scale_id  INT NULL,
    is_active               BIT NOT NULL CONSTRAINT DF_program_task_templates_is_active DEFAULT (1),
    created_by              INT NULL,
    created_at              DATETIME2(0) NOT NULL CONSTRAINT DF_program_task_templates_created_at DEFAULT (SYSDATETIME()),
    updated_at              DATETIME2(0) NULL,

    CONSTRAINT PK_program_task_templates PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_task_templates_domains
        FOREIGN KEY (domain_id) REFERENCES program_ops.domains(id),
    CONSTRAINT FK_program_task_templates_score_scales
        FOREIGN KEY (default_score_scale_id) REFERENCES program_ops.score_scales(id),
    CONSTRAINT FK_program_task_templates_users
        FOREIGN KEY (created_by) REFERENCES program_ops.users(id)
);
GO

/*=============================================================================
  6. Daily Status Lookup Tables
=============================================================================*/

CREATE TABLE program_ops.closure_reasons (
    id              INT IDENTITY(1,1) NOT NULL,
    title           NVARCHAR(200) NOT NULL,
    description     NVARCHAR(1000) NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_closure_reasons_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_closure_reasons_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_closure_reasons PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_program_closure_reasons_title UNIQUE (title)
);
GO

CREATE TABLE program_ops.absence_reasons (
    id              INT IDENTITY(1,1) NOT NULL,
    title           NVARCHAR(200) NOT NULL,
    description     NVARCHAR(1000) NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_absence_reasons_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_absence_reasons_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_absence_reasons PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_program_absence_reasons_title UNIQUE (title)
);
GO

CREATE TABLE program_ops.no_score_reasons (
    id              INT IDENTITY(1,1) NOT NULL,
    title           NVARCHAR(200) NOT NULL,
    description     NVARCHAR(1000) NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_program_no_score_reasons_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_no_score_reasons_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_program_no_score_reasons PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_program_no_score_reasons_title UNIQUE (title)
);
GO

/*=============================================================================
  7. Daily Operational Status Tables
=============================================================================*/

CREATE TABLE program_ops.center_daily_status (
    id                  INT IDENTITY(1,1) NOT NULL,
    center_id           INT NOT NULL,
    [date]              DATE NOT NULL,
    status              NVARCHAR(50) NOT NULL,
    closure_reason_id   INT NULL,
    note                NVARCHAR(2000) NULL,
    created_by          INT NULL,
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_program_center_daily_status_created_at DEFAULT (SYSDATETIME()),
    updated_at          DATETIME2(0) NULL,

    CONSTRAINT PK_program_center_daily_status PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_center_daily_status_centers
        FOREIGN KEY (center_id) REFERENCES program_ops.centers(id),
    CONSTRAINT FK_program_center_daily_status_closure_reasons
        FOREIGN KEY (closure_reason_id) REFERENCES program_ops.closure_reasons(id),
    CONSTRAINT FK_program_center_daily_status_users
        FOREIGN KEY (created_by) REFERENCES program_ops.users(id),
    CONSTRAINT UQ_program_center_daily_status_center_date UNIQUE (center_id, [date])
);
GO

CREATE TABLE program_ops.child_daily_status (
    id                  INT IDENTITY(1,1) NOT NULL,
    child_id            INT NOT NULL,
    [date]              DATE NOT NULL,
    status              NVARCHAR(50) NOT NULL,
    absence_reason_id   INT NULL,
    note                NVARCHAR(2000) NULL,
    created_by          INT NULL,
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_program_child_daily_status_created_at DEFAULT (SYSDATETIME()),
    updated_at          DATETIME2(0) NULL,

    CONSTRAINT PK_program_child_daily_status PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_child_daily_status_children
        FOREIGN KEY (child_id) REFERENCES program_ops.children(id),
    CONSTRAINT FK_program_child_daily_status_absence_reasons
        FOREIGN KEY (absence_reason_id) REFERENCES program_ops.absence_reasons(id),
    CONSTRAINT FK_program_child_daily_status_users
        FOREIGN KEY (created_by) REFERENCES program_ops.users(id),
    CONSTRAINT UQ_program_child_daily_status_child_date UNIQUE (child_id, [date])
);
GO

/*=============================================================================
  8. Task Planning and Assignment Tables
=============================================================================*/

CREATE TABLE program_ops.child_task_plans (
    id                  INT IDENTITY(1,1) NOT NULL,
    child_id            INT NOT NULL,
    task_template_id    INT NULL,
    domain_id           INT NOT NULL,
    task_title          NVARCHAR(300) NOT NULL,
    score_scale_id      INT NOT NULL,
    start_date          DATE NOT NULL,
    end_date            DATE NULL,
    is_active           BIT NOT NULL CONSTRAINT DF_program_child_task_plans_is_active DEFAULT (1),
    created_by          INT NULL,
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_program_child_task_plans_created_at DEFAULT (SYSDATETIME()),
    updated_at          DATETIME2(0) NULL,

    CONSTRAINT PK_program_child_task_plans PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_child_task_plans_children
        FOREIGN KEY (child_id) REFERENCES program_ops.children(id),
    CONSTRAINT FK_program_child_task_plans_task_templates
        FOREIGN KEY (task_template_id) REFERENCES program_ops.task_templates(id),
    CONSTRAINT FK_program_child_task_plans_domains
        FOREIGN KEY (domain_id) REFERENCES program_ops.domains(id),
    CONSTRAINT FK_program_child_task_plans_score_scales
        FOREIGN KEY (score_scale_id) REFERENCES program_ops.score_scales(id),
    CONSTRAINT FK_program_child_task_plans_users
        FOREIGN KEY (created_by) REFERENCES program_ops.users(id),
    CONSTRAINT CK_program_child_task_plans_date_range CHECK (end_date IS NULL OR start_date <= end_date)
);
GO

CREATE TABLE program_ops.daily_task_assignments (
    id                      INT IDENTITY(1,1) NOT NULL,
    child_id                INT NOT NULL,
    [date]                  DATE NOT NULL,
    child_task_plan_id      INT NULL,
    task_template_id        INT NULL,
    domain_id               INT NOT NULL,
    task_title              NVARCHAR(300) NOT NULL,
    score_scale_id          INT NOT NULL,
    planned_by              INT NULL,
    status                  NVARCHAR(50) NOT NULL CONSTRAINT DF_program_daily_task_assignments_status DEFAULT (N'planned'),
    created_at              DATETIME2(0) NOT NULL CONSTRAINT DF_program_daily_task_assignments_created_at DEFAULT (SYSDATETIME()),
    updated_at              DATETIME2(0) NULL,

    CONSTRAINT PK_program_daily_task_assignments PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_daily_task_assignments_children
        FOREIGN KEY (child_id) REFERENCES program_ops.children(id),
    CONSTRAINT FK_program_daily_task_assignments_child_task_plans
        FOREIGN KEY (child_task_plan_id) REFERENCES program_ops.child_task_plans(id),
    CONSTRAINT FK_program_daily_task_assignments_task_templates
        FOREIGN KEY (task_template_id) REFERENCES program_ops.task_templates(id),
    CONSTRAINT FK_program_daily_task_assignments_domains
        FOREIGN KEY (domain_id) REFERENCES program_ops.domains(id),
    CONSTRAINT FK_program_daily_task_assignments_score_scales
        FOREIGN KEY (score_scale_id) REFERENCES program_ops.score_scales(id),
    CONSTRAINT FK_program_daily_task_assignments_users
        FOREIGN KEY (planned_by) REFERENCES program_ops.users(id)
);
GO

/*=============================================================================
  9. Assessment Session and Assessment Result Tables
=============================================================================*/

CREATE TABLE program_ops.assessment_sessions (
    id                  INT IDENTITY(1,1) NOT NULL,
    child_id            INT NOT NULL,
    teacher_id          INT NOT NULL,
    center_id           INT NOT NULL,
    [date]              DATE NOT NULL,
    started_at          DATETIME2(0) NULL,
    ended_at            DATETIME2(0) NULL,
    session_status      NVARCHAR(50) NOT NULL CONSTRAINT DF_program_assessment_sessions_status DEFAULT (N'open'),
    general_note        NVARCHAR(2000) NULL,
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_program_assessment_sessions_created_at DEFAULT (SYSDATETIME()),
    updated_at          DATETIME2(0) NULL,

    CONSTRAINT PK_program_assessment_sessions PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_assessment_sessions_children
        FOREIGN KEY (child_id) REFERENCES program_ops.children(id),
    CONSTRAINT FK_program_assessment_sessions_teachers
        FOREIGN KEY (teacher_id) REFERENCES program_ops.teachers(id),
    CONSTRAINT FK_program_assessment_sessions_centers
        FOREIGN KEY (center_id) REFERENCES program_ops.centers(id),
    CONSTRAINT CK_program_assessment_sessions_time_range CHECK (ended_at IS NULL OR started_at IS NULL OR started_at <= ended_at)
);
GO

CREATE TABLE program_ops.task_assessments (
    id                          INT IDENTITY(1,1) NOT NULL,
    daily_task_assignment_id    INT NOT NULL,
    assessment_session_id       INT NOT NULL,
    child_id                    INT NOT NULL,
    teacher_id                  INT NOT NULL,
    [date]                      DATE NOT NULL,
    score                       DECIMAL(10,2) NULL,
    normalized_score            DECIMAL(10,4) NULL,
    assessment_status           NVARCHAR(50) NOT NULL,
    no_score_reason_id          INT NULL,
    attempt_no                  INT NOT NULL CONSTRAINT DF_program_task_assessments_attempt_no DEFAULT (1),
    note                        NVARCHAR(2000) NULL,
    created_at                  DATETIME2(0) NOT NULL CONSTRAINT DF_program_task_assessments_created_at DEFAULT (SYSDATETIME()),
    updated_at                  DATETIME2(0) NULL,

    CONSTRAINT PK_program_task_assessments PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_task_assessments_daily_task_assignments
        FOREIGN KEY (daily_task_assignment_id) REFERENCES program_ops.daily_task_assignments(id),
    CONSTRAINT FK_program_task_assessments_assessment_sessions
        FOREIGN KEY (assessment_session_id) REFERENCES program_ops.assessment_sessions(id),
    CONSTRAINT FK_program_task_assessments_children
        FOREIGN KEY (child_id) REFERENCES program_ops.children(id),
    CONSTRAINT FK_program_task_assessments_teachers
        FOREIGN KEY (teacher_id) REFERENCES program_ops.teachers(id),
    CONSTRAINT FK_program_task_assessments_no_score_reasons
        FOREIGN KEY (no_score_reason_id) REFERENCES program_ops.no_score_reasons(id),
    CONSTRAINT CK_program_task_assessments_attempt_no CHECK (attempt_no >= 1),
    CONSTRAINT CK_program_task_assessments_normalized_score CHECK (normalized_score IS NULL OR normalized_score BETWEEN 0 AND 100)
);
GO

/*=============================================================================
  10. Notes and Batch Notes
=============================================================================*/

CREATE TABLE program_ops.notes (
    id                          INT IDENTITY(1,1) NOT NULL,
    note_scope                  NVARCHAR(50) NOT NULL,
    center_id                   INT NULL,
    child_id                    INT NULL,
    teacher_id                  INT NULL,
    [date]                      DATE NULL,
    daily_task_assignment_id    INT NULL,
    task_assessment_id          INT NULL,
    note_text                   NVARCHAR(MAX) NOT NULL,
    created_by                  INT NULL,
    created_at                  DATETIME2(0) NOT NULL CONSTRAINT DF_program_notes_created_at DEFAULT (SYSDATETIME()),
    updated_at                  DATETIME2(0) NULL,

    CONSTRAINT PK_program_notes PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_notes_centers
        FOREIGN KEY (center_id) REFERENCES program_ops.centers(id),
    CONSTRAINT FK_program_notes_children
        FOREIGN KEY (child_id) REFERENCES program_ops.children(id),
    CONSTRAINT FK_program_notes_teachers
        FOREIGN KEY (teacher_id) REFERENCES program_ops.teachers(id),
    CONSTRAINT FK_program_notes_daily_task_assignments
        FOREIGN KEY (daily_task_assignment_id) REFERENCES program_ops.daily_task_assignments(id),
    CONSTRAINT FK_program_notes_task_assessments
        FOREIGN KEY (task_assessment_id) REFERENCES program_ops.task_assessments(id),
    CONSTRAINT FK_program_notes_users
        FOREIGN KEY (created_by) REFERENCES program_ops.users(id)
);
GO

CREATE TABLE program_ops.note_batches (
    id              INT IDENTITY(1,1) NOT NULL,
    created_by      INT NULL,
    note_scope      NVARCHAR(50) NOT NULL,
    note_text       NVARCHAR(MAX) NOT NULL,
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_note_batches_created_at DEFAULT (SYSDATETIME()),

    CONSTRAINT PK_program_note_batches PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_note_batches_users
        FOREIGN KEY (created_by) REFERENCES program_ops.users(id)
);
GO

CREATE TABLE program_ops.note_batch_items (
    id              INT IDENTITY(1,1) NOT NULL,
    note_batch_id   INT NOT NULL,
    note_id         INT NOT NULL,

    CONSTRAINT PK_program_note_batch_items PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_note_batch_items_note_batches
        FOREIGN KEY (note_batch_id) REFERENCES program_ops.note_batches(id),
    CONSTRAINT FK_program_note_batch_items_notes
        FOREIGN KEY (note_id) REFERENCES program_ops.notes(id),
    CONSTRAINT UQ_program_note_batch_items_batch_note UNIQUE (note_batch_id, note_id)
);
GO

/*=============================================================================
  11. Audit Log
=============================================================================*/

CREATE TABLE program_ops.audit_logs (
    id              BIGINT IDENTITY(1,1) NOT NULL,
    user_id         INT NULL,
    entity_name     NVARCHAR(200) NOT NULL,
    entity_id       INT NOT NULL,
    action          NVARCHAR(50) NOT NULL,
    old_value       NVARCHAR(MAX) NULL,
    new_value       NVARCHAR(MAX) NULL,
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_program_audit_logs_created_at DEFAULT (SYSDATETIME()),

    CONSTRAINT PK_program_audit_logs PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_program_audit_logs_users
        FOREIGN KEY (user_id) REFERENCES program_ops.users(id)
);
GO

/*=============================================================================
  12. Helpful Indexes for ETL Extraction
=============================================================================*/

CREATE INDEX IX_program_children_center_id
    ON program_ops.children(center_id);
GO

CREATE INDEX IX_program_children_updated_at
    ON program_ops.children(updated_at);
GO

CREATE INDEX IX_program_teachers_center_id
    ON program_ops.teachers(center_id);
GO

CREATE INDEX IX_program_teachers_updated_at
    ON program_ops.teachers(updated_at);
GO

CREATE INDEX IX_program_center_daily_status_date
    ON program_ops.center_daily_status([date], center_id);
GO

CREATE INDEX IX_program_child_daily_status_date
    ON program_ops.child_daily_status([date], child_id);
GO

CREATE INDEX IX_program_child_task_plans_child_date
    ON program_ops.child_task_plans(child_id, start_date, end_date);
GO

CREATE INDEX IX_program_daily_task_assignments_date
    ON program_ops.daily_task_assignments([date], child_id);
GO

CREATE INDEX IX_program_assessment_sessions_date
    ON program_ops.assessment_sessions([date], child_id, teacher_id);
GO

CREATE INDEX IX_program_task_assessments_date
    ON program_ops.task_assessments([date], child_id, teacher_id);
GO

CREATE INDEX IX_program_audit_logs_entity
    ON program_ops.audit_logs(entity_name, entity_id, created_at);
GO

/*=============================================================================
  13. Completion Message
=============================================================================*/

PRINT 'Source_ProgramOps_DB created successfully.';
PRINT 'Schema created: program_ops';
PRINT 'Phase 1 script completed: Program Operations operational source database.';
GO
