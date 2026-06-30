/*
===============================================================================
 Project      : Charity Data Warehouse Project
 File         : 08_create_stg_program_ops_etl_tmp_tables.sql
 Purpose      : Create permanent ETL temporary/work tables for ProgramOps staging
                procedures. These tables replace SQL Server local temp tables
                #src and #valid.

 Important:
   - These are normal permanent tables under stg_program_ops.
   - No IDENTITY, no constraints, no defaults.
   - ETL procedures TRUNCATE them at the start of each run.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Stg_ProgramOps_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stg_program_ops')
BEGIN
    EXEC(N'CREATE SCHEMA stg_program_ops');
END
GO

/* Drop old ETL temp/work tables so this script is re-runnable. */
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_audit_logs_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_audit_logs_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_note_batch_items_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_note_batch_items_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_note_batches_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_note_batches_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_notes_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_notes_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_task_assessments_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_task_assessments_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_assessment_sessions_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_assessment_sessions_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_daily_task_assignments_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_daily_task_assignments_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_child_task_plans_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_child_task_plans_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_child_daily_status_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_child_daily_status_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_center_daily_status_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_center_daily_status_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_task_templates_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_task_templates_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_no_score_reasons_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_no_score_reasons_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_absence_reasons_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_absence_reasons_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_closure_reasons_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_closure_reasons_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_score_scales_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_score_scales_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_domains_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_domains_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_users_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_users_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_teachers_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_teachers_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_children_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_children_src;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_centers_valid;
DROP TABLE IF EXISTS stg_program_ops.etl_tmp_centers_src;
GO

/* Create ETL src and valid work tables. */

/* centers: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_centers_src (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [city] NVARCHAR(100) NULL,
    [address] NVARCHAR(500) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* centers: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_centers_valid (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [city] NVARCHAR(100) NULL,
    [address] NVARCHAR(500) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* children: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_children_src (
    [id] INT NULL,
    [center_id] INT NULL,
    [first_name] NVARCHAR(100) NULL,
    [last_name] NVARCHAR(100) NULL,
    [national_code] NVARCHAR(20) NULL,
    [birth_date] DATE NULL,
    [gender] NVARCHAR(20) NULL,
    [enrollment_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* children: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_children_valid (
    [id] INT NULL,
    [center_id] INT NULL,
    [first_name] NVARCHAR(100) NULL,
    [last_name] NVARCHAR(100) NULL,
    [national_code] NVARCHAR(20) NULL,
    [birth_date] DATE NULL,
    [gender] NVARCHAR(20) NULL,
    [enrollment_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* teachers: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_teachers_src (
    [id] INT NULL,
    [center_id] INT NULL,
    [first_name] NVARCHAR(100) NULL,
    [last_name] NVARCHAR(100) NULL,
    [phone] NVARCHAR(30) NULL,
    [email] NVARCHAR(255) NULL,
    [employment_status] NVARCHAR(50) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* teachers: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_teachers_valid (
    [id] INT NULL,
    [center_id] INT NULL,
    [first_name] NVARCHAR(100) NULL,
    [last_name] NVARCHAR(100) NULL,
    [phone] NVARCHAR(30) NULL,
    [email] NVARCHAR(255) NULL,
    [employment_status] NVARCHAR(50) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* users: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_users_src (
    [id] INT NULL,
    [username] NVARCHAR(100) NULL,
    [password_hash] NVARCHAR(500) NULL,
    [role] NVARCHAR(50) NULL,
    [teacher_id] INT NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* users: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_users_valid (
    [id] INT NULL,
    [username] NVARCHAR(100) NULL,
    [password_hash] NVARCHAR(500) NULL,
    [role] NVARCHAR(50) NULL,
    [teacher_id] INT NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* domains: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_domains_src (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* domains: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_domains_valid (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* score_scales: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_score_scales_src (
    [id] INT NULL,
    [name] NVARCHAR(100) NULL,
    [min_score] DECIMAL(10,2) NULL,
    [max_score] DECIMAL(10,2) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* score_scales: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_score_scales_valid (
    [id] INT NULL,
    [name] NVARCHAR(100) NULL,
    [min_score] DECIMAL(10,2) NULL,
    [max_score] DECIMAL(10,2) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* closure_reasons: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_closure_reasons_src (
    [id] INT NULL,
    [title] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* closure_reasons: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_closure_reasons_valid (
    [id] INT NULL,
    [title] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* absence_reasons: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_absence_reasons_src (
    [id] INT NULL,
    [title] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* absence_reasons: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_absence_reasons_valid (
    [id] INT NULL,
    [title] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* no_score_reasons: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_no_score_reasons_src (
    [id] INT NULL,
    [title] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* no_score_reasons: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_no_score_reasons_valid (
    [id] INT NULL,
    [title] NVARCHAR(200) NULL,
    [description] NVARCHAR(1000) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* task_templates: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_task_templates_src (
    [id] INT NULL,
    [domain_id] INT NULL,
    [title] NVARCHAR(300) NULL,
    [description] NVARCHAR(2000) NULL,
    [default_score_scale_id] INT NULL,
    [is_active] BIT NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* task_templates: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_task_templates_valid (
    [id] INT NULL,
    [domain_id] INT NULL,
    [title] NVARCHAR(300) NULL,
    [description] NVARCHAR(2000) NULL,
    [default_score_scale_id] INT NULL,
    [is_active] BIT NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* center_daily_status: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_center_daily_status_src (
    [id] INT NULL,
    [center_id] INT NULL,
    [date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [closure_reason_id] INT NULL,
    [note] NVARCHAR(2000) NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* center_daily_status: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_center_daily_status_valid (
    [id] INT NULL,
    [center_id] INT NULL,
    [date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [closure_reason_id] INT NULL,
    [note] NVARCHAR(2000) NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* child_daily_status: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_child_daily_status_src (
    [id] INT NULL,
    [child_id] INT NULL,
    [date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [absence_reason_id] INT NULL,
    [note] NVARCHAR(2000) NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* child_daily_status: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_child_daily_status_valid (
    [id] INT NULL,
    [child_id] INT NULL,
    [date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [absence_reason_id] INT NULL,
    [note] NVARCHAR(2000) NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* child_task_plans: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_child_task_plans_src (
    [id] INT NULL,
    [child_id] INT NULL,
    [task_template_id] INT NULL,
    [domain_id] INT NULL,
    [task_title] NVARCHAR(300) NULL,
    [score_scale_id] INT NULL,
    [start_date] DATE NULL,
    [end_date] DATE NULL,
    [is_active] BIT NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* child_task_plans: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_child_task_plans_valid (
    [id] INT NULL,
    [child_id] INT NULL,
    [task_template_id] INT NULL,
    [domain_id] INT NULL,
    [task_title] NVARCHAR(300) NULL,
    [score_scale_id] INT NULL,
    [start_date] DATE NULL,
    [end_date] DATE NULL,
    [is_active] BIT NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* daily_task_assignments: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_daily_task_assignments_src (
    [id] INT NULL,
    [child_id] INT NULL,
    [date] DATE NULL,
    [child_task_plan_id] INT NULL,
    [task_template_id] INT NULL,
    [domain_id] INT NULL,
    [task_title] NVARCHAR(300) NULL,
    [score_scale_id] INT NULL,
    [planned_by] INT NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* daily_task_assignments: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_daily_task_assignments_valid (
    [id] INT NULL,
    [child_id] INT NULL,
    [date] DATE NULL,
    [child_task_plan_id] INT NULL,
    [task_template_id] INT NULL,
    [domain_id] INT NULL,
    [task_title] NVARCHAR(300) NULL,
    [score_scale_id] INT NULL,
    [planned_by] INT NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* assessment_sessions: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_assessment_sessions_src (
    [id] INT NULL,
    [child_id] INT NULL,
    [teacher_id] INT NULL,
    [center_id] INT NULL,
    [date] DATE NULL,
    [started_at] DATETIME2(0) NULL,
    [ended_at] DATETIME2(0) NULL,
    [session_status] NVARCHAR(50) NULL,
    [general_note] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* assessment_sessions: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_assessment_sessions_valid (
    [id] INT NULL,
    [child_id] INT NULL,
    [teacher_id] INT NULL,
    [center_id] INT NULL,
    [date] DATE NULL,
    [started_at] DATETIME2(0) NULL,
    [ended_at] DATETIME2(0) NULL,
    [session_status] NVARCHAR(50) NULL,
    [general_note] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* task_assessments: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_task_assessments_src (
    [id] INT NULL,
    [daily_task_assignment_id] INT NULL,
    [assessment_session_id] INT NULL,
    [child_id] INT NULL,
    [teacher_id] INT NULL,
    [date] DATE NULL,
    [score] DECIMAL(10,2) NULL,
    [normalized_score] DECIMAL(10,4) NULL,
    [assessment_status] NVARCHAR(50) NULL,
    [no_score_reason_id] INT NULL,
    [attempt_no] INT NULL,
    [note] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* task_assessments: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_task_assessments_valid (
    [id] INT NULL,
    [daily_task_assignment_id] INT NULL,
    [assessment_session_id] INT NULL,
    [child_id] INT NULL,
    [teacher_id] INT NULL,
    [date] DATE NULL,
    [score] DECIMAL(10,2) NULL,
    [normalized_score] DECIMAL(10,4) NULL,
    [assessment_status] NVARCHAR(50) NULL,
    [no_score_reason_id] INT NULL,
    [attempt_no] INT NULL,
    [note] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* notes: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_notes_src (
    [id] INT NULL,
    [note_scope] NVARCHAR(50) NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [teacher_id] INT NULL,
    [date] DATE NULL,
    [daily_task_assignment_id] INT NULL,
    [task_assessment_id] INT NULL,
    [note_text] NVARCHAR(MAX) NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* notes: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_notes_valid (
    [id] INT NULL,
    [note_scope] NVARCHAR(50) NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [teacher_id] INT NULL,
    [date] DATE NULL,
    [daily_task_assignment_id] INT NULL,
    [task_assessment_id] INT NULL,
    [note_text] NVARCHAR(MAX) NULL,
    [created_by] INT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* note_batches: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_note_batches_src (
    [id] INT NULL,
    [created_by] INT NULL,
    [note_scope] NVARCHAR(50) NULL,
    [note_text] NVARCHAR(MAX) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* note_batches: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_note_batches_valid (
    [id] INT NULL,
    [created_by] INT NULL,
    [note_scope] NVARCHAR(50) NULL,
    [note_text] NVARCHAR(MAX) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* note_batch_items: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_note_batch_items_src (
    [id] INT NULL,
    [note_batch_id] INT NULL,
    [note_id] INT NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* note_batch_items: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_note_batch_items_valid (
    [id] INT NULL,
    [note_batch_id] INT NULL,
    [note_id] INT NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* audit_logs: replacement for #src */
CREATE TABLE stg_program_ops.etl_tmp_audit_logs_src (
    [id] BIGINT NULL,
    [user_id] INT NULL,
    [entity_name] NVARCHAR(200) NULL,
    [entity_id] INT NULL,
    [action] NVARCHAR(50) NULL,
    [old_value] NVARCHAR(MAX) NULL,
    [new_value] NVARCHAR(MAX) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* audit_logs: replacement for #valid */
CREATE TABLE stg_program_ops.etl_tmp_audit_logs_valid (
    [id] BIGINT NULL,
    [user_id] INT NULL,
    [entity_name] NVARCHAR(200) NULL,
    [entity_id] INT NULL,
    [action] NVARCHAR(50) NULL,
    [old_value] NVARCHAR(MAX) NULL,
    [new_value] NVARCHAR(MAX) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO