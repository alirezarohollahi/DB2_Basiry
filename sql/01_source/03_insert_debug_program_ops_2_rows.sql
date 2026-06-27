/*
===============================================================================
 Debug seed data - Program Ops
 Purpose: only 2 fixed rows per table for tracing Source -> Staging -> DW
 Notes:
   - Fixed IDs are used with IDENTITY_INSERT.
   - Fixed datetime values are used.
   - DELETE section makes this script re-runnable for these debug IDs.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Source_ProgramOps_DB;
GO

DECLARE @created_at DATETIME2(0) = '2026-06-01 08:00:00';
DECLARE @updated_at DATETIME2(0) = '2026-06-02 09:30:00';

/* Re-run cleanup: delete children tables first */
DELETE FROM program_ops.audit_logs WHERE id IN (100001, 100002);
DELETE FROM program_ops.note_batch_items WHERE id IN (100001, 100002);
DELETE FROM program_ops.note_batches WHERE id IN (100001, 100002);
DELETE FROM program_ops.notes WHERE id IN (100001, 100002);
DELETE FROM program_ops.task_assessments WHERE id IN (100001, 100002);
DELETE FROM program_ops.assessment_sessions WHERE id IN (100001, 100002);
DELETE FROM program_ops.daily_task_assignments WHERE id IN (100001, 100002);
DELETE FROM program_ops.child_task_plans WHERE id IN (100001, 100002);
DELETE FROM program_ops.child_daily_status WHERE id IN (100001, 100002);
DELETE FROM program_ops.center_daily_status WHERE id IN (100001, 100002);
DELETE FROM program_ops.task_templates WHERE id IN (100001, 100002);
DELETE FROM program_ops.score_scales WHERE id IN (100001, 100002);
DELETE FROM program_ops.domains WHERE id IN (100001, 100002);
DELETE FROM program_ops.users WHERE id IN (100001, 100002);
DELETE FROM program_ops.teachers WHERE id IN (100001, 100002);
DELETE FROM program_ops.children WHERE id IN (100001, 100002);
DELETE FROM program_ops.centers WHERE id IN (100001, 100002);
DELETE FROM program_ops.absence_reasons WHERE id IN (100001, 100002);
DELETE FROM program_ops.closure_reasons WHERE id IN (100001, 100002);
DELETE FROM program_ops.no_score_reasons WHERE id IN (100001, 100002);
GO

/* Lookup / master data */
SET IDENTITY_INSERT program_ops.centers ON;
INSERT INTO program_ops.centers (id, name, city, address, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Center A', N'Tehran', N'Debug address A', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Center B', N'Shiraz', N'Debug address B', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.centers OFF;
GO

SET IDENTITY_INSERT program_ops.children ON;
INSERT INTO program_ops.children (id, center_id, first_name, last_name, national_code, birth_date, gender, enrollment_date, status, created_at, updated_at)
VALUES
(100001, 100001, N'DebugChildA', N'TraceA', N'DBG-CH-100001', '2018-04-10', N'male',   '2026-01-10', N'active',   '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, N'DebugChildB', N'TraceB', N'DBG-CH-100002', '2019-07-15', N'female', '2026-01-15', N'active',   '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.children OFF;
GO

SET IDENTITY_INSERT program_ops.teachers ON;
INSERT INTO program_ops.teachers (id, center_id, first_name, last_name, phone, email, employment_status, is_active, created_at, updated_at)
VALUES
(100001, 100001, N'DebugTeacherA', N'TraceA', N'09120000001', N'debug.teacher.a@example.com', N'active', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, N'DebugTeacherB', N'TraceB', N'09120000002', N'debug.teacher.b@example.com', N'active', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.teachers OFF;
GO

SET IDENTITY_INSERT program_ops.users ON;
INSERT INTO program_ops.users (id, username, password_hash, role, teacher_id, is_active, created_at, updated_at)
VALUES
(100001, N'debug_user_a', N'DEBUG_HASH_A', N'teacher', 100001, 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'debug_user_b', N'DEBUG_HASH_B', N'teacher', 100002, 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.users OFF;
GO

SET IDENTITY_INSERT program_ops.domains ON;
INSERT INTO program_ops.domains (id, name, description, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Communication', N'Debug domain for ETL trace A', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Motor Skills',   N'Debug domain for ETL trace B', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.domains OFF;
GO

SET IDENTITY_INSERT program_ops.score_scales ON;
INSERT INTO program_ops.score_scales (id, name, min_score, max_score, description, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Scale 0-5',  0.00, 5.00,  N'Debug score scale A', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Scale 0-10', 0.00, 10.00, N'Debug score scale B', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.score_scales OFF;
GO

SET IDENTITY_INSERT program_ops.closure_reasons ON;
INSERT INTO program_ops.closure_reasons (id, title, description, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Holiday', N'Debug center closure reason A', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Maintenance', N'Debug center closure reason B', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.closure_reasons OFF;
GO

SET IDENTITY_INSERT program_ops.absence_reasons ON;
INSERT INTO program_ops.absence_reasons (id, title, description, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Sick', N'Debug child absence reason A', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Family', N'Debug child absence reason B', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.absence_reasons OFF;
GO

SET IDENTITY_INSERT program_ops.no_score_reasons ON;
INSERT INTO program_ops.no_score_reasons (id, title, description, is_active, created_at, updated_at)
VALUES
(100001, N'DEBUG Not Ready', N'Debug no-score reason A', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'DEBUG Refused',   N'Debug no-score reason B', 1, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.no_score_reasons OFF;
GO

/* Dependent operational data */
SET IDENTITY_INSERT program_ops.task_templates ON;
INSERT INTO program_ops.task_templates (id, domain_id, title, description, default_score_scale_id, is_active, created_by, created_at, updated_at)
VALUES
(100001, 100001, N'DEBUG Eye Contact Task', N'Debug task template A', 100001, 1, 100001, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, N'DEBUG Fine Motor Task',  N'Debug task template B', 100002, 1, 100002, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.task_templates OFF;
GO

SET IDENTITY_INSERT program_ops.center_daily_status ON;
INSERT INTO program_ops.center_daily_status (id, center_id, [date], status, closure_reason_id, note, created_by, created_at, updated_at)
VALUES
(100001, 100001, '2026-06-03', N'open',   NULL,   N'Debug center open day A',   100001, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, '2026-06-03', N'closed', 100002, N'Debug center closed day B', 100002, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.center_daily_status OFF;
GO

SET IDENTITY_INSERT program_ops.child_daily_status ON;
INSERT INTO program_ops.child_daily_status (id, child_id, [date], status, absence_reason_id, note, created_by, created_at, updated_at)
VALUES
(100001, 100001, '2026-06-03', N'present', NULL,   N'Debug child present A', 100001, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, '2026-06-03', N'absent',  100001, N'Debug child absent B',  100002, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.child_daily_status OFF;
GO

SET IDENTITY_INSERT program_ops.child_task_plans ON;
INSERT INTO program_ops.child_task_plans (id, child_id, task_template_id, domain_id, task_title, score_scale_id, start_date, end_date, is_active, created_by, created_at, updated_at)
VALUES
(100001, 100001, 100001, 100001, N'DEBUG Eye Contact Task', 100001, '2026-06-01', '2026-06-30', 1, 100001, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, 100002, 100002, N'DEBUG Fine Motor Task',  100002, '2026-06-01', '2026-06-30', 1, 100002, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.child_task_plans OFF;
GO

SET IDENTITY_INSERT program_ops.daily_task_assignments ON;
INSERT INTO program_ops.daily_task_assignments (id, child_id, [date], child_task_plan_id, task_template_id, domain_id, task_title, score_scale_id, planned_by, status, created_at, updated_at)
VALUES
(100001, 100001, '2026-06-03', 100001, 100001, 100001, N'DEBUG Eye Contact Task', 100001, 100001, N'planned',   '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, '2026-06-03', 100002, 100002, 100002, N'DEBUG Fine Motor Task',  100002, 100002, N'completed', '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.daily_task_assignments OFF;
GO

SET IDENTITY_INSERT program_ops.assessment_sessions ON;
INSERT INTO program_ops.assessment_sessions (id, child_id, teacher_id, center_id, [date], started_at, ended_at, session_status, general_note, created_at, updated_at)
VALUES
(100001, 100001, 100001, 100001, '2026-06-03', '2026-06-03 09:00:00', '2026-06-03 09:30:00', N'closed', N'Debug assessment session A', '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, 100002, 100002, '2026-06-03', '2026-06-03 10:00:00', '2026-06-03 10:30:00', N'closed', N'Debug assessment session B', '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.assessment_sessions OFF;
GO

SET IDENTITY_INSERT program_ops.task_assessments ON;
INSERT INTO program_ops.task_assessments (id, daily_task_assignment_id, assessment_session_id, child_id, teacher_id, [date], score, normalized_score, assessment_status, no_score_reason_id, attempt_no, note, created_at, updated_at)
VALUES
(100001, 100001, 100001, 100001, 100001, '2026-06-03', 4.00, 80.0000, N'scored', NULL,   1, N'Debug scored assessment A',   '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, 100002, 100002, 100002, 100002, '2026-06-03', NULL, NULL,    N'no_score', 100002, 1, N'Debug no-score assessment B', '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.task_assessments OFF;
GO

SET IDENTITY_INSERT program_ops.notes ON;
INSERT INTO program_ops.notes (id, note_scope, center_id, child_id, teacher_id, [date], daily_task_assignment_id, task_assessment_id, note_text, created_by, created_at, updated_at)
VALUES
(100001, N'task_assessment', 100001, 100001, 100001, '2026-06-03', 100001, 100001, N'Debug note A for tracing assessment row 100001', 100001, '2026-06-01 08:00:00', '2026-06-02 09:30:00'),
(100002, N'task_assessment', 100002, 100002, 100002, '2026-06-03', 100002, 100002, N'Debug note B for tracing assessment row 100002', 100002, '2026-06-01 08:00:00', '2026-06-02 09:30:00');
SET IDENTITY_INSERT program_ops.notes OFF;
GO

SET IDENTITY_INSERT program_ops.note_batches ON;
INSERT INTO program_ops.note_batches (id, created_by, note_scope, note_text, created_at)
VALUES
(100001, 100001, N'task_assessment', N'Debug batch note A', '2026-06-01 08:00:00'),
(100002, 100002, N'task_assessment', N'Debug batch note B', '2026-06-01 08:00:00');
SET IDENTITY_INSERT program_ops.note_batches OFF;
GO

SET IDENTITY_INSERT program_ops.note_batch_items ON;
INSERT INTO program_ops.note_batch_items (id, note_batch_id, note_id)
VALUES
(100001, 100001, 100001),
(100002, 100002, 100002);
SET IDENTITY_INSERT program_ops.note_batch_items OFF;
GO

SET IDENTITY_INSERT program_ops.audit_logs ON;
INSERT INTO program_ops.audit_logs (id, user_id, entity_name, entity_id, action, old_value, new_value, created_at)
VALUES
(100001, 100001, N'program_ops.children', 100001, N'INSERT', NULL, N'{"debug":"child A inserted"}', '2026-06-01 08:00:00'),
(100002, 100002, N'program_ops.children', 100002, N'INSERT', NULL, N'{"debug":"child B inserted"}', '2026-06-01 08:00:00');
SET IDENTITY_INSERT program_ops.audit_logs OFF;
GO

PRINT 'Debug Program Ops seed data inserted: 2 fixed rows per table.';
GO
