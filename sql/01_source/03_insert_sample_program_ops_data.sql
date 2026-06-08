/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 1 - Operational Source Sample Data
 File         : 03_insert_sample_program_ops_data.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Insert realistic sample data into Source_ProgramOps_DB.program_ops.

 Important fix:
   This script does NOT assume identity IDs start from 1.
   It inserts parent rows first, captures real generated IDs using SELECT queries,
   and then uses those IDs for dependent child rows.

 Prerequisite:
   Run this first:
   - 01_create_source_program_ops_db.sql
===============================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE Source_ProgramOps_DB;
GO

BEGIN TRY
    BEGIN TRANSACTION;

    /*=========================================================================
      1. Clear Existing Sample Data
      Delete child tables first, then parent tables.
    =========================================================================*/

    DELETE FROM program_ops.audit_logs;
    DELETE FROM program_ops.note_batch_items;
    DELETE FROM program_ops.note_batches;
    DELETE FROM program_ops.notes;
    DELETE FROM program_ops.task_assessments;
    DELETE FROM program_ops.assessment_sessions;
    DELETE FROM program_ops.daily_task_assignments;
    DELETE FROM program_ops.child_task_plans;
    DELETE FROM program_ops.child_daily_status;
    DELETE FROM program_ops.center_daily_status;
    DELETE FROM program_ops.task_templates;
    DELETE FROM program_ops.users;
    DELETE FROM program_ops.teachers;
    DELETE FROM program_ops.children;
    DELETE FROM program_ops.no_score_reasons;
    DELETE FROM program_ops.absence_reasons;
    DELETE FROM program_ops.closure_reasons;
    DELETE FROM program_ops.score_scales;
    DELETE FROM program_ops.domains;
    DELETE FROM program_ops.centers;

    /*=========================================================================
      2. Parent / Master Tables
    =========================================================================*/

    INSERT INTO program_ops.centers
        (name, city, address, is_active, created_at, updated_at)
    VALUES
        (N'Hope Learning Center - Tehran', N'Tehran', N'No. 12, Valiasr Street, Tehran', 1, '2025-01-01 08:00:00', NULL),
        (N'Kind Steps Center - Shiraz', N'Shiraz', N'No. 45, Eram Boulevard, Shiraz', 1, '2025-01-02 08:00:00', NULL),
        (N'Bright Future Center - Isfahan', N'Isfahan', N'No. 78, Chaharbagh Street, Isfahan', 1, '2025-01-03 08:00:00', NULL),
        (N'Archived Pilot Center - Karaj', N'Karaj', N'Old pilot site, Karaj', 0, '2024-08-01 08:00:00', '2025-02-01 12:00:00');

    DECLARE
        @center_tehran  INT,
        @center_shiraz  INT,
        @center_isfahan INT,
        @center_karaj   INT;

    SELECT @center_tehran  = id FROM program_ops.centers WHERE name = N'Hope Learning Center - Tehran';
    SELECT @center_shiraz  = id FROM program_ops.centers WHERE name = N'Kind Steps Center - Shiraz';
    SELECT @center_isfahan = id FROM program_ops.centers WHERE name = N'Bright Future Center - Isfahan';
    SELECT @center_karaj   = id FROM program_ops.centers WHERE name = N'Archived Pilot Center - Karaj';

    INSERT INTO program_ops.children
        (center_id, first_name, last_name, national_code, birth_date, gender, enrollment_date, status, created_at, updated_at)
    VALUES
        (@center_tehran,  N'Ali',   N'Ahmadi',   N'0012345671', '2018-03-12', N'male',   '2025-01-10', N'active',   '2025-01-10 09:00:00', NULL),
        (@center_tehran,  N'Sara',  N'Moradi',   N'0012345672', '2017-07-22', N'female', '2025-01-12', N'active',   '2025-01-12 09:00:00', NULL),
        (@center_tehran,  N'Reza',  N'Karimi',   N'0012345673', '2019-01-05', N'male',   '2025-02-01', N'active',   '2025-02-01 09:00:00', NULL),
        (@center_shiraz,  N'Nika',  N'Rahimi',   N'0012345674', '2018-11-15', N'female', '2025-01-15', N'active',   '2025-01-15 09:00:00', NULL),
        (@center_shiraz,  N'Matin', N'Hosseini', N'0012345675', '2017-09-09', N'male',   '2025-02-03', N'active',   '2025-02-03 09:00:00', NULL),
        (@center_isfahan, N'Yasna', N'Sadeghi',  N'0012345676', '2018-05-30', N'female', '2025-01-20', N'active',   '2025-01-20 09:00:00', NULL),
        (@center_isfahan, N'Armin', N'Nazari',   N'0012345677', '2016-12-18', N'male',   '2025-01-25', N'inactive', '2025-01-25 09:00:00', '2025-04-01 10:00:00');

    DECLARE
        @child_ali   INT,
        @child_sara  INT,
        @child_reza  INT,
        @child_nika  INT,
        @child_matin INT,
        @child_yasna INT,
        @child_armin INT;

    SELECT @child_ali   = id FROM program_ops.children WHERE national_code = N'0012345671';
    SELECT @child_sara  = id FROM program_ops.children WHERE national_code = N'0012345672';
    SELECT @child_reza  = id FROM program_ops.children WHERE national_code = N'0012345673';
    SELECT @child_nika  = id FROM program_ops.children WHERE national_code = N'0012345674';
    SELECT @child_matin = id FROM program_ops.children WHERE national_code = N'0012345675';
    SELECT @child_yasna = id FROM program_ops.children WHERE national_code = N'0012345676';
    SELECT @child_armin = id FROM program_ops.children WHERE national_code = N'0012345677';

    INSERT INTO program_ops.teachers
        (center_id, first_name, last_name, phone, email, employment_status, is_active, created_at, updated_at)
    VALUES
        (@center_tehran,  N'Mina',    N'Jafari',  N'09120000001', N'mina.jafari@example.org',   N'full_time', 1, '2025-01-05 08:30:00', NULL),
        (@center_tehran,  N'Omid',    N'Farhadi', N'09120000002', N'omid.farhadi@example.org',  N'part_time', 1, '2025-01-05 08:30:00', NULL),
        (@center_shiraz,  N'Laleh',   N'Abbasi',  N'09120000003', N'laleh.abbasi@example.org',  N'full_time', 1, '2025-01-06 08:30:00', NULL),
        (@center_isfahan, N'Hamed',   N'Zarei',   N'09120000004', N'hamed.zarei@example.org',   N'full_time', 1, '2025-01-07 08:30:00', NULL),
        (@center_isfahan, N'Nazanin', N'Azimi',   N'09120000005', N'nazanin.azimi@example.org', N'inactive',  0, '2024-09-01 08:30:00', '2025-03-15 11:00:00');

    DECLARE
        @teacher_mina    INT,
        @teacher_omid    INT,
        @teacher_laleh   INT,
        @teacher_hamed   INT,
        @teacher_nazanin INT;

    SELECT @teacher_mina    = id FROM program_ops.teachers WHERE email = N'mina.jafari@example.org';
    SELECT @teacher_omid    = id FROM program_ops.teachers WHERE email = N'omid.farhadi@example.org';
    SELECT @teacher_laleh   = id FROM program_ops.teachers WHERE email = N'laleh.abbasi@example.org';
    SELECT @teacher_hamed   = id FROM program_ops.teachers WHERE email = N'hamed.zarei@example.org';
    SELECT @teacher_nazanin = id FROM program_ops.teachers WHERE email = N'nazanin.azimi@example.org';

    INSERT INTO program_ops.users
        (username, password_hash, role, teacher_id, is_active, created_at, updated_at)
    VALUES
        (N'admin',                 N'HASH_ADMIN_SAMPLE',   N'admin',          NULL,          1, '2025-01-01 08:00:00', NULL),
        (N'mina.jafari',           N'HASH_MINA_SAMPLE',    N'teacher',        @teacher_mina, 1, '2025-01-05 09:00:00', NULL),
        (N'omid.farhadi',          N'HASH_OMID_SAMPLE',    N'teacher',        @teacher_omid, 1, '2025-01-05 09:00:00', NULL),
        (N'laleh.abbasi',          N'HASH_LALEH_SAMPLE',   N'teacher',        @teacher_laleh,1, '2025-01-06 09:00:00', NULL),
        (N'hamed.zarei',           N'HASH_HAMED_SAMPLE',   N'teacher',        @teacher_hamed,1, '2025-01-07 09:00:00', NULL),
        (N'center.manager.tehran',  N'HASH_MANAGER_SAMPLE', N'center_manager', NULL,          1, '2025-01-08 09:00:00', NULL);

    DECLARE
        @user_admin         INT,
        @user_mina          INT,
        @user_omid          INT,
        @user_laleh         INT,
        @user_hamed         INT,
        @user_tehran_mgr    INT;

    SELECT @user_admin      = id FROM program_ops.users WHERE username = N'admin';
    SELECT @user_mina       = id FROM program_ops.users WHERE username = N'mina.jafari';
    SELECT @user_omid       = id FROM program_ops.users WHERE username = N'omid.farhadi';
    SELECT @user_laleh      = id FROM program_ops.users WHERE username = N'laleh.abbasi';
    SELECT @user_hamed      = id FROM program_ops.users WHERE username = N'hamed.zarei';
    SELECT @user_tehran_mgr = id FROM program_ops.users WHERE username = N'center.manager.tehran';

    INSERT INTO program_ops.domains
        (name, description, is_active, created_at, updated_at)
    VALUES
        (N'Communication',      N'Communication, speech, and response skills.', 1, '2025-01-01 08:00:00', NULL),
        (N'Motor Skills',       N'Fine and gross motor skill activities.', 1, '2025-01-01 08:00:00', NULL),
        (N'Cognitive Skills',   N'Memory, attention, classification, and problem solving.', 1, '2025-01-01 08:00:00', NULL),
        (N'Social Interaction', N'Peer interaction, turn-taking, and group activity.', 1, '2025-01-01 08:00:00', NULL),
        (N'Self Care',          N'Basic independence and daily living activities.', 1, '2025-01-01 08:00:00', NULL);

    DECLARE
        @domain_comm       INT,
        @domain_motor      INT,
        @domain_cognitive  INT,
        @domain_social     INT,
        @domain_selfcare   INT;

    SELECT @domain_comm      = id FROM program_ops.domains WHERE name = N'Communication';
    SELECT @domain_motor     = id FROM program_ops.domains WHERE name = N'Motor Skills';
    SELECT @domain_cognitive = id FROM program_ops.domains WHERE name = N'Cognitive Skills';
    SELECT @domain_social    = id FROM program_ops.domains WHERE name = N'Social Interaction';
    SELECT @domain_selfcare  = id FROM program_ops.domains WHERE name = N'Self Care';

    INSERT INTO program_ops.score_scales
        (name, min_score, max_score, description, is_active, created_at, updated_at)
    VALUES
        (N'Binary 0-1',       0, 1,   N'0 = not achieved, 1 = achieved.', 1, '2025-01-01 08:00:00', NULL),
        (N'Rating 0-5',       0, 5,   N'Rating scale from 0 to 5.', 1, '2025-01-01 08:00:00', NULL),
        (N'Percentage 0-100', 0, 100, N'Percentage score from 0 to 100.', 1, '2025-01-01 08:00:00', NULL);

    DECLARE
        @scale_binary INT,
        @scale_rating INT,
        @scale_pct    INT;

    SELECT @scale_binary = id FROM program_ops.score_scales WHERE name = N'Binary 0-1';
    SELECT @scale_rating = id FROM program_ops.score_scales WHERE name = N'Rating 0-5';
    SELECT @scale_pct    = id FROM program_ops.score_scales WHERE name = N'Percentage 0-100';

    INSERT INTO program_ops.closure_reasons
        (title, description, is_active, created_at, updated_at)
    VALUES
        (N'Public Holiday', N'Official public holiday.', 1, '2025-01-01 08:00:00', NULL),
        (N'Weather', N'Closure due to unsafe weather conditions.', 1, '2025-01-01 08:00:00', NULL),
        (N'Maintenance', N'Center closed for building maintenance.', 1, '2025-01-01 08:00:00', NULL);

    DECLARE
        @closure_public_holiday INT,
        @closure_weather        INT,
        @closure_maintenance    INT;

    SELECT @closure_public_holiday = id FROM program_ops.closure_reasons WHERE title = N'Public Holiday';
    SELECT @closure_weather        = id FROM program_ops.closure_reasons WHERE title = N'Weather';
    SELECT @closure_maintenance    = id FROM program_ops.closure_reasons WHERE title = N'Maintenance';

    INSERT INTO program_ops.absence_reasons
        (title, description, is_active, created_at, updated_at)
    VALUES
        (N'Illness', N'Child was sick.', 1, '2025-01-01 08:00:00', NULL),
        (N'Family Reason', N'Family-related absence.', 1, '2025-01-01 08:00:00', NULL),
        (N'Transport Issue', N'Child could not attend due to transport issue.', 1, '2025-01-01 08:00:00', NULL);

    DECLARE
        @absence_illness   INT,
        @absence_family    INT,
        @absence_transport INT;

    SELECT @absence_illness   = id FROM program_ops.absence_reasons WHERE title = N'Illness';
    SELECT @absence_family    = id FROM program_ops.absence_reasons WHERE title = N'Family Reason';
    SELECT @absence_transport = id FROM program_ops.absence_reasons WHERE title = N'Transport Issue';

    INSERT INTO program_ops.no_score_reasons
        (title, description, is_active, created_at, updated_at)
    VALUES
        (N'Absent', N'Child was absent and could not be assessed.', 1, '2025-01-01 08:00:00', NULL),
        (N'Refused', N'Child refused to perform the task.', 1, '2025-01-01 08:00:00', NULL),
        (N'Incomplete', N'Task was started but not completed.', 1, '2025-01-01 08:00:00', NULL),
        (N'Center Closed', N'Assessment did not happen because the center was closed.', 1, '2025-01-01 08:00:00', NULL);

    DECLARE
        @noscore_absent        INT,
        @noscore_refused       INT,
        @noscore_incomplete    INT,
        @noscore_center_closed INT;

    SELECT @noscore_absent        = id FROM program_ops.no_score_reasons WHERE title = N'Absent';
    SELECT @noscore_refused       = id FROM program_ops.no_score_reasons WHERE title = N'Refused';
    SELECT @noscore_incomplete    = id FROM program_ops.no_score_reasons WHERE title = N'Incomplete';
    SELECT @noscore_center_closed = id FROM program_ops.no_score_reasons WHERE title = N'Center Closed';

    /*=========================================================================
      3. Task Templates
    =========================================================================*/

    INSERT INTO program_ops.task_templates
        (domain_id, title, description, default_score_scale_id, is_active, created_by, created_at, updated_at)
    VALUES
        (@domain_comm,      N'Respond to Name',              N'Child responds when name is called.',  @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_comm,      N'Follow One-Step Instruction',  N'Child follows a simple one-step instruction.', @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_motor,     N'Stack Blocks',                 N'Child stacks blocks using fine motor control.', @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_motor,     N'Walk on Line',                 N'Child walks on a straight line for balance.', @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_cognitive, N'Match Colors',                 N'Child matches basic colors.', @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_cognitive, N'Sort Shapes',                  N'Child sorts basic shapes by category.', @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_social,    N'Take Turns',                   N'Child takes turns during a structured activity.', @scale_rating, 1, @user_admin, '2025-01-10 10:00:00', NULL),
        (@domain_selfcare,  N'Wash Hands',                   N'Child follows hand-washing routine.', @scale_binary, 1, @user_admin, '2025-01-10 10:00:00', NULL);

    DECLARE
        @task_respond_name INT,
        @task_instruction  INT,
        @task_stack_blocks INT,
        @task_walk_line    INT,
        @task_match_colors INT,
        @task_sort_shapes  INT,
        @task_take_turns   INT,
        @task_wash_hands   INT;

    SELECT @task_respond_name = id FROM program_ops.task_templates WHERE title = N'Respond to Name';
    SELECT @task_instruction  = id FROM program_ops.task_templates WHERE title = N'Follow One-Step Instruction';
    SELECT @task_stack_blocks = id FROM program_ops.task_templates WHERE title = N'Stack Blocks';
    SELECT @task_walk_line    = id FROM program_ops.task_templates WHERE title = N'Walk on Line';
    SELECT @task_match_colors = id FROM program_ops.task_templates WHERE title = N'Match Colors';
    SELECT @task_sort_shapes  = id FROM program_ops.task_templates WHERE title = N'Sort Shapes';
    SELECT @task_take_turns   = id FROM program_ops.task_templates WHERE title = N'Take Turns';
    SELECT @task_wash_hands   = id FROM program_ops.task_templates WHERE title = N'Wash Hands';

    /*=========================================================================
      4. Daily Status
    =========================================================================*/

    INSERT INTO program_ops.center_daily_status
        (center_id, [date], status, closure_reason_id, note, created_by, created_at, updated_at)
    VALUES
        (@center_tehran,  '2025-05-01', N'open',   NULL,                    NULL,                  @user_tehran_mgr, '2025-05-01 07:30:00', NULL),
        (@center_tehran,  '2025-05-02', N'open',   NULL,                    NULL,                  @user_tehran_mgr, '2025-05-02 07:30:00', NULL),
        (@center_tehran,  '2025-05-03', N'closed', @closure_public_holiday, N'Official holiday.',   @user_tehran_mgr, '2025-05-03 07:30:00', NULL),
        (@center_shiraz,  '2025-05-01', N'open',   NULL,                    NULL,                  @user_laleh,      '2025-05-01 07:30:00', NULL),
        (@center_shiraz,  '2025-05-02', N'open',   NULL,                    NULL,                  @user_laleh,      '2025-05-02 07:30:00', NULL),
        (@center_isfahan, '2025-05-01', N'open',   NULL,                    NULL,                  @user_hamed,      '2025-05-01 07:30:00', NULL),
        (@center_isfahan, '2025-05-02', N'closed', @closure_maintenance,    N'Planned maintenance.',@user_hamed,      '2025-05-02 07:30:00', NULL);

    INSERT INTO program_ops.child_daily_status
        (child_id, [date], status, absence_reason_id, note, created_by, created_at, updated_at)
    VALUES
        (@child_ali,   '2025-05-01', N'present', NULL,               NULL,                         @user_mina,  '2025-05-01 08:00:00', NULL),
        (@child_sara,  '2025-05-01', N'present', NULL,               NULL,                         @user_mina,  '2025-05-01 08:00:00', NULL),
        (@child_reza,  '2025-05-01', N'absent',  @absence_illness,   N'Parent reported illness.',   @user_mina,  '2025-05-01 08:00:00', NULL),
        (@child_nika,  '2025-05-01', N'present', NULL,               NULL,                         @user_laleh, '2025-05-01 08:00:00', NULL),
        (@child_matin, '2025-05-01', N'present', NULL,               NULL,                         @user_laleh, '2025-05-01 08:00:00', NULL),
        (@child_yasna, '2025-05-01', N'present', NULL,               NULL,                         @user_hamed, '2025-05-01 08:00:00', NULL),

        (@child_ali,   '2025-05-02', N'present', NULL,               NULL,                         @user_mina,  '2025-05-02 08:00:00', NULL),
        (@child_sara,  '2025-05-02', N'absent',  @absence_family,    N'Family appointment.',        @user_mina,  '2025-05-02 08:00:00', NULL),
        (@child_reza,  '2025-05-02', N'present', NULL,               NULL,                         @user_mina,  '2025-05-02 08:00:00', NULL),
        (@child_nika,  '2025-05-02', N'present', NULL,               NULL,                         @user_laleh, '2025-05-02 08:00:00', NULL),
        (@child_matin, '2025-05-02', N'absent',  @absence_transport, N'Transport issue.',           @user_laleh, '2025-05-02 08:00:00', NULL);

    /*=========================================================================
      5. Child Task Plans
    =========================================================================*/

    INSERT INTO program_ops.child_task_plans
        (child_id, task_template_id, domain_id, task_title, score_scale_id, start_date, end_date, is_active, created_by, created_at, updated_at)
    VALUES
        (@child_ali,   @task_respond_name, @domain_comm,      N'Respond to Name',              @scale_rating, '2025-05-01', '2025-05-31', 1, @user_mina,  '2025-04-28 09:00:00', NULL),
        (@child_ali,   @task_stack_blocks, @domain_motor,     N'Stack Blocks',                 @scale_rating, '2025-05-01', '2025-05-31', 1, @user_mina,  '2025-04-28 09:00:00', NULL),
        (@child_sara,  @task_instruction,  @domain_comm,      N'Follow One-Step Instruction',  @scale_rating, '2025-05-01', '2025-05-31', 1, @user_mina,  '2025-04-28 09:00:00', NULL),
        (@child_sara,  @task_match_colors, @domain_cognitive, N'Match Colors',                 @scale_rating, '2025-05-01', '2025-05-31', 1, @user_mina,  '2025-04-28 09:00:00', NULL),
        (@child_reza,  @task_sort_shapes,  @domain_cognitive, N'Sort Shapes',                  @scale_rating, '2025-05-01', '2025-05-31', 1, @user_omid,  '2025-04-28 09:00:00', NULL),
        (@child_nika,  @task_take_turns,   @domain_social,    N'Take Turns',                   @scale_rating, '2025-05-01', '2025-05-31', 1, @user_laleh, '2025-04-28 09:00:00', NULL),
        (@child_matin, @task_wash_hands,   @domain_selfcare,  N'Wash Hands',                   @scale_binary, '2025-05-01', '2025-05-31', 1, @user_laleh, '2025-04-28 09:00:00', NULL),
        (@child_yasna, @task_walk_line,    @domain_motor,     N'Walk on Line',                 @scale_rating, '2025-05-01', '2025-05-31', 1, @user_hamed, '2025-04-28 09:00:00', NULL);

    DECLARE
        @plan_ali_respond   INT,
        @plan_ali_stack     INT,
        @plan_sara_instr    INT,
        @plan_sara_colors   INT,
        @plan_reza_shapes   INT,
        @plan_nika_turns    INT,
        @plan_matin_hands   INT,
        @plan_yasna_walk    INT;

    SELECT @plan_ali_respond = id FROM program_ops.child_task_plans WHERE child_id = @child_ali AND task_title = N'Respond to Name';
    SELECT @plan_ali_stack   = id FROM program_ops.child_task_plans WHERE child_id = @child_ali AND task_title = N'Stack Blocks';
    SELECT @plan_sara_instr  = id FROM program_ops.child_task_plans WHERE child_id = @child_sara AND task_title = N'Follow One-Step Instruction';
    SELECT @plan_sara_colors = id FROM program_ops.child_task_plans WHERE child_id = @child_sara AND task_title = N'Match Colors';
    SELECT @plan_reza_shapes = id FROM program_ops.child_task_plans WHERE child_id = @child_reza AND task_title = N'Sort Shapes';
    SELECT @plan_nika_turns  = id FROM program_ops.child_task_plans WHERE child_id = @child_nika AND task_title = N'Take Turns';
    SELECT @plan_matin_hands = id FROM program_ops.child_task_plans WHERE child_id = @child_matin AND task_title = N'Wash Hands';
    SELECT @plan_yasna_walk  = id FROM program_ops.child_task_plans WHERE child_id = @child_yasna AND task_title = N'Walk on Line';

    /*=========================================================================
      6. Daily Task Assignments
    =========================================================================*/

    INSERT INTO program_ops.daily_task_assignments
        (child_id, [date], child_task_plan_id, task_template_id, domain_id, task_title, score_scale_id, planned_by, status, created_at, updated_at)
    VALUES
        (@child_ali,   '2025-05-01', @plan_ali_respond, @task_respond_name, @domain_comm,      N'Respond to Name',             @scale_rating, @user_mina,  N'completed', '2025-05-01 08:30:00', NULL),
        (@child_ali,   '2025-05-01', @plan_ali_stack,   @task_stack_blocks, @domain_motor,     N'Stack Blocks',                @scale_rating, @user_mina,  N'completed', '2025-05-01 08:30:00', NULL),
        (@child_sara,  '2025-05-01', @plan_sara_instr,  @task_instruction,  @domain_comm,      N'Follow One-Step Instruction', @scale_rating, @user_mina,  N'completed', '2025-05-01 08:30:00', NULL),
        (@child_sara,  '2025-05-01', @plan_sara_colors, @task_match_colors, @domain_cognitive, N'Match Colors',                @scale_rating, @user_mina,  N'completed', '2025-05-01 08:30:00', NULL),
        (@child_reza,  '2025-05-01', @plan_reza_shapes, @task_sort_shapes,  @domain_cognitive, N'Sort Shapes',                 @scale_rating, @user_omid,  N'not_done',  '2025-05-01 08:30:00', NULL),
        (@child_nika,  '2025-05-01', @plan_nika_turns,  @task_take_turns,   @domain_social,    N'Take Turns',                  @scale_rating, @user_laleh, N'completed', '2025-05-01 08:30:00', NULL),
        (@child_matin, '2025-05-01', @plan_matin_hands, @task_wash_hands,   @domain_selfcare,  N'Wash Hands',                  @scale_binary, @user_laleh, N'completed', '2025-05-01 08:30:00', NULL),
        (@child_yasna, '2025-05-01', @plan_yasna_walk,  @task_walk_line,    @domain_motor,     N'Walk on Line',                @scale_rating, @user_hamed, N'completed', '2025-05-01 08:30:00', NULL),

        (@child_ali,   '2025-05-02', @plan_ali_respond, @task_respond_name, @domain_comm,      N'Respond to Name',             @scale_rating, @user_mina,  N'completed', '2025-05-02 08:30:00', NULL),
        (@child_ali,   '2025-05-02', @plan_ali_stack,   @task_stack_blocks, @domain_motor,     N'Stack Blocks',                @scale_rating, @user_mina,  N'completed', '2025-05-02 08:30:00', NULL),
        (@child_sara,  '2025-05-02', @plan_sara_instr,  @task_instruction,  @domain_comm,      N'Follow One-Step Instruction', @scale_rating, @user_mina,  N'not_done',  '2025-05-02 08:30:00', NULL),
        (@child_reza,  '2025-05-02', @plan_reza_shapes, @task_sort_shapes,  @domain_cognitive, N'Sort Shapes',                 @scale_rating, @user_omid,  N'completed', '2025-05-02 08:30:00', NULL),
        (@child_nika,  '2025-05-02', @plan_nika_turns,  @task_take_turns,   @domain_social,    N'Take Turns',                  @scale_rating, @user_laleh, N'completed', '2025-05-02 08:30:00', NULL),
        (@child_matin, '2025-05-02', @plan_matin_hands, @task_wash_hands,   @domain_selfcare,  N'Wash Hands',                  @scale_binary, @user_laleh, N'not_done',  '2025-05-02 08:30:00', NULL);

    DECLARE
        @assign_ali_respond_0501   INT,
        @assign_ali_stack_0501     INT,
        @assign_sara_instr_0501    INT,
        @assign_sara_colors_0501   INT,
        @assign_reza_shapes_0501   INT,
        @assign_nika_turns_0501    INT,
        @assign_matin_hands_0501   INT,
        @assign_yasna_walk_0501    INT,
        @assign_ali_respond_0502   INT,
        @assign_ali_stack_0502     INT,
        @assign_sara_instr_0502    INT,
        @assign_reza_shapes_0502   INT,
        @assign_nika_turns_0502    INT,
        @assign_matin_hands_0502   INT;

    SELECT @assign_ali_respond_0501 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_ali AND [date] = '2025-05-01' AND task_title = N'Respond to Name';
    SELECT @assign_ali_stack_0501   = id FROM program_ops.daily_task_assignments WHERE child_id = @child_ali AND [date] = '2025-05-01' AND task_title = N'Stack Blocks';
    SELECT @assign_sara_instr_0501  = id FROM program_ops.daily_task_assignments WHERE child_id = @child_sara AND [date] = '2025-05-01' AND task_title = N'Follow One-Step Instruction';
    SELECT @assign_sara_colors_0501 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_sara AND [date] = '2025-05-01' AND task_title = N'Match Colors';
    SELECT @assign_reza_shapes_0501 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_reza AND [date] = '2025-05-01' AND task_title = N'Sort Shapes';
    SELECT @assign_nika_turns_0501  = id FROM program_ops.daily_task_assignments WHERE child_id = @child_nika AND [date] = '2025-05-01' AND task_title = N'Take Turns';
    SELECT @assign_matin_hands_0501 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_matin AND [date] = '2025-05-01' AND task_title = N'Wash Hands';
    SELECT @assign_yasna_walk_0501  = id FROM program_ops.daily_task_assignments WHERE child_id = @child_yasna AND [date] = '2025-05-01' AND task_title = N'Walk on Line';

    SELECT @assign_ali_respond_0502 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_ali AND [date] = '2025-05-02' AND task_title = N'Respond to Name';
    SELECT @assign_ali_stack_0502   = id FROM program_ops.daily_task_assignments WHERE child_id = @child_ali AND [date] = '2025-05-02' AND task_title = N'Stack Blocks';
    SELECT @assign_sara_instr_0502  = id FROM program_ops.daily_task_assignments WHERE child_id = @child_sara AND [date] = '2025-05-02' AND task_title = N'Follow One-Step Instruction';
    SELECT @assign_reza_shapes_0502 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_reza AND [date] = '2025-05-02' AND task_title = N'Sort Shapes';
    SELECT @assign_nika_turns_0502  = id FROM program_ops.daily_task_assignments WHERE child_id = @child_nika AND [date] = '2025-05-02' AND task_title = N'Take Turns';
    SELECT @assign_matin_hands_0502 = id FROM program_ops.daily_task_assignments WHERE child_id = @child_matin AND [date] = '2025-05-02' AND task_title = N'Wash Hands';

    /*=========================================================================
      7. Assessment Sessions
    =========================================================================*/

    INSERT INTO program_ops.assessment_sessions
        (child_id, teacher_id, center_id, [date], started_at, ended_at, session_status, general_note, created_at, updated_at)
    VALUES
        (@child_ali,   @teacher_mina,  @center_tehran,  '2025-05-01', '2025-05-01 09:00:00', '2025-05-01 09:25:00', N'completed', N'Good focus today.', '2025-05-01 09:00:00', NULL),
        (@child_sara,  @teacher_mina,  @center_tehran,  '2025-05-01', '2025-05-01 09:30:00', '2025-05-01 09:55:00', N'completed', N'Needed repetition.', '2025-05-01 09:30:00', NULL),
        (@child_reza,  @teacher_omid,  @center_tehran,  '2025-05-01', NULL, NULL, N'cancelled', N'Child absent.', '2025-05-01 10:00:00', NULL),
        (@child_nika,  @teacher_laleh, @center_shiraz,  '2025-05-01', '2025-05-01 10:00:00', '2025-05-01 10:20:00', N'completed', N'Participated well.', '2025-05-01 10:00:00', NULL),
        (@child_matin, @teacher_laleh, @center_shiraz,  '2025-05-01', '2025-05-01 10:30:00', '2025-05-01 10:45:00', N'completed', N'Completed self-care routine.', '2025-05-01 10:30:00', NULL),
        (@child_yasna, @teacher_hamed, @center_isfahan, '2025-05-01', '2025-05-01 11:00:00', '2025-05-01 11:15:00', N'completed', N'Balance improved.', '2025-05-01 11:00:00', NULL),

        (@child_ali,   @teacher_mina,  @center_tehran,  '2025-05-02', '2025-05-02 09:00:00', '2025-05-02 09:20:00', N'completed', N'Consistent performance.', '2025-05-02 09:00:00', NULL),
        (@child_sara,  @teacher_mina,  @center_tehran,  '2025-05-02', NULL, NULL, N'cancelled', N'Family reason absence.', '2025-05-02 09:30:00', NULL),
        (@child_reza,  @teacher_omid,  @center_tehran,  '2025-05-02', '2025-05-02 10:00:00', '2025-05-02 10:25:00', N'completed', N'Returned after illness.', '2025-05-02 10:00:00', NULL),
        (@child_nika,  @teacher_laleh, @center_shiraz,  '2025-05-02', '2025-05-02 10:30:00', '2025-05-02 10:50:00', N'completed', N'Refused once, then completed.', '2025-05-02 10:30:00', NULL),
        (@child_matin, @teacher_laleh, @center_shiraz,  '2025-05-02', NULL, NULL, N'cancelled', N'Transport issue.', '2025-05-02 11:00:00', NULL);

    DECLARE
        @session_ali_0501   INT,
        @session_sara_0501  INT,
        @session_reza_0501  INT,
        @session_nika_0501  INT,
        @session_matin_0501 INT,
        @session_yasna_0501 INT,
        @session_ali_0502   INT,
        @session_sara_0502  INT,
        @session_reza_0502  INT,
        @session_nika_0502  INT,
        @session_matin_0502 INT;

    SELECT @session_ali_0501   = id FROM program_ops.assessment_sessions WHERE child_id = @child_ali   AND [date] = '2025-05-01';
    SELECT @session_sara_0501  = id FROM program_ops.assessment_sessions WHERE child_id = @child_sara  AND [date] = '2025-05-01';
    SELECT @session_reza_0501  = id FROM program_ops.assessment_sessions WHERE child_id = @child_reza  AND [date] = '2025-05-01';
    SELECT @session_nika_0501  = id FROM program_ops.assessment_sessions WHERE child_id = @child_nika  AND [date] = '2025-05-01';
    SELECT @session_matin_0501 = id FROM program_ops.assessment_sessions WHERE child_id = @child_matin AND [date] = '2025-05-01';
    SELECT @session_yasna_0501 = id FROM program_ops.assessment_sessions WHERE child_id = @child_yasna AND [date] = '2025-05-01';
    SELECT @session_ali_0502   = id FROM program_ops.assessment_sessions WHERE child_id = @child_ali   AND [date] = '2025-05-02';
    SELECT @session_sara_0502  = id FROM program_ops.assessment_sessions WHERE child_id = @child_sara  AND [date] = '2025-05-02';
    SELECT @session_reza_0502  = id FROM program_ops.assessment_sessions WHERE child_id = @child_reza  AND [date] = '2025-05-02';
    SELECT @session_nika_0502  = id FROM program_ops.assessment_sessions WHERE child_id = @child_nika  AND [date] = '2025-05-02';
    SELECT @session_matin_0502 = id FROM program_ops.assessment_sessions WHERE child_id = @child_matin AND [date] = '2025-05-02';

    /*=========================================================================
      8. Task Assessments
    =========================================================================*/

    INSERT INTO program_ops.task_assessments
        (daily_task_assignment_id, assessment_session_id, child_id, teacher_id, [date], score, normalized_score, assessment_status, no_score_reason_id, attempt_no, note, created_at, updated_at)
    VALUES
        (@assign_ali_respond_0501, @session_ali_0501,   @child_ali,   @teacher_mina,  '2025-05-01', 4,    80.0000,  N'scored',   NULL,             1, N'Responded after second call.',        '2025-05-01 09:10:00', NULL),
        (@assign_ali_stack_0501,   @session_ali_0501,   @child_ali,   @teacher_mina,  '2025-05-01', 3,    60.0000,  N'scored',   NULL,             1, N'Stacked three blocks.',              '2025-05-01 09:18:00', NULL),
        (@assign_sara_instr_0501,  @session_sara_0501,  @child_sara,  @teacher_mina,  '2025-05-01', 2,    40.0000,  N'scored',   NULL,             1, N'Needed physical prompt.',            '2025-05-01 09:40:00', NULL),
        (@assign_sara_colors_0501, @session_sara_0501,  @child_sara,  @teacher_mina,  '2025-05-01', 5,   100.0000,  N'scored',   NULL,             1, N'Matched all colors.',                 '2025-05-01 09:50:00', NULL),
        (@assign_reza_shapes_0501, @session_reza_0501,  @child_reza,  @teacher_omid,  '2025-05-01', NULL, NULL,      N'no_score', @noscore_absent,  1, N'Absent due to illness.',              '2025-05-01 10:05:00', NULL),
        (@assign_nika_turns_0501,  @session_nika_0501,  @child_nika,  @teacher_laleh, '2025-05-01', 4,    80.0000,  N'scored',   NULL,             1, N'Good peer interaction.',              '2025-05-01 10:12:00', NULL),
        (@assign_matin_hands_0501, @session_matin_0501, @child_matin, @teacher_laleh, '2025-05-01', 1,   100.0000,  N'scored',   NULL,             1, N'Completed independently.',            '2025-05-01 10:38:00', NULL),
        (@assign_yasna_walk_0501,  @session_yasna_0501, @child_yasna, @teacher_hamed, '2025-05-01', 3,    60.0000,  N'scored',   NULL,             1, N'Walked halfway without support.',      '2025-05-01 11:10:00', NULL),

        (@assign_ali_respond_0502, @session_ali_0502,   @child_ali,   @teacher_mina,  '2025-05-02', 5,   100.0000,  N'scored',   NULL,             1, N'Immediate response.',                 '2025-05-02 09:08:00', NULL),
        (@assign_ali_stack_0502,   @session_ali_0502,   @child_ali,   @teacher_mina,  '2025-05-02', 4,    80.0000,  N'scored',   NULL,             1, N'Improved block stacking.',            '2025-05-02 09:16:00', NULL),
        (@assign_sara_instr_0502,  @session_sara_0502,  @child_sara,  @teacher_mina,  '2025-05-02', NULL, NULL,      N'no_score', @noscore_absent,  1, N'Absent for family reason.',            '2025-05-02 09:35:00', NULL),
        (@assign_reza_shapes_0502, @session_reza_0502,  @child_reza,  @teacher_omid,  '2025-05-02', 3,    60.0000,  N'scored',   NULL,             1, N'Sorted circles and squares.',         '2025-05-02 10:15:00', NULL),
        (@assign_nika_turns_0502,  @session_nika_0502,  @child_nika,  @teacher_laleh, '2025-05-02', NULL, NULL,      N'no_score', @noscore_refused, 1, N'Refused group activity.',              '2025-05-02 10:40:00', NULL),
        (@assign_matin_hands_0502, @session_matin_0502, @child_matin, @teacher_laleh, '2025-05-02', NULL, NULL,      N'no_score', @noscore_absent,  1, N'Absent due to transport issue.',       '2025-05-02 11:05:00', NULL);

    DECLARE
        @assessment_sara_instr_0501 INT,
        @assessment_nika_0502       INT;

    SELECT @assessment_sara_instr_0501 = id FROM program_ops.task_assessments WHERE daily_task_assignment_id = @assign_sara_instr_0501;
    SELECT @assessment_nika_0502       = id FROM program_ops.task_assessments WHERE daily_task_assignment_id = @assign_nika_turns_0502;

    /*=========================================================================
      9. Notes and Note Batches
    =========================================================================*/

    INSERT INTO program_ops.notes
        (note_scope, center_id, child_id, teacher_id, [date], daily_task_assignment_id, task_assessment_id, note_text, created_by, created_at, updated_at)
    VALUES
        (N'child_day',   NULL,           @child_ali,  @teacher_mina,  '2025-05-01', NULL,                     NULL,                         N'Ali was calm and cooperative during morning activities.', @user_mina,       '2025-05-01 12:00:00', NULL),
        (N'assessment',  NULL,           @child_sara, @teacher_mina,  '2025-05-01', @assign_sara_instr_0501, @assessment_sara_instr_0501,   N'Sara needed repeated verbal prompts for instruction following.', @user_mina, '2025-05-01 12:05:00', NULL),
        (N'center_day',  @center_tehran, NULL,        NULL,           '2025-05-03', NULL,                     NULL,                         N'Tehran center closed due to public holiday.', @user_tehran_mgr, '2025-05-03 08:00:00', NULL),
        (N'child_day',   NULL,           @child_reza, @teacher_omid,  '2025-05-02', NULL,                     NULL,                         N'Reza returned after illness and participated in cognitive task.', @user_omid, '2025-05-02 12:00:00', NULL),
        (N'assessment',  NULL,           @child_nika, @teacher_laleh, '2025-05-02', @assign_nika_turns_0502,  @assessment_nika_0502,         N'Nika refused turn-taking task today.', @user_laleh, '2025-05-02 12:10:00', NULL);

    DECLARE
        @note_ali_day        INT,
        @note_sara_assess    INT,
        @note_center_holiday INT;

    SELECT @note_ali_day        = id FROM program_ops.notes WHERE note_text = N'Ali was calm and cooperative during morning activities.';
    SELECT @note_sara_assess    = id FROM program_ops.notes WHERE note_text = N'Sara needed repeated verbal prompts for instruction following.';
    SELECT @note_center_holiday = id FROM program_ops.notes WHERE note_text = N'Tehran center closed due to public holiday.';

    INSERT INTO program_ops.note_batches
        (created_by, note_scope, note_text, created_at)
    VALUES
        (@user_tehran_mgr, N'center_day', N'General reminder: update daily statuses before 09:00.', '2025-05-01 07:45:00'),
        (@user_mina,       N'child_day',  N'Children were observed during morning routine.',        '2025-05-01 12:30:00');

    DECLARE
        @batch_center_reminder INT,
        @batch_child_morning   INT;

    SELECT @batch_center_reminder = id FROM program_ops.note_batches WHERE note_text = N'General reminder: update daily statuses before 09:00.';
    SELECT @batch_child_morning   = id FROM program_ops.note_batches WHERE note_text = N'Children were observed during morning routine.';

    INSERT INTO program_ops.note_batch_items
        (note_batch_id, note_id)
    VALUES
        (@batch_child_morning, @note_ali_day),
        (@batch_child_morning, @note_sara_assess);

    /*=========================================================================
      10. Audit Logs
    =========================================================================*/

    INSERT INTO program_ops.audit_logs
        (user_id, entity_name, entity_id, action, old_value, new_value, created_at)
    VALUES
        (@user_admin,      N'centers',                @center_tehran,            N'INSERT', NULL, N'Hope Learning Center - Tehran',                 '2025-01-01 08:00:00'),
        (@user_admin,      N'children',               @child_ali,                N'INSERT', NULL, N'Ali Ahmadi',                                    '2025-01-10 09:00:00'),
        (@user_mina,       N'daily_task_assignments', @assign_ali_respond_0501,  N'INSERT', NULL, N'Respond to Name planned for Ali',               '2025-05-01 08:30:00'),
        (@user_mina,       N'task_assessments',       @assessment_sara_instr_0501,N'INSERT', NULL, N'Score 2 for Follow One-Step Instruction',       '2025-05-01 09:40:00'),
        (@user_tehran_mgr, N'center_daily_status',    @center_tehran,            N'INSERT', NULL, N'Tehran center closed for public holiday',        '2025-05-03 07:30:00');

    COMMIT TRANSACTION;

    PRINT 'Program operations sample data inserted successfully.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    DECLARE
        @ErrorMessage NVARCHAR(4000),
        @ErrorSeverity INT,
        @ErrorState INT;

    SELECT
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE();

    PRINT 'Program operations sample data insert failed. Transaction rolled back.';
    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH;
GO

/*=============================================================================
  11. Validation Queries
=============================================================================*/

PRINT 'Row counts by table:';

SELECT 'centers' AS table_name, COUNT(*) AS row_count FROM program_ops.centers
UNION ALL SELECT 'children', COUNT(*) FROM program_ops.children
UNION ALL SELECT 'teachers', COUNT(*) FROM program_ops.teachers
UNION ALL SELECT 'users', COUNT(*) FROM program_ops.users
UNION ALL SELECT 'domains', COUNT(*) FROM program_ops.domains
UNION ALL SELECT 'score_scales', COUNT(*) FROM program_ops.score_scales
UNION ALL SELECT 'task_templates', COUNT(*) FROM program_ops.task_templates
UNION ALL SELECT 'closure_reasons', COUNT(*) FROM program_ops.closure_reasons
UNION ALL SELECT 'absence_reasons', COUNT(*) FROM program_ops.absence_reasons
UNION ALL SELECT 'no_score_reasons', COUNT(*) FROM program_ops.no_score_reasons
UNION ALL SELECT 'center_daily_status', COUNT(*) FROM program_ops.center_daily_status
UNION ALL SELECT 'child_daily_status', COUNT(*) FROM program_ops.child_daily_status
UNION ALL SELECT 'child_task_plans', COUNT(*) FROM program_ops.child_task_plans
UNION ALL SELECT 'daily_task_assignments', COUNT(*) FROM program_ops.daily_task_assignments
UNION ALL SELECT 'assessment_sessions', COUNT(*) FROM program_ops.assessment_sessions
UNION ALL SELECT 'task_assessments', COUNT(*) FROM program_ops.task_assessments
UNION ALL SELECT 'notes', COUNT(*) FROM program_ops.notes
UNION ALL SELECT 'note_batches', COUNT(*) FROM program_ops.note_batches
UNION ALL SELECT 'note_batch_items', COUNT(*) FROM program_ops.note_batch_items
UNION ALL SELECT 'audit_logs', COUNT(*) FROM program_ops.audit_logs
ORDER BY table_name;
GO

/*=============================================================================
  12. Foreign Key Sanity Checks
=============================================================================*/

PRINT 'Foreign key sanity checks: expected result is zero rows for each check.';

SELECT 'task_templates missing domain' AS check_name, COUNT(*) AS problem_count
FROM program_ops.task_templates t
LEFT JOIN program_ops.domains d ON d.id = t.domain_id
WHERE d.id IS NULL

UNION ALL

SELECT 'child_task_plans missing child', COUNT(*)
FROM program_ops.child_task_plans p
LEFT JOIN program_ops.children c ON c.id = p.child_id
WHERE c.id IS NULL

UNION ALL

SELECT 'daily_task_assignments missing child_task_plan', COUNT(*)
FROM program_ops.daily_task_assignments a
LEFT JOIN program_ops.child_task_plans p ON p.id = a.child_task_plan_id
WHERE a.child_task_plan_id IS NOT NULL AND p.id IS NULL

UNION ALL

SELECT 'task_assessments missing assignment', COUNT(*)
FROM program_ops.task_assessments ta
LEFT JOIN program_ops.daily_task_assignments a ON a.id = ta.daily_task_assignment_id
WHERE a.id IS NULL

UNION ALL

SELECT 'notes missing task_assessment', COUNT(*)
FROM program_ops.notes n
LEFT JOIN program_ops.task_assessments ta ON ta.id = n.task_assessment_id
WHERE n.task_assessment_id IS NOT NULL AND ta.id IS NULL

UNION ALL

SELECT 'note_batch_items missing note_batch', COUNT(*)
FROM program_ops.note_batch_items nbi
LEFT JOIN program_ops.note_batches nb ON nb.id = nbi.note_batch_id
WHERE nb.id IS NULL;
GO
