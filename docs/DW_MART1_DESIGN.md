# DW MART 1 Design - Student / Child Task Progress

This document summarizes the first warehouse mart created in `sql/03_warehouse/12_create_dw_mart1_tables.sql`.

## Database and schema

- Database: `Charity_DW_DB`
- Main schema: `dw`
- ETL admin schema: `etl_admin`

## Dimensions

MART 1 uses these dimensions:

- `dw.dim_date`
- `dw.dim_center`
- `dw.dim_teacher`
- `dw.dim_child`
- `dw.dim_domain`
- `dw.dim_task`
- `dw.dim_score_scale`
- `dw.dim_assessment_status`
- `dw.dim_no_score_reason`

`dim_center` and `dim_teacher` include Type 2 SCD fields:

- `effective_from`
- `effective_to`
- `is_current`

All dimensions include an unknown/default row with surrogate key `-1` where applicable. This allows fact rows to load even when a dimension lookup fails, while keeping the problem visible for data-quality checks.

## Facts

### `dw.fact_tran_student_task_progress`

Grain: one row per child task assessment / daily assignment transaction.

Used for detailed task-level analytics such as:

- planned/scored/not-scored flags
- raw and normalized scores
- completed/cancelled/incomplete/refused/absent/center-closed flags

### `dw.fact_daily_student_task_progress`

Grain: one row per child / date / center / teacher daily summary.

Used for daily progress reporting such as:

- planned task count
- assessment count
- completed task count
- scored and not-scored task count
- daily raw and normalized score summaries

### `dw.fact_child_snapshot_accumulation`

Grain: one row per child snapshot date.

Used for child-level accumulating progress analysis.

### `dw.fact_child_task_event`

Grain: one row per child task event.

Used for event-based task history and audit-friendly analytics.

## Next ETL phase

The next step is to create ETL procedures from staging to warehouse:

- Load dimensions first.
- Load facts after all dimension keys are available.
- Use `UPDATE + INSERT`, no `MERGE`.
- Keep load logging in `etl_admin.etl_load_log`.
- Add validation and unknown-key handling before fact loading.
