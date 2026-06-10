# Execution Order

Run these scripts in SQL Server Management Studio:

## Source layer

1. `sql/01_source/01_create_source_program_ops_db.sql`
2. `sql/01_source/02_create_source_finance_ops_db.sql`
3. `sql/01_source/03_insert_sample_program_ops_data.sql`
4. `sql/01_source/04_insert_sample_finance_ops_data.sql`

## Staging layer

5. `sql/02_staging/05_create_stg_program_ops_db.sql`
6. `sql/02_staging/06_create_stg_program_ops_tables.sql`
7. `sql/02_staging/07_create_stg_finance_ops_db.sql`
8. `sql/02_staging/08_create_stg_finance_ops_tables.sql`

## ETL layer

9. `sql/04_etl/09_create_etl_program_ops_to_staging_procedures.sql`
10. `sql/04_etl/10_create_etl_finance_ops_to_staging_procedures.sql`

## Run Finance Ops ETL later

```sql
USE Stg_FinanceOps_DB;
GO

EXEC etl_admin.usp_run_stg_finance_ops_all
    @to_date = '2025-12-31 23:59:59';
```

Finance ETL note:
- No `UPDATE` then `INSERT` command is used.
- Small tables use `TRUNCATE + INSERT`.
- Large tables use `UPDATE existing rows` then `INSERT new rows`.

---

## Phase 3 - Data Warehouse / MART 1

After source and staging scripts are created and staging ETL scripts are ready, run the warehouse scripts in this order:

1. `sql/03_warehouse/11_create_dw_db.sql`
   - Creates `Charity_DW_DB`
   - Creates schemas: `dw`, `etl_admin`
   - Creates DW ETL batch/log control tables

2. `sql/03_warehouse/12_create_dw_mart1_tables.sql`
   - Creates MART 1 dimensions:
     - `dw.dim_date`
     - `dw.dim_center`
     - `dw.dim_teacher`
     - `dw.dim_child`
     - `dw.dim_domain`
     - `dw.dim_task`
     - `dw.dim_score_scale`
     - `dw.dim_assessment_status`
     - `dw.dim_no_score_reason`
   - Creates MART 1 facts:
     - `dw.fact_tran_student_task_progress`
     - `dw.fact_daily_student_task_progress`
     - `dw.fact_child_snapshot_accumulation`
     - `dw.fact_child_task_event`
   - Adds unknown dimension rows with surrogate key `-1`
   - Adds foreign keys and query indexes

Next step after this phase:
- Build ETL procedures from `Stg_ProgramOps_DB.stg_program_ops` to `Charity_DW_DB.dw` for MART 1 dimensions and facts.

---

## Warehouse MART 2 Table Creation

After MART 1 tables are created, run:

```sql
:r sql/03_warehouse/13_create_dw_mart2_tables.sql
```

This creates the charity financial mart tables:

- `dw.dim_donor`
- `dw.dim_campaign`
- `dw.dim_category`
- `dw.dim_donation_type`
- `dw.dim_status`
- `dw.dim_currency`
- `dw.dim_allocation_type`
- `dw.fact_donation_transaction`
- `dw.fact_monthly_financial_snapshot`
- `dw.fact_donation_lifecycle`
- `dw.fact_budget_allocation_event`

MART 2 reuses shared dimensions from MART 1:

- `dw.dim_date`
- `dw.dim_center`
- `dw.dim_child`
