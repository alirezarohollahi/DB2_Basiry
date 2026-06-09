# Charity Data Warehouse Project

Target DBMS:
- Microsoft SQL Server
- Designed for SQL Server Management Studio (SSMS)

Current phase:
- Phase 1: Create and populate operational source databases.
- Phase 2: Create staging databases/tables and source-to-staging ETL procedures for both source systems.

Included SQL scripts:
1. `sql/01_source/01_create_source_program_ops_db.sql`
2. `sql/01_source/02_create_source_finance_ops_db.sql`
3. `sql/01_source/03_insert_sample_program_ops_data.sql`
4. `sql/01_source/04_insert_sample_finance_ops_data.sql`
5. `sql/02_staging/05_create_stg_program_ops_db.sql`
6. `sql/02_staging/06_create_stg_program_ops_tables.sql`
7. `sql/02_staging/07_create_stg_finance_ops_db.sql`
8. `sql/02_staging/08_create_stg_finance_ops_tables.sql`
9. `sql/04_etl/09_create_etl_program_ops_to_staging_procedures.sql`
10. `sql/04_etl/10_create_etl_finance_ops_to_staging_procedures.sql`

Important finance ETL note:
- `10_create_etl_finance_ops_to_staging_procedures.sql` does not use SQL Server `UPDATE` then `INSERT`.
- Small tables use `TRUNCATE TABLE` + `INSERT`.
- Large/transactional tables use `UPDATE` existing rows, then `INSERT` new rows.

Recommended execution order in SSMS:
1. Run `sql/01_source/01_create_source_program_ops_db.sql`
2. Run `sql/01_source/02_create_source_finance_ops_db.sql`
3. Run `sql/01_source/03_insert_sample_program_ops_data.sql`
4. Run `sql/01_source/04_insert_sample_finance_ops_data.sql`
5. Run `sql/02_staging/05_create_stg_program_ops_db.sql`
6. Run `sql/02_staging/06_create_stg_program_ops_tables.sql`
7. Run `sql/02_staging/07_create_stg_finance_ops_db.sql`
8. Run `sql/02_staging/08_create_stg_finance_ops_tables.sql`
9. Run `sql/04_etl/09_create_etl_program_ops_to_staging_procedures.sql`
10. Run `sql/04_etl/10_create_etl_finance_ops_to_staging_procedures.sql`

To run Finance Ops ETL later as a job:

```sql
USE Stg_FinanceOps_DB;
GO

EXEC etl_admin.usp_run_stg_finance_ops_all
    @to_date = '2025-12-31 23:59:59';
```
