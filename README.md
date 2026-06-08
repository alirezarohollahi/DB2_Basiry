# Charity Data Warehouse Project

Target DBMS:
- Microsoft SQL Server
- Designed for SQL Server Management Studio (SSMS)

Current phase:
- Phase 1: Create and populate operational source databases.
- Phase 2 start: Create the Program Operations staging database.

Included SQL scripts:
1. `sql/01_source/01_create_source_program_ops_db.sql`
2. `sql/01_source/02_create_source_finance_ops_db.sql`
3. `sql/01_source/03_insert_sample_program_ops_data.sql`
4. `sql/01_source/04_insert_sample_finance_ops_data.sql`
5. `sql/02_staging/05_create_stg_program_ops_db.sql`

Recommended execution order in SSMS:
1. Run `sql/01_source/01_create_source_program_ops_db.sql`
2. Run `sql/01_source/02_create_source_finance_ops_db.sql`
3. Run `sql/01_source/03_insert_sample_program_ops_data.sql`
4. Run `sql/01_source/04_insert_sample_finance_ops_data.sql`
5. Run `sql/02_staging/05_create_stg_program_ops_db.sql`

Created databases so far:
- `Source_ProgramOps_DB`
- `Source_FinanceOps_DB`
- `Stg_ProgramOps_DB`

Created schemas so far:
- `Source_ProgramOps_DB.program_ops`
- `Source_FinanceOps_DB.finance_ops`
- `Stg_ProgramOps_DB.stg_program_ops`
- `Stg_ProgramOps_DB.etl_admin`

Important design:
- Source systems remain independent.
- Program staging is separated from source data.
- `etl_admin` is used for ETL batch control and load logging.
- Staging tables will be added in the next step.
