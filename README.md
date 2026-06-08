# Charity Data Warehouse Project

Target DBMS:
- Microsoft SQL Server
- Designed for SQL Server Management Studio (SSMS)

Current phase:
- Phase 1: Create and populate the operational source databases.

Included SQL scripts:
1. `sql/01_source/01_create_source_program_ops_db.sql`
2. `sql/01_source/02_create_source_finance_ops_db.sql`
3. `sql/01_source/03_insert_sample_program_ops_data.sql`

Important fix in this version:
- The program operations sample-data script no longer assumes identity values start from 1.
- It inserts parent records first.
- It captures real generated IDs into variables using natural/business values.
- Child inserts use those variables, which avoids foreign key errors caused by unexpected identity values.

Recommended execution order in SSMS:
1. Run `sql/01_source/01_create_source_program_ops_db.sql`
2. Run `sql/01_source/02_create_source_finance_ops_db.sql`
3. Run `sql/01_source/03_insert_sample_program_ops_data.sql`
