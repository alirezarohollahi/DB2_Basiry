# Phase 1 Execution Order

Run these scripts in SQL Server Management Studio:

1. `sql/01_source/01_create_source_program_ops_db.sql`
2. `sql/01_source/02_create_source_finance_ops_db.sql`
3. `sql/01_source/03_insert_sample_program_ops_data.sql`

The sample-data script is designed to avoid foreign key errors by:
- inserting parent tables first
- reading generated IDs into variables
- using those variables for child tables
- wrapping the load in a transaction
