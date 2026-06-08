# Charity Data Warehouse Project

Target DBMS:
- Microsoft SQL Server
- Designed for SQL Server Management Studio (SSMS)

Current phase:
- Phase 1: Create the operational source databases.

Included SQL scripts:
1. `sql/01_source/01_create_source_program_ops_db.sql`
2. `sql/01_source/02_create_source_finance_ops_db.sql`

Source database naming decision:
- `Source_ProgramOps_DB`
  - Operational source system for centers, children, teachers, tasks, assessments, attendance/status, notes, and audit logs.
  - Schema: `program_ops`

- `Source_FinanceOps_DB`
  - Operational source system for donors, campaigns, donations, expenses, payments, budget allocations, transactions, and currency rates.
  - Schema: `finance_ops`

Important modeling decision:
- Some business entities are shared conceptually across both systems:
  - center
  - child
  - teacher
  - date

- In the operational source layer, each source system stays independent.
- Cross-database foreign keys are intentionally avoided.
- Finance tables store business reference IDs such as `center_id`, `child_id`, and `teacher_id`.
- In the warehouse layer, these will become conformed dimensions:
  - `dw.dim_center`
  - `dw.dim_child`
  - `dw.dim_teacher`
  - `dw.dim_date`

Recommended execution order in SSMS:
1. Run `sql/01_source/01_create_source_program_ops_db.sql`
2. Run `sql/01_source/02_create_source_finance_ops_db.sql`

Later phases will add:
- staging schemas
- data warehouse dimensions and facts
- ETL loading procedures
- data quality checks
- OLAP views / semantic layer
- BI dashboards
