# Naming and Schema Design

## Source layer

The project has two independent operational source systems.

### Program Operations Source
- Database: `Source_ProgramOps_DB`
- Schema: `program_ops`

This source owns centers, children, teachers, users, task planning, assessments, daily statuses, notes, and audit logs.

### Finance Operations Source
- Database: `Source_FinanceOps_DB`
- Schema: `finance_ops`

This source owns donors, campaigns, donations, expenses, payments, budget allocations, transactions, and currency rates.

## Shared entities

The following entities are shared conceptually:

- center
- child
- teacher
- date

In the source layer, the two operational systems stay independent. Cross-database foreign keys are intentionally avoided.

In the warehouse layer, these should become conformed dimensions:

- `dw.dim_center`
- `dw.dim_child`
- `dw.dim_teacher`
- `dw.dim_date`
