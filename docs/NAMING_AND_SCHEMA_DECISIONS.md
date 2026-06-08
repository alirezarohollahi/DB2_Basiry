# Naming and Schema Design

## Source layer

The project has two independent operational source systems.

### 1. Program Operations Source

Database:
- `Source_ProgramOps_DB`

Schema:
- `program_ops`

Reason:
- The child/teacher/assessment system is broader than only child and teacher data.
- It includes educational/therapy operations, centers, children, teachers, task planning, daily status, assessments, notes, and audit logs.
- `program_ops` is a clearer business name than `src` or `child_teacher`.

### 2. Finance Operations Source

Database:
- `Source_FinanceOps_DB`

Schema:
- `finance_ops`

Reason:
- This source owns donations, campaigns, expenses, payments, allocations, transactions, and currency rates.
- `finance_ops` is explicit and avoids confusion with warehouse finance marts.

## Shared entities

The following entities are shared conceptually:

- center
- child
- teacher
- date

In Phase 1, they are not physically shared as SQL tables across both source databases.

Reason:
- Operational systems should stay independent.
- Cross-database foreign keys make deployment and ETL harder.
- The finance source may only receive `center_id`, `child_id`, and `teacher_id` as reference IDs from another operational system.

## Warehouse layer later

In the data warehouse, shared entities should become conformed dimensions:

- `dw.dim_center`
- `dw.dim_child`
- `dw.dim_teacher`
- `dw.dim_date`

Both marts should use the same dimension keys for these entities.
