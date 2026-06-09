# Finance Operations Source-to-Staging ETL

## Script

`sql/04_etl/10_create_etl_finance_ops_to_staging_procedures.sql`

## Important change

This script does **not** use SQL Server `UPDATE` then `INSERT`.

## Source

`Source_FinanceOps_DB.finance_ops`

## Target

`Stg_FinanceOps_DB.stg_finance_ops`

## Main procedure

```sql
USE Stg_FinanceOps_DB;
GO

EXEC etl_admin.usp_run_stg_finance_ops_all
    @to_date = '2025-12-31 23:59:59';
```

## Loading strategies

### Small tables

These tables are expected to stay small, so they use:

```sql
TRUNCATE TABLE target;
INSERT INTO target (...)
SELECT ...
FROM validated_source;
```

Tables:

- `donors`
- `campaigns`
- `expense_categories`

### Large / transactional / growing tables

These tables do **not** use truncate and do **not** use full reload.

They use:

```sql
UPDATE existing rows where row_hash changed;

INSERT new rows that do not exist in staging;
```

Tables:

- `donations`
- `expenses`
- `payments`
- `budget_allocations`
- `financial_transactions`
- `currency_rates`

## Validation

Each procedure validates rows before loading them to staging.

Examples:

- required fields are not null
- amounts are positive
- date ranges are valid
- status/type values are valid
- parent references exist in the finance source

## Logging

Each procedure logs to:

- `etl_admin.etl_batch`
- `etl_admin.etl_load_log`

Logged values include:

- rows read
- rows inserted
- rows updated
- rows rejected
- status
- start/end time
- error message
