# DW MART 2 Design - Charity Financial Analytics

This document summarizes the warehouse tables added for MART 2.

## Scope

MART 2 covers financial analytics for the charity organization. It is based on the finance operational source and uses the staged finance tables as ETL input.

Main analytical areas:

- Donation transactions
- Monthly center financial snapshot
- Donation lifecycle tracking
- Budget allocation events

## Shared dimensions reused from MART 1

MART 2 reuses these shared dimensions from MART 1:

- `dw.dim_date`
- `dw.dim_center`
- `dw.dim_child`

Because these dimensions are already needed by MART 1, the MART 2 table script does not drop or recreate them.

## MART 2 dimensions

The script `sql/03_warehouse/13_create_dw_mart2_tables.sql` creates these MART 2 dimensions:

- `dw.dim_donor`
- `dw.dim_campaign`
- `dw.dim_category`
- `dw.dim_donation_type`
- `dw.dim_status`
- `dw.dim_currency`
- `dw.dim_allocation_type`

`dw.dim_allocation_type` was added because the MART 2 mapping includes `allocation_type_key` for budget allocation events.

## MART 2 facts

The script creates these fact tables:

### `dw.fact_donation_transaction`

Grain: one row per donation transaction.

Important measures:

- `amount`
- `is_confirmed`
- `is_refunded`

Traceability columns:

- `source_donation_id`
- `source_donor_id`
- `source_campaign_id`
- `source_reference_code`

### `dw.fact_monthly_financial_snapshot`

Grain: one row per month and center.

Important measures:

- `total_donation_amount`
- `total_expense_amount`
- `total_payment_amount`
- `net_balance`
- `donation_count`
- `expense_count`
- `payment_count`
- `allocation_count`

`net_balance` is a persisted computed column:

```sql
net_balance = total_donation_amount - total_expense_amount - total_payment_amount
```

### `dw.fact_donation_lifecycle`

Grain: one row per donation lifecycle.

Important measures:

- `donation_amount`
- `days_to_confirm`
- `days_to_allocate`

Lifecycle fields:

- `created_date_key`
- `confirmed_date_key`
- `allocated_date_key`
- `current_stage`
- `lifecycle_status_key`

### `dw.fact_budget_allocation_event`

Grain: one row per budget allocation event.

Important measures:

- `allocated_amount`

Traceability columns:

- `source_allocation_id`
- `source_type`
- `source_id`
- `source_center_id`
- `source_child_id`
- `source_category_id`

## Unknown rows

Every MART 2 dimension has an unknown row with surrogate key `-1`. This keeps fact loads safe when a related dimension member is missing or not yet loaded.

## Execution order

Run these scripts in this order:

1. `sql/03_warehouse/11_create_dw_db.sql`
2. `sql/03_warehouse/12_create_dw_mart1_tables.sql`
3. `sql/03_warehouse/13_create_dw_mart2_tables.sql`

## Next step

The next step is to create DW ETL procedures that load:

1. Shared dimensions
2. MART 2 dimensions
3. MART 2 facts

The ETL should follow the project style already used in staging:

- No `MERGE`
- Good logging
- Validation before loading facts
- `UPDATE` existing rows and `INSERT` new rows for dimensions/facts where appropriate
- Main runner procedure for the mart
