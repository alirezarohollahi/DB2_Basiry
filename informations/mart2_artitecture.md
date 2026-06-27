I read all 5 SQL files and understood the current warehouse workflow.

## 1. Overall architecture

Your flow is:

```text
Source_FinanceOps_DB
        |
        |  source-to-staging ETL procedures
        v
Stg_FinanceOps_DB
        |
        |  next procedures we will create later
        v
Charity_DW_DB / MART 2
```

So the current project is separated into:

1. **Operational source layer**
   `Source_FinanceOps_DB.finance_ops`

2. **Staging layer**
   `Stg_FinanceOps_DB.stg_finance_ops`

3. **ETL admin / logging layer**
   `Stg_FinanceOps_DB.etl_admin`

4. **Data warehouse MART 2**
   `Charity_DW_DB.dw`

---

## 2. Source database understanding

File:

```text
02_create_source_finance_ops_db.sql
```

This creates the normalized finance source database:

```text
Source_FinanceOps_DB
schema: finance_ops
```

Main source tables:

| Table                    | Role                                                            |
| ------------------------ | --------------------------------------------------------------- |
| `donors`                 | Donor master data                                               |
| `campaigns`              | Fundraising campaign master data                                |
| `donations`              | Donation transactions                                           |
| `expense_categories`     | Expense category hierarchy                                      |
| `expenses`               | Expense transactions by center / child / category               |
| `payments`               | Payments such as salary, bonus, vendor, refund                  |
| `budget_allocations`     | Allocation of donation/internal budget to center/child/category |
| `financial_transactions` | Optional accounting/audit transaction layer                     |
| `currency_rates`         | Optional currency exchange rates                                |

Important source relationships:

```text
donations.donor_id      -> donors.id
donations.campaign_id   -> campaigns.id
expenses.category_id    -> expense_categories.id
budget_allocations.category_id -> expense_categories.id
expense_categories.parent_id   -> expense_categories.id
```

Also, some IDs are intentionally not foreign keys in this finance source:

```text
center_id
child_id
teacher_id
```

Those belong conceptually to another source system, probably Program Operations, and should be resolved in the warehouse through conformed dimensions like:

```text
dw.dim_center
dw.dim_child
dw.dim_teacher
dw.dim_date
```

But in the uploaded MART 2 script, only these shared dimensions are required:

```text
dw.dim_date
dw.dim_center
dw.dim_child
```

`dw.dim_teacher` is mentioned conceptually in the source comments, but it is not used in the MART 2 table script you uploaded.

---

## 3. Staging database understanding

Files:

```text
07_create_stg_finance_ops_db.sql
08_create_stg_finance_ops_tables.sql
```

The staging database is:

```text
Stg_FinanceOps_DB
schema: stg_finance_ops
admin schema: etl_admin
```

The staging tables mirror the source tables:

```text
stg_finance_ops.donors
stg_finance_ops.campaigns
stg_finance_ops.donations
stg_finance_ops.expense_categories
stg_finance_ops.expenses
stg_finance_ops.payments
stg_finance_ops.budget_allocations
stg_finance_ops.financial_transactions
stg_finance_ops.currency_rates
```

Each staging table has:

```text
stg_row_id
etl_batch_id
source_system
source_database
source_schema
source_table
extracted_at
source_updated_at
row_hash
is_valid
validation_message
```

The staging design is permissive:

```text
No business foreign keys
No strict source constraints
Mostly nullable business columns
Only staging primary key: stg_row_id
```

So staging is designed as a landing layer with ETL metadata.

---

## 4. Source-to-staging ETL understanding

File:

```text
10_create_etl_finance_ops_to_staging_procedures.sql
```

This file already creates procedures to load source data into staging.

Main procedures:

```text
etl_admin.usp_load_stg_finance_ops_donors
etl_admin.usp_load_stg_finance_ops_campaigns
etl_admin.usp_load_stg_finance_ops_expense_categories
etl_admin.usp_load_stg_finance_ops_donations
etl_admin.usp_load_stg_finance_ops_expenses
etl_admin.usp_load_stg_finance_ops_payments
etl_admin.usp_load_stg_finance_ops_budget_allocations
etl_admin.usp_load_stg_finance_ops_financial_transactions
etl_admin.usp_load_stg_finance_ops_currency_rates
```

Main orchestrator:

```text
etl_admin.usp_run_stg_finance_ops_all
```

The orchestrator runs the procedures in this order:

```text
donors
campaigns
expense_categories
donations
expenses
payments
budget_allocations
financial_transactions
currency_rates
```

That order makes sense because master/reference tables are loaded before transactional tables.

Each procedure accepts:

```sql
@to_date DATETIME2(0)
@etl_batch_id INT = NULL
```

The logic is:

```text
1. Create or reuse ETL batch
2. Write load log
3. Extract source rows up to @to_date
4. Calculate row_hash
5. Validate rows
6. Load valid rows into staging
7. Update logs and batch status
```

Loading strategy:

| Table type          | Tables                                                                                                | Strategy                                           |
| ------------------- | ----------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Small/master        | `donors`, `campaigns`, `expense_categories`                                                           | `TRUNCATE + INSERT`                                |
| Large/transactional | `donations`, `expenses`, `payments`, `budget_allocations`, `financial_transactions`, `currency_rates` | update changed rows by `row_hash`, insert new rows |

Important note: the comments say “No UPDATE/INSERT” in some places, but the procedures actually **do use `UPDATE` and `INSERT`**. I think the intended meaning was probably “No MERGE” or “No full reload for large tables.”

Another important note: staging tables have `is_valid` and `validation_message`, but the current source-to-staging procedures only insert **valid rows**. Rejected rows are counted in logs, but they are not inserted into staging with `is_valid = 0`. That is a design decision we should keep in mind.

---

## 5. DW MART 2 understanding

File:

```text
13_create_dw_mart2_tables.sql
```

This creates MART 2:

```text
Charity_DW_DB.dw
```

Purpose:

```text
Charity Financial Analytics
```

The DW script assumes MART 1 already exists and provides shared dimensions:

```text
dw.dim_date
dw.dim_center
dw.dim_child
```

MART 2 creates these finance-specific dimensions:

```text
dw.dim_donor
dw.dim_campaign
dw.dim_category
dw.dim_donation_type
dw.dim_status
dw.dim_currency
dw.dim_allocation_type
```

It also inserts unknown rows with key `-1` into all MART 2 dimensions.

Facts:

| Fact table                        | Grain                                 |
| --------------------------------- | ------------------------------------- |
| `fact_donation_transaction`       | One row per donation transaction      |
| `fact_monthly_financial_snapshot` | One row per month / center summary    |
| `fact_donation_lifecycle`         | One row per donation lifecycle record |
| `fact_budget_allocation_event`    | One row per budget allocation event   |

The DW tables are intentionally fast-load style:

```text
No primary keys
No foreign keys
No unique constraints
No check constraints
No default constraints
No active indexes
Heap tables
Optional columnstore indexes commented at the end
```

So your DW design favors ETL speed over database-enforced integrity.

---

## 6. Expected next ETL flow: staging to DW

The next procedures we will write should probably be:

### Dimension load procedures

```text
stg_finance_ops.donors              -> dw.dim_donor
stg_finance_ops.campaigns           -> dw.dim_campaign
stg_finance_ops.expense_categories  -> dw.dim_category
static / distinct donation_type     -> dw.dim_donation_type
distinct statuses                   -> dw.dim_status
distinct currencies                 -> dw.dim_currency
distinct allocation source_type     -> dw.dim_allocation_type
```

Recommended dimension load order:

```text
dim_donor
dim_campaignV
dim_category
dim_donation_type
dim_status
dim_currency
dim_allocation_type
```

Then fact load order:

```text
fact_donation_transaction
fact_budget_allocation_event
fact_monthly_financial_snapshot
fact_donation_lifecycle
```

---

## 7. Important architecture points before writing procedures

There are a few business-rule decisions we need to be careful about later:

### 1. `fact_donation_transaction.center_key`

`donations` does not have `center_id`.

But the fact table has:

```text
center_key
```

So for each donation transaction, we need to decide:

```text
Should center_key be -1?
Should it be derived from budget_allocations?
Should one donation be duplicated across multiple centers if allocated to multiple centers?
```

This is a key design decision.

---

### 2. Monthly financial snapshot

`expenses` and `payments` have `center_id`.

But `donations` do not.

So for:

```text
fact_monthly_financial_snapshot.total_donation_amount
```

we need to decide whether donation amount by center should come from:

```text
donations directly
```

or from:

```text
budget_allocations where source_type = 'donation'
```

For center-level finance analytics, using allocation data may be more logical.

---

### 3. Donation lifecycle

The source has:

```text
donation_date
created_at
updated_at
status
```

But it does not have a clear:

```text
confirmed_at
allocated_at
```

So for:

```text
days_to_confirm
days_to_allocate
confirmed_date_key
allocated_date_key
```

we need rules.

Possible inference:

```text
created_date_key = donations.created_at
confirmed_date_key = donation_date if status = confirmed
allocated_date_key = first budget_allocations.allocation_date for that donation
```

But this is an inferred rule, not explicitly present in source.

---

### 4. Rejected staging rows

The staging schema supports invalid rows, but the current ETL only stores valid rows. If the goal is auditability, later we may want to either:

```text
insert invalid rows into staging with is_valid = 0
```

or keep the current behavior and only store counts in `etl_load_log`.

---

### 5. Currency conversion

You have `currency_rates`, but the MART 2 facts only store:

```text
amount
allocated_amount
```

There is no converted amount column such as:

```text
amount_irr
amount_usd
exchange_rate
```

So if multi-currency reporting is required, the DW fact structure may need extra fields.

---

## 8. My current understanding in one sentence

You already have a Finance Operations source database, a staging mirror with ETL logging, and a MART 2 warehouse model for charity financial analytics; the next real task is to write **staging-to-DW procedures** that load finance dimensions first, then populate donation, allocation, monthly snapshot, and lifecycle facts using conformed dimensions from MART 1 where needed.
