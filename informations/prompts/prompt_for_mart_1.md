We need to write SQL Server ETL stored procedures fact_child_task_event for above data warehouse.
at the end give me two procedure in sql file one for first load and one for increamental 
dim_date already loaded and stablished 
care about notes:
Please follow these rules strictly and pay close attention to the type of each dimension and fact table.
General ETL rules:
* Every procedure must receive `@start_time` and `@end_time` as input parameters.
* Use a half-open time range:
  `@start_time <= source_time < @end_time`
* This prevents overlap between daily jobs and avoids duplicate loads.
* Every dimension table must have an unknown row with key `-1`.
* Fact loads must not fail when a dimension lookup is missing; instead, use the related unknown key `-1`.
* ETL logs must be written in the DW database, not in staging.
* Logging must be step-level, not row-level.
* Do not insert one log row per data row.
* `etl_batch` must only store the final summary of the business operation.
* `etl_load_log` can store internal ETL steps.
* `rows_read`, `rows_inserted`, `rows_updated`, and `rows_rejected` must have accurate business meaning.
* Do not use `MERGE`, because it can be risky and hard to debug in SQL Server production ETL.
* Do not use window functions unless absolutely necessary.
* Avoid heavy and dangerous delete/reload patterns.
* The normal incremental load pattern should be:

  1. Detect affected records.
  2. Update changed records.
  3. Insert new records.
* For first load, the process may be more aggressive, but the unknown row must always be preserved.
* For each dimension or fact procedure, provide two versions:

  1. First-load procedure.
  2. Normal/incremental-load procedure.
* At the beginning of first load, reset identity/sequence to the initial value when appropriate.
* At the beginning of incremental load, align identity/sequence with `MAX(key) + interval` when needed.

Execution order:

1. Load dimensions first.
2. Load transaction facts.
3. Load snapshot facts.
4. Load lifecycle / accumulating facts.
5. Run orchestration procedures.

Dimension rules:

* Dimensions usually do not need a daily loop.
* Do not use `WHILE` loops for dimensions.
* Dimensions must be loaded set-based.
* Use `LEFT JOIN` or `FULL JOIN` between staging/source and dimension to detect inserts and updates.
* Use `row_hash` to detect changes whenever possible.
* If the source record is new, insert it.
* If the source record exists and `row_hash` changed, update it.
* If nothing changed, do nothing.
* Never update or delete the unknown row with key `-1`.
* Dimensions must not be loaded with a date range so narrow that related facts later resolve to `-1`.
* Before loading facts, make sure required dimensions for the same period have already been loaded.

SCD Type 1 dimensions:

* Examples: `dim_donor`, `dim_campaign`, `dim_category`, `dim_donation_type`, `dim_status`, `dim_currency`, `dim_allocation_type`.
* Attribute changes overwrite the previous value.
* No history is stored.
* Do not use `effective_from`, `effective_to`, or `is_current`.
* Use `row_hash` for change detection.
* Pattern:

  * Source exists and dimension does not exist → insert.
  * Source exists and dimension exists but hash changed → update.
  * Source exists and hash is the same → no action.
* Do not truncate the whole dimension in incremental loads.
* If truncating during first load, reinsert the unknown row.
* For small reference dimensions like status, currency, and type, distinct values from staging are enough.
* For status dimensions, the natural key should not be only `code`; use something like `status_type + code`, because the same status code can mean different things in different business contexts.
SCD Type 2 dimensions:
* If a dimension has columns like `effective_from`, `effective_to`, `is_current`, or `version_number`, it is probably Type 2.
* Do not overwrite the previous row.
* When a historical attribute changes:

  * Set the old current row to `is_current = 0`.
  * Set `effective_to = change_date`.
  * Insert a new current row with `is_current = 1`.
* Facts must join to the correct dimension version based on the transaction date.
* Fact-to-dimension join must use both business key and date range.
* `row_hash` should cover only historical attributes.
* Non-historical attributes can be handled as Type 1 inside Type 2.
* Type 2 is heavier and should not be used unless needed.
SCD Type 3 dimensions:
* Handle them similarly to Type 2 in terms of tracking previous values.
* But do not insert a new row.
* Update the existing row and maintain previous-value columns.
Static / reference dimensions:
* Usually have a small number of rows.
* Do not need time loops.
* Build them from distinct values in staging.
* Normalize values:

  * Trim strings.
  * Lowercase codes.
  * Remove empty values.
* Always keep the unknown row.
* Insert new source values when they appear.
* Update title/description if they change.
* Prefer controlled codes to avoid messy dimensions.
Hierarchical dimensions:

* Resolve parent-child relationships.
* If the parent is not loaded yet, temporarily use unknown/null parent information.
* In later incremental loads, try to resolve the parent again.
* Watch for missing parents and self-references.
* Avoid heavy recursive queries in ETL unless truly required.
* For simple category levels, direct joins are enough.
* If hierarchy becomes deep, consider a bridge table or flattened hierarchy columns.

Conformed dimensions:

* These are shared across marts.
* If `dim_center` or `dim_child` is incomplete, facts may receive `center_key = -1` or `child_key = -1`.
* `dim_date` must be populated in advance for the whole ETL period.
* If a date is missing in `dim_date`, the fact should use `date_key = -1`.
* Create a separate idempotent procedure for `dim_date`.
* The `dim_date` procedure should generate one row per day from start date to end date.
* If the date already exists, update it; otherwise insert it.

Transaction facts:

* Grain must be clear:
  `one row per source transaction`
* These facts are not accumulating facts.
* Each row represents one independent event or transaction.
* Do not reload the whole fact in incremental loads.
* Do not use broad delete/reload logic.
* Use this pattern:

  * Source transaction exists and fact does not exist → insert.
  * Source transaction exists and fact exists but changed → update.
  * Source transaction unchanged → no action.
* Resolve `date_key` by direct join to `dim_date`.
* Resolve `center_key` by direct join to `dim_center`.
* Do not create unnecessary temp lookup tables for date/center.
* If a dimension lookup is missing, use `-1`.
* Transaction facts must be partition-friendly by date.
* Snapshot facts should read from transaction/event facts, not directly from source/staging.

Event facts / factless facts:

* Usually represent a business relationship or event between entities.
* Similar to transaction facts.
* Not accumulating.
* Do not recalculate from beginning to end.
* In incremental loads, only insert/update affected events or allocations.
* Must be partition-friendly using `date_key`.

Periodic snapshot facts:

* Represent the state of a business process during a period.
* Grain must be explicit, for example:
  `one row per month per center`
* If the grain is daily, create one row per day.
* This fact is allowed to use a loop.
* `WHILE` is acceptable only for snapshot facts and should be used for period-by-period snapshot creation.
* Snapshot facts must not read directly from source/staging.
* Snapshot facts should read from transaction/event facts.
* In incremental loads, if an old transaction is updated today, rebuild the period related to the transaction date, not only today’s period.
* For monthly snapshots, use `month_key` as the partition-friendly key.
* The snapshot must be able to create months that have expense/payment but no donation.
* It must also handle centers that only have allocation.
* If `dim_center` is incomplete, values may be grouped under `center_key = -1`.

Accumulating snapshot facts:

* Represent the lifecycle of a business process.
* Grain:
  `one row per lifecycle`
* Usually does not have a normal transaction-style date key.
* Unlike transaction facts, this fact is expected to be updated.
* Updating it is not risky because its design is to store the current lifecycle state.
* Incremental window should only detect affected business processes, such as affected donations.
* Lifecycle state must be recalculated from the full available history of that business process, not only from the incremental time window.
* Pattern:

  * Time window → detect affected donations/processes.
  * Full available history → recalculate lifecycle state.
* Existing rows from the same fact may also be used during recalculation.
* The fact must be idempotent.
* Rerunning the ETL must not create duplicate lifecycle rows.

Fact table partitioning:

* Physical partitioning in SQL Server must be defined in table/index/partition scheme design, not only in ETL.
* However, ETL must be written in a partition-friendly way.
* For transaction/event facts, the best partition key is usually `date_key`.
* For monthly snapshots, use `month_key`.
* ETL filters must use these keys or equivalent date ranges.
* Avoid function-wrapped date predicates because they may prevent partition elimination.
* Use range-based predicates.
* For large loads, process affected periods separately.
* If real partitioning is added later, partition switching or partition truncation can be considered.
* For now, do not use broad deletes on the whole fact table.

Logging rules:

* Logs must be stored in:
  `Charity_DW_DB.etl_admin.etl_batch`
  `Charity_DW_DB.etl_admin.etl_load_log`
* `etl_batch` must store procedure-level summary.
* `etl_load_log` must store step-level details.
* Logging must not be row-level.
* Temp steps may be logged, but they must not inflate the final batch summary.
* For dimensions:

  * `rows_read` = source candidates.
  * `rows_inserted` = inserted dimension rows.
  * `rows_updated` = updated dimension rows.
  * `rows_rejected` = invalid/rejected rows.
* For facts:

  * `rows_read` = source/fact candidate rows.
  * `rows_inserted` = inserted fact rows.
  * `rows_updated` = updated fact rows.
  * `rows_rejected` = unresolved or critically invalid rows.
* If a dimension lookup is missing but the fact is still loaded with `-1`, it is usually not rejected.
* A rejected row means the row cannot be loaded at all.
* `created_by` in `etl_batch` must have a value.
* At the end of each procedure, update `ended_at` and `batch_status`.
* On failure, store the error message.

Very important final principles:

* Use the minimum possible number of loops.
* Avoid unnecessary temp tables.
* Join directly to dimensions when possible.
* Do not use `MERGE`.
* Do not use window functions unless truly necessary.
* Do not use broad deletes.
* Load only affected data.
* Use partition-friendly predicates.
* Always design the ETL based on the exact type of dimension or fact.
* For every requested table, explain which type it is and why, then write both first-load and incremental-load procedures accordingly.
