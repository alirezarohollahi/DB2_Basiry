/*
===============================================================================
 Finance MART 2 ETL Work Tables - Synced with newest DW architecture

 Purpose:
   Persistent ETL work tables used instead of SQL Server session temp tables.
   Procedures TRUNCATE and reuse these tables.

 Notes:
   - DW facts/dimensions do NOT keep source_* columns or source_system.
   - source IDs are kept only in etl_work.fact_source_load_map and work tables
     so append-only incremental ETL can stay idempotent after removing source IDs
     from the DW fact tables.
   - Allocation-type dimension/key logic was removed.
===============================================================================
*/
USE Charity_DW_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_work')
BEGIN
    EXEC(N'CREATE SCHEMA etl_work');
END
GO

/* Recreate only reusable work tables so old columns from earlier versions cannot break the fixed ETL.
   The persistent source map is not dropped here. First-load procedures delete their own map rows. */
DROP TABLE IF EXISTS etl_work.tmp_fact_donation_lifecycle_final;
DROP TABLE IF EXISTS etl_work.tmp_fact_donation_lifecycle_current;
DROP TABLE IF EXISTS etl_work.tmp_fact_monthly_snapshot_load;
DROP TABLE IF EXISTS etl_work.tmp_fact_budget_allocation_event_load;
DROP TABLE IF EXISTS etl_work.tmp_fact_payment_transaction_load;
DROP TABLE IF EXISTS etl_work.tmp_fact_expense_transaction_load;
DROP TABLE IF EXISTS etl_work.tmp_fact_donation_transaction_load;
DROP TABLE IF EXISTS etl_work.tmp_dim_currency_load;
DROP TABLE IF EXISTS etl_work.tmp_dim_status_load;
DROP TABLE IF EXISTS etl_work.tmp_dim_donation_type_load;
DROP TABLE IF EXISTS etl_work.tmp_dim_category_load;
DROP TABLE IF EXISTS etl_work.tmp_dim_campaign_load;
DROP TABLE IF EXISTS etl_work.tmp_dim_donor_load;
GO

IF OBJECT_ID(N'etl_work.tmp_dim_donor_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_dim_donor_load (
    existing_key INT NULL,
    donor_id INT NULL,
    full_name NVARCHAR(250) NULL,
    donor_type NVARCHAR(50) NULL,
    is_active BIT NULL,
    row_hash VARBINARY(32) NULL,
    created_at DATETIME2(0) NULL,
    updated_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_dim_campaign_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_dim_campaign_load (
    existing_key INT NULL,
    campaign_id INT NULL,
    title NVARCHAR(250) NULL,
    campaign_status NVARCHAR(50) NULL,
    target_amount DECIMAL(18,2) NULL,
    start_date DATE NULL,
    end_date DATE NULL,
    row_hash VARBINARY(32) NULL,
    created_at DATETIME2(0) NULL,
    updated_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_dim_category_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_dim_category_load (
    existing_key INT NULL,
    category_id INT NULL,
    category_name NVARCHAR(200) NULL,
    parent_category_id INT NULL,
    parent_category_name NVARCHAR(200) NULL,
    category_status NVARCHAR(30) NULL,
    row_hash VARBINARY(32) NULL,
    created_at DATETIME2(0) NULL,
    updated_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_dim_donation_type_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_dim_donation_type_load (
    existing_key INT NULL,
    code NVARCHAR(50) NULL,
    title NVARCHAR(100) NULL,
    created_at DATETIME2(0) NULL,
    updated_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_dim_status_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_dim_status_load (
    existing_key INT NULL,
    status_type NVARCHAR(50) NULL,
    code NVARCHAR(50) NULL,
    title NVARCHAR(100) NULL,
    category NVARCHAR(50) NULL,
    created_at DATETIME2(0) NULL,
    updated_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_dim_currency_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_dim_currency_load (
    existing_key INT NULL,
    code NVARCHAR(10) NULL,
    name NVARCHAR(100) NULL,
    created_at DATETIME2(0) NULL,
    updated_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_donation_transaction_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_donation_transaction_load (
    date_key INT NULL,
    donor_key INT NULL,
    campaign_key INT NULL,
    center_key INT NULL,
    donation_type_key INT NULL,
    currency_key INT NULL,
    status_key INT NULL,
    amount DECIMAL(18,2) NULL,
    is_confirmed BIT NULL,
    is_refunded BIT NULL,
    source_donation_id BIGINT NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_expense_transaction_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_expense_transaction_load (
    date_key INT NULL,
    center_key INT NULL,
    child_key INT NULL,
    category_key INT NULL,
    currency_key INT NULL,
    status_key INT NULL,
    amount DECIMAL(18,2) NULL,
    is_approved BIT NULL,
    is_rejected BIT NULL,
    source_expense_id BIGINT NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_payment_transaction_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_payment_transaction_load (
    date_key INT NULL,
    center_key INT NULL,
    currency_key INT NULL,
    status_key INT NULL,
    payment_type NVARCHAR(50) NULL,
    amount DECIMAL(18,2) NULL,
    is_paid BIT NULL,
    is_cancelled BIT NULL,
    source_payment_id BIGINT NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_budget_allocation_event_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_budget_allocation_event_load (
    date_key INT NULL,
    donor_key INT NULL,
    center_key INT NULL,
    child_key INT NULL,
    category_key INT NULL,
    campaign_key INT NULL,
    source_allocation_id BIGINT NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_monthly_snapshot_load', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_monthly_snapshot_load (
    month_key INT NULL,
    center_key INT NULL,
    total_donation_amount DECIMAL(18,2) NULL,
    total_expense_amount DECIMAL(18,2) NULL,
    total_payment_amount DECIMAL(18,2) NULL,
    net_balance DECIMAL(18,2) NULL,
    donation_count INT NULL,
    expense_count INT NULL,
    payment_count INT NULL,
    allocation_count INT NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_donation_lifecycle_current', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_donation_lifecycle_current (
    source_donation_id BIGINT NULL,
    donor_key INT NULL,
    campaign_key INT NULL,
    created_date_key INT NULL,
    confirmed_date_key INT NULL,
    allocated_date_key INT NULL,
    lifecycle_status_key INT NULL,
    current_stage NVARCHAR(50) NULL,
    donation_amount DECIMAL(18,2) NULL,
    min_donation DECIMAL(18,2) NULL,
    max_donation DECIMAL(18,2) NULL,
    avg_donation DECIMAL(18,2) NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.tmp_fact_donation_lifecycle_final', N'U') IS NULL
CREATE TABLE etl_work.tmp_fact_donation_lifecycle_final (
    source_donation_id BIGINT NULL,
    donor_key INT NULL,
    campaign_key INT NULL,
    created_date_key INT NULL,
    confirmed_date_key INT NULL,
    allocated_date_key INT NULL,
    lifecycle_status_key INT NULL,
    current_stage NVARCHAR(50) NULL,
    donation_amount DECIMAL(18,2) NULL,
    min_donation DECIMAL(18,2) NULL,
    max_donation DECIMAL(18,2) NULL,
    avg_donation DECIMAL(18,2) NULL,
    etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NULL
);
GO

IF OBJECT_ID(N'etl_work.fact_source_load_map', N'U') IS NULL
CREATE TABLE etl_work.fact_source_load_map (
    fact_name NVARCHAR(128) NOT NULL,
    source_table NVARCHAR(128) NOT NULL,
    source_id BIGINT NOT NULL,
    loaded_etl_batch_id INT NULL,
    loaded_at DATETIME2(0) NOT NULL CONSTRAINT DF_fact_source_load_map_loaded_at DEFAULT (SYSDATETIME())
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'UX_fact_source_load_map' AND object_id=OBJECT_ID(N'etl_work.fact_source_load_map'))
CREATE UNIQUE INDEX UX_fact_source_load_map
    ON etl_work.fact_source_load_map(fact_name, source_table, source_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_dim_donor_load_existing_key' AND object_id=OBJECT_ID(N'etl_work.tmp_dim_donor_load'))
CREATE INDEX IX_tmp_dim_donor_load_existing_key ON etl_work.tmp_dim_donor_load(existing_key);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_dim_campaign_load_existing_key' AND object_id=OBJECT_ID(N'etl_work.tmp_dim_campaign_load'))
CREATE INDEX IX_tmp_dim_campaign_load_existing_key ON etl_work.tmp_dim_campaign_load(existing_key);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_dim_category_load_existing_key' AND object_id=OBJECT_ID(N'etl_work.tmp_dim_category_load'))
CREATE INDEX IX_tmp_dim_category_load_existing_key ON etl_work.tmp_dim_category_load(existing_key);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_fact_donation_transaction_load_source' AND object_id=OBJECT_ID(N'etl_work.tmp_fact_donation_transaction_load'))
CREATE INDEX IX_tmp_fact_donation_transaction_load_source ON etl_work.tmp_fact_donation_transaction_load(source_donation_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_fact_expense_transaction_load_source' AND object_id=OBJECT_ID(N'etl_work.tmp_fact_expense_transaction_load'))
CREATE INDEX IX_tmp_fact_expense_transaction_load_source ON etl_work.tmp_fact_expense_transaction_load(source_expense_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_fact_payment_transaction_load_source' AND object_id=OBJECT_ID(N'etl_work.tmp_fact_payment_transaction_load'))
CREATE INDEX IX_tmp_fact_payment_transaction_load_source ON etl_work.tmp_fact_payment_transaction_load(source_payment_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_fact_budget_allocation_event_load_source' AND object_id=OBJECT_ID(N'etl_work.tmp_fact_budget_allocation_event_load'))
CREATE INDEX IX_tmp_fact_budget_allocation_event_load_source ON etl_work.tmp_fact_budget_allocation_event_load(source_allocation_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name=N'IX_tmp_fact_donation_lifecycle_current_source' AND object_id=OBJECT_ID(N'etl_work.tmp_fact_donation_lifecycle_current'))
CREATE INDEX IX_tmp_fact_donation_lifecycle_current_source ON etl_work.tmp_fact_donation_lifecycle_current(source_donation_id);
GO
