/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 2 - Staging ETL Work Tables
 File         : 09_create_stg_finance_ops_etl_work_tables.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create normal staging work tables used by the FinanceOps source-to-staging ETL.
   These tables replace procedure-local #temp tables.

 Usage:
   1. Run this file after the main staging table creation script.
   2. Each ETL procedure TRUNCATEs its own work tables at the start of the load.
   3. These tables are ETL scratch/work tables, not analytical staging outputs.

 Note:
   Because these are shared physical work tables, do not run two FinanceOps staging
   loads in parallel unless you add batch/session isolation columns.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Stg_FinanceOps_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stg_finance_ops')
BEGIN
    EXEC(N'CREATE SCHEMA stg_finance_ops');
END
GO

/* Work table for donors: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_donors_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_donors_src (
    [id] INT NULL,
    [full_name] NVARCHAR(200) NULL,
    [national_id] NVARCHAR(50) NULL,
    [phone] NVARCHAR(30) NULL,
    [email] NVARCHAR(255) NULL,
    [donor_type] NVARCHAR(50) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for donors: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_donors_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_donors_validated (
    [id] INT NULL,
    [full_name] NVARCHAR(200) NULL,
    [national_id] NVARCHAR(50) NULL,
    [phone] NVARCHAR(30) NULL,
    [email] NVARCHAR(255) NULL,
    [donor_type] NVARCHAR(50) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for donors: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_donors_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_donors_valid (
    [id] INT NULL,
    [full_name] NVARCHAR(200) NULL,
    [national_id] NVARCHAR(50) NULL,
    [phone] NVARCHAR(30) NULL,
    [email] NVARCHAR(255) NULL,
    [donor_type] NVARCHAR(50) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_donors_valid_id
ON stg_finance_ops.etl_tmp_donors_valid ([id]);
GO

/* Work table for campaigns: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_campaigns_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_campaigns_src (
    [id] INT NULL,
    [title] NVARCHAR(300) NULL,
    [description] NVARCHAR(2000) NULL,
    [target_amount] DECIMAL(18,2) NULL,
    [start_date] DATE NULL,
    [end_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for campaigns: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_campaigns_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_campaigns_validated (
    [id] INT NULL,
    [title] NVARCHAR(300) NULL,
    [description] NVARCHAR(2000) NULL,
    [target_amount] DECIMAL(18,2) NULL,
    [start_date] DATE NULL,
    [end_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for campaigns: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_campaigns_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_campaigns_valid (
    [id] INT NULL,
    [title] NVARCHAR(300) NULL,
    [description] NVARCHAR(2000) NULL,
    [target_amount] DECIMAL(18,2) NULL,
    [start_date] DATE NULL,
    [end_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_campaigns_valid_id
ON stg_finance_ops.etl_tmp_campaigns_valid ([id]);
GO

/* Work table for expense_categories: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_expense_categories_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_expense_categories_src (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for expense_categories: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_expense_categories_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_expense_categories_validated (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for expense_categories: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_expense_categories_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_expense_categories_valid (
    [id] INT NULL,
    [name] NVARCHAR(200) NULL,
    [is_active] BIT NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_expense_categories_valid_id
ON stg_finance_ops.etl_tmp_expense_categories_valid ([id]);
GO

/* Work table for donations: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_donations_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_donations_src (
    [id] INT NULL,
    [donor_id] INT NULL,
    [campaign_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [donation_type] NVARCHAR(50) NULL,
    [donation_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [reference_code] NVARCHAR(100) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for donations: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_donations_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_donations_validated (
    [id] INT NULL,
    [donor_id] INT NULL,
    [campaign_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [donation_type] NVARCHAR(50) NULL,
    [donation_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [reference_code] NVARCHAR(100) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for donations: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_donations_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_donations_valid (
    [id] INT NULL,
    [donor_id] INT NULL,
    [campaign_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [donation_type] NVARCHAR(50) NULL,
    [donation_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [reference_code] NVARCHAR(100) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_donations_valid_id
ON stg_finance_ops.etl_tmp_donations_valid ([id]);
GO

/* Work table for expenses: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_expenses_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_expenses_src (
    [id] INT NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [category_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [expense_date] DATE NULL,
    [description] NVARCHAR(2000) NULL,
    [approved_by_user_id] INT NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for expenses: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_expenses_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_expenses_validated (
    [id] INT NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [category_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [expense_date] DATE NULL,
    [description] NVARCHAR(2000) NULL,
    [approved_by_user_id] INT NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for expenses: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_expenses_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_expenses_valid (
    [id] INT NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [category_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [expense_date] DATE NULL,
    [description] NVARCHAR(2000) NULL,
    [approved_by_user_id] INT NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_expenses_valid_id
ON stg_finance_ops.etl_tmp_expenses_valid ([id]);
GO

/* Work table for payments: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_payments_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_payments_src (
    [id] INT NULL,
    [payment_type] NVARCHAR(50) NULL,
    [teacher_id] INT NULL,
    [center_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [payment_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for payments: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_payments_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_payments_validated (
    [id] INT NULL,
    [payment_type] NVARCHAR(50) NULL,
    [teacher_id] INT NULL,
    [center_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [payment_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for payments: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_payments_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_payments_valid (
    [id] INT NULL,
    [payment_type] NVARCHAR(50) NULL,
    [teacher_id] INT NULL,
    [center_id] INT NULL,
    [amount] DECIMAL(18,2) NULL,
    [currency] CHAR(3) NULL,
    [payment_date] DATE NULL,
    [status] NVARCHAR(50) NULL,
    [created_at] DATETIME2(0) NULL,
    [updated_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_payments_valid_id
ON stg_finance_ops.etl_tmp_payments_valid ([id]);
GO

/* Work table for budget_allocations: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_budget_allocations_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_budget_allocations_src (
    [id] INT NULL,
    [source_type] NVARCHAR(50) NULL,
    [source_id] INT NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [category_id] INT NULL,
    [allocated_amount] DECIMAL(18,2) NULL,
    [allocation_date] DATE NULL,
    [reason] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for budget_allocations: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_budget_allocations_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_budget_allocations_validated (
    [id] INT NULL,
    [source_type] NVARCHAR(50) NULL,
    [source_id] INT NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [category_id] INT NULL,
    [allocated_amount] DECIMAL(18,2) NULL,
    [allocation_date] DATE NULL,
    [reason] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for budget_allocations: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_budget_allocations_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_budget_allocations_valid (
    [id] INT NULL,
    [source_type] NVARCHAR(50) NULL,
    [source_id] INT NULL,
    [center_id] INT NULL,
    [child_id] INT NULL,
    [category_id] INT NULL,
    [allocated_amount] DECIMAL(18,2) NULL,
    [allocation_date] DATE NULL,
    [reason] NVARCHAR(2000) NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_budget_allocations_valid_id
ON stg_finance_ops.etl_tmp_budget_allocations_valid ([id]);
GO

/* Work table for financial_transactions: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_financial_transactions_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_financial_transactions_src (
    [id] BIGINT NULL,
    [entity_type] NVARCHAR(50) NULL,
    [entity_id] INT NULL,
    [transaction_type] NVARCHAR(50) NULL,
    [amount] DECIMAL(18,2) NULL,
    [transaction_date] DATE NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for financial_transactions: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_financial_transactions_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_financial_transactions_validated (
    [id] BIGINT NULL,
    [entity_type] NVARCHAR(50) NULL,
    [entity_id] INT NULL,
    [transaction_type] NVARCHAR(50) NULL,
    [amount] DECIMAL(18,2) NULL,
    [transaction_date] DATE NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for financial_transactions: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_financial_transactions_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_financial_transactions_valid (
    [id] BIGINT NULL,
    [entity_type] NVARCHAR(50) NULL,
    [entity_id] INT NULL,
    [transaction_type] NVARCHAR(50) NULL,
    [amount] DECIMAL(18,2) NULL,
    [transaction_date] DATE NULL,
    [created_at] DATETIME2(0) NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_financial_transactions_valid_id
ON stg_finance_ops.etl_tmp_financial_transactions_valid ([id]);
GO

/* Work table for currency_rates: src */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_currency_rates_src;
GO

CREATE TABLE stg_finance_ops.etl_tmp_currency_rates_src (
    [id] INT NULL,
    [from_currency] CHAR(3) NULL,
    [to_currency] CHAR(3) NULL,
    [rate] DECIMAL(18,8) NULL,
    [rate_date] DATE NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL
);
GO

/* Work table for currency_rates: validated */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_currency_rates_validated;
GO

CREATE TABLE stg_finance_ops.etl_tmp_currency_rates_validated (
    [id] INT NULL,
    [from_currency] CHAR(3) NULL,
    [to_currency] CHAR(3) NULL,
    [rate] DECIMAL(18,8) NULL,
    [rate_date] DATE NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

/* Work table for currency_rates: valid */
DROP TABLE IF EXISTS stg_finance_ops.etl_tmp_currency_rates_valid;
GO

CREATE TABLE stg_finance_ops.etl_tmp_currency_rates_valid (
    [id] INT NULL,
    [from_currency] CHAR(3) NULL,
    [to_currency] CHAR(3) NULL,
    [rate] DECIMAL(18,8) NULL,
    [rate_date] DATE NULL,
    [source_updated_at] DATETIME2(0) NULL,
    [row_hash] VARBINARY(32) NULL,
    [validation_message] NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_etl_tmp_currency_rates_valid_id
ON stg_finance_ops.etl_tmp_currency_rates_valid ([id]);
GO
