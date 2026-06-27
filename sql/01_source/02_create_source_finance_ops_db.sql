/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 1 - Operational Source Databases
 File         : 02_create_source_finance_ops_db.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create the operational source database for Finance Operations:
   donors, campaigns, donations, expense categories, expenses, payments,
   budget allocations, financial transactions, and currency rates.

 Naming decision:
   Database : Source_FinanceOps_DB
   Schema   : finance_ops

 Important shared-entity decision:
   This finance source uses reference IDs for shared business entities:
   - center_id
   - child_id
   - teacher_id

   These IDs refer conceptually to entities managed in Source_ProgramOps_DB,
   but cross-database foreign keys are intentionally avoided in the source layer.
   In the warehouse layer, these will be integrated through conformed dimensions:
   - dw.dim_center
   - dw.dim_child
   - dw.dim_teacher
   - dw.dim_date

 Notes:
   - This script is development-friendly and re-runnable.
   - All operational source finance tables are created under schema finance_ops.
   - This is a normalized OLTP-style source database.
===============================================================================
*/

SET NOCOUNT ON;
GO

/*=============================================================================
  1. Create Database
=============================================================================*/

IF DB_ID(N'Source_FinanceOps_DB') IS NULL
BEGIN
    CREATE DATABASE Source_FinanceOps_DB;
END
GO

USE Source_FinanceOps_DB;
GO

/*=============================================================================
  2. Create Schema
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'finance_ops'
)
BEGIN
    EXEC(N'CREATE SCHEMA finance_ops');
END
GO

/*=============================================================================
  3. Drop Existing Tables

     This makes the script re-runnable during development.
=============================================================================*/

DROP TABLE IF EXISTS finance_ops.currency_rates;
DROP TABLE IF EXISTS finance_ops.financial_transactions;
DROP TABLE IF EXISTS finance_ops.budget_allocations;
DROP TABLE IF EXISTS finance_ops.payments;
DROP TABLE IF EXISTS finance_ops.expenses;
DROP TABLE IF EXISTS finance_ops.expense_categories;
DROP TABLE IF EXISTS finance_ops.donations;
DROP TABLE IF EXISTS finance_ops.campaigns;
DROP TABLE IF EXISTS finance_ops.donors;
GO

/*=============================================================================
  4. Donor and Campaign Master Tables
=============================================================================*/

CREATE TABLE finance_ops.donors (
    id              INT IDENTITY(1,1) NOT NULL,
    full_name       NVARCHAR(200) NOT NULL,
    national_id     NVARCHAR(50) NULL,
    phone           NVARCHAR(30) NULL,
    email           NVARCHAR(255) NULL,
    donor_type      NVARCHAR(50) NOT NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_finance_donors_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_finance_donors_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_finance_donors PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_finance_donors_national_id UNIQUE (national_id),
    CONSTRAINT CK_finance_donors_donor_type
        CHECK (donor_type IN (N'individual', N'organization'))
);
GO

CREATE TABLE finance_ops.campaigns (
    id              INT IDENTITY(1,1) NOT NULL,
    title           NVARCHAR(300) NOT NULL,
    description     NVARCHAR(2000) NULL,
    target_amount   DECIMAL(18,2) NULL,
    start_date      DATE NULL,
    end_date        DATE NULL,
    status          NVARCHAR(50) NOT NULL CONSTRAINT DF_finance_campaigns_status DEFAULT (N'planned'),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_finance_campaigns_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_finance_campaigns PRIMARY KEY CLUSTERED (id),
    CONSTRAINT CK_finance_campaigns_target_amount CHECK (target_amount IS NULL OR target_amount >= 0),
    CONSTRAINT CK_finance_campaigns_date_range CHECK (end_date IS NULL OR start_date IS NULL OR start_date <= end_date)
);
GO

/*=============================================================================
  5. Donation Transactions
=============================================================================*/

CREATE TABLE finance_ops.donations (
    id              INT IDENTITY(1,1) NOT NULL,
    donor_id        INT NOT NULL,
    campaign_id     INT NULL,
    amount          DECIMAL(18,2) NOT NULL,
    currency        CHAR(3) NOT NULL CONSTRAINT DF_finance_donations_currency DEFAULT ('IRR'),
    donation_type   NVARCHAR(50) NOT NULL,
    donation_date   DATE NOT NULL,
    status          NVARCHAR(50) NOT NULL CONSTRAINT DF_finance_donations_status DEFAULT (N'pending'),
    reference_code  NVARCHAR(100) NULL,
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_finance_donations_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_finance_donations PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_finance_donations_donors
        FOREIGN KEY (donor_id) REFERENCES finance_ops.donors(id),
    CONSTRAINT FK_finance_donations_campaigns
        FOREIGN KEY (campaign_id) REFERENCES finance_ops.campaigns(id),
    CONSTRAINT CK_finance_donations_amount CHECK (amount > 0),
    CONSTRAINT CK_finance_donations_type
        CHECK (donation_type IN (N'cash', N'bank_transfer', N'online', N'in_kind')),
    CONSTRAINT CK_finance_donations_status
        CHECK (status IN (N'pending', N'confirmed', N'rejected', N'refunded')),
    CONSTRAINT UQ_finance_donations_reference_code UNIQUE (reference_code)
);
GO

/*=============================================================================
  6. Expense Categories and Expenses
=============================================================================*/

CREATE TABLE finance_ops.expense_categories (
    id              INT IDENTITY(1,1) NOT NULL,
    name            NVARCHAR(200) NOT NULL,
    parent_id       INT NULL,
    is_active       BIT NOT NULL CONSTRAINT DF_finance_expense_categories_is_active DEFAULT (1),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_finance_expense_categories_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_finance_expense_categories PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_finance_expense_categories_parent
        FOREIGN KEY (parent_id) REFERENCES finance_ops.expense_categories(id),
    CONSTRAINT UQ_finance_expense_categories_name UNIQUE (name)
);
GO

CREATE TABLE finance_ops.expenses (
    id                      INT IDENTITY(1,1) NOT NULL,
    center_id               INT NOT NULL,
    child_id                INT NULL,
    category_id             INT NOT NULL,
    amount                  DECIMAL(18,2) NOT NULL,
    currency                CHAR(3) NOT NULL CONSTRAINT DF_finance_expenses_currency DEFAULT ('IRR'),
    expense_date            DATE NOT NULL,
    description             NVARCHAR(2000) NULL,
    approved_by_user_id     INT NULL,
    status                  NVARCHAR(50) NOT NULL CONSTRAINT DF_finance_expenses_status DEFAULT (N'pending'),
    created_at              DATETIME2(0) NOT NULL CONSTRAINT DF_finance_expenses_created_at DEFAULT (SYSDATETIME()),
    updated_at              DATETIME2(0) NULL,

    CONSTRAINT PK_finance_expenses PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_finance_expenses_categories
        FOREIGN KEY (category_id) REFERENCES finance_ops.expense_categories(id),
    CONSTRAINT CK_finance_expenses_amount CHECK (amount > 0),
    CONSTRAINT CK_finance_expenses_status
        CHECK (status IN (N'pending', N'approved', N'rejected'))
);
GO

/*=============================================================================
  7. Payments
=============================================================================*/

CREATE TABLE finance_ops.payments (
    id              INT IDENTITY(1,1) NOT NULL,
    payment_type    NVARCHAR(50) NOT NULL,
    teacher_id      INT NULL,
    center_id       INT NOT NULL,
    amount          DECIMAL(18,2) NOT NULL,
    currency        CHAR(3) NOT NULL CONSTRAINT DF_finance_payments_currency DEFAULT ('IRR'),
    payment_date    DATE NOT NULL,
    status          NVARCHAR(50) NOT NULL CONSTRAINT DF_finance_payments_status DEFAULT (N'pending'),
    created_at      DATETIME2(0) NOT NULL CONSTRAINT DF_finance_payments_created_at DEFAULT (SYSDATETIME()),
    updated_at      DATETIME2(0) NULL,

    CONSTRAINT PK_finance_payments PRIMARY KEY CLUSTERED (id),
    CONSTRAINT CK_finance_payments_amount CHECK (amount > 0),
    CONSTRAINT CK_finance_payments_type
        CHECK (payment_type IN (N'salary', N'bonus', N'vendor', N'refund')),
    CONSTRAINT CK_finance_payments_status
        CHECK (status IN (N'pending', N'approved', N'paid', N'cancelled', N'rejected'))
);
GO

/*=============================================================================
  8. Budget Allocation Events
=============================================================================*/

CREATE TABLE finance_ops.budget_allocations (
    id                  INT IDENTITY(1,1) NOT NULL,
    source_type         NVARCHAR(50) NOT NULL,
    source_id           INT NULL,
    center_id           INT NOT NULL,
    child_id            INT NULL,
    category_id         INT NULL,
    allocated_amount    DECIMAL(18,2) NOT NULL,
    allocation_date     DATE NOT NULL,
    reason              NVARCHAR(2000) NULL,
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_finance_budget_allocations_created_at DEFAULT (SYSDATETIME()),

    CONSTRAINT PK_finance_budget_allocations PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_finance_budget_allocations_categories
        FOREIGN KEY (category_id) REFERENCES finance_ops.expense_categories(id),
    CONSTRAINT CK_finance_budget_allocations_amount CHECK (allocated_amount > 0),
    CONSTRAINT CK_finance_budget_allocations_source_type
        CHECK (source_type IN (N'donation', N'internal_budget'))
);
GO

/*=============================================================================
  9. Optional Financial Audit / Transaction Layer
=============================================================================*/

CREATE TABLE finance_ops.financial_transactions (
    id                  BIGINT IDENTITY(1,1) NOT NULL,
    entity_type         NVARCHAR(50) NOT NULL,
    entity_id           INT NOT NULL,
    transaction_type    NVARCHAR(50) NOT NULL,
    amount              DECIMAL(18,2) NOT NULL,
    transaction_date    DATE NOT NULL,
    created_at          DATETIME2(0) NOT NULL CONSTRAINT DF_finance_financial_transactions_created_at DEFAULT (SYSDATETIME()),

    CONSTRAINT PK_finance_financial_transactions PRIMARY KEY CLUSTERED (id),
    CONSTRAINT CK_finance_financial_transactions_entity_type
        CHECK (entity_type IN (N'donation', N'expense', N'payment')),
    CONSTRAINT CK_finance_financial_transactions_transaction_type
        CHECK (transaction_type IN (N'credit', N'debit')),
    CONSTRAINT CK_finance_financial_transactions_amount CHECK (amount > 0)
);
GO

/*=============================================================================
  10. Optional Currency Rates
=============================================================================*/

CREATE TABLE finance_ops.currency_rates (
    id              INT IDENTITY(1,1) NOT NULL,
    from_currency   CHAR(3) NOT NULL,
    to_currency     CHAR(3) NOT NULL,
    rate            DECIMAL(18,8) NOT NULL,
    rate_date       DATE NOT NULL,

    CONSTRAINT PK_finance_currency_rates PRIMARY KEY CLUSTERED (id),
    CONSTRAINT CK_finance_currency_rates_rate CHECK (rate > 0),
    CONSTRAINT UQ_finance_currency_rates_pair_date
        UNIQUE (from_currency, to_currency, rate_date)
);
GO

/*=============================================================================
  11. Helpful Indexes for ETL Extraction
=============================================================================*/

CREATE INDEX IX_finance_donors_updated_at
    ON finance_ops.donors(updated_at);
GO

CREATE INDEX IX_finance_campaigns_updated_at
    ON finance_ops.campaigns(updated_at);
GO

CREATE INDEX IX_finance_donations_date
    ON finance_ops.donations(donation_date, donor_id, campaign_id);
GO

CREATE INDEX IX_finance_donations_status
    ON finance_ops.donations(status);
GO

CREATE INDEX IX_finance_donations_updated_at
    ON finance_ops.donations(updated_at);
GO

CREATE INDEX IX_finance_expenses_date
    ON finance_ops.expenses(expense_date, center_id, child_id, category_id);
GO

CREATE INDEX IX_finance_expenses_status
    ON finance_ops.expenses(status);
GO

CREATE INDEX IX_finance_expenses_updated_at
    ON finance_ops.expenses(updated_at);
GO

CREATE INDEX IX_finance_payments_date
    ON finance_ops.payments(payment_date, center_id, teacher_id);
GO

CREATE INDEX IX_finance_payments_status
    ON finance_ops.payments(status);
GO

CREATE INDEX IX_finance_payments_updated_at
    ON finance_ops.payments(updated_at);
GO

CREATE INDEX IX_finance_budget_allocations_date
    ON finance_ops.budget_allocations(allocation_date, center_id, child_id, category_id);
GO

CREATE INDEX IX_finance_financial_transactions_entity
    ON finance_ops.financial_transactions(entity_type, entity_id);
GO

CREATE INDEX IX_finance_financial_transactions_date
    ON finance_ops.financial_transactions(transaction_date);
GO

CREATE INDEX IX_finance_currency_rates_date
    ON finance_ops.currency_rates(rate_date, from_currency, to_currency);
GO

/*=============================================================================
  12. Completion Message
=============================================================================*/

PRINT 'Source_FinanceOps_DB created successfully.';
PRINT 'Schema created: finance_ops';
PRINT 'Phase 1 script completed: Finance Operations operational source database.';
GO
