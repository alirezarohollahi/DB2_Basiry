/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 3 - Data Warehouse Layer
 File         : 13_create_dw_mart2_tables.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create all Data Warehouse tables required for MART 2: Charity Financial
   Analytics.

 Fast DW loading style:
   - No primary keys.
   - No foreign keys.
   - No unique rules.
   - No check rules.
   - No default rules.
   - No active rowstore indexes.
   - Tables are created as heaps to keep ETL loading fast.
   - Optional columnstore indexes are provided at the end as commented code.

 Prerequisite:
   Run these first:
   - 11_create_dw_db.sql
   - 12_create_dw_mart1_tables.sql

 MART 2 grain and scope:
   - fact_donation_transaction:
       One row per donation transaction.
   - fact_expense_transaction:
       One row per approved/pending/rejected expense transaction.
   - fact_payment_transaction:
       One row per salary/bonus/vendor/refund payment transaction.
   - fact_monthly_financial_snapshot:
       One row per month / center financial summary.
   - fact_donation_lifecycle:
       One row per donation lifecycle record.
   - fact_budget_allocation_event:
       One row per budget allocation event.

 Design choices:
   - No MERGE is used here. This script only creates tables.
   - MART 2 reuses shared dimensions already created in MART 1:
       dw.dim_date, dw.dim_center, dw.dim_child.
   - New MART 2 dimensions use surrogate IDENTITY keys.
   - Unknown dimension rows use key = -1, inserted with IDENTITY_INSERT.
   - Facts keep source natural identifiers for traceability back to staging/source.
   - Re-runnable for development: drops MART 2 facts and MART 2-only dimensions
     before recreating them. Shared MART 1 dimensions are not dropped.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Charity_DW_DB;
GO

/*=============================================================================
  1. Verify Required Schemas and Shared Dimensions
=============================================================================*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw')
BEGIN
    EXEC(N'CREATE SCHEMA dw');
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END
GO

IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL
BEGIN
    THROW 51001, 'Missing shared dimension dw.dim_date. Run 12_create_dw_mart1_tables.sql first.', 1;
END;
GO

IF OBJECT_ID(N'dw.dim_center', N'U') IS NULL
BEGIN
    THROW 51002, 'Missing shared dimension dw.dim_center. Run 12_create_dw_mart1_tables.sql first.', 1;
END;
GO

IF OBJECT_ID(N'dw.dim_child', N'U') IS NULL
BEGIN
    THROW 51003, 'Missing shared dimension dw.dim_child. Run 12_create_dw_mart1_tables.sql first.', 1;
END;
GO

/*=============================================================================
  2. Drop Existing MART 2 Tables - Dependency Order
=============================================================================*/

DROP TABLE IF EXISTS dw.fact_budget_allocation_event;
DROP TABLE IF EXISTS dw.fact_donation_lifecycle;
DROP TABLE IF EXISTS dw.fact_monthly_financial_snapshot;
DROP TABLE IF EXISTS dw.fact_payment_transaction;
DROP TABLE IF EXISTS dw.fact_expense_transaction;
DROP TABLE IF EXISTS dw.fact_donation_transaction;

DROP TABLE IF EXISTS dw.dim_allocation_type;
DROP TABLE IF EXISTS dw.dim_currency;
DROP TABLE IF EXISTS dw.dim_status;
DROP TABLE IF EXISTS dw.dim_donation_type;
DROP TABLE IF EXISTS dw.dim_category;
DROP TABLE IF EXISTS dw.dim_campaign;
DROP TABLE IF EXISTS dw.dim_donor;
GO

/*=============================================================================
  3. MART 2 Dimensions - Heap Tables
=============================================================================*/

CREATE TABLE dw.dim_donor (
    donor_key            INT IDENTITY(1,1) NOT NULL,
    donor_id             INT NULL,
    full_name            NVARCHAR(250) NULL,
    donor_type           NVARCHAR(50) NULL,
    is_active            BIT NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_campaign (
    campaign_key         INT IDENTITY(1,1) NOT NULL,
    campaign_id          INT NULL,
    title                NVARCHAR(250) NULL,
    campaign_status      NVARCHAR(50) NULL,
    target_amount        DECIMAL(18,2) NULL,
    start_date           DATE NULL,
    end_date             DATE NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_category (
    category_key         INT IDENTITY(1,1) NOT NULL,
    category_id          INT NULL,
    category_name        NVARCHAR(200) NULL,
    parent_category_id   INT NULL,
    parent_category_name NVARCHAR(200) NULL,
    category_status      NVARCHAR(30) NULL,
    source_system        NVARCHAR(100) NULL,
    row_hash             VARBINARY(32) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_donation_type (
    donation_type_key    INT IDENTITY(1,1) NOT NULL,
    code                 NVARCHAR(50) NULL,
    title                NVARCHAR(100) NULL,
    source_system        NVARCHAR(100) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_status (
    status_key           INT IDENTITY(1,1) NOT NULL,
    status_type          NVARCHAR(50) NULL,
    code                 NVARCHAR(50) NULL,
    title                NVARCHAR(100) NULL,
    category             NVARCHAR(50) NULL,
    source_system        NVARCHAR(100) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_currency (
    currency_key         INT IDENTITY(1,1) NOT NULL,
    code                 NVARCHAR(10) NULL,
    name                 NVARCHAR(100) NULL,
    source_system        NVARCHAR(100) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

CREATE TABLE dw.dim_allocation_type (
    allocation_type_key  INT IDENTITY(1,1) NOT NULL,
    code                 NVARCHAR(50) NULL,
    title                NVARCHAR(100) NULL,
    source_system        NVARCHAR(100) NULL,
    created_at           DATETIME2(0) NULL,
    updated_at           DATETIME2(0) NULL
);
GO

/*=============================================================================
  4. Unknown Dimension Rows
=============================================================================*/

SET IDENTITY_INSERT dw.dim_donor ON;
INSERT INTO dw.dim_donor
    (donor_key, donor_id, full_name, donor_type, is_active, source_system, row_hash, created_at, updated_at)
VALUES
    (-1, -1, N'Unknown', N'unknown', 0, N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_donor OFF;
GO

SET IDENTITY_INSERT dw.dim_campaign ON;
INSERT INTO dw.dim_campaign
    (campaign_key, campaign_id, title, campaign_status, target_amount, start_date, end_date, source_system, row_hash, created_at, updated_at)
VALUES
    (-1, -1, N'Unknown', N'unknown', NULL, NULL, NULL, N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_campaign OFF;
GO

SET IDENTITY_INSERT dw.dim_category ON;
INSERT INTO dw.dim_category
    (category_key, category_id, category_name, parent_category_id, parent_category_name, category_status, source_system, row_hash, created_at, updated_at)
VALUES
    (-1, -1, N'Unknown', NULL, NULL, N'unknown', N'FINANCE_OPS', NULL, SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_category OFF;
GO

SET IDENTITY_INSERT dw.dim_donation_type ON;
INSERT INTO dw.dim_donation_type
    (donation_type_key, code, title, source_system, created_at, updated_at)
VALUES
    (-1, N'unknown', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_donation_type OFF;
GO

SET IDENTITY_INSERT dw.dim_status ON;
INSERT INTO dw.dim_status
    (status_key, status_type, code, title, category, source_system, created_at, updated_at)
VALUES
    (-1, N'unknown', N'unknown', N'Unknown', N'unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_status OFF;
GO

SET IDENTITY_INSERT dw.dim_currency ON;
INSERT INTO dw.dim_currency
    (currency_key, code, name, source_system, created_at, updated_at)
VALUES
    (-1, N'UNK', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_currency OFF;
GO

SET IDENTITY_INSERT dw.dim_allocation_type ON;
INSERT INTO dw.dim_allocation_type
    (allocation_type_key, code, title, source_system, created_at, updated_at)
VALUES
    (-1, N'unknown', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);
SET IDENTITY_INSERT dw.dim_allocation_type OFF;
GO

/*=============================================================================
  5. Fact Tables - MART 2 Heap Tables
=============================================================================*/

CREATE TABLE dw.fact_donation_transaction (
    donation_transaction_key BIGINT IDENTITY(1,1) NOT NULL,
    date_key                 INT NULL,
    donor_key                INT NULL,
    campaign_key             INT NULL,
    center_key               INT NULL,
    donation_type_key        INT NULL,
    currency_key             INT NULL,
    status_key               INT NULL,

    amount                   DECIMAL(18,2) NULL,
    is_confirmed             BIT NULL,
    is_refunded              BIT NULL,

    source_donation_id       BIGINT NULL,
    source_donor_id          BIGINT NULL,
    source_campaign_id       BIGINT NULL,
    source_reference_code    NVARCHAR(100) NULL,
    source_system            NVARCHAR(100) NULL,
    etl_batch_id             INT NULL,
    loaded_at                DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_expense_transaction (
    expense_transaction_key BIGINT IDENTITY(1,1) NOT NULL,
    date_key                INT NULL,
    center_key              INT NULL,
    child_key               INT NULL,
    category_key            INT NULL,
    currency_key            INT NULL,
    status_key              INT NULL,

    amount                  DECIMAL(18,2) NULL,
    is_approved             BIT NULL,
    is_rejected             BIT NULL,
    description             NVARCHAR(2000) NULL,

    source_expense_id       BIGINT NULL,
    source_center_id        BIGINT NULL,
    source_child_id         BIGINT NULL,
    source_category_id      BIGINT NULL,
    source_system           NVARCHAR(100) NULL,
    etl_batch_id            INT NULL,
    loaded_at               DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_payment_transaction (
    payment_transaction_key BIGINT IDENTITY(1,1) NOT NULL,
    date_key                INT NULL,
    center_key              INT NULL,
    currency_key            INT NULL,
    status_key              INT NULL,

    payment_type            NVARCHAR(50) NULL,
    source_teacher_id       BIGINT NULL,
    amount                  DECIMAL(18,2) NULL,
    is_paid                 BIT NULL,
    is_cancelled            BIT NULL,

    source_payment_id       BIGINT NULL,
    source_center_id        BIGINT NULL,
    source_system           NVARCHAR(100) NULL,
    etl_batch_id            INT NULL,
    loaded_at               DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_monthly_financial_snapshot (
    monthly_financial_snapshot_key BIGINT IDENTITY(1,1) NOT NULL,
    month_key                      INT NULL,
    center_key                     INT NULL,

    total_donation_amount          DECIMAL(18,2) NULL,
    total_expense_amount           DECIMAL(18,2) NULL,
    total_payment_amount           DECIMAL(18,2) NULL,
    net_balance                    DECIMAL(18,2) NULL,
    donation_count                 INT NULL,
    expense_count                  INT NULL,
    payment_count                  INT NULL,
    allocation_count               INT NULL,

    source_system                  NVARCHAR(100) NULL,
    etl_batch_id                   INT NULL,
    loaded_at                      DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_donation_lifecycle (
    donation_lifecycle_key BIGINT IDENTITY(1,1) NOT NULL,
    donor_key              INT NULL,
    campaign_key           INT NULL,
    created_date_key       INT NULL,
    confirmed_date_key     INT NULL,
    allocated_date_key     INT NULL,
    lifecycle_status_key   INT NULL,

    current_stage          NVARCHAR(50) NULL,
    donation_amount        DECIMAL(18,2) NULL,
    days_to_confirm        INT NULL,
    days_to_allocate       INT NULL,

    source_donation_id     BIGINT NULL,
    source_donor_id        BIGINT NULL,
    source_campaign_id     BIGINT NULL,
    source_system          NVARCHAR(100) NULL,
    etl_batch_id           INT NULL,
    loaded_at              DATETIME2(0) NULL
);
GO

CREATE TABLE dw.fact_budget_allocation_event (
    allocation_event_key   BIGINT IDENTITY(1,1) NOT NULL,
    date_key               INT NULL,
    donor_key              INT NULL,
    center_key             INT NULL,
    child_key              INT NULL,
    category_key           INT NULL,
    campaign_key           INT NULL,
    allocation_type_key    INT NULL,

    allocated_amount       DECIMAL(18,2) NULL,
    reason                 NVARCHAR(MAX) NULL,
    source_allocation_id   BIGINT NULL,
    source_type            NVARCHAR(50) NULL,
    source_id              BIGINT NULL,
    source_center_id       BIGINT NULL,
    source_child_id        BIGINT NULL,
    source_category_id     BIGINT NULL,
    source_system          NVARCHAR(100) NULL,
    etl_batch_id           INT NULL,
    loaded_at              DATETIME2(0) NULL
);
GO

/*=============================================================================
  6. Optional Analytics Acceleration After Bulk Loading

  Keep this disabled during ETL loading. Enable only after loading if analytical
  reads are more important than raw insert/update speed.
=============================================================================*/

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_donation_transaction
--     ON dw.fact_donation_transaction;
-- GO

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_expense_transaction
--     ON dw.fact_expense_transaction;
-- GO
--
-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_payment_transaction
--     ON dw.fact_payment_transaction;
-- GO
--
-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_monthly_financial_snapshot
--     ON dw.fact_monthly_financial_snapshot;
-- GO

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_donation_lifecycle
--     ON dw.fact_donation_lifecycle;
-- GO

-- CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_budget_allocation_event
--     ON dw.fact_budget_allocation_event;
-- GO
