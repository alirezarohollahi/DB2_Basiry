/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 3 - Data Warehouse Layer
 File         : 11_create_dw_db.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create the central data warehouse database and required schemas.

 Naming decision:
   Database : Charity_DW_DB
   Schema   : dw
   Admin    : etl_admin

 Scope:
   This script prepares the database for MART 1 and future marts.
   MART 1 table creation is handled by:
   - 12_create_dw_mart1_tables.sql
===============================================================================
*/

SET NOCOUNT ON;
GO

/*=============================================================================
  1. Create Data Warehouse Database
=============================================================================*/

IF DB_ID(N'Charity_DW_DB') IS NULL
BEGIN
    CREATE DATABASE Charity_DW_DB;
END
GO

USE Charity_DW_DB;
GO

/*=============================================================================
  2. Create Data Warehouse Schema
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'dw'
)
BEGIN
    EXEC(N'CREATE SCHEMA dw');
END
GO

/*=============================================================================
  3. Create ETL Admin Schema
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'etl_admin'
)
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END
GO

/*=============================================================================
  4. Create DW ETL Batch Control Table
=============================================================================*/

IF OBJECT_ID(N'etl_admin.etl_batch', N'U') IS NULL
BEGIN
    CREATE TABLE etl_admin.etl_batch (
        etl_batch_id        INT IDENTITY(1,1) NOT NULL,
        source_system       NVARCHAR(100) NOT NULL,
        target_layer        NVARCHAR(100) NOT NULL,
        mart_name           NVARCHAR(100) NULL,
        batch_status        NVARCHAR(50) NOT NULL,
        started_at          DATETIME2(0) NOT NULL,
        ended_at            DATETIME2(0) NULL,
        rows_read           INT NULL,
        rows_inserted       INT NULL,
        rows_updated        INT NULL,
        rows_rejected       INT NULL,
        error_message       NVARCHAR(MAX) NULL,
        created_by          NVARCHAR(128) NOT NULL
    );
END
GO

/*=============================================================================
  5. Create DW ETL Load Log Table
=============================================================================*/

IF OBJECT_ID(N'etl_admin.etl_load_log', N'U') IS NULL
BEGIN
    CREATE TABLE etl_admin.etl_load_log (
        etl_load_log_id     BIGINT IDENTITY(1,1) NOT NULL,
        etl_batch_id        INT NULL,
        source_database     NVARCHAR(128) NOT NULL,
        source_schema       NVARCHAR(128) NOT NULL,
        source_table        NVARCHAR(128) NOT NULL,
        target_database     NVARCHAR(128) NOT NULL,
        target_schema       NVARCHAR(128) NOT NULL,
        target_table        NVARCHAR(128) NOT NULL,
        load_status         NVARCHAR(50) NOT NULL,
        rows_read           INT NULL,
        rows_inserted       INT NULL,
        rows_updated        INT NULL,
        rows_rejected       INT NULL,
        started_at          DATETIME2(0) NOT NULL,
        ended_at            DATETIME2(0) NULL,
        message             NVARCHAR(MAX) NULL
    );
END
GO

/*=============================================================================
  6. Helpful Indexes
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_dw_etl_batch_status'
      AND object_id = OBJECT_ID(N'etl_admin.etl_batch')
)
BEGIN
    CREATE INDEX IX_dw_etl_batch_status
        ON etl_admin.etl_batch(source_system, target_layer, mart_name, batch_status, started_at);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_dw_etl_load_log_batch'
      AND object_id = OBJECT_ID(N'etl_admin.etl_load_log')
)
BEGIN
    CREATE INDEX IX_dw_etl_load_log_batch
        ON etl_admin.etl_load_log(etl_batch_id, target_schema, target_table, load_status);
END
GO