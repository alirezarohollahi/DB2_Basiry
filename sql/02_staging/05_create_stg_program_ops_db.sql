/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : Phase 2 - Staging Layer
 File         : 05_create_stg_program_ops_db.sql
 DBMS         : Microsoft SQL Server
 Tool         : SQL Server Management Studio (SSMS)

 Purpose:
   Create the staging database for Program Operations source data.

 Naming decision:
   Database : Stg_ProgramOps_DB
   Schema   : stg_program_ops

 Why this name:
   - `Stg_ProgramOps_DB` clearly identifies this as the staging database for
     the Program Operations source system.
   - `stg_program_ops` keeps the schema name aligned with the source schema
     `program_ops`, while clearly separating staging from source.
   - Later, a separate finance staging database/schema can be created:
       Database : Stg_FinanceOps_DB
       Schema   : stg_finance_ops

 Current scope:
   - For now, this script only creates the staging database and schema.
   - Staging tables will be created in the next step.

 Future staging table pattern:
   Staging tables should generally copy source columns and add ETL metadata:

       etl_batch_id
       source_system
       extracted_at
       source_database
       source_schema
       source_table
       row_hash
       is_valid
       validation_message

 Suggested future table naming:
   - stg_program_ops.centers
   - stg_program_ops.children
   - stg_program_ops.teachers
   - stg_program_ops.task_assessments

 Source system:
   - Source_ProgramOps_DB.program_ops
===============================================================================
*/

SET NOCOUNT ON;
GO

/*=============================================================================
  1. Create Staging Database
=============================================================================*/

IF DB_ID(N'Stg_ProgramOps_DB') IS NULL
BEGIN
    CREATE DATABASE Stg_ProgramOps_DB;
END
GO

USE Stg_ProgramOps_DB;
GO

/*=============================================================================
  2. Create Staging Schema
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'stg_program_ops'
)
BEGIN
    EXEC(N'CREATE SCHEMA stg_program_ops');
END
GO

/*=============================================================================
  3. Create ETL Admin Schema
     This is reserved for batch control, load logging, and ETL metadata.
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
  4. Create Minimal ETL Batch Control Table
     This table is useful now, even before staging tables are created.
=============================================================================*/

IF OBJECT_ID(N'etl_admin.etl_batch', N'U') IS NULL
BEGIN
    CREATE TABLE etl_admin.etl_batch (
        etl_batch_id        INT IDENTITY(1,1) NOT NULL,
        source_system       NVARCHAR(100) NOT NULL,
        target_layer        NVARCHAR(100) NOT NULL,
        batch_status        NVARCHAR(50) NOT NULL CONSTRAINT DF_etl_batch_status DEFAULT (N'created'),
        started_at          DATETIME2(0) NOT NULL CONSTRAINT DF_etl_batch_started_at DEFAULT (SYSDATETIME()),
        ended_at            DATETIME2(0) NULL,
        rows_extracted      INT NULL,
        rows_inserted       INT NULL,
        rows_rejected       INT NULL,
        error_message       NVARCHAR(MAX) NULL,
        created_by          NVARCHAR(128) NOT NULL CONSTRAINT DF_etl_batch_created_by DEFAULT (SUSER_SNAME()),

        CONSTRAINT PK_etl_batch PRIMARY KEY CLUSTERED (etl_batch_id),
        CONSTRAINT CK_etl_batch_status
            CHECK (batch_status IN (N'created', N'running', N'succeeded', N'failed', N'cancelled'))
    );
END
GO

/*=============================================================================
  5. Create Minimal ETL Load Log Table
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
        rows_written        INT NULL,
        rows_rejected       INT NULL,
        started_at          DATETIME2(0) NOT NULL CONSTRAINT DF_etl_load_log_started_at DEFAULT (SYSDATETIME()),
        ended_at            DATETIME2(0) NULL,
        message             NVARCHAR(MAX) NULL,

        CONSTRAINT PK_etl_load_log PRIMARY KEY CLUSTERED (etl_load_log_id),
        CONSTRAINT FK_etl_load_log_etl_batch
            FOREIGN KEY (etl_batch_id) REFERENCES etl_admin.etl_batch(etl_batch_id),
        CONSTRAINT CK_etl_load_log_status
            CHECK (load_status IN (N'created', N'running', N'succeeded', N'failed', N'skipped'))
    );
END
GO

/*=============================================================================
  6. Helpful Indexes
=============================================================================*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_etl_batch_source_status'
      AND object_id = OBJECT_ID(N'etl_admin.etl_batch')
)
BEGIN
    CREATE INDEX IX_etl_batch_source_status
        ON etl_admin.etl_batch(source_system, target_layer, batch_status, started_at);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_etl_load_log_batch'
      AND object_id = OBJECT_ID(N'etl_admin.etl_load_log')
)
BEGIN
    CREATE INDEX IX_etl_load_log_batch
        ON etl_admin.etl_load_log(etl_batch_id, load_status, started_at);
END
GO

/*=============================================================================
  7. Completion Message
=============================================================================*/

PRINT 'Stg_ProgramOps_DB created successfully.';
PRINT 'Schemas created or verified: stg_program_ops, etl_admin';
PRINT 'Minimal ETL admin tables created or verified: etl_batch, etl_load_log';
PRINT 'Next step: create staging tables under stg_program_ops.';
GO
