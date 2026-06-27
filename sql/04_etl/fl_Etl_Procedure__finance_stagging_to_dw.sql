USE Charity_DW_DB;
GO

CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_donor
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id BIGINT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT,
          @rows_work_inserted INT,
          @rows_work_updated INT,
          @rows_deleted INT,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52001, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52002, '@start_time must be earlier than @end_time.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = SCOPE_IDENTITY();

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donors',
             N'Charity_DW_DB', N'dw', N'dim_donor',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_donor. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              donor_id          INT NOT NULL PRIMARY KEY,
              full_name         NVARCHAR(250) NULL,
              donor_type        NVARCHAR(50) NULL,
              is_active         BIT NULL,
              source_system     NVARCHAR(100) NULL,
              row_hash          VARBINARY(32) NULL,
              created_at        DATETIME2(0) NULL,
              updated_at        DATETIME2(0) NULL,
              source_updated_at DATETIME2(0) NULL,
              stg_row_id        BIGINT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donors',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #donor_work
        (
              donor_id          INT NOT NULL PRIMARY KEY,
              full_name         NVARCHAR(250) NULL,
              donor_type        NVARCHAR(50) NULL,
              is_active         BIT NULL,
              source_system     NVARCHAR(100) NULL,
              row_hash          VARBINARY(32) NULL,
              created_at        DATETIME2(0) NULL,
              updated_at        DATETIME2(0) NULL,
              source_updated_at DATETIME2(0) NULL,
              stg_row_id        BIGINT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donors',
             N'tempdb', N'#', N'#donor_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #donor_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  donor_id,
                  full_name,
                  donor_type,
                  is_active,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  s.id,
                  s.full_name,
                  s.donor_type,
                  s.is_active,
                  s.source_system,
                  s.row_hash,
                  s.created_at,
                  s.updated_at,
                  COALESCE(s.source_updated_at, s.updated_at, s.created_at),
                  s.stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.donors AS s
            WHERE s.is_valid = 1
              AND s.id IS NOT NULL
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at) >= @current_from
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at) <  @current_to
              AND NOT EXISTS
              (
                    SELECT 1
                    FROM Stg_FinanceOps_DB.stg_finance_ops.donors AS s2
                    WHERE s2.is_valid = 1
                      AND s2.id = s.id
                      AND COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at) >= @current_from
                      AND COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at) <  @current_to
                      AND
                      (
                           COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at)
                               > COALESCE(s.source_updated_at, s.updated_at, s.created_at)
                           OR
                           (
                               COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at)
                                   = COALESCE(s.source_updated_at, s.updated_at, s.created_at)
                               AND s2.stg_row_id > s.stg_row_id
                           )
                      )
              );

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donors',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted daily donor rows into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.full_name         = s.full_name,
                   w.donor_type        = s.donor_type,
                   w.is_active         = s.is_active,
                   w.source_system     = s.source_system,
                   w.row_hash          = s.row_hash,
                   w.created_at        = s.created_at,
                   w.updated_at        = s.updated_at,
                   w.source_updated_at = s.source_updated_at,
                   w.stg_row_id        = s.stg_row_id
            FROM #donor_work AS w
            INNER JOIN #loop_src AS s
                    ON s.donor_id = w.donor_id
            WHERE s.source_updated_at > w.source_updated_at
               OR (s.source_updated_at = w.source_updated_at AND s.stg_row_id > w.stg_row_id);

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#donor_work',
                 N'succeeded', @rows_loop_inserted, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Updated existing rows in #donor_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #donor_work
            (
                  donor_id,
                  full_name,
                  donor_type,
                  is_active,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  s.donor_id,
                  s.full_name,
                  s.donor_type,
                  s.is_active,
                  s.source_system,
                  s.row_hash,
                  s.created_at,
                  s.updated_at,
                  s.source_updated_at,
                  s.stg_row_id
            FROM #loop_src AS s
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #donor_work AS w
                WHERE w.donor_id = s.donor_id
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#donor_work',
                 N'succeeded', @rows_loop_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted new rows into #donor_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @current_from = @current_to;
        END;

        BEGIN TRANSACTION;

            TRUNCATE TABLE dw.dim_donor;

            SET IDENTITY_INSERT dw.dim_donor ON;

            INSERT INTO dw.dim_donor
            (
                  donor_key,
                  donor_id,
                  full_name,
                  donor_type,
                  is_active,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at
            )
            VALUES
            (
                  -1,
                  -1,
                  N'Unknown',
                  N'unknown',
                  0,
                  N'FINANCE_OPS',
                  NULL,
                  SYSDATETIME(),
                  NULL
            );

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_donor OFF;

            DBCC CHECKIDENT ('dw.dim_donor', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO dw.dim_donor
            (
                  donor_id,
                  full_name,
                  donor_type,
                  is_active,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at
            )
            SELECT
                  w.donor_id,
                  w.full_name,
                  w.donor_type,
                  w.is_active,
                  w.source_system,
                  w.row_hash,
                  w.created_at,
                  w.updated_at
            FROM #donor_work AS w;

            SET @rows_dim_inserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donor',
             N'Charity_DW_DB', N'dw', N'dim_donor',
             N'succeeded', NULL, NULL,
             0, 0,
             SYSDATETIME(), SYSDATETIME(), N'Truncated dw.dim_donor for first load.');

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'constant', N'unknown_row', N'dim_donor',
             N'Charity_DW_DB', N'dw', N'dim_donor',
             N'succeeded', 1, @rows_unknown_inserted,
             0, 0,
             SYSDATETIME(), SYSDATETIME(), N'Inserted unknown donor row with donor_key = -1.');

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donor',
             N'Charity_DW_DB', N'dw', N'dim_donor',
             N'succeeded', 0, 0,
             0, 0,
             SYSDATETIME(), SYSDATETIME(), N'Reset dim_donor identity seed to 0; next real donor_key starts from 1.');

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#donor_work',
             N'Charity_DW_DB', N'dw', N'dim_donor',
             N'succeeded', @rows_work_inserted_total, @rows_dim_inserted,
             0, 0,
             SYSDATETIME(), SYSDATETIME(), N'Inserted business donor rows into dw.dim_donor.');

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_dim_inserted + @rows_unknown_inserted,
               rows_rejected = 0,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(N'First load succeeded. Staging rows read: ', @rows_read_total,
                                      N'; work inserted: ', @rows_work_inserted_total,
                                      N'; work updated: ', @rows_work_updated_total,
                                      N'; dimension inserted: ', @rows_dim_inserted,
                                      N'; unknown inserted: ', @rows_unknown_inserted, N'.')
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            IF OBJECT_ID('dw.dim_donor', 'U') IS NOT NULL
            BEGIN
                BEGIN TRY
                    SET IDENTITY_INSERT dw.dim_donor OFF;
                END TRY
                BEGIN CATCH
                END CATCH;
            END;

            ROLLBACK TRANSACTION;
        END;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   rows_read     = @rows_read_total,
                   rows_inserted  = ISNULL(@rows_dim_inserted, 0) + ISNULL(@rows_unknown_inserted, 0),
                   rows_rejected = 0,
                   ended_at      = SYSDATETIME(),
                   message       = @error_message
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 15_create_dw_dim_campaign_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.dim_campaign from
   Stg_FinanceOps_DB.stg_finance_ops.campaigns.

 Procedures:
   1. etl_admin.usp_first_load_dw_dim_campaign
      - First/full load for a selected period.
      - Builds a temp working set day by day from @start_time to @end_time.
      - Truncates dw.dim_campaign.
      - Resets the identity seed so the next real campaign_key starts from 1.
      - Re-inserts the unknown row with campaign_key = -1.
      - Inserts all period campaigns.

   2. etl_admin.usp_load_dw_dim_campaign_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day from @start_time to @end_time.
      - Ensures the unknown row exists.
      - Resets identity seed to MAX(campaign_key), so the next inserted row uses
        MAX(campaign_key) + 1.
      - Updates changed campaigns by dimension-level row_hash.
      - Inserts new campaigns.

 SCD design:
   - dw.dim_campaign does not contain effective_from/effective_to/is_current.
   - Therefore this procedure implements SCD Type 1.
   - Existing campaign rows are overwritten when campaign attributes change.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch
===============================================================================
*/

SET NOCOUNT ON;
GO



/*=============================================================================
  Procedure 1: First / Full Period Load for dw.dim_campaign
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_campaign
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT = 0,
          @rows_work_inserted INT = 0,
          @rows_work_updated INT = 0,
          @rows_deleted INT = 0,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @sql NVARCHAR(MAX),
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52101, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52102, '@start_time must be earlier than @end_time.', 1;
    END;

    IF OBJECT_ID(N'dw.dim_campaign', N'U') IS NULL
    BEGIN
        THROW 52103, 'Missing target table Charity_DW_DB.dw.dim_campaign.', 1;
    END;

    IF OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.campaigns', N'U') IS NULL
    BEGIN
        THROW 52104, 'Missing source table Stg_FinanceOps_DB.stg_finance_ops.campaigns.', 1;
    END;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
       OR OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
    BEGIN
        THROW 52105, 'Missing ETL admin log tables in Charity_DW_DB.etl_admin.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION_FIRST_LOAD', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'campaigns',
             N'Charity_DW_DB', N'dw', N'dim_campaign',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_campaign. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N'). SCD Type: 1.'));

        SET @main_log_id = SCOPE_IDENTITY();

        IF OBJECT_ID('tempdb..#loop_src') IS NOT NULL DROP TABLE #loop_src;
        IF OBJECT_ID('tempdb..#campaign_work') IS NOT NULL DROP TABLE #campaign_work;

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              campaign_id       INT NOT NULL,
              source_system     NVARCHAR(100) NOT NULL,
              title             NVARCHAR(250) NULL,
              campaign_status   NVARCHAR(50) NULL,
              target_amount     DECIMAL(18,2) NULL,
              start_date        DATE NULL,
              end_date          DATE NULL,
              row_hash          VARBINARY(32) NULL,
              created_at        DATETIME2(0) NULL,
              updated_at        DATETIME2(0) NULL,
              source_updated_at DATETIME2(0) NULL,
              stg_row_id        BIGINT NULL,
              PRIMARY KEY (campaign_id, source_system)
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'campaigns',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #campaign_work
        (
              campaign_id       INT NOT NULL,
              source_system     NVARCHAR(100) NOT NULL,
              title             NVARCHAR(250) NULL,
              campaign_status   NVARCHAR(50) NULL,
              target_amount     DECIMAL(18,2) NULL,
              start_date        DATE NULL,
              end_date          DATE NULL,
              row_hash          VARBINARY(32) NULL,
              created_at        DATETIME2(0) NULL,
              updated_at        DATETIME2(0) NULL,
              source_updated_at DATETIME2(0) NULL,
              stg_row_id        BIGINT NULL,
              PRIMARY KEY (campaign_id, source_system)
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'campaigns',
             N'tempdb', N'#', N'#campaign_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #campaign_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  campaign_id,
                  source_system,
                  title,
                  campaign_status,
                  target_amount,
                  start_date,
                  end_date,
                  row_hash,
                  created_at,
                  updated_at,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  s.id AS campaign_id,
                  ISNULL(s.source_system, N'FINANCE_OPS') AS source_system,
                  LEFT(s.title, 250) AS title,
                  s.status AS campaign_status,
                  s.target_amount,
                  s.start_date,
                  s.end_date,
                  HASHBYTES(
                      'SHA2_256',
                      CONCAT(
                          ISNULL(CONVERT(NVARCHAR(50), s.id), N'<NULL>'), N'|',
                          ISNULL(ISNULL(s.source_system, N'FINANCE_OPS'), N'<NULL>'), N'|',
                          ISNULL(LEFT(s.title, 250), N'<NULL>'), N'|',
                          ISNULL(s.status, N'<NULL>'), N'|',
                          ISNULL(CONVERT(NVARCHAR(50), s.target_amount), N'<NULL>'), N'|',
                          ISNULL(CONVERT(NVARCHAR(30), s.start_date, 126), N'<NULL>'), N'|',
                          ISNULL(CONVERT(NVARCHAR(30), s.end_date, 126), N'<NULL>')
                      )
                  ) AS row_hash,
                  s.created_at,
                  s.updated_at,
                  COALESCE(s.source_updated_at, s.updated_at, s.created_at) AS source_updated_at,
                  s.stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns AS s
            WHERE s.is_valid = 1
              AND s.id IS NOT NULL
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at) >= @current_from
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at) <  @current_to
              AND s.stg_row_id =
              (
                  SELECT MAX(s2.stg_row_id)
                  FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns AS s2
                  WHERE s2.is_valid = 1
                    AND s2.id = s.id
                    AND ISNULL(s2.source_system, N'FINANCE_OPS') = ISNULL(s.source_system, N'FINANCE_OPS')
                    AND COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at) >= @current_from
                    AND COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at) <  @current_to
              );

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total = @rows_read_total + @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'campaigns',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted source campaigns into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.title             = l.title,
                   w.campaign_status   = l.campaign_status,
                   w.target_amount     = l.target_amount,
                   w.start_date        = l.start_date,
                   w.end_date          = l.end_date,
                   w.row_hash          = l.row_hash,
                   w.created_at        = l.created_at,
                   w.updated_at        = l.updated_at,
                   w.source_updated_at = l.source_updated_at,
                   w.stg_row_id        = l.stg_row_id
            FROM #campaign_work AS w
            INNER JOIN #loop_src AS l
                    ON l.campaign_id = w.campaign_id
                   AND l.source_system = w.source_system
            WHERE l.source_updated_at >= ISNULL(w.source_updated_at, '19000101');

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total = @rows_work_updated_total + @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#campaign_work',
                 N'succeeded', @rows_work_updated, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Updated existing campaign rows inside #campaign_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #campaign_work
            (
                  campaign_id,
                  source_system,
                  title,
                  campaign_status,
                  target_amount,
                  start_date,
                  end_date,
                  row_hash,
                  created_at,
                  updated_at,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  l.campaign_id,
                  l.source_system,
                  l.title,
                  l.campaign_status,
                  l.target_amount,
                  l.start_date,
                  l.end_date,
                  l.row_hash,
                  l.created_at,
                  l.updated_at,
                  l.source_updated_at,
                  l.stg_row_id
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #campaign_work AS w
                WHERE w.campaign_id = l.campaign_id
                  AND w.source_system = l.source_system
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total = @rows_work_inserted_total + @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#campaign_work',
                 N'succeeded', @rows_work_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted new campaign rows into #campaign_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @current_from = @current_to;
        END;

        SET @step_started_at = SYSDATETIME();

        BEGIN TRANSACTION;

            TRUNCATE TABLE dw.dim_campaign;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_campaign',
                 N'Charity_DW_DB', N'dw', N'dim_campaign',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Truncated dw.dim_campaign for first load.');

            SET @step_started_at = SYSDATETIME();

            DBCC CHECKIDENT ('dw.dim_campaign', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_campaign',
                 N'Charity_DW_DB', N'dw', N'dim_campaign',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Reset dw.dim_campaign identity seed to 0 for first load.');

            SET @step_started_at = SYSDATETIME();

            SET IDENTITY_INSERT dw.dim_campaign ON;

            INSERT INTO dw.dim_campaign
            (
                  campaign_key,
                  campaign_id,
                  title,
                  campaign_status,
                  target_amount,
                  start_date,
                  end_date,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at
            )
            VALUES
            (
                  -1,
                  -1,
                  N'Unknown',
                  N'unknown',
                  NULL,
                  NULL,
                  NULL,
                  N'FINANCE_OPS',
                  NULL,
                  SYSDATETIME(),
                  NULL
            );

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_campaign OFF;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'constant', N'unknown_row', N'dim_campaign',
                 N'Charity_DW_DB', N'dw', N'dim_campaign',
                 N'succeeded', 1, @rows_unknown_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Inserted unknown row into dw.dim_campaign with campaign_key = -1.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.dim_campaign
            (
                  campaign_id,
                  title,
                  campaign_status,
                  target_amount,
                  start_date,
                  end_date,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at
            )
            SELECT
                  w.campaign_id,
                  w.title,
                  w.campaign_status,
                  w.target_amount,
                  w.start_date,
                  w.end_date,
                  w.source_system,
                  w.row_hash,
                  w.created_at,
                  w.updated_at
            FROM #campaign_work AS w;

            SET @rows_dim_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#campaign_work',
                 N'Charity_DW_DB', N'dw', N'dim_campaign',
                 N'succeeded', @rows_dim_inserted, @rows_dim_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Inserted first-load campaign rows into dw.dim_campaign.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_unknown_inserted + @rows_dim_inserted,
               rows_rejected = 0,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(
                                  N'First load succeeded for dw.dim_campaign. ',
                                  N'SCD Type 1. ',
                                  N'Rows read from staging: ', @rows_read_total,
                                  N'; work inserted: ', @rows_work_inserted_total,
                                  N'; work updated: ', @rows_work_updated_total,
                                  N'; unknown inserted: ', @rows_unknown_inserted,
                                  N'; dim inserted: ', @rows_dim_inserted,
                                  N'; period: [', CONVERT(NVARCHAR(30), @start_time, 126),
                                  N', ', CONVERT(NVARCHAR(30), @end_time, 126), N').'
                               )
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   rows_read     = @rows_read_total,
                   rows_inserted  = ISNULL(@rows_unknown_inserted, 0) + ISNULL(@rows_dim_inserted, 0),
                   rows_rejected = 0,
                   ended_at      = SYSDATETIME(),
                   message       = @error_message
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO

/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 16_create_dw_dim_category_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.dim_category from
   Stg_FinanceOps_DB.stg_finance_ops.expense_categories.

 Procedures:
   1. etl_admin.usp_first_load_dw_dim_category
      - First/full load for a selected period.
      - Builds a temp working set day by day from @start_time to @end_time.
      - Resolves parent_category_name from the current work set first, then from
        the latest available staging parent row before @end_time.
      - Truncates dw.dim_category.
      - Resets the identity seed so the next real category_key starts from 1.
      - Re-inserts the unknown row with category_key = -1.
      - Inserts all period categories.

   2. etl_admin.usp_load_dw_dim_category_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day from @start_time to @end_time.
      - Resolves parent_category_name from the current work set first, then from
        existing dw.dim_category, then from latest staging parent row before @end_time.
      - Ensures the unknown row exists.
      - Resets identity seed to MAX(category_key), so the next inserted row uses
        MAX(category_key) + 1.
      - Updates changed categories by dimension-level row_hash or parent name change.
      - Inserts new categories.
      - Refreshes denormalized parent_category_name for existing child categories
        after parent category changes.

 SCD design:
   - dw.dim_category does not contain effective_from/effective_to/is_current.
   - Therefore these procedures implement SCD Type 1.
   - Existing category rows are overwritten when source attributes change.
   - parent_category_name is a denormalized derived attribute and is refreshed
     whenever the parent name is available/changed.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch
===============================================================================
*/


/*=============================================================================
  Procedure 1: First / Full Period Load for dw.dim_category
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_category
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT = 0,
          @rows_work_inserted INT = 0,
          @rows_work_updated INT = 0,
          @rows_deleted INT = 0,
          @rows_parent_updated INT = 0,
          @rows_parent_staging_updated INT = 0,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52301, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52302, '@start_time must be earlier than @end_time.', 1;
    END;

    IF OBJECT_ID(N'dw.dim_category', N'U') IS NULL
    BEGIN
        THROW 52303, 'Missing target table Charity_DW_DB.dw.dim_category.', 1;
    END;

    IF OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.expense_categories', N'U') IS NULL
    BEGIN
        THROW 52304, 'Missing source table Stg_FinanceOps_DB.stg_finance_ops.expense_categories.', 1;
    END;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
       OR OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
    BEGIN
        THROW 52305, 'Missing ETL admin log tables in Charity_DW_DB.etl_admin.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION_FIRST_LOAD', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expense_categories',
             N'Charity_DW_DB', N'dw', N'dim_category',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_category. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N'). SCD Type: 1.'));

        SET @main_log_id = SCOPE_IDENTITY();

        IF OBJECT_ID('tempdb..#loop_src') IS NOT NULL DROP TABLE #loop_src;
        IF OBJECT_ID('tempdb..#category_work') IS NOT NULL DROP TABLE #category_work;

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              category_id          INT NOT NULL,
              source_system        NVARCHAR(100) NOT NULL,
              category_name        NVARCHAR(200) NULL,
              parent_category_id   INT NULL,
              parent_category_name NVARCHAR(200) NULL,
              category_status      NVARCHAR(30) NULL,
              row_hash             VARBINARY(32) NULL,
              created_at           DATETIME2(0) NULL,
              updated_at           DATETIME2(0) NULL,
              source_updated_at    DATETIME2(0) NULL,
              stg_row_id           BIGINT NULL,
              PRIMARY KEY (category_id, source_system)
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expense_categories',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #category_work
        (
              category_id          INT NOT NULL,
              source_system        NVARCHAR(100) NOT NULL,
              category_name        NVARCHAR(200) NULL,
              parent_category_id   INT NULL,
              parent_category_name NVARCHAR(200) NULL,
              category_status      NVARCHAR(30) NULL,
              row_hash             VARBINARY(32) NULL,
              created_at           DATETIME2(0) NULL,
              updated_at           DATETIME2(0) NULL,
              source_updated_at    DATETIME2(0) NULL,
              stg_row_id           BIGINT NULL,
              PRIMARY KEY (category_id, source_system)
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expense_categories',
             N'tempdb', N'#', N'#category_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #category_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  category_id,
                  source_system,
                  category_name,
                  parent_category_id,
                  parent_category_name,
                  category_status,
                  row_hash,
                  created_at,
                  updated_at,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  s.id AS category_id,
                  ISNULL(s.source_system, N'FINANCE_OPS') AS source_system,
                  LEFT(s.name, 200) AS category_name,
                  s.parent_id AS parent_category_id,
                  NULL AS parent_category_name,
                  CASE
                      WHEN s.is_active = 1 THEN N'active'
                      WHEN s.is_active = 0 THEN N'inactive'
                      ELSE N'unknown'
                  END AS category_status,
                  HASHBYTES(
                      'SHA2_256',
                      CONCAT(
                          ISNULL(CONVERT(NVARCHAR(50), s.id), N'<NULL>'), N'|',
                          ISNULL(ISNULL(s.source_system, N'FINANCE_OPS'), N'<NULL>'), N'|',
                          ISNULL(LEFT(s.name, 200), N'<NULL>'), N'|',
                          ISNULL(CONVERT(NVARCHAR(50), s.parent_id), N'<NULL>'), N'|',
                          ISNULL(CONVERT(NVARCHAR(10), s.is_active), N'<NULL>')
                      )
                  ) AS row_hash,
                  s.created_at,
                  s.updated_at,
                  COALESCE(s.source_updated_at, s.updated_at, s.created_at) AS source_updated_at,
                  s.stg_row_id
            FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories AS s
            WHERE s.is_valid = 1
              AND s.id IS NOT NULL
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at) >= @current_from
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at) <  @current_to
              AND NOT EXISTS
              (
                  SELECT 1
                  FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories AS s2
                  WHERE s2.is_valid = 1
                    AND s2.id = s.id
                    AND ISNULL(s2.source_system, N'FINANCE_OPS') = ISNULL(s.source_system, N'FINANCE_OPS')
                    AND COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at) >= @current_from
                    AND COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at) <  @current_to
                    AND
                    (
                           COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at)
                         > COALESCE(s.source_updated_at, s.updated_at, s.created_at)
                        OR
                        (
                               COALESCE(s2.source_updated_at, s2.updated_at, s2.created_at)
                             = COALESCE(s.source_updated_at, s.updated_at, s.created_at)
                           AND s2.stg_row_id > s.stg_row_id
                        )
                    )
              );

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total = @rows_read_total + @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expense_categories',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted source categories into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.category_name        = l.category_name,
                   w.parent_category_id   = l.parent_category_id,
                   w.parent_category_name = l.parent_category_name,
                   w.category_status      = l.category_status,
                   w.row_hash             = l.row_hash,
                   w.created_at           = l.created_at,
                   w.updated_at           = l.updated_at,
                   w.source_updated_at    = l.source_updated_at,
                   w.stg_row_id           = l.stg_row_id
            FROM #category_work AS w
            INNER JOIN #loop_src AS l
                    ON l.category_id = w.category_id
                   AND l.source_system = w.source_system
            WHERE l.source_updated_at >= ISNULL(w.source_updated_at, '19000101');

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total = @rows_work_updated_total + @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#category_work',
                 N'succeeded', @rows_work_updated, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Updated existing category rows inside #category_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #category_work
            (
                  category_id,
                  source_system,
                  category_name,
                  parent_category_id,
                  parent_category_name,
                  category_status,
                  row_hash,
                  created_at,
                  updated_at,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  l.category_id,
                  l.source_system,
                  l.category_name,
                  l.parent_category_id,
                  l.parent_category_name,
                  l.category_status,
                  l.row_hash,
                  l.created_at,
                  l.updated_at,
                  l.source_updated_at,
                  l.stg_row_id
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #category_work AS w
                WHERE w.category_id = l.category_id
                  AND w.source_system = l.source_system
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total = @rows_work_inserted_total + @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#category_work',
                 N'succeeded', @rows_work_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted new category rows into #category_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @current_from = @current_to;
        END;

        SET @step_started_at = SYSDATETIME();

        UPDATE c
           SET c.parent_category_name = p.category_name
        FROM #category_work AS c
        INNER JOIN #category_work AS p
                ON p.category_id = c.parent_category_id
               AND p.source_system = c.source_system
        WHERE c.parent_category_id IS NOT NULL
          AND
          (
                (c.parent_category_name IS NULL AND p.category_name IS NOT NULL)
             OR (c.parent_category_name IS NOT NULL AND p.category_name IS NULL)
             OR (c.parent_category_name <> p.category_name)
          );

        SET @rows_parent_updated = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#category_work',
             N'tempdb', N'#', N'#category_work',
             N'succeeded', @rows_parent_updated, 0,
             @rows_parent_updated, 0,
             @step_started_at, SYSDATETIME(),
             N'Resolved parent_category_name in #category_work from categories already present in the same work set.');

        SET @step_started_at = SYSDATETIME();

        UPDATE c
           SET c.parent_category_name = LEFT(p.name, 200)
        FROM #category_work AS c
        INNER JOIN Stg_FinanceOps_DB.stg_finance_ops.expense_categories AS p
                ON p.id = c.parent_category_id
               AND ISNULL(p.source_system, N'FINANCE_OPS') = c.source_system
        WHERE c.parent_category_id IS NOT NULL
          AND c.parent_category_name IS NULL
          AND p.is_valid = 1
          AND COALESCE(p.source_updated_at, p.updated_at, p.created_at) < @end_time
          AND NOT EXISTS
          (
              SELECT 1
              FROM Stg_FinanceOps_DB.stg_finance_ops.expense_categories AS p2
              WHERE p2.is_valid = 1
                AND p2.id = p.id
                AND ISNULL(p2.source_system, N'FINANCE_OPS') = ISNULL(p.source_system, N'FINANCE_OPS')
                AND COALESCE(p2.source_updated_at, p2.updated_at, p2.created_at) < @end_time
                AND
                (
                       COALESCE(p2.source_updated_at, p2.updated_at, p2.created_at)
                     > COALESCE(p.source_updated_at, p.updated_at, p.created_at)
                    OR
                    (
                           COALESCE(p2.source_updated_at, p2.updated_at, p2.created_at)
                         = COALESCE(p.source_updated_at, p.updated_at, p.created_at)
                       AND p2.stg_row_id > p.stg_row_id
                    )
                )
          );

        SET @rows_parent_staging_updated = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expense_categories',
             N'tempdb', N'#', N'#category_work',
             N'succeeded', @rows_parent_staging_updated, 0,
             @rows_parent_staging_updated, 0,
             @step_started_at, SYSDATETIME(),
             N'Resolved remaining parent_category_name values in #category_work from latest available staging parent rows before @end_time.');

        SET @step_started_at = SYSDATETIME();

        BEGIN TRANSACTION;

            TRUNCATE TABLE dw.dim_category;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_category',
                 N'Charity_DW_DB', N'dw', N'dim_category',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Truncated dw.dim_category for first load.');

            SET @step_started_at = SYSDATETIME();

            DBCC CHECKIDENT ('dw.dim_category', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_category',
                 N'Charity_DW_DB', N'dw', N'dim_category',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Reset dw.dim_category identity seed to 0 for first load.');

            SET @step_started_at = SYSDATETIME();

            SET IDENTITY_INSERT dw.dim_category ON;

            INSERT INTO dw.dim_category
            (
                  category_key,
                  category_id,
                  category_name,
                  parent_category_id,
                  parent_category_name,
                  category_status,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at
            )
            VALUES
            (
                  -1,
                  -1,
                  N'Unknown',
                  NULL,
                  NULL,
                  N'unknown',
                  N'FINANCE_OPS',
                  NULL,
                  SYSDATETIME(),
                  NULL
            );

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_category OFF;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'constant', N'unknown_row', N'dim_category',
                 N'Charity_DW_DB', N'dw', N'dim_category',
                 N'succeeded', 1, @rows_unknown_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Inserted unknown row into dw.dim_category with category_key = -1.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.dim_category
            (
                  category_id,
                  category_name,
                  parent_category_id,
                  parent_category_name,
                  category_status,
                  source_system,
                  row_hash,
                  created_at,
                  updated_at
            )
            SELECT
                  w.category_id,
                  w.category_name,
                  w.parent_category_id,
                  w.parent_category_name,
                  w.category_status,
                  w.source_system,
                  w.row_hash,
                  w.created_at,
                  w.updated_at
            FROM #category_work AS w;

            SET @rows_dim_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#category_work',
                 N'Charity_DW_DB', N'dw', N'dim_category',
                 N'succeeded', @rows_dim_inserted, @rows_dim_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Inserted first-load category rows into dw.dim_category.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_unknown_inserted + @rows_dim_inserted,
               rows_rejected = 0,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(
                                  N'First load succeeded for dw.dim_category. ',
                                  N'SCD Type 1. ',
                                  N'Rows read from staging: ', @rows_read_total,
                                  N'; work inserted: ', @rows_work_inserted_total,
                                  N'; work updated: ', @rows_work_updated_total,
                                  N'; parent names resolved from work set: ', @rows_parent_updated,
                                  N'; parent names resolved from staging: ', @rows_parent_staging_updated,
                                  N'; unknown inserted: ', @rows_unknown_inserted,
                                  N'; dim inserted: ', @rows_dim_inserted,
                                  N'; period: [', CONVERT(NVARCHAR(30), @start_time, 126),
                                  N', ', CONVERT(NVARCHAR(30), @end_time, 126), N').'
                               )
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        BEGIN TRY
            SET IDENTITY_INSERT dw.dim_category OFF;
        END TRY
        BEGIN CATCH
        END CATCH;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   rows_read     = @rows_read_total,
                   rows_inserted  = ISNULL(@rows_unknown_inserted, 0) + ISNULL(@rows_dim_inserted, 0),
                   rows_rejected = 0,
                   ended_at      = SYSDATETIME(),
                   message       = @error_message
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO

/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 17_create_dw_dim_donation_type_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.dim_donation_type from
   distinct donation_type values in Stg_FinanceOps_DB.stg_finance_ops.donations.

 Procedures:
   1. etl_admin.usp_first_load_dw_dim_donation_type
      - First/full load for a selected period.
      - Builds a temp working set day by day.
      - Truncates dw.dim_donation_type.
      - Re-inserts the unknown row with donation_type_key = -1.
      - Resets the identity seed so the next real key starts from 1.
      - Inserts all donation types found in the period.

   2. etl_admin.usp_load_dw_dim_donation_type_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day.
      - Ensures unknown row exists.
      - Resets identity seed to MAX(donation_type_key), so the next inserted row
        uses MAX(donation_type_key) + 1.
      - Updates changed Type 1 attributes.
      - Inserts new donation types.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch

 SCD Decision:
   - dw.dim_donation_type has no effective_from, effective_to, is_current,
     row_hash, or version columns.
   - Therefore this dimension is loaded as SCD Type 1.
===============================================================================
*/

/*=============================================================================
  Procedure 1: First / Full Period Load for dw.dim_donation_type
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_donation_type
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT,
          @rows_work_inserted INT,
          @rows_work_updated INT,
          @rows_deleted INT,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52301, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52302, '@start_time must be earlier than @end_time.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'Charity_DW_DB', N'dw', N'dim_donation_type',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_donation_type. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              code              NVARCHAR(50)  NOT NULL PRIMARY KEY,
              title             NVARCHAR(100) NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #donation_type_work
        (
              code              NVARCHAR(50)  NOT NULL PRIMARY KEY,
              title             NVARCHAR(100) NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#donation_type_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #donation_type_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.donation_type)))) AS code,
                  CONVERT(NVARCHAR(100), MIN(LTRIM(RTRIM(s.donation_type)))) AS title,
                  CONVERT(NVARCHAR(100), MIN(s.source_system)) AS source_system,
                  MIN(COALESCE(s.created_at, s.extracted_at)) AS created_at,
                  MAX(COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at)) AS updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS s
            WHERE s.is_valid = 1
              AND s.donation_type IS NOT NULL
              AND LTRIM(RTRIM(s.donation_type)) <> N''
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
              AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to
            GROUP BY LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.donation_type))));

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Loaded distinct donation types into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.title         = l.title,
                   w.source_system = l.source_system,
                   w.created_at    = CASE
                                         WHEN w.created_at IS NULL THEN l.created_at
                                         WHEN l.created_at IS NULL THEN w.created_at
                                         WHEN l.created_at < w.created_at THEN l.created_at
                                         ELSE w.created_at
                                     END,
                   w.updated_at    = CASE
                                         WHEN l.updated_at IS NULL THEN w.updated_at
                                         WHEN w.updated_at IS NULL THEN l.updated_at
                                         WHEN l.updated_at >= w.updated_at THEN l.updated_at
                                         ELSE w.updated_at
                                     END
            FROM #donation_type_work AS w
            INNER JOIN #loop_src AS l
                ON l.code = w.code
            WHERE ISNULL(w.title, N'') <> ISNULL(l.title, N'')
               OR ISNULL(w.source_system, N'') <> ISNULL(l.source_system, N'')
               OR ISNULL(w.updated_at, CONVERT(DATETIME2(0), '19000101')) < ISNULL(l.updated_at, CONVERT(DATETIME2(0), '19000101'));

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#donation_type_work',
                 N'succeeded', @rows_work_updated, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Updated existing rows in #donation_type_work from #loop_src.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #donation_type_work
            (
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  l.code,
                  l.title,
                  l.source_system,
                  l.created_at,
                  l.updated_at
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #donation_type_work AS w
                WHERE w.code = l.code
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#donation_type_work',
                 N'succeeded', @rows_work_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted new rows into #donation_type_work from #loop_src.');

            SET @current_from = @current_to;
        END;

        BEGIN TRANSACTION;

            SET @step_started_at = SYSDATETIME();

            TRUNCATE TABLE dw.dim_donation_type;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donation_type',
                 N'Charity_DW_DB', N'dw', N'dim_donation_type',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Truncated dw.dim_donation_type for first load.');

            SET @step_started_at = SYSDATETIME();

            SET IDENTITY_INSERT dw.dim_donation_type ON;

            INSERT INTO dw.dim_donation_type
            (
                  donation_type_key,
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            VALUES
            (
                  -1,
                  N'unknown',
                  N'Unknown',
                  N'FINANCE_OPS',
                  SYSDATETIME(),
                  NULL
            );

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_donation_type OFF;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'system', N'system', N'unknown_row',
                 N'Charity_DW_DB', N'dw', N'dim_donation_type',
                 N'succeeded', 1, @rows_unknown_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted unknown row into dw.dim_donation_type with donation_type_key = -1.');

            SET @step_started_at = SYSDATETIME();

            DBCC CHECKIDENT ('dw.dim_donation_type', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donation_type',
                 N'Charity_DW_DB', N'dw', N'dim_donation_type',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Reset identity seed for dw.dim_donation_type to 0 after unknown row.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.dim_donation_type
            (
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  w.code,
                  w.title,
                  ISNULL(w.source_system, N'FINANCE_OPS'),
                  ISNULL(w.created_at, SYSDATETIME()),
                  w.updated_at
            FROM #donation_type_work AS w
            WHERE w.code <> N'unknown';

            SET @rows_dim_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#donation_type_work',
                 N'Charity_DW_DB', N'dw', N'dim_donation_type',
                 N'succeeded', @rows_dim_inserted, @rows_dim_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted first-load rows into dw.dim_donation_type.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_unknown_inserted + @rows_dim_inserted,
               rows_rejected = 0,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(N'Finished first load for dw.dim_donation_type. ',
                                      N'Distinct loop rows read: ', @rows_read_total,
                                      N'. Temp inserted: ', @rows_work_inserted_total,
                                      N'. Temp updated: ', @rows_work_updated_total,
                                      N'. Unknown rows inserted: ', @rows_unknown_inserted,
                                      N'. Dimension rows inserted: ', @rows_dim_inserted, N'.')
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   ended_at      = SYSDATETIME(),
                   message       = CONCAT(N'First load failed for dw.dim_donation_type. Error: ', @error_message)
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 18_create_dw_dim_status_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.dim_status from distinct
   status values in Finance Operations staging tables.

 Sources:
   - Stg_FinanceOps_DB.stg_finance_ops.campaigns.status  -> status_type = campaign
   - Stg_FinanceOps_DB.stg_finance_ops.donations.status  -> status_type = donation
   - Stg_FinanceOps_DB.stg_finance_ops.expenses.status   -> status_type = expense
   - Stg_FinanceOps_DB.stg_finance_ops.payments.status   -> status_type = payment

 Procedures:
   1. etl_admin.usp_first_load_dw_dim_status
      - First/full load for a selected period.
      - Builds a temp working set day by day.
      - Truncates dw.dim_status.
      - Re-inserts the unknown row with status_key = -1.
      - Resets the identity seed so the next real key starts from 1.
      - Inserts all status values found in the period.

   2. etl_admin.usp_load_dw_dim_status_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day.
      - Ensures the unknown row exists.
      - Resets the identity seed to MAX(status_key).
      - Updates changed Type 1 attributes.
      - Inserts new status values.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - Natural key is (status_type, code), because the same code can exist in
     different source business processes.
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch

 SCD Decision:
   - dw.dim_status has no effective_from, effective_to, is_current, row_hash,
     or version columns.
   - Therefore this dimension is loaded as SCD Type 1.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Charity_DW_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END;
GO

/*=============================================================================
  Procedure 1: First / Full Period Load for dw.dim_status
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_status
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT,
          @rows_work_inserted INT,
          @rows_work_updated INT,
          @rows_deleted INT,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52401, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52402, '@start_time must be earlier than @end_time.', 1;
    END;

    IF OBJECT_ID(N'dw.dim_status', N'U') IS NULL
    BEGIN
        THROW 52403, 'Missing target table Charity_DW_DB.dw.dim_status.', 1;
    END;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
       OR OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
    BEGIN
        THROW 52404, 'Missing ETL admin tables in Charity_DW_DB.etl_admin.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_status_sources',
             N'Charity_DW_DB', N'dw', N'dim_status',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_status. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              status_type       NVARCHAR(50)  NOT NULL,
              code              NVARCHAR(50)  NOT NULL,
              title             NVARCHAR(100) NULL,
              category          NVARCHAR(50)  NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL,
              PRIMARY KEY CLUSTERED (status_type, code)
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_status_sources',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #status_work
        (
              status_type       NVARCHAR(50)  NOT NULL,
              code              NVARCHAR(50)  NOT NULL,
              title             NVARCHAR(100) NULL,
              category          NVARCHAR(50)  NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL,
              PRIMARY KEY CLUSTERED (status_type, code)
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_status_sources',
             N'tempdb', N'#', N'#status_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #status_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  status_type,
                  code,
                  title,
                  category,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  src.status_type,
                  src.code,
                  CONVERT(NVARCHAR(100), MIN(src.title)) AS title,
                  src.category,
                  CONVERT(NVARCHAR(100), MIN(src.source_system)) AS source_system,
                  MIN(src.created_at) AS created_at,
                  MAX(src.updated_at) AS updated_at
            FROM
            (
                SELECT
                      CONVERT(NVARCHAR(50), N'campaign') AS status_type,
                      LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) AS code,
                      CONVERT(NVARCHAR(100), LTRIM(RTRIM(s.status))) AS title,
                      CONVERT(NVARCHAR(50), N'master') AS category,
                      s.source_system,
                      COALESCE(s.created_at, s.extracted_at) AS created_at,
                      COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                FROM Stg_FinanceOps_DB.stg_finance_ops.campaigns AS s
                WHERE s.is_valid = 1
                  AND s.status IS NOT NULL
                  AND LTRIM(RTRIM(s.status)) <> N''
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to

                UNION ALL

                SELECT
                      CONVERT(NVARCHAR(50), N'donation') AS status_type,
                      LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) AS code,
                      CONVERT(NVARCHAR(100), LTRIM(RTRIM(s.status))) AS title,
                      CONVERT(NVARCHAR(50), N'transaction') AS category,
                      s.source_system,
                      COALESCE(s.created_at, s.extracted_at) AS created_at,
                      COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS s
                WHERE s.is_valid = 1
                  AND s.status IS NOT NULL
                  AND LTRIM(RTRIM(s.status)) <> N''
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to

                UNION ALL

                SELECT
                      CONVERT(NVARCHAR(50), N'expense') AS status_type,
                      LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) AS code,
                      CONVERT(NVARCHAR(100), LTRIM(RTRIM(s.status))) AS title,
                      CONVERT(NVARCHAR(50), N'transaction') AS category,
                      s.source_system,
                      COALESCE(s.created_at, s.extracted_at) AS created_at,
                      COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                FROM Stg_FinanceOps_DB.stg_finance_ops.expenses AS s
                WHERE s.is_valid = 1
                  AND s.status IS NOT NULL
                  AND LTRIM(RTRIM(s.status)) <> N''
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to

                UNION ALL

                SELECT
                      CONVERT(NVARCHAR(50), N'payment') AS status_type,
                      LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) AS code,
                      CONVERT(NVARCHAR(100), LTRIM(RTRIM(s.status))) AS title,
                      CONVERT(NVARCHAR(50), N'transaction') AS category,
                      s.source_system,
                      COALESCE(s.created_at, s.extracted_at) AS created_at,
                      COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                FROM Stg_FinanceOps_DB.stg_finance_ops.payments AS s
                WHERE s.is_valid = 1
                  AND s.status IS NOT NULL
                  AND LTRIM(RTRIM(s.status)) <> N''
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                  AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to
            ) AS src
            GROUP BY src.status_type, src.code, src.category;

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_status_sources',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Loaded distinct status values into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.title         = l.title,
                   w.category      = l.category,
                   w.source_system = l.source_system,
                   w.created_at    = CASE
                                         WHEN w.created_at IS NULL THEN l.created_at
                                         WHEN l.created_at IS NULL THEN w.created_at
                                         WHEN l.created_at < w.created_at THEN l.created_at
                                         ELSE w.created_at
                                     END,
                   w.updated_at    = CASE
                                         WHEN l.updated_at IS NULL THEN w.updated_at
                                         WHEN w.updated_at IS NULL THEN l.updated_at
                                         WHEN l.updated_at >= w.updated_at THEN l.updated_at
                                         ELSE w.updated_at
                                     END
            FROM #status_work AS w
            INNER JOIN #loop_src AS l
                ON l.status_type = w.status_type
               AND l.code = w.code
            WHERE ISNULL(w.title, N'') <> ISNULL(l.title, N'')
               OR ISNULL(w.category, N'') <> ISNULL(l.category, N'')
               OR ISNULL(w.source_system, N'') <> ISNULL(l.source_system, N'')
               OR ISNULL(w.updated_at, CONVERT(DATETIME2(0), '19000101')) < ISNULL(l.updated_at, CONVERT(DATETIME2(0), '19000101'));

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#status_work',
                 N'succeeded', @rows_loop_inserted, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Updated existing rows in #status_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #status_work
            (
                  status_type,
                  code,
                  title,
                  category,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  l.status_type,
                  l.code,
                  l.title,
                  l.category,
                  l.source_system,
                  l.created_at,
                  l.updated_at
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #status_work AS w
                WHERE w.status_type = l.status_type
                  AND w.code = l.code
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#status_work',
                 N'succeeded', @rows_loop_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted new rows into #status_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @current_from = @current_to;
        END;

        BEGIN TRANSACTION;

            SET @step_started_at = SYSDATETIME();

            TRUNCATE TABLE dw.dim_status;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_status',
                 N'Charity_DW_DB', N'dw', N'dim_status',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Truncated dw.dim_status for first load.');

            SET @step_started_at = SYSDATETIME();

            SET IDENTITY_INSERT dw.dim_status ON;

            INSERT INTO dw.dim_status
            (
                  status_key,
                  status_type,
                  code,
                  title,
                  category,
                  source_system,
                  created_at,
                  updated_at
            )
            VALUES
            (
                  -1,
                  N'unknown',
                  N'unknown',
                  N'Unknown',
                  N'unknown',
                  N'FINANCE_OPS',
                  SYSDATETIME(),
                  NULL
            );

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_status OFF;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'system', N'system', N'unknown_row',
                 N'Charity_DW_DB', N'dw', N'dim_status',
                 N'succeeded', 1, @rows_unknown_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted unknown row into dw.dim_status with status_key = -1.');

            SET @step_started_at = SYSDATETIME();

            DBCC CHECKIDENT ('dw.dim_status', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_status',
                 N'Charity_DW_DB', N'dw', N'dim_status',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Reset identity seed of dw.dim_status to 0 after first-load unknown row.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.dim_status
            (
                  status_type,
                  code,
                  title,
                  category,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  w.status_type,
                  w.code,
                  w.title,
                  w.category,
                  ISNULL(w.source_system, N'FINANCE_OPS'),
                  ISNULL(w.created_at, SYSDATETIME()),
                  w.updated_at
            FROM #status_work AS w;

            SET @rows_dim_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#status_work',
                 N'Charity_DW_DB', N'dw', N'dim_status',
                 N'succeeded', @rows_dim_inserted, @rows_dim_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted first-load status rows into dw.dim_status.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status = N'succeeded',
               rows_read = @rows_read_total,
               rows_inserted = @rows_dim_inserted + @rows_unknown_inserted,
               rows_rejected = 0,
               ended_at = SYSDATETIME(),
               message = CONCAT(N'First load finished for dw.dim_status. Work inserted=',
                                @rows_work_inserted_total, N', work updated=',
                                @rows_work_updated_total, N', dimension inserted=',
                                @rows_dim_inserted, N'.')
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status = N'failed',
                   ended_at = SYSDATETIME(),
                   message = CONCAT(N'First load failed for dw.dim_status. Error: ', @error_message)
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO

/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 19_create_dw_dim_currency_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.dim_currency from all
   Finance staging sources that can introduce a currency code.

 Procedures:
   1. etl_admin.usp_first_load_dw_dim_currency
      - First/full load for a selected period.
      - Builds a temp working set day by day.
      - Truncates dw.dim_currency.
      - Re-inserts the unknown row with currency_key = -1.
      - Resets the identity seed so the next real key starts from 1.
      - Inserts all currency codes found in the selected period.

   2. etl_admin.usp_load_dw_dim_currency_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day.
      - Ensures unknown row exists.
      - Resets identity seed to MAX(currency_key), so the next inserted row
        uses MAX(currency_key) + 1.
      - Updates changed Type 1 attributes.
      - Inserts new currency codes.

 Sources:
   - Stg_FinanceOps_DB.stg_finance_ops.donations.currency
   - Stg_FinanceOps_DB.stg_finance_ops.expenses.currency
   - Stg_FinanceOps_DB.stg_finance_ops.payments.currency
   - Stg_FinanceOps_DB.stg_finance_ops.currency_rates.from_currency
   - Stg_FinanceOps_DB.stg_finance_ops.currency_rates.to_currency

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch

 SCD Decision:
   - dw.dim_currency has no effective_from, effective_to, is_current,
     row_hash, or version columns.
   - Therefore this dimension is loaded as SCD Type 1.
===============================================================================
*/

SET NOCOUNT ON;
GO

USE Charity_DW_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'etl_admin')
BEGIN
    EXEC(N'CREATE SCHEMA etl_admin');
END;
GO

/*=============================================================================
  Procedure 1: First / Full Period Load for dw.dim_currency
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_currency
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT,
          @rows_work_inserted INT,
          @rows_work_updated INT,
          @rows_deleted INT,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52501, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52502, '@start_time must be earlier than @end_time.', 1;
    END;

    IF OBJECT_ID(N'dw.dim_currency', N'U') IS NULL
    BEGIN
        THROW 52503, 'Missing table Charity_DW_DB.dw.dim_currency.', 1;
    END;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
       OR OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
    BEGIN
        THROW 52504, 'Missing ETL admin tables in Charity_DW_DB.etl_admin.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_currency_sources',
             N'Charity_DW_DB', N'dw', N'dim_currency',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_currency. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              code              NVARCHAR(10)  NOT NULL PRIMARY KEY,
              name              NVARCHAR(100) NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_currency_sources',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #currency_work
        (
              code              NVARCHAR(10)  NOT NULL PRIMARY KEY,
              name              NVARCHAR(100) NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_currency_sources',
             N'tempdb', N'#', N'#currency_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #currency_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  code,
                  name,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  src.code,
                  CASE src.code
                       WHEN N'IRR' THEN N'Iranian Rial'
                       WHEN N'USD' THEN N'US Dollar'
                       WHEN N'EUR' THEN N'Euro'
                       WHEN N'GBP' THEN N'British Pound'
                       WHEN N'AED' THEN N'UAE Dirham'
                       WHEN N'TRY' THEN N'Turkish Lira'
                       WHEN N'CAD' THEN N'Canadian Dollar'
                       WHEN N'AUD' THEN N'Australian Dollar'
                       WHEN N'CHF' THEN N'Swiss Franc'
                       WHEN N'CNY' THEN N'Chinese Yuan'
                       WHEN N'JPY' THEN N'Japanese Yen'
                       WHEN N'RUB' THEN N'Russian Ruble'
                       ELSE src.code
                  END AS name,
                  MIN(src.source_system) AS source_system,
                  MIN(src.created_at) AS created_at,
                  MAX(src.updated_at) AS updated_at
            FROM
            (
                  SELECT
                        UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(s.currency)))) AS code,
                        s.source_system,
                        COALESCE(s.created_at, s.extracted_at) AS created_at,
                        COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                  FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS s
                  WHERE s.is_valid = 1
                    AND s.currency IS NOT NULL
                    AND LTRIM(RTRIM(s.currency)) <> N''
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to

                  UNION ALL

                  SELECT
                        UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(s.currency)))) AS code,
                        s.source_system,
                        COALESCE(s.created_at, s.extracted_at) AS created_at,
                        COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                  FROM Stg_FinanceOps_DB.stg_finance_ops.expenses AS s
                  WHERE s.is_valid = 1
                    AND s.currency IS NOT NULL
                    AND LTRIM(RTRIM(s.currency)) <> N''
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to

                  UNION ALL

                  SELECT
                        UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(s.currency)))) AS code,
                        s.source_system,
                        COALESCE(s.created_at, s.extracted_at) AS created_at,
                        COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS updated_at
                  FROM Stg_FinanceOps_DB.stg_finance_ops.payments AS s
                  WHERE s.is_valid = 1
                    AND s.currency IS NOT NULL
                    AND LTRIM(RTRIM(s.currency)) <> N''
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to

                  UNION ALL

                  SELECT
                        UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(s.from_currency)))) AS code,
                        s.source_system,
                        s.extracted_at AS created_at,
                        COALESCE(s.source_updated_at, s.extracted_at) AS updated_at
                  FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates AS s
                  WHERE s.is_valid = 1
                    AND s.from_currency IS NOT NULL
                    AND LTRIM(RTRIM(s.from_currency)) <> N''
                    AND COALESCE(s.source_updated_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.extracted_at) <  @current_to

                  UNION ALL

                  SELECT
                        UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(s.to_currency)))) AS code,
                        s.source_system,
                        s.extracted_at AS created_at,
                        COALESCE(s.source_updated_at, s.extracted_at) AS updated_at
                  FROM Stg_FinanceOps_DB.stg_finance_ops.currency_rates AS s
                  WHERE s.is_valid = 1
                    AND s.to_currency IS NOT NULL
                    AND LTRIM(RTRIM(s.to_currency)) <> N''
                    AND COALESCE(s.source_updated_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.extracted_at) <  @current_to
            ) AS src
            WHERE src.code IS NOT NULL
              AND src.code <> N''
            GROUP BY src.code;

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'multiple_currency_sources',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted distinct currency rows into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.name = l.name,
                   w.source_system = ISNULL(w.source_system, l.source_system),
                   w.created_at = CASE
                                      WHEN l.created_at IS NOT NULL
                                           AND (w.created_at IS NULL OR l.created_at < w.created_at)
                                      THEN l.created_at
                                      ELSE w.created_at
                                  END,
                   w.updated_at = CASE
                                      WHEN l.updated_at IS NOT NULL
                                           AND (w.updated_at IS NULL OR l.updated_at > w.updated_at)
                                      THEN l.updated_at
                                      ELSE w.updated_at
                                  END
            FROM #currency_work AS w
            INNER JOIN #loop_src AS l
                ON l.code = w.code;

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#currency_work',
                 N'succeeded', @rows_work_updated, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Updated existing rows in #currency_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #currency_work
            (
                  code,
                  name,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  l.code,
                  l.name,
                  ISNULL(l.source_system, N'FINANCE_OPS'),
                  l.created_at,
                  l.updated_at
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                  SELECT 1
                  FROM #currency_work AS w
                  WHERE w.code = l.code
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#currency_work',
                 N'succeeded', @rows_work_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted new rows into #currency_work for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @current_from = @current_to;
        END;

        BEGIN TRANSACTION;
            SET @step_started_at = SYSDATETIME();

            TRUNCATE TABLE dw.dim_currency;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_currency',
                 N'Charity_DW_DB', N'dw', N'dim_currency',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Truncated dw.dim_currency for first load.');

            SET @step_started_at = SYSDATETIME();

            SET IDENTITY_INSERT dw.dim_currency ON;

            INSERT INTO dw.dim_currency
                (currency_key, code, name, source_system, created_at, updated_at)
            VALUES
                (-1, N'UNK', N'Unknown', N'FINANCE_OPS', SYSDATETIME(), NULL);

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_currency OFF;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#currency_work',
                 N'Charity_DW_DB', N'dw', N'dim_currency',
                 N'succeeded', 1, @rows_unknown_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted unknown row into dw.dim_currency with currency_key = -1.');

            SET @step_started_at = SYSDATETIME();

            DBCC CHECKIDENT ('dw.dim_currency', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_currency',
                 N'Charity_DW_DB', N'dw', N'dim_currency',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Reset dw.dim_currency identity seed to 0 after inserting unknown row.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.dim_currency
            (
                  code,
                  name,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  w.code,
                  w.name,
                  ISNULL(w.source_system, N'FINANCE_OPS'),
                  ISNULL(w.created_at, SYSDATETIME()),
                  w.updated_at
            FROM #currency_work AS w
            WHERE w.code <> N'UNK';

            SET @rows_dim_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#currency_work',
                 N'Charity_DW_DB', N'dw', N'dim_currency',
                 N'succeeded', @rows_dim_inserted, @rows_dim_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted first-load rows into dw.dim_currency.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_unknown_inserted + @rows_dim_inserted,
               rows_rejected = 0,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(N'Finished first load for dw.dim_currency. ',
                                      N'Distinct loop rows read: ', @rows_read_total,
                                      N'. Temp inserted: ', @rows_work_inserted_total,
                                      N'. Temp updated: ', @rows_work_updated_total,
                                      N'. Unknown rows inserted: ', @rows_unknown_inserted,
                                      N'. Dimension rows inserted: ', @rows_dim_inserted, N'.')
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   ended_at      = SYSDATETIME(),
                   message       = CONCAT(N'First load failed for dw.dim_currency. Error: ', @error_message)
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO

/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 20_create_dw_dim_allocation_type_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.dim_allocation_type from
   distinct source_type values in Stg_FinanceOps_DB.stg_finance_ops.budget_allocations.

 Procedures:
   1. etl_admin.usp_first_load_dw_dim_allocation_type
      - First/full load for a selected period.
      - Builds a temp working set day by day.
      - Truncates dw.dim_allocation_type.
      - Re-inserts the unknown row with allocation_type_key = -1.
      - Resets the identity seed so the next real key starts from 1.
      - Inserts all allocation types found in the period.

   2. etl_admin.usp_load_dw_dim_allocation_type_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day.
      - Ensures unknown row exists.
      - Resets identity seed to MAX(allocation_type_key), so the next inserted row
        uses MAX(allocation_type_key) + 1.
      - Updates changed Type 1 attributes.
      - Inserts new allocation types.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch

 SCD Decision:
   - dw.dim_allocation_type has no effective_from, effective_to, is_current,
     row_hash, or version columns.
   - Therefore this dimension is loaded as SCD Type 1.
===============================================================================
*/

USE Charity_DW_DB;
GO

GO

/*=============================================================================
  Procedure 1: First / Full Period Load for dw.dim_allocation_type
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_dim_allocation_type
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_loop_inserted INT,
          @rows_work_inserted INT,
          @rows_work_updated INT,
          @rows_deleted INT,
          @rows_unknown_inserted INT = 0,
          @rows_dim_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52301, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52302, '@start_time must be earlier than @end_time.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_DIMENSION', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'Charity_DW_DB', N'dw', N'dim_allocation_type',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.dim_allocation_type. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #loop_src
        (
              code              NVARCHAR(50)  NOT NULL PRIMARY KEY,
              title             NVARCHAR(100) NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #allocation_type_work
        (
              code              NVARCHAR(50)  NOT NULL PRIMARY KEY,
              title             NVARCHAR(100) NULL,
              source_system     NVARCHAR(100) NULL,
              created_at        DATETIME2(0)  NULL,
              updated_at        DATETIME2(0)  NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'tempdb', N'#', N'#allocation_type_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #allocation_type_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.source_type)))) AS code,
                  CONVERT(NVARCHAR(100), MIN(LTRIM(RTRIM(s.source_type)))) AS title,
                  CONVERT(NVARCHAR(100), MIN(s.source_system)) AS source_system,
                  MIN(COALESCE(s.created_at, s.extracted_at)) AS created_at,
                  MAX(COALESCE(s.source_updated_at, s.created_at, s.extracted_at)) AS updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations AS s
            WHERE s.is_valid = 1
              AND s.source_type IS NOT NULL
              AND LTRIM(RTRIM(s.source_type)) <> N''
              AND COALESCE(s.source_updated_at, s.created_at, s.extracted_at) >= @current_from
              AND COALESCE(s.source_updated_at, s.created_at, s.extracted_at) <  @current_to
            GROUP BY LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.source_type))));

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Loaded distinct allocation types into #loop_src for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            UPDATE w
               SET w.title         = l.title,
                   w.source_system = l.source_system,
                   w.created_at    = CASE
                                         WHEN w.created_at IS NULL THEN l.created_at
                                         WHEN l.created_at IS NULL THEN w.created_at
                                         WHEN l.created_at < w.created_at THEN l.created_at
                                         ELSE w.created_at
                                     END,
                   w.updated_at    = CASE
                                         WHEN l.updated_at IS NULL THEN w.updated_at
                                         WHEN w.updated_at IS NULL THEN l.updated_at
                                         WHEN l.updated_at >= w.updated_at THEN l.updated_at
                                         ELSE w.updated_at
                                     END
            FROM #allocation_type_work AS w
            INNER JOIN #loop_src AS l
                ON l.code = w.code
            WHERE ISNULL(w.title, N'') <> ISNULL(l.title, N'')
               OR ISNULL(w.source_system, N'') <> ISNULL(l.source_system, N'')
               OR ISNULL(w.updated_at, CONVERT(DATETIME2(0), '19000101')) < ISNULL(l.updated_at, CONVERT(DATETIME2(0), '19000101'));

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#allocation_type_work',
                 N'succeeded', @rows_work_updated, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Updated existing rows in #allocation_type_work from #loop_src.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #allocation_type_work
            (
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  l.code,
                  l.title,
                  l.source_system,
                  l.created_at,
                  l.updated_at
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #allocation_type_work AS w
                WHERE w.code = l.code
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#allocation_type_work',
                 N'succeeded', @rows_work_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted new rows into #allocation_type_work from #loop_src.');

            SET @current_from = @current_to;
        END;

        BEGIN TRANSACTION;

            SET @step_started_at = SYSDATETIME();

            TRUNCATE TABLE dw.dim_allocation_type;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_allocation_type',
                 N'Charity_DW_DB', N'dw', N'dim_allocation_type',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Truncated dw.dim_allocation_type for first load.');

            SET @step_started_at = SYSDATETIME();

            SET IDENTITY_INSERT dw.dim_allocation_type ON;

            INSERT INTO dw.dim_allocation_type
            (
                  allocation_type_key,
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            VALUES
            (
                  -1,
                  N'unknown',
                  N'Unknown',
                  N'FINANCE_OPS',
                  SYSDATETIME(),
                  NULL
            );

            SET @rows_unknown_inserted = @@ROWCOUNT;

            SET IDENTITY_INSERT dw.dim_allocation_type OFF;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'system', N'system', N'unknown_row',
                 N'Charity_DW_DB', N'dw', N'dim_allocation_type',
                 N'succeeded', 1, @rows_unknown_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted unknown row into dw.dim_allocation_type with allocation_type_key = -1.');

            SET @step_started_at = SYSDATETIME();

            DBCC CHECKIDENT ('dw.dim_allocation_type', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_allocation_type',
                 N'Charity_DW_DB', N'dw', N'dim_allocation_type',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Reset identity seed for dw.dim_allocation_type to 0 after unknown row.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.dim_allocation_type
            (
                  code,
                  title,
                  source_system,
                  created_at,
                  updated_at
            )
            SELECT
                  w.code,
                  w.title,
                  ISNULL(w.source_system, N'FINANCE_OPS'),
                  ISNULL(w.created_at, SYSDATETIME()),
                  w.updated_at
            FROM #allocation_type_work AS w
            WHERE w.code <> N'unknown';

            SET @rows_dim_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#allocation_type_work',
                 N'Charity_DW_DB', N'dw', N'dim_allocation_type',
                 N'succeeded', @rows_dim_inserted, @rows_dim_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted first-load rows into dw.dim_allocation_type.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_unknown_inserted + @rows_dim_inserted,
               rows_rejected = 0,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(N'Finished first load for dw.dim_allocation_type. ',
                                      N'Distinct loop rows read: ', @rows_read_total,
                                      N'. Temp inserted: ', @rows_work_inserted_total,
                                      N'. Temp updated: ', @rows_work_updated_total,
                                      N'. Unknown rows inserted: ', @rows_unknown_inserted,
                                      N'. Dimension rows inserted: ', @rows_dim_inserted, N'.')
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   ended_at      = SYSDATETIME(),
                   message       = CONCAT(N'First load failed for dw.dim_allocation_type. Error: ', @error_message)
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 21_create_dw_fact_donation_transaction_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.fact_donation_transaction
   from Stg_FinanceOps_DB.stg_finance_ops.donations.

 Procedures:
   1. etl_admin.usp_first_load_dw_fact_donation_transaction
      - First/full load for a selected period.
      - Builds a temp working set day by day.
      - Truncates dw.fact_donation_transaction.
      - Resets the identity seed so the first real fact key starts from 1.
      - Inserts one row per donation transaction.

   2. etl_admin.usp_load_dw_fact_donation_transaction_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day.
      - Resets identity seed to the current MAX(donation_transaction_key).
      - Deletes affected source donation rows from the fact table.
      - Inserts the latest affected rows again.

 Fact Type Decision:
   - dw.fact_donation_transaction is a TRANSACTION FACT.
   - It has a business event date through donation_date/date_key.
   - It is not an accumulating snapshot, so measures are not recalculated from
     the beginning to the present. Incremental ETL reloads only affected
     donation transaction rows.

 Grain:
   - One row per source donation transaction.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - The ETL window detects affected rows by donation_date OR source change time
     so late status/reference changes can update older donation events.
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Dimension lookup failures use unknown keys = -1.
   - center_key is set to -1 because source donations has no center_id.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch
===============================================================================
*/


/*=============================================================================
  Procedure 1: First / Full Period Load for dw.fact_donation_transaction
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_donation_transaction
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_deleted INT,
          @rows_loop_ids INT,
          @rows_rejected INT,
          @rows_loop_inserted INT,
          @rows_updated INT,
          @rows_inserted INT,
          @rows_work_inserted INT,
          @rows_work_updated INT,
          @rows_fact_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_rejected_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @date_lookup_sql NVARCHAR(MAX),
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52401, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52402, '@start_time must be earlier than @end_time.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_FACT', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'Charity_DW_DB', N'dw', N'fact_donation_transaction',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.fact_donation_transaction. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        /*---------------------------------------------------------------------
          Small dimension lookup temp tables.
          These avoid repeated scans of heap dimensions inside the daily loop.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_date
        (
              date_value DATE NOT NULL PRIMARY KEY,
              date_key   INT  NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_date', N'FullDateAlternateKey') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'TimeKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDateAlternateKey AS DATE), MIN(TimeKey)
                FROM dw.dim_date
                WHERE FullDateAlternateKey IS NOT NULL
                  AND ISNULL(TimeKey, -1) <> -1
                GROUP BY CAST(FullDateAlternateKey AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'full_date') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'date_key') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(full_date AS DATE), MIN(date_key)
                FROM dw.dim_date
                WHERE full_date IS NOT NULL
                  AND ISNULL(date_key, -1) <> -1
                GROUP BY CAST(full_date AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'FullDate') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_date', N'DateKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDate AS DATE), MIN(DateKey)
                FROM dw.dim_date
                WHERE FullDate IS NOT NULL
                  AND ISNULL(DateKey, -1) <> -1
                GROUP BY CAST(FullDate AS DATE);';
        END
        ELSE
        BEGIN
            THROW 52403, 'Cannot resolve dw.dim_date columns. Expected (TimeKey, FullDateAlternateKey), (date_key, full_date), or (DateKey, FullDate).', 1;
        END;

        EXEC sys.sp_executesql @date_lookup_sql;
        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_date',
             N'tempdb', N'#', N'#dim_date',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_date lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_donor
        (
              donor_id  BIGINT NOT NULL PRIMARY KEY,
              donor_key INT    NOT NULL
        );

        INSERT INTO #dim_donor (donor_id, donor_key)
        SELECT CONVERT(BIGINT, donor_id), MIN(donor_key)
        FROM dw.dim_donor
        WHERE donor_id IS NOT NULL
          AND ISNULL(donor_key, -1) <> -1
        GROUP BY CONVERT(BIGINT, donor_id);

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donor',
             N'tempdb', N'#', N'#dim_donor',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_donor lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_campaign
        (
              campaign_id  BIGINT NOT NULL PRIMARY KEY,
              campaign_key INT    NOT NULL
        );

        INSERT INTO #dim_campaign (campaign_id, campaign_key)
        SELECT CONVERT(BIGINT, campaign_id), MIN(campaign_key)
        FROM dw.dim_campaign
        WHERE campaign_id IS NOT NULL
          AND ISNULL(campaign_key, -1) <> -1
        GROUP BY CONVERT(BIGINT, campaign_id);

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_campaign',
             N'tempdb', N'#', N'#dim_campaign',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_campaign lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_donation_type
        (
              code              NVARCHAR(50) NOT NULL PRIMARY KEY,
              donation_type_key INT          NOT NULL
        );

        INSERT INTO #dim_donation_type (code, donation_type_key)
        SELECT LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(code)))), MIN(donation_type_key)
        FROM dw.dim_donation_type
        WHERE code IS NOT NULL
          AND LTRIM(RTRIM(code)) <> N''
          AND ISNULL(donation_type_key, -1) <> -1
        GROUP BY LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(code))));

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donation_type',
             N'tempdb', N'#', N'#dim_donation_type',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_donation_type lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_currency
        (
              code         NVARCHAR(10) NOT NULL PRIMARY KEY,
              currency_key INT          NOT NULL
        );

        INSERT INTO #dim_currency (code, currency_key)
        SELECT UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(code)))), MIN(currency_key)
        FROM dw.dim_currency
        WHERE code IS NOT NULL
          AND LTRIM(RTRIM(code)) <> N''
          AND ISNULL(currency_key, -1) <> -1
        GROUP BY UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(code))));

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_currency',
             N'tempdb', N'#', N'#dim_currency',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_currency lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_status
        (
              status_type NVARCHAR(50) NOT NULL,
              code        NVARCHAR(50) NOT NULL,
              status_key  INT          NOT NULL,
              PRIMARY KEY (status_type, code)
        );

        INSERT INTO #dim_status (status_type, code, status_key)
        SELECT
              LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(status_type)))),
              LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(code)))),
              MIN(status_key)
        FROM dw.dim_status
        WHERE status_type IS NOT NULL
          AND code IS NOT NULL
          AND LTRIM(RTRIM(status_type)) <> N''
          AND LTRIM(RTRIM(code)) <> N''
          AND ISNULL(status_key, -1) <> -1
        GROUP BY
              LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(status_type)))),
              LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(code))));

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_status',
             N'tempdb', N'#', N'#dim_status',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_status lookup table.');

        /*---------------------------------------------------------------------
          Working temp tables.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #loop_ids
        (
              source_donation_id BIGINT NOT NULL PRIMARY KEY,
              stg_row_id         BIGINT NOT NULL,
              source_updated_at  DATETIME2(0) NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#loop_ids',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_ids.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #loop_src
        (
              source_donation_id    BIGINT NOT NULL PRIMARY KEY,
              donation_date         DATE NULL,
              source_donor_id       BIGINT NULL,
              source_campaign_id    BIGINT NULL,
              donation_type_code    NVARCHAR(50) NULL,
              currency_code         NVARCHAR(10) NULL,
              status_code           NVARCHAR(50) NULL,
              amount                DECIMAL(18,2) NULL,
              source_reference_code NVARCHAR(100) NULL,
              source_system         NVARCHAR(100) NULL,
              source_updated_at     DATETIME2(0) NULL,
              stg_row_id            BIGINT NULL,

              date_key              INT NOT NULL DEFAULT (-1),
              donor_key             INT NOT NULL DEFAULT (-1),
              campaign_key          INT NOT NULL DEFAULT (-1),
              center_key            INT NOT NULL DEFAULT (-1),
              donation_type_key     INT NOT NULL DEFAULT (-1),
              currency_key          INT NOT NULL DEFAULT (-1),
              status_key            INT NOT NULL DEFAULT (-1),
              is_confirmed          BIT NULL,
              is_refunded           BIT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #donation_work
        (
              source_donation_id    BIGINT NOT NULL PRIMARY KEY,
              date_key              INT NOT NULL,
              donor_key             INT NOT NULL,
              campaign_key          INT NOT NULL,
              center_key            INT NOT NULL,
              donation_type_key     INT NOT NULL,
              currency_key          INT NOT NULL,
              status_key            INT NOT NULL,
              amount                DECIMAL(18,2) NULL,
              is_confirmed          BIT NULL,
              is_refunded           BIT NULL,
              source_donor_id       BIGINT NULL,
              source_campaign_id    BIGINT NULL,
              source_reference_code NVARCHAR(100) NULL,
              source_system         NVARCHAR(100) NULL,
              source_updated_at     DATETIME2(0) NULL,
              stg_row_id            BIGINT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#donation_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #donation_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();
            DELETE FROM #loop_ids;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_ids',
                 N'tempdb', N'#', N'#loop_ids',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_ids for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            SELECT @rows_rejected = COUNT(1)
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS s
            WHERE
            (
                   (s.donation_date IS NOT NULL
                    AND CONVERT(DATETIME2(0), s.donation_date) >= @current_from
                    AND CONVERT(DATETIME2(0), s.donation_date) <  @current_to)
                OR (COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to)
            )
            AND (ISNULL(s.is_valid, 0) <> 1 OR s.id IS NULL);

            INSERT INTO #loop_ids
            (
                  source_donation_id,
                  stg_row_id,
                  source_updated_at
            )
            SELECT
                  CONVERT(BIGINT, s.id) AS source_donation_id,
                  MAX(s.stg_row_id) AS stg_row_id,
                  MAX(COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at)) AS source_updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS s
            WHERE s.is_valid = 1
              AND s.id IS NOT NULL
              AND
              (
                     (s.donation_date IS NOT NULL
                      AND CONVERT(DATETIME2(0), s.donation_date) >= @current_from
                      AND CONVERT(DATETIME2(0), s.donation_date) <  @current_to)
                  OR (COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) >= @current_from
                      AND COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) <  @current_to)
              )
            GROUP BY CONVERT(BIGINT, s.id);

            SET @rows_loop_ids = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_ids;
            SET @rows_rejected_total += ISNULL(@rows_rejected, 0);

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
                 N'tempdb', N'#', N'#loop_ids',
                 N'succeeded', @rows_loop_ids, @rows_loop_ids,
                 0, @rows_rejected,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Loaded latest affected donation IDs into #loop_ids for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();
            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Cleared #loop_src before loading current period donation rows.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  source_donation_id,
                  donation_date,
                  source_donor_id,
                  source_campaign_id,
                  donation_type_code,
                  currency_code,
                  status_code,
                  amount,
                  source_reference_code,
                  source_system,
                  source_updated_at,
                  stg_row_id,
                  center_key,
                  is_confirmed,
                  is_refunded
            )
            SELECT
                  CONVERT(BIGINT, s.id) AS source_donation_id,
                  s.donation_date,
                  CONVERT(BIGINT, s.donor_id) AS source_donor_id,
                  CONVERT(BIGINT, s.campaign_id) AS source_campaign_id,
                  LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.donation_type)))) AS donation_type_code,
                  UPPER(CONVERT(NVARCHAR(10), LTRIM(RTRIM(s.currency)))) AS currency_code,
                  LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) AS status_code,
                  s.amount,
                  s.reference_code,
                  ISNULL(s.source_system, N'FINANCE_OPS') AS source_system,
                  COALESCE(s.source_updated_at, s.updated_at, s.created_at, s.extracted_at) AS source_updated_at,
                  s.stg_row_id,
                  -1 AS center_key,
                  CASE
                      WHEN LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) IN
                           (N'confirmed', N'complete', N'completed', N'paid', N'success', N'succeeded', N'approved')
                      THEN 1 ELSE 0
                  END AS is_confirmed,
                  CASE
                      WHEN LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.status)))) IN
                           (N'refund', N'refunded', N'chargeback')
                      THEN 1 ELSE 0
                  END AS is_refunded
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS s
            INNER JOIN #loop_ids AS i
                ON i.stg_row_id = s.stg_row_id;

            SET @rows_loop_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted current period donation rows into #loop_src with normalized business codes.');

            SET @step_started_at = SYSDATETIME();
            UPDATE l
               SET l.date_key = ISNULL(d.date_key, -1)
            FROM #loop_src AS l
            LEFT JOIN #dim_date AS d
                ON d.date_value = l.donation_date;
            SET @rows_updated = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#dim_date',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_updated, 0,
                 @rows_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated date_key in #loop_src.');

            SET @step_started_at = SYSDATETIME();
            UPDATE l
               SET l.donor_key = ISNULL(d.donor_key, -1)
            FROM #loop_src AS l
            LEFT JOIN #dim_donor AS d
                ON d.donor_id = l.source_donor_id;
            SET @rows_updated = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#dim_donor',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_updated, 0,
                 @rows_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated donor_key in #loop_src.');

            SET @step_started_at = SYSDATETIME();
            UPDATE l
               SET l.campaign_key = ISNULL(c.campaign_key, -1)
            FROM #loop_src AS l
            LEFT JOIN #dim_campaign AS c
                ON c.campaign_id = l.source_campaign_id;
            SET @rows_updated = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#dim_campaign',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_updated, 0,
                 @rows_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated campaign_key in #loop_src.');

            SET @step_started_at = SYSDATETIME();
            UPDATE l
               SET l.donation_type_key = ISNULL(t.donation_type_key, -1)
            FROM #loop_src AS l
            LEFT JOIN #dim_donation_type AS t
                ON t.code = l.donation_type_code;
            SET @rows_updated = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#dim_donation_type',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_updated, 0,
                 @rows_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated donation_type_key in #loop_src.');

            SET @step_started_at = SYSDATETIME();
            UPDATE l
               SET l.currency_key = ISNULL(c.currency_key, -1)
            FROM #loop_src AS l
            LEFT JOIN #dim_currency AS c
                ON c.code = l.currency_code;
            SET @rows_updated = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#dim_currency',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_updated, 0,
                 @rows_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated currency_key in #loop_src.');

            SET @step_started_at = SYSDATETIME();
            UPDATE l
               SET l.status_key = ISNULL(s.status_key, -1)
            FROM #loop_src AS l
            LEFT JOIN #dim_status AS s
                ON s.status_type = N'donation'
               AND s.code = l.status_code;
            SET @rows_updated = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#dim_status',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_updated, 0,
                 @rows_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated status_key in #loop_src.');

            SET @step_started_at = SYSDATETIME();
            UPDATE w
               SET w.date_key              = l.date_key,
                   w.donor_key             = l.donor_key,
                   w.campaign_key          = l.campaign_key,
                   w.center_key            = l.center_key,
                   w.donation_type_key     = l.donation_type_key,
                   w.currency_key          = l.currency_key,
                   w.status_key            = l.status_key,
                   w.amount                = l.amount,
                   w.is_confirmed          = l.is_confirmed,
                   w.is_refunded           = l.is_refunded,
                   w.source_donor_id       = l.source_donor_id,
                   w.source_campaign_id    = l.source_campaign_id,
                   w.source_reference_code = l.source_reference_code,
                   w.source_system         = l.source_system,
                   w.source_updated_at     = l.source_updated_at,
                   w.stg_row_id            = l.stg_row_id
            FROM #donation_work AS w
            INNER JOIN #loop_src AS l
                ON l.source_donation_id = w.source_donation_id
            WHERE ISNULL(l.source_updated_at, CONVERT(DATETIME2(0), '19000101')) >= ISNULL(w.source_updated_at, CONVERT(DATETIME2(0), '19000101'))
              AND ISNULL(l.stg_row_id, 0) >= ISNULL(w.stg_row_id, 0);

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#donation_work',
                 N'succeeded', @rows_work_updated, 0,
                 @rows_work_updated, 0,
                 @step_started_at, SYSDATETIME(), N'Updated existing donation rows in #donation_work from #loop_src.');

            SET @step_started_at = SYSDATETIME();
            INSERT INTO #donation_work
            (
                  source_donation_id,
                  date_key,
                  donor_key,
                  campaign_key,
                  center_key,
                  donation_type_key,
                  currency_key,
                  status_key,
                  amount,
                  is_confirmed,
                  is_refunded,
                  source_donor_id,
                  source_campaign_id,
                  source_reference_code,
                  source_system,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  l.source_donation_id,
                  l.date_key,
                  l.donor_key,
                  l.campaign_key,
                  l.center_key,
                  l.donation_type_key,
                  l.currency_key,
                  l.status_key,
                  l.amount,
                  l.is_confirmed,
                  l.is_refunded,
                  l.source_donor_id,
                  l.source_campaign_id,
                  l.source_reference_code,
                  l.source_system,
                  l.source_updated_at,
                  l.stg_row_id
            FROM #loop_src AS l
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #donation_work AS w
                WHERE w.source_donation_id = l.source_donation_id
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#donation_work',
                 N'succeeded', @rows_work_inserted, @rows_work_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Inserted new donation rows into #donation_work from #loop_src.');

            SET @current_from = @current_to;
        END;

        BEGIN TRANSACTION;

            SET @step_started_at = SYSDATETIME();
            TRUNCATE TABLE dw.fact_donation_transaction;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_donation_transaction',
                 N'Charity_DW_DB', N'dw', N'fact_donation_transaction',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Truncated dw.fact_donation_transaction for first load.');

            SET @step_started_at = SYSDATETIME();
            DBCC CHECKIDENT ('dw.fact_donation_transaction', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_donation_transaction',
                 N'Charity_DW_DB', N'dw', N'fact_donation_transaction',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Reset identity seed for dw.fact_donation_transaction to 0.');

            SET @step_started_at = SYSDATETIME();
            INSERT INTO dw.fact_donation_transaction
            (
                  date_key,
                  donor_key,
                  campaign_key,
                  center_key,
                  donation_type_key,
                  currency_key,
                  status_key,
                  amount,
                  is_confirmed,
                  is_refunded,
                  source_donation_id,
                  source_donor_id,
                  source_campaign_id,
                  source_reference_code,
                  source_system,
                  etl_batch_id,
                  loaded_at
            )
            SELECT
                  w.date_key,
                  w.donor_key,
                  w.campaign_key,
                  w.center_key,
                  w.donation_type_key,
                  w.currency_key,
                  w.status_key,
                  w.amount,
                  w.is_confirmed,
                  w.is_refunded,
                  w.source_donation_id,
                  w.source_donor_id,
                  w.source_campaign_id,
                  w.source_reference_code,
                  ISNULL(w.source_system, N'FINANCE_OPS'),
                  @etl_batch_id,
                  SYSDATETIME()
            FROM #donation_work AS w;

            SET @rows_fact_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#donation_work',
                 N'Charity_DW_DB', N'dw', N'fact_donation_transaction',
                 N'succeeded', @rows_fact_inserted, @rows_fact_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Inserted first-load rows into dw.fact_donation_transaction.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
           SET load_status   = N'succeeded',
               rows_read     = @rows_read_total,
               rows_inserted  = @rows_fact_inserted,
               rows_rejected = @rows_rejected_total,
               ended_at      = SYSDATETIME(),
               message       = CONCAT(N'First load completed for dw.fact_donation_transaction. Work inserts=',
                                      @rows_work_inserted_total, N', work updates=',
                                      @rows_work_updated_total, N', fact inserts=',
                                      @rows_fact_inserted, N'.')
         WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
               SET load_status   = N'failed',
                   ended_at      = SYSDATETIME(),
                   message       = CONCAT(N'First load failed for dw.fact_donation_transaction. Error: ', @error_message)
             WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 22_create_dw_fact_budget_allocation_event_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.fact_budget_allocation_event
   from Stg_FinanceOps_DB.stg_finance_ops.budget_allocations.

 Procedures:
   1. etl_admin.usp_first_load_dw_fact_budget_allocation_event
      - First/full load for a selected period.
      - Builds a temp working set day by day.
      - Truncates dw.fact_budget_allocation_event.
      - Resets the identity seed so the first real fact key starts from 1.
      - Inserts one row per source budget allocation event.

   2. etl_admin.usp_load_dw_fact_budget_allocation_event_incremental
      - Incremental/normal load for a selected period.
      - Builds a temp working set day by day.
      - Resets identity seed to the current MAX(allocation_event_key).
      - Deletes affected source allocation rows from the fact table.
      - Inserts the latest affected rows again.

 Fact Type Decision:
   - dw.fact_budget_allocation_event is a TRANSACTION / EVENT FACT.
   - It has an event date through allocation_date/date_key.
   - It is not an accumulating snapshot, so measures are not recalculated from
     the beginning to the present. Incremental ETL reloads only affected
     allocation event rows.

 Grain:
   - One row per source budget allocation event.

 Business Mapping:
   - date_key               -> allocation_date through dw.dim_date.
   - center_key             -> budget_allocations.center_id through dw.dim_center.
   - child_key              -> budget_allocations.child_id through dw.dim_child.
   - category_key           -> budget_allocations.category_id through dw.dim_category.
   - allocation_type_key    -> budget_allocations.source_type through dw.dim_allocation_type.
   - donor_key/campaign_key -> only derived when source_type = 'donation'
                               by looking up source_id in staging donations.
                               For internal_budget or unresolved donation,
                               unknown key -1 is used.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - The ETL window detects affected rows by allocation_date OR source change time.
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Dimension lookup failures use unknown keys = -1.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch
===============================================================================
*/

/*=============================================================================
  Procedure 1: First / Full Period Load for dw.fact_budget_allocation_event
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_budget_allocation_event
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_deleted INT = 0,
          @rows_loop_ids INT = 0,
          @rows_rejected INT = 0,
          @rows_inserted INT = 0,
          @rows_updated INT = 0,
          @rows_work_inserted INT = 0,
          @rows_work_updated INT = 0,
          @rows_fact_inserted INT = 0,
          @rows_read_total INT = 0,
          @rows_rejected_total INT = 0,
          @rows_work_inserted_total INT = 0,
          @rows_work_updated_total INT = 0,
          @date_lookup_sql NVARCHAR(MAX),
          @center_lookup_sql NVARCHAR(MAX),
          @child_lookup_sql NVARCHAR(MAX),
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52501, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52502, '@start_time must be earlier than @end_time.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_FACT', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'Charity_DW_DB', N'dw', N'fact_budget_allocation_event',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.fact_budget_allocation_event. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        /*---------------------------------------------------------------------
          Dimension lookup temp tables.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_date
        (
              date_value DATE NOT NULL PRIMARY KEY,
              date_key   INT  NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_date', N'FullDateAlternateKey') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'TimeKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDateAlternateKey AS DATE), MIN(TimeKey)
                FROM dw.dim_date
                WHERE FullDateAlternateKey IS NOT NULL
                  AND ISNULL(TimeKey, -1) <> -1
                GROUP BY CAST(FullDateAlternateKey AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'full_date') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'date_key') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(full_date AS DATE), MIN(date_key)
                FROM dw.dim_date
                WHERE full_date IS NOT NULL
                  AND ISNULL(date_key, -1) <> -1
                GROUP BY CAST(full_date AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'FullDate') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_date', N'DateKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDate AS DATE), MIN(DateKey)
                FROM dw.dim_date
                WHERE FullDate IS NOT NULL
                  AND ISNULL(DateKey, -1) <> -1
                GROUP BY CAST(FullDate AS DATE);';
        END
        ELSE
        BEGIN
            THROW 52503, 'Cannot resolve dw.dim_date columns. Expected (TimeKey, FullDateAlternateKey), (date_key, full_date), or (DateKey, FullDate).', 1;
        END;

        EXEC sys.sp_executesql @date_lookup_sql;
        SELECT @rows_inserted = COUNT(1) FROM #dim_date;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_date',
             N'tempdb', N'#', N'#dim_date',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_date lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_center
        (
              source_center_id BIGINT NOT NULL PRIMARY KEY,
              center_key       INT    NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_center', N'center_id') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_center', N'center_key') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, center_id), MIN(center_key)
                FROM dw.dim_center
                WHERE center_id IS NOT NULL
                  AND ISNULL(center_key, -1) <> -1
                GROUP BY CONVERT(BIGINT, center_id);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_center', N'source_center_id') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_center', N'center_key') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, source_center_id), MIN(center_key)
                FROM dw.dim_center
                WHERE source_center_id IS NOT NULL
                  AND ISNULL(center_key, -1) <> -1
                GROUP BY CONVERT(BIGINT, source_center_id);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_center', N'CenterID') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_center', N'CenterKey') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, CenterID), MIN(CenterKey)
                FROM dw.dim_center
                WHERE CenterID IS NOT NULL
                  AND ISNULL(CenterKey, -1) <> -1
                GROUP BY CONVERT(BIGINT, CenterID);';
        END
        ELSE
        BEGIN
            THROW 52504, 'Cannot resolve dw.dim_center columns. Expected center_key with center_id/source_center_id, or CenterKey with CenterID.', 1;
        END;

        EXEC sys.sp_executesql @center_lookup_sql;
        SELECT @rows_inserted = COUNT(1) FROM #dim_center;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_center',
             N'tempdb', N'#', N'#dim_center',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_center lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_child
        (
              source_child_id BIGINT NOT NULL PRIMARY KEY,
              child_key       INT    NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_child', N'child_id') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_child', N'child_key') IS NOT NULL
        BEGIN
            SET @child_lookup_sql = N'
                INSERT INTO #dim_child (source_child_id, child_key)
                SELECT CONVERT(BIGINT, child_id), MIN(child_key)
                FROM dw.dim_child
                WHERE child_id IS NOT NULL
                  AND ISNULL(child_key, -1) <> -1
                GROUP BY CONVERT(BIGINT, child_id);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_child', N'source_child_id') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_child', N'child_key') IS NOT NULL
        BEGIN
            SET @child_lookup_sql = N'
                INSERT INTO #dim_child (source_child_id, child_key)
                SELECT CONVERT(BIGINT, source_child_id), MIN(child_key)
                FROM dw.dim_child
                WHERE source_child_id IS NOT NULL
                  AND ISNULL(child_key, -1) <> -1
                GROUP BY CONVERT(BIGINT, source_child_id);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_child', N'ChildID') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_child', N'ChildKey') IS NOT NULL
        BEGIN
            SET @child_lookup_sql = N'
                INSERT INTO #dim_child (source_child_id, child_key)
                SELECT CONVERT(BIGINT, ChildID), MIN(ChildKey)
                FROM dw.dim_child
                WHERE ChildID IS NOT NULL
                  AND ISNULL(ChildKey, -1) <> -1
                GROUP BY CONVERT(BIGINT, ChildID);';
        END
        ELSE
        BEGIN
            THROW 52505, 'Cannot resolve dw.dim_child columns. Expected child_key with child_id/source_child_id, or ChildKey with ChildID.', 1;
        END;

        EXEC sys.sp_executesql @child_lookup_sql;
        SELECT @rows_inserted = COUNT(1) FROM #dim_child;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_child',
             N'tempdb', N'#', N'#dim_child',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_child lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_category
        (
              source_category_id BIGINT NOT NULL PRIMARY KEY,
              category_key       INT    NOT NULL
        );

        INSERT INTO #dim_category (source_category_id, category_key)
        SELECT CONVERT(BIGINT, category_id), MIN(category_key)
        FROM dw.dim_category
        WHERE category_id IS NOT NULL
          AND ISNULL(category_key, -1) <> -1
        GROUP BY CONVERT(BIGINT, category_id);

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_category',
             N'tempdb', N'#', N'#dim_category',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_category lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_allocation_type
        (
              code                NVARCHAR(50) NOT NULL PRIMARY KEY,
              allocation_type_key INT          NOT NULL
        );

        INSERT INTO #dim_allocation_type (code, allocation_type_key)
        SELECT LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(code)))), MIN(allocation_type_key)
        FROM dw.dim_allocation_type
        WHERE code IS NOT NULL
          AND LTRIM(RTRIM(code)) <> N''
          AND ISNULL(allocation_type_key, -1) <> -1
        GROUP BY LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(code))));

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_allocation_type',
             N'tempdb', N'#', N'#dim_allocation_type',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_allocation_type lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_donor
        (
              donor_id  BIGINT NOT NULL PRIMARY KEY,
              donor_key INT    NOT NULL
        );

        INSERT INTO #dim_donor (donor_id, donor_key)
        SELECT CONVERT(BIGINT, donor_id), MIN(donor_key)
        FROM dw.dim_donor
        WHERE donor_id IS NOT NULL
          AND ISNULL(donor_key, -1) <> -1
        GROUP BY CONVERT(BIGINT, donor_id);

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donor',
             N'tempdb', N'#', N'#dim_donor',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_donor lookup table.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #dim_campaign
        (
              campaign_id  BIGINT NOT NULL PRIMARY KEY,
              campaign_key INT    NOT NULL
        );

        INSERT INTO #dim_campaign (campaign_id, campaign_key)
        SELECT CONVERT(BIGINT, campaign_id), MIN(campaign_key)
        FROM dw.dim_campaign
        WHERE campaign_id IS NOT NULL
          AND ISNULL(campaign_key, -1) <> -1
        GROUP BY CONVERT(BIGINT, campaign_id);

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_campaign',
             N'tempdb', N'#', N'#dim_campaign',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_campaign lookup table.');

        /*---------------------------------------------------------------------
          Working temp tables.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #loop_ids
        (
              source_allocation_id BIGINT NOT NULL PRIMARY KEY,
              stg_row_id           BIGINT NOT NULL,
              source_updated_at    DATETIME2(0) NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'tempdb', N'#', N'#loop_ids',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_ids.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #loop_src
        (
              source_allocation_id BIGINT NOT NULL PRIMARY KEY,
              allocation_date      DATE NULL,
              source_type_code     NVARCHAR(50) NULL,
              source_id            BIGINT NULL,
              source_center_id     BIGINT NULL,
              source_child_id      BIGINT NULL,
              source_category_id   BIGINT NULL,
              allocated_amount     DECIMAL(18,2) NULL,
              reason               NVARCHAR(MAX) NULL,
              source_system        NVARCHAR(100) NULL,
              source_updated_at    DATETIME2(0) NULL,
              stg_row_id           BIGINT NULL,

              date_key             INT NOT NULL DEFAULT (-1),
              donor_key            INT NOT NULL DEFAULT (-1),
              center_key           INT NOT NULL DEFAULT (-1),
              child_key            INT NOT NULL DEFAULT (-1),
              category_key         INT NOT NULL DEFAULT (-1),
              campaign_key         INT NOT NULL DEFAULT (-1),
              allocation_type_key  INT NOT NULL DEFAULT (-1),
              source_donor_id      BIGINT NULL,
              source_campaign_id   BIGINT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'tempdb', N'#', N'#loop_src',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #loop_src.');

        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #allocation_work
        (
              source_allocation_id BIGINT NOT NULL PRIMARY KEY,
              date_key             INT NOT NULL,
              donor_key            INT NOT NULL,
              center_key           INT NOT NULL,
              child_key            INT NOT NULL,
              category_key         INT NOT NULL,
              campaign_key         INT NOT NULL,
              allocation_type_key  INT NOT NULL,
              allocated_amount     DECIMAL(18,2) NULL,
              reason               NVARCHAR(MAX) NULL,
              source_type          NVARCHAR(50) NULL,
              source_id            BIGINT NULL,
              source_center_id     BIGINT NULL,
              source_child_id      BIGINT NULL,
              source_category_id   BIGINT NULL,
              source_donor_id      BIGINT NULL,
              source_campaign_id   BIGINT NULL,
              source_system        NVARCHAR(100) NULL,
              source_updated_at    DATETIME2(0) NULL,
              stg_row_id           BIGINT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'tempdb', N'#', N'#allocation_work',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table #allocation_work.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time
                SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();
            DELETE FROM #loop_ids;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_ids',
                 N'tempdb', N'#', N'#loop_ids',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Cleared #loop_ids for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            SELECT @rows_rejected = COUNT(1)
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations AS s
            WHERE
            (
                   (s.allocation_date IS NOT NULL
                    AND CONVERT(DATETIME2(0), s.allocation_date) >= @current_from
                    AND CONVERT(DATETIME2(0), s.allocation_date) <  @current_to)
                OR (COALESCE(s.source_updated_at, s.created_at, s.extracted_at) >= @current_from
                    AND COALESCE(s.source_updated_at, s.created_at, s.extracted_at) <  @current_to)
            )
            AND (ISNULL(s.is_valid, 0) <> 1 OR s.id IS NULL);

            INSERT INTO #loop_ids
            (
                  source_allocation_id,
                  stg_row_id,
                  source_updated_at
            )
            SELECT
                  CONVERT(BIGINT, s.id) AS source_allocation_id,
                  MAX(s.stg_row_id) AS stg_row_id,
                  MAX(COALESCE(s.source_updated_at, s.created_at, s.extracted_at)) AS source_updated_at
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations AS s
            WHERE s.is_valid = 1
              AND s.id IS NOT NULL
              AND
              (
                     (s.allocation_date IS NOT NULL
                      AND CONVERT(DATETIME2(0), s.allocation_date) >= @current_from
                      AND CONVERT(DATETIME2(0), s.allocation_date) <  @current_to)
                  OR (COALESCE(s.source_updated_at, s.created_at, s.extracted_at) >= @current_from
                      AND COALESCE(s.source_updated_at, s.created_at, s.extracted_at) <  @current_to)
              )
            GROUP BY CONVERT(BIGINT, s.id);

            SET @rows_loop_ids = @@ROWCOUNT;
            SET @rows_read_total += @rows_loop_ids;
            SET @rows_rejected_total += ISNULL(@rows_rejected, 0);

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
                 N'tempdb', N'#', N'#loop_ids',
                 N'succeeded', @rows_loop_ids, @rows_loop_ids,
                 0, @rows_rejected,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Loaded latest affected allocation IDs into #loop_ids for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();
            DELETE FROM #loop_src;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_deleted, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Cleared #loop_src before loading current period allocation rows.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #loop_src
            (
                  source_allocation_id,
                  allocation_date,
                  source_type_code,
                  source_id,
                  source_center_id,
                  source_child_id,
                  source_category_id,
                  allocated_amount,
                  reason,
                  source_system,
                  source_updated_at,
                  stg_row_id,
                  date_key,
                  center_key,
                  child_key,
                  category_key,
                  allocation_type_key
            )
            SELECT
                  CONVERT(BIGINT, s.id),
                  s.allocation_date,
                  LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.source_type)))),
                  CONVERT(BIGINT, s.source_id),
                  CONVERT(BIGINT, s.center_id),
                  CONVERT(BIGINT, s.child_id),
                  CONVERT(BIGINT, s.category_id),
                  s.allocated_amount,
                  CONVERT(NVARCHAR(MAX), s.reason),
                  s.source_system,
                  COALESCE(s.source_updated_at, s.created_at, s.extracted_at),
                  s.stg_row_id,
                  ISNULL(dd.date_key, -1),
                  ISNULL(dc.center_key, -1),
                  ISNULL(dch.child_key, -1),
                  ISNULL(dcat.category_key, -1),
                  ISNULL(dat.allocation_type_key, -1)
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations AS s
            INNER JOIN #loop_ids AS ids
                    ON ids.stg_row_id = s.stg_row_id
            LEFT JOIN #dim_date AS dd
                   ON dd.date_value = s.allocation_date
            LEFT JOIN #dim_center AS dc
                   ON dc.source_center_id = CONVERT(BIGINT, s.center_id)
            LEFT JOIN #dim_child AS dch
                   ON dch.source_child_id = CONVERT(BIGINT, s.child_id)
            LEFT JOIN #dim_category AS dcat
                   ON dcat.source_category_id = CONVERT(BIGINT, s.category_id)
            LEFT JOIN #dim_allocation_type AS dat
                   ON dat.code = LOWER(CONVERT(NVARCHAR(50), LTRIM(RTRIM(s.source_type))));

            SET @rows_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
                 N'tempdb', N'#', N'#loop_src',
                 N'succeeded', @rows_inserted, @rows_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 N'Loaded current period allocation rows into #loop_src with basic dimension keys.');

            SET @step_started_at = SYSDATETIME();

            UPDATE tgt
            SET
                  tgt.date_key            = src.date_key,
                  tgt.donor_key           = src.donor_key,
                  tgt.center_key          = src.center_key,
                  tgt.child_key           = src.child_key,
                  tgt.category_key        = src.category_key,
                  tgt.campaign_key        = src.campaign_key,
                  tgt.allocation_type_key = src.allocation_type_key,
                  tgt.allocated_amount    = src.allocated_amount,
                  tgt.reason              = src.reason,
                  tgt.source_type         = src.source_type_code,
                  tgt.source_id           = src.source_id,
                  tgt.source_center_id    = src.source_center_id,
                  tgt.source_child_id     = src.source_child_id,
                  tgt.source_category_id  = src.source_category_id,
                  tgt.source_donor_id     = src.source_donor_id,
                  tgt.source_campaign_id  = src.source_campaign_id,
                  tgt.source_system       = src.source_system,
                  tgt.source_updated_at   = src.source_updated_at,
                  tgt.stg_row_id          = src.stg_row_id
            FROM #allocation_work AS tgt
            INNER JOIN #loop_src AS src
                    ON src.source_allocation_id = tgt.source_allocation_id;

            SET @rows_work_updated = @@ROWCOUNT;
            SET @rows_work_updated_total += @rows_work_updated;

            INSERT INTO #allocation_work
            (
                  source_allocation_id,
                  date_key,
                  donor_key,
                  center_key,
                  child_key,
                  category_key,
                  campaign_key,
                  allocation_type_key,
                  allocated_amount,
                  reason,
                  source_type,
                  source_id,
                  source_center_id,
                  source_child_id,
                  source_category_id,
                  source_donor_id,
                  source_campaign_id,
                  source_system,
                  source_updated_at,
                  stg_row_id
            )
            SELECT
                  src.source_allocation_id,
                  src.date_key,
                  src.donor_key,
                  src.center_key,
                  src.child_key,
                  src.category_key,
                  src.campaign_key,
                  src.allocation_type_key,
                  src.allocated_amount,
                  src.reason,
                  src.source_type_code,
                  src.source_id,
                  src.source_center_id,
                  src.source_child_id,
                  src.source_category_id,
                  src.source_donor_id,
                  src.source_campaign_id,
                  src.source_system,
                  src.source_updated_at,
                  src.stg_row_id
            FROM #loop_src AS src
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #allocation_work AS tgt
                WHERE tgt.source_allocation_id = src.source_allocation_id
            );

            SET @rows_work_inserted = @@ROWCOUNT;
            SET @rows_work_inserted_total += @rows_work_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#loop_src',
                 N'tempdb', N'#', N'#allocation_work',
                 N'succeeded', @rows_inserted, @rows_work_inserted + @rows_work_updated,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Upserted #allocation_work. Inserted: ', @rows_work_inserted,
                        N'; updated: ', @rows_work_updated, N'.'));

            SET @current_from = @current_to;
        END;

        /*---------------------------------------------------------------------
          Resolve donation donor/campaign for allocation events whose source is
          a donation. This step is done after the loop to avoid repeatedly
          scanning staging donations inside each day.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();
        CREATE TABLE #donation_latest_ids
        (
              source_donation_id BIGINT NOT NULL PRIMARY KEY,
              stg_row_id         BIGINT NOT NULL
        );

        INSERT INTO #donation_latest_ids (source_donation_id, stg_row_id)
        SELECT
              CONVERT(BIGINT, d.id),
              MAX(d.stg_row_id)
        FROM Stg_FinanceOps_DB.stg_finance_ops.donations AS d
        INNER JOIN #allocation_work AS aw
                ON aw.source_type = N'donation'
               AND aw.source_id = CONVERT(BIGINT, d.id)
        WHERE d.is_valid = 1
          AND d.id IS NOT NULL
        GROUP BY CONVERT(BIGINT, d.id);

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#donation_latest_ids',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Loaded latest referenced donation IDs for donation-based allocations.');

        SET @step_started_at = SYSDATETIME();

        UPDATE aw
        SET
              aw.source_donor_id = CONVERT(BIGINT, d.donor_id),
              aw.source_campaign_id = CONVERT(BIGINT, d.campaign_id),
              aw.donor_key = ISNULL(ddonor.donor_key, -1),
              aw.campaign_key = ISNULL(dcamp.campaign_key, -1)
        FROM #allocation_work AS aw
        INNER JOIN #donation_latest_ids AS ids
                ON ids.source_donation_id = aw.source_id
        INNER JOIN Stg_FinanceOps_DB.stg_finance_ops.donations AS d
                ON d.stg_row_id = ids.stg_row_id
        LEFT JOIN #dim_donor AS ddonor
               ON ddonor.donor_id = CONVERT(BIGINT, d.donor_id)
        LEFT JOIN #dim_campaign AS dcamp
               ON dcamp.campaign_id = CONVERT(BIGINT, d.campaign_id)
        WHERE aw.source_type = N'donation';

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#allocation_work',
             N'tempdb', N'#', N'#allocation_work',
             N'succeeded', @rows_updated, 0,
             @rows_updated, 0,
             @step_started_at, SYSDATETIME(), N'Resolved donor_key and campaign_key for donation-based allocations.');

        /*---------------------------------------------------------------------
          First load DW write.
        ---------------------------------------------------------------------*/
        BEGIN TRANSACTION;

        SET @step_started_at = SYSDATETIME();
        TRUNCATE TABLE dw.fact_budget_allocation_event;
        SET @rows_deleted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_budget_allocation_event',
             N'Charity_DW_DB', N'dw', N'fact_budget_allocation_event',
             N'succeeded', @rows_deleted, @rows_deleted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Truncated dw.fact_budget_allocation_event for first load.');

        SET @step_started_at = SYSDATETIME();
        DBCC CHECKIDENT (N'dw.fact_budget_allocation_event', RESEED, 0) WITH NO_INFOMSGS;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_budget_allocation_event',
             N'Charity_DW_DB', N'dw', N'fact_budget_allocation_event',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Reset fact identity seed to 0 for first load.');

        SET @step_started_at = SYSDATETIME();

        INSERT INTO dw.fact_budget_allocation_event
        (
              date_key,
              donor_key,
              center_key,
              child_key,
              category_key,
              campaign_key,
              allocation_type_key,
              allocated_amount,
              reason,
              source_allocation_id,
              source_type,
              source_id,
              source_center_id,
              source_child_id,
              source_category_id,
              source_system,
              etl_batch_id,
              loaded_at
        )
        SELECT
              aw.date_key,
              aw.donor_key,
              aw.center_key,
              aw.child_key,
              aw.category_key,
              aw.campaign_key,
              aw.allocation_type_key,
              aw.allocated_amount,
              aw.reason,
              aw.source_allocation_id,
              aw.source_type,
              aw.source_id,
              aw.source_center_id,
              aw.source_child_id,
              aw.source_category_id,
              aw.source_system,
              @etl_batch_id,
              SYSDATETIME()
        FROM #allocation_work AS aw;

        SET @rows_fact_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#allocation_work',
             N'Charity_DW_DB', N'dw', N'fact_budget_allocation_event',
             N'succeeded', @rows_fact_inserted, @rows_fact_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Inserted first-load rows into dw.fact_budget_allocation_event.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
        SET
              load_status = N'succeeded',
              rows_read = @rows_read_total,
              rows_inserted = @rows_fact_inserted,
              rows_rejected = @rows_rejected_total,
              ended_at = SYSDATETIME(),
              message = CONCAT(
                    N'Succeeded first load for dw.fact_budget_allocation_event. Work inserted: ', @rows_work_inserted_total,
                    N'; work updated: ', @rows_work_updated_total,
                    N'; fact inserted: ', @rows_fact_inserted,
                    N'; rejected source rows: ', @rows_rejected_total, N'.')
        WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
            SET
                  load_status = N'failed',
                  ended_at = SYSDATETIME(),
                  message = CONCAT(N'Failed first load for dw.fact_budget_allocation_event. Error: ', @error_message)
            WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO





/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 23_create_dw_fact_monthly_financial_snapshot_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading
   dw.fact_monthly_financial_snapshot from Finance staging tables.

 Procedures:
   1. etl_admin.usp_first_load_dw_fact_monthly_financial_snapshot
      - First/full load for selected period months.
      - Builds month and movement temp tables.
      - Truncates dw.fact_monthly_financial_snapshot.
      - Resets identity so the first real fact key starts from 1.
      - Inserts one row per month / center with monthly financial totals.

   2. etl_admin.usp_load_dw_fact_monthly_financial_snapshot_incremental
      - Incremental/normal load.
      - Finds months affected by the input period and by source-row changes.
      - Rebuilds complete affected month snapshots.
      - Deletes existing target rows for affected months and inserts recalculated rows.
      - Resets identity seed to the current MAX(monthly_financial_snapshot_key).

 Fact Type Decision:
   - dw.fact_monthly_financial_snapshot is a PERIODIC SNAPSHOT FACT.
   - It has a month_key, so it is not an accumulating fact.
   - Measures are calculated for the snapshot month only, not from all historical
     time to the present.

 Grain:
   - One row per month_key / center_key.

 Business Rules:
   - total_donation_amount:
       Sum of budget allocation amounts whose source_type = 'donation'.
       Because source donations do not contain center_id, center-level donation
       value is derived through budget_allocations.
       If the linked donation exists, only confirmed donations are counted.
   - total_expense_amount:
       Sum of approved expenses by expense_date and center_id.
   - total_payment_amount:
       Sum of paid payments by payment_date and center_id.
   - net_balance:
       total_donation_amount - total_expense_amount - total_payment_amount.
   - donation_count:
       Count of distinct donation source_ids allocated to the center in the month.
   - expense_count:
       Count of approved expense rows in the month.
   - payment_count:
       Count of paid payment rows in the month.
   - allocation_count:
       Count of all valid budget allocation events in the month.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys / clustered keys where useful.
   - Dimension lookup failures use unknown keys = -1.
   - The month/date dimension must contain the month-end date. Missing month-end
     dates are logged and excluded to avoid corrupt month_key = -1 snapshots.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch
===============================================================================
*/


/*=============================================================================
  Procedure 1: First / Full Period Load for dw.fact_monthly_financial_snapshot
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_monthly_financial_snapshot
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @period_start_date DATE,
          @period_last_date DATE,
          @month_cursor DATE,
          @current_month_key INT,
          @current_month_start DATE,
          @current_month_end DATE,
          @current_month_end_exclusive DATE,
          @rows_inserted INT = 0,
          @rows_deleted INT = 0,
          @rows_read INT = 0,
          @rows_rejected INT = 0,
          @rows_months INT = 0,
          @rows_fact_inserted INT = 0,
          @rows_work_total INT = 0,
          @rows_rejected_total INT = 0,
          @identity_seed BIGINT = 0,
          @date_lookup_sql NVARCHAR(MAX),
          @center_lookup_sql NVARCHAR(MAX),
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52701, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52702, '@start_time must be earlier than @end_time.', 1;
    END;

    IF OBJECT_ID(N'dw.fact_monthly_financial_snapshot', N'U') IS NULL
    BEGIN
        THROW 52703, 'Missing target table dw.fact_monthly_financial_snapshot.', 1;
    END;

    IF OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.budget_allocations', N'U') IS NULL
       OR OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.expenses', N'U') IS NULL
       OR OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.payments', N'U') IS NULL
       OR OBJECT_ID(N'Stg_FinanceOps_DB.stg_finance_ops.donations', N'U') IS NULL
    BEGIN
        THROW 52704, 'Missing one or more required staging finance tables.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_FACT', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'finance_snapshot_sources',
             N'Charity_DW_DB', N'dw', N'fact_monthly_financial_snapshot',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.fact_monthly_financial_snapshot. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        /*---------------------------------------------------------------------
          Create date lookup.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_date
        (
              date_value DATE NOT NULL PRIMARY KEY,
              date_key   INT  NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_date', N'FullDateAlternateKey') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'TimeKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDateAlternateKey AS DATE), MIN(TimeKey)
                FROM dw.dim_date
                WHERE FullDateAlternateKey IS NOT NULL
                  AND ISNULL(TimeKey, -1) <> -1
                GROUP BY CAST(FullDateAlternateKey AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'full_date') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'date_key') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(full_date AS DATE), MIN(date_key)
                FROM dw.dim_date
                WHERE full_date IS NOT NULL
                  AND ISNULL(date_key, -1) <> -1
                GROUP BY CAST(full_date AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'FullDate') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_date', N'DateKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDate AS DATE), MIN(DateKey)
                FROM dw.dim_date
                WHERE FullDate IS NOT NULL
                  AND ISNULL(DateKey, -1) <> -1
                GROUP BY CAST(FullDate AS DATE);';
        END
        ELSE
        BEGIN
            THROW 52705, 'Cannot resolve dw.dim_date columns. Expected (TimeKey, FullDateAlternateKey), (date_key, full_date), or (DateKey, FullDate).', 1;
        END;

        EXEC sys.sp_executesql @date_lookup_sql;
        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_date',
             N'tempdb', N'#', N'#dim_date',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_date lookup table.');

        /*---------------------------------------------------------------------
          Create center lookup.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_center
        (
              source_center_id BIGINT NOT NULL PRIMARY KEY,
              center_key       INT    NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_center', N'center_id') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_center', N'center_key') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, center_id), MIN(center_key)
                FROM dw.dim_center
                WHERE center_id IS NOT NULL
                  AND ISNULL(center_key, -1) <> -1
                GROUP BY CONVERT(BIGINT, center_id);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_center', N'source_center_id') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_center', N'center_key') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, source_center_id), MIN(center_key)
                FROM dw.dim_center
                WHERE source_center_id IS NOT NULL
                  AND ISNULL(center_key, -1) <> -1
                GROUP BY CONVERT(BIGINT, source_center_id);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_center', N'CenterID') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_center', N'CenterKey') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, CenterID), MIN(CenterKey)
                FROM dw.dim_center
                WHERE CenterID IS NOT NULL
                  AND ISNULL(CenterKey, -1) <> -1
                GROUP BY CONVERT(BIGINT, CenterID);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_center', N'SourceCenterID') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_center', N'CenterKey') IS NOT NULL
        BEGIN
            SET @center_lookup_sql = N'
                INSERT INTO #dim_center (source_center_id, center_key)
                SELECT CONVERT(BIGINT, SourceCenterID), MIN(CenterKey)
                FROM dw.dim_center
                WHERE SourceCenterID IS NOT NULL
                  AND ISNULL(CenterKey, -1) <> -1
                GROUP BY CONVERT(BIGINT, SourceCenterID);';
        END
        ELSE
        BEGIN
            THROW 52706, 'Cannot resolve dw.dim_center columns. Expected source center id and center key columns.', 1;
        END;

        EXEC sys.sp_executesql @center_lookup_sql;
        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_center',
             N'tempdb', N'#', N'#dim_center',
             N'succeeded', @rows_inserted, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created and loaded #dim_center lookup table.');

        /*---------------------------------------------------------------------
          Build snapshot months from the requested period.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #snapshot_month_candidates
        (
              month_end_date DATE NOT NULL PRIMARY KEY
        );

        SET @period_start_date = CAST(@start_time AS DATE);
        SET @period_last_date = CASE
                                    WHEN @end_time = CONVERT(DATETIME2(0), CAST(@end_time AS DATE))
                                        THEN DATEADD(DAY, -1, CAST(@end_time AS DATE))
                                    ELSE CAST(@end_time AS DATE)
                                END;

        SET @month_cursor = DATEFROMPARTS(YEAR(@period_start_date), MONTH(@period_start_date), 1);

        WHILE @month_cursor <= DATEFROMPARTS(YEAR(@period_last_date), MONTH(@period_last_date), 1)
        BEGIN
            INSERT INTO #snapshot_month_candidates (month_end_date)
            SELECT EOMONTH(@month_cursor)
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM #snapshot_month_candidates
                WHERE month_end_date = EOMONTH(@month_cursor)
            );

            SET @rows_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'procedure', N'etl_admin', N'period_month_loop',
                 N'tempdb', N'#', N'#snapshot_month_candidates',
                 N'succeeded', 1, @rows_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Added month candidate from requested period: ', CONVERT(NVARCHAR(10), EOMONTH(@month_cursor), 120), N'.'));

            SET @step_started_at = SYSDATETIME();
            SET @month_cursor = DATEADD(MONTH, 1, @month_cursor);
        END;

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #snapshot_months
        (
              month_key                 INT  NOT NULL PRIMARY KEY,
              month_start_date          DATE NOT NULL,
              month_end_date            DATE NOT NULL,
              month_end_exclusive_date  DATE NOT NULL
        );

        INSERT INTO #snapshot_months
            (month_key, month_start_date, month_end_date, month_end_exclusive_date)
        SELECT
              D.date_key,
              DATEFROMPARTS(YEAR(C.month_end_date), MONTH(C.month_end_date), 1),
              C.month_end_date,
              DATEADD(DAY, 1, C.month_end_date)
        FROM #snapshot_month_candidates C
        JOIN #dim_date D
             ON D.date_value = C.month_end_date;

        SET @rows_months = @@ROWCOUNT;

        SELECT @rows_rejected = COUNT(1)
        FROM #snapshot_month_candidates C
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM #dim_date D
            WHERE D.date_value = C.month_end_date
        );

        SET @rows_rejected_total += ISNULL(@rows_rejected, 0);

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#snapshot_month_candidates',
             N'tempdb', N'#', N'#snapshot_months',
             N'succeeded', @rows_months + @rows_rejected, @rows_months,
             0, @rows_rejected,
             @step_started_at, SYSDATETIME(), N'Created #snapshot_months with month-end date keys.');

        /*---------------------------------------------------------------------
          Build monthly movement table month by month.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #CurrentMonthMovement
        (
              month_key               INT NOT NULL,
              source_center_id        BIGINT NOT NULL,
              center_key              INT NOT NULL,
              total_donation_amount   DECIMAL(18,2) NOT NULL,
              total_expense_amount    DECIMAL(18,2) NOT NULL,
              total_payment_amount    DECIMAL(18,2) NOT NULL,
              donation_count          INT NOT NULL,
              expense_count           INT NOT NULL,
              payment_count           INT NOT NULL,
              allocation_count        INT NOT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'procedure', N'etl_admin', N'create_temp_table',
             N'tempdb', N'#', N'#CurrentMonthMovement',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created #CurrentMonthMovement temp table.');

        DECLARE month_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT month_key, month_start_date, month_end_date, month_end_exclusive_date
            FROM #snapshot_months
            ORDER BY month_start_date;

        OPEN month_cur;
        FETCH NEXT FROM month_cur
            INTO @current_month_key, @current_month_start, @current_month_end, @current_month_end_exclusive;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            /* Donation amount from donation-based allocations */
            SET @step_started_at = SYSDATETIME();

            INSERT INTO #CurrentMonthMovement
                (month_key, source_center_id, center_key,
                 total_donation_amount, total_expense_amount, total_payment_amount,
                 donation_count, expense_count, payment_count, allocation_count)
            SELECT
                  @current_month_key,
                  CONVERT(BIGINT, BA.center_id),
                  ISNULL(DC.center_key, -1),
                  SUM(ISNULL(BA.allocated_amount, 0)),
                  CONVERT(DECIMAL(18,2), 0),
                  CONVERT(DECIMAL(18,2), 0),
                  COUNT(DISTINCT BA.source_id),
                  0,
                  0,
                  0
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations BA
            LEFT JOIN Stg_FinanceOps_DB.stg_finance_ops.donations D
                   ON LOWER(LTRIM(RTRIM(ISNULL(BA.source_type, N'')))) = N'donation'
                  AND BA.source_id = D.id
                  AND D.is_valid = 1
            LEFT JOIN #dim_center DC
                   ON DC.source_center_id = CONVERT(BIGINT, BA.center_id)
            WHERE BA.is_valid = 1
              AND LOWER(LTRIM(RTRIM(ISNULL(BA.source_type, N'')))) = N'donation'
              AND BA.allocation_date >= @current_month_start
              AND BA.allocation_date <  @current_month_end_exclusive
              AND BA.center_id IS NOT NULL
              AND (D.id IS NULL OR LOWER(LTRIM(RTRIM(ISNULL(D.status, N'')))) = N'confirmed')
            GROUP BY CONVERT(BIGINT, BA.center_id), ISNULL(DC.center_key, -1);

            SET @rows_inserted = @@ROWCOUNT;
            SET @rows_work_total += ISNULL(@rows_inserted, 0);

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations/donations',
                 N'tempdb', N'#', N'#CurrentMonthMovement',
                 N'succeeded', NULL, @rows_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted donation allocation movement for month_key ', @current_month_key, N'.'));

            /* Expense amount */
            SET @step_started_at = SYSDATETIME();

            INSERT INTO #CurrentMonthMovement
                (month_key, source_center_id, center_key,
                 total_donation_amount, total_expense_amount, total_payment_amount,
                 donation_count, expense_count, payment_count, allocation_count)
            SELECT
                  @current_month_key,
                  CONVERT(BIGINT, E.center_id),
                  ISNULL(DC.center_key, -1),
                  CONVERT(DECIMAL(18,2), 0),
                  SUM(ISNULL(E.amount, 0)),
                  CONVERT(DECIMAL(18,2), 0),
                  0,
                  COUNT(1),
                  0,
                  0
            FROM Stg_FinanceOps_DB.stg_finance_ops.expenses E
            LEFT JOIN #dim_center DC
                   ON DC.source_center_id = CONVERT(BIGINT, E.center_id)
            WHERE E.is_valid = 1
              AND LOWER(LTRIM(RTRIM(ISNULL(E.status, N'')))) = N'approved'
              AND E.expense_date >= @current_month_start
              AND E.expense_date <  @current_month_end_exclusive
              AND E.center_id IS NOT NULL
            GROUP BY CONVERT(BIGINT, E.center_id), ISNULL(DC.center_key, -1);

            SET @rows_inserted = @@ROWCOUNT;
            SET @rows_work_total += ISNULL(@rows_inserted, 0);

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'expenses',
                 N'tempdb', N'#', N'#CurrentMonthMovement',
                 N'succeeded', NULL, @rows_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted approved expense movement for month_key ', @current_month_key, N'.'));

            /* Payment amount */
            SET @step_started_at = SYSDATETIME();

            INSERT INTO #CurrentMonthMovement
                (month_key, source_center_id, center_key,
                 total_donation_amount, total_expense_amount, total_payment_amount,
                 donation_count, expense_count, payment_count, allocation_count)
            SELECT
                  @current_month_key,
                  CONVERT(BIGINT, P.center_id),
                  ISNULL(DC.center_key, -1),
                  CONVERT(DECIMAL(18,2), 0),
                  CONVERT(DECIMAL(18,2), 0),
                  SUM(ISNULL(P.amount, 0)),
                  0,
                  0,
                  COUNT(1),
                  0
            FROM Stg_FinanceOps_DB.stg_finance_ops.payments P
            LEFT JOIN #dim_center DC
                   ON DC.source_center_id = CONVERT(BIGINT, P.center_id)
            WHERE P.is_valid = 1
              AND LOWER(LTRIM(RTRIM(ISNULL(P.status, N'')))) = N'paid'
              AND P.payment_date >= @current_month_start
              AND P.payment_date <  @current_month_end_exclusive
              AND P.center_id IS NOT NULL
            GROUP BY CONVERT(BIGINT, P.center_id), ISNULL(DC.center_key, -1);

            SET @rows_inserted = @@ROWCOUNT;
            SET @rows_work_total += ISNULL(@rows_inserted, 0);

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'payments',
                 N'tempdb', N'#', N'#CurrentMonthMovement',
                 N'succeeded', NULL, @rows_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted paid payment movement for month_key ', @current_month_key, N'.'));

            /* Allocation count, all allocation source types */
            SET @step_started_at = SYSDATETIME();

            INSERT INTO #CurrentMonthMovement
                (month_key, source_center_id, center_key,
                 total_donation_amount, total_expense_amount, total_payment_amount,
                 donation_count, expense_count, payment_count, allocation_count)
            SELECT
                  @current_month_key,
                  CONVERT(BIGINT, BA.center_id),
                  ISNULL(DC.center_key, -1),
                  CONVERT(DECIMAL(18,2), 0),
                  CONVERT(DECIMAL(18,2), 0),
                  CONVERT(DECIMAL(18,2), 0),
                  0,
                  0,
                  0,
                  COUNT(1)
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations BA
            LEFT JOIN #dim_center DC
                   ON DC.source_center_id = CONVERT(BIGINT, BA.center_id)
            WHERE BA.is_valid = 1
              AND BA.allocation_date >= @current_month_start
              AND BA.allocation_date <  @current_month_end_exclusive
              AND BA.center_id IS NOT NULL
            GROUP BY CONVERT(BIGINT, BA.center_id), ISNULL(DC.center_key, -1);

            SET @rows_inserted = @@ROWCOUNT;
            SET @rows_work_total += ISNULL(@rows_inserted, 0);

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
                 N'tempdb', N'#', N'#CurrentMonthMovement',
                 N'succeeded', NULL, @rows_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Inserted allocation count movement for month_key ', @current_month_key, N'.'));

            FETCH NEXT FROM month_cur
                INTO @current_month_key, @current_month_start, @current_month_end, @current_month_end_exclusive;
        END;

        CLOSE month_cur;
        DEALLOCATE month_cur;

        /*---------------------------------------------------------------------
          Aggregate movement into final snapshot grain.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #NewSnapshot
        (
              month_key               INT NOT NULL,
              center_key              INT NOT NULL,
              total_donation_amount   DECIMAL(18,2) NOT NULL,
              total_expense_amount    DECIMAL(18,2) NOT NULL,
              total_payment_amount    DECIMAL(18,2) NOT NULL,
              net_balance             DECIMAL(18,2) NOT NULL,
              donation_count          INT NOT NULL,
              expense_count           INT NOT NULL,
              payment_count           INT NOT NULL,
              allocation_count        INT NOT NULL,
              PRIMARY KEY CLUSTERED (month_key, center_key)
        );

        INSERT INTO #NewSnapshot
            (month_key, center_key,
             total_donation_amount, total_expense_amount, total_payment_amount, net_balance,
             donation_count, expense_count, payment_count, allocation_count)
        SELECT
              M.month_key,
              M.center_key,
              SUM(M.total_donation_amount),
              SUM(M.total_expense_amount),
              SUM(M.total_payment_amount),
              SUM(M.total_donation_amount) - SUM(M.total_expense_amount) - SUM(M.total_payment_amount),
              SUM(M.donation_count),
              SUM(M.expense_count),
              SUM(M.payment_count),
              SUM(M.allocation_count)
        FROM #CurrentMonthMovement M
        GROUP BY M.month_key, M.center_key;

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#CurrentMonthMovement',
             N'tempdb', N'#', N'#NewSnapshot',
             N'succeeded', @rows_work_total, @rows_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Aggregated #CurrentMonthMovement into #NewSnapshot at month/center grain.');

        /*---------------------------------------------------------------------
          First load into target.
        ---------------------------------------------------------------------*/
        BEGIN TRANSACTION;

            SET @step_started_at = SYSDATETIME();

            TRUNCATE TABLE dw.fact_monthly_financial_snapshot;
            SET @rows_deleted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_monthly_financial_snapshot',
                 N'Charity_DW_DB', N'dw', N'fact_monthly_financial_snapshot',
                 N'succeeded', NULL, @rows_deleted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Truncated dw.fact_monthly_financial_snapshot for first load.');

            SET @step_started_at = SYSDATETIME();
            DBCC CHECKIDENT ('dw.fact_monthly_financial_snapshot', RESEED, 0) WITH NO_INFOMSGS;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'procedure', N'etl_admin', N'identity_reset',
                 N'Charity_DW_DB', N'dw', N'fact_monthly_financial_snapshot',
                 N'succeeded', 0, 0,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Reset identity seed to 0 for first load.');

            SET @step_started_at = SYSDATETIME();

            INSERT INTO dw.fact_monthly_financial_snapshot
                (month_key, center_key,
                 total_donation_amount, total_expense_amount, total_payment_amount, net_balance,
                 donation_count, expense_count, payment_count, allocation_count,
                 source_system, etl_batch_id, loaded_at)
            SELECT
                 month_key,
                 center_key,
                 total_donation_amount,
                 total_expense_amount,
                 total_payment_amount,
                 net_balance,
                 donation_count,
                 expense_count,
                 payment_count,
                 allocation_count,
                 N'FINANCE_OPS',
                 @etl_batch_id,
                 SYSDATETIME()
            FROM #NewSnapshot;

            SET @rows_fact_inserted = @@ROWCOUNT;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'tempdb', N'#', N'#NewSnapshot',
                 N'Charity_DW_DB', N'dw', N'fact_monthly_financial_snapshot',
                 N'succeeded', @rows_fact_inserted, @rows_fact_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(), N'Inserted first-load monthly financial snapshot rows.');

        COMMIT TRANSACTION;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
        SET load_status = N'succeeded',
            rows_read = @rows_work_total,
            rows_inserted = @rows_fact_inserted,
            rows_rejected = @rows_rejected_total,
            ended_at = SYSDATETIME(),
            message = CONCAT(N'First load completed for dw.fact_monthly_financial_snapshot. Months loaded: ',
                             @rows_months, N'. Fact rows inserted: ', @rows_fact_inserted, N'.')
        WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'month_cur') >= -1
        BEGIN
            CLOSE month_cur;
            DEALLOCATE month_cur;
        END;

        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
            SET load_status = N'failed',
                ended_at = SYSDATETIME(),
                message = CONCAT(ISNULL(message, N''), N' Error: ', @error_message)
            WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO



/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL
 File         : 24_create_dw_fact_donation_lifecycle_etl_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two practical ETL procedures for loading dw.fact_donation_lifecycle
   from Stg_FinanceOps_DB.stg_finance_ops.donations and
   Stg_FinanceOps_DB.stg_finance_ops.budget_allocations.

 Procedures:
   1. etl_admin.usp_first_load_dw_fact_donation_lifecycle
      - First/full load for a selected period.
      - Builds the affected donation set day by day.
      - Truncates dw.fact_donation_lifecycle.
      - Resets the identity seed so the first real fact key starts from 1.
      - Inserts one accumulating lifecycle row per donation.

   2. etl_admin.usp_load_dw_fact_donation_lifecycle_incremental
      - Incremental/normal load for a selected period.
      - Builds the affected donation set day by day from donation changes and
        allocation changes.
      - Resets identity seed to MAX(donation_lifecycle_key).
      - Deletes affected donation lifecycle rows.
      - Recalculates the complete lifecycle state for those donations from all
        available staging data, not only from the requested period.

 Fact Type Decision:
   - dw.fact_donation_lifecycle is an ACCUMULATING SNAPSHOT FACT.
   - It has multiple milestone dates: created, confirmed, allocated.
   - The input period decides which source donation ids are affected.
   - The measures and milestone dates are then recalculated from all available
     staging history for those affected donation ids.
   - This matches the accumulative-fact rule: values are not computed only inside
     the single load period.

 Grain:
   - One row per source donation lifecycle.

 Business Rules:
   - created_date_key:
       CAST(COALESCE(donations.created_at, donations.donation_date) AS DATE)
   - confirmed_date_key:
       donations.donation_date when status IN ('confirmed', 'refunded')
       otherwise unknown key = -1.
   - allocated_date_key:
       MIN(budget_allocations.allocation_date) for allocation rows where
       source_type = 'donation' and source_id = donation id.
   - lifecycle_status_key:
       dw.dim_status where status_type = 'donation' and code = normalized status.
   - current_stage:
       refunded  -> 'refunded'
       rejected  -> 'rejected'
       allocated -> 'allocated'
       confirmed -> 'confirmed'
       otherwise -> 'created'
   - days_to_confirm:
       DATEDIFF(DAY, created_date, confirmed_date), floored at 0.
   - days_to_allocate:
       DATEDIFF(DAY, confirmed_date, allocated_date), floored at 0.

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - No MERGE.
   - No window functions.
   - Temp tables use simple primary keys for speed.
   - Dimension lookup failures use unknown keys = -1.
   - Step-level logs are written into:
       Charity_DW_DB.etl_admin.etl_load_log
   - Batch rows are written into:
       Charity_DW_DB.etl_admin.etl_batch
===============================================================================
*/

/*=============================================================================
  Procedure 1: First / Full Period Load for dw.fact_donation_lifecycle
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_fact_donation_lifecycle
      @start_time DATETIME2(0),
      @end_time   DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @current_from DATETIME2(0),
          @current_to DATETIME2(0),
          @rows_deleted INT = 0,
          @rows_inserted INT = 0,
          @rows_read INT = 0,
          @rows_rejected INT = 0,
          @rows_loop_inserted INT = 0,
          @rows_loop_alloc_inserted INT = 0,
          @rows_loop_total INT = 0,
          @rows_work_inserted INT = 0,
          @rows_fact_inserted INT = 0,
          @rows_lookup INT = 0,
          @date_lookup_sql NVARCHAR(MAX),
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        THROW 52701, '@start_time and @end_time are required.', 1;
    END;

    IF @start_time >= @end_time
    BEGIN
        THROW 52702, '@start_time must be earlier than @end_time.', 1;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_FACT', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = CONVERT(INT, SCOPE_IDENTITY());

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected, started_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations/budget_allocations',
             N'Charity_DW_DB', N'dw', N'fact_donation_lifecycle',
             N'running',
             0,
             0,
             0,
             0, SYSDATETIME(),
             CONCAT(N'Start first load for dw.fact_donation_lifecycle. Period: [',
                    CONVERT(NVARCHAR(30), @start_time, 126), N', ',
                    CONVERT(NVARCHAR(30), @end_time, 126), N').'));

        SET @main_log_id = SCOPE_IDENTITY();

        /*---------------------------------------------------------------------
          Dimension lookup temp tables.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_date
        (
              date_value DATE NOT NULL PRIMARY KEY,
              date_key   INT  NOT NULL
        );

        IF COL_LENGTH(N'dw.dim_date', N'FullDateAlternateKey') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'TimeKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDateAlternateKey AS DATE), MIN(TimeKey)
                FROM dw.dim_date
                WHERE FullDateAlternateKey IS NOT NULL
                  AND ISNULL(TimeKey, -1) <> -1
                GROUP BY CAST(FullDateAlternateKey AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'full_date') IS NOT NULL
           AND COL_LENGTH(N'dw.dim_date', N'date_key') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(full_date AS DATE), MIN(date_key)
                FROM dw.dim_date
                WHERE full_date IS NOT NULL
                  AND ISNULL(date_key, -1) <> -1
                GROUP BY CAST(full_date AS DATE);';
        END
        ELSE IF COL_LENGTH(N'dw.dim_date', N'FullDate') IS NOT NULL
             AND COL_LENGTH(N'dw.dim_date', N'DateKey') IS NOT NULL
        BEGIN
            SET @date_lookup_sql = N'
                INSERT INTO #dim_date (date_value, date_key)
                SELECT CAST(FullDate AS DATE), MIN(DateKey)
                FROM dw.dim_date
                WHERE FullDate IS NOT NULL
                  AND ISNULL(DateKey, -1) <> -1
                GROUP BY CAST(FullDate AS DATE);';
        END
        ELSE
        BEGIN
            THROW 52703, 'Cannot resolve dw.dim_date columns. Expected (TimeKey, FullDateAlternateKey), (date_key, full_date), or (DateKey, FullDate).', 1;
        END;

        EXEC sys.sp_executesql @date_lookup_sql;
        SET @rows_lookup = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_date',
             N'tempdb', N'#', N'#dim_date',
             N'succeeded', @rows_lookup, @rows_lookup,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Loaded date lookup temp table.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_donor
        (
              donor_id  INT NOT NULL PRIMARY KEY,
              donor_key INT NOT NULL
        );

        INSERT INTO #dim_donor (donor_id, donor_key)
        SELECT donor_id, MIN(donor_key)
        FROM dw.dim_donor
        WHERE donor_id IS NOT NULL
          AND donor_id <> -1
        GROUP BY donor_id;

        SET @rows_lookup = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_donor',
             N'tempdb', N'#', N'#dim_donor',
             N'succeeded', @rows_lookup, @rows_lookup,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Loaded donor lookup temp table.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_campaign
        (
              campaign_id  INT NOT NULL PRIMARY KEY,
              campaign_key INT NOT NULL
        );

        INSERT INTO #dim_campaign (campaign_id, campaign_key)
        SELECT campaign_id, MIN(campaign_key)
        FROM dw.dim_campaign
        WHERE campaign_id IS NOT NULL
          AND campaign_id <> -1
        GROUP BY campaign_id;

        SET @rows_lookup = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_campaign',
             N'tempdb', N'#', N'#dim_campaign',
             N'succeeded', @rows_lookup, @rows_lookup,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Loaded campaign lookup temp table.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #dim_status
        (
              status_code NVARCHAR(50) NOT NULL PRIMARY KEY,
              status_key  INT NOT NULL
        );

        INSERT INTO #dim_status (status_code, status_key)
        SELECT LOWER(LTRIM(RTRIM(code))) AS status_code,
               MIN(status_key) AS status_key
        FROM dw.dim_status
        WHERE status_type = N'donation'
          AND code IS NOT NULL
          AND status_key <> -1
        GROUP BY LOWER(LTRIM(RTRIM(code)));

        SET @rows_lookup = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'dim_status',
             N'tempdb', N'#', N'#dim_status',
             N'succeeded', @rows_lookup, @rows_lookup,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Loaded donation-status lookup temp table.');

        /*---------------------------------------------------------------------
          Build affected donation ids day by day.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #affected_donation
        (
              source_donation_id BIGINT NOT NULL PRIMARY KEY
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations/budget_allocations',
             N'tempdb', N'#', N'#affected_donation',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(), N'Created temp table for affected donation ids.');

        SET @current_from = @start_time;

        WHILE @current_from < @end_time
        BEGIN
            SET @current_to = DATEADD(DAY, 1, @current_from);
            IF @current_to > @end_time SET @current_to = @end_time;

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #affected_donation (source_donation_id)
            SELECT DISTINCT CONVERT(BIGINT, d.id)
            FROM Stg_FinanceOps_DB.stg_finance_ops.donations d
            WHERE d.is_valid = 1
              AND d.id IS NOT NULL
              AND (
                    (d.donation_date >= CAST(@current_from AS DATE) AND d.donation_date < CAST(@current_to AS DATE))
                 OR (d.created_at >= @current_from AND d.created_at < @current_to)
                 OR (d.updated_at >= @current_from AND d.updated_at < @current_to)
                 OR (d.source_updated_at >= @current_from AND d.source_updated_at < @current_to)
                 OR (d.extracted_at >= @current_from AND d.extracted_at < @current_to)
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM #affected_donation a
                    WHERE a.source_donation_id = CONVERT(BIGINT, d.id)
              );

            SET @rows_loop_inserted = @@ROWCOUNT;
            SET @rows_loop_total += @rows_loop_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
                 N'tempdb', N'#', N'#affected_donation',
                 N'succeeded', @rows_loop_inserted, @rows_loop_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Added affected donations from donations for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @step_started_at = SYSDATETIME();

            INSERT INTO #affected_donation (source_donation_id)
            SELECT DISTINCT CONVERT(BIGINT, ba.source_id)
            FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations ba
            WHERE ba.is_valid = 1
              AND LOWER(LTRIM(RTRIM(ISNULL(ba.source_type, N'')))) = N'donation'
              AND ba.source_id IS NOT NULL
              AND (
                    (ba.allocation_date >= CAST(@current_from AS DATE) AND ba.allocation_date < CAST(@current_to AS DATE))
                 OR (ba.created_at >= @current_from AND ba.created_at < @current_to)
                 OR (ba.source_updated_at >= @current_from AND ba.source_updated_at < @current_to)
                 OR (ba.extracted_at >= @current_from AND ba.extracted_at < @current_to)
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM #affected_donation a
                    WHERE a.source_donation_id = CONVERT(BIGINT, ba.source_id)
              );

            SET @rows_loop_alloc_inserted = @@ROWCOUNT;
            SET @rows_loop_total += @rows_loop_alloc_inserted;

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
                 N'tempdb', N'#', N'#affected_donation',
                 N'succeeded', @rows_loop_alloc_inserted, @rows_loop_alloc_inserted,
                 0, 0,
                 @step_started_at, SYSDATETIME(),
                 CONCAT(N'Added affected donations from donation allocations for period [',
                        CONVERT(NVARCHAR(30), @current_from, 126), N', ',
                        CONVERT(NVARCHAR(30), @current_to, 126), N').'));

            SET @current_from = @current_to;
        END;

        /*---------------------------------------------------------------------
          Latest valid donation row per affected source donation id.
          No window functions: use GROUP BY and MAX(stg_row_id).
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #latest_donation_stg
        (
              source_donation_id BIGINT NOT NULL PRIMARY KEY,
              max_stg_row_id     BIGINT NOT NULL
        );

        INSERT INTO #latest_donation_stg (source_donation_id, max_stg_row_id)
        SELECT CONVERT(BIGINT, d.id), MAX(d.stg_row_id)
        FROM Stg_FinanceOps_DB.stg_finance_ops.donations d
        JOIN #affected_donation a
          ON a.source_donation_id = CONVERT(BIGINT, d.id)
        WHERE d.is_valid = 1
          AND d.id IS NOT NULL
        GROUP BY CONVERT(BIGINT, d.id);

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations',
             N'tempdb', N'#', N'#latest_donation_stg',
             N'succeeded', @rows_read, @rows_read,
             0, 0,
             @step_started_at, SYSDATETIME(),
             N'Selected latest valid staging donation row for each affected donation.');

        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #allocation_first
        (
              source_donation_id BIGINT NOT NULL PRIMARY KEY,
              allocated_date     DATE NULL
        );

        INSERT INTO #allocation_first (source_donation_id, allocated_date)
        SELECT CONVERT(BIGINT, ba.source_id) AS source_donation_id,
               MIN(ba.allocation_date) AS allocated_date
        FROM Stg_FinanceOps_DB.stg_finance_ops.budget_allocations ba
        JOIN #affected_donation a
          ON a.source_donation_id = CONVERT(BIGINT, ba.source_id)
        WHERE ba.is_valid = 1
          AND LOWER(LTRIM(RTRIM(ISNULL(ba.source_type, N'')))) = N'donation'
          AND ba.source_id IS NOT NULL
          AND ba.allocation_date IS NOT NULL
        GROUP BY CONVERT(BIGINT, ba.source_id);

        SET @rows_read = @@ROWCOUNT;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'budget_allocations',
             N'tempdb', N'#', N'#allocation_first',
             N'succeeded', @rows_read, @rows_read,
             0, 0,
             @step_started_at, SYSDATETIME(),
             N'Calculated first allocation date for each affected donation using all available staging allocation rows.');

        /*---------------------------------------------------------------------
          Build lifecycle work table.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        CREATE TABLE #work_lifecycle
        (
              source_donation_id   BIGINT NOT NULL PRIMARY KEY,
              source_donor_id      BIGINT NULL,
              source_campaign_id   BIGINT NULL,
              donor_key            INT NOT NULL,
              campaign_key         INT NOT NULL,
              created_date         DATE NULL,
              confirmed_date       DATE NULL,
              allocated_date       DATE NULL,
              created_date_key     INT NOT NULL,
              confirmed_date_key   INT NOT NULL,
              allocated_date_key   INT NOT NULL,
              lifecycle_status_key INT NOT NULL,
              current_stage        NVARCHAR(50) NULL,
              donation_amount      DECIMAL(18,2) NULL,
              days_to_confirm      INT NULL,
              days_to_allocate     INT NULL,
              source_system        NVARCHAR(100) NULL
        );

        INSERT INTO #work_lifecycle
        (
              source_donation_id, source_donor_id, source_campaign_id,
              donor_key, campaign_key,
              created_date, confirmed_date, allocated_date,
              created_date_key, confirmed_date_key, allocated_date_key,
              lifecycle_status_key, current_stage, donation_amount,
              days_to_confirm, days_to_allocate, source_system
        )
        SELECT
              CONVERT(BIGINT, d.id) AS source_donation_id,
              CONVERT(BIGINT, d.donor_id) AS source_donor_id,
              CONVERT(BIGINT, d.campaign_id) AS source_campaign_id,
              ISNULL(dd.donor_key, -1) AS donor_key,
              ISNULL(dc.campaign_key, -1) AS campaign_key,
              CAST(COALESCE(d.created_at, CONVERT(DATETIME2(0), d.donation_date)) AS DATE) AS created_date,
              CASE
                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) IN (N'confirmed', N'refunded')
                      THEN d.donation_date
                  ELSE NULL
              END AS confirmed_date,
              af.allocated_date,
              ISNULL(cd.date_key, -1) AS created_date_key,
              ISNULL(cfd.date_key, -1) AS confirmed_date_key,
              ISNULL(ad.date_key, -1) AS allocated_date_key,
              ISNULL(ds.status_key, -1) AS lifecycle_status_key,
              CASE
                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) = N'refunded' THEN N'refunded'
                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) = N'rejected' THEN N'rejected'
                  WHEN af.allocated_date IS NOT NULL THEN N'allocated'
                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) = N'confirmed' THEN N'confirmed'
                  ELSE N'created'
              END AS current_stage,
              d.amount AS donation_amount,
              CASE
                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) IN (N'confirmed', N'refunded')
                   AND d.donation_date IS NOT NULL
                   AND COALESCE(d.created_at, CONVERT(DATETIME2(0), d.donation_date)) IS NOT NULL
                      THEN CASE
                               WHEN DATEDIFF(DAY, CAST(COALESCE(d.created_at, CONVERT(DATETIME2(0), d.donation_date)) AS DATE), d.donation_date) < 0
                                   THEN 0
                               ELSE DATEDIFF(DAY, CAST(COALESCE(d.created_at, CONVERT(DATETIME2(0), d.donation_date)) AS DATE), d.donation_date)
                           END
                  ELSE NULL
              END AS days_to_confirm,
              CASE
                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) IN (N'confirmed', N'refunded')
                   AND d.donation_date IS NOT NULL
                   AND af.allocated_date IS NOT NULL
                      THEN CASE
                               WHEN DATEDIFF(DAY, d.donation_date, af.allocated_date) < 0
                                   THEN 0
                               ELSE DATEDIFF(DAY, d.donation_date, af.allocated_date)
                           END
                  ELSE NULL
              END AS days_to_allocate,
              d.source_system
        FROM #latest_donation_stg l
        JOIN Stg_FinanceOps_DB.stg_finance_ops.donations d
          ON d.stg_row_id = l.max_stg_row_id
        LEFT JOIN #allocation_first af
          ON af.source_donation_id = CONVERT(BIGINT, d.id)
        LEFT JOIN #dim_donor dd
          ON dd.donor_id = d.donor_id
        LEFT JOIN #dim_campaign dc
          ON dc.campaign_id = d.campaign_id
        LEFT JOIN #dim_date cd
          ON cd.date_value = CAST(COALESCE(d.created_at, CONVERT(DATETIME2(0), d.donation_date)) AS DATE)
        LEFT JOIN #dim_date cfd
          ON cfd.date_value = CASE
                                  WHEN LOWER(LTRIM(RTRIM(ISNULL(d.status, N'')))) IN (N'confirmed', N'refunded')
                                      THEN d.donation_date
                                  ELSE NULL
                              END
        LEFT JOIN #dim_date ad
          ON ad.date_value = af.allocated_date
        LEFT JOIN #dim_status ds
          ON ds.status_code = LOWER(LTRIM(RTRIM(ISNULL(d.status, N''))));

        SET @rows_work_inserted = @@ROWCOUNT;

        SELECT @rows_rejected = COUNT(*)
        FROM #affected_donation a
        WHERE NOT EXISTS (
            SELECT 1
            FROM #work_lifecycle w
            WHERE w.source_donation_id = a.source_donation_id
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'donations/budget_allocations',
             N'tempdb', N'#', N'#work_lifecycle',
             N'succeeded', @rows_loop_total, @rows_work_inserted,
             0, @rows_rejected,
             @step_started_at, SYSDATETIME(),
             N'Built complete lifecycle work table for affected donations. Rejected means affected id had no valid latest donation row.');

        /*---------------------------------------------------------------------
          Full load table reset and insert.
        ---------------------------------------------------------------------*/
        SET @step_started_at = SYSDATETIME();

        BEGIN TRANSACTION;

            TRUNCATE TABLE dw.fact_donation_lifecycle;
            SET @rows_deleted = @@ROWCOUNT;

            DBCC CHECKIDENT (N'dw.fact_donation_lifecycle', RESEED, 0) WITH NO_INFOMSGS;

        COMMIT TRANSACTION;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'dw', N'fact_donation_lifecycle',
             N'Charity_DW_DB', N'dw', N'fact_donation_lifecycle',
             N'succeeded', 0, 0,
             0, 0,
             @step_started_at, SYSDATETIME(),
             N'Truncated fact table and reset identity seed to 0 for first load.');

        SET @step_started_at = SYSDATETIME();

        BEGIN TRANSACTION;

            INSERT INTO dw.fact_donation_lifecycle
            (
                  donor_key, campaign_key,
                  created_date_key, confirmed_date_key, allocated_date_key,
                  lifecycle_status_key, current_stage, donation_amount,
                  days_to_confirm, days_to_allocate,
                  source_donation_id, source_donor_id, source_campaign_id,
                  source_system, etl_batch_id, loaded_at
            )
            SELECT
                  donor_key,
                  campaign_key,
                  created_date_key,
                  confirmed_date_key,
                  allocated_date_key,
                  lifecycle_status_key,
                  current_stage,
                  donation_amount,
                  days_to_confirm,
                  days_to_allocate,
                  source_donation_id,
                  source_donor_id,
                  source_campaign_id,
                  ISNULL(source_system, N'FINANCE_OPS'),
                  @etl_batch_id,
                  SYSDATETIME()
            FROM #work_lifecycle;

            SET @rows_fact_inserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'tempdb', N'#', N'#work_lifecycle',
             N'Charity_DW_DB', N'dw', N'fact_donation_lifecycle',
             N'succeeded', @rows_work_inserted, @rows_fact_inserted,
             0, 0,
             @step_started_at, SYSDATETIME(),
             N'Inserted lifecycle rows into dw.fact_donation_lifecycle.');

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
        SET load_status = N'succeeded',
            rows_read = @rows_loop_total,
            rows_inserted = @rows_fact_inserted,
            rows_rejected = @rows_rejected,
            ended_at = SYSDATETIME(),
            message = CONCAT(N'Finished first load for dw.fact_donation_lifecycle. Inserted rows: ', @rows_fact_inserted,
                             N'. Affected donation ids: ', @rows_loop_total,
                             N'. Rejected affected ids: ', @rows_rejected, N'.')
        WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @error_message = ERROR_MESSAGE();

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
            SET load_status = N'failed',
                ended_at = SYSDATETIME(),
                message = CONCAT(N'First load failed for dw.fact_donation_lifecycle. Error: ', @error_message)
            WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO


/*
===============================================================================
 Project      : Charity Data Warehouse Project
 Phase        : MART 2 - Finance DW ETL Orchestration
 File         : 25_create_dw_finance_mart2_orchestration_procedures.sql
 DBMS         : Microsoft SQL Server

 Purpose:
   Create two orchestration procedures that execute all Finance MART 2
   dimension and fact ETL procedures in the correct dependency order.

 Procedures:
   1. etl_admin.usp_first_load_dw_finance_mart2_all
      - One-time first/full load.
      - Optionally runs source-to-staging Finance ETL first.
      - Runs all DW first-load procedures in dependency order.

   2. etl_admin.usp_load_dw_finance_mart2_daily
      - Normal/daily job procedure.
      - Optionally runs source-to-staging Finance ETL first.
      - Runs all DW incremental procedures in dependency order.

 Dependency order:
   Optional staging refresh
   1. dim_donor
   2. dim_campaign
   3. dim_category
   4. dim_donation_type
   5. dim_status
   6. dim_currency
   7. dim_allocation_type
   8. fact_donation_transaction
   9. fact_budget_allocation_event
  10. fact_monthly_financial_snapshot
  11. fact_donation_lifecycle

 Design rules:
   - Each procedure accepts @start_time and @end_time.
   - Period is half-open: [@start_time, @end_time).
   - Each orchestration step is logged in:
       Charity_DW_DB.etl_admin.etl_load_log
   - The master orchestration batch is logged in:
       Charity_DW_DB.etl_admin.etl_batch
   - Child procedures also create their own detailed logs.
   - No MERGE.
   - No window functions.
   - No single long transaction around the whole pipeline; each child procedure
     manages its own transaction. This is safer for practical daily jobs.
   - Identity/sequence reset is handled inside each child first-load/incremental
     procedure, according to the table it loads.
===============================================================================
*/


USE Charity_DW_DB;
GO

/*=============================================================================
  Procedure 1: One-Time First Load for Finance MART 2
=============================================================================*/
CREATE OR ALTER PROCEDURE etl_admin.usp_first_load_dw_finance_mart2_all
      @start_time  DATETIME2(0),
      @end_time    DATETIME2(0),
      @run_staging BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
          @etl_batch_id INT,
          @main_log_id BIGINT,
          @step_log_id BIGINT,
          @step_started_at DATETIME2(0),
          @step_id INT,
          @max_step_id INT,
          @step_name NVARCHAR(200),
          @procedure_name SYSNAME,
          @database_name SYSNAME,
          @parameter_style NVARCHAR(30),
          @sql NVARCHAR(MAX),
          @rows_steps_inserted INT = 0,
          @error_message NVARCHAR(MAX);

    IF @start_time IS NULL OR @end_time IS NULL
    BEGIN
        RAISERROR('@start_time and @end_time are required.', 16, 1);
        RETURN;
    END;

    IF @start_time >= @end_time
    BEGIN
        RAISERROR('@start_time must be earlier than @end_time.', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_batch', N'U') IS NULL
    BEGIN
        RAISERROR('Required table Charity_DW_DB.etl_admin.etl_batch does not exist.', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID(N'Charity_DW_DB.etl_admin.etl_load_log', N'U') IS NULL
    BEGIN
        RAISERROR('Required table Charity_DW_DB.etl_admin.etl_load_log does not exist.', 16, 1);
        RETURN;
    END;

    BEGIN TRY
        INSERT INTO Charity_DW_DB.etl_admin.etl_batch
            (source_system, target_layer, batch_status, started_at, rows_read, rows_inserted, rows_updated, rows_rejected, created_by)
        VALUES
            (N'FINANCE_OPS', N'DW_MART2_FIRST_LOAD_ORCHESTRATION', N'running', SYSDATETIME(), 0, 0, 0, 0, COALESCE(SUSER_SNAME(), ORIGINAL_LOGIN(), N'DW_ETL'));

        SET @etl_batch_id = SCOPE_IDENTITY();

        SET @step_started_at = SYSDATETIME();

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Stg_FinanceOps_DB', N'stg_finance_ops', N'orchestration',
             N'Charity_DW_DB', N'etl_admin', N'usp_first_load_dw_finance_mart2_all',
             N'running', 0, 0,
             0, 0,
             @step_started_at, NULL,
             CONCAT(N'Finance MART 2 first-load orchestration started. Period: ',
                    CONVERT(NVARCHAR(30), @start_time, 126), N' to ',
                    CONVERT(NVARCHAR(30), @end_time, 126),
                    N'. run_staging=', CONVERT(NVARCHAR(10), @run_staging), N'.'));

        SET @main_log_id = SCOPE_IDENTITY();

        CREATE TABLE #JobSteps
        (
              step_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
              step_name NVARCHAR(200) NOT NULL,
              database_name SYSNAME NOT NULL,
              procedure_name SYSNAME NOT NULL,
              parameter_style NVARCHAR(30) NOT NULL
        );

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'tempdb', N'#JobSteps',
             N'Charity_DW_DB', N'etl_admin', N'usp_first_load_dw_finance_mart2_all',
             N'succeeded', 0, 1,
             0, 0,
             SYSDATETIME(), SYSDATETIME(),
             N'Temp table #JobSteps created for first-load orchestration.');

        IF @run_staging = 1
        BEGIN
            INSERT INTO #JobSteps (step_name, database_name, procedure_name, parameter_style)
            VALUES
                (N'00 - Refresh source Finance data into staging up to @end_time',
                 N'Stg_FinanceOps_DB', N'usp_run_stg_finance_ops_all', N'to_date');
        END;

        INSERT INTO #JobSteps (step_name, database_name, procedure_name, parameter_style)
        VALUES
            (N'01 - First load dw.dim_donor',                  N'Charity_DW_DB', N'usp_first_load_dw_dim_donor', N'start_end'),
            (N'02 - First load dw.dim_campaign',               N'Charity_DW_DB', N'usp_first_load_dw_dim_campaign', N'start_end'),
            (N'03 - First load dw.dim_category',               N'Charity_DW_DB', N'usp_first_load_dw_dim_category', N'start_end'),
            (N'04 - First load dw.dim_donation_type',          N'Charity_DW_DB', N'usp_first_load_dw_dim_donation_type', N'start_end'),
            (N'05 - First load dw.dim_status',                 N'Charity_DW_DB', N'usp_first_load_dw_dim_status', N'start_end'),
            (N'06 - First load dw.dim_currency',               N'Charity_DW_DB', N'usp_first_load_dw_dim_currency', N'start_end'),
            (N'07 - First load dw.dim_allocation_type',        N'Charity_DW_DB', N'usp_first_load_dw_dim_allocation_type', N'start_end'),
            (N'08 - First load dw.fact_donation_transaction',  N'Charity_DW_DB', N'usp_first_load_dw_fact_donation_transaction', N'start_end'),
            (N'09 - First load dw.fact_budget_allocation_event', N'Charity_DW_DB', N'usp_first_load_dw_fact_budget_allocation_event', N'start_end'),
            (N'10 - First load dw.fact_monthly_financial_snapshot', N'Charity_DW_DB', N'usp_first_load_dw_fact_monthly_financial_snapshot', N'start_end'),
            (N'11 - First load dw.fact_donation_lifecycle',    N'Charity_DW_DB', N'usp_first_load_dw_fact_donation_lifecycle', N'start_end');

        SET @rows_steps_inserted = @@ROWCOUNT + CASE WHEN @run_staging = 1 THEN 1 ELSE 0 END;

        INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
            (etl_batch_id, source_database, source_schema, source_table,
             target_database, target_schema, target_table,
             load_status,
             rows_read, rows_inserted, rows_updated, rows_rejected,
             started_at, ended_at, message)
        VALUES
            (@etl_batch_id, N'Charity_DW_DB', N'tempdb', N'#JobSteps',
             N'Charity_DW_DB', N'etl_admin', N'usp_first_load_dw_finance_mart2_all',
             N'succeeded', 0, @rows_steps_inserted,
             0, 0,
             SYSDATETIME(), SYSDATETIME(),
             N'Execution steps inserted into #JobSteps in dependency order.');

        SELECT @step_id = MIN(step_id), @max_step_id = MAX(step_id)
        FROM #JobSteps;

        WHILE @step_id IS NOT NULL AND @step_id <= @max_step_id
        BEGIN
            SELECT
                  @step_name = step_name,
                  @database_name = database_name,
                  @procedure_name = procedure_name,
                  @parameter_style = parameter_style
            FROM #JobSteps
            WHERE step_id = @step_id;

            SET @step_started_at = SYSDATETIME();

            INSERT INTO Charity_DW_DB.etl_admin.etl_load_log
                (etl_batch_id, source_database, source_schema, source_table,
                 target_database, target_schema, target_table,
                 load_status,
                 rows_read, rows_inserted, rows_updated, rows_rejected,
                 started_at, ended_at, message)
            VALUES
                (@etl_batch_id, N'ORCHESTRATOR', N'etl_admin', N'usp_first_load_dw_finance_mart2_all',
                 @database_name, N'etl_admin', @procedure_name,
                 N'running', 0, 0,
                 0, 0,
                 @step_started_at, NULL,
                 CONCAT(N'Starting step ', CONVERT(NVARCHAR(20), @step_id), N': ', @step_name));

            SET @step_log_id = SCOPE_IDENTITY();

            IF @parameter_style = N'to_date'
            BEGIN
                SET @sql = N'EXEC ' + QUOTENAME(@database_name) + N'.etl_admin.' + QUOTENAME(@procedure_name) +
                           N' @to_date = @p_end_time;';

                EXEC sys.sp_executesql
                    @sql,
                    N'@p_end_time DATETIME2(0)',
                    @p_end_time = @end_time;
            END
            ELSE
            BEGIN
                SET @sql = N'EXEC ' + QUOTENAME(@database_name) + N'.etl_admin.' + QUOTENAME(@procedure_name) +
                           N' @start_time = @p_start_time, @end_time = @p_end_time;';

                EXEC sys.sp_executesql
                    @sql,
                    N'@p_start_time DATETIME2(0), @p_end_time DATETIME2(0)',
                    @p_start_time = @start_time,
                    @p_end_time = @end_time;
            END;

            UPDATE Charity_DW_DB.etl_admin.etl_load_log
            SET
                load_status = N'succeeded',
                rows_read = 1,
                rows_inserted = 1,
                rows_rejected = 0,
                ended_at = SYSDATETIME(),
                message = CONCAT(N'Succeeded step ', CONVERT(NVARCHAR(20), @step_id), N': ', @step_name)
            WHERE etl_load_log_id = @step_log_id;

            SET @step_id = @step_id + 1;
        END;

        UPDATE Charity_DW_DB.etl_admin.etl_load_log
        SET
            load_status = N'succeeded',
            rows_read = @rows_steps_inserted,
            rows_inserted = @rows_steps_inserted,
            rows_rejected = 0,
            ended_at = SYSDATETIME(),
            message = CONCAT(N'Finance MART 2 first-load orchestration succeeded. Executed steps: ',
                             CONVERT(NVARCHAR(20), @rows_steps_inserted), N'.')
        WHERE etl_load_log_id = @main_log_id;

        UPDATE Charity_DW_DB.etl_admin.etl_batch
           SET batch_status  = N'succeeded',
               ended_at      = SYSDATETIME(),
               rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                        FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
               error_message = NULL
         WHERE etl_batch_id = @etl_batch_id;
    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        IF @step_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
            SET
                load_status = N'failed',
                ended_at = SYSDATETIME(),
                message = CONCAT(N'Failed orchestration step. Error: ', @error_message)
            WHERE etl_load_log_id = @step_log_id
              AND load_status = N'running';
        END;

        IF @main_log_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_load_log
            SET
                load_status = N'failed',
                ended_at = SYSDATETIME(),
                message = CONCAT(N'Finance MART 2 first-load orchestration failed. Error: ', @error_message)
            WHERE etl_load_log_id = @main_log_id;
        END;

        IF @etl_batch_id IS NOT NULL
        BEGIN
            UPDATE Charity_DW_DB.etl_admin.etl_batch
               SET batch_status  = N'failed',
                   ended_at      = SYSDATETIME(),
                   rows_read     = ISNULL((SELECT SUM(ISNULL(rows_read, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_inserted = ISNULL((SELECT SUM(ISNULL(rows_inserted, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_updated  = ISNULL((SELECT SUM(ISNULL(rows_updated, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   rows_rejected = ISNULL((SELECT SUM(ISNULL(rows_rejected, 0))
                                            FROM Charity_DW_DB.etl_admin.etl_load_log
                                        WHERE etl_load_log_id = @main_log_id), 0),
                   error_message = @error_message
             WHERE etl_batch_id = @etl_batch_id;
        END;

        THROW;
    END CATCH;
END;
GO



