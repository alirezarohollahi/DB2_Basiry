/*=============================================================================
  Example execution
===============================================================================

-- First load for a historical period:
EXEC Charity_DW_DB.etl_admin.usp_first_load_dw_dim_donor
     @start_time = '2025-01-01T00:00:00',
     @end_time   = '2026-01-01T00:00:00';

-- Incremental daily load:
EXEC Charity_DW_DB.etl_admin.usp_load_dw_dim_donor_incremental
     @start_time = '2026-01-01T00:00:00',
     @end_time   = '2026-01-02T00:00:00';

-- Review logs:
SELECT TOP (100) *
FROM Stg_FinanceOps_DB.etl_admin.etl_load_log
WHERE target_database = N'Charity_DW_DB'
  AND target_schema = N'dw'
  AND target_table = N'dim_donor'
ORDER BY etl_load_log_id DESC;
=============================================================================*/



/*=============================================================================
  Example usage
=============================================================================

EXEC Charity_DW_DB.etl_admin.usp_first_load_dw_dim_campaign
     @start_time = '2025-01-01T00:00:00',
     @end_time   = '2026-01-01T00:00:00';

EXEC Charity_DW_DB.etl_admin.usp_load_dw_dim_campaign_incremental
     @start_time = '2026-01-01T00:00:00',
     @end_time   = '2026-01-02T00:00:00';

SELECT TOP (100) *
FROM Stg_FinanceOps_DB.etl_admin.etl_load_log
WHERE target_database = N'Charity_DW_DB'
  AND target_schema = N'dw'
  AND target_table = N'dim_campaign'
ORDER BY etl_load_log_id DESC;

=============================================================================*/




/*=============================================================================
  Example calls
=============================================================================

-- One-time first load:
EXEC Charity_DW_DB.etl_admin.usp_first_load_dw_finance_mart2_all
     @start_time  = '2025-01-01T00:00:00',
     @end_time    = '2026-01-01T00:00:00',
     @run_staging = 1;

-- Daily SQL Agent job example for yesterday -> today:
DECLARE @job_end_time DATETIME2(0) = CONVERT(DATETIME2(0), CONVERT(DATE, SYSDATETIME()));
DECLARE @job_start_time DATETIME2(0) = DATEADD(DAY, -1, @job_end_time);

EXEC Charity_DW_DB.etl_admin.usp_load_dw_finance_mart2_daily
     @start_time  = @job_start_time,
     @end_time    = @job_end_time,
     @run_staging = 1;

-- Log check:
SELECT TOP (200) *
FROM Stg_FinanceOps_DB.etl_admin.etl_load_log
WHERE target_database IN (N'Charity_DW_DB', N'Stg_FinanceOps_DB')
ORDER BY etl_load_log_id DESC;

=============================================================================*/
-- --- first load
-- declare @start_time DATETIME2(0);
-- declare @end_time DATETIME2(0);
-- set @start_time = getdate()-360;
-- set @end_time = getdate()+360;
-- EXEC Charity_DW_DB.etl_admin.usp_first_load_dw_finance_mart2_all
--      @start_time,
--      @end_time ;
--- asyn varibles 
declare @job_start_time DATETIME2(0);
declare @job_end_time DATETIME2(0);
set @job_start_time = getdate()-720;
set @job_end_time = getdate()-360;
EXEC Charity_DW_DB.etl_admin.usp_load_dw_finance_mart2_daily
     @start_time  = @job_start_time,
     @end_time    = @job_end_time;