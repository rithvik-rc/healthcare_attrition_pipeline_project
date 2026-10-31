-- =====================================================================
-- SQL Server Agent job: run the attrition pipeline daily at 02:00.
-- Needs SQL Server Agent (not included in Express: use Task Scheduler instead,
-- see run_attrition_pipeline.bat). Run once; safe to rerun (it recreates the job).
--
-- Edit @BatPath to where run_attrition_pipeline.bat lives. The Agent service account
-- needs read/write on that folder and Windows login access to the database.
-- A non-zero exit code (1 audit failed, 2 error, 3 locked) marks the job step as failed.
-- =====================================================================
USE msdb;
GO

DECLARE @JobName NVARCHAR(128) = N'Attrition Pipeline - Daily';
DECLARE @BatPath NVARCHAR(400) = N'C:\attrition\run_attrition_pipeline.bat';   -- <-- edit
DECLARE @Command NVARCHAR(4000) = N'cmd /c ""' + @BatPath + N'""';

IF EXISTS (SELECT 1 FROM dbo.sysjobs WHERE name = @JobName)
    EXEC dbo.sp_delete_job @job_name = @JobName;

EXEC dbo.sp_add_job
    @job_name = @JobName,
    @enabled = 1,
    @description = N'Extract, validate, load, build metrics and audit the attrition warehouse.',
    @notify_level_eventlog = 2;                 -- log failures to the Windows event log

EXEC dbo.sp_add_jobstep
    @job_name = @JobName,
    @step_name = N'Run attrition_pipeline.py',
    @subsystem = N'CmdExec',
    @command = @Command,
    @on_success_action = 1,                     -- quit with success
    @on_fail_action = 2;                        -- quit with failure

EXEC dbo.sp_add_jobschedule
    @job_name = @JobName,
    @name = N'Daily 02:00',
    @freq_type = 4,                             -- daily
    @freq_interval = 1,
    @active_start_time = 020000;

EXEC dbo.sp_add_jobserver @job_name = @JobName, @server_name = N'(local)';
GO

-- Test it:   EXEC msdb.dbo.sp_start_job @job_name = N'Attrition Pipeline - Daily';
-- History:   SELECT * FROM msdb.dbo.sysjobhistory WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Attrition Pipeline - Daily');
