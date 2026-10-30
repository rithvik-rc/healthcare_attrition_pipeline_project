@echo off
REM =====================================================================
REM Scheduler wrapper for the attrition pipeline.
REM   * Runs from this file's folder so relative paths (output\, logs\) work.
REM   * Passes the pipeline's exit code back to the scheduler:
REM       0 ok | 1 audit failed | 2 pipeline error | 3 another run in progress
REM
REM Task Scheduler (works on every SQL Server edition, including Express):
REM   schtasks /Create /SC DAILY /ST 02:00 /TN "AttritionPipeline" ^
REM            /TR "\"C:\attrition\run_attrition_pipeline.bat\"" /RU SYSTEM
REM   (use /RU with the Windows account that has access to SQL Server if SYSTEM does not)
REM SQL Server Agent (not available in Express): see schedule_attrition_agent_job.sql
REM =====================================================================
setlocal
cd /d "%~dp0"

REM Uncomment to point at a named instance or a different ODBC driver:
REM set ATTRITION_SQL_SERVER=localhost\SQLEXPRESS
REM set ATTRITION_SQL_DRIVER=ODBC Driver 18 for SQL Server

python attrition_pipeline.py --input-dir output --log-dir logs %*
exit /b %ERRORLEVEL%
