-- =====================================================================
-- Phase 5: operations - run log, audit checks, housekeeping
-- Run after attrition_metrics.sql and attrition_reporting_views.sql
-- (deploy_attrition.sql does this).
--
--   dbo.PipelineRunLog         one row per pipeline run: status, row counts, churn, error
--   dbo.PipelineAuditResult    one row per audit check per run: PASS / WARN / FAIL
--   dbo.usp_RunAttritionAudits @RunID, @BatchID   data-side audits for a run (atomic, re-runnable)
--   dbo.vw_PipelineRunHistory  run history for an operations dashboard
--   dbo.usp_CloseStaleRuns     marks runs that died while 'Running' as Failed
--   dbo.usp_PurgePipelineLogs  deletes run history older than the retention period
--
-- Contract with attrition_pipeline.py (do not change without changing it):
--   PipelineRunLog columns and statuses, PipelineAuditResult(RunID, CheckName, Status, Detail),
--   usp_RunAttritionAudits returns (CheckName, Status, Detail) for the checks IT wrote.
--   The Python side adds its own 'Quarantine rate' audit row; the procedure leaves it alone.
--
-- Severity
--   FAIL  the load is wrong (rows missing, DQ rules broken, features out of sync, impossible retention)
--   WARN  the load ran but looks unusual (incomplete window, churn outside the band, no claims)
-- =====================================================================

IF DB_ID('WarehouseDB') IS NULL
    CREATE DATABASE WarehouseDB;
GO

USE WarehouseDB;
GO

-- Filtered index on FactEnrollment: any procedure that writes to it must be created with these ON
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.AttritionConfig', 'U')    IS NULL
   OR OBJECT_ID('dbo.MemberMonthFeatures', 'U') IS NULL
   OR OBJECT_ID('dbo.CohortRetention', 'U')     IS NULL
   OR OBJECT_ID('dbo.vw_DQ_Checks', 'V')        IS NULL
   OR OBJECT_ID('dbo.vw_MonthlyChurn', 'V')     IS NULL
   OR OBJECT_ID('dbo.vw_ObservationWindow', 'V') IS NULL
   OR OBJECT_ID('dbo.stg_members', 'U')         IS NULL
    THROW 50030, 'Earlier phases not found. Run deploy_attrition.sql (or the Phase 1-4 scripts) first.', 1;
GO

-- 1. Thresholds (insert-only; edit the values in dbo.AttritionConfig to tune)
MERGE dbo.AttritionConfig AS tgt
USING (VALUES
    ('MinMonthlyChurnBps', 10,  'Lower bound of the expected monthly churn, in basis points (10 = 0.10%).'),
    ('MaxMonthlyChurnBps', 500, 'Upper bound of the expected monthly churn, in basis points (500 = 5.00%).'),
    ('MaxQuarantinePct',   5,   'Warn when more than this percent of source rows are quarantined in a run.'),
    ('LogRetentionDays',   180, 'Run history older than this is removed by usp_PurgePipelineLogs.')
) AS src (ConfigKey, ConfigValue, Description)
    ON tgt.ConfigKey = src.ConfigKey
WHEN NOT MATCHED THEN
    INSERT (ConfigKey, ConfigValue, Description)
    VALUES (src.ConfigKey, src.ConfigValue, src.Description);
GO

-- 2. Run log
IF OBJECT_ID('dbo.PipelineRunLog', 'U') IS NULL
CREATE TABLE dbo.PipelineRunLog (
    RunID              BIGINT IDENTITY(1,1) PRIMARY KEY,
    BatchID            VARCHAR(36)    NULL,
    StartedAt          DATETIME2      NOT NULL CONSTRAINT DF_PipelineRunLog_Started DEFAULT SYSUTCDATETIME(),
    FinishedAt         DATETIME2      NULL,
    Status             VARCHAR(20)    NOT NULL CONSTRAINT DF_PipelineRunLog_Status DEFAULT 'Running',
    TriggeredBy        VARCHAR(100)   NULL,
    RowsRead           INT            NULL,
    RowsQuarantined    INT            NULL,
    RowsStaged         INT            NULL,
    MemberInserts      INT            NULL,
    MemberUpdates      INT            NULL,
    EnrollmentInserts  INT            NULL,
    EnrollmentUpdates  INT            NULL,
    ClaimInserts       INT            NULL,
    ClaimUpdates       INT            NULL,
    FeatureRows        INT            NULL,
    AnnualChurnRate    DECIMAL(9, 6)  NULL,
    ErrorMessage       NVARCHAR(2000) NULL,
    CONSTRAINT CK_PipelineRunLog_Status
        CHECK (Status IN ('Running', 'Succeeded', 'Warnings', 'Failed'))
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PipelineRunLog_Started' AND object_id = OBJECT_ID('dbo.PipelineRunLog'))
    CREATE INDEX IX_PipelineRunLog_Started ON dbo.PipelineRunLog(StartedAt DESC);
GO

-- 3. Audit results
IF OBJECT_ID('dbo.PipelineAuditResult', 'U') IS NULL
CREATE TABLE dbo.PipelineAuditResult (
    AuditID    BIGINT IDENTITY(1,1) PRIMARY KEY,
    RunID      BIGINT         NOT NULL,
    CheckName  VARCHAR(100)   NOT NULL,
    Status     VARCHAR(4)     NOT NULL,
    Detail     NVARCHAR(500)  NULL,
    CheckedAt  DATETIME2      NOT NULL CONSTRAINT DF_PipelineAuditResult_Checked DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_PipelineAuditResult_Run FOREIGN KEY (RunID) REFERENCES dbo.PipelineRunLog(RunID),
    CONSTRAINT CK_PipelineAuditResult_Status CHECK (Status IN ('PASS', 'WARN', 'FAIL'))
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PipelineAuditResult_Run' AND object_id = OBJECT_ID('dbo.PipelineAuditResult'))
    CREATE INDEX IX_PipelineAuditResult_Run ON dbo.PipelineAuditResult(RunID);
GO

-- 4. Data-side audit checks
--    All checks run in ONE statement (one consistent snapshot) inside ONE transaction, so a run
--    never ends up with a half-written set of results. Re-running replaces the procedure's own
--    rows for that run; the Python 'Quarantine rate' row is left alone.
CREATE OR ALTER PROCEDURE dbo.usp_RunAttritionAudits
    @RunID   BIGINT,
    @BatchID VARCHAR(36)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;   -- roll back the whole transaction automatically on runtime errors

    IF NOT EXISTS (SELECT 1 FROM dbo.PipelineRunLog WHERE RunID = @RunID)
        THROW 50031, 'Unknown RunID: no such row in dbo.PipelineRunLog.', 1;

    DECLARE @MinBps INT = (SELECT ConfigValue FROM dbo.AttritionConfig WHERE ConfigKey = 'MinMonthlyChurnBps');
    DECLARE @MaxBps INT = (SELECT ConfigValue FROM dbo.AttritionConfig WHERE ConfigKey = 'MaxMonthlyChurnBps');

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE FROM dbo.PipelineAuditResult
        WHERE RunID = @RunID AND CheckName <> 'Quarantine rate';

        -- BEGIN:audits
        INSERT INTO dbo.PipelineAuditResult (RunID, CheckName, Status, Detail)
        SELECT @RunID, c.CheckName, c.Status, c.Detail
        FROM (
            -- 1-3 Row reconciliation: everything staged in this batch must now exist in the warehouse
            SELECT 1 AS Ord, 'Staged members loaded' AS CheckName,
                   CASE WHEN x.n = 0 THEN 'PASS' ELSE 'FAIL' END AS Status,
                   CONCAT(x.n, ' staged member(s) missing from DimMember') AS Detail
            FROM (SELECT COUNT(*) AS n
                  FROM dbo.stg_members s
                  WHERE s.batch_id = @BatchID
                    AND NOT EXISTS (SELECT 1 FROM dbo.DimMember m WHERE m.MemberID = s.member_id)) AS x

            UNION ALL
            SELECT 2, 'Staged enrollments loaded',
                   CASE WHEN x.n = 0 THEN 'PASS' ELSE 'FAIL' END,
                   CONCAT(x.n, ' staged enrollment(s) missing from FactEnrollment')
            FROM (SELECT COUNT(*) AS n
                  FROM dbo.stg_enrollments s
                  WHERE s.batch_id = @BatchID
                    AND NOT EXISTS (SELECT 1
                                    FROM dbo.FactEnrollment f
                                    JOIN dbo.DimMember m ON m.MemberSK = f.MemberSK
                                    WHERE m.MemberID = s.member_id
                                      AND f.DateKey = YEAR(s.month_start) * 10000
                                                    + MONTH(s.month_start) * 100
                                                    + DAY(s.month_start))) AS x

            UNION ALL
            SELECT 3, 'Staged claims loaded',
                   CASE WHEN x.n = 0 THEN 'PASS' ELSE 'FAIL' END,
                   CONCAT(x.n, ' staged claim(s) missing from FactClaims')
            FROM (SELECT COUNT(*) AS n
                  FROM dbo.stg_claims s
                  WHERE s.batch_id = @BatchID
                    AND NOT EXISTS (SELECT 1 FROM dbo.FactClaims fc WHERE fc.ClaimID = s.claim_id)) AS x

            -- 4 Cross-table rules the constraints cannot express (attrition_dq_checks.sql)
            UNION ALL
            SELECT 4, 'Data-quality checks (vw_DQ_Checks)',
                   CASE WHEN x.failing = 0 THEN 'PASS' ELSE 'FAIL' END,
                   CONCAT(x.failing_checks, ' check(s) failing, ', x.failing,
                          ' row(s). Run: SELECT * FROM dbo.vw_DQ_Checks WHERE FailingRows > 0')
            FROM (SELECT ISNULL(SUM(FailingRows), 0) AS failing,
                         COUNT(CASE WHEN FailingRows > 0 THEN 1 END) AS failing_checks
                  FROM dbo.vw_DQ_Checks) AS x

            -- 5-6 Phase 4 output must be in step with the facts it was built from
            UNION ALL
            SELECT 5, 'Features match enrollment facts',
                   CASE WHEN x.fact_rows = x.feature_rows THEN 'PASS' ELSE 'FAIL' END,
                   CONCAT('FactEnrollment ', x.fact_rows, ' rows vs MemberMonthFeatures ', x.feature_rows, ' rows')
            FROM (SELECT (SELECT COUNT(*) FROM dbo.FactEnrollment)      AS fact_rows,
                         (SELECT COUNT(*) FROM dbo.MemberMonthFeatures) AS feature_rows) AS x

            UNION ALL
            SELECT 6, 'Cohort retention never rises with age',
                   CASE WHEN x.n = 0 THEN 'PASS' ELSE 'FAIL' END,
                   CONCAT(x.n, ' cohort row(s) retain more members than the month before')
            FROM (SELECT COUNT(*) AS n
                  FROM (SELECT RetentionRate,
                               LAG(RetentionRate) OVER (PARTITION BY CohortMonth
                                                        ORDER BY MonthsSinceEnrollment) AS PrevRate
                        FROM dbo.CohortRetention) AS y
                  WHERE y.PrevRate IS NOT NULL AND y.RetentionRate > y.PrevRate) AS x

            -- 7 Observation window: all 12 months present?
            UNION ALL
            SELECT 7, 'Observation window complete',
                   CASE WHEN w.IsWindowComplete = 1 THEN 'PASS' ELSE 'WARN' END,
                   CONCAT(ISNULL(w.MonthsInWindow, 0), ' of ', w.WindowMonths, ' months loaded in the window')
            FROM dbo.vw_ObservationWindow w

            -- 8 A sudden 0% or 50% monthly churn usually means a bug, not a business event.
            --   Pooled rate = ratio of sums (never an average of monthly rates), plus any single month outside the band.
            UNION ALL
            SELECT 8, 'Monthly churn within expected band',
                   CASE WHEN r.Rate IS NULL THEN 'WARN'
                        WHEN r.Rate * 10000 BETWEEN @MinBps AND @MaxBps AND r.MonthsOutside = 0 THEN 'PASS'
                        ELSE 'WARN' END,
                   CONCAT('Window monthly churn ', CAST(r.Rate * 100 AS DECIMAL(9, 3)), '% (expected ',
                          CAST(@MinBps / 100.0 AS DECIMAL(9, 2)), '% to ', CAST(@MaxBps / 100.0 AS DECIMAL(9, 2)),
                          '%); ', r.MonthsOutside, ' month(s) outside the band')
            FROM (SELECT SUM(ChurnedMembers) * 1.0 / NULLIF(SUM(ActiveMembers), 0) AS Rate,
                         COUNT(CASE WHEN MonthlyChurnRate * 10000 < @MinBps
                                      OR MonthlyChurnRate * 10000 > @MaxBps THEN 1 END) AS MonthsOutside
                  FROM dbo.vw_MonthlyChurn) AS r

            -- 9 Active members should have a row in the latest loaded month
            UNION ALL
            SELECT 9, 'Active members present in latest month',
                   CASE WHEN x.n = 0 THEN 'PASS' ELSE 'WARN' END,
                   CONCAT(x.n, ' active member(s) have no FactEnrollment row for the latest month')
            FROM (SELECT COUNT(*) AS n
                  FROM dbo.DimMember m
                  CROSS JOIN dbo.vw_ObservationWindow w
                  WHERE m.MemberStatus = 'Active'
                    AND m.EnrollmentDate <= w.WindowEnd
                    AND NOT EXISTS (SELECT 1
                                    FROM dbo.FactEnrollment f
                                    JOIN dbo.DimDate d ON d.DateKey = f.DateKey
                                    WHERE f.MemberSK = m.MemberSK AND d.FullDate = w.WindowEnd)) AS x

            -- 10 No claims at all in the latest month usually means the claims feed is missing
            UNION ALL
            SELECT 10, 'Claims present in latest month',
                   CASE WHEN x.n > 0 THEN 'PASS' ELSE 'WARN' END,
                   CONCAT(x.n, ' claim(s) dated in the latest loaded month')
            FROM (SELECT COUNT(*) AS n
                  FROM dbo.FactClaims fc
                  JOIN dbo.DimDate d ON d.DateKey = fc.DateKey
                  CROSS JOIN dbo.vw_ObservationWindow w
                  WHERE d.FullDate BETWEEN w.WindowEnd AND EOMONTH(w.WindowEnd)) AS x
        ) AS c
        ORDER BY c.Ord;      -- identity values follow this order, so AuditID reads 1..10
        -- END:audits

        COMMIT TRANSACTION;

        SELECT CheckName, Status, Detail
        FROM dbo.PipelineAuditResult
        WHERE RunID = @RunID AND CheckName <> 'Quarantine rate'
        ORDER BY AuditID;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;  -- re-raise the original error
    END CATCH
END;
GO

-- 5. Run history for an operations dashboard (Tableau, or just SELECT)
CREATE OR ALTER VIEW dbo.vw_PipelineRunHistory AS
WITH a AS (
    SELECT RunID,
           COUNT(*)                                          AS TotalChecks,
           SUM(CASE WHEN Status = 'FAIL' THEN 1 ELSE 0 END)  AS FailedChecks,
           SUM(CASE WHEN Status = 'WARN' THEN 1 ELSE 0 END)  AS WarningChecks
    FROM dbo.PipelineAuditResult
    GROUP BY RunID
)
SELECT r.RunID, r.BatchID, r.StartedAt, r.FinishedAt,
       DATEDIFF(SECOND, r.StartedAt, r.FinishedAt) AS DurationSeconds,
       r.Status, r.TriggeredBy,
       r.RowsRead, r.RowsQuarantined,
       CASE WHEN r.RowsRead > 0 THEN r.RowsQuarantined * 100.0 / r.RowsRead END AS QuarantinePct,
       r.RowsStaged,
       r.MemberInserts, r.MemberUpdates, r.EnrollmentInserts, r.EnrollmentUpdates,
       r.ClaimInserts, r.ClaimUpdates, r.FeatureRows, r.AnnualChurnRate,
       ISNULL(a.TotalChecks, 0)   AS TotalChecks,
       ISNULL(a.FailedChecks, 0)  AS FailedChecks,
       ISNULL(a.WarningChecks, 0) AS WarningChecks,
       r.ErrorMessage
FROM dbo.PipelineRunLog r
LEFT JOIN a ON a.RunID = r.RunID;
GO

-- 6. Housekeeping
--    usp_CloseStaleRuns: a run killed mid-way (power loss, closed window) stays 'Running' forever.
--    Call it only when no run can be active, e.g. right after taking the pipeline's application lock.
CREATE OR ALTER PROCEDURE dbo.usp_CloseStaleRuns
    @MaxMinutes INT = 240
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.PipelineRunLog
    SET Status       = 'Failed',
        FinishedAt   = SYSUTCDATETIME(),
        ErrorMessage = N'Run never finished (the process stopped or lost its connection).'
    WHERE Status = 'Running'
      AND StartedAt < DATEADD(MINUTE, -@MaxMinutes, SYSUTCDATETIME());

    SELECT @@ROWCOUNT AS RunsClosed;
END;
GO

--    usp_PurgePipelineLogs: keeps the history table small. Never touches runs still 'Running'.
CREATE OR ALTER PROCEDURE dbo.usp_PurgePipelineLogs
    @KeepDays INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @KeepDays IS NULL
        SELECT @KeepDays = ConfigValue FROM dbo.AttritionConfig WHERE ConfigKey = 'LogRetentionDays';
    IF @KeepDays IS NULL OR @KeepDays < 1
        THROW 50032, 'KeepDays must be a positive number (or set LogRetentionDays in dbo.AttritionConfig).', 1;

    DECLARE @cutoff DATETIME2 = DATEADD(DAY, -@KeepDays, SYSUTCDATETIME());
    DECLARE @runs INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE a
        FROM dbo.PipelineAuditResult a
        JOIN dbo.PipelineRunLog r ON r.RunID = a.RunID
        WHERE r.StartedAt < @cutoff AND r.Status <> 'Running';

        DELETE FROM dbo.PipelineRunLog
        WHERE StartedAt < @cutoff AND Status <> 'Running';
        SET @runs = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT @runs AS RunsPurged;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH
END;
GO

-- Verification:
-- SELECT * FROM dbo.vw_PipelineRunHistory ORDER BY RunID DESC;
-- SELECT * FROM dbo.PipelineAuditResult WHERE RunID = <id> ORDER BY AuditID;
-- EXEC dbo.usp_CloseStaleRuns;          -- after a crashed run
-- EXEC dbo.usp_PurgePipelineLogs;       -- uses LogRetentionDays (default 180)
