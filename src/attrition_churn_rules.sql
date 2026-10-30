-- =====================================================================
-- Churn rules: 15-day grace period + 12-month observation window
-- Run order: warehouse_star_schema.sql -> attrition_schema.sql
--            -> attrition_staging.sql -> this file -> attrition_dq_checks.sql
--            (or run everything with deploy_attrition.sql)
--
-- Definitions
--   Grace period (15 days): a member who misses a premium stays covered for
--     15 days. Coverage lapses (TerminationReason = 'NonPayment', status
--     'Lapsed') only if the premium is still unpaid when the grace period
--     ends, so TerminationDate >= FirstMissedPaymentDate + 15 days.
--   Churn event: IsChurnFlag = 1 in the month containing TerminationDate.
--   Observation window (12 months): churn metrics use only the trailing
--     12 monthly snapshots ending at the latest loaded month. The window is
--     "complete" only when all 12 of those months are actually loaded (a
--     missing month inside the window makes it incomplete). Annual rates are
--     NULL until the window is complete.
--   Annual churn rate = members active at window start who churned inside
--     the window / members active at window start.
--   Renewal-driven risk (attrition_metrics.sql): an ACTIVE member is flagged at risk
--     when the premium rose 10% or more (AttritionConfig 'PremiumIncreaseThresholdPct') at
--     their LAST RENEWAL (plan-year renewal month, 'RenewalMonth', default January) and they
--     had a denied claim in the last 3 months. The flag holds until the next renewal; a
--     renewal with a smaller increase clears it. vw_Rpt_RenewalIncreaseChurn shows how monthly
--     churn actually varies with the size of the last renewal increase.
-- =====================================================================

IF DB_ID('WarehouseDB') IS NULL
    CREATE DATABASE WarehouseDB;
GO

USE WarehouseDB;
GO

IF OBJECT_ID('dbo.DimMember', 'U') IS NULL OR OBJECT_ID('dbo.FactEnrollment', 'U') IS NULL
    THROW 50003, 'Phase 1 tables not found. Run attrition_schema.sql first.', 1;
GO

-- ---------------------------------------------------------------------
-- 1. Config table (single source of truth for the rules used by views
--    and later stored procedures). Insert-only: re-running never overwrites
--    a value you changed.
-- ---------------------------------------------------------------------
IF OBJECT_ID('dbo.AttritionConfig', 'U') IS NULL
CREATE TABLE dbo.AttritionConfig (
    ConfigKey   VARCHAR(50)  NOT NULL PRIMARY KEY,
    ConfigValue INT          NOT NULL,
    Description VARCHAR(200) NOT NULL,
    CONSTRAINT CK_AttritionConfig_Positive CHECK (ConfigValue > 0)
);
GO

MERGE dbo.AttritionConfig AS tgt
USING (VALUES
    ('GracePeriodDays',         15, 'Days after a missed premium before coverage lapses. Also hard-coded in CK_DimMember_NonPaymentGrace.'),
    ('ObservationWindowMonths', 12, 'Trailing monthly snapshots used for churn metrics.')
) AS src (ConfigKey, ConfigValue, Description)
    ON tgt.ConfigKey = src.ConfigKey
WHEN NOT MATCHED THEN
    INSERT (ConfigKey, ConfigValue, Description)
    VALUES (src.ConfigKey, src.ConfigValue, src.Description);
GO

-- ---------------------------------------------------------------------
-- 2. Migration for databases created before these rules existed.
--    No-ops on a fresh build, where attrition_schema.sql / attrition_staging.sql
--    already include these objects.
--    (If DimMember already holds rows that break a new rule, the ADD
--    CONSTRAINT statement fails; fix or reload that data.)
-- ---------------------------------------------------------------------
IF COL_LENGTH('dbo.DimMember', 'FirstMissedPaymentDate') IS NULL
    ALTER TABLE dbo.DimMember ADD FirstMissedPaymentDate DATE NULL;
GO

IF OBJECT_ID('dbo.stg_members', 'U') IS NOT NULL
   AND COL_LENGTH('dbo.stg_members', 'first_missed_payment_date') IS NULL
    ALTER TABLE dbo.stg_members ADD first_missed_payment_date DATE NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE name = 'CK_DimMember_NonPaymentGrace' AND parent_object_id = OBJECT_ID('dbo.DimMember'))
    ALTER TABLE dbo.DimMember ADD CONSTRAINT CK_DimMember_NonPaymentGrace
        CHECK ((TerminationReason = 'NonPayment'
                AND FirstMissedPaymentDate IS NOT NULL
                AND TerminationDate >= DATEADD(DAY, 15, FirstMissedPaymentDate))
            OR (ISNULL(TerminationReason, '') <> 'NonPayment'
                AND FirstMissedPaymentDate IS NULL));
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE name = 'CK_DimMember_LapsedReason' AND parent_object_id = OBJECT_ID('dbo.DimMember'))
    ALTER TABLE dbo.DimMember ADD CONSTRAINT CK_DimMember_LapsedReason
        CHECK ((MemberStatus = 'Lapsed' AND TerminationReason = 'NonPayment')
            OR (MemberStatus <> 'Lapsed' AND ISNULL(TerminationReason, '') <> 'NonPayment'));
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE name = 'CK_DimMember_MemberID' AND parent_object_id = OBJECT_ID('dbo.DimMember'))
    ALTER TABLE dbo.DimMember ADD CONSTRAINT CK_DimMember_MemberID
        CHECK (LEN(LTRIM(RTRIM(MemberID))) > 0);
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE name = 'CK_DimMember_AgeBand' AND parent_object_id = OBJECT_ID('dbo.DimMember'))
    ALTER TABLE dbo.DimMember ADD CONSTRAINT CK_DimMember_AgeBand
        CHECK (AgeBand IN ('0-17', '18-25', '26-35', '36-45', '46-55', '56-64', '65+'));
GO

-- Drift guard: the grace period is hard-coded in the CHECK constraint (a constraint
-- cannot read a table). Warn if the config value no longer matches it.
IF NOT EXISTS (
    SELECT 1
    FROM sys.check_constraints cc
    JOIN dbo.AttritionConfig a ON a.ConfigKey = 'GracePeriodDays'
    WHERE cc.name = 'CK_DimMember_NonPaymentGrace'
      AND cc.parent_object_id = OBJECT_ID('dbo.DimMember')
      AND cc.definition LIKE '%(' + CAST(a.ConfigValue AS VARCHAR(10)) + ')%'
)
    PRINT 'WARNING: GracePeriodDays in dbo.AttritionConfig does not match CK_DimMember_NonPaymentGrace. Recreate the constraint.';
GO

-- ---------------------------------------------------------------------
-- 3. Observation window view (one row)
--    WindowEnd        = latest month loaded into FactEnrollment
--    WindowStart      = WindowEnd minus (window months - 1)
--    MonthsInWindow   = distinct months actually loaded inside that span
--    IsWindowComplete = 1 only when MonthsInWindow = WindowMonths
--                       (catches both short history and missing months)
-- ---------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_ObservationWindow AS
WITH cfg AS (
    SELECT ConfigValue AS WindowMonths
    FROM dbo.AttritionConfig
    WHERE ConfigKey = 'ObservationWindowMonths'
),
loaded AS (
    SELECT DISTINCT d.FullDate AS MonthStart
    FROM dbo.FactEnrollment f
    JOIN dbo.DimDate d ON d.DateKey = f.DateKey
),
bounds AS (
    SELECT MAX(MonthStart) AS LastMonth, COUNT(*) AS MonthsAvailable
    FROM loaded
)
SELECT
    cfg.WindowMonths,
    DATEADD(MONTH, -(cfg.WindowMonths - 1), b.LastMonth) AS WindowStart,
    b.LastMonth                                          AS WindowEnd,
    b.MonthsAvailable,
    x.MonthsInWindow,
    CASE WHEN x.MonthsInWindow = cfg.WindowMonths THEN 1 ELSE 0 END AS IsWindowComplete
FROM cfg
CROSS JOIN bounds b
CROSS APPLY (
    SELECT COUNT(*) AS MonthsInWindow
    FROM loaded l
    WHERE l.MonthStart BETWEEN DATEADD(MONTH, -(cfg.WindowMonths - 1), b.LastMonth) AND b.LastMonth
) AS x;
GO

-- ---------------------------------------------------------------------
-- 4. Monthly churn inside the window
-- ---------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_MonthlyChurn AS
SELECT
    d.FullDate                                                    AS MonthStart,
    SUM(CAST(f.IsActive AS INT))                                  AS ActiveMembers,
    SUM(CAST(f.IsChurnFlag AS INT))                               AS ChurnedMembers,
    SUM(CAST(f.IsChurnFlag AS INT)) * 1.0
        / NULLIF(SUM(CAST(f.IsActive AS INT)), 0)                 AS MonthlyChurnRate,
    w.IsWindowComplete
FROM dbo.FactEnrollment f
JOIN dbo.DimDate d ON d.DateKey = f.DateKey
JOIN dbo.vw_ObservationWindow w
    ON d.FullDate BETWEEN w.WindowStart AND w.WindowEnd
GROUP BY d.FullDate, w.IsWindowComplete;
GO

-- ---------------------------------------------------------------------
-- 5. Annual churn over the window (one row).
--    AnnualChurnRate / AnnualRetentionRate are NULL until the window is complete.
-- ---------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_AnnualChurn AS
WITH w AS (
    SELECT * FROM dbo.vw_ObservationWindow
),
cohort AS (   -- members covered on the first day of the window
    SELECT DISTINCT f.MemberSK
    FROM dbo.FactEnrollment f
    JOIN dbo.DimDate d ON d.DateKey = f.DateKey
    JOIN w ON d.FullDate = w.WindowStart
    WHERE f.IsActive = 1
),
churned AS (  -- cohort members whose coverage ended inside the window
    SELECT DISTINCT f.MemberSK
    FROM dbo.FactEnrollment f
    JOIN dbo.DimDate d ON d.DateKey = f.DateKey
    JOIN w ON d.FullDate BETWEEN w.WindowStart AND w.WindowEnd
    WHERE f.IsChurnFlag = 1
      AND f.MemberSK IN (SELECT MemberSK FROM cohort)
)
SELECT
    w.WindowStart,
    w.WindowEnd,
    w.WindowMonths,
    w.IsWindowComplete,
    (SELECT COUNT(*) FROM cohort)  AS CohortMembers,
    (SELECT COUNT(*) FROM churned) AS ChurnedMembers,
    CASE WHEN w.IsWindowComplete = 1
         THEN (SELECT COUNT(*) FROM churned) * 1.0 / NULLIF((SELECT COUNT(*) FROM cohort), 0)
    END AS AnnualChurnRate,
    CASE WHEN w.IsWindowComplete = 1
         THEN 1.0 - (SELECT COUNT(*) FROM churned) * 1.0 / NULLIF((SELECT COUNT(*) FROM cohort), 0)
    END AS AnnualRetentionRate
FROM w;
GO

-- 6. Verification
-- SELECT * FROM dbo.AttritionConfig;
-- SELECT * FROM dbo.vw_ObservationWindow;
-- SELECT * FROM dbo.vw_MonthlyChurn ORDER BY MonthStart;
-- SELECT * FROM dbo.vw_AnnualChurn;
