-- =====================================================================
-- Phase 4: attrition metrics (features, risk flags, cohort retention)
-- Run after attrition_load_warehouse.sql (deploy_attrition.sql does this).
--
--   EXEC dbo.usp_BuildAttritionMetrics;      -- full, idempotent rebuild
--
-- Builds two tables inside ONE transaction (all or nothing):
--   dbo.MemberMonthFeatures  grain = member + month (same grain as FactEnrollment)
--   dbo.CohortRetention      grain = enrollment cohort month + months since enrollment
--
-- Metric definitions
--   TenureMonths        months between enrollment month and this month (0 = first month)
--   PremiumChangePct    (premium - previous month premium) / previous month premium, 4 d.p.
--   MaxPremiumChange3M  largest PremiumChangePct in the trailing 3 months (this one + 2 before)
--   Claims3M/Denied3M   claims / denied claims in the trailing 3 months (~90 days)
--   DenialRate3M        Denied3M / Claims3M
--   Claims6M            claims in the trailing 6 months
--   DaysSinceLastClaim  days from the last observed claim to the end of this month
--   AnnualizedPremium   MonthlyPremium x 12 (premium "exposure"; summed over churned rows
--                       it is the annual premium walking out the door, not realized loss)
--   IsHighPremiumIncrease  MaxPremiumChange3M >= threshold (AttritionConfig, default 10%); a short-term
--                          signal that lapses 3 months after any premium change
--   LastRenewalMonth       the member's most recent renewal month on or before this row. A renewal
--                          month is the plan-year renewal month (AttritionConfig 'RenewalMonth', default
--                          1 = January), and only counts if the member also has a row for the month before
--   LastRenewalChangePct   premium change at that renewal (same formula as PremiumChangePct), carried
--                          forward until the next renewal. A 0% renewal REPLACES an earlier increase, so a
--                          flag never outlives its plan year. NULL until a renewal with a prior month is seen
--                          (members who joined that month, or any renewal before the loaded data starts)
--   IsHighRenewalIncrease  LastRenewalChangePct >= threshold (same 10% setting)
--   HasRecentDenial        Denied3M > 0
--   IsDisengaged           no claims in 6 months, with at least 6 months observed
--   IsAtRisk               IsHighRenewalIncrease AND HasRecentDenial
--                          (a premium increase at the last renewal plus a denied claim in the last 3 months)
--   All trailing windows use only past and current rows (no look-ahead), so the
--   features are safe to reuse later for a predictive model.
--
--   Cohort retention counts only members who enrolled INSIDE the loaded months, so
--   every cohort is observed from month 0. Members enrolled before the data starts
--   are left out (left-truncation). A cohort has no row for months not yet reached
--   (right-censoring), so a missing 12-month value means "not observable yet".
-- =====================================================================

USE WarehouseDB;
GO

-- Filtered index on FactEnrollment: any procedure that writes to it must be created with these ON
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.FactEnrollment', 'U') IS NULL
   OR OBJECT_ID('dbo.FactClaims', 'U') IS NULL
   OR OBJECT_ID('dbo.AttritionConfig', 'U') IS NULL
    THROW 50020, 'Required objects missing. Run attrition_schema.sql, attrition_staging.sql and attrition_churn_rules.sql first.', 1;
GO

MERGE dbo.AttritionConfig AS tgt
USING (VALUES
    ('PremiumIncreaseThresholdPct', 10, 'Premium increase (%) that counts as high: at the last renewal (at-risk rule) and over 3 months (short-term flag).'),
    ('RenewalMonth',                 1, 'Plan-year renewal month (1-12). Premium changes in this month define the last-renewal increase.')
) AS src (ConfigKey, ConfigValue, Description)
    ON tgt.ConfigKey = src.ConfigKey
WHEN NOT MATCHED THEN
    INSERT (ConfigKey, ConfigValue, Description)
    VALUES (src.ConfigKey, src.ConfigValue, src.Description);
GO

-- Refresh the wording of the threshold description on databases built before the renewal rule
UPDATE dbo.AttritionConfig
SET Description = 'Premium increase (%) that counts as high: at the last renewal (at-risk rule) and over 3 months (short-term flag).'
WHERE ConfigKey = 'PremiumIncreaseThresholdPct' AND Description LIKE '%over 3 months that counts%';
GO

-- BEGIN:ddl_features
IF OBJECT_ID('dbo.MemberMonthFeatures', 'U') IS NULL
CREATE TABLE dbo.MemberMonthFeatures (
    MemberSK              INT           NOT NULL,
    DateKey               INT           NOT NULL,
    MonthStart            DATE          NOT NULL,
    PlanSK                INT           NOT NULL,
    TenureMonths          INT           NOT NULL,
    TenureBucket          VARCHAR(10)   NOT NULL,
    MonthlyPremium        DECIMAL(10,2) NOT NULL,
    PrevPremium           DECIMAL(10,2) NULL,
    PremiumChangePct      DECIMAL(9,4)  NULL,
    MaxPremiumChange3M    DECIMAL(9,4)  NULL,
    ClaimsInMonth         INT           NOT NULL,
    DeniedInMonth         INT           NOT NULL,
    Claims3M              INT           NOT NULL,
    Denied3M              INT           NOT NULL,
    DenialRate3M          DECIMAL(9,4)  NULL,
    Claims6M              INT           NOT NULL,
    LastClaimDate         DATE          NULL,
    DaysSinceLastClaim    INT           NULL,
    IsActive              BIT           NOT NULL,
    IsChurnFlag           BIT           NOT NULL,
    AnnualizedPremium     DECIMAL(12,2) NOT NULL,
    IsHighPremiumIncrease BIT           NOT NULL,
    LastRenewalMonth      DATE          NULL,
    LastRenewalChangePct  DECIMAL(9,4)  NULL,
    IsHighRenewalIncrease BIT           NOT NULL,
    HasRecentDenial       BIT           NOT NULL,
    IsDisengaged          BIT           NOT NULL,
    IsAtRisk              BIT           NOT NULL,
    BuiltAt               DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_MemberMonthFeatures PRIMARY KEY (MemberSK, DateKey)
);
-- END:ddl_features
GO

-- Migration: databases built before the renewal rule get the new columns (the table is rebuilt on every run)
IF COL_LENGTH('dbo.MemberMonthFeatures', 'LastRenewalMonth') IS NULL
    ALTER TABLE dbo.MemberMonthFeatures ADD LastRenewalMonth DATE NULL;
GO
IF COL_LENGTH('dbo.MemberMonthFeatures', 'LastRenewalChangePct') IS NULL
    ALTER TABLE dbo.MemberMonthFeatures ADD LastRenewalChangePct DECIMAL(9,4) NULL;
GO
IF COL_LENGTH('dbo.MemberMonthFeatures', 'IsHighRenewalIncrease') IS NULL
    ALTER TABLE dbo.MemberMonthFeatures
        ADD IsHighRenewalIncrease BIT NOT NULL CONSTRAINT DF_MemberMonthFeatures_HighRenewal DEFAULT 0;
GO

-- BEGIN:ddl_cohort
IF OBJECT_ID('dbo.CohortRetention', 'U') IS NULL
CREATE TABLE dbo.CohortRetention (
    CohortMonth           DATE          NOT NULL,
    MonthsSinceEnrollment INT           NOT NULL,
    CohortSize            INT           NOT NULL,
    ActiveMembers         INT           NOT NULL,
    RetentionRate         DECIMAL(9,4)  NOT NULL,
    BuiltAt               DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_CohortRetention PRIMARY KEY (CohortMonth, MonthsSinceEnrollment)
);
-- END:ddl_cohort
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_MemberMonthFeatures_Month' AND object_id = OBJECT_ID('dbo.MemberMonthFeatures'))
    CREATE INDEX IX_MemberMonthFeatures_Month ON dbo.MemberMonthFeatures(MonthStart)
        INCLUDE (PlanSK, IsActive, IsChurnFlag, IsAtRisk, TenureBucket, AnnualizedPremium);
GO

CREATE OR ALTER PROCEDURE dbo.usp_BuildAttritionMetrics
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;   -- roll back the whole transaction automatically on runtime errors

    DECLARE @threshold DECIMAL(9,4);
    SELECT @threshold = ConfigValue / 100.0
    FROM dbo.AttritionConfig
    WHERE ConfigKey = 'PremiumIncreaseThresholdPct';

    DECLARE @renewalMonth INT;
    SELECT @renewalMonth = ConfigValue
    FROM dbo.AttritionConfig
    WHERE ConfigKey = 'RenewalMonth';

    IF @threshold IS NULL
        THROW 50021, 'PremiumIncreaseThresholdPct missing from dbo.AttritionConfig.', 1;
    IF @renewalMonth IS NULL OR @renewalMonth NOT BETWEEN 1 AND 12
        THROW 50023, 'RenewalMonth in dbo.AttritionConfig must be a month number from 1 to 12.', 1;
    IF NOT EXISTS (SELECT 1 FROM dbo.FactEnrollment)
        THROW 50022, 'FactEnrollment is empty. Run the ingestion first.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        TRUNCATE TABLE dbo.MemberMonthFeatures;
        TRUNCATE TABLE dbo.CohortRetention;

        -- BEGIN:features
        -- Window functions cannot be nested, so the logic is layered in CTEs:
        --   claims_m -> base -> w (LAG / rolling SUM / running MAX / ROW_NUMBER)
        --   -> p (PremiumChangePct) -> q (rolling MAX of the change)
        -- and, separately, rp -> ren (the premium change in each renewal month), joined back through
        -- w.LastRenewalKey = the running MAX of the renewal-month DateKey = "the last renewal so far".
        WITH claims_m AS (
            SELECT c.MemberSK,
                   DATEFROMPARTS(YEAR(d.FullDate), MONTH(d.FullDate), 1) AS MonthStart,
                   COUNT(*)                                              AS ClaimsInMonth,
                   SUM(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE 0 END) AS DeniedInMonth,
                   MAX(d.FullDate)                                       AS LastClaimInMonth
            FROM dbo.FactClaims c
            JOIN dbo.DimDate d ON d.DateKey = c.DateKey
            GROUP BY c.MemberSK, DATEFROMPARTS(YEAR(d.FullDate), MONTH(d.FullDate), 1)
        ),
        base AS (
            SELECT f.MemberSK, f.DateKey, dd.FullDate AS MonthStart, f.PlanSK, m.EnrollmentDate,
                   f.MonthlyPremium, f.IsActive, f.IsChurnFlag,
                   ISNULL(cm.ClaimsInMonth, 0) AS ClaimsInMonth,
                   ISNULL(cm.DeniedInMonth, 0) AS DeniedInMonth,
                   cm.LastClaimInMonth
            FROM dbo.FactEnrollment f
            JOIN dbo.DimDate   dd ON dd.DateKey  = f.DateKey
            JOIN dbo.DimMember m  ON m.MemberSK  = f.MemberSK
            LEFT JOIN claims_m cm ON cm.MemberSK = f.MemberSK AND cm.MonthStart = dd.FullDate
        ),
        w AS (
            SELECT b.MemberSK, b.DateKey, b.MonthStart, b.PlanSK, b.MonthlyPremium, b.IsActive, b.IsChurnFlag,
                   b.ClaimsInMonth, b.DeniedInMonth,
                   DATEDIFF(MONTH, b.EnrollmentDate, b.MonthStart) AS TenureMonths,
                   LAG(b.MonthlyPremium) OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey) AS PrevPremium,
                   SUM(b.ClaimsInMonth) OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey
                                              ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Claims3M,
                   SUM(b.DeniedInMonth) OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey
                                              ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Denied3M,
                   SUM(b.ClaimsInMonth) OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey
                                              ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS Claims6M,
                   MAX(b.LastClaimInMonth) OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LastClaimDate,
                   ROW_NUMBER() OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey) AS MonthsObserved,
                   MAX(CASE WHEN MONTH(b.MonthStart) = @renewalMonth THEN b.DateKey END)
                       OVER (PARTITION BY b.MemberSK ORDER BY b.DateKey
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LastRenewalKey
            FROM base b
        ),
        p AS (
            SELECT w.*,
                   ROUND(CASE WHEN w.PrevPremium > 0
                              THEN (w.MonthlyPremium - w.PrevPremium) * 1.0 / w.PrevPremium END, 4) AS PremiumChangePct
            FROM w
        ),
        q AS (
            SELECT p.*,
                   MAX(p.PremiumChangePct) OVER (PARTITION BY p.MemberSK ORDER BY p.DateKey
                                                 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MaxPremiumChange3M
            FROM p
        ),
        rp AS (        -- light pass over the fact table (already ordered by its unique index)
            SELECT f.MemberSK, f.DateKey, dd.FullDate AS MonthStart, f.MonthlyPremium,
                   LAG(f.MonthlyPremium) OVER (PARTITION BY f.MemberSK ORDER BY f.DateKey) AS PrevPremium
            FROM dbo.FactEnrollment f
            JOIN dbo.DimDate dd ON dd.DateKey = f.DateKey
        ),
        ren AS (       -- one row per member per renewal month; NULL change when there is no prior month
            SELECT MemberSK, DateKey AS RenewalKey, MonthStart AS RenewalMonth,
                   ROUND(CASE WHEN PrevPremium > 0
                              THEN (MonthlyPremium - PrevPremium) * 1.0 / PrevPremium END, 4) AS RenewalChangePct
            FROM rp
            WHERE MONTH(MonthStart) = @renewalMonth
        )
        INSERT INTO dbo.MemberMonthFeatures
            (MemberSK, DateKey, MonthStart, PlanSK, TenureMonths, TenureBucket, MonthlyPremium,
             PrevPremium, PremiumChangePct, MaxPremiumChange3M, ClaimsInMonth, DeniedInMonth,
             Claims3M, Denied3M, DenialRate3M, Claims6M, LastClaimDate, DaysSinceLastClaim,
             IsActive, IsChurnFlag, AnnualizedPremium,
             IsHighPremiumIncrease, LastRenewalMonth, LastRenewalChangePct, IsHighRenewalIncrease,
             HasRecentDenial, IsDisengaged, IsAtRisk)
        SELECT q.MemberSK, q.DateKey, q.MonthStart, q.PlanSK, q.TenureMonths,
               CASE WHEN q.TenureMonths < 6  THEN '0-5'
                    WHEN q.TenureMonths < 12 THEN '6-11'
                    WHEN q.TenureMonths < 24 THEN '12-23'
                    ELSE '24+' END,
               q.MonthlyPremium, q.PrevPremium, q.PremiumChangePct, q.MaxPremiumChange3M,
               q.ClaimsInMonth, q.DeniedInMonth, q.Claims3M, q.Denied3M,
               CASE WHEN q.Claims3M > 0 THEN q.Denied3M * 1.0 / q.Claims3M END,
               q.Claims6M, q.LastClaimDate,
               CASE WHEN q.LastClaimDate IS NOT NULL
                    THEN DATEDIFF(DAY, q.LastClaimDate, EOMONTH(q.MonthStart)) END,
               q.IsActive, q.IsChurnFlag,
               q.MonthlyPremium * 12,
               CASE WHEN q.MaxPremiumChange3M >= @threshold THEN 1 ELSE 0 END,
               ren.RenewalMonth,
               ren.RenewalChangePct,
               CASE WHEN ren.RenewalChangePct >= @threshold THEN 1 ELSE 0 END,
               CASE WHEN q.Denied3M > 0 THEN 1 ELSE 0 END,
               CASE WHEN q.Claims6M = 0 AND q.MonthsObserved >= 6 THEN 1 ELSE 0 END,
               CASE WHEN ren.RenewalChangePct >= @threshold AND q.Denied3M > 0 THEN 1 ELSE 0 END
        FROM q
        LEFT JOIN ren ON ren.MemberSK = q.MemberSK AND ren.RenewalKey = q.LastRenewalKey;
        -- END:features

        -- BEGIN:cohort
        WITH bounds AS (
            SELECT MIN(MonthStart) AS FirstMonth, MAX(MonthStart) AS LastMonth
            FROM dbo.MemberMonthFeatures
        ),
        cohort AS (   -- members who enrolled inside the loaded months
            SELECT DATEFROMPARTS(YEAR(m.EnrollmentDate), MONTH(m.EnrollmentDate), 1) AS CohortMonth,
                   m.MemberSK
            FROM dbo.DimMember m
            CROSS JOIN bounds b
            WHERE m.EnrollmentDate >= b.FirstMonth
              AND m.EnrollmentDate <= EOMONTH(b.LastMonth)
        ),
        sizes AS (
            SELECT CohortMonth, COUNT(*) AS CohortSize
            FROM cohort
            GROUP BY CohortMonth
        ),
        active AS (   -- members still covered k months after their enrollment month
            SELECT c.CohortMonth,
                   DATEDIFF(MONTH, c.CohortMonth, f.MonthStart) AS MonthsSinceEnrollment,
                   COUNT(DISTINCT f.MemberSK)                   AS ActiveMembers
            FROM cohort c
            JOIN dbo.MemberMonthFeatures f ON f.MemberSK = c.MemberSK AND f.IsActive = 1
            GROUP BY c.CohortMonth, DATEDIFF(MONTH, c.CohortMonth, f.MonthStart)
        )
        INSERT INTO dbo.CohortRetention (CohortMonth, MonthsSinceEnrollment, CohortSize, ActiveMembers, RetentionRate)
        SELECT a.CohortMonth, a.MonthsSinceEnrollment, s.CohortSize, a.ActiveMembers,
               a.ActiveMembers * 1.0 / s.CohortSize
        FROM active a
        JOIN sizes s ON s.CohortMonth = a.CohortMonth
        WHERE a.MonthsSinceEnrollment >= 0;
        -- END:cohort

        COMMIT TRANSACTION;

        SELECT 'MemberMonthFeatures' AS TableName, COUNT(*) AS RowsBuilt FROM dbo.MemberMonthFeatures
        UNION ALL
        SELECT 'CohortRetention', COUNT(*) FROM dbo.CohortRetention;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;  -- re-raise the original error
    END CATCH
END;
GO

-- Usage:
-- EXEC dbo.usp_BuildAttritionMetrics;
-- SELECT TOP 20 * FROM dbo.MemberMonthFeatures WHERE IsAtRisk = 1 ORDER BY MonthStart DESC;
-- SELECT * FROM dbo.CohortRetention ORDER BY CohortMonth, MonthsSinceEnrollment;
