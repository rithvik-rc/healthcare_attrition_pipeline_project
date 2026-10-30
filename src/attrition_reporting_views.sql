-- =====================================================================
-- Phase 6: reporting views for Tableau (connect to these, not to the raw tables)
-- Run after attrition_metrics.sql (deploy_attrition.sql does this).
--
--   vw_Rpt_ExecutiveSummary   one row: window, annual churn, retention, premium lost, at-risk
--   vw_Rpt_MonthlyChurn       monthly trend: rate, 3-month rolling rate, prior-year rate, premium lost
--   vw_Rpt_SegmentChurn       churn by plan tier / plan type / region / age band / tenure (long format)
--   vw_Rpt_CohortRetention    cohort heatmap data (cohort month x months since enrollment)
--   vw_Rpt_CohortMilestones   3 / 6 / 12-month retention per cohort (NULL = not observable yet)
--   vw_Rpt_AtRiskMembers      outreach list: active members with a 10%+ increase at their last renewal and a recent denial, ranked
--   vw_Rpt_RenewalIncreaseChurn  monthly churn by size of the last renewal increase (does the rule predict churn?)
--   vw_Rpt_TerminationReasons terminations by month, reason and category (voluntary / involuntary / death)
--
-- Governance: only de-identified member IDs, age bands and regions are exposed.
-- =====================================================================

USE WarehouseDB;
GO

-- Filtered index on FactEnrollment: any procedure that writes to it must be created with these ON
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.MemberMonthFeatures', 'U') IS NULL OR OBJECT_ID('dbo.CohortRetention', 'U') IS NULL
    THROW 50040, 'Run attrition_metrics.sql first.', 1;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_MonthlyChurn AS
WITH monthly AS (
    SELECT f.MonthStart,
           SUM(CAST(f.IsActive AS INT))    AS ActiveMembers,
           SUM(CAST(f.IsChurnFlag AS INT)) AS ChurnedMembers,
           SUM(CASE WHEN f.IsChurnFlag = 1 THEN f.AnnualizedPremium ELSE 0 END) AS ChurnedAnnualizedPremium
    FROM dbo.MemberMonthFeatures f
    GROUP BY f.MonthStart
),
rates AS (
    SELECT MonthStart, ActiveMembers, ChurnedMembers, ChurnedAnnualizedPremium,
           ChurnedMembers * 1.0 / NULLIF(ActiveMembers, 0) AS MonthlyChurnRate
    FROM monthly
)
SELECT r.MonthStart,
       r.ActiveMembers,
       r.ChurnedMembers,
       r.MonthlyChurnRate,
       AVG(r.MonthlyChurnRate) OVER (ORDER BY r.MonthStart
                                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Rolling3MChurnRate,
       py.MonthlyChurnRate AS PriorYearChurnRate,      -- same month, one year earlier (seasonality)
       r.ChurnedAnnualizedPremium
FROM rates r
LEFT JOIN rates py ON py.MonthStart = DATEADD(YEAR, -1, r.MonthStart);
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_SegmentChurn AS
WITH seg AS (
    SELECT f.IsActive, f.IsChurnFlag, f.AnnualizedPremium, f.TenureBucket,
           m.AgeBand, ISNULL(m.Region, 'Unknown') AS Region, p.PlanTier, p.PlanType
    FROM dbo.MemberMonthFeatures f
    JOIN dbo.DimMember m ON m.MemberSK = f.MemberSK
    JOIN dbo.DimPlan   p ON p.PlanSK   = f.PlanSK
    JOIN dbo.vw_ObservationWindow w ON f.MonthStart BETWEEN w.WindowStart AND w.WindowEnd
),
segments AS (
    SELECT 'Plan tier' AS SegmentType, PlanTier   AS SegmentValue, IsActive, IsChurnFlag, AnnualizedPremium FROM seg
    UNION ALL
    SELECT 'Plan type',  PlanType,    IsActive, IsChurnFlag, AnnualizedPremium FROM seg
    UNION ALL
    SELECT 'Region',     Region,      IsActive, IsChurnFlag, AnnualizedPremium FROM seg
    UNION ALL
    SELECT 'Age band',   AgeBand,     IsActive, IsChurnFlag, AnnualizedPremium FROM seg
    UNION ALL
    SELECT 'Tenure',     TenureBucket, IsActive, IsChurnFlag, AnnualizedPremium FROM seg
),
agg AS (
    SELECT SegmentType, SegmentValue,
           SUM(CAST(IsActive AS INT))    AS MemberMonths,
           SUM(CAST(IsChurnFlag AS INT)) AS ChurnedMembers,
           SUM(CASE WHEN IsChurnFlag = 1 THEN AnnualizedPremium ELSE 0 END) AS ChurnedAnnualizedPremium
    FROM segments
    GROUP BY SegmentType, SegmentValue
)
SELECT SegmentType, SegmentValue, MemberMonths, ChurnedMembers,
       ChurnedMembers * 1.0 / NULLIF(MemberMonths, 0) AS MonthlyChurnRate,
       -- compound the monthly rate to a yearly equivalent so segments compare with the annual KPI
       1.0 - POWER(1.0 - CAST(ChurnedMembers * 1.0 / NULLIF(MemberMonths, 0) AS FLOAT), 12) AS AnnualizedChurnRate,
       ChurnedAnnualizedPremium
FROM agg;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_CohortRetention AS
SELECT CohortMonth, MonthsSinceEnrollment, CohortSize, ActiveMembers, RetentionRate
FROM dbo.CohortRetention;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_CohortMilestones AS
SELECT CohortMonth, CohortSize,
       MAX(CASE WHEN MonthsSinceEnrollment = 3  THEN RetentionRate END) AS Retention3M,
       MAX(CASE WHEN MonthsSinceEnrollment = 6  THEN RetentionRate END) AS Retention6M,
       MAX(CASE WHEN MonthsSinceEnrollment = 12 THEN RetentionRate END) AS Retention12M
FROM dbo.CohortRetention
GROUP BY CohortMonth, CohortSize;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_AtRiskMembers AS
WITH latest AS (
    SELECT MAX(MonthStart) AS LastMonth FROM dbo.MemberMonthFeatures
)
SELECT m.MemberID, m.AgeBand, ISNULL(m.Region, 'Unknown') AS Region,
       p.PlanName, p.PlanTier,
       f.MonthStart AS AsOfMonth, f.TenureMonths, f.MonthlyPremium,
       f.LastRenewalMonth, f.LastRenewalChangePct, f.MaxPremiumChange3M, f.Denied3M, f.Claims3M, f.DaysSinceLastClaim, f.IsDisengaged,
       f.AnnualizedPremium AS AnnualizedPremiumAtRisk,
       ROW_NUMBER() OVER (ORDER BY f.AnnualizedPremium DESC, f.Denied3M DESC, m.MemberID) AS PriorityRank
FROM dbo.MemberMonthFeatures f
JOIN latest l        ON f.MonthStart = l.LastMonth
JOIN dbo.DimMember m ON m.MemberSK   = f.MemberSK
JOIN dbo.DimPlan   p ON p.PlanSK     = f.PlanSK
WHERE f.IsActive = 1 AND f.IsChurnFlag = 0 AND f.IsAtRisk = 1;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_RenewalIncreaseChurn AS
WITH seg AS (
    SELECT f.IsActive, f.IsChurnFlag,
           CASE WHEN f.LastRenewalChangePct IS NULL THEN 0
                WHEN f.LastRenewalChangePct <= 0    THEN 1
                WHEN f.LastRenewalChangePct <  0.05 THEN 2
                WHEN f.LastRenewalChangePct <  0.10 THEN 3
                WHEN f.LastRenewalChangePct <  0.15 THEN 4
                ELSE 5 END AS BandOrder
    FROM dbo.MemberMonthFeatures f
    JOIN dbo.vw_ObservationWindow w ON f.MonthStart BETWEEN w.WindowStart AND w.WindowEnd
)
SELECT CASE BandOrder WHEN 0 THEN 'No renewal observed'
                      WHEN 1 THEN 'No increase'
                      WHEN 2 THEN '0-5%'
                      WHEN 3 THEN '5-10%'
                      WHEN 4 THEN '10-15%'
                      ELSE '15%+' END AS RenewalIncreaseBand,
       BandOrder,
       SUM(CAST(IsActive AS INT))    AS MemberMonths,
       SUM(CAST(IsChurnFlag AS INT)) AS ChurnedMembers,
       SUM(CAST(IsChurnFlag AS INT)) * 1.0 / NULLIF(SUM(CAST(IsActive AS INT)), 0) AS MonthlyChurnRate
FROM seg
GROUP BY BandOrder;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_TerminationReasons AS
SELECT DATEFROMPARTS(YEAR(TerminationDate), MONTH(TerminationDate), 1) AS TerminationMonth,
       TerminationReason,
       CASE WHEN TerminationReason IN ('Voluntary', 'NonRenewal') THEN 'Voluntary'
            WHEN TerminationReason = 'NonPayment'                 THEN 'Involuntary'
            WHEN TerminationReason = 'Deceased'                   THEN 'Death'
            ELSE 'Other' END AS ChurnCategory,
       COUNT(*) AS Members
FROM dbo.DimMember
WHERE TerminationDate IS NOT NULL
GROUP BY DATEFROMPARTS(YEAR(TerminationDate), MONTH(TerminationDate), 1), TerminationReason;
GO

CREATE OR ALTER VIEW dbo.vw_Rpt_ExecutiveSummary AS
SELECT a.WindowStart, a.WindowEnd, a.WindowMonths, a.IsWindowComplete,
       a.CohortMembers, a.ChurnedMembers, a.AnnualChurnRate, a.AnnualRetentionRate,
       (SELECT mc.ActiveMembers FROM dbo.vw_Rpt_MonthlyChurn mc WHERE mc.MonthStart = a.WindowEnd) AS ActiveMembersLatestMonth,
       (SELECT SUM(m2.ChurnedMembers) * 1.0 / NULLIF(SUM(m2.ActiveMembers), 0) FROM dbo.vw_MonthlyChurn m2) AS AvgMonthlyChurnRate,
       (SELECT SUM(f.AnnualizedPremium) FROM dbo.MemberMonthFeatures f
         WHERE f.IsChurnFlag = 1 AND f.MonthStart BETWEEN a.WindowStart AND a.WindowEnd)          AS ChurnedAnnualizedPremium,
       (SELECT COUNT(*) FROM dbo.vw_Rpt_AtRiskMembers)                                            AS AtRiskMembers,
       (SELECT SUM(r.AnnualizedPremiumAtRisk) FROM dbo.vw_Rpt_AtRiskMembers r)                    AS AnnualizedPremiumAtRisk
FROM dbo.vw_AnnualChurn a;
GO

-- SELECT * FROM dbo.vw_Rpt_ExecutiveSummary;
-- SELECT * FROM dbo.vw_Rpt_MonthlyChurn ORDER BY MonthStart;
-- SELECT * FROM dbo.vw_Rpt_SegmentChurn ORDER BY SegmentType, MonthlyChurnRate DESC;
-- SELECT TOP 25 * FROM dbo.vw_Rpt_AtRiskMembers ORDER BY PriorityRank;
-- SELECT * FROM dbo.vw_Rpt_RenewalIncreaseChurn ORDER BY BandOrder;
