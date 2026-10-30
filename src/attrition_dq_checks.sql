-- =====================================================================
-- Data-quality checks for the attrition warehouse (Phase 1/2 objects).
-- Constraints cannot express rules that span tables, so this view does.
-- Phase 3 / Phase 5 audits should run:
--     SELECT * FROM dbo.vw_DQ_Checks WHERE FailingRows > 0;
-- An empty result means every check passes.
-- Run after attrition_churn_rules.sql.
-- =====================================================================

USE WarehouseDB;
GO

CREATE OR ALTER VIEW dbo.vw_DQ_Checks AS
SELECT CAST('Enrollment row after termination month' AS VARCHAR(80)) AS CheckName,
       COUNT(*) AS FailingRows
FROM dbo.FactEnrollment f
JOIN dbo.DimMember m ON m.MemberSK = f.MemberSK
JOIN dbo.DimDate   d ON d.DateKey  = f.DateKey
WHERE m.TerminationDate IS NOT NULL
  AND d.FullDate > DATEFROMPARTS(YEAR(m.TerminationDate), MONTH(m.TerminationDate), 1)

UNION ALL
SELECT 'Enrollment row before enrollment month', COUNT(*)
FROM dbo.FactEnrollment f
JOIN dbo.DimMember m ON m.MemberSK = f.MemberSK
JOIN dbo.DimDate   d ON d.DateKey  = f.DateKey
WHERE d.FullDate < DATEFROMPARTS(YEAR(m.EnrollmentDate), MONTH(m.EnrollmentDate), 1)

UNION ALL
SELECT 'Churn flag outside termination month', COUNT(*)
FROM dbo.FactEnrollment f
JOIN dbo.DimMember m ON m.MemberSK = f.MemberSK
JOIN dbo.DimDate   d ON d.DateKey  = f.DateKey
WHERE f.IsChurnFlag = 1
  AND (m.TerminationDate IS NULL
       OR d.FullDate <> DATEFROMPARTS(YEAR(m.TerminationDate), MONTH(m.TerminationDate), 1))

UNION ALL
SELECT 'Member with more than one churn flag', COUNT(*)
FROM (SELECT MemberSK FROM dbo.FactEnrollment
      WHERE IsChurnFlag = 1
      GROUP BY MemberSK HAVING COUNT(*) > 1) AS x

UNION ALL
SELECT 'Terminated member (inside loaded months) without churn flag', COUNT(*)
FROM dbo.DimMember m
WHERE m.TerminationDate >= (SELECT MIN(d2.FullDate)
                            FROM dbo.FactEnrollment f2
                            JOIN dbo.DimDate d2 ON d2.DateKey = f2.DateKey)
  AND NOT EXISTS (SELECT 1 FROM dbo.FactEnrollment f3
                  WHERE f3.MemberSK = m.MemberSK AND f3.IsChurnFlag = 1)

UNION ALL
SELECT 'Claim before enrollment date', COUNT(*)
FROM dbo.FactClaims c
JOIN dbo.DimMember m ON m.MemberSK = c.MemberSK
JOIN dbo.DimDate   d ON d.DateKey  = c.DateKey
WHERE d.FullDate < m.EnrollmentDate

UNION ALL
SELECT 'Claim after termination date', COUNT(*)
FROM dbo.FactClaims c
JOIN dbo.DimMember m ON m.MemberSK = c.MemberSK
JOIN dbo.DimDate   d ON d.DateKey  = c.DateKey
WHERE m.TerminationDate IS NOT NULL AND d.FullDate > m.TerminationDate

UNION ALL
SELECT 'Missing months inside loaded enrollment range',
       ISNULL((DATEDIFF(MONTH, MIN(d.FullDate), MAX(d.FullDate)) + 1) - COUNT(DISTINCT d.FullDate), 0)
FROM dbo.FactEnrollment f
JOIN dbo.DimDate d ON d.DateKey = f.DateKey;
GO

-- SELECT * FROM dbo.vw_DQ_Checks;                       -- all checks
-- SELECT * FROM dbo.vw_DQ_Checks WHERE FailingRows > 0; -- failures only
