-- =====================================================================
-- Phase 1: Healthcare insurance attrition schema (extends star schema)
-- Reuses dbo.DimDate from warehouse_star_schema.sql (no changes needed).
-- Churn rules: 15-day grace period, 12-month observation window
-- (see attrition_churn_rules.sql for the config table and window views).
-- Privacy: de-identified / synthetic data only. No names, DOB, SSN,
-- addresses or diagnosis fields. Age is stored as a band, region as
-- a coarse label.
-- =====================================================================

-- 1. Ensure Target Database Exists
IF DB_ID('WarehouseDB') IS NULL
    CREATE DATABASE WarehouseDB;
GO

USE WarehouseDB;
GO

-- The filtered unique index in section 6 requires these session options
-- (sqlcmd defaults QUOTED_IDENTIFIER to OFF unless run with -I).
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

-- 2. Safety check: DimDate must exist (created by warehouse_star_schema.sql)
IF OBJECT_ID('dbo.DimDate', 'U') IS NULL
    THROW 50001, 'dbo.DimDate not found. Run warehouse_star_schema.sql first.', 1;
GO

-- 3. DimMember (insurance analogue of DimCustomer)
--    Type 1 dimension: attributes are overwritten on change. If you later
--    need history (e.g., region moves), convert to Type 2 with
--    EffectiveFrom / EffectiveTo / IsCurrent columns.
IF OBJECT_ID('dbo.DimMember', 'U') IS NULL
CREATE TABLE dbo.DimMember (
    MemberSK          INT IDENTITY(1,1) PRIMARY KEY,
    MemberID          VARCHAR(50)  NOT NULL UNIQUE,   -- de-identified business key
    AgeBand           VARCHAR(10)  NOT NULL,          -- e.g., '18-25', '26-35'
    Region            VARCHAR(50)  NULL,
    EnrollmentDate    DATE         NOT NULL,          -- first coverage start
    TerminationDate   DATE         NULL,              -- NULL while active
    TerminationReason VARCHAR(30)  NULL,
    FirstMissedPaymentDate DATE    NULL,              -- first unpaid premium due date; starts the grace period
    MemberStatus      VARCHAR(15)  NOT NULL CONSTRAINT DF_DimMember_Status DEFAULT 'Active',

    CONSTRAINT CK_DimMember_MemberID
        CHECK (LEN(LTRIM(RTRIM(MemberID))) > 0),
    CONSTRAINT CK_DimMember_AgeBand
        CHECK (AgeBand IN ('0-17', '18-25', '26-35', '36-45', '46-55', '56-64', '65+')),
    CONSTRAINT CK_DimMember_Status
        CHECK (MemberStatus IN ('Active', 'Terminated', 'Lapsed')),
    CONSTRAINT CK_DimMember_TermReason
        CHECK (TerminationReason IS NULL
               OR TerminationReason IN ('NonRenewal', 'Voluntary', 'NonPayment', 'Deceased', 'Other')),
    CONSTRAINT CK_DimMember_DateOrder
        CHECK (TerminationDate IS NULL OR TerminationDate >= EnrollmentDate),
    -- Active members must have no termination date; others must have one
    CONSTRAINT CK_DimMember_StatusDate
        CHECK ((MemberStatus = 'Active' AND TerminationDate IS NULL)
            OR (MemberStatus <> 'Active' AND TerminationDate IS NOT NULL)),
    -- Grace period = 15 days (keep in sync with dbo.AttritionConfig 'GracePeriodDays').
    -- A non-payment lapse is only valid once 15 days have passed since the first
    -- missed payment; all other terminations must not carry a missed-payment date.
    CONSTRAINT CK_DimMember_NonPaymentGrace
        CHECK ((TerminationReason = 'NonPayment'
                AND FirstMissedPaymentDate IS NOT NULL
                AND TerminationDate >= DATEADD(DAY, 15, FirstMissedPaymentDate))
            OR (ISNULL(TerminationReason, '') <> 'NonPayment'
                AND FirstMissedPaymentDate IS NULL)),
    -- 'Lapsed' means coverage ended for non-payment after the grace period
    CONSTRAINT CK_DimMember_LapsedReason
        CHECK ((MemberStatus = 'Lapsed' AND TerminationReason = 'NonPayment')
            OR (MemberStatus <> 'Lapsed' AND ISNULL(TerminationReason, '') <> 'NonPayment'))
);
GO

-- 4. DimPlan (insurance analogue of DimProduct)
IF OBJECT_ID('dbo.DimPlan', 'U') IS NULL
CREATE TABLE dbo.DimPlan (
    PlanSK         INT IDENTITY(1,1) PRIMARY KEY,
    PlanID         VARCHAR(50)  NOT NULL UNIQUE,
    PlanName       VARCHAR(100) NOT NULL,
    PlanTier       VARCHAR(10)  NOT NULL,            -- metal tier
    PlanType       VARCHAR(10)  NOT NULL,            -- network type
    BaseMonthlyPremium DECIMAL(10, 2) NOT NULL,      -- list premium; actual billed premium is on the fact

    CONSTRAINT CK_DimPlan_Tier
        CHECK (PlanTier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
    CONSTRAINT CK_DimPlan_Type
        CHECK (PlanType IN ('HMO', 'PPO', 'EPO', 'POS', 'HDHP')),
    CONSTRAINT CK_DimPlan_Premium
        CHECK (BaseMonthlyPremium > 0)
);
GO

-- 5. FactEnrollment
--    Grain: one row per member per month.
--    DateKey must be the FIRST day of the month (yyyymm01) so month
--    joins to DimDate are unambiguous.
--
--    IsActive     = 1 if the member had coverage on the first day of the month
--    IsChurnFlag  = 1 if coverage ended during this month
--    Monthly churn rate = SUM(IsChurnFlag) / SUM(IsActive)
IF OBJECT_ID('dbo.FactEnrollment', 'U') IS NULL
CREATE TABLE dbo.FactEnrollment (
    EnrollmentFactID BIGINT IDENTITY(1,1) PRIMARY KEY,
    MemberSK         INT NOT NULL,
    PlanSK           INT NOT NULL,
    DateKey          INT NOT NULL,
    MonthlyPremium   DECIMAL(10, 2) NOT NULL,        -- premium actually billed that month
    IsActive         BIT NOT NULL CONSTRAINT DF_FactEnrollment_IsActive DEFAULT 1,
    IsChurnFlag      BIT NOT NULL CONSTRAINT DF_FactEnrollment_IsChurn DEFAULT 0,
    LoadedAt         DATETIME2 NOT NULL CONSTRAINT DF_FactEnrollment_LoadedAt DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_FactEnrollment_Member FOREIGN KEY (MemberSK) REFERENCES dbo.DimMember(MemberSK),
    CONSTRAINT FK_FactEnrollment_Plan   FOREIGN KEY (PlanSK)   REFERENCES dbo.DimPlan(PlanSK),
    CONSTRAINT FK_FactEnrollment_Date   FOREIGN KEY (DateKey)  REFERENCES dbo.DimDate(DateKey),

    -- One row per member per month (also makes MERGE upserts idempotent)
    CONSTRAINT UQ_FactEnrollment_MemberMonth UNIQUE (MemberSK, DateKey),
    CONSTRAINT CK_FactEnrollment_MonthStart CHECK (DateKey % 100 = 1),
    CONSTRAINT CK_FactEnrollment_Premium    CHECK (MonthlyPremium >= 0),
    -- A churn event can only occur in a month the member was active
    CONSTRAINT CK_FactEnrollment_ChurnActive CHECK (IsChurnFlag = 0 OR IsActive = 1)
);
GO

-- 6. Secondary indexes
--    (MemberSK, DateKey) is already indexed by the UNIQUE constraint.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactEnrollment_PlanSK' AND object_id = OBJECT_ID('dbo.FactEnrollment'))
    CREATE INDEX IX_FactEnrollment_PlanSK ON dbo.FactEnrollment(PlanSK);

-- Covers monthly churn-rate and revenue queries without touching the base table
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactEnrollment_DateKey' AND object_id = OBJECT_ID('dbo.FactEnrollment'))
    CREATE INDEX IX_FactEnrollment_DateKey ON dbo.FactEnrollment(DateKey)
        INCLUDE (PlanSK, MemberSK, IsActive, IsChurnFlag, MonthlyPremium);

-- A member can churn only once: at most one IsChurnFlag = 1 row per member
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_FactEnrollment_OneChurn' AND object_id = OBJECT_ID('dbo.FactEnrollment'))
    CREATE UNIQUE INDEX UX_FactEnrollment_OneChurn ON dbo.FactEnrollment(MemberSK) WHERE IsChurnFlag = 1;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DimMember_Status' AND object_id = OBJECT_ID('dbo.DimMember'))
    CREATE INDEX IX_DimMember_Status ON dbo.DimMember(MemberStatus) INCLUDE (EnrollmentDate, TerminationDate);
GO

-- 7. Verification: confirm all three tables and their constraints exist
-- SELECT t.name AS table_name, COUNT(c.object_id) AS check_constraints
-- FROM sys.tables t
-- LEFT JOIN sys.check_constraints c ON c.parent_object_id = t.object_id
-- WHERE t.name IN ('DimMember', 'DimPlan', 'FactEnrollment')
-- GROUP BY t.name;
