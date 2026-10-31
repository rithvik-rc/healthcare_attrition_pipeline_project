-- =====================================================================
-- Phase 2: Staging layer, FactClaims, QuarantineLog, DimDate population
-- Run order: warehouse_star_schema.sql -> attrition_schema.sql -> this file
-- Privacy: de-identified / synthetic data only.
-- =====================================================================

IF DB_ID('WarehouseDB') IS NULL
    CREATE DATABASE WarehouseDB;
GO

USE WarehouseDB;
GO

-- Safety check: Phase 1 tables must exist
IF OBJECT_ID('dbo.DimMember', 'U') IS NULL OR OBJECT_ID('dbo.FactEnrollment', 'U') IS NULL
    THROW 50002, 'Phase 1 tables not found. Run attrition_schema.sql first.', 1;
GO

-- ---------------------------------------------------------------------
-- 1. Staging tables (landing zone; follows the raw_orders_stage pattern)
--    Data is validated in Python BEFORE it lands here (Phase 3), so these
--    tables carry primary keys but no business-rule CHECK constraints.
-- ---------------------------------------------------------------------
IF OBJECT_ID('dbo.stg_members', 'U') IS NULL
CREATE TABLE dbo.stg_members (
    member_id          VARCHAR(50)  NOT NULL PRIMARY KEY,
    age_band           VARCHAR(10)  NOT NULL,
    region             VARCHAR(50)  NULL,
    enrollment_date    DATE         NOT NULL,
    termination_date   DATE         NULL,
    termination_reason VARCHAR(30)  NULL,
    first_missed_payment_date DATE  NULL,      -- starts the 15-day grace period
    member_status      VARCHAR(15)  NOT NULL,
    batch_id           VARCHAR(36)  NOT NULL,
    loaded_at          DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('dbo.stg_enrollments', 'U') IS NULL
CREATE TABLE dbo.stg_enrollments (
    member_id       VARCHAR(50)    NOT NULL,
    plan_id         VARCHAR(50)    NOT NULL,
    month_start     DATE           NOT NULL,   -- first day of the month
    monthly_premium DECIMAL(10, 2) NOT NULL,
    is_active       BIT            NOT NULL,
    is_churn_flag   BIT            NOT NULL,
    batch_id        VARCHAR(36)    NOT NULL,
    loaded_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_stg_enrollments PRIMARY KEY (member_id, month_start)
);
GO

IF OBJECT_ID('dbo.stg_claims', 'U') IS NULL
CREATE TABLE dbo.stg_claims (
    claim_id     VARCHAR(20)    NOT NULL PRIMARY KEY,
    member_id    VARCHAR(50)    NOT NULL,
    claim_date   DATE           NOT NULL,
    claim_amount DECIMAL(10, 2) NOT NULL,
    claim_status VARCHAR(10)    NOT NULL,
    batch_id     VARCHAR(36)    NOT NULL,
    loaded_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_stg_claims_member' AND object_id = OBJECT_ID('dbo.stg_claims'))
    CREATE INDEX IX_stg_claims_member ON dbo.stg_claims(member_id);
GO

-- ---------------------------------------------------------------------
-- 2. FactClaims (deferred from Phase 1)
--    Grain: one row per claim.
-- ---------------------------------------------------------------------
IF OBJECT_ID('dbo.FactClaims', 'U') IS NULL
CREATE TABLE dbo.FactClaims (
    ClaimSK      BIGINT IDENTITY(1,1) PRIMARY KEY,
    ClaimID      VARCHAR(20)    NOT NULL UNIQUE,
    MemberSK     INT            NOT NULL,
    DateKey      INT            NOT NULL,
    ClaimAmount  DECIMAL(10, 2) NOT NULL,
    ClaimStatus  VARCHAR(10)    NOT NULL,
    LoadedAt     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_FactClaims_Member FOREIGN KEY (MemberSK) REFERENCES dbo.DimMember(MemberSK),
    CONSTRAINT FK_FactClaims_Date   FOREIGN KEY (DateKey)  REFERENCES dbo.DimDate(DateKey),
    CONSTRAINT CK_FactClaims_Status CHECK (ClaimStatus IN ('Paid', 'Denied', 'Pending')),
    CONSTRAINT CK_FactClaims_Amount CHECK (ClaimAmount > 0)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactClaims_MemberDate' AND object_id = OBJECT_ID('dbo.FactClaims'))
    CREATE INDEX IX_FactClaims_MemberDate ON dbo.FactClaims(MemberSK, DateKey)
        INCLUDE (ClaimStatus, ClaimAmount);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactClaims_DateKey' AND object_id = OBJECT_ID('dbo.FactClaims'))
    CREATE INDEX IX_FactClaims_DateKey ON dbo.FactClaims(DateKey);
GO

-- ---------------------------------------------------------------------
-- 3. QuarantineLog (persists rejected records instead of only logging)
-- ---------------------------------------------------------------------
IF OBJECT_ID('dbo.QuarantineLog', 'U') IS NULL
CREATE TABLE dbo.QuarantineLog (
    QuarantineID  BIGINT IDENTITY(1,1) PRIMARY KEY,
    SourceName    VARCHAR(50)    NOT NULL,       -- 'members' | 'enrollments' | 'claims'
    BatchID       VARCHAR(36)    NULL,
    RecordIndex   INT            NULL,           -- row position in the source file
    RawRecord     NVARCHAR(MAX)  NOT NULL,       -- original record as JSON text
    ErrorMessage  NVARCHAR(500)  NOT NULL,
    QuarantinedAt DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_QuarantineLog_Source' AND object_id = OBJECT_ID('dbo.QuarantineLog'))
    CREATE INDEX IX_QuarantineLog_Source ON dbo.QuarantineLog(SourceName, QuarantinedAt);
GO

-- ---------------------------------------------------------------------
-- 4. Populate DimDate (2024-01-01 .. 2027-12-31), idempotent.
--    The original star-schema script creates DimDate but does not fill it,
--    and FactEnrollment / FactClaims need these keys for their FKs.
-- ---------------------------------------------------------------------
SET DATEFIRST 7;   -- Sunday = 1, so DayOfWeek values don't depend on server language settings
;WITH d AS (
    SELECT CAST('2024-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt) FROM d WHERE dt < '2027-12-31'
)
INSERT INTO dbo.DimDate (DateKey, FullDate, [Year], [Quarter], [Month], [DayOfWeek])
SELECT
    CONVERT(INT, CONVERT(CHAR(8), dt, 112)),
    dt,
    YEAR(dt),
    DATEPART(QUARTER, dt),
    MONTH(dt),
    DATEPART(WEEKDAY, dt)
FROM d
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.DimDate x
    WHERE x.DateKey = CONVERT(INT, CONVERT(CHAR(8), d.dt, 112))
)
OPTION (MAXRECURSION 0);
GO

-- 5. Verification
-- SELECT 'DimDate' AS tbl, COUNT(*) AS n FROM dbo.DimDate          -- expect 1461
-- UNION ALL SELECT 'stg_members',     COUNT(*) FROM dbo.stg_members
-- UNION ALL SELECT 'stg_enrollments', COUNT(*) FROM dbo.stg_enrollments
-- UNION ALL SELECT 'stg_claims',      COUNT(*) FROM dbo.stg_claims
-- UNION ALL SELECT 'QuarantineLog',   COUNT(*) FROM dbo.QuarantineLog;
