-- =====================================================================
-- Phase 3: staging -> warehouse (idempotent MERGE upserts)
-- Run after attrition_staging.sql and attrition_churn_rules.sql
-- (deploy_attrition.sql runs it for you).
--
--   EXEC dbo.usp_LoadAttritionWarehouse @BatchID = '<batch id>';
--
-- What it does, in ONE transaction (all or nothing):
--   1. MERGE dbo.DimMember      from stg_members      (Type 1 upsert)
--   2. Pre-check: every staged enrollment/claim must resolve to a member, plan
--      and date; otherwise THROW before loading (no silent inner-join row loss)
--   2b. Clear a member's old churn flag when this batch moves or removes it (one churn row per member)
--   3. MERGE dbo.FactEnrollment from stg_enrollments  (grain: member + month)
--   4. MERGE dbo.FactClaims     from stg_claims       (grain: claim)
--   5. Returns one row per table: Inserted / Updated counts
--
-- Idempotent: rerunning the same batch inserts and updates 0 rows, because
-- WHEN MATCHED only fires when a column really changed (NULL-safe EXCEPT test).
-- =====================================================================

USE WarehouseDB;
GO

-- Filtered index on FactEnrollment: any procedure that writes to it must be created with these ON
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.stg_members', 'U') IS NULL
   OR OBJECT_ID('dbo.DimMember', 'U') IS NULL
   OR OBJECT_ID('dbo.FactClaims', 'U') IS NULL
    THROW 50004, 'Required tables missing. Run attrition_schema.sql and attrition_staging.sql first.', 1;
GO

CREATE OR ALTER PROCEDURE dbo.usp_LoadAttritionWarehouse
    @BatchID VARCHAR(36)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;   -- roll back the whole transaction automatically on runtime errors

    -- Capture MERGE actions ($action = INSERT / UPDATE) to report counts
    DECLARE @m TABLE (Act NVARCHAR(10));
    DECLARE @e TABLE (Act NVARCHAR(10));
    DECLARE @c TABLE (Act NVARCHAR(10));

    IF NOT EXISTS (SELECT 1 FROM dbo.stg_members     WHERE batch_id = @BatchID)
       AND NOT EXISTS (SELECT 1 FROM dbo.stg_enrollments WHERE batch_id = @BatchID)
       AND NOT EXISTS (SELECT 1 FROM dbo.stg_claims      WHERE batch_id = @BatchID)
        THROW 50010, 'No staged rows found for the given BatchID.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. DimMember: Type 1 upsert on the business key (MemberID)
        MERGE dbo.DimMember WITH (HOLDLOCK) AS tgt
        USING (
            SELECT member_id, age_band, region, enrollment_date, termination_date,
                   termination_reason, first_missed_payment_date, member_status
            FROM dbo.stg_members
            WHERE batch_id = @BatchID
        ) AS src
            ON tgt.MemberID = src.member_id
        WHEN MATCHED AND EXISTS (          -- NULL-safe "did anything change?" test
            SELECT src.age_band, src.region, src.enrollment_date, src.termination_date,
                   src.termination_reason, src.first_missed_payment_date, src.member_status
            EXCEPT
            SELECT tgt.AgeBand, tgt.Region, tgt.EnrollmentDate, tgt.TerminationDate,
                   tgt.TerminationReason, tgt.FirstMissedPaymentDate, tgt.MemberStatus
        ) THEN
            UPDATE SET
                tgt.AgeBand                = src.age_band,
                tgt.Region                 = src.region,
                tgt.EnrollmentDate         = src.enrollment_date,
                tgt.TerminationDate        = src.termination_date,
                tgt.TerminationReason      = src.termination_reason,
                tgt.FirstMissedPaymentDate = src.first_missed_payment_date,
                tgt.MemberStatus           = src.member_status
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (MemberID, AgeBand, Region, EnrollmentDate, TerminationDate,
                    TerminationReason, FirstMissedPaymentDate, MemberStatus)
            VALUES (src.member_id, src.age_band, src.region, src.enrollment_date, src.termination_date,
                    src.termination_reason, src.first_missed_payment_date, src.member_status)
        OUTPUT $action INTO @m (Act);

        -- 2. Referential pre-checks (an INNER JOIN would silently drop unmatched rows)
        IF EXISTS (
            SELECT 1
            FROM dbo.stg_enrollments s
            LEFT JOIN dbo.DimMember m ON m.MemberID = s.member_id
            LEFT JOIN dbo.DimPlan   p ON p.PlanID   = s.plan_id
            LEFT JOIN dbo.DimDate   d ON d.DateKey  = CONVERT(INT, CONVERT(CHAR(8), s.month_start, 112))
            WHERE s.batch_id = @BatchID
              AND (m.MemberSK IS NULL OR p.PlanSK IS NULL OR d.DateKey IS NULL)
        )
            THROW 50011, 'Staged enrollments reference an unknown member, plan or date. Nothing was loaded.', 1;

        IF EXISTS (
            SELECT 1
            FROM dbo.stg_claims s
            LEFT JOIN dbo.DimMember m ON m.MemberID = s.member_id
            LEFT JOIN dbo.DimDate   d ON d.DateKey  = CONVERT(INT, CONVERT(CHAR(8), s.claim_date, 112))
            WHERE s.batch_id = @BatchID
              AND (m.MemberSK IS NULL OR d.DateKey IS NULL)
        )
            THROW 50012, 'Staged claims reference an unknown member or date. Nothing was loaded.', 1;

        -- 2b. Corrections: a member can have only one churn row (UX_FactEnrollment_OneChurn). If this batch
        --     moves a member's churn to a different month (or removes it), clear the old flag FIRST;
        --     otherwise the MERGE below would try to hold two churn rows at once and fail with error 2601.
        --     The MERGE then applies the corrected rows as ordinary updates / inserts.
        UPDATE f
        SET f.IsChurnFlag = 0,
            f.LoadedAt    = SYSUTCDATETIME()
        FROM dbo.FactEnrollment f
        JOIN dbo.DimMember m ON m.MemberSK = f.MemberSK
        WHERE f.IsChurnFlag = 1
          AND EXISTS (SELECT 1 FROM dbo.stg_enrollments s
                      WHERE s.batch_id = @BatchID AND s.member_id = m.MemberID)
          AND NOT EXISTS (SELECT 1 FROM dbo.stg_enrollments s
                          WHERE s.batch_id = @BatchID AND s.member_id = m.MemberID
                            AND s.is_churn_flag = 1
                            AND f.DateKey = CONVERT(INT, CONVERT(CHAR(8), s.month_start, 112)));

        -- 3. FactEnrollment: grain = member + month; surrogate keys resolved by joins
        MERGE dbo.FactEnrollment WITH (HOLDLOCK) AS tgt
        USING (
            SELECT m.MemberSK, p.PlanSK, d.DateKey,
                   s.monthly_premium, s.is_active, s.is_churn_flag
            FROM dbo.stg_enrollments s
            JOIN dbo.DimMember m ON m.MemberID = s.member_id
            JOIN dbo.DimPlan   p ON p.PlanID   = s.plan_id
            JOIN dbo.DimDate   d ON d.DateKey  = CONVERT(INT, CONVERT(CHAR(8), s.month_start, 112))
            WHERE s.batch_id = @BatchID
        ) AS src
            ON tgt.MemberSK = src.MemberSK AND tgt.DateKey = src.DateKey
        WHEN MATCHED AND EXISTS (
            SELECT src.PlanSK, src.monthly_premium, src.is_active, src.is_churn_flag
            EXCEPT
            SELECT tgt.PlanSK, tgt.MonthlyPremium, tgt.IsActive, tgt.IsChurnFlag
        ) THEN
            UPDATE SET
                tgt.PlanSK         = src.PlanSK,
                tgt.MonthlyPremium = src.monthly_premium,
                tgt.IsActive       = src.is_active,
                tgt.IsChurnFlag    = src.is_churn_flag,
                tgt.LoadedAt       = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (MemberSK, PlanSK, DateKey, MonthlyPremium, IsActive, IsChurnFlag)
            VALUES (src.MemberSK, src.PlanSK, src.DateKey, src.monthly_premium, src.is_active, src.is_churn_flag)
        OUTPUT $action INTO @e (Act);

        -- 4. FactClaims: grain = claim; a claim's status/amount can change (e.g., Pending -> Paid)
        MERGE dbo.FactClaims WITH (HOLDLOCK) AS tgt
        USING (
            SELECT s.claim_id, m.MemberSK, d.DateKey, s.claim_amount, s.claim_status
            FROM dbo.stg_claims s
            JOIN dbo.DimMember m ON m.MemberID = s.member_id
            JOIN dbo.DimDate   d ON d.DateKey  = CONVERT(INT, CONVERT(CHAR(8), s.claim_date, 112))
            WHERE s.batch_id = @BatchID
        ) AS src
            ON tgt.ClaimID = src.claim_id
        WHEN MATCHED AND EXISTS (
            SELECT src.MemberSK, src.DateKey, src.claim_amount, src.claim_status
            EXCEPT
            SELECT tgt.MemberSK, tgt.DateKey, tgt.ClaimAmount, tgt.ClaimStatus
        ) THEN
            UPDATE SET
                tgt.MemberSK    = src.MemberSK,
                tgt.DateKey     = src.DateKey,
                tgt.ClaimAmount = src.claim_amount,
                tgt.ClaimStatus = src.claim_status,
                tgt.LoadedAt    = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ClaimID, MemberSK, DateKey, ClaimAmount, ClaimStatus)
            VALUES (src.claim_id, src.MemberSK, src.DateKey, src.claim_amount, src.claim_status)
        OUTPUT $action INTO @c (Act);

        COMMIT TRANSACTION;

        -- 5. Result: one row per table
        SELECT 'DimMember' AS TableName,
               COUNT(CASE WHEN Act = 'INSERT' THEN 1 END) AS Inserted,
               COUNT(CASE WHEN Act = 'UPDATE' THEN 1 END) AS Updated
        FROM @m
        UNION ALL
        SELECT 'FactEnrollment',
               COUNT(CASE WHEN Act = 'INSERT' THEN 1 END),
               COUNT(CASE WHEN Act = 'UPDATE' THEN 1 END)
        FROM @e
        UNION ALL
        SELECT 'FactClaims',
               COUNT(CASE WHEN Act = 'INSERT' THEN 1 END),
               COUNT(CASE WHEN Act = 'UPDATE' THEN 1 END)
        FROM @c;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;  -- re-raise the original error
    END CATCH
END;
GO

-- Usage:
-- EXEC dbo.usp_LoadAttritionWarehouse @BatchID = '<the batch_id printed by attrition_ingest.py>';
