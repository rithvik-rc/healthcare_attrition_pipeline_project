"""
Logic tests for attrition_ops.sql (the audit checks and run-history view) and for
vw_DQ_Checks, run on SQLite with the same mechanical translation as test_attrition_sql_logic.py.
Verifies check logic (what passes, what warns, what fails); not T-SQL syntax.
Run:  python -m unittest test_attrition_ops_logic -v
"""
import os
import importlib.util
import re
import sqlite3
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
# Source code lives in ../src (override with ATTRITION_SRC); tests live here.
SRC = Path(os.environ.get("ATTRITION_SRC", HERE.parent / "src")).resolve()
spec = importlib.util.spec_from_file_location("sl", HERE / "test_attrition_sql_logic.py")
sl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sl)


_orig_to_sqlite = sl.to_sqlite


def _to_sqlite(sql, threshold="0.1"):
    return _orig_to_sqlite(sql, threshold).replace("DATEDIFF(SECOND,", "DATEDIFF_SECOND(")


sl.to_sqlite = _to_sqlite


def extra_udfs(con):
    from datetime import datetime
    con.create_function("DATEDIFF_SECOND", 2, lambda a, b: None if a is None or b is None else
                        int((datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds()))
    con.create_function("DAY", 1, lambda d: None if d is None else sl._d(d).day)
    con.create_function("CONCAT", -1, lambda *a: "".join("" if x is None else str(x) for x in a))


class OpsLogic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sl.SqlLogic.setUpClass()
        cls.sql = sl.SqlLogic
        base = cls.sql.con
        extra_udfs(base)
        base.executescript("""
            CREATE INDEX ix_dm_id ON DimMember(MemberID);
            CREATE INDEX ix_fe ON FactEnrollment(MemberSK, DateKey);
            CREATE INDEX ix_fc ON FactClaims(ClaimID);
            CREATE TABLE stg_members (member_id TEXT, batch_id TEXT);
            CREATE TABLE stg_enrollments (member_id TEXT, month_start TEXT, batch_id TEXT);
            CREATE TABLE stg_claims (claim_id TEXT, batch_id TEXT);
            INSERT INTO stg_members SELECT MemberID, 'batch-1' FROM DimMember;
            INSERT INTO stg_enrollments SELECT m.MemberID, d.FullDate, 'batch-1'
                FROM FactEnrollment f JOIN DimMember m ON m.MemberSK = f.MemberSK
                JOIN DimDate d ON d.DateKey = f.DateKey;
            INSERT INTO stg_claims SELECT ClaimID, 'batch-1' FROM FactClaims;
            CREATE TABLE PipelineRunLog (RunID INTEGER PRIMARY KEY, BatchID TEXT, StartedAt TEXT, FinishedAt TEXT,
                Status TEXT, TriggeredBy TEXT, RowsRead INT, RowsQuarantined INT, RowsStaged INT,
                MemberInserts INT, MemberUpdates INT, EnrollmentInserts INT, EnrollmentUpdates INT,
                ClaimInserts INT, ClaimUpdates INT, FeatureRows INT, AnnualChurnRate REAL, ErrorMessage TEXT);
            CREATE TABLE PipelineAuditResult (AuditID INTEGER PRIMARY KEY AUTOINCREMENT, RunID INT, CheckName TEXT,
                Status TEXT, Detail TEXT, CheckedAt TEXT);
            INSERT INTO PipelineRunLog (RunID, BatchID, StartedAt, FinishedAt, Status, RowsRead, RowsQuarantined)
                VALUES (1, 'batch-1', '2026-10-25 02:00:00', '2026-10-25 02:00:42', 'Running', 1000, 80);
        """)
        dq = sl.views_from(SRC / "attrition_dq_checks.sql")["vw_DQ_Checks"]
        base.executescript(sl.to_sqlite(f"CREATE VIEW vw_DQ_Checks AS {dq};"))
        ops = (SRC / "attrition_ops.sql").read_text()
        cls.audits_sql = sl.to_sqlite(sl.block(ops, "audits")).replace("@RunID", "1") \
            .replace("@BatchID", "'batch-1'").replace("@MinBps", "10").replace("@MaxBps", "500")
        hist = sl.views_from(SRC / "attrition_ops.sql")["vw_PipelineRunHistory"]
        base.executescript(sl.to_sqlite(f"CREATE VIEW vw_PipelineRunHistory AS {hist};"))

    def fresh(self):
        con = sqlite3.connect(":memory:")
        sl.register_udfs(con)
        extra_udfs(con)
        self.sql.con.backup(con)
        return con

    def audit(self, con):
        con.execute("DELETE FROM PipelineAuditResult")
        con.executescript(self.audits_sql)
        return pd.read_sql("SELECT CheckName, Status, Detail FROM PipelineAuditResult ORDER BY AuditID", con)

    # ---------------- clean data ----------------
    def test_clean_data_passes_every_check_in_order(self):
        r = self.audit(self.fresh())
        self.assertEqual(len(r), 10)
        self.assertTrue((r.Status == "PASS").all(), r.to_string())
        self.assertEqual(r.CheckName.iloc[0], "Staged members loaded")
        self.assertEqual(r.CheckName.iloc[-1], "Claims present in latest month")

    def test_dq_view_is_clean_on_clean_data(self):
        dq = pd.read_sql("SELECT * FROM vw_DQ_Checks", self.fresh())
        self.assertGreaterEqual(len(dq), 8)
        self.assertTrue((dq.FailingRows == 0).all(), dq.to_string())

    # ---------------- defects must be caught ----------------
    def status(self, con, name):
        r = self.audit(con)
        return r[r.CheckName == name].Status.iloc[0]

    def test_missing_fact_row_fails_reconciliation_and_feature_sync(self):
        con = self.fresh()
        con.execute("DELETE FROM FactEnrollment WHERE rowid = (SELECT MAX(rowid) FROM FactEnrollment)")
        r = self.audit(con).set_index("CheckName").Status
        self.assertEqual(r["Staged enrollments loaded"], "FAIL")
        self.assertEqual(r["Features match enrollment facts"], "FAIL")

    def test_missing_member_and_claim_fail(self):
        con = self.fresh()
        con.execute("DELETE FROM FactClaims WHERE rowid = (SELECT MAX(rowid) FROM FactClaims)")
        con.execute("DELETE FROM DimMember WHERE MemberSK = (SELECT MAX(MemberSK) FROM DimMember)")
        r = self.audit(con).set_index("CheckName").Status
        self.assertEqual(r["Staged claims loaded"], "FAIL")
        self.assertEqual(r["Staged members loaded"], "FAIL")

    def test_other_batch_rows_are_ignored(self):
        con = self.fresh()
        con.execute("UPDATE stg_members SET batch_id = 'old'")
        con.execute("DELETE FROM DimMember WHERE MemberSK = 1")
        self.assertEqual(self.status(con, "Staged members loaded"), "PASS")

    def test_dq_failure_is_reported(self):
        con = self.fresh()
        con.execute("UPDATE FactEnrollment SET IsChurnFlag = 1 WHERE rowid = "
                    "(SELECT rowid FROM FactEnrollment WHERE IsChurnFlag = 0 LIMIT 1)")
        self.assertEqual(self.status(con, "Data-quality checks (vw_DQ_Checks)"), "FAIL")

    def test_rising_retention_fails(self):
        con = self.fresh()
        con.execute("UPDATE CohortRetention SET RetentionRate = 0.5 WHERE CohortMonth = '2025-01-01' "
                    "AND MonthsSinceEnrollment = 1")
        con.execute("UPDATE CohortRetention SET RetentionRate = 0.9 WHERE CohortMonth = '2025-01-01' "
                    "AND MonthsSinceEnrollment = 2")
        self.assertEqual(self.status(con, "Cohort retention never rises with age"), "FAIL")

    def test_warnings(self):
        con = self.fresh()
        con.execute("UPDATE AttritionConfig SET ConfigValue = 24 WHERE ConfigKey = 'ObservationWindowMonths'")
        self.assertEqual(self.status(con, "Observation window complete"), "WARN")

        con = self.fresh()
        con.execute("UPDATE FactEnrollment SET IsChurnFlag = 0, IsActive = 1")      # zero churn: suspicious
        self.assertEqual(self.status(con, "Monthly churn within expected band"), "WARN")

        con = self.fresh()
        con.execute("DELETE FROM FactClaims WHERE DateKey >= 20260901")              # claims feed missing
        self.assertEqual(self.status(con, "Claims present in latest month"), "WARN")

        con = self.fresh()
        con.execute("DELETE FROM FactEnrollment WHERE DateKey = 20260901 AND IsChurnFlag = 0 AND rowid % 2 = 0")
        self.assertEqual(self.status(con, "Active members present in latest month"), "WARN")

    def test_rerun_replaces_results_for_the_run(self):
        con = self.fresh()
        self.audit(con)
        con.executescript(self.audits_sql)           # a second run on top, without the delete helper
        # the procedure's own DELETE is outside the block, so rows double here; emulate it as the proc does:
        con.execute("DELETE FROM PipelineAuditResult WHERE RunID = 1 AND CheckName <> 'Quarantine rate'")
        con.execute("INSERT INTO PipelineAuditResult (RunID, CheckName, Status, Detail) "
                    "VALUES (1, 'Quarantine rate', 'PASS', 'x')")
        con.executescript(self.audits_sql)
        n = pd.read_sql("SELECT COUNT(*) n FROM PipelineAuditResult", con).n.iloc[0]
        self.assertEqual(n, 11)                      # 10 audits + the Python row, not 21

    # ---------------- run history view ----------------
    def test_run_history_view(self):
        con = self.fresh()
        self.audit(con)
        con.execute("UPDATE FactClaims SET ClaimStatus = ClaimStatus")               # no-op
        con.execute("INSERT INTO PipelineAuditResult (RunID, CheckName, Status, Detail) VALUES (1, 'a', 'WARN', '')")
        con.execute("INSERT INTO PipelineAuditResult (RunID, CheckName, Status, Detail) VALUES (1, 'b', 'FAIL', '')")
        con.execute("INSERT INTO PipelineRunLog (RunID, Status, RowsRead, RowsQuarantined) VALUES (2, 'Succeeded', 0, 0)")
        h = pd.read_sql("SELECT * FROM vw_PipelineRunHistory ORDER BY RunID", con)
        r1, r2 = h.iloc[0], h.iloc[1]
        self.assertEqual((r1.TotalChecks, r1.FailedChecks, r1.WarningChecks), (12, 1, 1))
        self.assertAlmostEqual(r1.QuarantinePct, 8.0)
        self.assertEqual(r1.DurationSeconds, 42)
        self.assertEqual((r2.TotalChecks, r2.FailedChecks, r2.WarningChecks), (0, 0, 0))   # LEFT JOIN keeps empty runs
        self.assertTrue(pd.isna(r2.QuarantinePct))                                          # no divide by zero


if __name__ == "__main__":
    unittest.main()
