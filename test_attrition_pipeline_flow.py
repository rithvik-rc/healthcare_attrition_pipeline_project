"""
Flow tests for attrition_pipeline.run() with a scripted fake database connection:
exit codes, run-log updates, locking and failure handling.
Run:  python -m unittest test_attrition_pipeline_flow -v
"""
import os
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).parent
# Source code lives in ../src (override with ATTRITION_SRC); tests live here.
SRC = Path(os.environ.get("ATTRITION_SRC", HERE.parent / "src")).resolve()
sys.path.insert(0, str(SRC))
import attrition_pipeline as pipe


class Db:
    """Shared recorder + scripted answers for every cursor the pipeline opens."""

    def __init__(self, lock_result=0, audits=None, missing_object=None):
        self.calls, self.rolled_back = [], False
        self.lock_result, self.missing_object = lock_result, missing_object
        self.audits = audits if audits is not None else [("Staged members loaded", "PASS", "0 missing")]

    def connect(self, autocommit=False):
        return Conn(self)


class Conn:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return Cur(self.db)

    def commit(self):
        pass

    def rollback(self):
        self.db.rolled_back = True

    def close(self):
        pass


class Cur:
    def __init__(self, db):
        self.db, self.rows, self.description = db, [], None

    def execute(self, sql, *params):
        db, s = self.db, " ".join(sql.split())
        db.calls.append((s, params))
        self.rows = []
        if "sp_getapplock" in s:
            self.rows = [(db.lock_result,)]
        elif s.startswith("SELECT OBJECT_ID"):
            missing = db.missing_object and params[0] == f"dbo.{db.missing_object}"
            self.rows = [(None if missing else 1,)]
        elif "OUTPUT INSERTED.RunID" in s:
            self.rows = [(42,)]
        elif "usp_BuildAttritionMetrics" in s:
            self.rows = [("MemberMonthFeatures", 1000), ("CohortRetention", 50)]
        elif "usp_RunAttritionAudits" in s:
            self.rows = list(db.audits)
        elif "MaxQuarantinePct" in s:
            self.rows = [(5,)]
        elif "vw_Rpt_ExecutiveSummary" in s:
            self.rows = [("2025-10-01", "2026-09-01", 1, 0.127, 0.873, 1500, 250000.0, 12, 90000.0)]
        return self

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def nextset(self):
        return False

    def close(self):
        pass


GOOD_STATS = {"rows_read": 1000, "rows_quarantined": 10, "rows_staged": 990,
              "merge": {"DimMember": (5, 1), "FactEnrollment": (100, 2), "FactClaims": (50, 0)}}


class PipelineFlow(unittest.TestCase):
    def setUp(self):
        self.orig = pipe.ingest.run_ingest
        pipe.ingest.run_ingest = lambda conn, cur, in_dir, batch_id, log=print: dict(GOOD_STATS)

    def tearDown(self):
        pipe.ingest.run_ingest = self.orig

    def run_with(self, db, *argv):
        return pipe.run(pipe.parse_args(["--triggered-by", "test", *argv]), connect=db.connect)

    def updates(self, db):
        return [(s, p) for s, p in db.calls if s.startswith("UPDATE dbo.PipelineRunLog")]

    def test_success_exit_0_and_run_log_closed(self):
        db = Db()
        self.assertEqual(self.run_with(db), pipe.EXIT_OK)
        last_sql, last_params = self.updates(db)[-1]
        self.assertIn("Status = ?", last_sql)
        self.assertEqual(last_params[0], "Succeeded")
        self.assertAlmostEqual(last_params[-2], 0.127)          # AnnualChurnRate
        self.assertEqual(last_params[-1], 42)                    # RunID
        counts = " ".join(s for s, _ in self.updates(db))
        for col in ("RowsRead", "RowsQuarantined", "MemberInserts", "ClaimUpdates", "FeatureRows"):
            self.assertIn(col, counts)
        self.assertTrue(any("Quarantine rate" in str(p) for s, p in db.calls if "PipelineAuditResult" in s))

    def test_failed_audit_exit_1(self):
        db = Db(audits=[("Features match enrollment facts", "FAIL", "rows differ")])
        self.assertEqual(self.run_with(db), pipe.EXIT_AUDIT_FAILED)
        self.assertEqual(self.updates(db)[-1][1][0], "Failed")

    def test_warning_is_ok_unless_strict(self):
        db = Db(audits=[("Observation window complete", "WARN", "9 of 12")])
        self.assertEqual(self.run_with(db), pipe.EXIT_OK)
        self.assertEqual(self.updates(db)[-1][1][0], "Warnings")
        self.assertEqual(self.run_with(Db(audits=db.audits), "--strict"), pipe.EXIT_AUDIT_FAILED)

    def test_second_run_is_locked_out(self):
        db = Db(lock_result=-1)
        self.assertEqual(self.run_with(db), pipe.EXIT_LOCKED)
        self.assertFalse(any("INSERT INTO dbo.PipelineRunLog" in s for s, _ in db.calls))

    def test_pipeline_error_exit_2_records_the_error(self):
        def boom(*a, **k):
            raise RuntimeError("disk full")
        pipe.ingest.run_ingest = boom
        db = Db()
        self.assertEqual(self.run_with(db), pipe.EXIT_ERROR)
        self.assertTrue(db.rolled_back)
        sql, params = self.updates(db)[-1]
        self.assertIn("ErrorMessage = ?", sql)
        self.assertIn("RuntimeError: disk full", params[-2])
        self.assertEqual(params[0], "Failed")

    def test_missing_object_stops_before_any_work(self):
        db = Db(missing_object="usp_RunAttritionAudits")
        self.assertEqual(self.run_with(db), pipe.EXIT_ERROR)
        self.assertFalse(any("INSERT INTO dbo.PipelineRunLog" in s for s, _ in db.calls))

    def test_skip_ingest_still_audits(self):
        db = Db()
        db_rows = []
        called = []
        pipe.ingest.run_ingest = lambda *a, **k: called.append(1)
        self.assertEqual(self.run_with(db, "--skip-ingest"), pipe.EXIT_OK)
        self.assertEqual(called, [])
        self.assertTrue(any("usp_BuildAttritionMetrics" in s for s, _ in db.calls))
        self.assertFalse(any("Quarantine rate" in str(p) for s, p in db.calls if "PipelineAuditResult" in s))


if __name__ == "__main__":
    unittest.main()
