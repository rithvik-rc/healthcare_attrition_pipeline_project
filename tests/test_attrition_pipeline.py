"""Unit tests for the pure helpers in attrition_pipeline.py (no database needed).
Run:  python -m unittest test_attrition_pipeline -v
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


class QuarantineRate(unittest.TestCase):
    def test_pass_warn_fail_bands(self):
        self.assertEqual(pipe.quarantine_rate_check(1000, 0)[0], "PASS")
        self.assertEqual(pipe.quarantine_rate_check(1000, 50)[0], "PASS")     # exactly the 5% limit
        self.assertEqual(pipe.quarantine_rate_check(1000, 51)[0], "WARN")
        self.assertEqual(pipe.quarantine_rate_check(1000, 501)[0], "FAIL")    # >50%: wrong file
        self.assertEqual(pipe.quarantine_rate_check(1000, 80, max_pct=10)[0], "PASS")

    def test_empty_input_does_not_divide_by_zero(self):
        self.assertEqual(pipe.quarantine_rate_check(0, 0)[0], "PASS")


class DecideStatus(unittest.TestCase):
    ok = [("a", "PASS", ""), ("b", "PASS", "")]

    def test_all_pass(self):
        self.assertEqual(pipe.decide_status(self.ok), ("Succeeded", 0))

    def test_warning_is_ok_unless_strict(self):
        rows = self.ok + [("c", "WARN", "")]
        self.assertEqual(pipe.decide_status(rows), ("Warnings", 0))
        self.assertEqual(pipe.decide_status(rows, strict=True), ("Warnings", 1))

    def test_fail_beats_warn(self):
        rows = self.ok + [("c", "WARN", ""), ("d", "FAIL", "")]
        self.assertEqual(pipe.decide_status(rows), ("Failed", 1))

    def test_no_audits_is_success(self):
        self.assertEqual(pipe.decide_status([]), ("Succeeded", 0))


class UpdateSql(unittest.TestCase):
    def test_builds_parameterised_update(self):
        sql = pipe.build_update_sql(["Status", "RowsRead"])
        self.assertEqual(sql, "UPDATE dbo.PipelineRunLog SET Status = ?, RowsRead = ? WHERE RunID = ?;")

    def test_rejects_unknown_columns(self):
        with self.assertRaises(ValueError):
            pipe.build_update_sql(["Status; DROP TABLE x"])


if __name__ == "__main__":
    unittest.main()
