"""
Unit tests for the Phase 3 validators (no database needed).
Run:  python -m unittest test_attrition_ingest -v
Uses generate_attrition_data.py in the same folder to build realistic clean and dirty data.
"""
import os
import importlib.util
import io
import sys
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
# Source code lives in ../src (override with ATTRITION_SRC); tests live here.
SRC = Path(os.environ.get("ATTRITION_SRC", HERE.parent / "src")).resolve()
sys.path.insert(0, str(SRC))


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SRC / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gen = _load("gen", "generate_attrition_data.py")
ing = _load("ing", "attrition_ingest.py")


def as_raw(df):
    """Round-trip through CSV text exactly like read_source() does."""
    return pd.read_csv(io.StringIO(df.to_csv(index=False)), dtype=str, keep_default_na=False)


def run_all(members, plans, enrollments, claims, db_members=None):
    plan_res = ing.validate_plans(as_raw(plans))
    m_res = ing.validate_members(as_raw(members))
    known = ing.build_known_members(m_res.clean, db_members)
    e_res = ing.validate_enrollments(as_raw(enrollments), known, set(plan_res.clean["plan_id"]))
    c_res = ing.validate_claims(as_raw(claims), known)
    return plan_res, m_res, e_res, c_res


class CleanData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.members, cls.plans, cls.enr, cls.claims = gen.simulate(600, 11)
        cls.res = run_all(cls.members, cls.plans, cls.enr, cls.claims)

    def test_nothing_rejected(self):
        for r in self.res:
            self.assertEqual(len(r.rejects), 0, r.rejects.head().to_string())

    def test_row_counts_preserved(self):
        _, m, e, c = self.res
        self.assertEqual((len(m.clean), len(e.clean), len(c.clean)),
                         (len(self.members), len(self.enr), len(self.claims)))

    def test_clean_types_ready_for_staging(self):
        _, m, e, _ = self.res
        self.assertTrue(all(hasattr(d, "year") for d in e.clean["month_start"]))
        self.assertTrue(set(e.clean["is_churn_flag"]) <= {0, 1})
        self.assertTrue(m.clean["member_id"].is_unique)

    def test_deterministic(self):
        again = run_all(self.members, self.plans, self.enr, self.claims)
        for a, b in zip(self.res, again):
            pd.testing.assert_frame_equal(a.clean, b.clean)


class InjectedBadData(unittest.TestCase):
    K = 3

    @classmethod
    def setUpClass(cls):
        m, p, e, c = gen.simulate(600, 11)
        cls.orig = (len(m), len(e), len(c))
        bm, be, bc = gen.inject_bad_rows(m, e, c, cls.K)
        cls.res = run_all(bm, p, be, bc)

    def test_bad_rows_quarantined_and_good_rows_kept(self):
        _, m, e, c = self.res
        self.assertEqual(len(m.rejects), 3 * self.K)       # before-enrollment, in-grace, no missed date
        self.assertEqual(len(e.rejects), 3 * self.K)       # duplicate, negative premium, unknown plan
        self.assertEqual(len(c.rejects), 2 * self.K)       # negative amount, missing member
        self.assertEqual((len(m.clean), len(e.clean), len(c.clean)), self.orig)

    def test_expected_reasons_present(self):
        _, m, e, c = self.res
        text = lambda r: " | ".join(r.rejects["error_message"])
        self.assertIn("termination_date before enrollment_date", text(m))
        self.assertIn("grace period", text(m))
        self.assertIn("no first_missed_payment_date", text(m))
        self.assertIn("duplicate (member_id, month_start)", text(e))
        self.assertIn("negative monthly_premium", text(e))
        self.assertIn("unknown plan_id", text(e))
        self.assertIn("claim_amount must be > 0", text(c))
        self.assertIn("member_id missing", text(c))

    def test_rejects_keep_raw_record_and_position(self):
        _, m, _, _ = self.res
        row = m.rejects.iloc[0]
        self.assertIn("member_id", row["raw_record"])
        self.assertGreaterEqual(int(row["record_index"]), 0)


class SingleDefects(unittest.TestCase):
    """Each rule on its own, built from one clean member / enrollment / claim."""

    def setUp(self):
        self.m = pd.DataFrame([dict(member_id="M1", age_band="26-35", region="West",
                                    enrollment_date="2025-03-01", termination_date="2025-08-31",
                                    termination_reason="Voluntary", first_missed_payment_date="",
                                    member_status="Terminated")])
        self.known = ing.build_known_members(ing.validate_members(self.m).clean)
        self.plans = {"PLN-A"}
        self.e = dict(member_id="M1", plan_id="PLN-A", month_start="2025-05-01",
                      monthly_premium="400.00", is_active="1", is_churn_flag="0")
        self.c = dict(claim_id="CLM1", member_id="M1", claim_date="2025-05-10",
                      claim_amount="120.50", claim_status="Paid")

    def enr(self, **kw):
        return ing.validate_enrollments(pd.DataFrame([{**self.e, **kw}]), self.known, self.plans)

    def clm(self, **kw):
        return ing.validate_claims(pd.DataFrame([{**self.c, **kw}]), self.known)

    def mem(self, **kw):
        return ing.validate_members(pd.DataFrame([{**self.m.iloc[0].to_dict(), **kw}]))

    def msg(self, res):
        return " | ".join(res.rejects["error_message"])

    def test_baseline_rows_are_valid(self):
        self.assertEqual(len(self.enr().rejects), 0)
        self.assertEqual(len(self.clm().rejects), 0)
        self.assertEqual(len(self.mem().rejects), 0)

    def test_member_rules(self):
        self.assertIn("invalid age_band", self.msg(self.mem(age_band="30s")))
        self.assertIn("not YYYY-MM-DD", self.msg(self.mem(enrollment_date="03/01/2025")))
        self.assertIn("Active member has a termination_date", self.msg(self.mem(member_status="Active")))
        self.assertIn("no termination_date", self.msg(self.mem(termination_date="")))
        self.assertIn("Lapsed status requires", self.msg(self.mem(member_status="Lapsed")))
        lapse = dict(termination_reason="NonPayment", member_status="Lapsed",
                     first_missed_payment_date="2025-08-20")           # only 11 days of grace
        self.assertIn("15-day grace period", self.msg(self.mem(**lapse)))
        ok = dict(lapse, first_missed_payment_date="2025-08-16")        # exactly 15 days
        self.assertEqual(len(self.mem(**ok).rejects), 0)

    def test_member_grace_period_is_configurable(self):
        row = pd.DataFrame([{**self.m.iloc[0].to_dict(), "termination_reason": "NonPayment",
                             "member_status": "Lapsed", "first_missed_payment_date": "2025-08-16"}])
        self.assertEqual(len(ing.validate_members(row, grace_days=15).rejects), 0)
        self.assertEqual(len(ing.validate_members(row, grace_days=30).rejects), 1)

    def test_enrollment_rules(self):
        self.assertIn("not the first day", self.msg(self.enr(month_start="2025-05-15")))
        self.assertIn("outside the DimDate range", self.msg(self.enr(month_start="2030-01-01")))
        self.assertIn("unknown member_id", self.msg(self.enr(member_id="NOPE")))
        self.assertIn("unknown plan_id", self.msg(self.enr(plan_id="PLN-Z")))
        self.assertIn("not numeric", self.msg(self.enr(monthly_premium="abc")))
        self.assertIn("is_active must be 0 or 1", self.msg(self.enr(is_active="2")))
        self.assertIn("inactive month", self.msg(self.enr(is_active="0", is_churn_flag="1")))
        self.assertIn("before the member's enrollment month", self.msg(self.enr(month_start="2025-02-01")))
        self.assertIn("after the member's termination month", self.msg(self.enr(month_start="2025-09-01")))
        self.assertIn("outside the member's termination month", self.msg(self.enr(is_churn_flag="1")))
        self.assertEqual(len(self.enr(month_start="2025-08-01", is_churn_flag="1").rejects), 0)

    def test_claim_rules(self):
        self.assertIn("before the member's enrollment_date", self.msg(self.clm(claim_date="2025-02-28")))
        self.assertIn("after the member's termination_date", self.msg(self.clm(claim_date="2025-09-01")))
        self.assertIn("invalid claim_status", self.msg(self.clm(claim_status="Maybe")))
        self.assertIn("claim_date missing", self.msg(self.clm(claim_date="not a date")))
        self.assertIn("claim_amount must be > 0", self.msg(self.clm(claim_amount="0")))
        self.assertIn("claim_id longer than 20", self.msg(self.clm(claim_id="X" * 21)))

    def test_duplicates_keep_first_only(self):
        df = pd.DataFrame([self.e, self.e, {**self.e, "month_start": "2025-06-01"}])
        res = ing.validate_enrollments(df, self.known, self.plans)
        self.assertEqual((len(res.clean), len(res.rejects)), (2, 1))
        self.assertEqual(int(res.rejects.iloc[0]["record_index"]), 1)

    def test_database_members_count_as_known(self):
        db = pd.DataFrame([("M-DB", "2024-01-01", None)],
                          columns=["member_id", "enrollment_date", "termination_date"])
        known = ing.build_known_members(ing.validate_members(self.m).clean, db)
        res = ing.validate_enrollments(pd.DataFrame([{**self.e, "member_id": "M-DB"}]), known, self.plans)
        self.assertEqual(len(res.rejects), 0)

    def test_missing_columns_raise(self):
        with self.assertRaises(ValueError):
            path = HERE / "_tmp_bad.csv"
            path.write_text("member_id\nM1\n")
            try:
                ing.read_source(path, ing.MEMBER_COLS)
            finally:
                path.unlink()


if __name__ == "__main__":
    unittest.main()
