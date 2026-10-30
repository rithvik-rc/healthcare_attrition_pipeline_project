"""
Logic tests for the Phase 4 and Phase 6 SQL (no SQL Server needed).

The SQL blocks (marked -- BEGIN:/-- END: in attrition_metrics.sql) and the reporting views are
pulled straight out of the .sql files, translated mechanically to SQLite (function-name swaps
only), run against generated data, and compared with an independent pandas calculation.
This verifies the window-function logic and metric definitions; it does NOT verify T-SQL
syntax or SQL Server behaviour, so still run deploy_attrition.sql on your instance.

Run:  python -m unittest test_attrition_sql_logic -v
"""
import os
import calendar
import importlib.util
import re
import sqlite3
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
# Source code lives in ../src (override with ATTRITION_SRC); tests live here.
SRC = Path(os.environ.get("ATTRITION_SRC", HERE.parent / "src")).resolve()
METRICS = (SRC / "attrition_metrics.sql").read_text()


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SRC / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gen = _load("gen", "generate_attrition_data.py")


# ----------------------------- T-SQL -> SQLite (mechanical) -----------------------------
def _d(s):
    return date.fromisoformat(s) if isinstance(s, str) else s


def _add_months(d, n):
    y, m = divmod(d.year * 12 + d.month - 1 + n, 12)
    return date(y, m + 1, min(d.day, calendar.monthrange(y, m + 1)[1]))


def register_udfs(con):
    f = con.create_function
    f("DATEDIFF_MONTH", 2, lambda a, b: None if a is None or b is None else
      (_d(b).year - _d(a).year) * 12 + _d(b).month - _d(a).month)
    f("DATEDIFF_DAY", 2, lambda a, b: None if a is None or b is None else (_d(b) - _d(a)).days)
    f("DATEADD_MONTH", 2, lambda n, d: None if d is None else _add_months(_d(d), n).isoformat())
    f("DATEADD_YEAR", 2, lambda n, d: None if d is None else _add_months(_d(d), 12 * n).isoformat())
    f("EOMONTH", 1, lambda d: None if d is None else
      date(_d(d).year, _d(d).month, calendar.monthrange(_d(d).year, _d(d).month)[1]).isoformat())
    f("DATEFROMPARTS", 3, lambda y, m, d: date(y, m, d).isoformat())
    f("YEAR", 1, lambda d: None if d is None else _d(d).year)
    f("MONTH", 1, lambda d: None if d is None else _d(d).month)
    f("POWER", 2, lambda a, b: None if a is None else float(a) ** b)


def to_sqlite(sql, threshold="0.1", renewal_month="1"):
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = sql.replace("dbo.", "").replace(";WITH", "WITH")
    for old, new in [("DATEDIFF(MONTH,", "DATEDIFF_MONTH("), ("DATEDIFF(DAY,", "DATEDIFF_DAY("),
                     ("DATEADD(MONTH,", "DATEADD_MONTH("), ("DATEADD(YEAR,", "DATEADD_YEAR("),
                     ("ISNULL(", "IFNULL("), ("SYSUTCDATETIME()", "CURRENT_TIMESTAMP"),
                     ("@threshold", threshold), ("@renewalMonth", renewal_month)]:
        sql = sql.replace(old, new)
    return sql


def block(text, name):
    m = re.search(rf"-- BEGIN:{name}\n(.*?)-- END:{name}", text, re.S)
    assert m, f"block {name} not found"
    return m.group(1)


def views_from(path):
    out = {}
    for batch in re.split(r"^\s*GO\s*$", path.read_text(), flags=re.M | re.I):
        code = re.sub(r"--[^\n]*", "", batch).strip()
        m = re.match(r"CREATE\s+OR\s+ALTER\s+VIEW\s+dbo\.(\w+)\s+AS\s+(.*)", code, re.S | re.I)
        if m:
            out[m.group(1)] = m.group(2).strip().rstrip(";")
    return out


STAND_IN_WINDOW = """
CREATE VIEW vw_ObservationWindow AS
WITH cfg AS (SELECT ConfigValue AS WindowMonths FROM AttritionConfig WHERE ConfigKey = 'ObservationWindowMonths'),
loaded AS (SELECT DISTINCT d.FullDate AS MonthStart FROM FactEnrollment f JOIN DimDate d ON d.DateKey = f.DateKey),
bounds AS (SELECT MAX(MonthStart) AS LastMonth, COUNT(*) AS MonthsAvailable FROM loaded)
SELECT cfg.WindowMonths,
       DATEADD_MONTH(-(cfg.WindowMonths - 1), b.LastMonth) AS WindowStart,
       b.LastMonth AS WindowEnd,
       b.MonthsAvailable,
       (SELECT COUNT(*) FROM loaded l WHERE l.MonthStart BETWEEN DATEADD_MONTH(-(cfg.WindowMonths - 1), b.LastMonth) AND b.LastMonth) AS MonthsInWindow,
       CASE WHEN (SELECT COUNT(*) FROM loaded l WHERE l.MonthStart BETWEEN DATEADD_MONTH(-(cfg.WindowMonths - 1), b.LastMonth) AND b.LastMonth) = cfg.WindowMonths
            THEN 1 ELSE 0 END AS IsWindowComplete
FROM cfg CROSS JOIN bounds b
"""


# ----------------------------- fixture -----------------------------
class SqlLogic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        members, plans, enr, claims = gen.simulate(800, 21)
        cls.members, cls.plans, cls.enr, cls.claims = members, plans, enr, claims
        con = cls.con = sqlite3.connect(":memory:")
        register_udfs(con)
        con.executescript("""
            CREATE TABLE DimDate (DateKey INT PRIMARY KEY, FullDate TEXT);
            CREATE TABLE DimMember (MemberSK INTEGER PRIMARY KEY, MemberID TEXT, AgeBand TEXT, Region TEXT,
                EnrollmentDate TEXT, TerminationDate TEXT, TerminationReason TEXT, MemberStatus TEXT);
            CREATE TABLE DimPlan (PlanSK INTEGER PRIMARY KEY, PlanID TEXT, PlanName TEXT, PlanTier TEXT,
                PlanType TEXT, BaseMonthlyPremium REAL);
            CREATE TABLE FactEnrollment (MemberSK INT, PlanSK INT, DateKey INT, MonthlyPremium REAL,
                IsActive INT, IsChurnFlag INT);
            CREATE TABLE FactClaims (ClaimID TEXT, MemberSK INT, DateKey INT, ClaimAmount REAL, ClaimStatus TEXT);
            CREATE TABLE AttritionConfig (ConfigKey TEXT PRIMARY KEY, ConfigValue INT, Description TEXT);
        """)
        d0 = date(2024, 1, 1)
        con.executemany("INSERT INTO DimDate VALUES (?,?)",
                        [(int((d0 + timedelta(i)).strftime("%Y%m%d")), (d0 + timedelta(i)).isoformat())
                         for i in range((date(2027, 12, 31) - d0).days + 1)])
        con.executemany("INSERT INTO AttritionConfig VALUES (?,?,?)",
                        [("GracePeriodDays", 15, ""), ("ObservationWindowMonths", 12, ""),
                         ("PremiumIncreaseThresholdPct", 10, ""), ("RenewalMonth", 1, "")])
        mem = members.reset_index(drop=True)
        mem["MemberSK"] = mem.index + 1
        pl = plans.reset_index(drop=True)
        pl["PlanSK"] = pl.index + 1
        con.executemany("INSERT INTO DimMember VALUES (?,?,?,?,?,?,?,?)",
                        [(r.MemberSK, r.member_id, r.age_band, r.region, str(r.enrollment_date),
                          None if r.termination_date is None else str(r.termination_date),
                          r.termination_reason, r.member_status) for r in mem.itertuples()])
        con.executemany("INSERT INTO DimPlan VALUES (?,?,?,?,?,?)",
                        [(r.PlanSK, r.plan_id, r.plan_name, r.plan_tier, r.plan_type, r.base_monthly_premium)
                         for r in pl.itertuples()])
        sk, psk = dict(zip(mem.member_id, mem.MemberSK)), dict(zip(pl.plan_id, pl.PlanSK))
        key = lambda d: int(str(d).replace("-", ""))
        con.executemany("INSERT INTO FactEnrollment VALUES (?,?,?,?,?,?)",
                        [(sk[r.member_id], psk[r.plan_id], key(r.month_start), r.monthly_premium,
                          r.is_active, r.is_churn_flag) for r in enr.itertuples()])
        con.executemany("INSERT INTO FactClaims VALUES (?,?,?,?,?)",
                        [(r.claim_id, sk[r.member_id], key(r.claim_date), r.claim_amount, r.claim_status)
                         for r in claims.itertuples()])
        cls.sk = sk

        # --- run the real SQL blocks ---
        metrics = (SRC / "attrition_metrics.sql").read_text()
        for name in ("ddl_features", "ddl_cohort"):
            ddl = re.sub(r"^IF OBJECT_ID[^\n]*\n", "", block(metrics, name))
            con.executescript(to_sqlite(ddl))
        con.executescript(to_sqlite(block(metrics, "features")))
        con.executescript(to_sqlite(block(metrics, "cohort")))

        # --- real views (ObservationWindow replaced by an equivalent stand-in: CROSS APPLY) ---
        con.executescript(STAND_IN_WINDOW)
        views = views_from(SRC / "attrition_churn_rules.sql")
        views.update(views_from(SRC / "attrition_reporting_views.sql"))
        for name in ("vw_MonthlyChurn", "vw_AnnualChurn", "vw_Rpt_MonthlyChurn", "vw_Rpt_SegmentChurn",
                     "vw_Rpt_CohortRetention", "vw_Rpt_CohortMilestones", "vw_Rpt_AtRiskMembers",
                     "vw_Rpt_TerminationReasons", "vw_Rpt_ExecutiveSummary", "vw_Rpt_RenewalIncreaseChurn"):
            con.executescript(to_sqlite(f"CREATE VIEW {name} AS {views[name]};"))

        cls.feat = pd.read_sql("SELECT * FROM MemberMonthFeatures", con)
        cls.expected = cls.build_expected()

    @classmethod
    def build_expected(cls):
        """Independent pandas version of the feature logic."""
        e = cls.enr.copy()
        e["MemberSK"] = e.member_id.map(cls.sk)
        e["month"] = e.month_start.astype(str)
        e = e.sort_values(["MemberSK", "month"]).reset_index(drop=True)
        c = cls.claims.copy()
        c["MemberSK"] = c.member_id.map(cls.sk)
        c["month"] = c.claim_date.astype(str).str[:7] + "-01"
        c["den"] = (c.claim_status == "Denied").astype(int)
        cm = c.groupby(["MemberSK", "month"]).agg(n=("claim_id", "count"), den=("den", "sum"),
                                                  last=("claim_date", lambda s: max(str(x) for x in s))).reset_index()
        e = e.merge(cm, on=["MemberSK", "month"], how="left")
        e["n"], e["den"] = e["n"].fillna(0), e["den"].fillna(0)
        g = e.groupby("MemberSK")
        e["Claims3M"] = g["n"].transform(lambda s: s.rolling(3, min_periods=1).sum())
        e["Denied3M"] = g["den"].transform(lambda s: s.rolling(3, min_periods=1).sum())
        e["Claims6M"] = g["n"].transform(lambda s: s.rolling(6, min_periods=1).sum())
        e["Prev"] = g["monthly_premium"].shift(1)
        e["Chg"] = ((e.monthly_premium - e.Prev) / e.Prev).round(4)
        e["Max3"] = e.groupby("MemberSK")["Chg"].transform(lambda s: s.rolling(3, min_periods=1).max())
        # last-renewal change: carried forward from the most recent January row (needs a prior month)
        is_ren = e.month.str[5:7].astype(int) == 1
        e["RenChg"] = e.Chg.where(is_ren)
        e["RenMonth"] = e.month.where(is_ren)
        e["RenChg"] = e.groupby("MemberSK")["RenChg"].ffill()
        e["RenMonth"] = e.groupby("MemberSK")["RenMonth"].ffill()
        # a renewal row with no prior month must reset the carried value to NULL, not keep an older one
        first_ren = is_ren & e.Prev.isna()
        e.loc[first_ren, "RenChg"] = float("nan")
        e["HighRen"] = (e.RenChg >= 0.10).astype(int)
        e["AtRisk"] = ((e.HighRen == 1) & (e.Denied3M > 0)).astype(int)
        e["Last"] = e.groupby("MemberSK")["last"].transform(lambda s: s.ffill())
        e["DateKey"] = e.month.str.replace("-", "").astype(int)
        mon = cls.members.set_index("member_id")["enrollment_date"].astype(str)
        e["Tenure"] = [(int(m[:4]) - int(mon[i][:4])) * 12 + int(m[5:7]) - int(mon[i][5:7])
                       for i, m in zip(e.member_id, e.month)]
        return e

    # ----------------------------- Phase 4 -----------------------------
    def test_one_feature_row_per_enrollment_row(self):
        self.assertEqual(len(self.feat), len(self.enr))
        self.assertFalse(self.feat.duplicated(["MemberSK", "DateKey"]).any())

    def test_window_functions_match_pandas(self):
        j = self.feat.merge(self.expected, on=["MemberSK", "DateKey"], suffixes=("", "_e"))
        self.assertEqual(len(j), len(self.feat))
        for sql_col, pd_col in [("Claims3M", "Claims3M_e"), ("Denied3M", "Denied3M_e"), ("Claims6M", "Claims6M_e"),
                                ("TenureMonths", "Tenure"), ("IsAtRisk", "AtRisk"),
                                ("IsHighRenewalIncrease", "HighRen")]:
            self.assertTrue((j[sql_col] == j[pd_col]).all(), sql_col)
        self.assertTrue(((j.PrevPremium - j.Prev).abs().fillna(0) < 1e-6).all())
        self.assertTrue(((j.PremiumChangePct - j.Chg).abs().fillna(0) < 1e-4).all())
        self.assertTrue(((j.LastRenewalChangePct - j.RenChg).abs().fillna(0) < 1e-4).all())
        self.assertEqual(j.LastRenewalChangePct.isna().sum(), j.RenChg.isna().sum())
        self.assertTrue((j.LastRenewalMonth.fillna("") == j.RenMonth.fillna("")).all())
        self.assertTrue(((j.MaxPremiumChange3M - j.Max3).abs().fillna(0) < 1e-4).all())

    def test_days_since_last_claim(self):
        j = self.feat.merge(self.expected, on=["MemberSK", "DateKey"])
        has = j["Last"].notna()
        month_end = pd.to_datetime(j.month).dt.to_period("M").dt.to_timestamp("M")
        want = (month_end - pd.to_datetime(j["Last"])).dt.days
        self.assertTrue((j.loc[has, "DaysSinceLastClaim"] == want[has]).all())
        self.assertTrue(j.loc[~has, "DaysSinceLastClaim"].isna().all())

    def test_no_look_ahead_first_row_has_no_prior_premium(self):
        first = self.feat.sort_values("DateKey").groupby("MemberSK").head(1)
        self.assertTrue(first.PrevPremium.isna().all())
        self.assertTrue(first.PremiumChangePct.isna().all())

    def test_flag_definitions(self):
        f = self.feat
        self.assertTrue(((f.Denied3M > 0).astype(int) == f.HasRecentDenial).all())
        self.assertTrue((f.IsAtRisk == (f.IsHighRenewalIncrease & f.HasRecentDenial)).all())
        self.assertTrue(((f.AnnualizedPremium - f.MonthlyPremium * 12).abs() < 1e-6).all())
        self.assertGreater(f.IsAtRisk.sum(), 0)
        self.assertGreater(f.IsDisengaged.sum(), 0)
        self.assertTrue(set(f.TenureBucket) <= {"0-5", "6-11", "12-23", "24+"})

    def test_cohort_retention(self):
        con = self.con
        cr = pd.read_sql("SELECT * FROM CohortRetention", con)
        self.assertGreater(len(cr), 0)
        self.assertTrue((cr[cr.MonthsSinceEnrollment == 0].RetentionRate == 1.0).all())
        for _, grp in cr.groupby("CohortMonth"):
            r = grp.sort_values("MonthsSinceEnrollment").RetentionRate.tolist()
            self.assertEqual(r, sorted(r, reverse=True))        # never increases with age
        # independent check of one cohort / month
        mem = self.members.assign(cm=self.members.enrollment_date.astype(str).str[:7] + "-01")
        cohort = mem[mem.cm >= "2025-01-01"]
        row = cr[(cr.CohortMonth == "2025-03-01") & (cr.MonthsSinceEnrollment == 6)].iloc[0]
        ids = set(cohort[cohort.cm == "2025-03-01"].member_id)
        e = self.enr[self.enr.member_id.isin(ids) & (self.enr.month_start.astype(str) == "2025-09-01")]
        self.assertEqual(row.CohortSize, len(ids))
        self.assertEqual(row.ActiveMembers, len(e))
        # members enrolled before the data starts are excluded (left-truncation)
        self.assertEqual(cr.drop_duplicates("CohortMonth").CohortSize.sum(), len(cohort))

    # ----------------------------- Phase 6 views -----------------------------
    def test_monthly_trend_view(self):
        v = pd.read_sql("SELECT * FROM vw_Rpt_MonthlyChurn ORDER BY MonthStart", self.con)
        self.assertEqual(len(v), self.enr.month_start.nunique())
        self.assertTrue(((v.ChurnedMembers * 1.0 / v.ActiveMembers - v.MonthlyChurnRate).abs() < 1e-9).all())
        roll = v.MonthlyChurnRate.rolling(3, min_periods=1).mean()
        self.assertTrue(((roll - v.Rolling3MChurnRate).abs() < 1e-9).all())
        jan26 = v[v.MonthStart == "2026-01-01"].iloc[0]
        jan25 = v[v.MonthStart == "2025-01-01"].iloc[0]
        self.assertAlmostEqual(jan26.PriorYearChurnRate, jan25.MonthlyChurnRate)
        self.assertTrue(v[v.MonthStart < "2026-01-01"].PriorYearChurnRate.isna().all())
        self.assertEqual(v.ChurnedMembers.sum(), self.enr.is_churn_flag.sum())

    def test_segment_view_partitions_the_window(self):
        v = pd.read_sql("SELECT * FROM vw_Rpt_SegmentChurn", self.con)
        last12 = sorted(self.enr.month_start.astype(str).unique())[-12:]
        w = self.enr[self.enr.month_start.astype(str).isin(last12)]
        for seg, grp in v.groupby("SegmentType"):          # every dimension splits the same total
            self.assertEqual(grp.MemberMonths.sum(), len(w), seg)
            self.assertEqual(grp.ChurnedMembers.sum(), w.is_churn_flag.sum(), seg)
        self.assertEqual(set(v.SegmentType), {"Plan tier", "Plan type", "Region", "Age band", "Tenure"})
        row = v.iloc[0]
        want = 1 - (1 - row.MonthlyChurnRate) ** 12
        self.assertAlmostEqual(row.AnnualizedChurnRate, want, places=9)

    def test_at_risk_list(self):
        v = pd.read_sql("SELECT * FROM vw_Rpt_AtRiskMembers ORDER BY PriorityRank", self.con)
        last = self.feat.MonthStart.max()
        want = self.feat[(self.feat.MonthStart == last) & (self.feat.IsActive == 1)
                         & (self.feat.IsChurnFlag == 0) & (self.feat.IsAtRisk == 1)]
        self.assertEqual(len(v), len(want))
        self.assertTrue((v.AsOfMonth == last).all())
        self.assertEqual(v.PriorityRank.tolist(), list(range(1, len(v) + 1)))
        self.assertTrue(v.AnnualizedPremiumAtRisk.is_monotonic_decreasing)

    def test_cohort_milestones_are_null_when_not_observable(self):
        v = pd.read_sql("SELECT * FROM vw_Rpt_CohortMilestones ORDER BY CohortMonth", self.con)
        latest = v[v.CohortMonth == "2026-06-01"].iloc[0]        # newest cohort: only 3 months observed
        self.assertFalse(pd.isna(latest.Retention3M))
        self.assertTrue(pd.isna(latest.Retention12M) and pd.isna(latest.Retention6M))
        early = v[v.CohortMonth == "2025-01-01"].iloc[0]
        self.assertFalse(pd.isna(early.Retention12M))
        self.assertLessEqual(early.Retention12M, early.Retention6M)

    def test_termination_reasons_view(self):
        v = pd.read_sql("SELECT * FROM vw_Rpt_TerminationReasons", self.con)
        self.assertEqual(v.Members.sum(), (self.members.member_status != "Active").sum())
        self.assertEqual(set(v.ChurnCategory) - {"Voluntary", "Involuntary", "Death", "Other"}, set())

    def test_executive_summary_matches_generator_method(self):
        s = pd.read_sql("SELECT * FROM vw_Rpt_ExecutiveSummary", self.con)
        self.assertEqual(len(s), 1)
        r = s.iloc[0]
        months = sorted(self.enr.month_start.astype(str).unique())
        start = months[-12]
        cohort = set(self.enr[self.enr.month_start.astype(str) == start].member_id)
        inwin = self.enr[self.enr.month_start.astype(str) >= start]
        churned = set(inwin[inwin.is_churn_flag == 1].member_id) & cohort
        self.assertEqual((r.CohortMembers, r.ChurnedMembers), (len(cohort), len(churned)))
        self.assertAlmostEqual(r.AnnualChurnRate, len(churned) / len(cohort), places=9)
        self.assertAlmostEqual(r.AnnualRetentionRate, 1 - len(churned) / len(cohort), places=9)
        self.assertEqual(r.IsWindowComplete, 1)
        atrisk = pd.read_sql("SELECT COUNT(*) n, SUM(AnnualizedPremiumAtRisk) p FROM vw_Rpt_AtRiskMembers", self.con).iloc[0]
        self.assertEqual(r.AtRiskMembers, atrisk.n)
        self.assertAlmostEqual(r.AnnualizedPremiumAtRisk, atrisk.p, places=4)

    def test_avg_monthly_churn_is_pooled_ratio_not_average_of_rates(self):
        r = pd.read_sql("SELECT AvgMonthlyChurnRate FROM vw_Rpt_ExecutiveSummary", self.con).iloc[0, 0]
        months = sorted(self.enr.month_start.astype(str).unique())[-12:]
        w = self.enr[self.enr.month_start.astype(str).isin(months)]
        self.assertAlmostEqual(r, w.is_churn_flag.sum() / len(w), places=9)

    def test_window_incomplete_gives_null_annual_rate(self):
        con = sqlite3.connect(":memory:")
        register_udfs(con)
        self.con.backup(con)
        con.execute("UPDATE AttritionConfig SET ConfigValue = 24 WHERE ConfigKey = 'ObservationWindowMonths'")
        r = pd.read_sql("SELECT * FROM vw_AnnualChurn", con).iloc[0]
        self.assertEqual(r.IsWindowComplete, 0)
        self.assertTrue(pd.isna(r.AnnualChurnRate) and pd.isna(r.AnnualRetentionRate))


# ------------------- renewal rule on hand-built members -------------------
class RenewalRule(unittest.TestCase):
    """Tiny fixtures where the right answer is obvious. Premiums: (month, premium); claims: (month, status)."""

    @classmethod
    def run_features(cls, members, renewal_month="1"):
        con = sqlite3.connect(":memory:")
        register_udfs(con)
        con.executescript("""
            CREATE TABLE DimDate (DateKey INT PRIMARY KEY, FullDate TEXT);
            CREATE TABLE DimMember (MemberSK INTEGER PRIMARY KEY, EnrollmentDate TEXT);
            CREATE TABLE FactEnrollment (MemberSK INT, PlanSK INT, DateKey INT, MonthlyPremium REAL,
                IsActive INT, IsChurnFlag INT);
            CREATE TABLE FactClaims (ClaimID TEXT, MemberSK INT, DateKey INT, ClaimAmount REAL, ClaimStatus TEXT);
        """)
        d0 = date(2024, 1, 1)
        con.executemany("INSERT INTO DimDate VALUES (?,?)",
                        [(int((d0 + timedelta(i)).strftime("%Y%m%d")), (d0 + timedelta(i)).isoformat())
                         for i in range((date(2027, 12, 31) - d0).days + 1)])
        n = 0
        for sk, (enroll, premiums, claims) in members.items():
            con.execute("INSERT INTO DimMember VALUES (?,?)", (sk, enroll))
            for m, prem in premiums:
                con.execute("INSERT INTO FactEnrollment VALUES (?,?,?,?,1,0)",
                            (sk, 1, int(m.replace("-", "") + "01"), prem))
            for m, status in claims:
                n += 1
                con.execute("INSERT INTO FactClaims VALUES (?,?,?,?,?)",
                            (f"C{n}", sk, int(m.replace("-", "") + "10"), 100.0, status))
        ddl = re.sub(r"^IF OBJECT_ID[^\n]*\n", "", block(METRICS, "ddl_features"))
        con.executescript(to_sqlite(ddl, renewal_month=renewal_month))
        con.executescript(to_sqlite(block(METRICS, "features"), renewal_month=renewal_month))
        return pd.read_sql("SELECT * FROM MemberMonthFeatures ORDER BY MemberSK, DateKey", con)

    @staticmethod
    def months(start, n):
        y, m = map(int, start.split("-"))
        return [f"{y + (m - 1 + i) // 12}-{(m - 1 + i) % 12 + 1:02d}" for i in range(n)]

    def series(self, start, premiums):
        return list(zip(self.months(start, len(premiums)), premiums))

    def test_increase_at_renewal_flags_until_next_renewal(self):
        prem = [100] * 12 + [115] * 12            # Dec 2024 -> Jan 2025: +15%; rest of 2025 flat
        f = self.run_features({1: ("2024-01-01", self.series("2024-01", prem),
                                   [("2025-05", "Denied")])})
        by = f.set_index("MonthStart")
        self.assertEqual(by.loc["2025-01-01", "LastRenewalChangePct"], 0.15)
        # the old 3-month signal has long expired, but the renewal flag still holds in May
        self.assertEqual(by.loc["2025-05-01", "IsHighPremiumIncrease"], 0)
        self.assertEqual(by.loc["2025-05-01", "IsHighRenewalIncrease"], 1)
        self.assertEqual(by.loc["2025-05-01", "IsAtRisk"], 1)
        self.assertEqual(by.loc["2025-09-01", "IsAtRisk"], 0)     # denial is >3 months old by now
        self.assertEqual(by.loc["2025-09-01", "IsHighRenewalIncrease"], 1)

    def test_no_denial_means_not_at_risk(self):
        prem = [100] * 12 + [120] * 6
        f = self.run_features({1: ("2024-01-01", self.series("2024-01", prem), [("2025-03", "Paid")])})
        self.assertEqual(f.IsHighRenewalIncrease.iloc[-1], 1)
        self.assertEqual(f.IsAtRisk.sum(), 0)

    def test_mid_year_increase_is_not_a_renewal_increase(self):
        prem = [100] * 5 + [130] * 7 + [130] * 6   # +30% in June 2024, flat at the January renewal
        f = self.run_features({1: ("2024-01-01", self.series("2024-01", prem), [("2024-07", "Denied")])})
        by = f.set_index("MonthStart")
        self.assertEqual(by.loc["2024-07-01", "IsHighPremiumIncrease"], 1)   # short-term signal sees it
        self.assertEqual(f.IsHighRenewalIncrease.sum(), 0)                    # renewal rule does not
        self.assertEqual(f.IsAtRisk.sum(), 0)
        self.assertEqual(by.loc["2025-01-01", "LastRenewalChangePct"], 0.0)

    def test_flat_renewal_clears_earlier_increase(self):
        prem = [100] * 12 + [115] * 12 + [115] * 3  # +15% Jan 2025, 0% Jan 2026
        f = self.run_features({1: ("2024-01-01", self.series("2024-01", prem), [("2026-02", "Denied")])})
        by = f.set_index("MonthStart")
        self.assertEqual(by.loc["2025-12-01", "IsHighRenewalIncrease"], 1)
        self.assertEqual(by.loc["2026-01-01", "IsHighRenewalIncrease"], 0)
        self.assertEqual(by.loc["2026-02-01", "IsAtRisk"], 0)

    def test_no_prior_month_gives_null_not_a_flag(self):
        # joins in Jan 2025 (no December row) and is first loaded in the renewal month
        f = self.run_features({1: ("2025-01-01", self.series("2025-01", [200] * 4), [("2025-02", "Denied")])})
        self.assertTrue(f.LastRenewalChangePct.isna().all())
        self.assertTrue(f.LastRenewalMonth.notna().all())      # a renewal month exists...
        self.assertEqual(f.IsAtRisk.sum(), 0)                  # ...but with nothing to compare against

    def test_before_first_renewal_is_null(self):
        f = self.run_features({1: ("2024-02-01", self.series("2024-02", [100] * 5), [("2024-03", "Denied")])})
        self.assertTrue(f.LastRenewalMonth.isna().all())
        self.assertEqual(f.IsAtRisk.sum(), 0)

    def test_renewal_month_is_configurable(self):
        prem = [100] * 6 + [115] * 6              # +15% in July 2024
        data = {1: ("2024-01-01", self.series("2024-01", prem), [("2024-09", "Denied")])}
        self.assertEqual(self.run_features(data, renewal_month="1").IsAtRisk.sum(), 0)
        f7 = self.run_features(data, renewal_month="7")
        self.assertGreater(f7.IsAtRisk.sum(), 0)
        self.assertEqual(f7.set_index("MonthStart").loc["2024-09-01", "IsAtRisk"], 1)

    def test_members_do_not_leak_into_each_other(self):
        a = ("2024-01-01", self.series("2024-01", [100] * 12 + [115] * 3), [("2025-02", "Denied")])
        b = ("2024-01-01", self.series("2024-01", [100] * 15), [("2025-02", "Denied")])
        f = self.run_features({1: a, 2: b})
        self.assertGreater(f[f.MemberSK == 1].IsAtRisk.sum(), 0)
        self.assertEqual(f[f.MemberSK == 2].IsAtRisk.sum(), 0)


if __name__ == "__main__":
    unittest.main()

