"""
Phase 2: synthetic healthcare-insurance data generator + staging loader.

Generates de-identified members, plans, monthly enrollments and claims with
realistic attrition drivers, writes CSVs, and (optionally) loads the clean
data into the SQL Server staging tables from attrition_staging.sql.

Attrition drivers baked into the simulation (so Phase 4 metrics find signal):
  - open-enrollment spike (Dec/Jan)
  - premium renewal increases (higher hazard for 3 months after a >=10% rise)
  - recent claim denials
  - disengagement (no claims for 6+ months)
  - first-year members, Bronze tier and younger members churn more

Usage:
  python generate_attrition_data.py                       # CSVs only
  python generate_attrition_data.py --load                # CSVs + load staging
  python generate_attrition_data.py --inject-bad 5        # CSVs with dirty rows
                                                          # (for Phase 3 testing)
Every run self-checks the generated data against the same rules the database enforces
(CHECK constraints, grace period, one churn per member, coverage-date logic) and stops
with a list of problems if anything is off. --load also verifies staging row counts
before committing.
Requires: pandas, numpy (+ pyodbc for --load).
"""
import argparse
import os
import sys
import uuid
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Connection settings (Windows Authentication). Override with environment variables:
#   ATTRITION_SQL_SERVER, ATTRITION_SQL_DATABASE, ATTRITION_SQL_DRIVER (name without braces)
SERVER = os.environ.get("ATTRITION_SQL_SERVER", "localhost")
DATABASE = os.environ.get("ATTRITION_SQL_DATABASE", "WarehouseDB")
DRIVER = "{" + os.environ.get("ATTRITION_SQL_DRIVER", "ODBC Driver 17 for SQL Server") + "}"

SERVER_CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE=master;Trusted_Connection=yes;"
TARGET_CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"

# Simulation window: 21 monthly snapshots
MONTHS = pd.date_range("2025-01-01", "2026-09-01", freq="MS")

# Churn rules (must match dbo.AttritionConfig and CK_DimMember_NonPaymentGrace)
GRACE_PERIOD_DAYS = 15            # days after a missed premium before coverage lapses
OBSERVATION_WINDOW_MONTHS = 12    # churn is measured over the trailing 12 monthly snapshots
assert len(MONTHS) >= OBSERVATION_WINDOW_MONTHS, "simulation must cover the observation window"

AGE_BANDS = ["18-25", "26-35", "36-45", "46-55", "56-64"]
AGE_P = [0.12, 0.22, 0.24, 0.24, 0.18]
AGE_PREMIUM_FACTOR = np.array([0.80, 0.90, 1.00, 1.20, 1.50])
AGE_CLAIM_RATE = np.array([0.30, 0.35, 0.45, 0.60, 0.80])   # claims per member-month
AGE_CHURN_MULT = np.array([1.5, 1.2, 1.0, 0.9, 0.7])

REGIONS = ["West", "Midwest", "South", "Northeast"]
REGION_P = [0.30, 0.20, 0.30, 0.20]

TIER_BASE = {"Bronze": 280.0, "Silver": 400.0, "Gold": 520.0, "Platinum": 680.0}
TIER_WEIGHT = {"Bronze": 0.20, "Silver": 0.40, "Gold": 0.28, "Platinum": 0.12}
TIER_CHURN_MULT = {"Bronze": 1.3, "Silver": 1.0, "Gold": 0.85, "Platinum": 0.7}
TYPE_MULT = {"HMO": 1.00, "PPO": 1.12}
TYPE_WEIGHT = {"HMO": 0.55, "PPO": 0.45}

RENEWAL_INCREASES = np.array([0.0, 0.03, 0.06, 0.10, 0.15, 0.20])
RENEWAL_P = [0.20, 0.25, 0.25, 0.15, 0.10, 0.05]

BASE_HAZARD = 0.0055   # baseline monthly churn probability before multipliers


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
def build_plans() -> pd.DataFrame:
    rows = []
    for tier, base in TIER_BASE.items():
        for ptype, mult in TYPE_MULT.items():
            rows.append({
                "plan_id": f"PLN-{tier[:3].upper()}-{ptype}",
                "plan_name": f"{tier} {ptype}",
                "plan_tier": tier,
                "plan_type": ptype,
                "base_monthly_premium": round(base * mult, 2),
                "weight": TIER_WEIGHT[tier] * TYPE_WEIGHT[ptype],
            })
    return pd.DataFrame(rows)


def simulate(n_members: int, seed: int):
    rng = np.random.default_rng(seed)
    n = n_members
    plans = build_plans()

    member_ids = np.array([f"M{100000 + i}" for i in range(n)])
    age_idx = rng.choice(len(AGE_BANDS), n, p=AGE_P)
    region = rng.choice(REGIONS, n, p=REGION_P)

    # 60% were already enrolled before the window, 40% join during it
    existing = rng.random(n) < 0.60
    pre = pd.date_range("2022-01-01", "2024-12-01", freq="MS").values
    new = pd.date_range("2025-01-01", "2026-06-01", freq="MS").values
    enroll = pd.DatetimeIndex(np.where(existing, rng.choice(pre, n), rng.choice(new, n)))
    enroll_year, enroll_mon = enroll.year.values, enroll.month.values

    plan_idx = rng.choice(len(plans), n, p=plans["weight"] / plans["weight"].sum())
    plan_ids = plans["plan_id"].values[plan_idx]
    tier_mult = plans["plan_tier"].map(TIER_CHURN_MULT).values[plan_idx]
    premium = np.round(plans["base_monthly_premium"].values[plan_idx] * AGE_PREMIUM_FACTOR[age_idx], 2)

    claim_rate = AGE_CLAIM_RATE[age_idx] * rng.gamma(4.0, 0.25, n)   # mean multiplier = 1
    denial_prop = rng.beta(2, 14, n)                                  # mean ~12.5%
    age_churn = AGE_CHURN_MULT[age_idx]

    alive = np.ones(n, dtype=bool)
    churn_month_idx = np.full(n, -1)
    last_hi_incr = np.full(n, -99)
    last_denial = np.full(n, -99)
    last_claim = np.full(n, -3)

    enroll_np = enroll.values
    enroll_rows, claim_rows = [], []

    for m, month in enumerate(MONTHS):
        eligible = alive & (enroll_np <= month.to_datetime64())

        # January renewal: premium increases for members enrolled before this month
        if month.month == 1:
            renewing = eligible & (enroll_np < month.to_datetime64())
            incr = rng.choice(RENEWAL_INCREASES, n, p=RENEWAL_P)
            premium[renewing] = np.round(premium[renewing] * (1 + incr[renewing]), 2)
            last_hi_incr[renewing & (incr >= 0.10)] = m

        e = np.flatnonzero(eligible)
        if e.size == 0:
            continue

        # Claims for the month
        k = rng.poisson(claim_rate[e])
        if k.sum() > 0:
            mi = np.repeat(e, k)
            days = rng.integers(1, month.days_in_month + 1, mi.size)
            denied = rng.random(mi.size) < denial_prop[mi]
            pending = (~denied) & (m == len(MONTHS) - 1) & (rng.random(mi.size) < 0.12)
            claim_rows.append(pd.DataFrame({
                "member_idx": mi,
                "claim_date": pd.DatetimeIndex(month + pd.to_timedelta(days - 1, unit="D")).date,
                "claim_amount": np.round(rng.lognormal(5.5, 0.9, mi.size), 2),
                "claim_status": np.where(denied, "Denied", np.where(pending, "Pending", "Paid")),
            }))
            last_claim[mi] = m
            last_denial[mi[denied]] = m

        # Monthly churn hazard
        tenure = (month.year - enroll_year[e]) * 12 + (month.month - enroll_mon[e])
        h = np.full(e.size, BASE_HAZARD)
        if month.month in (12, 1):
            h *= 3.0
        h *= np.where(m - last_hi_incr[e] <= 3, 1.8, 1.0)
        h *= np.where(m - last_denial[e] <= 3, 1.6, 1.0)
        h *= np.where(m - last_claim[e] > 6, 1.3, 1.0)
        h *= np.where(tenure < 12, 1.5, 1.0)
        h *= tier_mult[e] * age_churn[e]
        churn = rng.random(e.size) < np.clip(h, 0, 0.25)

        enroll_rows.append(pd.DataFrame({
            "member_idx": e,
            "month_start": month.date(),
            "monthly_premium": premium[e],
            "is_active": 1,
            "is_churn_flag": churn.astype(int),
        }))
        alive[e[churn]] = False
        churn_month_idx[e[churn]] = m

    if not enroll_rows or not claim_rows:
        raise ValueError("Simulation produced no enrollments or claims; use more members (>= 100).")

    # --- Members ---
    termination_date = np.full(n, None, dtype=object)
    reason = np.full(n, None, dtype=object)
    first_missed = np.full(n, None, dtype=object)
    status = np.full(n, "Active", dtype=object)
    for i in np.flatnonzero(churn_month_idx >= 0):
        month = MONTHS[churn_month_idx[i]]
        termination_date[i] = (month + pd.offsets.MonthEnd(0)).date()
        r = rng.random()
        if month.month in (12, 1) and r < 0.60:
            reason[i] = "NonRenewal"
        else:
            r2 = rng.random()
            reason[i] = ("Voluntary" if r2 < 0.50 else "NonPayment" if r2 < 0.85
                         else "Deceased" if r2 < 0.87 else "Other")
        status[i] = "Lapsed" if reason[i] == "NonPayment" else "Terminated"
        if reason[i] == "NonPayment":
            # coverage runs through the grace period, then lapses on termination_date
            first_missed[i] = termination_date[i] - timedelta(days=GRACE_PERIOD_DAYS)

    members = pd.DataFrame({
        "member_id": member_ids,
        "age_band": np.array(AGE_BANDS)[age_idx],
        "region": region,
        "enrollment_date": enroll.date,
        "termination_date": termination_date,
        "termination_reason": reason,
        "first_missed_payment_date": first_missed,
        "member_status": status,
    })

    # --- Enrollments ---
    enrollments = pd.concat(enroll_rows, ignore_index=True)
    midx = enrollments.pop("member_idx").values
    enrollments.insert(0, "member_id", member_ids[midx])
    enrollments.insert(1, "plan_id", plan_ids[midx])

    # --- Claims ---
    claims = pd.concat(claim_rows, ignore_index=True)
    claims["member_id"] = member_ids[claims.pop("member_idx").values]
    claims = claims.sort_values(["claim_date", "member_id"]).reset_index(drop=True)
    claims.insert(0, "claim_id", [f"CLM{i:08d}" for i in range(1, len(claims) + 1)])
    claims = claims[["claim_id", "member_id", "claim_date", "claim_amount", "claim_status"]]

    return members, plans.drop(columns="weight"), enrollments, claims


def inject_bad_rows(members, enrollments, claims, k: int):
    """Append k dirty rows of each kind. For Phase 3 validation testing (CSV only)."""
    m_bad = members.head(k).copy()
    m_bad["member_id"] = [f"BAD-M{i}" for i in range(k)]
    m_bad["termination_date"] = [d - pd.Timedelta(days=30) for d in
                                 pd.to_datetime(m_bad["enrollment_date"]).dt.date]
    m_bad["member_status"] = "Terminated"
    m_bad["termination_reason"] = "Voluntary"

    base = members.iloc[k:2 * k].copy()
    enr = pd.to_datetime(base["enrollment_date"])
    term = enr + pd.Timedelta(days=40)
    g_bad = base.copy()                                    # lapse inside the grace period
    g_bad["member_id"] = [f"BAD-G{i}" for i in range(len(g_bad))]
    g_bad["termination_date"] = term.dt.date
    g_bad["termination_reason"] = "NonPayment"
    g_bad["member_status"] = "Lapsed"
    g_bad["first_missed_payment_date"] = (term - pd.Timedelta(days=GRACE_PERIOD_DAYS - 10)).dt.date
    n_bad = g_bad.copy()                                   # non-payment with no missed-payment date
    n_bad["member_id"] = [f"BAD-N{i}" for i in range(len(n_bad))]
    n_bad["first_missed_payment_date"] = None

    dup = enrollments.head(k).copy()                       # duplicate member-month
    neg = enrollments.iloc[k:2 * k].copy()                 # negative premium
    neg["monthly_premium"] = -abs(neg["monthly_premium"])
    unk = enrollments.iloc[2 * k:3 * k].copy()             # unknown plan
    unk["plan_id"] = "PLN-UNKNOWN"
    unk["month_start"] = pd.Timestamp("2026-09-01").date()

    c_neg = claims.head(k).copy()                          # negative claim amount
    c_neg["claim_id"] = [f"CLMBAD{i:03d}" for i in range(k)]
    c_neg["claim_amount"] = -abs(c_neg["claim_amount"])
    c_miss = claims.iloc[k:2 * k].copy()                   # missing member_id
    c_miss["claim_id"] = [f"CLMMISS{i:02d}" for i in range(k)]
    c_miss["member_id"] = ""

    return (pd.concat([members, m_bad, g_bad, n_bad], ignore_index=True),
            pd.concat([enrollments, dup, neg, unk], ignore_index=True),
            pd.concat([claims, c_neg, c_miss], ignore_index=True))


def check_invariants(members, plans, enrollments, claims):
    """Raise ValueError if the data would violate a database constraint or DQ rule.

    Mirrors the CHECK constraints in attrition_schema.sql and the checks in
    attrition_dq_checks.sql, so bad data is caught before it reaches SQL Server.
    """
    problems = []

    def check(ok, msg):
        if not bool(ok):
            problems.append(msg)

    # keys and referential integrity
    check(members["member_id"].is_unique, "duplicate member_id")
    check(not enrollments.duplicated(["member_id", "month_start"]).any(), "duplicate member-month")
    check(claims["claim_id"].is_unique, "duplicate claim_id")
    check(enrollments["member_id"].isin(members["member_id"]).all(), "enrollment for unknown member")
    check(enrollments["plan_id"].isin(plans["plan_id"]).all(), "enrollment for unknown plan")
    check(claims["member_id"].isin(members["member_id"]).all(), "claim for unknown member")

    # value rules
    check((enrollments["monthly_premium"] >= 0).all(), "negative premium")
    check((claims["claim_amount"] > 0).all(), "non-positive claim amount")
    check(set(claims["claim_status"]) <= {"Paid", "Denied", "Pending"}, "invalid claim status")
    check(set(members["age_band"]) <= set(AGE_BANDS), "invalid age band")

    # member rules (DimMember CHECK constraints)
    enr = pd.to_datetime(members["enrollment_date"])
    term = pd.to_datetime(members["termination_date"])
    miss = pd.to_datetime(members["first_missed_payment_date"])
    active = members["member_status"] == "Active"
    nonpay = members["termination_reason"] == "NonPayment"
    check((active == term.isna()).all(), "status / termination_date mismatch")
    check((term.isna() | (term >= enr)).all(), "termination before enrollment")
    check((nonpay == miss.notna()).all(), "missed-payment date must exist exactly when reason is NonPayment")
    check(((term - miss).dt.days[nonpay] >= GRACE_PERIOD_DAYS).all(),
          f"non-payment lapse inside the {GRACE_PERIOD_DAYS}-day grace period")
    check(((members["member_status"] == "Lapsed") == nonpay).all(), "Lapsed status must match NonPayment")

    # enrollment facts vs member dates
    e = enrollments.merge(members[["member_id", "enrollment_date", "termination_date"]], on="member_id")
    ms = pd.to_datetime(e["month_start"])
    e_month = pd.to_datetime(e["enrollment_date"]).dt.to_period("M").dt.to_timestamp()
    t_month = pd.to_datetime(e["termination_date"]).dt.to_period("M").dt.to_timestamp()
    check((ms.dt.day == 1).all(), "month_start is not the first day of a month")
    check((ms >= e_month).all(), "enrollment row before the member's enrollment month")
    check((t_month.isna() | (ms <= t_month)).all(), "enrollment row after the member's termination month")
    flagged = e[e["is_churn_flag"] == 1]
    check(not flagged["member_id"].duplicated().any(), "member flagged as churned more than once")
    f_ms = pd.to_datetime(flagged["month_start"])
    f_tm = pd.to_datetime(flagged["termination_date"]).dt.to_period("M").dt.to_timestamp()
    check((f_tm.notna() & (f_ms == f_tm)).all(), "churn flag outside the termination month")
    check(set(flagged["member_id"]) == set(members.loc[~active, "member_id"]),
          "churn-flagged members differ from terminated members")

    # claims vs member dates
    c = claims.merge(members[["member_id", "enrollment_date", "termination_date"]], on="member_id")
    cd = pd.to_datetime(c["claim_date"])
    ctd = pd.to_datetime(c["termination_date"])
    check((cd >= pd.to_datetime(c["enrollment_date"])).all(), "claim before enrollment")
    check((ctd.isna() | (cd <= ctd)).all(), "claim after termination")

    if problems:
        raise ValueError("Generated data failed integrity checks:\n  - " + "\n  - ".join(problems))


def summarize(members, enrollments, claims):
    monthly = enrollments.groupby("month_start").agg(
        active=("member_id", "count"), churned=("is_churn_flag", "sum"))
    monthly["churn_rate"] = monthly["churned"] / monthly["active"]

    window = monthly.tail(OBSERVATION_WINDOW_MONTHS)
    complete = len(monthly) >= OBSERVATION_WINDOW_MONTHS
    w_start, w_end = window.index[0], window.index[-1]

    # Annual churn = members active at window start who churned inside the window
    cohort = set(enrollments.loc[enrollments["month_start"] == w_start, "member_id"])
    in_window = enrollments[enrollments["month_start"] >= w_start]
    churned = set(in_window.loc[in_window["is_churn_flag"] == 1, "member_id"]) & cohort

    print(f"\nMembers: {len(members):,} | Enrollment rows: {len(enrollments):,} | Claims: {len(claims):,}")
    print(f"Terminated members: {(members['member_status'] != 'Active').sum():,}")
    print(f"Denial rate: {(claims['claim_status'] == 'Denied').mean():.1%}")
    print(f"\nObservation window: {w_start} to {w_end} ({OBSERVATION_WINDOW_MONTHS} months, "
          f"{'complete' if complete else 'INCOMPLETE'}) | grace period: {GRACE_PERIOD_DAYS} days")
    print(f"Cohort at window start: {len(cohort):,} | churned in window: {len(churned):,}")
    if complete and cohort:
        print(f"Annual churn rate: {len(churned) / len(cohort):.1%} | "
              f"avg monthly churn: {window['churn_rate'].mean():.2%}")
    else:
        print("Annual churn rate: n/a (observation window incomplete)")
    print("\nMonthly churn:")
    print(monthly.tail(6).to_string(float_format=lambda x: f"{x:.4f}"))


# ---------------------------------------------------------------------------
# SQL Server loading
# ---------------------------------------------------------------------------
def _records(df: pd.DataFrame):
    out = []
    for row in df.itertuples(index=False, name=None):
        out.append(tuple(
            None if (v is None or (not isinstance(v, (str, bytes)) and pd.isna(v)))
            else (v.item() if isinstance(v, np.generic) else v)
            for v in row))
    return out


def load_to_staging(members, plans, enrollments, claims, batch_id: str):
    import pyodbc  # imported lazily so CSV-only runs don't need it

    # Make sure the database exists (autocommit: CREATE DATABASE can't run in a transaction)
    conn = pyodbc.connect(SERVER_CONN_STR, autocommit=True)
    cur = conn.cursor()
    cur.execute(f"IF DB_ID('{DATABASE}') IS NULL CREATE DATABASE [{DATABASE}];")
    cur.close()
    conn.close()

    conn = pyodbc.connect(TARGET_CONN_STR)
    cur = conn.cursor()
    try:
        for tbl in ("stg_members", "stg_enrollments", "stg_claims", "DimPlan"):
            cur.execute("SELECT OBJECT_ID(?, 'U');", f"dbo.{tbl}")
            if cur.fetchone()[0] is None:
                raise RuntimeError(f"dbo.{tbl} does not exist. Run attrition_schema.sql and "
                                   "attrition_staging.sql first.")

        # Reference data: upsert plans into DimPlan
        for p in plans.itertuples(index=False):
            cur.execute("""
                MERGE dbo.DimPlan WITH (HOLDLOCK) AS tgt
                USING (SELECT CAST(? AS VARCHAR(50)) AS PlanID) AS src ON tgt.PlanID = src.PlanID
                WHEN MATCHED THEN UPDATE SET PlanName = ?, PlanTier = ?, PlanType = ?, BaseMonthlyPremium = ?
                WHEN NOT MATCHED THEN INSERT (PlanID, PlanName, PlanTier, PlanType, BaseMonthlyPremium)
                     VALUES (src.PlanID, ?, ?, ?, ?);
            """, p.plan_id, p.plan_name, p.plan_tier, p.plan_type, float(p.base_monthly_premium),
                 p.plan_name, p.plan_tier, p.plan_type, float(p.base_monthly_premium))

        cur.fast_executemany = True
        for tbl in ("stg_members", "stg_enrollments", "stg_claims"):
            cur.execute(f"TRUNCATE TABLE dbo.{tbl};")

        m = members.assign(batch_id=batch_id)
        cur.executemany("""INSERT INTO dbo.stg_members
            (member_id, age_band, region, enrollment_date, termination_date,
             termination_reason, first_missed_payment_date, member_status, batch_id)
            VALUES (?,?,?,?,?,?,?,?,?)""", _records(m))

        e = enrollments.assign(batch_id=batch_id)
        cur.executemany("""INSERT INTO dbo.stg_enrollments
            (member_id, plan_id, month_start, monthly_premium, is_active, is_churn_flag, batch_id)
            VALUES (?,?,?,?,?,?,?)""", _records(e))

        c = claims.assign(batch_id=batch_id)
        cur.executemany("""INSERT INTO dbo.stg_claims
            (claim_id, member_id, claim_date, claim_amount, claim_status, batch_id)
            VALUES (?,?,?,?,?,?)""", _records(c))

        # Reconcile before committing: what landed must equal what we sent
        for tbl, expected in (("stg_members", len(m)), ("stg_enrollments", len(e)), ("stg_claims", len(c))):
            cur.execute(f"SELECT COUNT(*) FROM dbo.{tbl} WHERE batch_id = ?;", batch_id)
            actual = cur.fetchone()[0]
            if actual != expected:
                raise RuntimeError(f"Row count mismatch in dbo.{tbl}: sent {expected}, staged {actual}.")

        conn.commit()
        print(f"Loaded batch {batch_id}: {len(m):,} members, {len(e):,} enrollments, {len(c):,} claims.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--members", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out-dir", default="output")
    ap.add_argument("--load", action="store_true", help="load clean data into SQL Server staging")
    ap.add_argument("--inject-bad", type=int, default=0, metavar="K",
                    help="append K dirty rows per defect type to the CSVs (cannot combine with --load)")
    args = ap.parse_args(argv)

    if args.members < 100:
        sys.exit("--members must be at least 100 so every table gets rows.")
    if not 0 <= args.inject_bad <= args.members // 10:
        sys.exit("--inject-bad must be between 0 and members/10.")
    if args.load and args.inject_bad:
        sys.exit("--inject-bad writes dirty CSVs for Phase 3 testing and cannot be combined with --load.")

    members, plans, enrollments, claims = simulate(args.members, args.seed)
    check_invariants(members, plans, enrollments, claims)
    print("Integrity checks passed.")
    summarize(members, enrollments, claims)

    if args.inject_bad:
        members, enrollments, claims = inject_bad_rows(members, enrollments, claims, args.inject_bad)
        print(f"\nInjected {args.inject_bad} dirty rows per defect type into the CSV outputs.")

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    members.to_csv(out / "members.csv", index=False)
    plans.to_csv(out / "plans.csv", index=False)
    enrollments.to_csv(out / "enrollments.csv", index=False)
    claims.to_csv(out / "claims.csv", index=False)
    print(f"\nCSVs written to {out.resolve()}")

    if args.load:
        load_to_staging(members, plans, enrollments, claims, batch_id=str(uuid.uuid4()))


if __name__ == "__main__":
    main()
