"""
Phase 3: validated ingestion for the healthcare attrition warehouse.

Pipeline
  1. Extract    read members / enrollments / claims (and optional plans) CSVs as raw text
  2. Validate   one validate_<source>() per source; bad rows are quarantined, not dropped
  3. Quarantine rejected rows -> dbo.QuarantineLog (+ quarantine_report.csv)
  4. Stage      clean rows -> dbo.stg_* (truncate + insert, row counts reconciled)
  5. Transform  EXEC dbo.usp_LoadAttritionWarehouse (idempotent MERGE into dims and facts)
  6. Audit      dbo.vw_DQ_Checks must be empty; prints window / annual churn

Rules come from the database where possible (grace period from dbo.AttritionConfig,
plans from dbo.DimPlan, date range from dbo.DimDate, existing members from dbo.DimMember),
so the Python validators and the SQL constraints cannot drift apart silently.

Usage
  python attrition_ingest.py --dry-run                 # offline: validate only, no database
  python attrition_ingest.py                           # full run against SQL Server
  python attrition_ingest.py --input-dir output        # CSV folder (default: output)

Two ways in, one pipeline: this file is the CSV path. attrition_api_client.py is the API path
(it pulls the same records from the Flask feed in attrition_api.py). Both hand their raw text
tables to ingest_raw(), so validation, quarantine, staging and the MERGE are identical.

Run order: deploy_attrition.sql (includes attrition_load_warehouse.sql), then this script.
Requires: pandas, numpy (+ pyodbc unless --dry-run).
"""
import argparse
import json
import os
import sys
import uuid
from collections import Counter
from pathlib import Path
from typing import NamedTuple, Optional

import numpy as np
import pandas as pd

# Connection settings (Windows Authentication). Override with environment variables:
#   ATTRITION_SQL_SERVER, ATTRITION_SQL_DATABASE, ATTRITION_SQL_DRIVER (name without braces)
SERVER = os.environ.get("ATTRITION_SQL_SERVER", "localhost")
DATABASE = os.environ.get("ATTRITION_SQL_DATABASE", "WarehouseDB")
DRIVER = "{" + os.environ.get("ATTRITION_SQL_DRIVER", "ODBC Driver 17 for SQL Server") + "}"
TARGET_CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"

# Allowed values (mirror the CHECK constraints in attrition_schema.sql / attrition_staging.sql)
AGE_BANDS = {"0-17", "18-25", "26-35", "36-45", "46-55", "56-64", "65+"}
MEMBER_STATUSES = {"Active", "Terminated", "Lapsed"}
TERMINATION_REASONS = {"NonRenewal", "Voluntary", "NonPayment", "Deceased", "Other"}
CLAIM_STATUSES = {"Paid", "Denied", "Pending"}
PLAN_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
PLAN_TYPES = {"HMO", "PPO", "EPO", "POS", "HDHP"}

# Offline fallbacks (used only with --dry-run); the database values win in a real run
DEFAULT_GRACE_DAYS = 15
DEFAULT_DATE_MIN = pd.Timestamp("2024-01-01")
DEFAULT_DATE_MAX = pd.Timestamp("2027-12-31")

MEMBER_COLS = ["member_id", "age_band", "region", "enrollment_date", "termination_date",
               "termination_reason", "first_missed_payment_date", "member_status"]
ENROLLMENT_COLS = ["member_id", "plan_id", "month_start", "monthly_premium", "is_active", "is_churn_flag"]
CLAIM_COLS = ["claim_id", "member_id", "claim_date", "claim_amount", "claim_status"]
PLAN_COLS = ["plan_id", "plan_name", "plan_tier", "plan_type", "base_monthly_premium"]
MAX_MONEY = 1e8   # DECIMAL(10,2) upper bound


class ValidationResult(NamedTuple):
    clean: pd.DataFrame     # typed rows ready for staging
    rejects: pd.DataFrame   # source_name, record_index, raw_record (JSON), error_message


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def read_source(path: Path, required) -> pd.DataFrame:
    """Read a CSV as raw text (no NA conversion) so blanks and bad values survive intact."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name} is missing required columns: {missing}")
    return df


def _new_errors(df):
    return pd.Series("", index=df.index, dtype=object)


def _flag(errs, mask, msg):
    mask = pd.Series(mask, index=errs.index).fillna(False).astype(bool)
    errs.loc[mask] = errs.loc[mask] + msg + "; "


def _text(s):
    return s.astype(str).str.strip()


def _to_date(s):
    s = _text(s)
    return pd.to_datetime(s.where(s != "", None), format="%Y-%m-%d", errors="coerce")


def _to_num(s):
    s = _text(s)
    return pd.to_numeric(s.where(s != "", None), errors="coerce")


def _blank_to_none(s):
    s = _text(s)
    return s.where(s != "", None)


def _month_of(dates):
    return dates.dt.to_period("M").dt.to_timestamp()


def _finish(source, raw, errs) -> (pd.Series, pd.DataFrame):
    bad = errs != ""
    rejects = pd.DataFrame({
        "source_name": source,
        "record_index": raw.index[bad].to_numpy(),
        "raw_record": [json.dumps(r, default=str) for r in raw.loc[bad].to_dict("records")],
        "error_message": errs[bad].str.rstrip("; ").str.slice(0, 500).to_numpy(),
    })
    return bad, rejects


def build_known_members(clean_members: pd.DataFrame, db_members: Optional[pd.DataFrame] = None):
    """Member lookup (indexed by member_id) used to validate enrollments and claims.

    Batch members win over database members with the same id."""
    cols = ["member_id", "enrollment_date", "termination_date"]
    known = clean_members[cols].copy()
    if db_members is not None and len(db_members):
        known = pd.concat([known, db_members[~db_members["member_id"].isin(known["member_id"])][cols]],
                          ignore_index=True)
    for c in ("enrollment_date", "termination_date"):
        known[c] = pd.to_datetime(known[c])
    return known.drop_duplicates("member_id").set_index("member_id")


# ---------------------------------------------------------------------------
# validate_<source>(): one function per source
# ---------------------------------------------------------------------------
def validate_plans(raw: pd.DataFrame) -> ValidationResult:
    errs = _new_errors(raw)
    pid = _text(raw["plan_id"])
    prem = _to_num(raw["base_monthly_premium"])

    _flag(errs, pid == "", "plan_id missing")
    _flag(errs, (pid != "") & pid.duplicated(keep="first"), "duplicate plan_id")
    _flag(errs, pid.str.len() > 50, "plan_id longer than 50 characters")
    _flag(errs, _text(raw["plan_name"]) == "", "plan_name missing")
    _flag(errs, ~_text(raw["plan_tier"]).isin(PLAN_TIERS), "invalid plan_tier")
    _flag(errs, ~_text(raw["plan_type"]).isin(PLAN_TYPES), "invalid plan_type")
    _flag(errs, ~np.isfinite(prem), "base_monthly_premium missing or not numeric")
    _flag(errs, np.isfinite(prem) & ((prem <= 0) | (prem >= MAX_MONEY)), "base_monthly_premium must be > 0")

    bad, rejects = _finish("plans", raw, errs)
    ok = raw.loc[~bad]
    clean = pd.DataFrame({
        "plan_id": _text(ok["plan_id"]), "plan_name": _text(ok["plan_name"]),
        "plan_tier": _text(ok["plan_tier"]), "plan_type": _text(ok["plan_type"]),
        "base_monthly_premium": prem[~bad].round(2),
    })
    return ValidationResult(clean, rejects)


def validate_members(raw: pd.DataFrame, grace_days: int = DEFAULT_GRACE_DAYS) -> ValidationResult:
    errs = _new_errors(raw)
    mid = _text(raw["member_id"])
    status = _text(raw["member_status"])
    reason = _text(raw["termination_reason"])
    enr = _to_date(raw["enrollment_date"])
    term_txt, miss_txt = _text(raw["termination_date"]), _text(raw["first_missed_payment_date"])
    term, miss = _to_date(raw["termination_date"]), _to_date(raw["first_missed_payment_date"])

    _flag(errs, mid == "", "member_id missing")
    _flag(errs, (mid != "") & mid.duplicated(keep="first"), "duplicate member_id")
    _flag(errs, mid.str.len() > 50, "member_id longer than 50 characters")
    _flag(errs, _text(raw["region"]).str.len() > 50, "region longer than 50 characters")
    _flag(errs, ~_text(raw["age_band"]).isin(AGE_BANDS), "invalid age_band")
    _flag(errs, enr.isna(), "enrollment_date missing or not YYYY-MM-DD")
    _flag(errs, (term_txt != "") & term.isna(), "termination_date not YYYY-MM-DD")
    _flag(errs, (miss_txt != "") & miss.isna(), "first_missed_payment_date not YYYY-MM-DD")
    _flag(errs, ~status.isin(MEMBER_STATUSES), "invalid member_status")
    _flag(errs, (reason != "") & ~reason.isin(TERMINATION_REASONS), "invalid termination_reason")

    active = status == "Active"
    _flag(errs, active & term.notna(), "Active member has a termination_date")
    _flag(errs, ~active & status.isin(MEMBER_STATUSES) & term.isna(), "terminated member has no termination_date")
    _flag(errs, term.notna() & enr.notna() & (term < enr), "termination_date before enrollment_date")

    nonpay = reason == "NonPayment"
    lapsed = status == "Lapsed"
    _flag(errs, nonpay & (miss_txt == ""), "NonPayment lapse has no first_missed_payment_date")
    _flag(errs, ~nonpay & (miss_txt != ""), "first_missed_payment_date set but reason is not NonPayment")
    _flag(errs, nonpay & miss.notna() & term.notna() & ((term - miss).dt.days < grace_days),
          f"lapse inside the {grace_days}-day grace period")
    _flag(errs, lapsed & ~nonpay, "Lapsed status requires reason NonPayment")
    _flag(errs, ~lapsed & nonpay, "NonPayment reason requires Lapsed status")

    bad, rejects = _finish("members", raw, errs)
    keep = ~bad
    clean = pd.DataFrame({
        "member_id": mid[keep],
        "age_band": _text(raw["age_band"])[keep],
        "region": _blank_to_none(raw["region"])[keep],
        "enrollment_date": enr[keep].dt.date,
        "termination_date": term[keep].dt.date,
        "termination_reason": _blank_to_none(raw["termination_reason"])[keep],
        "first_missed_payment_date": miss[keep].dt.date,
        "member_status": status[keep],
    })
    return ValidationResult(clean, rejects)


def validate_enrollments(raw: pd.DataFrame, known_members: pd.DataFrame, plan_ids,
                         date_min=DEFAULT_DATE_MIN, date_max=DEFAULT_DATE_MAX) -> ValidationResult:
    errs = _new_errors(raw)
    mid, pid = _text(raw["member_id"]), _text(raw["plan_id"])
    ms = _to_date(raw["month_start"])
    prem = _to_num(raw["monthly_premium"])
    act, churn = _to_num(raw["is_active"]), _to_num(raw["is_churn_flag"])

    _flag(errs, mid == "", "member_id missing")
    _flag(errs, (mid != "") & ~mid.isin(known_members.index),
          "unknown member_id (not in members file or dimension, or the member row was quarantined)")
    _flag(errs, (pid != "") & ~pid.isin(set(plan_ids)), "unknown plan_id")
    _flag(errs, pid == "", "plan_id missing")
    _flag(errs, ms.isna(), "month_start missing or not YYYY-MM-DD")
    _flag(errs, ms.notna() & (ms.dt.day != 1), "month_start is not the first day of a month")
    _flag(errs, ms.notna() & ((ms < date_min) | (ms > date_max)), "month_start outside the DimDate range")
    _flag(errs, ~np.isfinite(prem), "monthly_premium missing or not numeric")
    _flag(errs, np.isfinite(prem) & (prem < 0), "negative monthly_premium")
    _flag(errs, np.isfinite(prem) & (prem >= MAX_MONEY), "monthly_premium out of range")
    _flag(errs, ~act.isin([0, 1]), "is_active must be 0 or 1")
    _flag(errs, ~churn.isin([0, 1]), "is_churn_flag must be 0 or 1")
    _flag(errs, (churn == 1) & (act == 0), "churn flag on an inactive month")

    # duplicates of the grain (member, month): keep the first occurrence
    key = mid + "|" + ms.dt.strftime("%Y-%m-%d").fillna("")
    _flag(errs, ms.notna() & (mid != "") & key.duplicated(keep="first"), "duplicate (member_id, month_start)")

    # cross-entity date logic against the member dates
    enr_month = _month_of(mid.map(known_members["enrollment_date"]))
    term_month = _month_of(mid.map(known_members["termination_date"]))
    _flag(errs, ms.notna() & enr_month.notna() & (ms < enr_month), "month before the member's enrollment month")
    _flag(errs, ms.notna() & term_month.notna() & (ms > term_month), "month after the member's termination month")
    _flag(errs, (churn == 1) & ms.notna() & (term_month.isna() | (ms != term_month)),
          "churn flag outside the member's termination month")

    bad, rejects = _finish("enrollments", raw, errs)
    keep = ~bad
    clean = pd.DataFrame({
        "member_id": mid[keep], "plan_id": pid[keep],
        "month_start": ms[keep].dt.date,
        "monthly_premium": prem[keep].round(2),
        "is_active": act[keep].astype(int), "is_churn_flag": churn[keep].astype(int),
    })
    return ValidationResult(clean, rejects)


def validate_claims(raw: pd.DataFrame, known_members: pd.DataFrame,
                    date_min=DEFAULT_DATE_MIN, date_max=DEFAULT_DATE_MAX) -> ValidationResult:
    errs = _new_errors(raw)
    cid, mid = _text(raw["claim_id"]), _text(raw["member_id"])
    cd = _to_date(raw["claim_date"])
    amt = _to_num(raw["claim_amount"])
    status = _text(raw["claim_status"])

    _flag(errs, cid == "", "claim_id missing")
    _flag(errs, (cid != "") & cid.duplicated(keep="first"), "duplicate claim_id")
    _flag(errs, cid.str.len() > 20, "claim_id longer than 20 characters")
    _flag(errs, mid == "", "member_id missing")
    _flag(errs, (mid != "") & ~mid.isin(known_members.index),
          "unknown member_id (not in members file or dimension, or the member row was quarantined)")
    _flag(errs, cd.isna(), "claim_date missing or not YYYY-MM-DD")
    _flag(errs, cd.notna() & ((cd < date_min) | (cd > date_max)), "claim_date outside the DimDate range")
    _flag(errs, ~np.isfinite(amt), "claim_amount missing or not numeric")
    _flag(errs, np.isfinite(amt) & (amt <= 0), "claim_amount must be > 0")
    _flag(errs, np.isfinite(amt) & (amt >= MAX_MONEY), "claim_amount out of range")
    _flag(errs, ~status.isin(CLAIM_STATUSES), "invalid claim_status")

    enr_d = mid.map(known_members["enrollment_date"])
    term_d = mid.map(known_members["termination_date"])
    _flag(errs, cd.notna() & enr_d.notna() & (cd < enr_d), "claim before the member's enrollment_date")
    _flag(errs, cd.notna() & term_d.notna() & (cd > term_d), "claim after the member's termination_date")

    bad, rejects = _finish("claims", raw, errs)
    keep = ~bad
    clean = pd.DataFrame({
        "claim_id": cid[keep], "member_id": mid[keep],
        "claim_date": cd[keep].dt.date,
        "claim_amount": amt[keep].round(2),
        "claim_status": status[keep],
    })
    return ValidationResult(clean, rejects)


def summarize_rejects(rejects: pd.DataFrame):
    """Counts of rejected rows per source and per distinct error message."""
    counts = Counter()
    for source, msg in zip(rejects["source_name"], rejects["error_message"]):
        for part in str(msg).split("; "):
            counts[(source, part)] += 1
    return counts


# ---------------------------------------------------------------------------
# Database steps
# ---------------------------------------------------------------------------
def _records(df: pd.DataFrame):
    out = []
    for row in df.itertuples(index=False, name=None):
        vals = []
        for v in row:
            if v is None or (not isinstance(v, (str, bytes)) and pd.isna(v)):
                vals.append(None)
            elif isinstance(v, pd.Timestamp):
                vals.append(v.date())
            elif isinstance(v, np.generic):
                vals.append(v.item())
            else:
                vals.append(v)
        out.append(tuple(vals))
    return out


REQUIRED_OBJECTS = [("stg_members", "U"), ("stg_enrollments", "U"), ("stg_claims", "U"),
                    ("DimMember", "U"), ("DimPlan", "U"), ("DimDate", "U"),
                    ("FactEnrollment", "U"), ("FactClaims", "U"), ("QuarantineLog", "U"),
                    ("AttritionConfig", "U"), ("vw_DQ_Checks", "V"), ("vw_AnnualChurn", "V"),
                    ("usp_LoadAttritionWarehouse", "P")]


def check_database_objects(cur):
    for name, kind in REQUIRED_OBJECTS:
        cur.execute("SELECT OBJECT_ID(?, ?);", f"dbo.{name}", kind)
        if cur.fetchone()[0] is None:
            raise RuntimeError(f"dbo.{name} does not exist. Run deploy_attrition.sql first.")


def read_reference_data(cur):
    cur.execute("SELECT ConfigValue FROM dbo.AttritionConfig WHERE ConfigKey = 'GracePeriodDays';")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("GracePeriodDays missing from dbo.AttritionConfig. Run attrition_churn_rules.sql.")
    grace = int(row[0])

    cur.execute("SELECT MIN(FullDate), MAX(FullDate) FROM dbo.DimDate;")
    dmin, dmax = cur.fetchone()
    if dmin is None:
        raise RuntimeError("dbo.DimDate is empty. Run attrition_staging.sql to populate it.")
    return grace, pd.Timestamp(dmin), pd.Timestamp(dmax)


def read_db_members(cur) -> pd.DataFrame:
    cur.execute("SELECT MemberID, EnrollmentDate, TerminationDate FROM dbo.DimMember;")
    rows = [tuple(r) for r in cur.fetchall()]
    return pd.DataFrame(rows, columns=["member_id", "enrollment_date", "termination_date"])


def read_db_plan_ids(cur):
    cur.execute("SELECT PlanID FROM dbo.DimPlan;")
    return {r[0] for r in cur.fetchall()}


def upsert_plans(cur, plans: pd.DataFrame):
    """Reference data: upsert validated plans into DimPlan (Type 1)."""
    for p in plans.itertuples(index=False):
        cur.execute("""
            MERGE dbo.DimPlan WITH (HOLDLOCK) AS tgt
            USING (SELECT CAST(? AS VARCHAR(50)) AS PlanID) AS src ON tgt.PlanID = src.PlanID
            WHEN MATCHED THEN UPDATE SET PlanName = ?, PlanTier = ?, PlanType = ?, BaseMonthlyPremium = ?
            WHEN NOT MATCHED THEN INSERT (PlanID, PlanName, PlanTier, PlanType, BaseMonthlyPremium)
                 VALUES (src.PlanID, ?, ?, ?, ?);
        """, p.plan_id, p.plan_name, p.plan_tier, p.plan_type, float(p.base_monthly_premium),
             p.plan_name, p.plan_tier, p.plan_type, float(p.base_monthly_premium))


def write_quarantine(cur, rejects: pd.DataFrame, batch_id: str):
    if rejects.empty:
        return
    cur.fast_executemany = False   # NVARCHAR(MAX) parameters; rejects are few
    cur.executemany(
        "INSERT INTO dbo.QuarantineLog (SourceName, BatchID, RecordIndex, RawRecord, ErrorMessage) "
        "VALUES (?,?,?,?,?);",
        [(r.source_name, batch_id, int(r.record_index), r.raw_record, r.error_message)
         for r in rejects.itertuples(index=False)])


def load_staging(cur, members, enrollments, claims, batch_id: str):
    """Truncate + insert the clean rows, then reconcile counts (caller commits or rolls back)."""
    cur.fast_executemany = True
    for tbl in ("stg_members", "stg_enrollments", "stg_claims"):
        cur.execute(f"TRUNCATE TABLE dbo.{tbl};")

    m, e, c = (members.assign(batch_id=batch_id), enrollments.assign(batch_id=batch_id),
               claims.assign(batch_id=batch_id))
    if len(m):
        cur.executemany("""INSERT INTO dbo.stg_members
            (member_id, age_band, region, enrollment_date, termination_date,
             termination_reason, first_missed_payment_date, member_status, batch_id)
            VALUES (?,?,?,?,?,?,?,?,?)""", _records(m))
    if len(e):
        cur.executemany("""INSERT INTO dbo.stg_enrollments
            (member_id, plan_id, month_start, monthly_premium, is_active, is_churn_flag, batch_id)
            VALUES (?,?,?,?,?,?,?)""", _records(e))
    if len(c):
        cur.executemany("""INSERT INTO dbo.stg_claims
            (claim_id, member_id, claim_date, claim_amount, claim_status, batch_id)
            VALUES (?,?,?,?,?,?)""", _records(c))

    for tbl, expected in (("stg_members", len(m)), ("stg_enrollments", len(e)), ("stg_claims", len(c))):
        cur.execute(f"SELECT COUNT(*) FROM dbo.{tbl} WHERE batch_id = ?;", batch_id)
        actual = cur.fetchone()[0]
        if actual != expected:
            raise RuntimeError(f"Row count mismatch in dbo.{tbl}: sent {expected}, staged {actual}.")


def run_transform(cur, batch_id: str):
    cur.execute("EXEC dbo.usp_LoadAttritionWarehouse @BatchID = ?;", batch_id)
    summary = [tuple(r) for r in cur.fetchall()]
    while cur.nextset():
        pass
    return summary


def run_audit(cur):
    cur.execute("SELECT CheckName, FailingRows FROM dbo.vw_DQ_Checks WHERE FailingRows > 0;")
    failures = [tuple(r) for r in cur.fetchall()]
    cur.execute("SELECT WindowStart, WindowEnd, IsWindowComplete, CohortMembers, ChurnedMembers, "
                "AnnualChurnRate FROM dbo.vw_AnnualChurn;")
    annual = cur.fetchone()
    return failures, annual


# ---------------------------------------------------------------------------
# Orchestration (reused by attrition_pipeline.py)
# ---------------------------------------------------------------------------
def read_inputs(in_dir: Path):
    raw = {
        "members": read_source(in_dir / "members.csv", MEMBER_COLS),
        "enrollments": read_source(in_dir / "enrollments.csv", ENROLLMENT_COLS),
        "claims": read_source(in_dir / "claims.csv", CLAIM_COLS),
    }
    plans_path = in_dir / "plans.csv"
    raw_plans = read_source(plans_path, PLAN_COLS) if plans_path.exists() else None
    return raw, raw_plans


def validate_batch(raw, grace, d_min, d_max, plan_ids, db_members=None):
    """Validate members first, then enrollments and claims against the known members."""
    members_res = validate_members(raw["members"], grace)
    known = build_known_members(members_res.clean, db_members)
    return {
        "members": members_res,
        "enrollments": validate_enrollments(raw["enrollments"], known, plan_ids, d_min, d_max),
        "claims": validate_claims(raw["claims"], known, d_min, d_max),
    }


def print_validation_summary(results, total_rows, log=print):
    log("Validation summary")
    for name, res in results.items():
        log(f"  {name:<12} read {total_rows[name]:>7,} | clean {len(res.clean):>7,} | "
            f"quarantined {len(res.rejects):>5,}")
    all_rejects = pd.concat([r.rejects for r in results.values()], ignore_index=True)
    if len(all_rejects):
        log("Top rejection reasons")
        for (source, msg), n in summarize_rejects(all_rejects).most_common(15):
            log(f"  {source:<12} {n:>5,}  {msg}")
    return all_rejects


def run_ingest(conn, cur, in_dir: Path, batch_id: str, log=print) -> dict:
    """CSV path: read the files in in_dir, then run the shared pipeline (ingest_raw)."""
    raw, raw_plans = read_inputs(in_dir)
    return ingest_raw(conn, cur, raw, raw_plans, in_dir, batch_id, log)


def ingest_raw(conn, cur, raw, raw_plans, report_dir: Path, batch_id: str, log=print) -> dict:
    """Validate -> quarantine -> stage -> MERGE into the warehouse, from raw text tables.

    raw = {"members", "enrollments", "claims"} DataFrames of strings; raw_plans = plans or None.
    Any source (CSV files, the API feed) that produces these tables gets identical treatment.
    Commits after each stage so quarantined rows survive a later failure; raises on any
    error (the caller rolls back and records the failure). Returns counts for run logging."""
    check_database_objects(cur)
    grace, d_min, d_max = read_reference_data(cur)
    db_members = read_db_members(cur)

    plan_result = validate_plans(raw_plans) if raw_plans is not None else None
    if plan_result is not None and len(plan_result.clean):
        upsert_plans(cur, plan_result.clean)
        conn.commit()
    plan_ids = read_db_plan_ids(cur)

    results = validate_batch(raw, grace, d_min, d_max, plan_ids, db_members)
    totals = {k: len(v) for k, v in raw.items()}
    if plan_result is not None:
        results = {"plans": plan_result, **results}
        totals["plans"] = len(raw_plans)
    log(f"Grace period: {grace} days | DimDate range: {d_min.date()} to {d_max.date()}")
    all_rejects = print_validation_summary(results, totals, log)

    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report = report_dir / "quarantine_report.csv"
    all_rejects.to_csv(report, index=False)
    log(f"Quarantine report: {report}")

    write_quarantine(cur, all_rejects, batch_id)
    conn.commit()
    if len(all_rejects):
        log(f"Wrote {len(all_rejects):,} rejected rows to dbo.QuarantineLog.")

    members, enrollments, claims = (results["members"].clean, results["enrollments"].clean,
                                    results["claims"].clean)
    load_staging(cur, members, enrollments, claims, batch_id)
    conn.commit()
    log("Staging loaded and reconciled.")

    summary = run_transform(cur, batch_id)
    conn.commit()
    staged = {"DimMember": len(members), "FactEnrollment": len(enrollments), "FactClaims": len(claims)}
    merge = {}
    log("Warehouse load (MERGE) results")
    for name, inserted, updated in summary:
        merge[name] = (int(inserted), int(updated))
        log(f"  {name:<15} inserted {inserted:>7,} | updated {updated:>7,} | "
            f"unchanged {staged[name] - inserted - updated:>7,}")

    return {
        "grace_days": grace,
        "rows_read": sum(totals.values()),
        "rows_quarantined": len(all_rejects),
        "rows_staged": sum(staged.values()),
        "merge": merge,
        "rejects": all_rejects,
    }


def dry_run_validate(raw, raw_plans, report_dir: Path, log=print) -> int:
    """Offline check: validate raw tables with the built-in defaults, touch no database."""
    if raw_plans is None:
        raise RuntimeError("--dry-run needs plans to know the valid plan_ids.")
    plan_result = validate_plans(raw_plans)
    results = validate_batch(raw, DEFAULT_GRACE_DAYS, DEFAULT_DATE_MIN, DEFAULT_DATE_MAX,
                             set(plan_result.clean["plan_id"]))
    results = {"plans": plan_result, **results}
    totals = {**{k: len(v) for k, v in raw.items()}, "plans": len(raw_plans)}
    log(f"Grace period: {DEFAULT_GRACE_DAYS} days | DimDate range: "
        f"{DEFAULT_DATE_MIN.date()} to {DEFAULT_DATE_MAX.date()} (offline defaults)")
    all_rejects = print_validation_summary(results, totals, log)
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report = report_dir / "quarantine_report.csv"
    all_rejects.to_csv(report, index=False)
    log(f"Quarantine report: {report}")
    log("Dry run complete: nothing was written to the database.")
    return 0


def print_audit(cur, log=print) -> int:
    """Print the post-load audit (window, annual churn, vw_DQ_Checks). 0 = clean, 1 = failures."""
    failures, annual = run_audit(cur)
    log("Audit")
    if annual is not None:
        w_start, w_end, complete, cohort, churned, rate = annual
        rate_txt = f"{float(rate):.1%}" if rate is not None else "n/a (window incomplete)"
        log(f"  Window {w_start} to {w_end} | cohort {cohort:,} | churned {churned:,} | annual churn {rate_txt}")
    if failures:
        log("  DATA QUALITY FAILURES:")
        for name, n in failures:
            log(f"    {n:>7,}  {name}")
        return 1
    log("  vw_DQ_Checks: all checks pass.")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", default="output", help="folder with members/enrollments/claims[/plans].csv")
    ap.add_argument("--dry-run", action="store_true", help="validate only; no database connection")
    args = ap.parse_args(argv)

    in_dir = Path(args.input_dir)
    batch_id = str(uuid.uuid4())
    print(f"Batch {batch_id} | input: {in_dir.resolve()} | mode: {'DRY RUN' if args.dry_run else 'LOAD'}")

    if args.dry_run:
        raw, raw_plans = read_inputs(in_dir)
        return dry_run_validate(raw, raw_plans, in_dir)

    import pyodbc  # lazy: --dry-run does not need it
    conn = pyodbc.connect(TARGET_CONN_STR)
    cur = conn.cursor()
    try:
        run_ingest(conn, cur, in_dir, batch_id)
        return print_audit(cur)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
