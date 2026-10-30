"""
Phase 5: end-to-end orchestrator for the attrition pipeline.

One run does:
  1 lock       one run at a time (SQL Server application lock; exit code 3 if another run holds it)
  2 preflight  every required table, view and procedure exists
  3 generate   (optional, --generate) synthetic CSVs from generate_attrition_data.py
  4 ingest     validate -> quarantine -> stage -> MERGE   (attrition_ingest.run_ingest for --source csv,
               attrition_api_client.run_api_ingest for --source api; same validation and load code)
  5 metrics    EXEC dbo.usp_BuildAttritionMetrics        (features, risk flags, cohort retention)
  6 audit      EXEC dbo.usp_RunAttritionAudits + a quarantine-rate check; results are saved in
               dbo.PipelineAuditResult
  7 export     (optional, --export-dir) Tableau reporting views as CSV

The run is recorded in dbo.PipelineRunLog (counts, status, error) so history survives failures.

Exit codes (used by run_attrition_pipeline.bat and the SQL Server Agent job)
  0 succeeded (warnings allowed)   1 audit failed (or warnings with --strict)
  2 pipeline error                 3 another run is in progress

Usage
  python attrition_pipeline.py --generate                  # synthetic test run
  python attrition_pipeline.py --input-dir output          # CSVs already in place
  python attrition_pipeline.py --source api --api-url http://127.0.0.1:5000   # read through the Flask feed
  python attrition_pipeline.py --skip-ingest               # rebuild metrics + audits only
  python attrition_pipeline.py --log-dir logs --export-dir exports --strict

History:  SELECT * FROM dbo.vw_PipelineRunHistory ORDER BY RunID DESC;
Requires: pandas, numpy, pyodbc.
"""
import argparse
import getpass
import importlib.util
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ingest = _load("attrition_ingest", "attrition_ingest.py")
api_client = _load("attrition_api_client", "attrition_api_client.py")
generator = _load("generate_attrition_data", "generate_attrition_data.py")

EXIT_OK, EXIT_AUDIT_FAILED, EXIT_ERROR, EXIT_LOCKED = 0, 1, 2, 3
LOCK_NAME = "AttritionPipeline"
DEFAULT_MAX_QUARANTINE_PCT = 5
WRONG_FILE_PCT = 50   # more than this share quarantined = wrong file or format, not dirty data

PIPELINE_OBJECTS = [("MemberMonthFeatures", "U"), ("CohortRetention", "U"), ("PipelineRunLog", "U"),
                    ("PipelineAuditResult", "U"), ("usp_BuildAttritionMetrics", "P"),
                    ("usp_RunAttritionAudits", "P"), ("vw_Rpt_ExecutiveSummary", "V"),
                    ("vw_PipelineRunHistory", "V")]
RUNLOG_COLUMNS = {"BatchID", "FinishedAt", "Status", "TriggeredBy", "RowsRead", "RowsQuarantined",
                  "RowsStaged", "MemberInserts", "MemberUpdates", "EnrollmentInserts", "EnrollmentUpdates",
                  "ClaimInserts", "ClaimUpdates", "FeatureRows", "AnnualChurnRate", "ErrorMessage"}
REPORT_VIEWS = ["vw_Rpt_ExecutiveSummary", "vw_Rpt_MonthlyChurn", "vw_Rpt_SegmentChurn",
                "vw_Rpt_CohortRetention", "vw_Rpt_CohortMilestones", "vw_Rpt_AtRiskMembers",
                "vw_Rpt_TerminationReasons", "vw_Rpt_RenewalIncreaseChurn"]


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested without a database)
# ---------------------------------------------------------------------------
def quarantine_rate_check(rows_read, quarantined, max_pct=DEFAULT_MAX_QUARANTINE_PCT):
    """PASS up to max_pct quarantined, WARN above it, FAIL above 50% (wrong file or format)."""
    if rows_read <= 0:
        return "PASS", "no source rows read"
    pct = 100.0 * quarantined / rows_read
    detail = f"{quarantined:,} of {rows_read:,} source rows quarantined ({pct:.1f}%; warn above {max_pct}%)"
    if pct > WRONG_FILE_PCT:
        return "FAIL", detail + " - more than half the file was rejected; check the file or its format"
    if pct > max_pct:
        return "WARN", detail
    return "PASS", detail


def decide_status(audits, strict=False):
    """audits = [(check_name, 'PASS'|'WARN'|'FAIL', detail)] -> (run status, exit code)."""
    statuses = {a[1] for a in audits}
    if "FAIL" in statuses:
        return "Failed", EXIT_AUDIT_FAILED
    if "WARN" in statuses:
        return "Warnings", EXIT_AUDIT_FAILED if strict else EXIT_OK
    return "Succeeded", EXIT_OK


def build_update_sql(columns):
    """Parameterised UPDATE of dbo.PipelineRunLog; column names are checked against a whitelist."""
    unknown = [c for c in columns if c not in RUNLOG_COLUMNS]
    if unknown:
        raise ValueError(f"unknown PipelineRunLog column(s): {unknown}")
    return f"UPDATE dbo.PipelineRunLog SET {', '.join(f'{c} = ?' for c in columns)} WHERE RunID = ?;"


class Tee:
    """Mirror stdout to a log file so scheduled runs leave a readable record."""

    def __init__(self, stream, path):
        self.stream, self.file = stream, open(path, "a", encoding="utf-8")

    def write(self, text):
        self.stream.write(text)
        self.file.write(text)

    def flush(self):
        self.stream.flush()
        self.file.flush()

    def close(self):
        self.file.close()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def acquire_lock(cur) -> bool:
    cur.execute("SET NOCOUNT ON; DECLARE @r INT; "
                "EXEC @r = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
                "@LockOwner = 'Session', @LockTimeout = 0; SELECT @r;", LOCK_NAME)
    return cur.fetchone()[0] >= 0


def preflight(cur):
    ingest.check_database_objects(cur)
    for name, kind in PIPELINE_OBJECTS:
        cur.execute("SELECT OBJECT_ID(?, ?);", f"dbo.{name}", kind)
        if cur.fetchone()[0] is None:
            raise RuntimeError(f"dbo.{name} does not exist. Run deploy_attrition.sql first.")


class RunLog:
    """dbo.PipelineRunLog writes on a separate autocommit connection: they survive rollbacks."""

    def __init__(self, conn):
        self.cur = conn.cursor()
        self.run_id = None

    def start(self, batch_id, triggered_by):
        self.cur.execute("INSERT INTO dbo.PipelineRunLog (BatchID, TriggeredBy) OUTPUT INSERTED.RunID VALUES (?, ?);",
                         batch_id, triggered_by[:100])
        self.run_id = int(self.cur.fetchone()[0])
        return self.run_id

    def update(self, **cols):
        if cols and self.run_id is not None:
            self.cur.execute(build_update_sql(list(cols)), *cols.values(), self.run_id)

    def finish(self, status, **cols):
        self.update(Status=status, FinishedAt=datetime.now(timezone.utc).replace(tzinfo=None), **cols)


def run_metrics(conn, cur):
    cur.execute("EXEC dbo.usp_BuildAttritionMetrics;")
    rows = {r[0]: int(r[1]) for r in cur.fetchall()}
    while cur.nextset():
        pass
    conn.commit()
    return rows


def run_audits(conn, cur, run_id, batch_id, ingest_stats):
    """Data-side audits (stored procedure) + the quarantine-rate check; returns [(name, status, detail)]."""
    cur.execute("EXEC dbo.usp_RunAttritionAudits @RunID = ?, @BatchID = ?;", run_id, batch_id)
    audits = [tuple(r) for r in cur.fetchall()]
    while cur.nextset():
        pass
    if ingest_stats is not None:
        cur.execute("SELECT ConfigValue FROM dbo.AttritionConfig WHERE ConfigKey = 'MaxQuarantinePct';")
        row = cur.fetchone()
        max_pct = int(row[0]) if row else DEFAULT_MAX_QUARANTINE_PCT
        status, detail = quarantine_rate_check(ingest_stats["rows_read"], ingest_stats["rows_quarantined"], max_pct)
        cur.execute("INSERT INTO dbo.PipelineAuditResult (RunID, CheckName, Status, Detail) VALUES (?,?,?,?);",
                    run_id, "Quarantine rate", status, detail)
        audits.append(("Quarantine rate", status, detail))
    conn.commit()
    return audits


def print_kpis(cur):
    cur.execute("SELECT WindowStart, WindowEnd, IsWindowComplete, AnnualChurnRate, AnnualRetentionRate, "
                "ActiveMembersLatestMonth, ChurnedAnnualizedPremium, AtRiskMembers, AnnualizedPremiumAtRisk "
                "FROM dbo.vw_Rpt_ExecutiveSummary;")
    r = cur.fetchone()
    if r is None:
        return None
    w0, w1, complete, churn, retention, active, lost, at_risk, at_risk_prem = r
    pct = lambda v: f"{float(v):.1%}" if v is not None else "n/a (window incomplete)"
    print("\nHeadline KPIs\n"
          f"  Window: {w0} to {w1} ({'complete' if complete else 'INCOMPLETE'})\n"
          f"  Annual churn: {pct(churn)} | retention: {pct(retention)}\n"
          f"  Active members (latest month): {active:,}\n"
          f"  Annualized premium churned in window: ${float(lost or 0):,.0f}\n"
          f"  At-risk members: {at_risk:,} (annualized premium ${float(at_risk_prem or 0):,.0f})")
    return churn


def export_views(cur, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for view in REPORT_VIEWS:
        cur.execute(f"SELECT * FROM dbo.{view};")
        cols = [c[0] for c in cur.description]
        df = pd.DataFrame([tuple(r) for r in cur.fetchall()], columns=cols)
        df.to_csv(out / f"{view}.csv", index=False)
        print(f"  exported {view}: {len(df):,} rows")


# ---------------------------------------------------------------------------
def run(args, connect=None) -> int:
    """connect(autocommit=False) -> pyodbc-style connection (injectable for tests)."""
    if connect is None:
        import pyodbc  # lazy: helpers and tests do not need the driver
        connect = lambda autocommit=False: pyodbc.connect(ingest.TARGET_CONN_STR, autocommit=autocommit)

    batch_id = str(uuid.uuid4())
    started = time.time()
    conn = connect()
    cur = conn.cursor()
    log_conn = connect(autocommit=True)
    runlog = RunLog(log_conn)
    stats = None
    try:
        if not acquire_lock(cur):
            print("Another pipeline run is in progress; exiting.")
            return EXIT_LOCKED
        preflight(cur)
        run_id = runlog.start(batch_id, args.triggered_by)
        print(f"Pipeline run {run_id} | batch {batch_id} | triggered by {args.triggered_by}")

        if args.generate:
            print("\n=== generate ===")
            generator.main(["--members", str(args.members), "--seed", str(args.seed),
                            "--out-dir", str(args.input_dir)])

        if not args.skip_ingest:
            print("\n=== ingest ===")
            if args.source == "api":
                print(f"Source: API {args.api_url}")
                api = api_client.ApiSession(args.api_url, args.api_key, args.api_page_size)
                stats = api_client.run_api_ingest(conn, cur, api, batch_id, Path(args.input_dir))
            else:
                print(f"Source: CSV files in {args.input_dir}")
                stats = ingest.run_ingest(conn, cur, Path(args.input_dir), batch_id)
            merge = stats["merge"]
            runlog.update(RowsRead=stats["rows_read"], RowsQuarantined=stats["rows_quarantined"],
                          RowsStaged=stats["rows_staged"],
                          MemberInserts=merge["DimMember"][0], MemberUpdates=merge["DimMember"][1],
                          EnrollmentInserts=merge["FactEnrollment"][0], EnrollmentUpdates=merge["FactEnrollment"][1],
                          ClaimInserts=merge["FactClaims"][0], ClaimUpdates=merge["FactClaims"][1])
        else:   # audit against whatever batch is currently staged
            cur.execute("SELECT TOP 1 batch_id FROM dbo.stg_members;")
            row = cur.fetchone()
            batch_id = row[0] if row else batch_id
            runlog.update(BatchID=batch_id)

        print("\n=== metrics ===")
        built = run_metrics(conn, cur)
        runlog.update(FeatureRows=built.get("MemberMonthFeatures"))
        print("  " + ", ".join(f"{k}: {v:,}" for k, v in built.items()))

        print("\n=== audit ===")
        audits = run_audits(conn, cur, run_id, batch_id, stats)
        for name, status, detail in audits:
            print(f"  [{status}] {name}: {detail}")
        churn = print_kpis(cur)
        status, code = decide_status(audits, strict=args.strict)
        runlog.finish(status, AnnualChurnRate=None if churn is None else float(churn))

        if args.export_dir and status != "Failed":
            print("\n=== export ===")
            export_views(cur, args.export_dir)

        print(f"\nRun {run_id} {status.upper()} in {time.time() - started:.1f}s (exit code {code})")
        return code
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        message = f"{type(exc).__name__}: {exc}"
        print(f"\nPIPELINE ERROR: {message}")
        try:
            runlog.finish("Failed", ErrorMessage=message[:2000])
        except Exception:
            pass
        return EXIT_ERROR
    finally:
        for c in (cur, runlog.cur):
            try:
                c.close()
            except Exception:
                pass
        conn.close()          # also releases the session-level application lock
        log_conn.close()


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", default="output", help="folder with the source CSVs (default: output); with --source api it only receives the quarantine report, and --generate writes the CSVs the API serves")
    ap.add_argument("--source", choices=["csv", "api"], default="csv",
                    help="where the data comes from: csv files in --input-dir (default) or the Flask API feed")
    ap.add_argument("--api-url", default=os.environ.get("ATTRITION_API_URL", api_client.DEFAULT_API_URL),
                    help="base URL of attrition_api.py (with --source api)")
    ap.add_argument("--api-key", default=os.environ.get("ATTRITION_API_KEY"),
                    help="X-API-Key for the feed (default: ATTRITION_API_KEY environment variable)")
    ap.add_argument("--api-page-size", type=int, default=api_client.DEFAULT_PAGE_SIZE,
                    help="rows per API request (with --source api; max 5000)")
    ap.add_argument("--log-dir", help="also write the console output to a timestamped file in this folder")
    ap.add_argument("--generate", action="store_true", help="create synthetic CSVs first (test mode)")
    ap.add_argument("--members", type=int, default=2000, help="synthetic members (with --generate)")
    ap.add_argument("--seed", type=int, default=42, help="random seed (with --generate)")
    ap.add_argument("--skip-ingest", action="store_true", help="reuse the loaded warehouse; rebuild metrics + audits")
    ap.add_argument("--export-dir", help="also write the Tableau reporting views to CSV in this folder")
    ap.add_argument("--strict", action="store_true", help="treat audit warnings as a failure (exit code 1)")
    ap.add_argument("--triggered-by", default=os.environ.get("USERNAME") or getpass.getuser(),
                    help="who or what started the run (stored in the run log)")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    tee = None
    if args.log_dir:
        Path(args.log_dir).mkdir(parents=True, exist_ok=True)
        tee = Tee(sys.stdout, Path(args.log_dir) / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log")
        sys.stdout = tee
    try:
        return run(args)
    finally:
        if tee is not None:
            sys.stdout = tee.stream
            tee.close()


if __name__ == "__main__":
    sys.exit(main())
