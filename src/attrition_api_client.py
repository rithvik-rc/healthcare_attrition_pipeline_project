"""
API ingestion layer: pulls the source tables from the Flask feed (attrition_api.py) and loads
them through the SAME pipeline as the CSV path.

  CSV path   attrition_ingest.run_ingest   reads members/enrollments/claims/plans CSV files
  API path   this file                      reads the same tables page by page over HTTP
             both then call attrition_ingest.ingest_raw(): validate -> quarantine -> stage -> MERGE

The API path only replaces step 1 (extract). The records arrive as raw text, in the same column
layout as the CSV files, so every validator, quarantine rule, staging step and MERGE behaves exactly
as it does for files. The CSV path is untouched and stays available.

What this layer adds on top of the plain download
  * pages through each source until has_next is false
  * checks that the rows received equal the total_rows the server reported (no silent truncation)
  * checks the required columns, like read_source() does for files
  * retries connection errors, timeouts and 429/502/503/504 responses with backoff
  * sends the X-API-Key header when a key is configured

Usage
  python attrition_api.py --data-dir output                          # terminal 1: start the feed
  python attrition_api_client.py --dry-run                           # terminal 2: validate only
  python attrition_api_client.py --api-url http://127.0.0.1:5000     # full load into SQL Server
  python attrition_pipeline.py --source api --api-url http://127.0.0.1:5000   # whole pipeline

Environment: ATTRITION_API_URL (default http://127.0.0.1:5000), ATTRITION_API_KEY
Requires: requests, pandas (+ pyodbc unless --dry-run)
"""
import argparse
import os
import sys
import time
import uuid
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import attrition_ingest as ingest  # noqa: E402  (same folder)

DEFAULT_API_URL = "http://127.0.0.1:5000"
DEFAULT_PAGE_SIZE = 1000
REQUIRED = {"members": ingest.MEMBER_COLS, "enrollments": ingest.ENROLLMENT_COLS, "claims": ingest.CLAIM_COLS}
OPTIONAL = {"plans": ingest.PLAN_COLS}
RETRY_STATUS = {429, 502, 503, 504}
MAX_PAGES = 100_000   # safety stop against a server that never reports has_next = false


class ApiError(RuntimeError):
    """The feed could not be read (unreachable, rejected, inconsistent or incomplete)."""


def default_http():
    try:
        import requests
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ApiError("The API ingestion path needs the 'requests' package: pip install requests") from exc
    return requests.Session()


def _get_json(http, url, *, params=None, headers=None, timeout=30, retries=3, backoff=1.0, sleep=time.sleep):
    """GET with retries on connection problems and transient statuses. Returns parsed JSON."""
    last = None
    for attempt in range(1, retries + 1):
        try:
            resp = http.get(url, params=params, headers=headers, timeout=timeout)
        except OSError as exc:      # requests' connection / timeout errors are OSError subclasses
            last = f"cannot reach {url} ({exc.__class__.__name__}: {exc})"
        else:
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as exc:
                    raise ApiError(f"{url} did not return JSON") from exc
            if resp.status_code in RETRY_STATUS:
                last = f"{url} returned HTTP {resp.status_code}"
            else:
                detail = ""
                try:
                    detail = resp.json().get("error", "")
                except Exception:
                    detail = (getattr(resp, "text", "") or "")[:200]
                hint = " (set --api-key / ATTRITION_API_KEY)" if resp.status_code == 401 else ""
                raise ApiError(f"{url} returned HTTP {resp.status_code}: {detail}{hint}")
        if attempt < retries:
            sleep(backoff * (2 ** (attempt - 1)))
    raise ApiError(f"giving up after {retries} attempts: {last}")


class ApiSession:
    """Bundles base URL, key and HTTP settings so each call site stays short."""

    def __init__(self, base_url, api_key=None, page_size=DEFAULT_PAGE_SIZE, http=None,
                 timeout=30, retries=3, backoff=1.0, sleep=time.sleep):
        self.base = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.page_size = page_size
        self.http = http if http is not None else default_http()
        self.timeout, self.retries, self.backoff, self.sleep = timeout, retries, backoff, sleep

    def get(self, path, params=None):
        return _get_json(self.http, f"{self.base}{path}", params=params, headers=self.headers,
                         timeout=self.timeout, retries=self.retries, backoff=self.backoff, sleep=self.sleep)


def fetch_source(api: ApiSession, source: str, required_cols, expected_rows=None, log=print) -> pd.DataFrame:
    """Download every page of one source and return it as a raw-text DataFrame."""
    frames, columns, total, page = [], None, None, 1
    while True:
        body = api.get(f"/api/v1/ingest/{source}", {"page": page, "page_size": api.page_size})
        if body.get("source") != source:
            raise ApiError(f"asked for '{source}' but the API answered with '{body.get('source')}'")
        if total is None:
            total, columns = body["total_rows"], body["columns"]
        elif body["total_rows"] != total:
            raise ApiError(f"{source}: the source changed while downloading "
                           f"(total_rows {total} then {body['total_rows']}). Rerun when the feed is stable.")
        if body["records"]:
            frames.append(pd.DataFrame.from_records(body["records"], columns=columns))
        if not body["has_next"]:
            break
        page += 1
        if page > MAX_PAGES:
            raise ApiError(f"{source}: more than {MAX_PAGES} pages; stopping")

    df = (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)).astype(str)
    if len(df) != total:
        raise ApiError(f"{source}: received {len(df):,} rows but the API reported {total:,}")
    if expected_rows is not None and expected_rows != total:
        raise ApiError(f"{source}: manifest said {expected_rows:,} rows but the data pages said {total:,}")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{source} (API) is missing required columns: {missing}")
    log(f"  fetched {source:<12} {len(df):>8,} rows in {page} page(s)")
    return df


def fetch_inputs(api: ApiSession, log=print):
    """Same return shape as attrition_ingest.read_inputs(): (raw tables dict, plans DataFrame or None)."""
    log(f"Reading from API {api.base}")
    manifest = api.get("/api/v1/ingest/manifest")["sources"]
    for name in REQUIRED:
        if not manifest.get(name, {}).get("available"):
            raise ApiError(f"the API has no '{name}' data; generate it first "
                           f"(python generate_attrition_data.py) and check attrition_api.py --data-dir")
    raw = {name: fetch_source(api, name, cols, manifest[name]["total_rows"], log)
           for name, cols in REQUIRED.items()}
    raw_plans = None
    if manifest.get("plans", {}).get("available"):
        raw_plans = fetch_source(api, "plans", OPTIONAL["plans"], manifest["plans"]["total_rows"], log)
    return raw, raw_plans


def run_api_ingest(conn, cur, api: ApiSession, batch_id: str, report_dir=Path("output"), log=print) -> dict:
    """API path: fetch the tables, then run the shared pipeline (validate -> ... -> MERGE)."""
    raw, raw_plans = fetch_inputs(api, log)
    stats = ingest.ingest_raw(conn, cur, raw, raw_plans, Path(report_dir), batch_id, log)
    stats["source"] = "api"
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api-url", default=os.environ.get("ATTRITION_API_URL", DEFAULT_API_URL))
    ap.add_argument("--api-key", default=os.environ.get("ATTRITION_API_KEY"))
    ap.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="rows per request (max 5000)")
    ap.add_argument("--report-dir", default="output", help="where quarantine_report.csv is written")
    ap.add_argument("--dry-run", action="store_true", help="fetch and validate only; no database connection")
    args = ap.parse_args(argv)

    api = ApiSession(args.api_url, args.api_key, args.page_size)
    batch_id = str(uuid.uuid4())
    print(f"Batch {batch_id} | source: API {api.base} | mode: {'DRY RUN' if args.dry_run else 'LOAD'}")
    try:
        if args.dry_run:
            raw, raw_plans = fetch_inputs(api)
            return ingest.dry_run_validate(raw, raw_plans, Path(args.report_dir))

        import pyodbc  # lazy: --dry-run does not need it
        conn = pyodbc.connect(ingest.TARGET_CONN_STR)
        cur = conn.cursor()
        try:
            run_api_ingest(conn, cur, api, batch_id, Path(args.report_dir))
            return ingest.print_audit(cur)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()
    except ApiError as exc:
        print(f"API INGESTION ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
