"""
Flask API: serves the attrition source CSVs as a paged JSON ingestion feed.

This is the server half of the API ingestion path. The client half (attrition_api_client.py)
pulls these pages and feeds them to the same validate -> quarantine -> stage -> MERGE code the
CSV path uses (attrition_ingest.ingest_raw), so nothing downstream knows which path was used.

Endpoints
  GET /health                          liveness + which CSV files are present (no API key needed)
  GET /api/v1/ingest/manifest          row count, page count and columns for every source
  GET /api/v1/ingest/<source>          one page of raw records; source = members | enrollments |
                                       claims | plans
        ?page=1            1-based page number (default 1)
        &page_size=1000    rows per page (default 1000, maximum 5000)

Page response (JSON)
  {"source", "page", "page_size", "total_rows", "total_pages", "has_next", "columns", "records"}
  Every value in "records" is the raw CSV text (blanks stay "" and are never turned into null),
  so the validators see exactly what they would see reading the file.

Security
  Set ATTRITION_API_KEY (or --api-key) to require an "X-API-Key" header on /api/v1/*.
  With no key configured the API is open, which is only suitable for 127.0.0.1 on a dev machine.
  The server binds to 127.0.0.1 by default. The built-in Flask server is for development; behind
  a real network use a production server (for example: waitress-serve --call attrition_api:create_app).

Usage
  python attrition_api.py                              # serve ./output on http://127.0.0.1:5000
  python attrition_api.py --data-dir output --port 5050
  set ATTRITION_API_KEY=change-me && python attrition_api.py

Requires: flask, pandas
"""
import argparse
import hmac
import os
import sys
import threading
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request

# Required columns per source (mirrors attrition_ingest.py). Extra columns are passed through.
SOURCES = {
    "members": ["member_id", "age_band", "region", "enrollment_date", "termination_date",
                "termination_reason", "first_missed_payment_date", "member_status"],
    "enrollments": ["member_id", "plan_id", "month_start", "monthly_premium", "is_active", "is_churn_flag"],
    "claims": ["claim_id", "member_id", "claim_date", "claim_amount", "claim_status"],
    "plans": ["plan_id", "plan_name", "plan_tier", "plan_type", "base_monthly_premium"],
}
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 5000


class SourceStore:
    """Reads each CSV as raw text and caches it until the file changes (path + mtime + size)."""

    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self._cache = {}
        self._lock = threading.Lock()

    def path(self, source):
        return self.data_dir / f"{source}.csv"

    def available(self):
        return {s: self.path(s).exists() for s in SOURCES}

    def load(self, source):
        path = self.path(source)
        if not path.exists():
            raise FileNotFoundError(f"{path.name} not found in {self.data_dir}")
        stat = path.stat()
        stamp = (stat.st_mtime_ns, stat.st_size)
        with self._lock:
            hit = self._cache.get(source)
            if hit and hit[0] == stamp:
                return hit[1]
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        missing = [c for c in SOURCES[source] if c not in df.columns]
        if missing:
            raise ValueError(f"{path.name} is missing required columns: {missing}")
        with self._lock:
            self._cache[source] = (stamp, df)
        return df


def _error(status, message):
    return jsonify({"error": message}), status


def _int_arg(name, default, minimum, maximum):
    raw = request.args.get(name)
    if raw is None or raw == "":
        return default, None
    try:
        value = int(raw)
    except ValueError:
        return None, f"{name} must be an integer"
    if value < minimum or (maximum is not None and value > maximum):
        bound = f"between {minimum} and {maximum}" if maximum is not None else f"at least {minimum}"
        return None, f"{name} must be {bound}"
    return value, None


def create_app(data_dir="output", api_key=None):
    """App factory (used by the CLI, by tests, and by production servers)."""
    app = Flask(__name__)
    app.json.sort_keys = False
    store = SourceStore(data_dir)
    key = api_key if api_key is not None else os.environ.get("ATTRITION_API_KEY") or None
    app.config["ATTRITION_STORE"] = store

    @app.before_request
    def require_key():
        if key is None or not request.path.startswith("/api/"):
            return None
        sent = request.headers.get("X-API-Key", "")
        if not hmac.compare_digest(sent.encode("utf-8"), key.encode("utf-8")):
            return _error(401, "missing or invalid X-API-Key header")
        return None

    @app.errorhandler(404)
    def not_found(_):
        return _error(404, "not found")

    @app.errorhandler(405)
    def not_allowed(_):
        return _error(405, "method not allowed; this API is read-only (GET)")

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "data_dir": str(store.data_dir.resolve()),
                        "auth_required": key is not None, "sources": store.available()})

    @app.get("/api/v1/ingest/manifest")
    def manifest():
        out = {}
        for source in SOURCES:
            try:
                df = store.load(source)
            except FileNotFoundError:
                out[source] = {"available": False}
                continue
            except ValueError as exc:
                return _error(500, str(exc))
            out[source] = {"available": True, "total_rows": int(len(df)), "columns": list(df.columns),
                           "default_page_size": DEFAULT_PAGE_SIZE, "max_page_size": MAX_PAGE_SIZE}
        return jsonify({"sources": out})

    @app.get("/api/v1/ingest/<source>")
    def ingest_page(source):
        if source not in SOURCES:
            return _error(404, f"unknown source '{source}'; use one of {sorted(SOURCES)}")
        page, err = _int_arg("page", 1, 1, None)
        if err:
            return _error(400, err)
        size, err = _int_arg("page_size", DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
        if err:
            return _error(400, err)
        try:
            df = store.load(source)
        except FileNotFoundError as exc:
            return _error(404, str(exc))
        except ValueError as exc:
            return _error(500, str(exc))

        total = int(len(df))
        pages = max(1, -(-total // size))
        start = (page - 1) * size
        chunk = df.iloc[start:start + size]
        return jsonify({
            "source": source, "page": page, "page_size": size,
            "total_rows": total, "total_pages": pages, "has_next": start + size < total,
            "columns": list(df.columns), "records": chunk.to_dict(orient="records"),
        })

    return app


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="output", help="folder with members/enrollments/claims/plans CSVs")
    ap.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1, local only)")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--api-key", help="require this X-API-Key (default: ATTRITION_API_KEY environment variable)")
    args = ap.parse_args(argv)

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        print(f"Data folder not found: {data_dir.resolve()}\nGenerate it first: python generate_attrition_data.py",
              file=sys.stderr)
        return 2
    app = create_app(data_dir, args.api_key)
    store = app.config["ATTRITION_STORE"]
    present = [s for s, ok in store.available().items() if ok]
    print(f"Serving {data_dir.resolve()} ({', '.join(present) or 'no CSVs yet'}) at "
          f"http://{args.host}:{args.port}")
    if args.api_key is None and not os.environ.get("ATTRITION_API_KEY"):
        print("WARNING: no API key set; /api/v1/* is open. Keep the host on 127.0.0.1 or set ATTRITION_API_KEY.")
    app.run(host=args.host, port=args.port, debug=False, threaded=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
