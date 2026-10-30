"""
Tests for the API ingestion path: the Flask feed (attrition_api.py), the client
(attrition_api_client.py) and the pipeline switch (--source api).

The key guarantee: the API path hands the SAME raw tables to the SAME validation / staging / MERGE
code as the CSV path, so both paths produce identical results from the same files.

No database or network is used: Flask's test client stands in for the server, and the database
helpers in attrition_ingest are replaced by recorders.
Run:  python -m unittest test_attrition_api -v
"""
import os
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

HERE = Path(__file__).parent
# Source code lives in ../src (override with ATTRITION_SRC); tests live here.
SRC = Path(os.environ.get("ATTRITION_SRC", HERE.parent / "src")).resolve()
sys.path.insert(0, str(SRC))

import attrition_api as api_mod            # noqa: E402
import attrition_api_client as client      # noqa: E402
import attrition_ingest as ing             # noqa: E402


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SRC / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gen = _load("gen_for_api_tests", "generate_attrition_data.py")
pipe = _load("pipe_for_api_tests", "attrition_pipeline.py")


class FlaskHttp:
    """requests-like adapter over Flask's test client (no sockets)."""

    def __init__(self, app, base="http://feed.test"):
        self.client, self.base, self.calls = app.test_client(), base, []

    def get(self, url, params=None, headers=None, timeout=None):
        assert url.startswith(self.base), url
        self.calls.append((url, params))
        resp = self.client.get(url[len(self.base):], query_string=params, headers=headers)
        return _Resp(resp)


class _Resp:
    def __init__(self, r):
        self.status_code, self._r = r.status_code, r

    def json(self):
        return self._r.get_json()

    @property
    def text(self):
        return self._r.get_data(as_text=True)


def write_csvs(out: Path, members=300, inject_bad=0):
    m, p, e, c = gen.simulate(members, 7)
    if inject_bad:
        m, e, c = gen.inject_bad_rows(m, e, c, inject_bad)
    out.mkdir(parents=True, exist_ok=True)
    m.to_csv(out / "members.csv", index=False)
    p.to_csv(out / "plans.csv", index=False)
    e.to_csv(out / "enrollments.csv", index=False)
    c.to_csv(out / "claims.csv", index=False)


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.dir = Path(cls._tmp.name) / "csv"
        write_csvs(cls.dir, inject_bad=3)         # dirty rows on purpose: raw text must survive
        cls.app = api_mod.create_app(cls.dir)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def session(self, **kw):
        return client.ApiSession("http://feed.test", http=FlaskHttp(self.app), sleep=lambda s: None, **kw)


# --------------------------------------------------------------------------- the Flask feed
class FeedEndpoints(Base):
    def setUp(self):
        self.c = self.app.test_client()

    def test_health_lists_sources(self):
        body = self.c.get("/health").get_json()
        self.assertEqual(body["status"], "ok")
        self.assertTrue(all(body["sources"].values()))
        self.assertFalse(body["auth_required"])

    def test_manifest_matches_the_files(self):
        sources = self.c.get("/api/v1/ingest/manifest").get_json()["sources"]
        for name in ("members", "enrollments", "claims", "plans"):
            n = len(pd.read_csv(self.dir / f"{name}.csv", dtype=str, keep_default_na=False))
            self.assertEqual(sources[name]["total_rows"], n)

    def test_pages_cover_every_row_exactly_once(self):
        got, page = [], 1
        while True:
            body = self.c.get(f"/api/v1/ingest/enrollments?page={page}&page_size=400").get_json()
            got += body["records"]
            if not body["has_next"]:
                break
            page += 1
        file_rows = pd.read_csv(self.dir / "enrollments.csv", dtype=str, keep_default_na=False)
        self.assertEqual(len(got), len(file_rows))
        self.assertEqual(body["total_pages"], page)
        self.assertEqual(pd.DataFrame(got, columns=file_rows.columns).to_dict("records"),
                         file_rows.to_dict("records"))

    def test_values_are_raw_text_blanks_stay_blank(self):
        recs = self.c.get("/api/v1/ingest/members?page_size=5000").get_json()["records"]
        self.assertTrue(all(isinstance(v, str) for r in recs for v in r.values()))
        self.assertTrue(any(r["termination_date"] == "" for r in recs))     # active members

    def test_page_beyond_the_end_is_empty_not_an_error(self):
        body = self.c.get("/api/v1/ingest/plans?page=99").get_json()
        self.assertEqual(body["records"], [])
        self.assertFalse(body["has_next"])

    def test_bad_parameters_are_400(self):
        for q in ("page=0", "page=abc", "page_size=0", f"page_size={api_mod.MAX_PAGE_SIZE + 1}", "page=-2"):
            r = self.c.get(f"/api/v1/ingest/claims?{q}")
            self.assertEqual(r.status_code, 400, q)
            self.assertIn("error", r.get_json())

    def test_unknown_source_and_read_only(self):
        self.assertEqual(self.c.get("/api/v1/ingest/secrets").status_code, 404)
        self.assertEqual(self.c.post("/api/v1/ingest/members").status_code, 405)
        self.assertEqual(self.c.get("/nope").status_code, 404)

    def test_missing_file_is_404_and_manifest_marks_it(self):
        with tempfile.TemporaryDirectory() as d:
            write_csvs(Path(d))
            (Path(d) / "claims.csv").unlink()
            app = api_mod.create_app(d)
            self.assertEqual(app.test_client().get("/api/v1/ingest/claims").status_code, 404)
            man = app.test_client().get("/api/v1/ingest/manifest").get_json()["sources"]
            self.assertFalse(man["claims"]["available"])
            self.assertTrue(man["members"]["available"])

    def test_csv_missing_required_columns_is_a_500_with_a_message(self):
        with tempfile.TemporaryDirectory() as d:
            write_csvs(Path(d))
            pd.read_csv(Path(d) / "claims.csv").drop(columns=["claim_status"]).to_csv(Path(d) / "claims.csv", index=False)
            r = api_mod.create_app(d).test_client().get("/api/v1/ingest/claims")
            self.assertEqual(r.status_code, 500)
            self.assertIn("claim_status", r.get_json()["error"])

    def test_file_changes_are_picked_up(self):
        with tempfile.TemporaryDirectory() as d:
            write_csvs(Path(d), members=200)
            app = api_mod.create_app(d)
            before = app.test_client().get("/api/v1/ingest/members").get_json()["total_rows"]
            write_csvs(Path(d), members=250)
            after = app.test_client().get("/api/v1/ingest/members").get_json()["total_rows"]
            self.assertEqual((before, after), (200, 250))


class FeedAuth(Base):
    def test_key_required_only_on_api_routes(self):
        c = api_mod.create_app(self.dir, api_key="s3cret").test_client()
        self.assertEqual(c.get("/health").status_code, 200)
        self.assertEqual(c.get("/api/v1/ingest/plans").status_code, 401)
        self.assertEqual(c.get("/api/v1/ingest/plans", headers={"X-API-Key": "wrong"}).status_code, 401)
        self.assertEqual(c.get("/api/v1/ingest/plans", headers={"X-API-Key": "s3cret"}).status_code, 200)

    def test_client_sends_the_key(self):
        app = api_mod.create_app(self.dir, api_key="s3cret")
        ok = client.ApiSession("http://feed.test", "s3cret", http=FlaskHttp(app), sleep=lambda s: None)
        self.assertEqual(len(client.fetch_inputs(ok, log=lambda *_: None)[0]), 3)
        bad = client.ApiSession("http://feed.test", None, http=FlaskHttp(app), sleep=lambda s: None)
        with self.assertRaisesRegex(client.ApiError, "401.*api-key"):
            client.fetch_inputs(bad, log=lambda *_: None)


# --------------------------------------------------------------------------- the client
class ClientMatchesCsvPath(Base):
    def test_fetched_tables_equal_the_csv_tables(self):
        raw_csv, plans_csv = ing.read_inputs(self.dir)
        raw_api, plans_api = client.fetch_inputs(self.session(page_size=250), log=lambda *_: None)
        for name in raw_csv:
            pd.testing.assert_frame_equal(raw_api[name], raw_csv[name], check_dtype=False, obj=name)
        pd.testing.assert_frame_equal(plans_api, plans_csv, check_dtype=False, obj="plans")

    def test_page_size_does_not_change_the_result(self):
        a, _ = client.fetch_inputs(self.session(page_size=7), log=lambda *_: None)
        b, _ = client.fetch_inputs(self.session(page_size=5000), log=lambda *_: None)
        for name in a:
            pd.testing.assert_frame_equal(a[name], b[name], obj=name)

    def test_validation_results_are_identical(self):
        raw_csv, plans_csv = ing.read_inputs(self.dir)
        raw_api, plans_api = client.fetch_inputs(self.session(), log=lambda *_: None)
        ids = set(ing.validate_plans(plans_csv).clean["plan_id"])
        r_csv = ing.validate_batch(raw_csv, 15, ing.DEFAULT_DATE_MIN, ing.DEFAULT_DATE_MAX, ids)
        r_api = ing.validate_batch(raw_api, 15, ing.DEFAULT_DATE_MIN, ing.DEFAULT_DATE_MAX, ids)
        for name in r_csv:
            pd.testing.assert_frame_equal(r_api[name].clean, r_csv[name].clean, obj=f"{name} clean")
            pd.testing.assert_frame_equal(r_api[name].rejects, r_csv[name].rejects, obj=f"{name} rejects")
        self.assertGreater(sum(len(r.rejects) for r in r_api.values()), 0)   # the dirty rows were caught

    def test_dry_run_through_the_api_writes_a_quarantine_report(self):
        raw, plans = client.fetch_inputs(self.session(), log=lambda *_: None)
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(ing.dry_run_validate(raw, plans, Path(d), log=lambda *_: None), 0)
            self.assertTrue((Path(d) / "quarantine_report.csv").exists())


class FakeDb:
    """Replaces the database helpers in attrition_ingest and records what reaches them."""

    def __init__(self):
        self.staged, self.quarantined, self.events = None, None, []

    def __enter__(self):
        self.patches = [
            mock.patch.object(ing, "check_database_objects", lambda cur: self.events.append("check")),
            mock.patch.object(ing, "read_reference_data",
                              lambda cur: (15, ing.DEFAULT_DATE_MIN, ing.DEFAULT_DATE_MAX)),
            mock.patch.object(ing, "read_db_members", lambda cur: None),
            mock.patch.object(ing, "upsert_plans", lambda cur, plans: self.events.append("plans")),
            mock.patch.object(ing, "read_db_plan_ids", lambda cur: set(self._plan_ids)),
            mock.patch.object(ing, "write_quarantine", self._quarantine),
            mock.patch.object(ing, "load_staging", self._stage),
            mock.patch.object(ing, "run_transform", lambda cur, batch: [
                ("DimMember", 1, 0), ("FactEnrollment", 2, 0), ("FactClaims", 3, 0)]),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()

    _plan_ids = ()

    def _quarantine(self, cur, rejects, batch):
        self.quarantined = rejects

    def _stage(self, cur, members, enrollments, claims, batch):
        self.staged = (members, enrollments, claims)


class Conn:
    def commit(self):
        pass


class SamePipelineBothPaths(Base):
    def run_path(self, which):
        with tempfile.TemporaryDirectory() as report, FakeDb() as db:
            db._plan_ids = pd.read_csv(self.dir / "plans.csv")["plan_id"].tolist()
            if which == "csv":
                stats = ing.run_ingest(Conn(), object(), self.dir, "batch-1", log=lambda *_: None)
            else:
                stats = client.run_api_ingest(Conn(), object(), self.session(page_size=300), "batch-1",
                                              Path(report), log=lambda *_: None)
            self.assertTrue((Path(report if which == "api" else self.dir) / "quarantine_report.csv").exists())
            return stats, db

    def test_same_rows_staged_and_quarantined(self):
        s_csv, d_csv = self.run_path("csv")
        s_api, d_api = self.run_path("api")
        for a, b in zip(d_api.staged, d_csv.staged):
            pd.testing.assert_frame_equal(a, b)
        pd.testing.assert_frame_equal(d_api.quarantined, d_csv.quarantined)
        for key in ("rows_read", "rows_quarantined", "rows_staged", "merge", "grace_days"):
            self.assertEqual(s_api[key], s_csv[key], key)
        self.assertEqual(s_api["source"], "api")
        self.assertNotIn("source", s_csv)


class ClientResilience(Base):
    class Flaky:
        def __init__(self, inner, fail_first):
            self.inner, self.left, self.calls = inner, fail_first, 0

        def get(self, url, **kw):
            self.calls += 1
            if self.left > 0:
                self.left -= 1
                raise ConnectionError("connection refused")
            return self.inner.get(url, **kw)

    def api(self, http, **kw):
        return client.ApiSession("http://feed.test", http=http, sleep=lambda s: None, **kw)

    def test_connection_errors_are_retried_then_succeed(self):
        flaky = self.Flaky(FlaskHttp(self.app), fail_first=2)
        body = self.api(flaky).get("/api/v1/ingest/manifest")
        self.assertIn("members", body["sources"])
        self.assertEqual(flaky.calls, 3)

    def test_gives_up_after_the_retry_limit(self):
        flaky = self.Flaky(FlaskHttp(self.app), fail_first=99)
        with self.assertRaisesRegex(client.ApiError, "giving up after 3 attempts"):
            self.api(flaky).get("/health")
        self.assertEqual(flaky.calls, 3)

    def test_503_is_retried_but_404_is_not(self):
        class Status:
            def __init__(self, codes):
                self.codes, self.calls = list(codes), 0

            def get(self, url, **kw):
                self.calls += 1
                code = self.codes.pop(0)
                return type("R", (), {"status_code": code, "text": "x",
                                      "json": lambda s: {"error": "nope"} if code != 200 else {"ok": 1}})()
        h = Status([503, 200])
        self.assertEqual(self.api(h).get("/x"), {"ok": 1})
        h = Status([404, 200])
        with self.assertRaisesRegex(client.ApiError, "404"):
            self.api(h).get("/x")
        self.assertEqual(h.calls, 1)

    def test_truncated_download_is_detected(self):
        real = FlaskHttp(self.app)

        class Truncating:
            def get(self, url, **kw):
                resp = real.get(url, **kw)
                body = resp.json()
                if "/ingest/claims" in url and body.get("records"):
                    body["records"] = body["records"][:-1]          # server drops a row

                return type("R", (), {"status_code": 200, "json": lambda s: body})()
        with self.assertRaisesRegex(client.ApiError, "received .* rows but the API reported"):
            client.fetch_inputs(self.api(Truncating(), page_size=5000), log=lambda *_: None)

    def test_missing_required_source_gives_a_helpful_error(self):
        with tempfile.TemporaryDirectory() as d:
            write_csvs(Path(d))
            (Path(d) / "enrollments.csv").unlink()
            api = client.ApiSession("http://feed.test", http=FlaskHttp(api_mod.create_app(d)), sleep=lambda s: None)
            with self.assertRaisesRegex(client.ApiError, "no 'enrollments' data"):
                client.fetch_inputs(api, log=lambda *_: None)

    def test_plans_are_optional(self):
        with tempfile.TemporaryDirectory() as d:
            write_csvs(Path(d))
            (Path(d) / "plans.csv").unlink()
            api = client.ApiSession("http://feed.test", http=FlaskHttp(api_mod.create_app(d)), sleep=lambda s: None)
            raw, plans = client.fetch_inputs(api, log=lambda *_: None)
            self.assertIsNone(plans)
            self.assertEqual(set(raw), {"members", "enrollments", "claims"})

    def test_source_changing_mid_download_is_rejected(self):
        real = FlaskHttp(self.app)
        state = {"n": 0}

        class Shifting:
            def get(self, url, **kw):
                body = real.get(url, **kw).json()
                state["n"] += 1
                if state["n"] > 1 and "/ingest/members" in url:
                    body["total_rows"] += 1
                return type("R", (), {"status_code": 200, "json": lambda s: body})()
        with self.assertRaisesRegex(client.ApiError, "changed while downloading"):
            client.fetch_source(self.api(Shifting(), page_size=100), "members", ing.MEMBER_COLS, None, lambda *_: None)

    def test_unreachable_server_error_is_clear(self):
        class Down:
            def get(self, url, **kw):
                raise ConnectionError("refused")
        with self.assertRaisesRegex(client.ApiError, "cannot reach"):
            client.fetch_inputs(self.api(Down()), log=lambda *_: None)


# --------------------------------------------------------------------------- pipeline switch
class PipelineSwitch(unittest.TestCase):
    def test_default_source_is_csv(self):
        args = pipe.parse_args([])
        self.assertEqual((args.source, args.api_url), ("csv", client.DEFAULT_API_URL))

    def test_api_flags_parse(self):
        args = pipe.parse_args(["--source", "api", "--api-url", "http://h:9", "--api-key", "k", "--api-page-size", "50"])
        self.assertEqual((args.source, args.api_url, args.api_key, args.api_page_size), ("api", "http://h:9", "k", 50))

    def test_unknown_source_is_rejected(self):
        with self.assertRaises(SystemExit):
            pipe.parse_args(["--source", "ftp"])

    def test_run_uses_the_selected_path(self):
        from test_attrition_pipeline_flow import Db, GOOD_STATS
        calls = []
        with mock.patch.object(pipe.ingest, "run_ingest", lambda *a, **k: calls.append("csv") or dict(GOOD_STATS)), \
             mock.patch.object(pipe.api_client, "run_api_ingest", lambda *a, **k: calls.append("api") or dict(GOOD_STATS)):
            for source in ("csv", "api", "csv"):
                code = pipe.run(pipe.parse_args(["--triggered-by", "t", "--source", source]), connect=Db().connect)
                self.assertEqual(code, pipe.EXIT_OK)
        self.assertEqual(calls, ["csv", "api", "csv"])

    def test_api_failure_is_a_pipeline_error_exit_2(self):
        from test_attrition_pipeline_flow import Db

        def down(*a, **k):
            raise pipe.api_client.ApiError("cannot reach http://127.0.0.1:5000")
        with mock.patch.object(pipe.api_client, "run_api_ingest", down):
            db = Db()
            code = pipe.run(pipe.parse_args(["--triggered-by", "t", "--source", "api"]), connect=db.connect)
        self.assertEqual(code, pipe.EXIT_ERROR)
        self.assertTrue(db.rolled_back)


if __name__ == "__main__":
    unittest.main()
