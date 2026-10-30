# API ingestion path

The pipeline has two ways to read source data. Both feed the same validation, quarantine,
staging and MERGE code (`attrition_ingest.ingest_raw`), so results are identical.

| Path | Reads from | Entry points |
| --- | --- | --- |
| CSV (unchanged) | `members/enrollments/claims/plans.csv` in `--input-dir` | `attrition_ingest.py`, `attrition_pipeline.py` |
| API (new) | the Flask feed in `attrition_api.py`, which serves those same CSVs as paged JSON | `attrition_api_client.py`, `attrition_pipeline.py --source api` |

## Setup
    pip install -r requirements_attrition.txt
    sqlcmd -S localhost -E -I -b -i deploy_attrition.sql     # once, and after any .sql change

## Run the API path
    # terminal 1: generate data (once) and start the feed
    python generate_attrition_data.py --members 5000 --out-dir output
    python attrition_api.py --data-dir output --port 5000

    # terminal 2: whole pipeline, reading through the API
    python attrition_pipeline.py --source api --api-url http://127.0.0.1:5000

    # or just the ingest step
    python attrition_api_client.py --dry-run                  # fetch + validate, no database
    python attrition_api_client.py                            # fetch + load into SQL Server

## Run the CSV path (unchanged)
    python attrition_pipeline.py --generate --members 5000
    python attrition_ingest.py --input-dir output

## Feed endpoints (GET only)
- `/health` liveness and which CSVs exist (no key needed)
- `/api/v1/ingest/manifest` row counts and columns per source
- `/api/v1/ingest/<source>?page=1&page_size=1000` with source = members, enrollments, claims or plans
  (page_size maximum 5000). Values are raw CSV text; blanks stay `""`.

## Security
Set `ATTRITION_API_KEY` (or `--api-key`) on the server and give the same key to the client to require
an `X-API-Key` header on `/api/v1/*`. The server binds to 127.0.0.1 by default. The built-in Flask server
is for development; use a production server such as waitress if the feed is reachable beyond your machine.

## Environment variables
`ATTRITION_API_URL` (client default `http://127.0.0.1:5000`), `ATTRITION_API_KEY`.

## Tests
    python -m unittest test_attrition_api -v
