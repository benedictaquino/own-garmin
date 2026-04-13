# Implementation Plan: Garmin Lakehouse (v1 — Activities)

## Context

The repo is greenfield — only `docs/ADR-garmin-lakehouse.md` and a README stub exist. The ADR proposes a local data lakehouse for personal Garmin data using a Bronze (raw JSON) / Silver (Parquet) medallion pattern, with Polars for transforms and DuckDB for queries. Goals: preserve raw responses immutably so Silver can be rebuilt without re-hitting Garmin, keep storage file-based so a future S3/GCS move is a URI swap, and persist sessions to avoid Cloudflare lockouts.

V1 scope: **activities only**, CLI-driven via Typer, managed with **uv**. Sleep, daily summaries, and wellness time-series come later and reuse the same plumbing.

## Project layout

```text
own-garmin/
├── pyproject.toml              # uv-managed
├── uv.lock
├── .env.example                # GARMIN_EMAIL, GARMIN_PASSWORD
├── .gitignore                  # data/, .env, .session/, .venv/, __pycache__/
├── data/                       # gitignored — bronze/ and silver/ live here
├── src/own_garmin/
│   ├── client                  
│   │   ├── __init__.py
│   │   ├── client.py           # DI Client + Session Persistence
│   │   ├── constants.py        # DI Client IDs and Garmin Endpoints
│   │   ├── strategies.py       # login chain strategy
│   │   └── exceptions.py       # Garmin-specific error hierarchy
│   ├── __init__.py
│   ├── cli.py                  # Typer app: login, ingest, process, query
│   ├── paths.py                # URI builders (local today, s3:// tomorrow)
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── activities.py       # fetch + write raw JSON summaries
│   │   ├── activity_details.py # fetch + write activity detail JSON
│   │   └── fit.py              # fetch + write FIT ZIP archives
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── activities.py       # Polars transform: JSON summaries → Parquet
│   │   └── fit_records.py      # Polars transform: FIT binary → per-second time-series
│   └── query.py                # DuckDB helper over silver parquet
├── tests/
│   ├── fixtures/activities/*.json
│   └── test_silver_activities.py
└── docs/ADR-garmin-lakehouse.md
```

## Dependencies (pyproject.toml)

Runtime:

- `curl-cffi >= 0.7.1` - TLS impersonation
- `requests` - API calls
- `polars` — transforms
- `duckdb` — query engine
- `typer` — CLI
- `python-dotenv` — load `.env`
- `garmin-fit-sdk` — parse raw FIT binary files

Dev:

- `pytest`
- `ruff` (format + lint)

## Module design

### `client.py` — Direct Integration (DI) / Vendored Stealth approach

- Session dir defaults to `~/.config/own-garmin/session/` (override via `OWN_GARMIN_SESSION_DIR`).
- Auth logic:
  - Load existing tokens
  - Check JWT expirty; if <15 minutes remaining, use `refresh_token`
  - If refresh fails or tokens missing, trigger _login_chain from strategies.py
- Expose thin methods used by bronze: `list_activities(start, end)`, `get_activity(activity_id)`, `download_fit(activity_id)`.
- Credentials read from env (`GARMIN_EMAIL`, `GARMIN_PASSWORD`) via `python-dotenv`.

### `paths.py` — URI builders

- `data_root() -> str` reads `OWN_GARMIN_DATA_DIR` (default `./data`). String return so swapping to `s3://bucket/...` later is transparent.
- `bronze_path(category, date) -> str` → `{root}/bronze/{category}/year=YYYY/month=MM/day=DD.json`
- `silver_path(category) -> str` → `{root}/silver/{category}/`
- All downstream code uses these helpers — no hardcoded paths.

### `bronze/activities.py` — activity summary ingestion

- `ingest(client, since: date, until: date)`:
  - Call `client.list_activities(since, until)` → summary list.
  - Group activities by `startTimeLocal` date.
  - Write each day's list to `bronze_path("activities", day)` as pretty JSON.
- **Idempotency:** if the target file exists, load it, merge by `activityId` (new wins), and rewrite only if content changed.

### `bronze/activity_details.py` — activity details ingestion

- `ingest(client, since: date, until: date)`:
  - Call `client.list_activities(since, until)` → activity list.
  - For each `activity_id`, call `client.get_activity_details(activity_id)` to fetch splits, laps, metrics.
  - Group by `startTimeLocal` date (from activity summary).
  - Write each day's details to `bronze_path("activity_details", day)` as pretty JSON.
- **Idempotency:** skip if file already exists; optional `sleep_sec` between requests (default ~0.5s).

### `bronze/fit.py` — FIT file ingestion

- `ingest(client, since: date, until: date)`:
  - Call `client.list_activities(since, until)` → activity list.
  - For each `activity_id`, call `client.download_fit(activity_id)` → ZIP bytes.
  - Write ZIP to `bronze_path("fit", day)/{activity_id}.zip`.
- **Idempotency:** skip if file already exists; optional `sleep_sec` between downloads (default ~0.5s).

### `silver/activities.py` — summary transform (pure function)

- `transform(bronze_json_paths: list[str]) -> pl.DataFrame`:
  - `pl.read_json` each file, concat.
  - Stable schema:
    - `activity_id: Int64`
    - `activity_type: Utf8` (from `activityType.typeKey`)
    - `start_time_local: Datetime` (from `startTimeLocal`)
    - `start_time_utc: Datetime` (from `startTimeGMT`)
    - `duration_sec: Float64`
    - `distance_m: Float64`
    - `avg_hr: Float64`, `max_hr: Float64`
    - `calories: Float64`
    - `elevation_gain_m: Float64`, `elevation_loss_m: Float64`
    - `start_lat: Float64`, `start_lon: Float64` — semicircles → degrees: `deg = semi * (180 / 2**31)`
    - `year`, `month` partition columns from `start_time_local`.
  - Deduplicate by `activity_id` keeping the latest ingested copy.
- `rebuild(bronze_root, silver_root)`:
  - Glob all `bronze/activities/**/*.json`, transform, write partitioned parquet to `silver/activities/year=YYYY/month=MM/`.

### `silver/fit_records.py` — FIT time-series transform (pure function)

- `transform(fit_zip_paths: list[str]) -> pl.DataFrame`:
  - For each ZIP file: extract the `.fit` binary using `zipfile.ZipFile`, parse with `garmin-fit-sdk`.
  - Extract FIT "record" messages (message type 20) containing per-second timestamp + telemetry.
  - Stable schema:
    - `activity_id: Int64` (from ZIP filename stem)
    - `timestamp: Datetime` (UTC, from FIT record `timestamp` field)
    - `heart_rate: Int32 (nullable)` — bpm
    - `cadence: Int32 (nullable)` — rpm
    - `speed: Float64 (nullable)` — m/s
    - `power: Int32 (nullable)` — watts
    - `distance: Float64 (nullable)` — metres cumulative
    - `altitude: Float64 (nullable)` — metres
    - `position_lat: Float64 (nullable)` — degrees, converted from semicircles: `deg = semi * (180 / 2**31)`
    - `position_lon: Float64 (nullable)` — degrees, converted from semicircles
    - `year`, `month` partition columns from `timestamp`.
  - Deduplicate by `(activity_id, timestamp)` keeping the latest row.
- `rebuild(bronze_root, silver_root)`:
  - Glob all `bronze/fit/**/*.zip`, transform, write partitioned parquet to `silver/fit_records/year=YYYY/month=MM/`.

### `query.py` — DuckDB helper

- `query(sql: str) -> pl.DataFrame`:
  - Open in-memory DuckDB.
  - `CREATE VIEW activities AS SELECT * FROM read_parquet('{silver_path("activities")}/**/*.parquet', hive_partitioning=1)`.
  - Run SQL, return as Polars DataFrame.

### `cli.py` — Typer commands

- `own-garmin login` — force fresh login, persist tokens. Useful for first-run and re-auth.
- `own-garmin ingest --since YYYY-MM-DD [--until YYYY-MM-DD]` — default `until` = today. Writes bronze.
- `own-garmin process` — rebuilds silver from bronze.
- `own-garmin query "SELECT ..."` — prints result table.

## Tests

- `tests/fixtures/activities/` holds 2–3 canned Garmin JSON day-files covering: one run, one ride, one activity with no GPS (no `startLatitude`), one duplicate across files to exercise dedup.
- `test_silver_activities.py`:
  - Feeds fixtures through `silver.activities.transform`.
  - Asserts schema, row count after dedup, semicircle→degree conversion for a known activity, null GPS handling.
- No network/auth tests — client is exercised manually via CLI.

## Config / secrets

- `.env.example` committed with placeholder keys; `.env` gitignored.
- Env vars:
  - `GARMIN_EMAIL`, `GARMIN_PASSWORD` — credentials.
  - `OWN_GARMIN_DATA_DIR` (default `./data`).
  - `OWN_GARMIN_SESSION_DIR` (default `~/.config/own-garmin/session`).

## Critical files to create

- `pyproject.toml`
- `.env.example`, `.gitignore`
- `src/own_garmin/{__init__,cli,paths,query}.py`
- `src/own_garmin/client/{__init__,client,strategies,constants,exceptions}.py`
- `src/own_garmin/bronze/{__init__,activities,activity_details,fit}.py`
- `src/own_garmin/silver/{__init__,activities,fit_records}.py`
- `tests/fixtures/activities/*.json`
- `tests/test_silver_activities.py`

## Verification

End-to-end smoke test after implementation:

1. `uv sync` — resolves + installs deps.
2. `cp .env.example .env` and fill credentials.
3. `uv run own-garmin login` → token files appear under `~/.config/own-garmin/session/`.
4. `uv run own-garmin ingest --since 2026-01-01` → JSON files under `data/bronze/activities/year=2026/month=*/day=*.json` with expected activity IDs.
5. `uv run own-garmin process` → parquet files under `data/silver/activities/year=*/month=*/`.
6. `uv run own-garmin query "SELECT activity_type, COUNT(*) FROM activities GROUP BY 1 ORDER BY 2 DESC"` → returns counts.
7. `uv run pytest` — silver transform tests pass.
8. Re-run `ingest` with the same window → no duplicate entries in bronze files (idempotency).

## Out of scope for v1

- Sleep / daily summary / wellness time-series ingestion (add a new `bronze/<category>.py` + `silver/<category>.py` pair each).
- S3/GCS backend (swap via `OWN_GARMIN_DATA_DIR=s3://...`; may need `s3fs` + DuckDB `httpfs` extension).
- Scheduled runs (cron/systemd/launchd) — CLI is scheduler-agnostic.
- Gold layer / BI dashboards.

## Task breakdown

Implementation tasks live in `plans/tasks/` and are designed to be picked up sequentially. Each task is self-contained enough to be executed in its own session.

1. `tasks/01-project-scaffold.md` — uv init, pyproject, gitignore, env template, directory layout
2. `tasks/02-paths-config.md` — `paths.py` URI helpers + env loading
3. `tasks/03-client-session.md` — Garmin client with token persistence
4. `tasks/04-bronze-activities.md` — activity ingestion to bronze JSON
5. `tasks/05-silver-activities.md` — Polars transform + partitioned parquet
6. `tasks/06-query-duckdb.md` — DuckDB query helper
7. `tasks/07-cli.md` — Typer CLI wiring the pieces together
8. `tasks/08-tests-silver.md` — fixtures + pytest for silver transform
9. `tasks/09-e2e-verification.md` — manual smoke test against real account
