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
│   ├── __init__.py
│   ├── cli.py                  # Typer app: login, ingest, process, query
│   ├── client.py               # GarminClient wrapper + session persistence
│   ├── paths.py                # URI builders (local today, s3:// tomorrow)
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── activities.py       # fetch + write raw JSON
│   ├── silver/
│   │   ├── __init__.py
│   │   └── activities.py       # Polars transform (pure function)
│   └── query.py                # DuckDB helper over silver parquet
├── tests/
│   ├── fixtures/activities/*.json
│   └── test_silver_activities.py
└── docs/ADR-garmin-lakehouse.md
```

## Dependencies (pyproject.toml)

Runtime:

- `garminconnect >= 0.3.0` — mobile SSO flow, garth-based token store
- `polars` — transforms
- `duckdb` — query engine
- `typer` — CLI
- `python-dotenv` — load `.env`

Dev:

- `pytest`
- `ruff` (format + lint)

## Module design

### `client.py` — session-aware wrapper

- Wraps `garminconnect.Garmin`.
- Session dir defaults to `~/.config/own-garmin/session/` (override via `OWN_GARMIN_SESSION_DIR`).
- On every call:
  1. Try `Garmin().login(tokenstore=session_dir)` to resume.
  2. On `GarminConnectAuthenticationError` / expiry, fall back to email+password login and immediately persist new tokens via `client.garth.dump(session_dir)`.
- Expose thin methods used by bronze: `list_activities(start, end)`, `get_activity(activity_id)`.
- Credentials read from env (`GARMIN_EMAIL`, `GARMIN_PASSWORD`) via `python-dotenv`.

### `paths.py` — URI builders

- `data_root() -> str` reads `OWN_GARMIN_DATA_DIR` (default `./data`). String return so swapping to `s3://bucket/...` later is transparent.
- `bronze_path(category, date) -> str` → `{root}/bronze/{category}/year=YYYY/month=MM/day=DD.json`
- `silver_path(category) -> str` → `{root}/silver/{category}/`
- All downstream code uses these helpers — no hardcoded paths.

### `bronze/activities.py` — ingestion

- `ingest(client, since: date, until: date)`:
  - Call `client.list_activities(since, until)` → summary list.
  - For each summary, call `client.get_activity(activity_id)` for full detail.
  - Group activities by `startTimeLocal` date.
  - Write each day's list to `bronze_path("activities", day)` as pretty JSON.
- **Idempotency:** if the target file exists, load it, merge by `activityId` (new wins — ADR note: retroactive updates require full-file replace), and rewrite only if content changed.
- Small `time.sleep` (~0.5s) between detail calls; configurable.

### `silver/activities.py` — transform (pure function)

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
  - Glob all `bronze/activities/**/*.json`, transform, write partitioned parquet to `silver/activities/year=YYYY/month=MM/` via `df.write_parquet(..., partition_by=["year","month"])`.
- Pure function over file paths so silver is fully rebuildable from bronze (core ADR guarantee).

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
- `src/own_garmin/{__init__,cli,client,paths,query}.py`
- `src/own_garmin/bronze/{__init__,activities}.py`
- `src/own_garmin/silver/{__init__,activities}.py`
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
3. `tasks/03-client-session.md` — Garmin client wrapper with token persistence
4. `tasks/04-bronze-activities.md` — activity ingestion to bronze JSON
5. `tasks/05-silver-activities.md` — Polars transform + partitioned parquet
6. `tasks/06-query-duckdb.md` — DuckDB query helper
7. `tasks/07-cli.md` — Typer CLI wiring the pieces together
8. `tasks/08-tests-silver.md` — fixtures + pytest for silver transform
9. `tasks/09-e2e-verification.md` — manual smoke test against real account
