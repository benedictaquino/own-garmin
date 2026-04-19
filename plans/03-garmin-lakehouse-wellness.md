# Implementation Plan: Garmin Lakehouse (v1.2 — Wellness & Health Metrics)

## Context

V1.0 shipped the local medallion lakehouse (Bronze/Silver/DuckDB) for Garmin activity data. V1.1
shipped cloud-readiness: S3-backed storage via `storage.py`, headless auth via `ntfy.sh`, and a
containerised CLI image. V1.2 expands the ingested data surface to 24/7 wellness and health metrics
per [ADR-003](../docs/adrs/ADR-003-expanding-scope.md), transforming `own-garmin` from an
activity-only pipeline into a true personal data lakehouse.

**Current status:** ADR-003 proposed; v1.1 fully shipped.

## Objectives

* Extend `GarminClient` with read-only GETs for 10 wellness endpoints.
* Add one bronze extractor module per category, writing one raw-JSON-per-day file per category.
* Add one silver transform module per category, each exposing `transform()` + `rebuild()` and
  writing typed, hive-partitioned Parquet.
* Separate **daily summary** tables (one row per date) from **high-resolution time-series** tables
  (many rows per date) so ad-hoc DuckDB queries scan only what they need.
* Support multi-year historical backfill with jittered pacing, per-category cursor persistence, and
  typed exception-based retry logic.

## Endpoint Map

**Endpoints verified against community references — may change if Garmin updates its internal API.
Tasks should confirm current shape by hitting the endpoint once manually before coding the parser.**

| Category | Endpoint (path under `connectapi.garmin.com`) | Notes |
|---|---|---|
| `sleep` | `/wellness-service/wellness/dailySleepData/{displayName}?date={YYYY-MM-DD}` | Returns sleep stages + summary |
| `body_battery` | `/wellness-service/wellness/bodyBattery/reports/daily?startDate={}&endDate={}` | Range query |
| `stress` | `/wellness-service/wellness/dailyStress/{date}` | Daily samples + summary |
| `hrv` | `/hrv-service/hrv/{date}` | Overnight HRV |
| `heart_rate` | `/wellness-service/wellness/dailyHeartRate?date={date}` | All-day HR samples + resting |
| `respiration` | `/wellness-service/wellness/daily/respiration/{date}` | All-day breath rate samples |
| `spo2` | `/wellness-service/wellness/daily/spo2/{date}` | All-day SpO2 samples |
| `intensity_minutes` | `/wellness-service/wellness/daily/im/{displayName}?start={}&end={}` | Moderate/vigorous minutes |
| `weight` | `/weight-service/weight/daterangesnapshot?startDate={}&endDate={}` | Weight + body composition |
| `steps` | `/usersummary-service/stats/steps/daily/{start}/{end}` | Daily step totals |

## Module Design

### `src/own_garmin/client/constants.py`

Add one URL template constant per row in the Endpoint Map (10 new constants). Naming mirrors the
existing pattern: `SLEEP_URL`, `BODY_BATTERY_URL`, `STRESS_URL`, `HRV_URL`, `HEART_RATE_URL`,
`RESPIRATION_URL`, `SPO2_URL`, `INTENSITY_MINUTES_URL`, `WEIGHT_URL`, `STEPS_URL`. The three
endpoints that use `displayName` are noted in comments; implementors pull `self.display_name` from
the client instance (already fetched on login via `SOCIAL_PROFILE_URL`).

### `src/own_garmin/client/client.py`

Add 10 new public methods to `GarminClient`, one per wellness category. Each method mirrors
`get_activity_details`: a single `self._connectapi(path, params=...)` call that returns the parsed
JSON (`dict` or `list[dict]` depending on the endpoint). Methods that accept a date range take
`start: date, end: date`; methods that fetch a single day take `day: date`. Where the endpoint
requires `displayName`, the method substitutes `self.display_name` automatically so callers pass
only dates. Each method raises the existing typed exceptions (`GarminConnectionError`,
`GarminAuthError`) on 4xx/5xx.

### `src/own_garmin/bronze/<category>.py`

One module per category (10 total). All share the shape of `bronze/activity_details.py`:

* `ingest(client: GarminClient, since: date, until: date, sleep_sec: float = 0.5) -> int`
* Iterates day by day from `since` through `until`.
* Writes one JSON file per day to `bronze_path("<category>", day)` (pretty-printed, `indent=2`).
* Idempotent merge on a stable key per category (documented in each task file).
* Returns count of day-files written or updated.

### `src/own_garmin/silver/<category>.py`

One module per category (10 total). Each matches `silver/activities.py`:

* `transform(bronze_json_paths: list[str]) -> pl.DataFrame` — pure function; reads bronze files via
  `storage.read_bytes`, flattens JSON to a typed schema, deduplicates.
* `rebuild() -> int` — globs bronze, calls `transform`, writes partitioned Parquet via
  `storage.write_partitioned_parquet`, returns row count.
* No side effects in `transform`; all filesystem writes are in `rebuild`.

### `src/own_garmin/query.py`

Extend `_SILVER_TABLES` to include the 15 new view names (10 daily + 5 time-series). The existing
`query()` function registers each table automatically.

### `src/own_garmin/cli.py`

Extend the `ingest` command:

* `--categories` — comma-separated list of wellness categories to ingest (default: all 10). The
  existing `--since` / `--until` flags drive the date range.
* `--chunk-days` — number of days per backfill chunk (default: 30). Combined with the existing
  `--sleep-sec` flag for per-request pacing.

Add a new `process` sub-option to rebuild specific silver categories instead of everything.

## Layer Separation

Two classes of silver table:

1. **Daily summaries** (one row per date per category):
   `sleep_summary`, `body_battery_daily`, `stress_daily`, `hrv_daily`, `heart_rate_daily`,
   `intensity_minutes_daily`, `spo2_daily`, `respiration_daily`, `weight_daily`, `steps_daily`.

2. **High-resolution samples** (many rows per date):
   `heart_rate_samples`, `stress_samples`, `spo2_samples`, `respiration_samples`, `sleep_stages`.

Both classes use hive partitioning on `year` / `month` so DuckDB can prune partitions in range
queries.

## Backfill & Pacing

* A generic date-chunking iterator in `src/own_garmin/bronze/_backfill.py` yields date windows of
  `--chunk-days` days, sleeps `sleep_sec` (jittered ± 20 %) between requests, and persists a
  cursor at `{OWN_GARMIN_STATE_DIR}/backfill-{category}.cursor` after each successfully written day.
* Resume = read cursor file, skip days already processed.
* 429 and connection errors bubble up as typed exceptions; the ingest loop catches them, sleeps a
  jittered back-off of 60–120 s, and retries up to 3 times before logging the failure, skipping
  the day, and continuing.

## Tests

* Fixture-driven unit tests for each silver transform. Fixtures live at
  `tests/fixtures/wellness/<category>/` as named JSON files (e.g., `normal.json`, `missing.json`,
  `duplicate.json`), each under 3 KB.
* Three fixture scenarios per category:
  1. Normal day with all expected fields present.
  2. A day where Garmin omits the nested sub-object (common for days with no data).
  3. A duplicate record across two files to exercise dedup logic.
* No network tests. No live-client tests.

## Config / Secrets

All existing env vars unchanged. One addition:

| Var | Default | Purpose |
|---|---|---|
| `OWN_GARMIN_STATE_DIR` | `~/.config/own-garmin/state` | Stores per-category backfill cursor files |

`paths.py` gets two new helpers: `state_dir() -> str` and `backfill_cursor_path(category: str) -> str`.

## Critical Files to Create or Modify

**New bronze modules:**

* `src/own_garmin/bronze/sleep.py`
* `src/own_garmin/bronze/body_battery.py`
* `src/own_garmin/bronze/stress.py`
* `src/own_garmin/bronze/hrv.py`
* `src/own_garmin/bronze/heart_rate.py`
* `src/own_garmin/bronze/respiration.py`
* `src/own_garmin/bronze/spo2.py`
* `src/own_garmin/bronze/intensity_minutes.py`
* `src/own_garmin/bronze/weight.py`
* `src/own_garmin/bronze/steps.py`
* `src/own_garmin/bronze/_backfill.py`

**New silver modules:**

* `src/own_garmin/silver/sleep.py`
* `src/own_garmin/silver/body_battery.py`
* `src/own_garmin/silver/stress.py`
* `src/own_garmin/silver/hrv.py`
* `src/own_garmin/silver/heart_rate.py`
* `src/own_garmin/silver/respiration.py`
* `src/own_garmin/silver/spo2.py`
* `src/own_garmin/silver/intensity_minutes.py`
* `src/own_garmin/silver/weight.py`
* `src/own_garmin/silver/steps.py`

**Modified modules:**

* `src/own_garmin/client/constants.py` — 10 new URL template constants
* `src/own_garmin/client/client.py` — 10 new public methods
* `src/own_garmin/paths.py` — `state_dir()`, `backfill_cursor_path()`
* `src/own_garmin/query.py` — extend `_SILVER_TABLES`
* `src/own_garmin/cli.py` — `--categories`, `--chunk-days`

**New test fixtures:**

* `tests/fixtures/wellness/<category>/{normal,missing,duplicate}.json` (30 files across 10 categories)

**New test files:**

* `tests/test_silver_wellness_daily.py`
* `tests/test_silver_wellness_timeseries.py`

## Verification (Manual)

End-to-end smoke test after all tasks complete:

1. `uv run own-garmin ingest --categories sleep --since 2026-04-01 --until 2026-04-07` → 7 JSON
   files under `data/bronze/sleep/year=2026/month=04/`.
2. `uv run own-garmin process` → Parquet files under `data/silver/sleep_summary/year=2026/month=04/`
   and `data/silver/sleep_stages/year=2026/month=04/`.
3. `uv run own-garmin query "SELECT * FROM sleep_summary ORDER BY date"` → 7 rows, typed schema.
4. Re-run step 1 with the same window → no files rewritten (idempotency check).
5. Repeat for `--categories heart_rate` and assert both `heart_rate_daily` and `heart_rate_samples`
   tables populate.
6. Run `uv run pytest` → all silver transform tests pass.

## Out of Scope for v1.2

* Gold-layer / aggregated tables or cross-metric correlations.
* Live incremental push ingestion (Garmin has no webhook API).
* Correlation dashboards (Gold-layer task, future plan).
* Mobile device battery or GPS track wellness endpoints (rarely useful, deferred).

## Task Breakdown

1. [Task 17](tasks/17-client-wellness-endpoints.md) — Client: wellness endpoint methods
2. [Task 18](tasks/18-bronze-wellness-recovery.md) — Bronze: sleep, body battery, stress, HRV
3. [Task 19](tasks/19-bronze-daily-physiology.md) — Bronze: HR, respiration, SpO2, intensity minutes
4. [Task 20](tasks/20-bronze-composition-activity.md) — Bronze: weight & body composition, steps & floors
5. [Task 21](tasks/21-silver-wellness-daily.md) — Silver: daily-summary tables
6. [Task 22](tasks/22-silver-wellness-timeseries.md) — Silver: high-resolution sample tables
7. [Task 23](tasks/23-backfill-pacing.md) — Incremental backfill with jittered pacing
8. [Task 24](tasks/24-cli-query-tests-verification.md) — CLI integration, query wiring, tests, and e2e verification
