# Task 24: CLI integration, query wiring, tests, and e2e verification

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Wire all wellness bronze extractors into the `ingest` CLI command, register all 15 new silver
views in `query.py`, add fixture-driven unit tests for all 15 silver transforms, and verify the
full pipeline end-to-end. This task is the gate: no v1.2 work is "done" until this task passes.

## Files

* `src/own_garmin/cli.py` — **modify**
* `src/own_garmin/query.py` — **modify**
* `tests/fixtures/wellness/<category>/{normal,missing,duplicate}.json` — **new** (30 files total)
* `tests/test_silver_wellness_daily.py` — **new**
* `tests/test_silver_wellness_timeseries.py` — **new**

## Public API

### CLI changes (`cli.py`)

```python
@app.command()
def ingest(
    since: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    until: Optional[str] = typer.Option(None, help="End date; defaults to today"),
    sleep_sec: float = typer.Option(0.5, help="Sleep between requests"),
    categories: str = typer.Option(
        "activities,activity_details,fit",
        help="Comma-separated categories to ingest",
    ),
    chunk_days: int = typer.Option(30, help="Days per backfill chunk"),
) -> None: ...

@app.command()
def process(
    categories: Optional[str] = typer.Option(
        None, help="Comma-separated silver categories to rebuild; default rebuilds all"
    ),
) -> None: ...
```

### `query.py` changes

```python
_SILVER_TABLES = (
    # Existing
    "activities",
    "fit_records",
    # Daily summaries
    "sleep_summary",
    "body_battery_daily",
    "stress_daily",
    "hrv_daily",
    "heart_rate_daily",
    "respiration_daily",
    "spo2_daily",
    "intensity_minutes_daily",
    "weight_daily",
    "steps_daily",
    # High-resolution samples
    "heart_rate_samples",
    "stress_samples",
    "spo2_samples",
    "respiration_samples",
    "sleep_stages",
)
```

## Behavior

### `ingest` command

1. Parse `--categories` as a comma-separated list; strip whitespace from each token.
2. Dispatch each category to its handler via a `_CATEGORY_HANDLERS` registry dict:

   ```python
   _CATEGORY_HANDLERS = {
       "activities": ...,
       "activity_details": ...,
       "fit": ...,
       "sleep": bronze.sleep.ingest,
       "body_battery": bronze.body_battery.ingest,
       "stress": bronze.stress.ingest,
       "hrv": bronze.hrv.ingest,
       "heart_rate": bronze.heart_rate.ingest,
       "respiration": bronze.respiration.ingest,
       "spo2": bronze.spo2.ingest,
       "intensity_minutes": bronze.intensity_minutes.ingest,
       "weight": bronze.weight.ingest,
       "steps": bronze.steps.ingest,
   }
   ```

3. Wellness categories (`sleep` through `steps`) receive the full date window chunked via
   `_backfill.iter_days(since_date, until_date, chunk_days)`. Each chunk calls the handler
   with `(client, chunk_start, chunk_end, sleep_sec=sleep_sec)`.
4. Activity categories (`activities`, `activity_details`, `fit`) retain existing behavior
   (no chunking).
5. Unknown category tokens print a warning to stderr and are skipped.
6. Default `--categories` value preserves backward compatibility: `activities,activity_details,fit`.

### `process` command

1. If `--categories` is omitted, rebuild all known silver tables (existing + all 10 wellness
   daily + all 5 sample tables).
2. If `--categories` is provided, rebuild only the specified tables.
3. Dispatch via a `_SILVER_HANDLERS` registry dict mapping table name to its `rebuild`
   function. Unknown names print a warning and are skipped.

### Test fixtures (`tests/fixtures/wellness/<category>/`)

Thirty small JSON files — three per category (`normal.json`, `missing.json`, `duplicate.json`),
each under 3 KB. Each file is a JSON array (consistent with bronze day-file format).

* `normal.json` — one record with all expected fields present.
* `missing.json` — one record where Garmin omits the nested sub-object (common for days with no
  data — e.g., no sleep data, no HRV reading).
* `duplicate.json` — two records with the same `calendarDate` to exercise dedup logic; the
  second record should have a field value that differs from the first so the keep-last behavior
  is observable.

### `tests/test_silver_wellness_daily.py`

One test function per daily table (10 total). Each test:

1. Loads `normal.json` and `duplicate.json` for that category.
2. Calls `transform([normal_path, duplicate_path])`.
3. Asserts schema matches exactly (column names and dtypes).
4. Asserts dedup: only one row per `date`; the duplicate's distinguishing field value is the
   one from the later record (keep last).
5. Calls `transform([missing_path])` and asserts it returns a DataFrame (not raises), with
   schema intact.

### `tests/test_silver_wellness_timeseries.py`

One test function per sample table (5 total). Same structure as daily tests, plus:

* Assert `timestamp` column dtype is `pl.Datetime` with `time_zone="UTC"`.
* Assert dedup on `(date, timestamp)` (or `(date, start_time, stage)` for `sleep_stages`).
* Assert empty intraday array in `missing.json` yields empty DataFrame with correct schema.

## Acceptance Criteria

* [ ] `uv run pytest` green (all existing + 15 new tests pass).
* [ ] `uv run ruff check .` clean.
* [ ] All 30 fixture files exist and are valid JSON arrays under 3 KB each.
* [ ] `uv run own-garmin ingest --categories sleep --since 2026-04-01 --until 2026-04-07`
      produces 7 bronze files under `data/bronze/sleep/year=2026/month=04/`.
* [ ] `uv run own-garmin process` produces Parquet under
      `data/silver/sleep_summary/year=2026/month=04/` and
      `data/silver/sleep_stages/year=2026/month=04/`.
* [ ] `uv run own-garmin query "SELECT * FROM sleep_summary ORDER BY date"` returns 7 rows
      with correct schema.
* [ ] Re-running `ingest` over the same window does not rewrite unchanged files (idempotency).
* [ ] `uv run own-garmin ingest --categories heart_rate --since 2026-04-01 --until 2026-04-07`
      followed by `process` populates both `heart_rate_daily` and `heart_rate_samples`.
* [ ] Default `ingest` (no `--categories`) still ingests activities, activity_details, and fit
      without error.

## Notes

* The 30 fixture files must not contain real personal health data. Use plausible-looking
  synthetic values (e.g., heart rate 55–160, SpO2 95–100, steps 3000–12000).
* Fixture paths resolve via `Path(__file__).parent / "fixtures" / "wellness" / <category> / ...`
  so tests are location-independent.
* No network calls in tests. Transform functions are pure and read only from the fixture files
  passed explicitly.
* Requires Tasks 17–23 to be complete.
