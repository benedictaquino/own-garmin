# Task 22: Silver: high-resolution sample tables

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Extend the five silver modules created in Task 21 to also produce high-resolution time-series
tables. Each module gains a second transform function alongside the daily-summary transform,
sharing the same bronze source files. No new files are created beyond those from Task 21.

## Files

* `src/own_garmin/silver/heart_rate.py` — **modify**: add `transform_samples` + `rebuild_samples`
* `src/own_garmin/silver/stress.py` — **modify**: add `transform_samples` + `rebuild_samples`
* `src/own_garmin/silver/spo2.py` — **modify**: add `transform_samples` + `rebuild_samples`
* `src/own_garmin/silver/respiration.py` — **modify**: add `transform_samples` + `rebuild_samples`
* `src/own_garmin/silver/sleep.py` — **modify**: add `transform_stages` + `rebuild_stages`

Task 21's `transform` and `rebuild` functions are renamed to `transform_daily` and
`rebuild_daily` for symmetry. A module-level alias `transform = transform_daily` and
`rebuild = rebuild_daily` is kept so callers written for Task 21 do not break.

## Public API

```python
import polars as pl

# heart_rate.py, stress.py, spo2.py, respiration.py
def transform_daily(bronze_json_paths: list[str]) -> pl.DataFrame: ...
def rebuild_daily() -> int: ...
def transform_samples(bronze_json_paths: list[str]) -> pl.DataFrame: ...
def rebuild_samples() -> int: ...

# Backward-compat aliases
transform = transform_daily
rebuild = rebuild_daily

# sleep.py
def transform_daily(bronze_json_paths: list[str]) -> pl.DataFrame: ...
def rebuild_daily() -> int: ...
def transform_stages(bronze_json_paths: list[str]) -> pl.DataFrame: ...
def rebuild_stages() -> int: ...

transform = transform_daily
rebuild = rebuild_daily
```

## Target Schemas

All sample tables include `year: Int32` and `month: Int32` partition columns derived from
`date`. Timestamps are UTC `pl.Datetime`. Dedup as noted per table.

**`heart_rate_samples`** (source: `bronze/heart_rate/`):

| Column | Type |
|---|---|
| `date` | Date |
| `timestamp` | Datetime (UTC) |
| `heart_rate` | Int32 |
| `year` | Int32 |
| `month` | Int32 |

Dedup on `(date, timestamp)`, keep last. Source: `heartRateValues` array of `[epoch_ms, bpm]`
pairs inside each bronze dict.

**`stress_samples`** (source: `bronze/stress/`):

| Column | Type |
|---|---|
| `date` | Date |
| `timestamp` | Datetime (UTC) |
| `stress_level` | Int32 |
| `year` | Int32 |
| `month` | Int32 |

Dedup on `(date, timestamp)`, keep last. Source: intraday stress values array in the stress
response (typically `stressValuesArray` of `[epoch_ms, level]` pairs).

**`spo2_samples`** (source: `bronze/spo2/`):

| Column | Type |
|---|---|
| `date` | Date |
| `timestamp` | Datetime (UTC) |
| `spo2_reading` | Float64 |
| `year` | Int32 |
| `month` | Int32 |

Dedup on `(date, timestamp)`, keep last.

**`respiration_samples`** (source: `bronze/respiration/`):

| Column | Type |
|---|---|
| `date` | Date |
| `timestamp` | Datetime (UTC) |
| `breath_rate` | Float64 |
| `year` | Int32 |
| `month` | Int32 |

Dedup on `(date, timestamp)`, keep last.

**`sleep_stages`** (source: `bronze/sleep/`):

| Column | Type |
|---|---|
| `date` | Date |
| `start_time` | Datetime (UTC) |
| `end_time` | Datetime (UTC) |
| `stage` | Utf8 |
| `year` | Int32 |
| `month` | Int32 |

`stage` values are normalized to lowercase members of `{"deep", "light", "rem", "awake"}`.
Dedup on `(date, start_time, stage)`, keep last. Source: `sleepLevels` array inside
`dailySleepDTO`, where each element has `startGMT`, `endGMT`, and `activityLevel` (or
equivalent field — confirm against live API before coding).

## Behavior

1. `transform_samples` / `transform_stages` reads the same bronze files as `transform_daily`.
2. For each record, locate the intraday array field; explode each `[epoch_ms, value]` pair
   (or object element for sleep stages) into individual rows.
3. Convert epoch-ms integers to `pl.Datetime(time_unit="ms", time_zone="UTC")`.
4. Apply dedup and partition columns as specified.
5. Return empty frame with correct schema if no intraday data is present (e.g., device did
   not record all-day HR).
6. `rebuild_samples` / `rebuild_stages`:
   1. Glob bronze, call `transform_samples` / `transform_stages`.
   2. Rmtree the sample silver target; write partitioned Parquet.
   3. Return row count.

## Acceptance Criteria

* [ ] All five modules expose both `_daily` and `_samples` / `_stages` function pairs.
* [ ] Backward-compat aliases `transform` and `rebuild` point to the daily variants.
* [ ] Schema matches the tables above exactly.
* [ ] Epoch-ms timestamps are converted to UTC Datetime (not left as integers).
* [ ] `stage` values in `sleep_stages` are normalized to lowercase.
* [ ] Empty intraday array → empty DataFrame with correct schema, no exception.
* [ ] Dedup: duplicate `(date, timestamp)` pairs produce one row; last wins.
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes.

## Notes

* The exact JSON key for intraday arrays must be confirmed against a live API response before
  coding. Garmin's internal API shape changes without notice. Common keys: `heartRateValues`,
  `stressValuesArray`, `spO2HourlyAverages`, `respirationValuesArray`.
* `sleep_stages` source field is within `dailySleepDTO.sleepLevels`; each element typically
  has `activityLevel` (a string like `"deep"`, `"light"`, `"rem"`, `"awake"`), `startGMT`, and
  `endGMT`. Normalize to lowercase and drop any levels not in the expected set (log at WARNING).
* All file I/O goes through `storage.*`.
* Requires Task 21 to be complete (these functions extend, not replace, that work).
