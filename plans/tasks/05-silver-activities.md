# Task 05: Silver transforms — activities and FIT time-series

## Goal

Transform two bronze categories into clean, typed, partitioned Parquet datasets:

1. **activities** — activity summaries from JSON (already scoped)
2. **fit_records** — per-second time-series from FIT ZIP archives

Both are pure functions — no I/O except reading bronze and writing silver.

## Files

- `src/own_garmin/silver/activities.py` — summary transform
- `src/own_garmin/silver/fit_records.py` — FIT time-series transform

## Public API

### `activities.py`

```python
import polars as pl

def transform(bronze_json_paths: list[str]) -> pl.DataFrame:
    """Transform activity summaries from bronze JSON. Returns typed DataFrame."""

def rebuild() -> int:
    """Rebuild activities silver from all bronze/activities/**/*.json. Returns row count."""
```

### `fit_records.py`

```python
import polars as pl

def transform(fit_zip_paths: list[str]) -> pl.DataFrame:
    """Parse FIT ZIPs into per-second time-series records. Returns typed DataFrame."""

def rebuild() -> int:
    """Rebuild fit_records silver from all bronze/fit/**/*.zip. Returns row count."""
```

## Target schema — `activities`

| Column              | Type      | Source / transform                                            |
|---------------------|-----------|---------------------------------------------------------------|
| `activity_id`       | Int64     | `activityId`                                                  |
| `activity_type`     | Utf8      | `activityType.typeKey`                                        |
| `start_time_local`  | Datetime  | parse `startTimeLocal`                                        |
| `start_time_utc`    | Datetime  | parse `startTimeGMT`                                          |
| `duration_sec`      | Float64   | `duration`                                                    |
| `distance_m`        | Float64   | `distance`                                                    |
| `avg_hr`            | Float64   | `averageHR`                                                   |
| `max_hr`            | Float64   | `maxHR`                                                       |
| `calories`          | Float64   | `calories`                                                    |
| `elevation_gain_m`  | Float64   | `elevationGain`                                               |
| `elevation_loss_m`  | Float64   | `elevationLoss`                                               |
| `start_lat`         | Float64   | `startLatitude` × (180 / 2**31) if present, else null         |
| `start_lon`         | Float64   | `startLongitude` × (180 / 2**31) if present, else null        |
| `year`              | Int32     | derived from `start_time_local`                               |
| `month`             | Int32     | derived from `start_time_local`                               |

## Target schema — `fit_records`

| Column              | Type      | Source / transform                                            |
|---------------------|-----------|---------------------------------------------------------------|
| `activity_id`       | Int64     | filename stem (e.g., `12345678.zip` → 12345678)              |
| `timestamp`         | Datetime  | FIT record `timestamp` field (UTC)                            |
| `heart_rate`        | Int32     | FIT record `heart_rate` (bpm, nullable)                       |
| `cadence`           | Int32     | FIT record `cadence` (rpm, nullable)                          |
| `speed`             | Float64   | FIT record `speed` (m/s, nullable)                            |
| `power`             | Int32     | FIT record `power` (watts, nullable)                          |
| `distance`          | Float64   | FIT record `distance` (metres cumulative, nullable)           |
| `altitude`          | Float64   | FIT record `altitude` (metres, nullable)                      |
| `position_lat`      | Float64   | FIT record `position_lat` × (180 / 2**31), nullable           |
| `position_lon`      | Float64   | FIT record `position_lon` × (180 / 2**31), nullable           |
| `year`              | Int32     | derived from `timestamp`                                       |
| `month`             | Int32     | derived from `timestamp`                                       |

## Behavior — `activities.py`

- `transform`:
  1. `pl.read_json(path)` each file, concat vertically.
  2. Select/rename/cast to the schema below.
  3. Semicircle → degrees: `pl.col("startLatitude") * (180.0 / 2**31)`. Handle nulls.
  4. Deduplicate on `activity_id`, keeping last.
- `rebuild`:
  1. Glob `{data_root()}/bronze/activities/**/*.json`.
  2. Call `transform`, write to `silver_path("activities")` with `partition_by=["year", "month"]`.
  3. Return `df.height`.

## Behavior — `fit_records.py`

- `transform`:
  1. For each ZIP path:
     - Open with `zipfile.ZipFile`.
     - Extract the `.fit` file (typically `{activity_id}.fit`).
     - Parse with `garmin-fit-sdk`: `fitfile = FitFile(io.BytesIO(fit_bytes))`.
     - Read "record" messages (type 20), which contain per-second telemetry.
     - Extract columns: `timestamp`, `heart_rate`, `cadence`, `speed`, `power`, `distance`, `altitude`, `position_lat` (semicircles), `position_lon` (semicircles).
     - Derive `activity_id` from the ZIP filename stem.
  2. Concat all records into a single DataFrame.
  3. Semicircle → degrees: `pl.col("position_lat") * (180.0 / 2**31)`. Handle nulls.
  4. Extract `year`, `month` from `timestamp`.
  5. Deduplicate on `(activity_id, timestamp)`, keeping last.
- `rebuild`:
  1. Glob `{data_root()}/bronze/fit/**/*.zip`.
  2. Call `transform`, write to `silver_path("fit_records")` with `partition_by=["year", "month"]`.
  3. Return `df.height`.

## Acceptance — `activities`

- Given fixture JSON with 3 activities (one duplicate), `transform` returns 2 rows with expected types.
- Semicircle conversion: a Garmin `startLatitude` of `523255203` yields ~43.86° (verify with a known fixture).
- `rebuild` produces `data/silver/activities/year=YYYY/month=MM/*.parquet`; readable by DuckDB with `hive_partitioning=1`.

## Acceptance — `fit_records`

- Given a fixture FIT ZIP with 100 record messages, `transform` returns 100 rows, each with `timestamp`, `heart_rate`, `position_lat` (in degrees, not semicircles), etc.
- Semicircle conversion: a FIT `position_lat` of `523255203` yields ~43.86°.
- `rebuild` produces `data/silver/fit_records/year=YYYY/month=MM/*.parquet`; readable by DuckDB.
- Null fields (e.g., `power` on a non-power-meter activity) appear as null rows, not errors.

## Notes

- Keep both `transform` functions pure — no filesystem writes, no env reads.
- `fit_records.transform` needs `garmin-fit-sdk` installed (add to `pyproject.toml` dependencies).
- FIT parsing: Use `garmin_fit_sdk.FitFile(io.BytesIO(fit_bytes))` to parse; iterate on `.messages` and filter by `name == "record"`.
- Semicircle values in FIT may be signed or unsigned; handle gracefully (Polars will infer the type).
- If a FIT file is corrupted or can't be parsed, log a warning and skip that ZIP rather than raising (fail gracefully for partial ingests).
