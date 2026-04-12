# Task 05: Silver transform — activities

## Goal

Transform bronze JSON into a clean, typed, partitioned Parquet dataset. Pure function — no I/O except reading bronze and writing silver.

## File

`src/own_garmin/silver/activities.py`

## Public API

```python
import polars as pl

def transform(bronze_json_paths: list[str]) -> pl.DataFrame: ...
def rebuild() -> int:
    """Rebuild the entire activities silver layer from bronze. Returns row count written."""
```

## Target schema

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

## Behavior

- `transform`:
  1. `pl.read_json(path)` each file, concat vertically.
  2. Select/rename/cast to the schema above.
  3. Semicircle → degrees conversion: `pl.col("startLatitude") * (180.0 / 2**31)`, same for longitude. Handle nulls.
  4. Deduplicate on `activity_id`, keeping last (most recently ingested wins — matches bronze merge semantics).
- `rebuild`:
  1. Glob `{data_root()}/bronze/activities/**/*.json`.
  2. Call `transform`.
  3. Clear `silver_path("activities")` before writing to guarantee a full rebuild (ADR: total Silver rebuilds must be possible).
  4. `df.write_parquet(silver_path("activities"), partition_by=["year", "month"])`.
  5. Return `df.height`.

## Acceptance

- Given fixture JSON with 3 activities (one duplicate), `transform` returns 2 rows with expected types.
- Semicircle conversion: a Garmin `startLatitude` of `523255203` yields ~43.86° (verify with a known fixture).
- `rebuild` produces `data/silver/activities/year=YYYY/month=MM/*.parquet`; files are readable by DuckDB with `hive_partitioning=1`.

## Notes

- Keep `transform` pure — no filesystem writes, no env reads. That keeps it trivially testable (task 08).
- If a field is missing from a given activity JSON, Polars will surface it as null — do not error.
- `pl.read_json` on Garmin payloads may need `schema_overrides` if inference picks the wrong type on sparse fields; add overrides only if tests fail.
