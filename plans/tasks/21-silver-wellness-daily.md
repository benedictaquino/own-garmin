# Task 21: Silver: daily-summary tables

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Implement ten silver transform modules — one per wellness category — each producing a typed,
hive-partitioned Parquet table with one row per date. Modules follow the same pure-function
shape as `silver/activities.py`.

## Files

* `src/own_garmin/silver/sleep.py` — **new**
* `src/own_garmin/silver/body_battery.py` — **new**
* `src/own_garmin/silver/stress.py` — **new**
* `src/own_garmin/silver/hrv.py` — **new**
* `src/own_garmin/silver/heart_rate.py` — **new**
* `src/own_garmin/silver/respiration.py` — **new**
* `src/own_garmin/silver/spo2.py` — **new**
* `src/own_garmin/silver/intensity_minutes.py` — **new**
* `src/own_garmin/silver/weight.py` — **new**
* `src/own_garmin/silver/steps.py` — **new**

## Public API

Each module exposes:

```python
import polars as pl

def transform(bronze_json_paths: list[str]) -> pl.DataFrame:
    """Transform bronze JSON files into a typed daily-summary DataFrame."""

def rebuild() -> int:
    """Rebuild <table_name> silver from all bronze/<category>/**/*.json. Returns row count."""
```

## Target Schemas

All tables include `year: Int32` and `month: Int32` partition columns derived from `date`.
Dedup by `date`, keeping last (same `unique(subset=["date"], keep="last")` pattern as
`silver/activities.py`). Nullable columns use the `nullable()` helper pattern from that module.

**`sleep_summary`** (source: `bronze/sleep/`):

| Column | Type |
|---|---|
| `date` | Date |
| `total_sleep_sec` | Float64 |
| `deep_sec` | Float64 |
| `light_sec` | Float64 |
| `rem_sec` | Float64 |
| `awake_sec` | Float64 |
| `sleep_score` | Int32 (nullable) |
| `year` | Int32 |
| `month` | Int32 |

**`body_battery_daily`** (source: `bronze/body_battery/`):

| Column | Type |
|---|---|
| `date` | Date |
| `bb_start` | Int32 |
| `bb_end` | Int32 |
| `bb_min` | Int32 |
| `bb_max` | Int32 |
| `bb_charged` | Int32 |
| `bb_drained` | Int32 |
| `year` | Int32 |
| `month` | Int32 |

**`stress_daily`** (source: `bronze/stress/`):

| Column | Type |
|---|---|
| `date` | Date |
| `avg_stress` | Int32 |
| `max_stress` | Int32 |
| `stress_qualifier` | Utf8 |
| `year` | Int32 |
| `month` | Int32 |

**`hrv_daily`** (source: `bronze/hrv/`):

| Column | Type |
|---|---|
| `date` | Date |
| `last_night_avg_ms` | Float64 |
| `last_night_5min_high_ms` | Float64 |
| `baseline_low_upper` | Float64 |
| `baseline_balanced_low` | Float64 |
| `baseline_balanced_upper` | Float64 |
| `status` | Utf8 |
| `year` | Int32 |
| `month` | Int32 |

**`heart_rate_daily`** (source: `bronze/heart_rate/`):

| Column | Type |
|---|---|
| `date` | Date |
| `resting_hr` | Int32 |
| `max_hr` | Int32 |
| `min_hr_daily` | Int32 |
| `avg_hr` | Int32 |
| `year` | Int32 |
| `month` | Int32 |

**`respiration_daily`** (source: `bronze/respiration/`):

| Column | Type |
|---|---|
| `date` | Date |
| `avg_breath_rate` | Float64 |
| `lowest` | Float64 |
| `highest` | Float64 |
| `year` | Int32 |
| `month` | Int32 |

**`spo2_daily`** (source: `bronze/spo2/`):

| Column | Type |
|---|---|
| `date` | Date |
| `avg_spo2` | Float64 |
| `lowest_spo2` | Float64 |
| `latest_spo2` | Float64 |
| `year` | Int32 |
| `month` | Int32 |

**`intensity_minutes_daily`** (source: `bronze/intensity_minutes/`):

| Column | Type |
|---|---|
| `date` | Date |
| `moderate` | Int32 |
| `vigorous` | Int32 |
| `total` | Int32 |
| `year` | Int32 |
| `month` | Int32 |

`total` is computed as `moderate + 2 * vigorous` — it is not read from the source JSON.

**`weight_daily`** (source: `bronze/weight/`):

| Column | Type |
|---|---|
| `date` | Date |
| `weight_g` | Float64 |
| `bmi` | Float64 |
| `body_fat_pct` | Float64 |
| `body_water_pct` | Float64 |
| `bone_mass_g` | Float64 |
| `muscle_mass_g` | Float64 |
| `year` | Int32 |
| `month` | Int32 |

**`steps_daily`** (source: `bronze/steps/`):

| Column | Type |
|---|---|
| `date` | Date |
| `total_steps` | Int32 |
| `total_distance_m` | Float64 |
| `step_goal` | Int32 |
| `floors_climbed` | Int32 |
| `year` | Int32 |
| `month` | Int32 |

## Behavior

Shared pattern for all ten modules (describe once, apply to each):

1. `transform(bronze_json_paths)`:
   1. For each path, `json.loads(storage.read_bytes(p))` → each file is a JSON array; flatten
      all records into a single list.
   2. Build a Polars DataFrame from the list using `pl.from_dicts`.
   3. Select/rename/cast to the target schema using the `nullable()` helper pattern (returns
      `pl.col(name).cast(dtype)` if the column exists, else `pl.lit(None, dtype=dtype)`).
   4. Parse `calendarDate` string to `pl.Date` via `str.to_date("%Y-%m-%d")`.
   5. Derive `year` and `month` from `date`.
   6. Dedup on `["date"]`, keep last.
   7. Return empty frame with correct schema if input list is empty or all files are empty.
2. `rebuild()`:
   1. Glob `{paths.data_root()}/bronze/<category>/**/*.json` via `storage.list_files`.
   2. Call `transform`, rmtree the silver target, write partitioned Parquet via
      `storage.write_partitioned_parquet(df, target, ["year", "month"])`.
   3. Return `df.height`.

## Acceptance Criteria

* [ ] All 10 modules are importable and expose `transform` and `rebuild`.
* [ ] Schema matches the tables above exactly (column names, dtypes, partition columns).
* [ ] `transform([])` returns an empty DataFrame with the correct schema (no exception).
* [ ] Dedup: two input records with the same `date` produce one output row; the later record
      in file order wins.
* [ ] `intensity_minutes_daily.total` equals `moderate + 2 * vigorous` (not read from JSON).
* [ ] `rebuild()` produces `data/silver/<table_name>/year=YYYY/month=MM/*.parquet`.
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes.

## Notes

* `sleep_summary` fields map from the `dailySleepDTO` nested object inside the Garmin sleep
  response. Silver must flatten that nested object; bronze stores it intact.
* `hrv_daily` fields live under an `hrvSummary` nested object in the Garmin HRV response.
  Silver flattens; bronze stores intact.
* `heart_rate_daily` summary fields (`restingHeartRate`, `maxHeartRate`, `minHeartRate`) live
  at the top level of the Garmin heart-rate response; intraday samples are in `heartRateValues`.
  This transform ignores the samples array — Task 22 owns that.
* All file I/O goes through `storage.*`.
* Requires Tasks 17–20 to be complete for real data; transform is pure and testable without them.
