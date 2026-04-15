# Task 08: Tests for silver transform

Resolves [#8](https://github.com/benedictaquino/own-garmin/issues/8).

## Goal

Lock down the silver schema and unit-conversion logic with fixture-driven pytest tests. Per global norms, tests use real data (captured fixtures), not mocks.

## Files

- `tests/fixtures/activities/year=2026/month=01/day=15.json` — a canned Garmin response containing 2 activities (a run and a ride).
- `tests/fixtures/activities/year=2026/month=01/day=16.json` — contains 2 activities: one with no GPS (`startLatitude` / `startLongitude` null), and one that is a duplicate `activityId` from day=15 to exercise dedup.
- `tests/test_silver_activities.py`

## Fixture design

Both fixtures are JSON arrays matching Garmin's `activities` list response shape. Include only fields `silver.activities.transform` consumes (keeps each file <5KB).

**`day=15.json` — 2 activities:**

1. `activityId: 1001` — run, `startTimeLocal: "2026-01-15 08:00:00"`, `startTimeGMT: "2026-01-15 16:00:00"`, `startLatitude: 523270182` (sentinel chosen so `semicircle → degree` yields 43.86 within `1e-4`), `startLongitude: -1073741824`, `distance: 5000.0`, full HR / calories / elevation fields populated.
2. `activityId: 1002` — ride (`activityType.typeKey: "cycling"`), same day, has GPS, different distance.

**`day=16.json` — 2 activities:**

1. `activityId: 1003` — run with **no `startLatitude` / `startLongitude` fields at all** (null GPS case).
2. `activityId: 1001` — duplicate of day=15's activity 1001, but with `distance: 9999.0` so dedup `keep="last"` behavior is observable.

Net: **4 input rows → 3 unique `activity_id` after dedup.**

## Tests — `tests/test_silver_activities.py`

Resolve paths via `Path(__file__).parent / "fixtures" / ...`, import only `own_garmin.silver.activities.transform` (no bronze dependency), no network, no writes to real `data/`.

```python
from pathlib import Path
import polars as pl
import pytest
from own_garmin.silver.activities import transform

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "activities"
DAY_15 = str(FIXTURE_DIR / "year=2026" / "month=01" / "day=15.json")
DAY_16 = str(FIXTURE_DIR / "year=2026" / "month=01" / "day=16.json")
```

1. **`test_transform_schema`** — `transform([DAY_15, DAY_16])`, assert every key in the target schema (`activity_id` Int64, `activity_type` Utf8, `start_time_local`/`start_time_utc` Datetime, `duration_sec` / `distance_m` / `avg_hr` / `max_hr` / `calories` / `elevation_gain_m` / `elevation_loss_m` / `start_lat` / `start_lon` Float64, `year` / `month` Int32) is present with expected dtype.
2. **`test_transform_dedup`** — `transform([DAY_15, DAY_16])` returns 3 unique rows (4 in, 1 dup on `activity_id=1001`); assert `set(df["activity_id"].to_list()) == {1001, 1002, 1003}` and the surviving `1001` row has `distance_m == 9999.0` (last-wins from day=16). *Note: original spec wording mentions "3 activities → 2 rows" but the fixture spec itself describes 4 activities with 1 duplicate; this plan follows the fixture spec and asserts 3 unique rows, which is the meaningful assertion.*
3. **`test_transform_semicircle_conversion`** — `transform([DAY_15])`, filter to sentinel `activity_id == 1001`, assert `start_lat == pytest.approx(43.86, abs=1e-4)`.
4. **`test_transform_null_gps`** — `transform([DAY_16])`, filter to `activity_id == 1003`, assert `start_lat is None and start_lon is None`.

## De-duplicate with inline tests

The older inline tests in `tests/silver/test_activities.py` covered the same four assertions via an inline `_make_activity()` factory. Delete those four (`test_transform_schema_types`, `test_transform_dedup_by_activity_id`, `test_transform_semicircle_conversion`, `test_transform_null_gps`) so there is a single source of truth per assertion. Keep the other inline tests in that file — they cover `rebuild()`, partitioning, empty-input, and year/month derivation, which these fixture tests do not.

## Critical files (read-only reference)

- `src/own_garmin/silver/activities.py:13-29` — `_SCHEMA` definitive column/dtype list.
- `src/own_garmin/silver/activities.py:32-80` — `transform()` signature, semicircle constant (`180.0 / 2**31`), `keep="last"` dedup.

## Verification

```bash
uv run pytest tests/test_silver_activities.py -q          # the four new tests pass
uv run pytest -q                                          # full suite still green
uv run ruff check .
ls -la tests/fixtures/activities/year=2026/month=01/      # confirm both files <5KB
```

No network is touched (transform reads local JSON only); no writes to `./data/` (transform returns a DataFrame, never writes).

## Acceptance criteria

- `uv run pytest -q` reports all four tests passing.
- Tests do not touch the network or the user's real `data/` directory.
- Fixtures are small (<5KB each).

## Notes

- Resolve fixture paths via `Path(__file__).parent / "fixtures" / ...` so tests are location-independent.
- Do not import the bronze module here — silver tests should only depend on `silver.activities.transform`.
