# Task 08: Tests for silver transform

## Goal
Lock down the silver schema and unit-conversion logic with fixture-driven pytest tests. Per global norms, tests use real data (captured fixtures), not mocks.

## Files
- `tests/fixtures/activities/year=2026/month=01/day=15.json` — a canned Garmin response containing 2 activities (a run and a ride).
- `tests/fixtures/activities/year=2026/month=01/day=16.json` — contains 2 activities: one with no GPS (`startLatitude` / `startLongitude` null), and one that is a duplicate `activityId` from day=15 to exercise dedup.
- `tests/test_silver_activities.py`

## Fixture construction
- Either capture fixtures from a real Garmin response (preferred — trim to essential fields) or hand-craft minimal JSON with only the fields the transform consumes. Either is fine as long as the JSON shape matches what Garmin returns.
- Include sentinels that make assertions easy (e.g., a run with `startLatitude = 523255203` which should convert to ~43.86°).

## Tests
```python
def test_transform_schema(): ...
def test_transform_dedup(): ...
def test_transform_semicircle_conversion(): ...
def test_transform_null_gps(): ...
```

- `test_transform_schema` — asserts every column in the target schema exists with the expected dtype.
- `test_transform_dedup` — 3 activities across two day-files, duplicate `activityId`; expect 2 rows out.
- `test_transform_semicircle_conversion` — asserts `start_lat` for the sentinel activity ≈ 43.86 (tolerance 1e-4).
- `test_transform_null_gps` — the activity with null GPS has null `start_lat` / `start_lon` rather than raising or producing 0.0.

## Acceptance
- `uv run pytest -q` reports all four tests passing.
- Tests do not touch the network or the user's real `data/` directory.
- Fixtures are small (<5KB each).

## Notes
- Resolve fixture paths via `Path(__file__).parent / "fixtures" / ...` so tests are location-independent.
- Do not import the bronze module here — silver tests should only depend on `silver.activities.transform`.
