# Task 20: Bronze: weight & body composition, steps & floors

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Implement two bronze extractor modules — `weight` and `steps` — for the Physical Composition &
Goals domain. Both endpoints accept date ranges and return multi-day lists; extractors fetch the
full requested window in one call, then split the response by date and write one file per day,
consistent with the per-day layout used by every other bronze category.

## Files

* `src/own_garmin/bronze/weight.py` — **new**
* `src/own_garmin/bronze/steps.py` — **new**

## Public API

```python
from datetime import date
from own_garmin.client import GarminClient

# Same signature for both modules:
def ingest(client: GarminClient, since: date, until: date, sleep_sec: float = 0.5) -> int:
    """Fetch <category> data for the date range, split by day, write to bronze.

    Returns count of day-files written or updated.
    """
```

## Behavior

1. Call the appropriate client method once for the full range:
   * `weight` → `client.get_weight(since, until)` returns `list[dict]`
   * `steps` → `client.get_steps(since, until)` returns `list[dict]`
2. Group the returned records by their `calendarDate` field (format `YYYY-MM-DD`). Records
   lacking a `calendarDate` key are skipped with a warning.
3. For each date present in the grouped result, determine the target path with
   `paths.bronze_path("<category>", day)`.
4. **Idempotency — merge on `calendarDate`:** if the file already exists, load it, merge on
   `calendarDate` (new wins), and rewrite only if the serialized content changed.
5. Write pretty-printed JSON (`indent=2`) via `storage.write_text`. Each day file contains a
   JSON array of records for that date (typically one element).
6. **Skip empty days:** if Garmin returns no records for a given calendar date within the
   range (e.g., no weigh-in that day), do not write an empty `[]` file. Only write files for
   dates that have at least one record in the API response.
7. Sleep `sleep_sec` (± 10 % jitter via `random.uniform`) after the single range request
   before returning — consistent with per-day extractors even though only one request is made.
8. Return count of files written or updated.
9. If the client raises `GarminConnectionError`, log a warning and return 0 rather than
   aborting — the caller may retry with a narrower window.

## Acceptance Criteria

* [ ] Both modules implement `ingest` with the signature above.
* [ ] Bronze files land at `data/bronze/<category>/year=YYYY/month=MM/day=DD.json`.
* [ ] The full API response per record is preserved verbatim — no field renaming or coercion.
* [ ] Re-running over the same date window does not rewrite files whose content has not changed.
* [ ] Days with no records in the API response produce no file (skipped, not written as `[]`).
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes.

## Notes

* `weight` records typically include fields such as `calendarDate`, `weight`, `bmi`, `bodyFat`,
  `bodyWater`, `boneMass`, `muscleMass`. Bronze stores all fields unchanged; silver (Task 21)
  owns field selection and type casting.
* `steps` records typically include `calendarDate`, `totalSteps`, `totalDistance`, `stepGoal`,
  `floorsAscended`. Bronze stores verbatim.
* Stable idempotency key for both categories is `calendarDate`.
* Unlike Tasks 18 and 19 where each day required one network call, these extractors issue one
  call per `ingest` invocation regardless of the date range length. The `sleep_sec` jitter still
  applies for rate-limit courtesy.
* All file I/O goes through `storage.*` (not `pathlib` or `open`) so S3 compatibility is automatic.
* Requires Task 17 to be complete.
