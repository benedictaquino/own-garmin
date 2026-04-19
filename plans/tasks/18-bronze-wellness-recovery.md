# Task 18: Bronze: sleep, body battery, stress, HRV

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Implement four bronze extractor modules for the Wellness & Recovery domain: `sleep`, `body_battery`,
`stress`, and `hrv`. Each fetches one day of raw JSON from the Garmin API and writes it to the
bronze layer using the standard day-partition path scheme. All four are idempotent and use
`calendarDate` as the stable merge key.

## Files

* `src/own_garmin/bronze/sleep.py` — **new**
* `src/own_garmin/bronze/body_battery.py` — **new**
* `src/own_garmin/bronze/stress.py` — **new**
* `src/own_garmin/bronze/hrv.py` — **new**

## Public API

```python
from datetime import date
from own_garmin.client import GarminClient

# Same signature for all four modules:
def ingest(client: GarminClient, since: date, until: date, sleep_sec: float = 0.5) -> int:
    """Fetch <category> data day by day, write to bronze. Returns count of files written/updated."""
```

## Behavior

1. Iterate `since` through `until` inclusive, one calendar day at a time.
2. For each day, call the appropriate client method introduced in Task 17:
   * `sleep` → `client.get_sleep(day)`
   * `body_battery` → `client.get_body_battery(day, day)` (single-day range)
   * `stress` → `client.get_stress(day)`
   * `hrv` → `client.get_hrv(day)`
3. Determine the target path with `paths.bronze_path("<category>", day)`.
4. **Idempotency — merge on `calendarDate`:** if the file already exists, load it, merge the new
   payload on `calendarDate` (new wins), and rewrite only if the serialized content changed.
   * For `sleep` and `stress` the response is a single `dict` — wrap as a one-element list for
     consistent storage and dedup.
   * For `body_battery` the response is a `list[dict]`; each element has `calendarDate`.
   * For `hrv` the response is a `dict` with a top-level `calendarDate` field — wrap as a
     one-element list.
5. Write pretty-printed JSON (`indent=2`) via `storage.write_text`.
6. Sleep `sleep_sec` seconds (± 10 % jitter via `random.uniform`) between requests.
7. Return count of files written or updated.
8. If the client raises `GarminConnectionError` for a specific day, log a warning and continue to
   the next day rather than aborting the entire window.

## Acceptance Criteria

* [ ] All four modules implement `ingest` with the signature above.
* [ ] Bronze files land at `data/bronze/<category>/year=YYYY/month=MM/day=DD.json`.
* [ ] Re-running over the same date window does not rewrite files whose content has not changed.
* [ ] A day that returns an empty dict or empty list from the API writes an empty JSON array `[]`
      rather than raising.
* [ ] Raw Garmin JSON shape is preserved exactly — no field renaming or type coercion.
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes.

## Notes

* Stable merge key for all four categories in this task is `calendarDate` (top-level string field,
  format `YYYY-MM-DD`). If the API response for a given day lacks `calendarDate`, use the
  requested `day.isoformat()` as the key to ensure idempotency.
* `body_battery` uses a range query internally but this extractor still writes one file per day.
  Pass `start=day, end=day` to `client.get_body_battery` and write the resulting list.
* The `sleep` response from the Garmin API often embeds sleep stage details in a nested
  `dailySleepDTO` object. Bronze must store the full response unchanged; silver (Task 21) owns
  flattening.
* All file I/O goes through `storage.*` (not `pathlib` or `open`) so S3 compatibility is automatic.
* Requires Task 17 to be complete.
