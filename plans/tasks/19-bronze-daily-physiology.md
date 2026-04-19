# Task 19: Bronze: HR, respiration, SpO2, intensity minutes

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Implement four bronze extractor modules for the Daily Physiological Metrics domain: `heart_rate`,
`respiration`, `spo2`, and `intensity_minutes`. The heart rate, respiration, and SpO2 responses
each contain both a daily summary object and an array of high-resolution intraday samples. Bronze
must write the full response unchanged; the split into `_daily` and `_samples` silver tables happens
downstream in Task 21 and Task 22.

## Files

* `src/own_garmin/bronze/heart_rate.py` — **new**
* `src/own_garmin/bronze/respiration.py` — **new**
* `src/own_garmin/bronze/spo2.py` — **new**
* `src/own_garmin/bronze/intensity_minutes.py` — **new**

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
   * `heart_rate` → `client.get_heart_rate(day)`
   * `respiration` → `client.get_respiration(day)`
   * `spo2` → `client.get_spo2(day)`
   * `intensity_minutes` → `client.get_intensity_minutes(day, day)` (single-day range)
3. Determine the target path with `paths.bronze_path("<category>", day)`.
4. **Idempotency — merge on `calendarDate`:** load existing file if present, merge on `calendarDate`
   (new wins), rewrite only if content changed.
   * `heart_rate`, `respiration`, and `spo2` each return a single `dict` with a top-level
     `calendarDate` field — wrap as a one-element list for consistent storage.
   * `intensity_minutes` returns a `dict` with a `calendarDate` field — wrap as a one-element list.
5. Write pretty-printed JSON (`indent=2`) via `storage.write_text`.
6. Sleep `sleep_sec` (± 10 % jitter) between requests.
7. Return count of files written or updated.
8. Log and skip days where the client raises `GarminConnectionError`; do not abort.

## Acceptance Criteria

* [ ] All four modules implement `ingest` with the signature above.
* [ ] Bronze files land at `data/bronze/<category>/year=YYYY/month=MM/day=DD.json`.
* [ ] The full API response (summary fields and intraday samples array) is stored verbatim for
      `heart_rate`, `respiration`, and `spo2` — no fields are dropped at the bronze layer.
* [ ] Re-running over the same date window does not rewrite unchanged files.
* [ ] An API response that omits the intraday samples array (e.g., a Garmin device that does not
      record all-day HR) writes the dict as-is — no `KeyError` on missing keys.
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes.

## Notes

* The `heart_rate` endpoint response typically has the structure:

  ```json
  {
    "calendarDate": "2026-04-01",
    "restingHeartRate": 52,
    "maxHeartRate": 178,
    "minHeartRate": 46,
    "heartRateValueDescriptors": [...],
    "heartRateValues": [[<epoch_ms>, <bpm>], ...]
  }
  ```

  Intraday samples live under `heartRateValues` as epoch-ms + bpm pairs. Silver (Task 22) is
  responsible for exploding these into rows.
* `respiration` and `spo2` have analogous structures with intraday arrays under different keys.
  Bronze does not need to know the key names — it stores the whole dict.
* `intensity_minutes` does not contain intraday samples; only a summary dict. No special handling
  required.
* All file I/O goes through `storage.*`.
* Requires Task 17 to be complete.
