# Task 04: Bronze ingestion — activities

## Goal
Fetch activity summaries + per-activity detail from Garmin and write them as immutable JSON, partitioned by start-date day.

## File
`src/own_garmin/bronze/activities.py`

## Public API
```python
from datetime import date
from own_garmin.client import GarminClient

def ingest(client: GarminClient, since: date, until: date, sleep_sec: float = 0.5) -> int:
    """Fetch activities between since and until, write bronze JSON. Returns count of activities written."""
```

## Behavior
1. Call `client.list_activities(since, until)` → list of summary dicts.
2. For each summary, call `client.get_activity(summary["activityId"])` for full detail. `time.sleep(sleep_sec)` between calls.
3. Use the full detail dict as the record (it's a superset of the summary).
4. Group records by their `startTimeLocal` date (parse with `datetime.fromisoformat` after normalizing Garmin's `YYYY-MM-DD HH:MM:SS` format).
5. For each day:
   - Resolve `bronze_path("activities", day)`.
   - `mkdir -p` its parent directory.
   - If the file exists, load it; merge with new records keyed by `activityId` (new wins — the ADR notes retroactive Garmin updates require replacing the full-file representation).
   - Sort records by `activityId` for stable output.
   - Write `json.dumps(records, indent=2, ensure_ascii=False)` only if content differs from existing (avoid churning mtimes on no-op runs).
6. Return total number of distinct activities written (post-merge).

## Acceptance
- Running twice over the same window is idempotent: no duplicate entries, second run is a no-op on disk.
- Files land at `data/bronze/activities/year=YYYY/month=MM/day=DD.json`.
- Each file is a valid JSON array of activity objects, pretty-printed.

## Notes
- Do not transform fields here — preserve Garmin's raw shape exactly. That is the whole point of the bronze layer.
- Sleep between detail calls is the only rate-limit lever; keep it configurable but default conservative (0.5s).
- If `list_activities` returns a summary missing `activityId`, log and skip rather than raising.
