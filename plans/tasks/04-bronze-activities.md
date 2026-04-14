# Task 04: Bronze ingestion — activities, details, and FIT files

## Goal

Ingest three new bronze categories:

1. **activities** — activity summaries (already scoped)
2. **activity_details** — per-activity detail JSON (splits, laps, charts)
3. **fit** — raw FIT ZIP archives

## Files

- `src/own_garmin/bronze/activities.py` — activity summaries
- `src/own_garmin/bronze/activity_details.py` — activity details
- `src/own_garmin/bronze/fit.py` — FIT downloads

## Public API

### `activities.py`

```python
from datetime import date
from own_garmin.client import GarminClient

def ingest(client: GarminClient, since: date, until: date) -> int:
    """Fetch activity summaries, write to bronze. Returns count of activities written."""
```

**Behavior:**

1. Call `client.list_activities(since, until)` → list of summary dicts.
2. Group by `startTimeLocal` date.
3. For each day, write to `bronze_path("activities", day)` as a JSON array.
4. Idempotent: load existing file, merge by `activityId` (new wins), rewrite only if content changed.
5. Return count of distinct activity IDs written.

### `activity_details.py`

```python
from datetime import date
from own_garmin.client import GarminClient

def ingest(client: GarminClient, since: date, until: date, sleep_sec: float = 0.5) -> int:
    """Fetch activity details (splits, laps, metrics), write to bronze. Returns count written."""
```

**Behavior:**

1. Call `client.list_activities(since, until)` → activity list.
2. For each `activity_id`, call `client.get_activity_details(activity_id)`.
3. Extract `startTimeLocal` from the main activity record to determine the day partition.
4. `time.sleep(sleep_sec)` between requests.
5. Group by day and write JSON files to `bronze_path("activity_details", day)`.
6. Idempotent: load existing day file, merge by `activityId` (new wins), rewrite only if content changed. Always re-fetches details so updated records (e.g. corrected splits) are picked up on subsequent runs.
7. Return count of day-files written or updated.

### `fit.py`

```python
from datetime import date
from own_garmin.client import GarminClient

def ingest(client: GarminClient, since: date, until: date, sleep_sec: float = 0.5) -> int:
    """Download FIT ZIPs, write to bronze. Returns count of ZIPs written."""
```

**Behavior:**

1. Call `client.list_activities(since, until)` → activity list.
2. For each `activity_id`, call `client.download_fit(activity_id)` → ZIP bytes.
3. Extract `startTimeLocal` from the activity record to determine the day partition.
4. Create parent directories under `bronze/fit/year=YYYY/month=MM/day=DD/`.
5. Write ZIP bytes to `{parent}/{activity_id}.zip`.
6. `time.sleep(sleep_sec)` between downloads.
7. Idempotent: skip if ZIP already exists.
8. Return count of newly written ZIPs.

## Acceptance

- All three modules run idempotently: re-running over the same date window writes nothing new to disk.
- Activity summaries land at `data/bronze/activities/year=YYYY/month=MM/day=DD.json` (JSON array).
- Activity details land at `data/bronze/activity_details/year=YYYY/month=MM/day=DD.json` (JSON array or single objects).
- FIT ZIPs land at `data/bronze/fit/year=YYYY/month=MM/day=DD/{activity_id}.zip`.
- All JSON is pretty-printed with indent=2.

## Notes

- **Authentication**: `GarminClient` handles the 5-strategy login chain and MFA prompts automatically on instantiation. The `ingest` function should receive an already-authenticated client.
- Do not transform fields here — preserve Garmin's raw shape exactly. That is the whole point of the bronze layer.
- Sleep between detail calls is the only rate-limit lever; keep it configurable but default conservative (0.5s).
- If `list_activities` returns a summary missing `activityId`, log and skip rather than raising.
