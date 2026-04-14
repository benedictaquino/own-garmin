import json
import time
from pathlib import Path

from own_garmin import paths
from own_garmin.bronze._common import group_by_day
from own_garmin.client import GarminClient


def ingest(client: GarminClient, activities: list[dict], sleep_sec: float = 0.5) -> int:
    """Fetch activity details (splits, laps, metrics), write to bronze.

    Each day file is merged on activityId (new detail payload wins) and
    only rewritten when its serialized content changes. Returns the count
    of day-files that were written or updated.
    """
    by_day = group_by_day(activities)

    files_changed = 0
    first_request = True
    for day, day_activities in by_day.items():
        path = paths.bronze_path("activity_details", day)

        existing: dict[int, dict] = {}
        if Path(path).exists():
            with open(path, encoding="utf-8") as f:
                for record in json.load(f):
                    if "activityId" in record:
                        existing[record["activityId"]] = record

        for activity in day_activities:
            if not first_request:
                time.sleep(sleep_sec)
            first_request = False
            detail = client.get_activity_details(activity["activityId"])
            existing[activity["activityId"]] = detail  # new wins

        merged = list(existing.values())
        new_json = json.dumps(merged, indent=2)
        target = Path(path)
        if not target.exists() or target.read_text(encoding="utf-8") != new_json:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(new_json, encoding="utf-8")
            files_changed += 1

    return files_changed
