import json
import time

from own_garmin import paths, storage
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
        if storage.exists(path):
            for record in json.loads(storage.read_text(path)):
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
        if not storage.exists(path) or storage.read_text(path) != new_json:
            storage.write_text(path, new_json)
            files_changed += 1

    return files_changed
