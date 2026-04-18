import json

from own_garmin import paths, storage
from own_garmin.bronze._common import group_by_day


def ingest(activities: list[dict]) -> int:
    """Write activity summaries to bronze, merging on activityId (new wins).

    Returns count of valid input activities (those with an id and parseable
    startTimeLocal). Entries without either are skipped and logged.
    """
    by_day = group_by_day(activities)

    total = 0
    for day, new_activities in by_day.items():
        path = paths.bronze_path("activities", day)
        existing: dict[int, dict] = {}
        if storage.exists(path):
            for a in json.loads(storage.read_text(path)):
                if "activityId" in a:
                    existing[a["activityId"]] = a

        for a in new_activities:
            existing[a["activityId"]] = a  # new wins on conflict

        merged = list(existing.values())
        new_json = json.dumps(merged, indent=2, sort_keys=True)
        if not storage.exists(path) or storage.read_text(path) != new_json:
            storage.write_text(path, new_json)

        total += len(new_activities)

    return total
