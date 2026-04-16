import json
from pathlib import Path

from own_garmin import paths
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
        if Path(path).exists():
            with open(path, encoding="utf-8") as f:
                for a in json.load(f):
                    existing[a["activityId"]] = a

        for a in new_activities:
            existing[a["activityId"]] = a  # new wins on conflict

        merged = list(existing.values())
        new_json = json.dumps(merged, indent=2, sort_keys=True)
        target = Path(path)
        if not target.exists() or target.read_text(encoding="utf-8") != new_json:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(new_json, encoding="utf-8")

        total += len(new_activities)

    return total
