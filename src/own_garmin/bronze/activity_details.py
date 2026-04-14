import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from own_garmin import paths
from own_garmin.client import GarminClient

_LOGGER = logging.getLogger(__name__)


def ingest(
    client: GarminClient, since: date, until: date, sleep_sec: float = 0.5
) -> int:
    """Fetch activity details (splits, laps, metrics), write to bronze.

    Returns count of day-files written or updated.
    """
    activities = client.list_activities(since, until)

    by_day: dict[date, list[dict]] = defaultdict(list)
    for activity in activities:
        if "activityId" not in activity:
            _LOGGER.warning("activity missing activityId, skipping: %r", activity)
            continue
        start_str = activity.get("startTimeLocal", "")
        try:
            day = datetime.strptime(start_str[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            _LOGGER.warning(
                "activity %s has unparseable startTimeLocal %r, skipping",
                activity["activityId"],
                start_str,
            )
            continue
        by_day[day].append(activity)

    files_changed = 0
    first_request = True
    for day, day_activities in by_day.items():
        path = paths.bronze_path("activity_details", day)

        existing: dict[int, dict] = {}
        if Path(path).exists():
            with open(path) as f:
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
        if not Path(path).exists() or Path(path).read_text() != new_json:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(new_json)
            files_changed += 1

    return files_changed
