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

    Returns count of newly written day-files.
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

    newly_written = 0
    first_request = True
    for day, day_activities in by_day.items():
        path = paths.bronze_path("activity_details", day)
        if Path(path).exists():
            continue

        details = []
        for activity in day_activities:
            if not first_request:
                time.sleep(sleep_sec)
            first_request = False
            details.append(client.get_activity_details(activity["activityId"]))

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(details, indent=2))
        newly_written += 1

    return newly_written
