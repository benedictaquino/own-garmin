import json
import logging
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from own_garmin import paths
from own_garmin.client import GarminClient

_LOGGER = logging.getLogger(__name__)


def ingest(client: GarminClient, since: date, until: date) -> int:
    """Fetch activity summaries, write to bronze.

    Returns count of activities written.
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

    total = 0
    for day, new_activities in by_day.items():
        path = paths.bronze_path("activities", day)
        existing: dict[int, dict] = {}
        if Path(path).exists():
            with open(path) as f:
                for a in json.load(f):
                    existing[a["activityId"]] = a

        for a in new_activities:
            existing[a["activityId"]] = a  # new wins on conflict

        merged = list(existing.values())
        new_json = json.dumps(merged, indent=2)
        if not Path(path).exists() or Path(path).read_text() != new_json:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(new_json)

        total += len(existing)

    return total
