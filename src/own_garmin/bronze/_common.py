import logging
from collections import defaultdict
from datetime import date, datetime

_LOGGER = logging.getLogger(__name__)


def group_by_day(activities: list[dict]) -> dict[date, list[dict]]:
    """Group activities by their startTimeLocal date.

    Skips entries with missing activityId or unparseable startTimeLocal,
    logging a warning for each.
    """
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
    return by_day
