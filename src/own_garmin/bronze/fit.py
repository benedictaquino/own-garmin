import logging
import time
from datetime import date, datetime
from pathlib import Path

from own_garmin import paths
from own_garmin.client import GarminClient

_LOGGER = logging.getLogger(__name__)


def ingest(
    client: GarminClient, since: date, until: date, sleep_sec: float = 0.5
) -> int:
    """Download FIT ZIPs, write to bronze. Returns count of ZIPs written."""
    activities = client.list_activities(since, until)

    newly_written = 0
    first_request = True
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

        activity_id = activity["activityId"]
        path = paths.bronze_fit_path(activity_id, day)
        if Path(path).exists():
            continue

        if not first_request:
            time.sleep(sleep_sec)
        first_request = False

        fit_bytes = client.download_fit(activity_id)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(fit_bytes)
        newly_written += 1

    return newly_written
