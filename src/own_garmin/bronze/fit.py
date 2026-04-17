import logging
import time
from datetime import datetime

from own_garmin import paths, storage
from own_garmin.client import GarminClient

_LOGGER = logging.getLogger(__name__)


def ingest(client: GarminClient, activities: list[dict], sleep_sec: float = 0.5) -> int:
    """Download FIT ZIPs, write to bronze. Returns count of ZIPs written.

    Unlike the activity and detail modules, existing ZIPs are never
    re-downloaded — FIT payloads are treated as immutable once stored.
    """
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
        if storage.exists(path):
            continue

        if not first_request:
            time.sleep(sleep_sec)
        first_request = False

        fit_bytes = client.download_fit(activity_id)
        storage.write_bytes(path, fit_bytes)
        newly_written += 1

    return newly_written
