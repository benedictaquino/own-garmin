import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from own_garmin.bronze import activity_details


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def make_activity(activity_id: int, start_time: str) -> dict:
    return {"activityId": activity_id, "startTimeLocal": start_time}


def test_ingest_writes_details(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
        make_activity(2, "2026-01-05 10:00:00"),
    ]
    client.get_activity_details.side_effect = lambda aid: {
        "activityId": aid,
        "splits": [],
    }

    with patch("time.sleep"):
        count = activity_details.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    path = Path(tmp_path / "bronze/activity_details/year=2026/month=01/day=05.json")
    assert path.exists()
    data = json.loads(path.read_text())
    assert len(data) == 2
    assert {a["activityId"] for a in data} == {1, 2}
    assert count == 1


def test_ingest_skips_existing_day(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
    ]
    client.get_activity_details.return_value = {"activityId": 1}

    # Pre-create the day file
    path = Path(tmp_path / "bronze/activity_details/year=2026/month=01/day=05.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([{"activityId": 1}], indent=2))

    with patch("time.sleep"):
        count = activity_details.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    client.get_activity_details.assert_not_called()
    assert count == 0


def test_ingest_returns_newly_written_count(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
        make_activity(2, "2026-01-06 09:00:00"),
    ]
    client.get_activity_details.side_effect = lambda aid: {"activityId": aid}

    with patch("time.sleep"):
        count = activity_details.ingest(client, date(2026, 1, 5), date(2026, 1, 6))

    assert count == 2


def test_ingest_sleeps_between_calls(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
        make_activity(2, "2026-01-05 09:00:00"),
        make_activity(3, "2026-01-05 10:00:00"),
    ]
    client.get_activity_details.side_effect = lambda aid: {"activityId": aid}

    with patch("own_garmin.bronze.activity_details.time") as mock_time:
        activity_details.ingest(
            client, date(2026, 1, 5), date(2026, 1, 5), sleep_sec=1.0
        )

    # sleep called between requests: 3 activities → 2 sleeps
    assert mock_time.sleep.call_count == 2
    mock_time.sleep.assert_called_with(1.0)


def test_ingest_skips_missing_activity_id(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        {"startTimeLocal": "2026-01-05 08:00:00"},  # no activityId
        make_activity(2, "2026-01-05 09:00:00"),
    ]
    client.get_activity_details.return_value = {"activityId": 2}

    with patch("time.sleep"):
        count = activity_details.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    assert client.get_activity_details.call_count == 1
    assert count == 1
