import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from own_garmin.bronze import activities


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def make_activity(activity_id: int, start_time: str, **extra) -> dict:
    return {"activityId": activity_id, "startTimeLocal": start_time, **extra}


def test_ingest_writes_json_array(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
        make_activity(2, "2026-01-05 10:00:00"),
    ]

    count = activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    path = Path(tmp_path / "bronze/activities/year=2026/month=01/day=05.json")
    assert path.exists()
    data = json.loads(path.read_text())
    assert len(data) == 2
    assert {a["activityId"] for a in data} == {1, 2}
    assert count == 2


def test_ingest_groups_by_day(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
        make_activity(2, "2026-01-06 09:00:00"),
        make_activity(3, "2026-01-06 11:00:00"),
    ]

    count = activities.ingest(client, date(2026, 1, 5), date(2026, 1, 6))

    day5 = Path(tmp_path / "bronze/activities/year=2026/month=01/day=05.json")
    day6 = Path(tmp_path / "bronze/activities/year=2026/month=01/day=06.json")
    assert day5.exists()
    assert day6.exists()
    assert len(json.loads(day5.read_text())) == 1
    assert len(json.loads(day6.read_text())) == 2
    assert count == 3


def test_ingest_idempotent(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
    ]

    activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))
    path = Path(tmp_path / "bronze/activities/year=2026/month=01/day=05.json")
    mtime_before = path.stat().st_mtime

    activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))
    mtime_after = path.stat().st_mtime

    assert mtime_before == mtime_after  # file not rewritten
    assert len(json.loads(path.read_text())) == 1


def test_ingest_merge_new_wins(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00", name="old"),
    ]
    activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00", name="new"),
    ]
    activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    path = Path(tmp_path / "bronze/activities/year=2026/month=01/day=05.json")
    data = json.loads(path.read_text())
    assert len(data) == 1
    assert data[0]["name"] == "new"


def test_ingest_skips_missing_activity_id(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        {"startTimeLocal": "2026-01-05 08:00:00", "name": "no-id"},
        make_activity(2, "2026-01-05 09:00:00"),
    ]

    count = activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    path = Path(tmp_path / "bronze/activities/year=2026/month=01/day=05.json")
    data = json.loads(path.read_text())
    assert len(data) == 1
    assert data[0]["activityId"] == 2
    assert count == 1


def test_ingest_returns_count(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(10, "2026-01-05 08:00:00"),
        make_activity(11, "2026-01-05 09:00:00"),
        make_activity(12, "2026-01-06 10:00:00"),
    ]

    count = activities.ingest(client, date(2026, 1, 5), date(2026, 1, 6))
    assert count == 3


def test_ingest_pretty_prints_json(tmp_path):
    client = MagicMock()
    client.list_activities.return_value = [
        make_activity(1, "2026-01-05 08:00:00"),
    ]

    activities.ingest(client, date(2026, 1, 5), date(2026, 1, 5))

    path = Path(tmp_path / "bronze/activities/year=2026/month=01/day=05.json")
    text = path.read_text()
    assert "\n" in text  # pretty-printed, not one line
