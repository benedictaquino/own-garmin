import json
from pathlib import Path

import pytest

from own_garmin import query
from own_garmin.silver import activities


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def _make_activity(
    activity_id: int, start_local: str = "2026-01-05 08:00:00", **extra
) -> dict:
    return {
        "activityId": activity_id,
        "activityType": {"typeKey": "running"},
        "startTimeLocal": start_local,
        "startTimeGMT": "2026-01-05 16:00:00",
        "duration": 1800.0,
        "distance": 5000.0,
        "averageHR": 150.0,
        "maxHR": 170.0,
        "calories": 400.0,
        "elevationGain": 50.0,
        "elevationLoss": 50.0,
        **extra,
    }


def _seed_silver(tmp_path: Path, records: list[dict]) -> None:
    bronze_path = tmp_path / "bronze/activities/year=2026/month=01/day=05.json"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text(json.dumps(records))
    activities.rebuild()


def test_query_count(tmp_path):
    _seed_silver(tmp_path, [_make_activity(1), _make_activity(2)])
    df = query.query("SELECT COUNT(*) AS n FROM activities")
    assert df.height == 1
    assert df.item(0, "n") == 2


def test_query_group_by_year(tmp_path):
    _seed_silver(tmp_path, [_make_activity(1), _make_activity(2)])
    df = query.query("SELECT year, COUNT(*) AS n FROM activities GROUP BY year")
    assert df.height == 1
    assert df.item(0, "year") == 2026
    assert df.item(0, "n") == 2


def test_query_missing_silver_raises_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="No parquet files found"):
        query.query("SELECT COUNT(*) FROM activities")
