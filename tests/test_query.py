import json
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from own_garmin import paths, query
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


def _seed_silver_activities(records: list[dict]) -> None:
    bronze_path = (
        Path(paths.data_root()) / "bronze/activities/year=2026/month=01/day=05.json"
    )
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text(json.dumps(records))
    activities.rebuild()


def _seed_silver_fit_records(rows: list[dict]) -> None:
    df = pl.DataFrame(rows)
    target = Path(paths.silver_path("fit_records"))
    target.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(target), partition_by=["year", "month"])


def test_query_count(tmp_path):
    _seed_silver_activities([_make_activity(1), _make_activity(2)])
    df = query.query("SELECT COUNT(*) AS n FROM activities")
    assert df.height == 1
    assert df.item(0, "n") == 2


def test_query_group_by_year(tmp_path):
    _seed_silver_activities([_make_activity(1), _make_activity(2)])
    df = query.query("SELECT year, COUNT(*) AS n FROM activities GROUP BY year")
    assert df.height == 1
    assert df.item(0, "year") == 2026
    assert df.item(0, "n") == 2


def test_query_fit_records_view_registered(tmp_path):
    _seed_silver_fit_records(
        [
            {
                "activity_id": 1,
                "timestamp": datetime(2026, 1, 5, 8, 0, 0),
                "heart_rate": 140,
                "year": 2026,
                "month": 1,
            },
            {
                "activity_id": 1,
                "timestamp": datetime(2026, 1, 5, 8, 0, 1),
                "heart_rate": 142,
                "year": 2026,
                "month": 1,
            },
        ]
    )
    df = query.query("SELECT COUNT(*) AS n FROM fit_records")
    assert df.item(0, "n") == 2


def test_query_invalid_sql_raises(tmp_path):
    _seed_silver_activities([_make_activity(1)])
    with pytest.raises(duckdb.Error):
        query.query("SELECT * FROM nonexistent_table")


def test_query_missing_silver_raises_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="No silver parquet found"):
        query.query("SELECT COUNT(*) FROM activities")
