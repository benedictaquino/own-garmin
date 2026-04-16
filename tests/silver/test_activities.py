import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from own_garmin.silver import activities


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def _make_activity(
    activity_id: int,
    start_local: str = "2026-01-05 08:00:00",
    start_gmt: str = "2026-01-05 16:00:00",
    **extra,
) -> dict:
    return {
        "activityId": activity_id,
        "activityType": {"typeKey": "running"},
        "startTimeLocal": start_local,
        "startTimeGMT": start_gmt,
        "duration": 1800.0,
        "distance": 5000.0,
        "averageHR": 150.0,
        "maxHR": 170.0,
        "calories": 400.0,
        "elevationGain": 50.0,
        "elevationLoss": 50.0,
        "startLatitude": 43.86,
        "startLongitude": -79.37,
        **extra,
    }


def _write_bronze_day(tmp_path: Path, day_activities: list[dict]) -> str:
    path = tmp_path / "bronze/activities/year=2026/month=01/day=05.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(day_activities))
    return str(path)


def test_transform_derives_year_month(tmp_path):
    path = _write_bronze_day(tmp_path, [_make_activity(1)])
    df = activities.transform([path])
    assert df.item(0, "year") == 2026
    assert df.item(0, "month") == 1
    assert df.item(0, "start_time_local") == datetime(2026, 1, 5, 8, 0, 0)


def test_transform_empty_paths_returns_empty_typed_frame():
    df = activities.transform([])
    assert df.height == 0
    assert "activity_id" in df.columns
    assert "year" in df.columns


def test_rebuild_writes_partitioned_parquet(tmp_path):
    _write_bronze_day(tmp_path, [_make_activity(1), _make_activity(2)])

    count = activities.rebuild()
    assert count == 2

    partition_dir = tmp_path / "silver/activities/year=2026/month=01"
    parquet_files = list(partition_dir.glob("*.parquet"))
    assert parquet_files, f"expected parquet under {partition_dir}"

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/activities/**/*.parquet")
    result = con.sql(
        f"SELECT COUNT(*) AS n, MAX(year) AS y, MAX(month) AS m "
        f"FROM read_parquet('{pattern}', hive_partitioning=1)"
    ).fetchone()
    assert result == (2, 2026, "01")


def test_rebuild_no_bronze_returns_zero(tmp_path):
    assert activities.rebuild() == 0


def test_rebuild_with_empty_bronze_clears_existing_silver(tmp_path):
    _write_bronze_day(tmp_path, [_make_activity(1)])
    assert activities.rebuild() == 1
    silver_dir = tmp_path / "silver/activities"
    assert list(silver_dir.rglob("*.parquet")), "expected silver parquet seeded"

    for bronze_file in (tmp_path / "bronze").rglob("*.json"):
        bronze_file.unlink()

    assert activities.rebuild() == 0
    assert not list(silver_dir.rglob("*.parquet"))


def test_rebuild_clears_stale_partitions(tmp_path):
    jan_activity = _make_activity(
        1, start_local="2026-01-05 08:00:00", start_gmt="2026-01-05 16:00:00"
    )
    feb_activity = _make_activity(
        2, start_local="2026-02-10 09:00:00", start_gmt="2026-02-10 17:00:00"
    )

    jan_path = tmp_path / "bronze/activities/year=2026/month=01/day=05.json"
    jan_path.parent.mkdir(parents=True, exist_ok=True)
    jan_path.write_text(json.dumps([jan_activity]))

    feb_path = tmp_path / "bronze/activities/year=2026/month=02/day=10.json"
    feb_path.parent.mkdir(parents=True, exist_ok=True)
    feb_path.write_text(json.dumps([feb_activity]))

    assert activities.rebuild() == 2

    feb_path.unlink()
    assert activities.rebuild() == 1

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/activities/**/*.parquet")
    rows = con.sql(
        f"SELECT activity_id, month "
        f"FROM read_parquet('{pattern}', hive_partitioning=1) "
        f"ORDER BY activity_id"
    ).fetchall()
    assert rows == [(1, "01")]  # month is zero-padded string in hive partitions
