import json
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
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
        "startLatitude": 523255203,
        "startLongitude": -1073741824,
        **extra,
    }


def _write_bronze_day(tmp_path: Path, day_activities: list[dict]) -> str:
    path = tmp_path / "bronze/activities/year=2026/month=01/day=05.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(day_activities))
    return str(path)


def test_transform_dedup_by_activity_id(tmp_path):
    path = _write_bronze_day(
        tmp_path,
        [
            _make_activity(1, distance=100.0),
            _make_activity(1, distance=999.0),
            _make_activity(2, distance=200.0),
        ],
    )
    df = activities.transform([path])
    assert df.height == 2

    by_id = {row["activity_id"]: row for row in df.to_dicts()}
    assert by_id[1]["distance_m"] == 999.0
    assert by_id[2]["distance_m"] == 200.0


def test_transform_semicircle_conversion(tmp_path):
    path = _write_bronze_day(tmp_path, [_make_activity(1, startLatitude=523255203)])
    df = activities.transform([path])
    assert df.item(0, "start_lat") == pytest.approx(43.86, abs=0.01)


def test_transform_null_gps(tmp_path):
    activity = _make_activity(1)
    activity.pop("startLatitude")
    activity.pop("startLongitude")
    path = _write_bronze_day(tmp_path, [activity])
    df = activities.transform([path])
    assert df.item(0, "start_lat") is None
    assert df.item(0, "start_lon") is None


def test_transform_schema_types(tmp_path):
    path = _write_bronze_day(tmp_path, [_make_activity(1)])
    df = activities.transform([path])
    schema = dict(df.schema)
    assert schema["activity_id"] == pl.Int64
    assert schema["activity_type"] == pl.Utf8
    assert isinstance(schema["start_time_local"], pl.Datetime)
    assert isinstance(schema["start_time_utc"], pl.Datetime)
    assert schema["duration_sec"] == pl.Float64
    assert schema["distance_m"] == pl.Float64
    assert schema["avg_hr"] == pl.Float64
    assert schema["max_hr"] == pl.Float64
    assert schema["calories"] == pl.Float64
    assert schema["elevation_gain_m"] == pl.Float64
    assert schema["elevation_loss_m"] == pl.Float64
    assert schema["start_lat"] == pl.Float64
    assert schema["start_lon"] == pl.Float64
    assert schema["year"] == pl.Int32
    assert schema["month"] == pl.Int32


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

    partition_dir = tmp_path / "silver/activities/year=2026/month=1"
    parquet_files = list(partition_dir.glob("*.parquet"))
    assert parquet_files, f"expected parquet under {partition_dir}"

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/activities/**/*.parquet")
    result = con.sql(
        f"SELECT COUNT(*) AS n, MAX(year) AS y, MAX(month) AS m "
        f"FROM read_parquet('{pattern}', hive_partitioning=1)"
    ).fetchone()
    assert result == (2, 2026, 1)


def test_rebuild_no_bronze_returns_zero(tmp_path):
    assert activities.rebuild() == 0


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
    assert rows == [(1, 1)]
