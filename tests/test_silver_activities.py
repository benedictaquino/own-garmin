from pathlib import Path

import polars as pl
import pytest

from own_garmin.silver.activities import transform

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "activities"
DAY_15 = str(FIXTURE_DIR / "year=2026" / "month=01" / "day=15.json")
DAY_16 = str(FIXTURE_DIR / "year=2026" / "month=01" / "day=16.json")


def test_transform_schema():
    df = transform([DAY_15, DAY_16])
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


def test_transform_dedup():
    df = transform([DAY_15, DAY_16])
    assert df.height == 3
    assert set(df["activity_id"].to_list()) == {1001, 1002, 1003}

    rows_1001 = df.filter(pl.col("activity_id") == 1001).to_dicts()
    assert len(rows_1001) == 1
    assert rows_1001[0]["distance_m"] == 9999.0


def test_transform_lat_lon_passthrough():
    df = transform([DAY_15])
    row = df.filter(pl.col("activity_id") == 1001).to_dicts()[0]
    assert row["start_lat"] == pytest.approx(43.8614, abs=1e-4)


def test_transform_null_gps():
    df = transform([DAY_16])
    row = df.filter(pl.col("activity_id") == 1003).to_dicts()[0]
    assert row["start_lat"] is None
    assert row["start_lon"] is None
