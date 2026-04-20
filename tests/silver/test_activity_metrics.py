import datetime
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from own_garmin.silver import activity_metrics

_FIXTURE = (
    Path(__file__).parent.parent
    / "fixtures/activity_details/year=2026/month=01/day=15.json"
)


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def _write_fixture(tmp_path: Path, payload: list[dict], day: str = "2026/01/15") -> str:
    year, month, dd = day.split("/")
    dest_dir = tmp_path / f"bronze/activity_details/year={year}/month={month}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"day={dd}.json"
    dest.write_text(json.dumps(payload))
    return str(dest)


def _load_fixture() -> list[dict]:
    return json.loads(_FIXTURE.read_text())


def _minimal_descriptor(key: str, idx: int, unit_key: str = "ms") -> dict:
    return {
        "metricsIndex": idx,
        "key": key,
        "unit": {"id": idx, "key": unit_key, "factor": 1.0},
    }


def _minimal_activity(
    activity_id: int,
    descriptors: list[dict],
    metrics_rows: list[list],
) -> dict:
    return {
        "activityId": activity_id,
        "measurementCount": len(descriptors),
        "metricsCount": len(metrics_rows),
        "totalMetricsCount": len(metrics_rows),
        "detailsAvailable": True,
        "pendingData": None,
        "heartRateDTOs": None,
        "metricDescriptors": descriptors,
        "activityDetailMetrics": [{"metrics": row} for row in metrics_rows],
        "geoPolylineDTO": None,
    }


# ---------------------------------------------------------------------------
# transform — schema and row count
# ---------------------------------------------------------------------------


def test_transform_returns_expected_row_count_and_dtypes(tmp_path):
    path = _write_fixture(tmp_path, _load_fixture())
    df = activity_metrics.transform([path])

    # Activity 2001 has 3 rows, activity 2002 has 4 rows
    assert df.height == 7

    expected_schema = activity_metrics._OUTPUT_SCHEMA
    for col, dtype in expected_schema.items():
        assert col in df.columns, f"missing column: {col}"
        assert df.schema[col] == dtype, (
            f"column {col}: expected {dtype}, got {df.schema[col]}"
        )


def test_transform_nulls_for_absent_metrics(tmp_path):
    """Activity 2002 has only timestamp/sumDuration/directHeartRate; rest null."""
    path = _write_fixture(tmp_path, _load_fixture())
    df = activity_metrics.transform([path])

    sparse = df.filter(pl.col("activity_id") == 2002)
    assert sparse.height == 4

    # speed_mps is not in activity 2002's metricDescriptors
    assert sparse["speed_mps"].is_null().all()
    assert sparse["position_lat"].is_null().all()
    # heart_rate should be present
    assert not sparse["heart_rate"].is_null().any()


# ---------------------------------------------------------------------------
# transform — coordinates are decimal degrees, no semicircle conversion
# ---------------------------------------------------------------------------


def test_transform_coords_are_decimal_degrees(tmp_path):
    """Coordinates must pass through as-is (decimal degrees, not semicircles)."""
    path = _write_fixture(tmp_path, _load_fixture())
    df = activity_metrics.transform([path])

    rows_with_lat = df.filter(pl.col("position_lat").is_not_null())
    assert rows_with_lat.height > 0

    # The fixture uses ~40.7 degrees (NYC area).  If semicircle conversion were
    # applied, the value would be far outside this range.
    lat_vals = rows_with_lat["position_lat"].to_list()
    for lat in lat_vals:
        assert 40.0 < lat < 41.0, (
            f"lat {lat} looks wrong — semicircle conversion applied?"
        )


# ---------------------------------------------------------------------------
# transform — timestamp round-trips from epoch-ms
# ---------------------------------------------------------------------------


def test_transform_timestamp_roundtrip(tmp_path):
    """Epoch-ms 1452826800000 -> 2016-01-15 03:00:00 UTC."""
    path = _write_fixture(tmp_path, _load_fixture())
    df = activity_metrics.transform([path])

    ts_col = df.filter(pl.col("activity_id") == 2001).sort("timestamp")["timestamp"]
    first_ts = ts_col[0]
    # 1452826800000 ms = 2016-01-15 03:00:00 UTC
    expected = datetime.datetime(2016, 1, 15, 3, 0, 0)
    assert first_ts == expected


# ---------------------------------------------------------------------------
# transform — dedup on (activity_id, timestamp)
# ---------------------------------------------------------------------------


def test_transform_dedup_keeps_last(tmp_path):
    """When two files have same (activity_id, timestamp), the last row wins."""
    descs = [
        _minimal_descriptor("directTimestamp", 0),
        _minimal_descriptor("directHeartRate", 1, unit_key="bpm"),
    ]
    payload1 = [_minimal_activity(3000, descs, [[1452826800000.0, 100.0]])]
    payload2 = [_minimal_activity(3000, descs, [[1452826800000.0, 180.0]])]

    path1 = _write_fixture(tmp_path, payload1, day="2026/01/15")
    path2 = _write_fixture(tmp_path, payload2, day="2026/01/16")

    df = activity_metrics.transform([path1, path2])
    assert df.height == 1
    assert df.item(0, "heart_rate") == pytest.approx(180.0)


# ---------------------------------------------------------------------------
# transform — unmapped metric keys do not raise
# ---------------------------------------------------------------------------


def test_transform_unmapped_metric_key_does_not_raise(tmp_path, caplog):
    """A metricDescriptors key absent from the canonical map is silently dropped."""
    descs = [
        _minimal_descriptor("directTimestamp", 0),
        _minimal_descriptor("unknownFutureGarminMetric", 1, unit_key="unknown"),
    ]
    payload = [_minimal_activity(4000, descs, [[1452826800000.0, 42.0]])]
    path = _write_fixture(tmp_path, payload)

    with caplog.at_level("DEBUG"):
        df = activity_metrics.transform([path])

    assert df.height == 1
    assert any("unknownFutureGarminMetric" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# transform — empty input returns typed empty frame
# ---------------------------------------------------------------------------


def test_transform_empty_input_returns_typed_empty_frame():
    df = activity_metrics.transform([])
    assert df.height == 0
    assert "activity_id" in df.columns
    assert df.schema["timestamp"] == pl.Datetime("ms")
    assert df.schema["position_lat"] == pl.Float64
    assert df.schema["year"] == pl.Int32


# ---------------------------------------------------------------------------
# rebuild — writes partitioned parquet readable by DuckDB
# ---------------------------------------------------------------------------


def test_rebuild_writes_partitioned_parquet(tmp_path):
    src = _FIXTURE
    dest_dir = tmp_path / "bronze/activity_details/year=2026/month=01"
    dest_dir.mkdir(parents=True, exist_ok=True)
    (dest_dir / "day=15.json").write_bytes(src.read_bytes())

    count = activity_metrics.rebuild()
    assert count == 7

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/activity_metrics/**/*.parquet")
    result = con.sql(
        f"SELECT COUNT(*) AS n FROM read_parquet('{pattern}', hive_partitioning=1)"
    ).fetchone()
    assert result == (7,)


# ---------------------------------------------------------------------------
# rebuild — stale partition removed when bronze day is deleted
# ---------------------------------------------------------------------------


def test_rebuild_stale_partition_removed(tmp_path):
    """Re-running rebuild after removing a bronze file drops its rows from silver."""
    payload_jan = _load_fixture()  # 7 rows total

    descs = [
        _minimal_descriptor("directTimestamp", 0),
        _minimal_descriptor("directHeartRate", 1, unit_key="bpm"),
    ]
    # 1455321600000 = 2016-02-13 00:00:00 UTC
    payload_feb = [_minimal_activity(5000, descs, [[1455321600000.0, 130.0]])]

    jan_dir = tmp_path / "bronze/activity_details/year=2026/month=01"
    jan_dir.mkdir(parents=True, exist_ok=True)
    jan_file = jan_dir / "day=15.json"
    jan_file.write_text(json.dumps(payload_jan))

    feb_dir = tmp_path / "bronze/activity_details/year=2026/month=02"
    feb_dir.mkdir(parents=True, exist_ok=True)
    feb_file = feb_dir / "day=13.json"
    feb_file.write_text(json.dumps(payload_feb))

    assert activity_metrics.rebuild() == 8

    # Remove the Feb bronze file and rebuild — silver should only have Jan data
    feb_file.unlink()
    count = activity_metrics.rebuild()
    assert count == 7

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/activity_metrics/**/*.parquet")
    rows = con.sql(
        f"SELECT DISTINCT activity_id"
        f" FROM read_parquet('{pattern}', hive_partitioning=1)"
    ).fetchall()
    ids = {r[0] for r in rows}
    assert 5000 not in ids


# ---------------------------------------------------------------------------
# rebuild — empty bronze returns 0 and no parquet remains
# ---------------------------------------------------------------------------


def test_rebuild_empty_bronze_returns_zero_and_no_parquet(tmp_path):
    # First seed with data
    src = _FIXTURE
    dest_dir = tmp_path / "bronze/activity_details/year=2026/month=01"
    dest_dir.mkdir(parents=True, exist_ok=True)
    bronze_file = dest_dir / "day=15.json"
    bronze_file.write_bytes(src.read_bytes())

    assert activity_metrics.rebuild() == 7

    # Now remove all bronze and rebuild
    bronze_file.unlink()
    assert activity_metrics.rebuild() == 0

    silver_dir = tmp_path / "silver/activity_metrics"
    parquet_files = list(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []
    assert not parquet_files
