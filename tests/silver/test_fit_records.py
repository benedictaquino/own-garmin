import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest
from garmin_fit_sdk import Encoder, Profile

from own_garmin.silver import fit_records

_UTC = datetime.timezone.utc


def _build_fit_bytes(records: list[dict]) -> bytes:
    enc = Encoder()
    enc.write_mesg(
        {
            "mesg_num": Profile["mesg_num"]["FILE_ID"],
            "type": "activity",
            "manufacturer": 1,
            "product": 1,
            "time_created": datetime.datetime(2026, 1, 5, 8, 0, 0, tzinfo=_UTC),
            "serial_number": 1,
        }
    )
    for rec in records:
        enc.write_mesg({"mesg_num": Profile["mesg_num"]["RECORD"], **rec})
    return enc.close()


def _write_fit_file(
    tmp_path: Path,
    activity_id: int,
    records: list[dict],
    day: str = "2026/01/05",
) -> str:
    fit_bytes = _build_fit_bytes(records)
    year, month, dd = day.split("/")
    zip_dir = tmp_path / f"bronze/fit/year={year}/month={month}/day={dd}"
    zip_dir.mkdir(parents=True, exist_ok=True)
    zip_path = zip_dir / f"{activity_id}.zip"
    zip_path.write_bytes(fit_bytes)
    return str(zip_path)


def _make_record(seconds: int, **overrides) -> dict:
    base_ts = datetime.datetime(2026, 1, 5, 8, 0, 0, tzinfo=_UTC)
    record = {
        "timestamp": base_ts + datetime.timedelta(seconds=seconds),
        "heart_rate": 120 + seconds,
        "cadence": 80,
        "speed": 3.5,
        "power": 200,
        "distance": float(seconds) * 3.5,
        "altitude": 100.0,
        "position_lat": 523255203,
        "position_long": -1073741824,
    }
    record.update(overrides)
    return record


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def test_transform_extracts_records(tmp_path):
    records = [_make_record(i) for i in range(10)]
    zip_path = _write_fit_file(tmp_path, 100, records)

    df = fit_records.transform([zip_path])
    assert df.height == 10
    assert set(df.columns) == {
        "activity_id",
        "timestamp",
        "heart_rate",
        "cadence",
        "speed",
        "power",
        "distance",
        "altitude",
        "position_lat",
        "position_lon",
        "year",
        "month",
    }
    assert (df["activity_id"] == 100).all()


def test_transform_semicircle_conversion(tmp_path):
    zip_path = _write_fit_file(tmp_path, 1, [_make_record(0, position_lat=523255203)])
    df = fit_records.transform([zip_path])
    assert df.item(0, "position_lat") == pytest.approx(43.86, abs=0.01)


def test_transform_null_power(tmp_path):
    rec = _make_record(0)
    rec.pop("power")
    zip_path = _write_fit_file(tmp_path, 2, [rec])
    df = fit_records.transform([zip_path])
    assert df.item(0, "power") is None
    assert df.item(0, "heart_rate") == 120


def test_transform_corrupt_fit_skipped(tmp_path, caplog):
    zip_dir = tmp_path / "bronze/fit/year=2026/month=01/day=05"
    zip_dir.mkdir(parents=True, exist_ok=True)
    bad_fit = zip_dir / "999.zip"
    bad_fit.write_bytes(b"not a fit file")

    with caplog.at_level("WARNING"):
        df = fit_records.transform([str(bad_fit)])
    assert df.height == 0
    assert any("999" in msg for msg in caplog.messages)


def test_transform_dedup_by_activity_timestamp(tmp_path):
    records = [_make_record(0, heart_rate=120), _make_record(0, heart_rate=180)]
    # two FIT files for the same activity_id containing overlapping timestamps
    zip1 = _write_fit_file(tmp_path, 50, [records[0]])
    # rewrite at a different path but same activity_id — simulate bronze re-ingest
    zip2 = _write_fit_file(tmp_path, 50, [records[1]], day="2026/02/01")

    df = fit_records.transform([zip1, zip2])
    assert df.height == 1
    assert df.item(0, "heart_rate") == 180


def test_transform_empty_returns_typed_frame():
    df = fit_records.transform([])
    assert df.height == 0
    assert "activity_id" in df.columns
    assert df.schema["position_lat"] == pl.Float64


def test_rebuild_writes_partitioned_parquet(tmp_path):
    records = [_make_record(i) for i in range(5)]
    _write_fit_file(tmp_path, 42, records)

    count = fit_records.rebuild()
    assert count == 5

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/fit_records/**/*.parquet")
    result = con.sql(
        f"SELECT COUNT(*) AS n FROM read_parquet('{pattern}', hive_partitioning=1)"
    ).fetchone()
    assert result == (5,)


def test_rebuild_no_bronze_returns_zero(tmp_path):
    assert fit_records.rebuild() == 0


def test_rebuild_with_empty_bronze_clears_existing_silver(tmp_path):
    _write_fit_file(tmp_path, 7, [_make_record(0)])
    assert fit_records.rebuild() == 1
    silver_dir = tmp_path / "silver/fit_records"
    assert list(silver_dir.rglob("*.parquet")), "expected silver parquet seeded"

    for zip_file in (tmp_path / "bronze").rglob("*.zip"):
        zip_file.unlink()

    assert fit_records.rebuild() == 0
    assert not list(silver_dir.rglob("*.parquet"))


def test_rebuild_clears_stale_partitions(tmp_path):
    jan_records = [_make_record(0)]
    feb_base = datetime.datetime(2026, 2, 10, 9, 0, 0, tzinfo=_UTC)
    feb_records = [
        {
            "timestamp": feb_base,
            "heart_rate": 130,
            "cadence": 80,
            "speed": 3.0,
            "power": 150,
            "distance": 0.0,
            "altitude": 50.0,
            "position_lat": 523255203,
            "position_long": -1073741824,
        }
    ]

    jan_zip = Path(_write_fit_file(tmp_path, 10, jan_records, day="2026/01/05"))
    feb_zip = Path(_write_fit_file(tmp_path, 20, feb_records, day="2026/02/10"))

    assert fit_records.rebuild() == 2

    feb_zip.unlink()
    assert fit_records.rebuild() == 1

    con = duckdb.connect(":memory:")
    pattern = str(tmp_path / "silver/fit_records/**/*.parquet")
    rows = con.sql(
        f"SELECT activity_id, month "
        f"FROM read_parquet('{pattern}', hive_partitioning=1) "
        f"ORDER BY activity_id"
    ).fetchall()
    assert rows == [(10, 1)]
    assert jan_zip.exists()
