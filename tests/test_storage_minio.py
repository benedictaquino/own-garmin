"""Opt-in MinIO integration tests for own_garmin.storage S3 code paths.

Requires a running MinIO instance and the test bucket created by docker-compose.

    docker compose up -d minio minio-bootstrap

Then run with:

    OWN_GARMIN_RUN_MINIO_TESTS=1 \\
    AWS_ENDPOINT_URL_S3=http://localhost:9000 \\
    AWS_ACCESS_KEY_ID=minioadmin \\
    AWS_SECRET_ACCESS_KEY=minioadmin \\
    AWS_REGION=us-east-1 \\
    uv run pytest tests/test_storage_minio.py -v
"""

from __future__ import annotations

import os
import uuid

import polars as pl
import pytest

import own_garmin.storage as storage

pytestmark = pytest.mark.skipif(
    os.environ.get("OWN_GARMIN_RUN_MINIO_TESTS") != "1",
    reason="set OWN_GARMIN_RUN_MINIO_TESTS=1 after starting MinIO",
)

_BUCKET = "own-garmin-test"


@pytest.fixture()
def prefix():
    """Unique S3 prefix per test; cleaned up after the test completes."""
    run_id = uuid.uuid4().hex
    base = f"s3://{_BUCKET}/pytest-{run_id}"
    yield base
    storage.rmtree(base)


# ---------------------------------------------------------------------------
# write_bytes / read_bytes round-trip
# ---------------------------------------------------------------------------


def test_write_read_bytes_roundtrip(prefix):
    path = f"{prefix}/data.bin"
    payload = b"\x00\xde\xad\xbe\xef\xff"
    storage.write_bytes(path, payload)
    assert storage.read_bytes(path) == payload


# ---------------------------------------------------------------------------
# list_files with glob
# ---------------------------------------------------------------------------


def test_list_files_glob(prefix):
    keys = ["a.json", "b.json", "c.txt"]
    for key in keys:
        storage.write_bytes(f"{prefix}/{key}", b"data")

    results = storage.list_files(f"{prefix}/*.json")

    assert len(results) == 2
    assert all(r.endswith(".json") for r in results)
    assert f"{prefix}/a.json" in results
    assert f"{prefix}/b.json" in results
    assert f"{prefix}/c.txt" not in results


# ---------------------------------------------------------------------------
# rmtree cleanup
# ---------------------------------------------------------------------------


def test_rmtree(prefix):
    # Write a few objects under the prefix
    for i in range(3):
        storage.write_bytes(f"{prefix}/sub/file{i}.bin", b"x")

    storage.rmtree(f"{prefix}/sub")

    # Nothing should remain under the sub prefix
    remaining = storage.list_files(f"{prefix}/sub/**/*")
    assert remaining == []


# ---------------------------------------------------------------------------
# write_partitioned_parquet produces expected hive keys
# ---------------------------------------------------------------------------


def test_write_partitioned_parquet_hive_keys(prefix):
    df = pl.DataFrame(
        {
            "year": ["2025", "2025", "2026"],
            "month": ["01", "01", "03"],
            "value": [1, 2, 3],
        }
    )
    target = f"{prefix}/silver/activities"
    storage.write_partitioned_parquet(df, target, partition_by=["year", "month"])

    results = storage.list_files(f"{target}/**/*.parquet")

    assert len(results) == 2
    keys = " ".join(results)
    assert "year=2025" in keys
    assert "month=01" in keys
    assert "year=2026" in keys
    assert "month=03" in keys
    assert all(r.endswith("/data.parquet") for r in results)
