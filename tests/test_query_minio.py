"""Opt-in MinIO integration tests for own_garmin.query S3 code paths.

Requires a running MinIO instance and the test bucket created by docker-compose.

    docker compose up -d minio minio-bootstrap

Then run with:

    OWN_GARMIN_RUN_MINIO_TESTS=1 \\
    AWS_ENDPOINT_URL_S3=http://localhost:9000 \\
    AWS_ACCESS_KEY_ID=minioadmin \\
    AWS_SECRET_ACCESS_KEY=minioadmin \\
    AWS_REGION=us-east-1 \\
    uv run pytest tests/test_query_minio.py -v
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

from own_garmin import paths, query, storage
from own_garmin.silver import activities

pytestmark = pytest.mark.skipif(
    os.environ.get("OWN_GARMIN_RUN_MINIO_TESTS") != "1",
    reason="set OWN_GARMIN_RUN_MINIO_TESTS=1 after starting MinIO",
)

_BUCKET = "own-garmin-test"


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


@pytest.fixture()
def minio_data_dir(monkeypatch):
    """Unique S3 data root per test run; cleaned up after the test completes."""
    run_id = uuid.uuid4().hex
    s3_root = f"s3://{_BUCKET}/query-{run_id}"

    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", s3_root)
    if not os.environ.get("AWS_ENDPOINT_URL_S3"):
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    if not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    if not os.environ.get("AWS_REGION"):
        monkeypatch.setenv("AWS_REGION", "us-east-1")

    yield s3_root

    storage.rmtree(s3_root)


def test_query_activities_count_via_minio(minio_data_dir):
    """query() returns correct row count when silver data lives in MinIO."""
    import datetime

    bronze_path = paths.bronze_path("activities", datetime.date(2026, 1, 5))
    storage.write_text(bronze_path, json.dumps([_make_activity(1), _make_activity(2)]))

    activities.rebuild()

    df = query.query("SELECT COUNT(*) AS n FROM activities")
    assert df.item(0, "n") == 2
