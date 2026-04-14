from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from own_garmin.bronze import fit


@pytest.fixture(autouse=True)
def set_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", str(tmp_path))


def make_activity(activity_id: int, start_time: str) -> dict:
    return {"activityId": activity_id, "startTimeLocal": start_time}


def test_ingest_writes_zip(tmp_path):
    client = MagicMock()
    client.download_fit.return_value = b"PK\x03\x04fake_zip_content"

    with patch("time.sleep"):
        count = fit.ingest(client, [make_activity(42, "2026-01-05 08:00:00")])

    path = Path(tmp_path / "bronze/fit/year=2026/month=01/day=05/42.zip")
    assert path.exists()
    assert path.read_bytes() == b"PK\x03\x04fake_zip_content"
    assert count == 1


def test_ingest_skips_existing_zip(tmp_path):
    client = MagicMock()

    path = Path(tmp_path / "bronze/fit/year=2026/month=01/day=05/42.zip")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"existing")

    with patch("time.sleep"):
        count = fit.ingest(client, [make_activity(42, "2026-01-05 08:00:00")])

    client.download_fit.assert_not_called()
    assert count == 0


def test_ingest_returns_newly_written_count():
    client = MagicMock()
    client.download_fit.return_value = b"zip"

    with patch("time.sleep"):
        count = fit.ingest(
            client,
            [
                make_activity(1, "2026-01-05 08:00:00"),
                make_activity(2, "2026-01-06 09:00:00"),
                make_activity(3, "2026-01-06 10:00:00"),
            ],
        )

    assert count == 3


def test_ingest_sleeps_between_downloads():
    client = MagicMock()
    client.download_fit.return_value = b"zip"

    with patch("own_garmin.bronze.fit.time") as mock_time:
        fit.ingest(
            client,
            [
                make_activity(1, "2026-01-05 08:00:00"),
                make_activity(2, "2026-01-05 09:00:00"),
                make_activity(3, "2026-01-05 10:00:00"),
            ],
            sleep_sec=0.3,
        )

    # 3 downloads → 2 sleeps (no sleep before first)
    assert mock_time.sleep.call_count == 2
    mock_time.sleep.assert_called_with(0.3)


def test_ingest_skips_missing_activity_id():
    client = MagicMock()

    with patch("time.sleep"):
        count = fit.ingest(
            client,
            [{"startTimeLocal": "2026-01-05 08:00:00"}],  # no activityId
        )

    client.download_fit.assert_not_called()
    assert count == 0
