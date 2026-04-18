"""Unit tests for own_garmin.storage — local and S3 code paths."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import polars as pl
import pytest

import own_garmin.storage as storage

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_s3_client(mocker):
    """Inject a fake boto3 + botocore into sys.modules and return the mock S3 client.

    storage.py uses lazy ``import boto3`` inside each function body, so patching
    sys.modules is the right seam — there is no module-level boto3 attribute.
    """
    mock_s3 = MagicMock()

    # Stub boto3 module so ``import boto3`` inside storage.py resolves
    fake_boto3 = MagicMock()
    fake_boto3.client.return_value = mock_s3
    mocker.patch.dict(sys.modules, {"boto3": fake_boto3})

    # Stub botocore.exceptions so ``from botocore.exceptions import ClientError``
    # inside storage.py resolves to a real ClientError we can construct
    import importlib

    try:
        botocore_exceptions = importlib.import_module("botocore.exceptions")
    except ModuleNotFoundError:
        # botocore not installed either — create a minimal stub
        from unittest.mock import MagicMock as MM

        botocore_exceptions = MM()
        botocore_exceptions.ClientError = _StubClientError

    mocker.patch.dict(
        sys.modules,
        {
            "botocore": MagicMock(),
            "botocore.exceptions": botocore_exceptions,
        },
    )

    return mock_s3


class _StubClientError(Exception):
    """Minimal stand-in for botocore.exceptions.ClientError when botocore is absent."""

    def __init__(self, error_response: dict, operation_name: str) -> None:
        self.response = error_response
        super().__init__(str(error_response))


# ---------------------------------------------------------------------------
# is_s3
# ---------------------------------------------------------------------------


def test_is_s3_true():
    assert storage.is_s3("s3://my-bucket/some/key") is True


def test_is_s3_false_local():
    assert storage.is_s3("./data") is False


def test_is_s3_false_absolute():
    assert storage.is_s3("/tmp/foo") is False


# ---------------------------------------------------------------------------
# read_text / write_text
# ---------------------------------------------------------------------------


def test_write_read_text_roundtrip(tmp_path):
    p = str(tmp_path / "file.txt")
    storage.write_text(p, "hello world")
    assert storage.read_text(p) == "hello world"


def test_write_text_creates_parent_dirs(tmp_path):
    p = str(tmp_path / "a" / "b" / "c" / "file.txt")
    storage.write_text(p, "nested")
    assert storage.read_text(p) == "nested"


def test_write_text_utf8(tmp_path):
    p = str(tmp_path / "unicode.txt")
    content = "garmin \u00e9l\u00e8ve \U0001f3c3"
    storage.write_text(p, content)
    assert storage.read_text(p) == content


# ---------------------------------------------------------------------------
# read_bytes / write_bytes
# ---------------------------------------------------------------------------


def test_write_read_bytes_roundtrip(tmp_path):
    p = str(tmp_path / "data.bin")
    data = b"\x00\x01\x02\xff"
    storage.write_bytes(p, data)
    assert storage.read_bytes(p) == data


def test_write_bytes_creates_parent_dirs(tmp_path):
    p = str(tmp_path / "deep" / "nested" / "data.bin")
    storage.write_bytes(p, b"bytes")
    assert storage.read_bytes(p) == b"bytes"


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


def test_exists_false_for_missing(tmp_path):
    assert storage.exists(str(tmp_path / "ghost.txt")) is False


def test_exists_true_after_write(tmp_path):
    p = str(tmp_path / "present.txt")
    storage.write_text(p, "here")
    assert storage.exists(p) is True


# ---------------------------------------------------------------------------
# list_files
# ---------------------------------------------------------------------------


def test_list_files_finds_expected(tmp_path):
    (tmp_path / "a.json").write_text("{}")
    (tmp_path / "b.json").write_text("{}")
    (tmp_path / "skip.txt").write_text("nope")

    results = storage.list_files(str(tmp_path / "*.json"))
    assert len(results) == 2
    assert all(r.endswith(".json") for r in results)


def test_list_files_recursive(tmp_path):
    sub = tmp_path / "sub"
    sub.mkdir()
    (tmp_path / "top.parquet").write_bytes(b"")
    (sub / "nested.parquet").write_bytes(b"")

    results = storage.list_files(str(tmp_path / "**/*.parquet"))
    assert len(results) == 2


def test_list_files_empty_when_no_match(tmp_path):
    results = storage.list_files(str(tmp_path / "*.parquet"))
    assert results == []


def test_list_files_is_sorted(tmp_path):
    for name in ["c.json", "a.json", "b.json"]:
        (tmp_path / name).write_text("{}")
    results = storage.list_files(str(tmp_path / "*.json"))
    assert results == sorted(results)


# ---------------------------------------------------------------------------
# rmtree
# ---------------------------------------------------------------------------


def test_rmtree_removes_directory(tmp_path):
    d = tmp_path / "tree"
    d.mkdir()
    (d / "file.txt").write_text("data")
    (d / "sub").mkdir()
    (d / "sub" / "nested.txt").write_text("nested")

    storage.rmtree(str(d))
    assert not d.exists()


def test_rmtree_ignores_missing(tmp_path):
    # Should not raise
    storage.rmtree(str(tmp_path / "does_not_exist"))


# ---------------------------------------------------------------------------
# write_partitioned_parquet
# ---------------------------------------------------------------------------


def test_write_partitioned_parquet_hive_structure(tmp_path):
    df = pl.DataFrame(
        {
            "year": ["2025", "2025", "2026"],
            "month": ["01", "01", "03"],
            "value": [1, 2, 3],
        }
    )
    target = str(tmp_path / "silver" / "activities")
    storage.write_partitioned_parquet(df, target, partition_by=["year", "month"])

    # Verify hive directory structure exists
    import os

    written_dirs = set()
    for dirpath, _, filenames in os.walk(target):
        if filenames:
            rel = os.path.relpath(dirpath, target)
            written_dirs.add(rel)

    assert "year=2025/month=01" in written_dirs
    assert "year=2026/month=03" in written_dirs


def test_write_partitioned_parquet_readable(tmp_path):
    df = pl.DataFrame(
        {
            "year": ["2025"],
            "month": ["06"],
            "activity_id": [42],
            "distance_m": [1000.0],
        }
    )
    target = str(tmp_path / "silver" / "activities")
    storage.write_partitioned_parquet(df, target, partition_by=["year", "month"])

    # Read back all parquet files and confirm data is preserved
    import glob

    pattern = str(tmp_path / "silver" / "activities" / "**/*.parquet")
    files = glob.glob(pattern, recursive=True)
    assert len(files) > 0
    result = pl.read_parquet(files[0])
    assert result.filter(pl.col("activity_id") == 42).height == 1


def test_write_partitioned_parquet_creates_target_dir(tmp_path):
    df = pl.DataFrame({"year": ["2025"], "month": ["01"], "val": [99]})
    target = str(tmp_path / "new" / "deep" / "dir")
    storage.write_partitioned_parquet(df, target, partition_by=["year", "month"])
    import os

    assert os.path.isdir(target)


# ---------------------------------------------------------------------------
# S3 — helpers
# ---------------------------------------------------------------------------


def _make_client_error(code: str) -> Exception:
    """Build a ClientError-like exception with the given error code.

    Uses _StubClientError so it works whether or not botocore is installed.
    storage.py catches ``exc.response["Error"]["Code"]``, which our stub exposes.
    """
    return _StubClientError({"Error": {"Code": code, "Message": "test"}}, "HeadObject")


def _make_paginator(pages: list[list[str]]) -> MagicMock:
    """Build a mock paginator that yields *pages* of S3 object keys."""
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": key} for key in page]} for page in pages
    ]
    return paginator


# ---------------------------------------------------------------------------
# S3 — read_text / write_text
# ---------------------------------------------------------------------------


def test_s3_read_text(mock_s3_client):
    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"hello s3")
    }

    result = storage.read_text("s3://my-bucket/path/to/file.txt")

    assert result == "hello s3"
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="my-bucket", Key="path/to/file.txt"
    )


def test_s3_write_text(mock_s3_client):
    storage.write_text("s3://my-bucket/path/to/file.txt", "hello s3")

    mock_s3_client.put_object.assert_called_once_with(
        Bucket="my-bucket", Key="path/to/file.txt", Body=b"hello s3"
    )


# ---------------------------------------------------------------------------
# S3 — exists
# ---------------------------------------------------------------------------


def test_s3_exists_exact_hit(mock_s3_client):
    mock_s3_client.head_object.return_value = {}

    assert (
        storage.exists("s3://my-bucket/silver/activities/year=2025/data.parquet")
        is True
    )


def test_s3_exists_prefix_hit(mock_s3_client):
    """head_object 404 → prefix probe finds objects → True."""
    mock_s3_client.head_object.side_effect = _make_client_error("404")
    mock_s3_client.list_objects_v2.return_value = {"KeyCount": 3}

    assert storage.exists("s3://my-bucket/silver/activities") is True
    mock_s3_client.list_objects_v2.assert_called_once_with(
        Bucket="my-bucket", Prefix="silver/activities/", MaxKeys=1
    )


def test_s3_exists_prefix_miss(mock_s3_client):
    """head_object NoSuchKey → prefix probe finds no objects → False."""
    mock_s3_client.head_object.side_effect = _make_client_error("NoSuchKey")
    mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}

    assert storage.exists("s3://my-bucket/silver/activities") is False


def test_s3_exists_reraises_other_errors(mock_s3_client):
    """Non-404 ClientErrors must propagate."""
    mock_s3_client.head_object.side_effect = _make_client_error("403")

    with pytest.raises(_StubClientError):
        storage.exists("s3://my-bucket/restricted/key")


# ---------------------------------------------------------------------------
# S3 — list_files
# ---------------------------------------------------------------------------


def test_s3_list_files_wildcard(mock_s3_client):
    paginator = _make_paginator(
        [
            [
                "silver/activities/year=2025/data.parquet",
                "silver/activities/year=2026/data.parquet",
            ]
        ]
    )
    mock_s3_client.get_paginator.return_value = paginator

    results = storage.list_files("s3://my-bucket/silver/activities/**/*.parquet")

    assert results == [
        "s3://my-bucket/silver/activities/year=2025/data.parquet",
        "s3://my-bucket/silver/activities/year=2026/data.parquet",
    ]


def test_s3_list_files_literal_exists(mock_s3_client):
    """Literal S3 path that exists returns a single-element list."""
    mock_s3_client.head_object.return_value = {}

    uri = "s3://my-bucket/silver/activities/year=2025/data.parquet"
    results = storage.list_files(uri)

    assert results == [uri]


def test_s3_list_files_literal_missing(mock_s3_client):
    """Literal S3 path that does not exist returns empty list."""
    mock_s3_client.head_object.side_effect = _make_client_error("404")
    mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}

    results = storage.list_files("s3://my-bucket/does/not/exist.parquet")

    assert results == []


def test_s3_list_files_suffix_filter(mock_s3_client):
    """Only keys matching the suffix after the wildcard are returned."""
    paginator = _make_paginator(
        [["prefix/file.parquet", "prefix/file.json", "prefix/sub/file.parquet"]]
    )
    mock_s3_client.get_paginator.return_value = paginator

    results = storage.list_files("s3://my-bucket/prefix/*.parquet")

    assert all(r.endswith(".parquet") for r in results)
    assert "s3://my-bucket/prefix/file.json" not in results


# ---------------------------------------------------------------------------
# S3 — rmtree
# ---------------------------------------------------------------------------


def test_s3_rmtree_calls_delete_objects(mock_s3_client):
    paginator = _make_paginator(
        [
            [
                "silver/activities/year=2025/data.parquet",
                "silver/activities/year=2026/data.parquet",
            ]
        ]
    )
    mock_s3_client.get_paginator.return_value = paginator

    storage.rmtree("s3://my-bucket/silver/activities")

    mock_s3_client.delete_objects.assert_called_once_with(
        Bucket="my-bucket",
        Delete={
            "Objects": [
                {"Key": "silver/activities/year=2025/data.parquet"},
                {"Key": "silver/activities/year=2026/data.parquet"},
            ]
        },
    )


def test_s3_rmtree_empty_prefix_no_delete(mock_s3_client):
    """rmtree on an empty prefix should not call delete_objects."""
    paginator = _make_paginator([[]])  # empty Contents
    mock_s3_client.get_paginator.return_value = paginator

    storage.rmtree("s3://my-bucket/empty/prefix")

    mock_s3_client.delete_objects.assert_not_called()


# ---------------------------------------------------------------------------
# S3 — write_partitioned_parquet
# ---------------------------------------------------------------------------


def test_s3_write_partitioned_parquet_put_object_calls(mock_s3_client):
    """Each partition group should produce one put_object call with the right key."""
    df = pl.DataFrame(
        {
            "year": ["2025", "2025", "2026"],
            "month": ["01", "01", "03"],
            "value": [1, 2, 3],
        }
    )
    storage.write_partitioned_parquet(
        df, "s3://my-bucket/silver/activities", partition_by=["year", "month"]
    )

    assert mock_s3_client.put_object.call_count == 2
    called_keys = {
        call.kwargs["Key"] for call in mock_s3_client.put_object.call_args_list
    }
    assert any("year=2025" in k and "month=01" in k for k in called_keys)
    assert any("year=2026" in k and "month=03" in k for k in called_keys)
    assert all(k.endswith("/data.parquet") for k in called_keys)


def test_s3_write_partitioned_parquet_drops_partition_cols(mock_s3_client):
    """Partition columns must not appear in the uploaded parquet payload."""
    import io

    uploaded_payloads: list[bytes] = []

    def capture_put_object(**kwargs):
        uploaded_payloads.append(kwargs["Body"])

    mock_s3_client.put_object.side_effect = capture_put_object

    df = pl.DataFrame(
        {
            "year": ["2025"],
            "month": ["06"],
            "activity_id": [42],
        }
    )
    storage.write_partitioned_parquet(
        df, "s3://my-bucket/silver/activities", partition_by=["year", "month"]
    )

    assert len(uploaded_payloads) == 1
    result = pl.read_parquet(io.BytesIO(uploaded_payloads[0]))
    assert "year" not in result.columns
    assert "month" not in result.columns
    assert "activity_id" in result.columns
