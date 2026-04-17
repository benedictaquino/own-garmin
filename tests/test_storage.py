"""Unit tests for own_garmin.storage — local code paths only."""

from __future__ import annotations

import polars as pl

import own_garmin.storage as storage

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
