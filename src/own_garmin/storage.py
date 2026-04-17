"""Thin I/O module that dispatches between local filesystem and S3.

All public functions accept paths as plain strings.  S3 URIs are detected by
the ``s3://`` prefix; everything else is treated as a local path.

``boto3`` is imported lazily — only inside S3 code paths — so local-only
users never need it installed.
"""

from __future__ import annotations

import glob as _glob
import shutil
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_s3(path: str) -> bool:
    """Return True when *path* is an S3 URI (starts with ``s3://``)."""
    return path.startswith("s3://")


def _parse_s3(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


# ---------------------------------------------------------------------------
# Text I/O
# ---------------------------------------------------------------------------


def read_text(path: str) -> str:
    """Read *path* as UTF-8 text."""
    if is_s3(path):
        import boto3

        bucket, key = _parse_s3(path)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return body.decode("utf-8")
    return Path(path).read_text(encoding="utf-8")


def write_text(path: str, data: str) -> None:
    """Write *data* as UTF-8 text to *path*, creating parent dirs as needed."""
    if is_s3(path):
        import boto3

        bucket, key = _parse_s3(path)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data.encode("utf-8"))
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(data, encoding="utf-8")


# ---------------------------------------------------------------------------
# Bytes I/O
# ---------------------------------------------------------------------------


def read_bytes(path: str) -> bytes:
    """Read *path* as raw bytes."""
    if is_s3(path):
        import boto3

        bucket, key = _parse_s3(path)
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return Path(path).read_bytes()


def write_bytes(path: str, data: bytes) -> None:
    """Write *data* as raw bytes to *path*, creating parent dirs as needed."""
    if is_s3(path):
        import boto3

        bucket, key = _parse_s3(path)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


# ---------------------------------------------------------------------------
# Existence / listing / deletion
# ---------------------------------------------------------------------------


def exists(path: str) -> bool:
    """Return True if *path* exists."""
    if is_s3(path):
        import boto3
        from botocore.exceptions import ClientError

        bucket, key = _parse_s3(path)
        try:
            boto3.client("s3").head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise
    return Path(path).exists()


def list_files(pattern: str) -> list[str]:
    """Return a sorted list of paths matching *pattern*.

    For local paths, delegates to :func:`glob.glob` with ``recursive=True``.

    For S3 patterns, extracts the bucket and prefix (everything before the
    first ``*``), lists all objects under that prefix, then filters by the
    suffix that follows the last ``*`` or ``**`` segment.
    """
    if is_s3(pattern):
        import boto3

        # Split at first wildcard to derive bucket and prefix
        star_idx = pattern.index("*")
        prefix_part = pattern[:star_idx]  # e.g. "s3://bucket/prefix/"
        suffix = pattern.split("*")[-1].lstrip("/")  # e.g. ".parquet"

        bucket, prefix = _parse_s3(prefix_part)

        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        results: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not suffix or key.endswith(suffix):
                    results.append(f"s3://{bucket}/{key}")
        return sorted(results)

    return sorted(_glob.glob(pattern, recursive=True))


def rmtree(path: str) -> None:
    """Recursively remove *path* and all its contents.

    Silently does nothing if *path* does not exist.
    """
    if is_s3(path):
        import boto3

        bucket, prefix = _parse_s3(path)
        if not prefix.endswith("/"):
            prefix += "/"

        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        return
    shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Partitioned Parquet
# ---------------------------------------------------------------------------


def write_partitioned_parquet(
    df: pl.DataFrame, target: str, partition_by: list[str]
) -> None:
    """Write *df* as hive-partitioned Parquet files under *target*.

    For local targets, creates the directory and delegates to
    ``df.write_parquet`` with ``partition_by``.

    For S3 targets, groups *df* by the partition columns and uploads each
    group as a separate ``data.parquet`` object, dropping the partition
    columns from the stored data to match Polars' local hive behaviour.
    """
    if is_s3(target):
        import io

        import boto3

        bucket, base_prefix = _parse_s3(target)
        if base_prefix and not base_prefix.endswith("/"):
            base_prefix += "/"

        s3 = boto3.client("s3")

        for group_keys, group_df in df.group_by(partition_by):
            # Build hive path segments: col=val/col=val/…
            if not isinstance(group_keys, (list, tuple)):
                group_keys = [group_keys]
            segments = "/".join(
                f"{col}={val}" for col, val in zip(partition_by, group_keys)
            )
            key = f"{base_prefix}{segments}/data.parquet"

            payload_df = group_df.drop(partition_by)
            buf = io.BytesIO()
            payload_df.write_parquet(buf)
            buf.seek(0)
            s3.put_object(Bucket=bucket, Key=key, Body=buf.read())
        return

    Path(target).mkdir(parents=True, exist_ok=True)
    df.write_parquet(target, partition_by=partition_by)
