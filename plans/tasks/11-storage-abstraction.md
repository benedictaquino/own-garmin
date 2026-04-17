# Task 11: Storage Abstraction Layer

Create `src/own_garmin/storage.py` — a thin I/O module that makes all data read/write operations work transparently with both local paths and S3 URIs.

## Objective

* Provide a unified API for file I/O that dispatches on `s3://` prefix.
* Keep `boto3` as a lazy import so local-only users never need it installed.
* Guard `paths.py` against `expanduser` mangling S3 URIs.

## Files

* `src/own_garmin/storage.py` (new)
* `src/own_garmin/paths.py` (modify)
* `pyproject.toml` (modify)
* `tests/test_storage.py` (new)

## Implementation Steps

### 1. Guard `paths.py` against S3 URIs

In `data_root()`, skip `os.path.expanduser()` when the path starts with `s3://`:

```python
def data_root() -> str:
    path = os.environ.get("OWN_GARMIN_DATA_DIR", "./data")
    if path.startswith("s3://"):
        return path.rstrip("/")
    return os.path.expanduser(path)
```

### 2. Create `src/own_garmin/storage.py`

Public API (~100 lines total):

```python
def is_s3(path: str) -> bool
def read_text(path: str) -> str
def write_text(path: str, data: str) -> None
def read_bytes(path: str) -> bytes
def write_bytes(path: str, data: bytes) -> None
def exists(path: str) -> bool
def list_files(pattern: str) -> list[str]
def rmtree(path: str) -> None
def write_partitioned_parquet(df: pl.DataFrame, target: str, partition_by: list[str]) -> None
```

**Local dispatch:**

* `read_text` / `write_text`: `pathlib.Path` read/write with utf-8 encoding. `write_text` creates parent dirs.
* `read_bytes` / `write_bytes`: same pattern with bytes.
* `exists`: `Path(path).exists()`.
* `list_files`: `glob.glob(pattern, recursive=True)`, sorted.
* `rmtree`: `shutil.rmtree(path, ignore_errors=True)`.
* `write_partitioned_parquet`: `Path(target).mkdir(parents=True, exist_ok=True)` then `df.write_parquet(target, partition_by=partition_by)`.

**S3 dispatch:**

* Parse `s3://bucket/key` into bucket + key with a helper `_parse_s3(uri) -> (bucket, key)`.
* `read_text` / `read_bytes`: `boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()`.
* `write_text` / `write_bytes`: `boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)`.
* `exists`: `head_object` with a `try/except ClientError` for 404.
* `list_files(pattern)`: Extract bucket and prefix from the pattern (everything before `*` or `**`). Call `list_objects_v2` with that prefix, paginate, then filter results by suffix (e.g., `.json`, `.parquet`, `.zip`). Return full `s3://bucket/key` URIs.
* `rmtree(path)`: List all objects under the prefix, batch-delete with `delete_objects`.
* `write_partitioned_parquet`: Group DataFrame by partition columns, write each group as a separate parquet file using `put_object`. Drop partition columns from the parquet data (matching Polars' local behavior for hive partitioning). Path format: `{target}/{col}={val}/.../{col}={val}/data.parquet`.

**Lazy boto3 import**: Import `boto3` inside the S3 code paths only, not at module level.

**Alternative considered — `fsspec` / `s3fs`:** A unified filesystem interface would let us write a single `open(path, "rb")` that transparently handles both `s3://bucket/key` and `./data/file`, eliminating the manual `is_s3`-based dispatch throughout this module. The tradeoff is an extra top-level dependency (`s3fs` pulls in `fsspec` + `aiobotocore`) and a heavier cloud runtime. We're sticking with raw `boto3` for v1.1 to keep the dependency surface minimal and the dispatch explicit, but this is worth revisiting if `storage.py` grows past ~200 lines or we need to support additional backends (GCS, Azure).

### 3. Add optional dependency to `pyproject.toml`

```toml
[project.optional-dependencies]
s3 = ["boto3>=1.28.0"]
```

### 4. Unit tests: `tests/test_storage.py`

Test local code paths using `tmp_path`:

* `is_s3("s3://bucket/key")` returns True; `is_s3("./data")` returns False
* `write_text` + `read_text` round-trip
* `write_bytes` + `read_bytes` round-trip
* `exists` returns False for missing path, True after write
* `list_files` with a glob pattern finds expected files
* `rmtree` removes a directory tree
* `write_partitioned_parquet` produces expected hive directory structure with correct parquet files
* `write_text` creates parent directories automatically

## Acceptance Criteria

* [ ] `is_s3` correctly identifies S3 URIs
* [ ] All local I/O functions pass round-trip tests
* [ ] `write_partitioned_parquet` produces hive-partitioned directory structure
* [ ] `paths.data_root()` returns S3 URIs unchanged (no expanduser mangling)
* [ ] `boto3` is not imported when `OWN_GARMIN_DATA_DIR` is a local path
* [ ] `uv run pytest` passes (existing tests unaffected)
