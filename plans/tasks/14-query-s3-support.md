# Task 14: S3 Support for Query Layer

Update `query.py` to load DuckDB's `httpfs` extension when querying S3-backed silver data, and replace `glob.glob` with `storage.list_files`.

**Depends on:** Task 11 (storage abstraction)

## Objective

* `own-garmin query "SELECT ..."` works when `OWN_GARMIN_DATA_DIR=s3://bucket/prefix`.
* DuckDB reads S3 parquet natively via httpfs — no custom download logic needed.

## Files

* `src/own_garmin/query.py`

## Implementation Steps

### 1. Replace glob with storage.list_files

Current:

```python
if not glob.glob(parquet_glob, recursive=True):
    continue
```

Replacement:

```python
if not storage.list_files(parquet_glob):
    continue
```

Remove `import glob`. Add `from own_garmin import storage`.

### 2. Load httpfs for S3 paths

Before the table registration loop, check if data root is S3 and load httpfs:

```python
if storage.is_s3(paths.data_root()):
    con.install_extension("httpfs")
    con.load_extension("httpfs")
```

DuckDB auto-detects AWS credentials from environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) and instance profiles. No explicit credential configuration needed beyond loading the extension.

### 3. Verify glob pattern works with DuckDB

DuckDB's `read_parquet` supports `s3://bucket/prefix/**/*.parquet` globs natively once httpfs is loaded. The existing `paths.silver_glob()` returns patterns like `{root}/silver/activities/**/*.parquet` — when `root` is `s3://bucket/prefix`, this becomes a valid S3 glob for DuckDB.

No changes needed to `paths.silver_glob()`.

## Acceptance Criteria

* [ ] No `glob.glob` usage in `query.py`
* [ ] httpfs extension loaded when data root is S3
* [ ] `uv run pytest` passes — existing tests unaffected
* [ ] `uv run ruff check .` passes
