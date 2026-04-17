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

### 2. Load httpfs + aws extensions for S3 paths

Before the table registration loop, check if data root is S3 and load both `httpfs` and `aws` extensions:

```python
if storage.is_s3(paths.data_root()):
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    con.install_extension("aws")
    con.load_extension("aws")
    con.execute("CALL load_aws_credentials();")
```

**Why the `aws` extension is required:** DuckDB's `httpfs` alone does **not** automatically inherit AWS credentials from the environment the way `boto3` does. Without the `aws` extension and the `load_aws_credentials()` call, queries against `s3://` paths will fail with authentication errors even when `AWS_ACCESS_KEY_ID` etc. are set. The `aws` extension's `load_aws_credentials()` resolves credentials through the standard AWS provider chain — env vars, shared config, EC2 instance profile, and ECS/Fargate/Lambda task roles — so the same code works for local development and cloud execution.

### 2a. Temp directory fallback for read-only filesystems

DuckDB spills to disk during complex queries. On Lambda (read-only filesystem except `/tmp`) or other constrained runtimes, the default spill location will fail. When running in a cloud environment, set the temp directory explicitly:

```python
if storage.is_s3(paths.data_root()):
    con.execute("SET temp_directory='/tmp/duckdb_temp';")
```

This is safe to apply unconditionally when data root is S3 — `/tmp` exists on Lambda, Fargate, and virtually any Linux container. Skip the setting for local runs so developers don't pollute `/tmp` on their workstations.

### 3. Verify glob pattern works with DuckDB

DuckDB's `read_parquet` supports `s3://bucket/prefix/**/*.parquet` globs natively once httpfs is loaded. The existing `paths.silver_glob()` returns patterns like `{root}/silver/activities/**/*.parquet` — when `root` is `s3://bucket/prefix`, this becomes a valid S3 glob for DuckDB.

No changes needed to `paths.silver_glob()`.

## Acceptance Criteria

* [ ] No `glob.glob` usage in `query.py`
* [ ] `httpfs` and `aws` extensions loaded when data root is S3
* [ ] `CALL load_aws_credentials();` is executed so IAM task roles / env vars are picked up automatically
* [ ] `temp_directory` is set to `/tmp/duckdb_temp` when running against S3 (safe under Lambda's read-only filesystem)
* [ ] `uv run pytest` passes — existing tests unaffected
* [ ] `uv run ruff check .` passes
