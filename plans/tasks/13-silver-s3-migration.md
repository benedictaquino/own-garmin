# Task 13: Migrate Silver Modules to Storage Abstraction

Replace all `glob`/`shutil`/`pathlib`/`pl.read_json` file I/O in silver modules with `storage.*` calls so silver transforms work with S3 URIs.

**Depends on:** Task 11 (storage abstraction)

## Objective

* Silver `rebuild()` and `transform()` functions work transparently with both local and S3 paths.
* Polars `read_json` (which doesn't support S3) is routed through `storage.read_bytes` + `io.BytesIO`.
* Partitioned parquet writes use `storage.write_partitioned_parquet`.

## Files

* `src/own_garmin/silver/activities.py`
* `src/own_garmin/silver/fit_records.py`

## Implementation Steps

### 1. `silver/activities.py`

#### `transform(bronze_json_paths)`

Current: `pl.read_json(p)` for each path.
Problem: `pl.read_json` does not support S3 URIs.
Fix: Read via storage, wrap in BytesIO:

```python
import io
from own_garmin import storage

frames = [pl.read_json(io.BytesIO(storage.read_bytes(p))) for p in bronze_json_paths]
```

#### `rebuild()`

| Current | Replacement |
|---|---|
| `glob.glob(pattern, recursive=True)` | `storage.list_files(pattern)` |
| `shutil.rmtree(target, ignore_errors=True)` | `storage.rmtree(target)` |
| `Path(target).mkdir(parents=True, exist_ok=True)` | (removed — handled by storage) |
| `df.write_parquet(target, partition_by=["year", "month"])` | `storage.write_partitioned_parquet(df, target, ["year", "month"])` |

Remove imports: `glob`, `shutil`, `from pathlib import Path`.
Add imports: `io`, `from own_garmin import storage`.

### 2. `silver/fit_records.py`

#### `transform(fit_zip_paths)`

No changes needed — `_decode_zip` handles the actual I/O.

#### `_decode_zip(zip_path)`

Current: `zipfile.ZipFile(zip_path)` opens file by path.
Problem: `zipfile.ZipFile` doesn't support S3 URIs.
Fix: Read bytes via storage, wrap in BytesIO:

```python
zip_data = storage.read_bytes(zip_path)
with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
    ...
```

Current: `Path(zip_path).stem` to extract activity_id.
Fix: This works for both local and S3 paths since `Path("s3://bucket/key/123.zip").stem` returns `"123"`. However, to avoid depending on this behavior, use string ops:

```python
activity_id = int(zip_path.rsplit("/", 1)[-1].split(".")[0])
```

#### `rebuild()` (fit_records)

Same pattern as activities.py:

| Current | Replacement |
|---|---|
| `glob.glob(pattern, recursive=True)` | `storage.list_files(pattern)` |
| `shutil.rmtree(target, ignore_errors=True)` | `storage.rmtree(target)` |
| `Path(target).mkdir(parents=True, exist_ok=True)` | (removed) |
| `df.write_parquet(target, partition_by=["year", "month"])` | `storage.write_partitioned_parquet(df, target, ["year", "month"])` |

Remove imports: `glob`, `shutil`, `from pathlib import Path`.
Add imports: `io`, `from own_garmin import storage`.

## Acceptance Criteria

* [ ] No `glob.glob`, `shutil.rmtree`, or `pathlib.Path` usage in silver modules
* [ ] `transform` reads JSON/ZIP via `storage.read_bytes` + BytesIO
* [ ] `rebuild` uses `storage.list_files`, `storage.rmtree`, `storage.write_partitioned_parquet`
* [ ] `uv run pytest` passes — existing silver fixture tests unaffected
* [ ] `uv run ruff check .` passes
