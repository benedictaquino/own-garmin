# Task 12: Migrate Bronze Modules to Storage Abstraction

Replace all `pathlib`/`open()`/`json.load()` file I/O in bronze modules with `storage.*` calls so bronze ingestion works with S3 URIs.

**Depends on:** Task 11 (storage abstraction)

## Objective

* All bronze write/read operations use `storage.*` instead of `pathlib.Path`.
* Existing behavior unchanged for local paths.
* No new tests needed — existing bronze tests cover the local code paths through `storage`.

## Files

* `src/own_garmin/bronze/activities.py`
* `src/own_garmin/bronze/activity_details.py`
* `src/own_garmin/bronze/fit.py`

## Implementation Steps

### 1. `bronze/activities.py`

Current I/O calls and their replacements:

| Current | Replacement |
|---|---|
| `Path(path).exists()` | `storage.exists(path)` |
| `open(path, encoding="utf-8") as f: json.load(f)` | `json.loads(storage.read_text(path))` |
| `target.read_text(encoding="utf-8")` | `storage.read_text(path)` |
| `target.parent.mkdir(parents=True, exist_ok=True)` | (removed — `storage.write_text` handles this) |
| `target.write_text(new_json, encoding="utf-8")` | `storage.write_text(path, new_json)` |

Remove `from pathlib import Path`. Add `from own_garmin import storage`.

The merge-on-activityId logic and idempotency check (`read existing → merge → compare → write if changed`) remain identical.

### 2. `bronze/activity_details.py`

Same pattern as activities.py:

| Current | Replacement |
|---|---|
| `Path(path).exists()` | `storage.exists(path)` |
| `open(path, encoding="utf-8") as f: json.load(f)` | `json.loads(storage.read_text(path))` |
| `target.read_text(encoding="utf-8")` | `storage.read_text(path)` |
| `target.parent.mkdir(parents=True, exist_ok=True)` | (removed) |
| `target.write_text(new_json, encoding="utf-8")` | `storage.write_text(path, new_json)` |

Remove `from pathlib import Path`. Add `from own_garmin import storage`.

### 3. `bronze/fit.py`

| Current | Replacement |
|---|---|
| `Path(path).exists()` | `storage.exists(path)` |
| `Path(path).parent.mkdir(parents=True, exist_ok=True)` | (removed) |
| `Path(path).write_bytes(fit_bytes)` | `storage.write_bytes(path, fit_bytes)` |

Remove `from pathlib import Path`. Add `from own_garmin import storage`.

## Acceptance Criteria

* [ ] No `pathlib.Path` usage remains in bronze modules (except `_common.py` if any)
* [ ] All bronze modules import and use `storage.*` for I/O
* [ ] `uv run pytest` passes — existing tests unaffected
* [ ] `uv run ruff check .` passes
