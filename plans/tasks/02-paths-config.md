# Task 02: Paths and config helpers

## Goal

Centralize all filesystem URI construction and env-var loading so downstream code never hardcodes a path.

## File

`src/own_garmin/paths.py`

## API

```python
from datetime import date

def data_root() -> str: ...
def session_dir() -> str: ...
def bronze_path(category: str, day: date) -> str: ...
def silver_path(category: str) -> str: ...
```

## Behavior

- `data_root()` reads `OWN_GARMIN_DATA_DIR`, defaulting to `./data`. Returns a **string** (not `Path`) so a future swap to `s3://bucket/...` requires no code changes.
- `session_dir()` reads `OWN_GARMIN_SESSION_DIR`, defaulting to `~/.config/own-garmin/session`. Expanduser applied.
- `bronze_path(category, day)` → `f"{data_root()}/bronze/{category}/year={day:%Y}/month={day:%m}/day={day:%d}.json"`.
- `silver_path(category)` → `f"{data_root()}/silver/{category}"`.
- Load `.env` once on import via `python-dotenv`'s `load_dotenv()` (no-op if file missing).

## Acceptance

- Unit-testable without filesystem: given `OWN_GARMIN_DATA_DIR=/tmp/foo`, `bronze_path("activities", date(2026, 4, 12))` returns `/tmp/foo/bronze/activities/year=2026/month=04/day=12.json`.
- Session dir creation (mkdir -p) is the caller's responsibility, not `paths.py`'s.

## Notes

- Avoid `pathlib.Path` for the return types — strings keep the door open for non-filesystem URIs.
- Use `os.makedirs(..., exist_ok=True)` in the code that actually writes files (bronze/silver modules), not here.
