# Task 23: Incremental backfill with jittered pacing

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Implement generic date-chunking and cursor-persistence helpers in `bronze/_backfill.py` and
extend `paths.py` with two new helpers for the state directory. These utilities support
multi-year historical backfill across all wellness categories while respecting Garmin's
undocumented rate limits. This task does not wire the helpers into any existing extractor —
Task 24 does that.

## Files

* `src/own_garmin/bronze/_backfill.py` — **new**
* `src/own_garmin/paths.py` — **modify**: add `state_dir()` and `backfill_cursor_path()`

## Public API

### `bronze/_backfill.py`

```python
from collections.abc import Iterator
from datetime import date

def iter_days(since: date, until: date, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Yield (chunk_start, chunk_end) windows walking since → until in chunk_days steps."""

def load_cursor(category: str) -> date | None:
    """Return the last successfully processed date for category, or None if no cursor exists."""

def save_cursor(category: str, last_day: date) -> None:
    """Persist last_day as the backfill cursor for category."""

def jittered_sleep(base_seconds: float, jitter_pct: float = 0.2) -> None:
    """Sleep base_seconds ± (base_seconds * jitter_pct / 2). Actual sleep in [base*(1-j/2), base*(1+j/2)]."""

def retry_with_backoff(
    fn,
    max_attempts: int = 3,
    min_sleep: float = 60.0,
    max_sleep: float = 120.0,
) -> None:
    """Call fn(); on GarminTooManyRequestsError or GarminConnectionError, sleep a jittered
    interval in [min_sleep, max_sleep] and retry. Re-raises on the final attempt."""
```

### `paths.py` additions

```python
def state_dir() -> str:
    """Return the local state directory path.

    Reads OWN_GARMIN_STATE_DIR (default: ~/.config/own-garmin/state).
    Always local (expanduser applied); S3 state storage is out of scope.
    """

def backfill_cursor_path(category: str) -> str:
    """Return the absolute path to the cursor file for category.

    Format: {state_dir()}/backfill-{category}.cursor
    """
```

## Behavior

1. `iter_days(since, until, chunk_days)`:
   * Yields `(chunk_start, chunk_end)` tuples.
   * First window: `(since, min(since + timedelta(days=chunk_days - 1), until))`.
   * Subsequent windows advance by `chunk_days` until `chunk_start > until`.
   * Last window's `chunk_end` is clamped to `until` — no window extends past `until`.
   * A `chunk_days` of 1 yields one window per day (identical start and end).

2. `load_cursor(category)`:
   * Reads `backfill_cursor_path(category)` as plain UTF-8 text.
   * Returns `date.fromisoformat(text.strip())`.
   * Returns `None` if the file does not exist.

3. `save_cursor(category, last_day)`:
   * Writes `last_day.isoformat()` to `backfill_cursor_path(category)`.
   * Creates parent directories if needed (plain `pathlib` — cursor files are always local).

4. `jittered_sleep(base_seconds, jitter_pct)`:
   * Computes `delta = base_seconds * jitter_pct / 2`.
   * Sleeps `random.uniform(base_seconds - delta, base_seconds + delta)` seconds.

5. `retry_with_backoff(fn, max_attempts, min_sleep, max_sleep)`:
   * Calls `fn()` up to `max_attempts` times.
   * Catches `GarminTooManyRequestsError` and `GarminConnectionError` only.
   * On a caught exception: sleep `random.uniform(min_sleep, max_sleep)` seconds, then retry.
   * After the final attempt fails, re-raises the last exception.
   * Other exceptions propagate immediately without retry.

6. `state_dir()` in `paths.py`:
   * Reads `OWN_GARMIN_STATE_DIR`; defaults to `~/.config/own-garmin/state`.
   * Always applies `os.path.expanduser` (state is never S3).

## Acceptance Criteria

* [ ] `iter_days` yields correct boundaries for even-divisible and remainder windows.
* [ ] `iter_days` with `chunk_days=1` yields one tuple per calendar day.
* [ ] `iter_days` last window is clamped to `until`.
* [ ] `load_cursor` returns `None` for a missing file; returns correct `date` after `save_cursor`.
* [ ] `save_cursor` + `load_cursor` round-trip preserves the date exactly.
* [ ] `jittered_sleep` actual sleep duration falls within `[base*(1-j/2), base*(1+j/2)]`
      (mock `time.sleep` in tests; assert the sleep argument is in range).
* [ ] `retry_with_backoff` calls `fn()` once on success.
* [ ] `retry_with_backoff` retries up to `max_attempts` times on `GarminTooManyRequestsError`.
* [ ] `retry_with_backoff` re-raises after exhausting all attempts.
* [ ] `retry_with_backoff` does not catch non-Garmin exceptions.
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes (unit tests only; no network).

## Notes

* `GarminTooManyRequestsError` and `GarminConnectionError` are defined in
  `src/own_garmin/client/exceptions.py`. Import from there; do not redefine.
* Cursor files are plain text (one ISO date per file) — no JSON or YAML overhead needed.
* `state_dir()` applies `expanduser` unconditionally. The S3 guard in `data_root()` is not
  needed here because state files are always local regardless of data storage backend.
* This task has no runtime dependencies beyond what is already installed.
* Tests for `save_cursor` / `load_cursor` should use `tmp_path` (pytest fixture) to avoid
  touching the user's real state directory.
