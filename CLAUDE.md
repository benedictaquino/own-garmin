# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                          # install dependencies
uv run pytest                    # run all tests
uv run pytest tests/test_silver_activities.py::test_name  # run a single test
uv run ruff check .              # lint
uv run ruff format .             # format
uv run own-garmin --help         # CLI entry point
```

## Architecture

**Medallion lakehouse pattern** — Bronze (raw) → Silver (processed) → DuckDB queries.

### Data layers

- **Bronze** (`data/bronze/activities/year=YYYY/month=MM/day=DD.json`): immutable raw JSON from Garmin API. Never modify; always re-ingest to update.
- **Silver** (`data/silver/activities/year=YYYY/month=MM/*.parquet`): Polars-transformed Parquet, fully rebuildable from Bronze without hitting Garmin's API.

### Module responsibilities

| Module | Role |
|---|---|
| `src/own_garmin/paths.py` | All path/URI construction — downstream code uses these helpers, never hardcoded paths. Returns strings so `s3://` is a drop-in future swap. |
| `src/own_garmin/client/` | Session-aware `GarminClient` package. `client.py` holds the main class; `strategies.py` implements the 5-strategy login chain; `constants.py` holds DI client IDs and endpoints; `exceptions.py` defines the error hierarchy. Tries token resume first; falls back to the login chain and persists new tokens. Session stored in `~/.config/own-garmin/session/`. |
| `src/own_garmin/bronze/activities.py` | Fetches from Garmin, groups by date, writes JSON. Idempotent: merges on `activityId` if file exists. |
| `src/own_garmin/silver/activities.py` | Pure function `transform(paths) -> pl.DataFrame`. Handles semicircle→degree conversion, dedup by `activity_id`. Rebuildable via `rebuild()`. |
| `src/own_garmin/query.py` | Opens in-memory DuckDB, registers silver Parquet as a view, returns Polars DataFrame. |
| `src/own_garmin/cli.py` | Typer app with four commands: `login`, `ingest`, `process`, `query`. |

### `GarminClient` public API

```python
class GarminClient:
    def list_activities(self, start: date, end: date) -> list[dict]: ...
    def get_activity(self, activity_id: int) -> dict: ...          # summary
    def get_activity_details(self, activity_id: int) -> dict: ...  # splits, laps, metrics
    def download_fit(self, activity_id: int) -> bytes: ...         # raw FIT file bytes
```

### Key design rules

- **Session tokens**: always try token resume before password login. Minimizes Garmin/Cloudflare friction.
- **Bronze is immutable**: transformation failures mean rebuild Silver, not re-ingest Bronze.
- **Silver transforms are pure functions**: given JSON paths in, deterministic Parquet out — no side effects, easy to test.
- **No hardcoded paths**: always go through `paths.py` helpers.

### Environment variables

| Var | Default | Purpose |
|---|---|---|
| `GARMIN_EMAIL` | — | Garmin Connect login |
| `GARMIN_PASSWORD` | — | Garmin Connect login |
| `OWN_GARMIN_DATA_DIR` | `./data` | Root for bronze/silver |
| `OWN_GARMIN_SESSION_DIR` | `~/.config/own-garmin/session` | Token persistence |

### Tests

Tests are unit-level only — no network calls. Fixtures in `tests/fixtures/activities/` are canned Garmin JSON day-files. Tests exercise `silver/activities.transform` for schema correctness, dedup, unit conversion, and null GPS handling.

### Implementation tasks

Tasks are tracked as GitHub Issues on [benedictaquino/own-garmin](https://github.com/benedictaquino/own-garmin/issues). Full specs remain in `plans/tasks/`. Pick up from the lowest-numbered open issue.
