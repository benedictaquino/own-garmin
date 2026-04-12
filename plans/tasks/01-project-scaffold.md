# Task 01: Project scaffold

## Goal
Set up the Python package skeleton managed with `uv`, with a src layout and pinned runtime/dev dependencies.

## Steps
1. Run `uv init --package --name own-garmin --lib` (or equivalent) so `pyproject.toml` and `src/own_garmin/__init__.py` exist.
2. Add runtime deps: `uv add garminconnect polars duckdb typer python-dotenv`.
3. Add dev deps: `uv add --dev pytest ruff`.
4. Register the CLI entrypoint in `pyproject.toml`:
   ```toml
   [project.scripts]
   own-garmin = "own_garmin.cli:app"
   ```
5. Create the directory layout:
   - `src/own_garmin/bronze/__init__.py`
   - `src/own_garmin/silver/__init__.py`
   - `tests/` (empty)
   - `data/` (not committed)
6. Create `.env.example`:
   ```
   GARMIN_EMAIL=
   GARMIN_PASSWORD=
   # Optional overrides
   # OWN_GARMIN_DATA_DIR=./data
   # OWN_GARMIN_SESSION_DIR=~/.config/own-garmin/session
   ```
7. Create `.gitignore` covering: `.venv/`, `__pycache__/`, `*.pyc`, `.env`, `data/`, `.pytest_cache/`, `.ruff_cache/`, `uv.lock` stays committed.

## Acceptance
- `uv sync` completes without errors.
- `uv run own-garmin --help` prints Typer help (after task 07 wires the CLI; for this task, a placeholder `app = typer.Typer()` in `cli.py` is enough).
- Repo contains `pyproject.toml`, `uv.lock`, `.env.example`, `.gitignore`, `src/own_garmin/` package.

## Notes
- Keep `uv.lock` committed so installs are reproducible.
- Do not commit `.env` or anything under `data/`.
