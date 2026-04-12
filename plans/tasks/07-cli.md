# Task 07: Typer CLI

## Goal
Expose all user-facing operations as subcommands of `own-garmin`. This is the single entrypoint for humans and schedulers.

## File
`src/own_garmin/cli.py`

## Commands
```
own-garmin login
own-garmin ingest --since YYYY-MM-DD [--until YYYY-MM-DD]
own-garmin process
own-garmin query "SELECT ..."
```

### `login`
- Force a fresh login: delete any existing token store contents, instantiate `GarminClient()` (which will re-login and persist).
- Print the session directory path on success.

### `ingest`
- Parse `--since` / `--until` as ISO dates. `--until` defaults to today.
- Instantiate `GarminClient()`, call `bronze.activities.ingest(client, since, until)`.
- Print the count of activities written.

### `process`
- Call `silver.activities.rebuild()`.
- Print the row count written to silver.

### `query`
- Take a single positional SQL string.
- Call `query.query(sql)` and print the DataFrame. Use `df.write_csv()` to stdout for pipe-ability, or Polars' default tabular repr for interactive use — pick one and document it.

## Behavior
- Use `typer.Typer(no_args_is_help=True)`.
- Wrap each command body in a try/except that prints a one-line error and exits non-zero on failure (typer handles tracebacks noisily otherwise).
- Top-level `app = typer.Typer()` is what `pyproject.toml`'s `[project.scripts]` points at.

## Acceptance
- `uv run own-garmin --help` lists all four commands.
- `uv run own-garmin ingest --since 2026-01-01 --until 2026-01-07` completes and prints a count.
- `uv run own-garmin query "SELECT COUNT(*) FROM activities"` prints a tabular result.
- Exit codes: 0 on success, non-zero on handled failure.

## Notes
- Do not add a `--verbose` flag or logging framework yet — keep the CLI output minimal and deterministic.
- No click groups / subgroups; flat commands are enough for v1.
