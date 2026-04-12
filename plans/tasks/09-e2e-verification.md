# Task 09: End-to-end verification

## Goal

Verify the full Bronze → Silver → Query pipeline works against a real Garmin account.

## Prerequisites

- Tasks 01–08 complete.
- Real Garmin credentials in `.env`.
- An account with at least one activity in the test window.

## Steps

1. `uv sync` — install deps.
2. `cp .env.example .env`, fill `GARMIN_EMAIL` and `GARMIN_PASSWORD`.
3. `uv run own-garmin login`
   - **Expect:** command prints the session directory path.
   - **Verify:** token files exist under `~/.config/own-garmin/session/`.
4. `uv run own-garmin ingest --since 2026-01-01`
   - **Expect:** prints an activity count > 0.
   - **Verify:** JSON files exist under `data/bronze/activities/year=2026/month=*/day=*.json`.
   - **Verify:** each file is a valid JSON array; spot-check one activity object for expected fields (`activityId`, `startTimeLocal`, `activityType.typeKey`).
5. Re-run step 4 with the same window.
   - **Expect:** printed count matches previous run (no duplicates).
   - **Verify:** file mtimes did not change (content-equality no-op).
6. `uv run own-garmin process`
   - **Expect:** prints a row count > 0.
   - **Verify:** parquet files exist under `data/silver/activities/year=*/month=*/`.
7. `uv run own-garmin query "SELECT activity_type, COUNT(*) AS n FROM activities GROUP BY 1 ORDER BY n DESC"`
   - **Expect:** tabular output grouped by activity type.
8. `uv run own-garmin query "SELECT activity_id, start_time_local, start_lat, start_lon FROM activities WHERE start_lat IS NOT NULL LIMIT 5"`
   - **Verify:** `start_lat` / `start_lon` are in the expected degree range (±90 / ±180), not raw semicircles.
9. `uv run pytest -q`
   - **Expect:** all silver tests pass.

## Failure modes to probe

- Delete the session dir, re-run `ingest` → should re-login and continue.
- Delete the silver dir, re-run `process` → should rebuild from bronze without touching Garmin.
- Pass a future `--since` → prints 0 activities, no error.

## Acceptance

- All expected outputs observed.
- Any unexpected failures logged as follow-up tasks before declaring the project complete.

## Notes

- Do not commit anything from `data/` or the session store.
- If Garmin returns a Cloudflare challenge during login, wait 10+ minutes before retrying — do not loop.
