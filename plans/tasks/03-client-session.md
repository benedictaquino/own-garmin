# Task 03: Garmin client with session persistence

## Goal
Wrap `python-garminconnect` with a session-aware client that prefers token refresh over full login, minimizing Cloudflare exposure (ADR: Stealth).

## File
`src/own_garmin/client.py`

## Public API
```python
from datetime import date

class GarminClient:
    def __init__(self) -> None: ...
    def list_activities(self, start: date, end: date) -> list[dict]: ...
    def get_activity(self, activity_id: int) -> dict: ...
```

## Behavior
- Constructor:
  1. Resolve `session_dir()` from `paths.py`, `mkdir -p` it.
  2. Try `Garmin().login(tokenstore=session_dir)` to resume the saved session.
  3. If that raises `GarminConnectAuthenticationError` (or tokens are missing), read `GARMIN_EMAIL` / `GARMIN_PASSWORD` from env, construct `Garmin(email, password)`, call `.login()`, then persist via `client.garth.dump(session_dir)`.
  4. Raise a clear error if credentials are missing and resume failed.
- `list_activities(start, end)` wraps `client.get_activities_by_date(start.isoformat(), end.isoformat())`.
- `get_activity(activity_id)` wraps `client.get_activity(activity_id)` — returns full detail dict.
- On `GarminConnectTooManyRequestsError` / 429, surface as an exception; do not retry silently here.

## Error handling
- Only catch auth-resume failures to trigger re-login. Let all other exceptions propagate so the CLI can render them.
- Do not mock or stub anything — this module is exercised manually.

## Acceptance
- `GarminClient()` returns successfully when a valid token store exists (no network login triggered).
- With an empty session dir and valid creds, constructor logs in and writes token files.
- `list_activities` + `get_activity` return plain Python dicts / lists ready to serialize to JSON.

## Notes
- The exact garth / garminconnect token-dump call may vary across library versions (e.g. `client.garth.dump(path)` vs `Garmin.dump(path)`). Verify against the installed version and adjust.
- No retry logic here — keep the wrapper thin. Rate limiting lives in the bronze ingestion layer.
