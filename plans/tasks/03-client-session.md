# Task 03: Garmin client with DI session persistence

## Goal

Implement a custom `GarminClient` that uses the vendored DI (Direct Integration) auth strategies. The client must prefer token refresh over full login to minimize Cloudflare exposure and handle MFA prompts gracefully via the CLI.

## Files

- `src/own_garmin/client/client.py`
- `src/own_garmin/client/strategies.py`
- `src/own_garmin/client/constants.py`
- `src/own_garmin/client/exceptions.py`

## Public API

```python
from datetime import date

class GarminClient:
    def __init__(self) -> None: ...
    def list_activities(self, start: date, end: date) -> list[dict]: ...
    def get_activity(self, activity_id: int) -> dict: ...
```

## Implementation Notes

- **5-Strategy Login Chain**: Implements a robust fallback mechanism (portal+cffi, portal+requests, mobile+cffi, mobile+requests, and widget+cffi fallbacks).
- **Session Persistence**: Tokens are saved to `garmin_tokens.json` in the session directory (`~/.config/own-garmin/session/` by default).
- **Auto-Refresh**: Automatically checks JWT expiry and uses `refresh_token` before API calls or full login.
- **MFA Support**: Handles MFA challenges by pausing the login chain and prompting for user input via the CLI.
- **Evasion Techniques**: Uses random delays (30-45s) and browser TLS impersonation (via `curl_cffi`) to mimic natural behavior.
- **Token Exchange**: Enhanced DI token exchange with client ID rotation for maximum resilience.
