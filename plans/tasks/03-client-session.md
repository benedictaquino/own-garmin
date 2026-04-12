# Task 03: Garmin client with DI session persistence

## Goal

Implement a custom `GarminClient` that uses the vendored DI (Direct Integration) auth strategies. The client must prefer token refresh over full login to minimize Cloudflare exposure and handle MFA prompts gracefully via the CLI.

## File

`src/own_garmin/client.py`

## Public API

```python
from datetime import date

class GarminClient:
    def __init__(self) -> None: ...
    def list_activities(self, start: date, end: date) -> list[dict]: ...
    def get_activity(self, activity_id: int) -> dict: ...

