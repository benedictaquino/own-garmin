# Task 17: Client: wellness endpoint methods

Part of [v1.2 Wellness plan](../03-garmin-lakehouse-wellness.md).

## Goal

Extend `GarminClient` with 10 new read-only methods (one per wellness category) and add the
corresponding URL template constants to `client/constants.py`. These methods are the only network
layer that bronze extractors call; no bronze or silver code changes are required in this task.

## Files

* `src/own_garmin/client/constants.py` — **modify**: add 10 URL template constants
* `src/own_garmin/client/client.py` — **modify**: add 10 public methods

## Public API

### New constants in `constants.py`

```python
# Wellness & Recovery
SLEEP_URL = "/wellness-service/wellness/dailySleepData/{display_name}"
BODY_BATTERY_URL = "/wellness-service/wellness/bodyBattery/reports/daily"
STRESS_URL = "/wellness-service/wellness/dailyStress/{date}"
HRV_URL = "/hrv-service/hrv/{date}"

# Daily Physiological Metrics
HEART_RATE_URL = "/wellness-service/wellness/dailyHeartRate"
RESPIRATION_URL = "/wellness-service/wellness/daily/respiration/{date}"
SPO2_URL = "/wellness-service/wellness/daily/spo2/{date}"
INTENSITY_MINUTES_URL = "/wellness-service/wellness/daily/im/{display_name}"

# Physical Composition & Goals
WEIGHT_URL = "/weight-service/weight/daterangesnapshot"
STEPS_URL = "/usersummary-service/stats/steps/daily/{start}/{end}"
```

### New methods in `GarminClient`

```python
def get_sleep(self, day: date) -> dict: ...
def get_body_battery(self, start: date, end: date) -> list[dict]: ...
def get_stress(self, day: date) -> dict: ...
def get_hrv(self, day: date) -> dict: ...
def get_heart_rate(self, day: date) -> dict: ...
def get_respiration(self, day: date) -> dict: ...
def get_spo2(self, day: date) -> dict: ...
def get_intensity_minutes(self, start: date, end: date) -> dict: ...
def get_weight(self, start: date, end: date) -> list[dict]: ...
def get_steps(self, start: date, end: date) -> list[dict]: ...
```

## Behavior

1. Each method calls `self._connectapi(path, params=...)` exactly once and returns the parsed
   response directly — `dict` for single-day endpoints, `list[dict]` for range endpoints.
2. Endpoints that embed `display_name` in the path substitute `self.display_name` (already
   available on the client; populated during login via `SOCIAL_PROFILE_URL`). These are:
   `get_sleep`, `get_intensity_minutes`.
3. Endpoints that use query parameters for dates pass them as keyword `params`: `body_battery`
   uses `startDate` / `endDate`; `heart_rate` uses `date`; `weight` uses `startDate` / `endDate`;
   `steps` embeds dates in the path as ISO strings.
4. Date arguments are formatted as `YYYY-MM-DD` strings using `day.isoformat()`.
5. On 4xx or 5xx responses the existing `_connectapi` machinery raises `GarminAuthError` (401/403)
   or `GarminConnectionError` (other errors) — no new exception types needed.
6. No pagination logic is added in this task. Range endpoints are assumed to return all results
   within the requested window in a single response; if Garmin paginates them in practice, that is
   addressed when the corresponding bronze extractor is written (Tasks 18–20).

## Acceptance Criteria

* [ ] All 10 constants are present in `constants.py` under a `# Wellness` section comment.
* [ ] All 10 methods are present in `GarminClient` with correct signatures.
* [ ] Each method returns the raw parsed JSON without modification.
* [ ] `display_name`-dependent methods do not expose `display_name` as a parameter (resolved
      internally from `self.display_name`).
* [ ] `uv run ruff check .` and `uv run ruff format .` produce no diff.
* [ ] `uv run pytest` passes (no existing tests broken; no new tests required for this task — live
      endpoint coverage comes via bronze verification in Tasks 18–20).

## Notes

* Do not transform or validate the response shape here. Bronze extractors own that responsibility.
* `self.display_name` is set during `_fetch_display_name()` which is called at the end of `login()`.
  If for any reason it is `None`, raise `GarminConnectionError("display_name not set — login first")`.
* Keep docstrings concise: one line describing the return value, one line noting the endpoint path.
* This task has no runtime dependencies beyond what is already installed.
