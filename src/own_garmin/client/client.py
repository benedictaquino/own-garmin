import base64
import io
import json
import logging
import os
import tempfile
import time
import zipfile
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from own_garmin import paths

from . import strategies
from .constants import (
    ACTIVITIES_URL,
    ACTIVITY_DETAILS_URL,
    ACTIVITY_FIT_URL,
    ACTIVITY_URL,
    DI_CLIENT_IDS,
    DI_GRANT_TYPE,
    DI_TOKEN_URL,
    HAS_CFFI,
    LOGIN_DELAY_MAX_S,
    MOBILE_SSO_SERVICE_URL,
    SOCIAL_PROFILE_URL,
    CffiRequestException,
    _build_basic_auth,
    _native_headers,
    cffi_requests,
)
from .exceptions import (
    GarminAuthenticationError,
    GarminConnectionError,
    GarminTooManyRequestsError,
)
from .strategies import StrategyResult

_LOGGER = logging.getLogger(__name__)

if HAS_CFFI and CffiRequestException is not None:
    _TRANSPORT_EXCEPTIONS: tuple[type[BaseException], ...] = (
        requests.RequestException,
        CffiRequestException,
    )
else:
    _TRANSPORT_EXCEPTIONS = (requests.RequestException,)


class GarminClient:
    """
    Garmin Connect client: Auto-resuming session + API access.
    Completely decoupled from python-garminconnect and garth.
    """

    def __init__(
        self,
        prompt_mfa: Callable[[], str] | None = None,
        *,
        resume_session: bool = True,
    ) -> None:
        self.domain = "garmin.com"
        self._sso = f"https://sso.{self.domain}"
        self._connect = f"https://connect.{self.domain}"
        self._connectapi_url = f"https://connectapi.{self.domain}"

        # DI Bearer tokens
        self.di_token: str | None = None
        self.di_refresh_token: str | None = None
        self.di_client_id: str | None = None

        self.display_name: str | None = None
        self.full_name: str | None = None

        self._api_session: requests.Session | None = None
        self._pool_connections: int = 20
        self._pool_maxsize: int = 20
        self._pending_mfa: str | None = None

        # MFA state — set by login strategies when a challenge is triggered
        self._widget_session: Any = None
        self._widget_signin_params: dict[str, str] | None = None
        self._widget_last_resp: Any = None
        self._mfa_method: str = "email"
        self._mfa_portal_web_session: Any = None
        self._mfa_portal_web_params: dict[str, str] | None = None
        self._mfa_portal_web_headers: dict[str, str] | None = None
        self._mfa_cffi_session: Any = None
        self._mfa_cffi_params: dict[str, str] | None = None
        self._mfa_cffi_headers: dict[str, str] | None = None
        self._mfa_session: Any = None  # mobile_requests MFA session

        self.session_dir = Path(paths.session_dir())
        self._tokenstore_path: str | None = None

        resume_success = False
        tokens_env = os.environ.get("GARMIN_TOKENS_JSON") if resume_session else None

        if tokens_env:
            try:
                self._load_tokens_from_json(tokens_env)
                self._load_profile()
                resume_success = True
                _LOGGER.info("Session side-loaded from GARMIN_TOKENS_JSON env var.")
            except (
                json.JSONDecodeError,
                ValueError,
                GarminAuthenticationError,
            ) as e:
                _LOGGER.info(
                    "GARMIN_TOKENS_JSON side-load failed (%s: %s). Falling back.",
                    type(e).__name__,
                    e,
                )

        # Only touch disk if env side-load did not already authenticate us.
        if not resume_success:
            try:
                self.session_dir.mkdir(parents=True, exist_ok=True)
                self._tokenstore_path = str(self.session_dir / "garmin_tokens.json")
            except OSError as e:
                _LOGGER.warning(
                    "Session dir %s is not writable (%s); tokens not persisted.",
                    self.session_dir,
                    e,
                )

        if resume_session and not resume_success:
            tokenstore_path = self._tokenstore_path
            if tokenstore_path and Path(tokenstore_path).exists():
                try:
                    self._load_tokens(tokenstore_path)
                    self._load_profile()
                    resume_success = True
                    _LOGGER.info("Session resumed successfully from local token store.")
                except (
                    OSError,
                    json.JSONDecodeError,
                    ValueError,
                    GarminAuthenticationError,
                ) as e:
                    _LOGGER.info(
                        "Auth-resume failed (%s: %s). Triggering full login.",
                        type(e).__name__,
                        e,
                    )
            elif not tokens_env:
                _LOGGER.info("No token file found. Triggering full login.")
        elif not resume_session:
            _LOGGER.info(
                "resume_session=False; skipping session resume, forcing fresh login."
            )

        if not resume_success:
            load_dotenv()
            email = os.getenv("GARMIN_EMAIL")
            password = os.getenv("GARMIN_PASSWORD")

            if not email or not password:
                raise ValueError(
                    "Missing GARMIN_EMAIL or GARMIN_PASSWORD in environment. "
                    "Cannot perform fresh login, and session resume failed."
                )

            _LOGGER.info("Initiating fresh 5-strategy login chain...")
            result = self._login_chain(email, password, return_on_mfa=True)

            if result and result[0] == "needs_mfa":
                mfa_code = (
                    prompt_mfa() if prompt_mfa else input("\nEnter Garmin MFA code: ")
                )
                self._resume_login_chain(mfa_code)

            if self._tokenstore_path:
                try:
                    self._dump_tokens(self._tokenstore_path)
                    _LOGGER.info(
                        "Login successful. Tokens persisted to %s",
                        self._tokenstore_path,
                    )
                except OSError as e:
                    self._tokenstore_path = None
                    _LOGGER.warning(
                        "Failed to persist login tokens (%s); kept in memory.",
                        e,
                    )
            else:
                _LOGGER.info(
                    "Login successful. Tokens kept in memory (no writable session dir)."
                )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_activities(self, start: date, end: date) -> list[dict]:
        """Fetch summary dicts of activities within a date range."""
        return self._connectapi(
            ACTIVITIES_URL,
            params={"startDate": start.isoformat(), "endDate": end.isoformat()},
        )

    def get_activity(self, activity_id: int) -> dict:
        """Fetch the full summary dictionary for a specific activity."""
        return self._connectapi(f"{ACTIVITY_URL}/{activity_id}")

    def get_activity_details(
        self, activity_id: int, max_chart: int = 99999, max_poly: int = 99999
    ) -> dict:
        """
        Fetch charts/metrics for an activity. Default to large values to get all data.

        :param activity_id: Garmin activity ID.
        :param max_chart: Max number of data points for charts (e.g., HR, pace).
        :param max_poly: Max number of points for the GPS polyline.
        """
        path = ACTIVITY_DETAILS_URL.format(activity_id=activity_id)
        params = {"maxChartSize": max_chart, "maxPolylineSize": max_poly}
        return self._connectapi(path, params=params)

    def download_fit(self, activity_id: int) -> bytes:
        """
        Download the FIT file for an activity and return its raw bytes.

        The download service wraps the FIT file in a ZIP archive. This method
        extracts and returns the first file inside that archive.

        :param activity_id: Garmin activity ID.
        :returns: Raw FIT file bytes.
        :raises GarminConnectionError: If the ZIP contains no files or the
            response body is not a valid ZIP archive.
        """
        path = ACTIVITY_FIT_URL.format(activity_id=activity_id)
        resp = self._request(
            "GET",
            path,
            headers={
                "Accept": "application/zip, */*",
                "DI-Backend": "connectapi.garmin.com",
            },
            timeout=60,
        )
        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                fit_names = [n for n in zf.namelist() if n.lower().endswith(".fit")]
                if not fit_names:
                    raise GarminConnectionError(
                        f"FIT zip for activity {activity_id} contained no .fit files "
                        f"(found: {zf.namelist()})"
                    )
                if len(fit_names) > 1:
                    _LOGGER.warning(
                        "FIT zip for activity %s contained %d .fit files %s; using %s",
                        activity_id,
                        len(fit_names),
                        fit_names,
                        fit_names[0],
                    )
                return zf.read(fit_names[0])
        except zipfile.BadZipFile as exc:
            raise GarminConnectionError(
                f"FIT response for activity {activity_id} is not a valid ZIP"
            ) from exc

    # ------------------------------------------------------------------
    # Persistence Handlers
    # ------------------------------------------------------------------

    def _load_tokens(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.di_token = data.get("di_token")
        self.di_refresh_token = data.get("di_refresh_token")
        self.di_client_id = data.get("di_client_id")

        if not self.di_token:
            raise GarminAuthenticationError("Missing di_token in saved state.")

    def _load_tokens_from_json(self, data: str) -> None:
        parsed = json.loads(data)
        if not isinstance(parsed, dict):
            raise ValueError(
                f"GARMIN_TOKENS_JSON must be a JSON object, got {type(parsed).__name__}"
            )
        self.di_token = parsed.get("di_token")
        self.di_refresh_token = parsed.get("di_refresh_token")
        self.di_client_id = parsed.get("di_client_id")
        if not self.di_token:
            raise GarminAuthenticationError("Missing di_token in GARMIN_TOKENS_JSON")

    def export_session(self) -> str:
        if not self.di_token:
            raise GarminAuthenticationError("No active session to export.")
        return json.dumps(
            {
                "di_token": self.di_token,
                "di_refresh_token": self.di_refresh_token,
                "di_client_id": self.di_client_id,
            }
        )

    def _dump_tokens(self, path: str) -> None:
        data = json.dumps(
            {
                "di_token": self.di_token,
                "di_refresh_token": self.di_refresh_token,
                "di_client_id": self.di_client_id,
            }
        )
        dir_path = str(Path(path).parent)
        fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(data)
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ------------------------------------------------------------------
    # Authentication state
    # ------------------------------------------------------------------

    @property
    def is_authenticated(self) -> bool:
        return bool(self.di_token)

    def get_api_headers(self) -> dict[str, str]:
        if not self.is_authenticated:
            raise GarminAuthenticationError("Not authenticated")
        return _native_headers(
            {
                "Authorization": f"Bearer {self.di_token}",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Login Chain (Vendored Strategies)
    # ------------------------------------------------------------------

    def _login_chain(
        self,
        email: str,
        password: str,
        prompt_mfa: Callable[[], str] | None = None,
        return_on_mfa: bool = False,
    ) -> StrategyResult:
        """Runs the 5-strategy Cloudflare evasion chain."""
        strategy_chain: list[tuple[str, Callable[..., StrategyResult]]] = []

        if HAS_CFFI:
            strategy_chain.append(
                (
                    "portal+cffi",
                    lambda *a, **k: strategies.portal_web_login_cffi(self, *a, **k),
                )
            )
        strategy_chain.append(
            (
                "portal+requests",
                lambda *a, **k: strategies.portal_web_login_requests(self, *a, **k),
            )
        )
        if HAS_CFFI:
            strategy_chain.append(
                (
                    "mobile+cffi",
                    lambda *a, **k: strategies.mobile_login_cffi(self, *a, **k),
                )
            )
        strategy_chain.append(
            (
                "mobile+requests",
                lambda *a, **k: strategies.mobile_login_requests(self, *a, **k),
            )
        )
        if HAS_CFFI:
            strategy_chain.append(
                (
                    "widget+cffi",
                    lambda *a, **k: strategies.widget_login_cffi(self, *a, **k),
                )
            )

        _LOGGER.warning(
            "Starting login chain. Each strategy may sleep up to %.0fs for "
            "Cloudflare evasion — total login may take several minutes.",
            LOGIN_DELAY_MAX_S,
        )
        last_err: Exception | None = None
        for name, method in strategy_chain:
            try:
                _LOGGER.info(f"Trying login strategy: {name}")
                result = method(
                    email, password, prompt_mfa=prompt_mfa, return_on_mfa=return_on_mfa
                )

                if not (isinstance(result, tuple) and result[0] == "needs_mfa"):
                    self._load_profile()
                return result

            except GarminAuthenticationError:
                # If we get an explicit auth failure (401/403 with JSON), it means
                # the credentials are bad. Don't waste time trying other strategies.
                raise
            except (GarminTooManyRequestsError, GarminConnectionError) as e:
                _LOGGER.warning(f"Login strategy {name} failed: {e}")
                last_err = e
                continue
            except Exception as e:
                _LOGGER.warning(
                    f"Login strategy {name} failed unexpectedly", exc_info=True
                )
                last_err = e
                continue

        if isinstance(last_err, GarminTooManyRequestsError):
            raise last_err
        raise GarminConnectionError(
            f"All login strategies failed. Last error: {last_err}"
        )

    def _resume_login_chain(self, mfa_code: str) -> None:
        if self._pending_mfa == "widget":
            ticket = strategies.complete_mfa_widget(self, mfa_code)
            self._establish_session(ticket, service_url=f"{self._sso}/sso/embed")
        elif self._pending_mfa == "portal_web":
            strategies.complete_mfa_portal_web(self, mfa_code)
        elif self._pending_mfa == "mobile_cffi":
            strategies.complete_mfa_mobile_cffi(self, mfa_code)
        elif self._pending_mfa == "mobile_requests":
            strategies.complete_mfa_mobile_requests(self, mfa_code)
        else:
            raise GarminAuthenticationError("No pending MFA challenge to resume.")

        self._pending_mfa = None
        self._load_profile()

    # ------------------------------------------------------------------
    # Session establishment + DI token exchange
    # ------------------------------------------------------------------

    def _establish_session(self, ticket: str, service_url: str | None = None) -> None:
        self._exchange_service_ticket(ticket, service_url=service_url)

    @staticmethod
    def _di_post(url: str, **kwargs: Any) -> Any:
        if HAS_CFFI:
            # Always use curl_cffi for DI token exchange to avoid Cloudflare blocks
            # on the diauth endpoints.
            return cffi_requests.post(url, impersonate="chrome", **kwargs)
        return requests.post(url, **kwargs)

    def _exchange_service_ticket(
        self, ticket: str, service_url: str | None = None
    ) -> None:
        svc_url = service_url or MOBILE_SSO_SERVICE_URL
        di_token = None
        di_refresh = None
        di_client_id = None
        last_transport_error: BaseException | None = None
        last_server_error: tuple | None = None
        had_auth_failure = False

        for client_id in DI_CLIENT_IDS:
            try:
                r = self._di_post(
                    DI_TOKEN_URL,
                    headers=_native_headers(
                        {
                            "Authorization": _build_basic_auth(client_id),
                            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Cache-Control": "no-cache",
                        }
                    ),
                    data={
                        "client_id": client_id,
                        "service_ticket": ticket,
                        "grant_type": DI_GRANT_TYPE,
                        "service_url": svc_url,
                    },
                    timeout=30,
                )
            except _TRANSPORT_EXCEPTIONS as exc:  # type: ignore[misc]
                _LOGGER.debug(f"DI exchange transport error for {client_id}: {exc}")
                last_transport_error = exc
                continue

            if r.status_code == 429:
                raise GarminTooManyRequestsError("DI token exchange rate limited")
            if not r.ok:
                _LOGGER.debug(f"DI exchange failed for {client_id}: {r.status_code}")
                if r.status_code >= 500:
                    last_server_error = (r.status_code, r.text[:200])
                else:
                    had_auth_failure = True
                continue

            try:
                data = r.json()
                di_token = data["access_token"]
                di_refresh = data.get("refresh_token")
                if not di_refresh:
                    raise ValueError("response missing refresh_token")
                di_client_id = self._extract_client_id_from_jwt(di_token) or client_id
                break
            except Exception as e:
                _LOGGER.debug(f"DI token parse failed for {client_id}: {e}")
                continue

        if not di_token:
            if last_transport_error is not None:
                raise GarminConnectionError(
                    f"DI token exchange transport error: {last_transport_error}"
                )
            if last_server_error is not None and not had_auth_failure:
                raise GarminConnectionError(
                    f"DI token exchange server error: HTTP {last_server_error[0]}"
                )
            raise GarminAuthenticationError(
                "DI token exchange failed for all client IDs"
            )

        self.di_token = di_token
        self.di_refresh_token = di_refresh
        self.di_client_id = di_client_id

    def _refresh_di_token(self) -> None:
        if not self.di_refresh_token or not self.di_client_id:
            raise GarminAuthenticationError("No DI refresh token available")
        try:
            r = self._di_post(
                DI_TOKEN_URL,
                headers=_native_headers(
                    {
                        "Authorization": _build_basic_auth(self.di_client_id),
                        "Accept": "application/json",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Cache-Control": "no-cache",
                    }
                ),
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.di_client_id,
                    "refresh_token": self.di_refresh_token,
                },
                timeout=30,
            )
        except _TRANSPORT_EXCEPTIONS as exc:
            raise GarminConnectionError(f"Refresh transport error: {exc}") from exc

        if r.status_code == 429:
            raise GarminTooManyRequestsError("DI token refresh rate limited")
        if not r.ok:
            raise GarminAuthenticationError(f"DI token refresh failed: {r.status_code}")

        data = r.json()
        new_token = data.get("access_token")
        if not new_token:
            raise GarminAuthenticationError("DI refresh missing access_token")

        self.di_token = new_token
        self.di_refresh_token = data.get("refresh_token", self.di_refresh_token)
        self.di_client_id = (
            self._extract_client_id_from_jwt(new_token) or self.di_client_id
        )

    @staticmethod
    def _decode_jwt_payload(token: str) -> dict | None:
        try:
            parts = token.split(".")
            if len(parts) < 2:
                return None
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            return json.loads(base64.urlsafe_b64decode(payload_b64).decode())
        except Exception as e:
            _LOGGER.debug("Failed to decode JWT payload: %s", e)
            return None

    @staticmethod
    def _extract_client_id_from_jwt(token: str) -> str | None:
        payload = GarminClient._decode_jwt_payload(token)
        if payload and payload.get("client_id"):
            return str(payload["client_id"])
        return None

    def _token_expires_soon(self) -> bool:
        if not self.di_token:
            return False
        payload = self._decode_jwt_payload(self.di_token)
        if payload:
            exp = payload.get("exp")
            if exp and time.time() > (int(exp) - 900):
                return True
        return False

    def _refresh_session(self) -> None:
        if not self.di_token:
            return
        self._refresh_di_token()
        if self._tokenstore_path:
            try:
                self._dump_tokens(self._tokenstore_path)
            except Exception as e:
                _LOGGER.warning(f"Failed to persist refreshed tokens: {e}")

    def _sleep(self, seconds: float) -> None:
        """Sleep for the given duration. Isolated here so tests can stub it."""
        time.sleep(seconds)

    # ------------------------------------------------------------------
    # Profile & API execution
    # ------------------------------------------------------------------

    def _load_profile(self) -> None:
        if self.display_name is not None:
            return
        profile = self._connectapi(SOCIAL_PROFILE_URL)
        if not profile or "displayName" not in profile:
            raise GarminAuthenticationError("Profile response missing displayName")
        self.display_name = profile["displayName"]
        self.full_name = profile.get("fullName")

    def _connectapi(self, path: str, **kwargs: Any) -> Any:
        resp = self._request("GET", path, **kwargs)
        if resp.status_code == 204:
            return {}
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as err:
            raise GarminConnectionError(f"Invalid JSON: {resp.status_code}") from err

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        if self.is_authenticated and self._token_expires_soon():
            self._refresh_session()

        url = f"{self._connectapi_url}/{path.lstrip('/')}"
        kwargs.setdefault("timeout", 15)
        custom_headers = kwargs.pop("headers", {}) or {}
        if "Authorization" in custom_headers:
            raise ValueError(
                "_request does not allow overriding the Authorization header; "
                "use _di_post directly for non-DI requests."
            )

        merged = self.get_api_headers()
        merged.update(custom_headers)

        if self._api_session is None:
            from requests.adapters import HTTPAdapter

            self._api_session = requests.Session()
            adapter = HTTPAdapter(
                pool_connections=self._pool_connections,
                pool_maxsize=self._pool_maxsize,
            )
            self._api_session.mount("https://", adapter)

        resp = self._api_session.request(method, url, headers=merged, **kwargs)

        # Explicit exception mapping to ensure 429s bubble up as requested
        if resp.status_code == 429:
            raise GarminTooManyRequestsError("Garmin API Rate Limit (429) Reached.")
        elif resp.status_code == 401:
            self._refresh_session()
            merged = self.get_api_headers()
            merged.update(custom_headers)
            resp = self._api_session.request(method, url, headers=merged, **kwargs)
            if resp.status_code == 429:
                raise GarminTooManyRequestsError(
                    "Garmin API Rate Limit (429) Reached on token refresh retry."
                )
            if resp.status_code == 401:
                raise GarminAuthenticationError("Token expired and refresh failed.")
            if not resp.ok:
                raise GarminConnectionError(
                    f"API Error {resp.status_code}: {resp.text[:100]}"
                )
        elif not resp.ok:
            raise GarminConnectionError(
                f"API Error {resp.status_code}: {resp.text[:100]}"
            )

        return resp
