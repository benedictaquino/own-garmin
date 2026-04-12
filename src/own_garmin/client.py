import base64
import contextlib
import json
import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException as _CffiRequestException

    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False
    _CffiRequestException = None  # type: ignore[assignment,misc]

if HAS_CFFI:
    _TRANSPORT_EXCEPTIONS: Tuple[type, ...] = (
        requests.RequestException,
        _CffiRequestException,
    )
else:
    _TRANSPORT_EXCEPTIONS = (requests.RequestException,)

# Import path resolution from own-garmin
from own_garmin import paths

# Note: This assumes you have copied the sibling modules from garmin-health-data
# (.strategies, .constants, .exceptions) into your own_garmin directory.
from . import strategies
from .constants import (
    DI_CLIENT_IDS,
    DI_GRANT_TYPE,
    DI_TOKEN_URL,
    MOBILE_SSO_SERVICE_URL,
    SOCIAL_PROFILE_URL,
    _build_basic_auth,
    _native_headers,
)
from .exceptions import (
    GarminAuthenticationError,
    GarminConnectionError,
    GarminTooManyRequestsError,
)

_LOGGER = logging.getLogger(__name__)


class GarminClient:
    """
    Garmin Connect client: Auto-resuming session + API access.
    Completely decoupled from python-garminconnect and garth.
    """

    def __init__(self) -> None:
        self.domain = "garmin.com"
        self._sso = f"https://sso.{self.domain}"
        self._connect = f"https://connect.{self.domain}"
        self._connectapi_url = f"https://connectapi.{self.domain}"

        # DI Bearer tokens
        self.di_token: Optional[str] = None
        self.di_refresh_token: Optional[str] = None
        self.di_client_id: Optional[str] = None

        self.display_name: Optional[str] = None
        self.full_name: Optional[str] = None

        self._api_session: Optional[requests.Session] = None
        self._pool_connections: int = 20
        self._pool_maxsize: int = 20

        # 1. Resolve session_dir() from paths.py, mkdir -p it.
        self.session_dir = Path(paths.session_dir())
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self._tokenstore_path = str(self.session_dir / "garmin_tokens.json")

        # 2. Try to resume the saved session.
        resume_success = False
        try:
            if Path(self._tokenstore_path).exists():
                self._load_tokens(self._tokenstore_path)
                self._load_profile()
                resume_success = True
                _LOGGER.info("Session resumed successfully from local token store.")
            else:
                raise FileNotFoundError("Local token file does not exist.")
        except Exception as e:
            # Only catch auth-resume failures to trigger re-login.
            _LOGGER.info(
                f"Auth-resume failed ({type(e).__name__}: {e}). Triggering full login."
            )

        # 3. If resume failed, read env vars, login, and persist.
        if not resume_success:
            from dotenv import load_dotenv

            load_dotenv()
            email = os.getenv("GARMIN_EMAIL")
            password = os.getenv("GARMIN_PASSWORD")

            # 4. Raise clear error if credentials missing and resume failed.
            if not email or not password:
                raise ValueError(
                    "Missing GARMIN_EMAIL or GARMIN_PASSWORD in environment. "
                    "Cannot perform fresh login, and session resume failed."
                )

            _LOGGER.info("Initiating fresh 5-strategy login chain...")
            # We use return_on_mfa=True to pause the chain and prompt via CLI
            result = self._login_chain(email, password, return_on_mfa=True)

            if result and result[0] == "needs_mfa":
                mfa_code = input("\nEnter Garmin MFA code: ")
                self._resume_login_chain(result[1], mfa_code)

            self._dump_tokens(self._tokenstore_path)
            _LOGGER.info(
                f"Login successful. Tokens persisted to {self._tokenstore_path}"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_activities(self, start: date, end: date) -> list[dict]:
        """Fetch summary dicts of activities within a date range."""
        path = (
            "/activitylist-service/activities/search/activities?"
            f"startDate={start.isoformat()}&endDate={end.isoformat()}"
        )
        return self._connectapi(path)

    def get_activity(self, activity_id: int) -> dict:
        """Fetch the full detail dictionary for a specific activity."""
        path = f"/activity-service/activity/{activity_id}"
        return self._connectapi(path)

    # ------------------------------------------------------------------
    # Persistence Handlers (Replacing garth.dump/load)
    # ------------------------------------------------------------------

    def _load_tokens(self, path: str) -> None:
        with open(path, "r") as f:
            data = json.load(f)
        self.di_token = data.get("di_token")
        self.di_refresh_token = data.get("di_refresh_token")
        self.di_client_id = data.get("di_client_id")

        if not self.di_token:
            raise GarminAuthenticationError("Missing di_token in saved state.")

    def _dump_tokens(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(
                {
                    "di_token": self.di_token,
                    "di_refresh_token": self.di_refresh_token,
                    "di_client_id": self.di_client_id,
                },
                f,
            )

    # ------------------------------------------------------------------
    # Authentication state
    # ------------------------------------------------------------------

    @property
    def is_authenticated(self) -> bool:
        return bool(self.di_token)

    def get_api_headers(self) -> Dict[str, str]:
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
        prompt_mfa: Optional[Callable[[], str]] = None,
        return_on_mfa: bool = False,
    ) -> Tuple[Optional[str], Any]:
        """Runs the 5-strategy Cloudflare evasion chain."""
        strategy_chain: List[Tuple[str, Callable[..., Tuple[Optional[str], Any]]]] = []

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
                ("mobile+cffi", lambda *a, **k: strategies.portal_login(self, *a, **k))
            )
        strategy_chain.append(
            ("mobile+requests", lambda *a, **k: strategies.mobile_login(self, *a, **k))
        )
        if HAS_CFFI:
            strategy_chain.append(
                (
                    "widget+cffi",
                    lambda *a, **k: strategies.widget_login_cffi(self, *a, **k),
                )
            )

        last_err: Optional[Exception] = None
        for name, method in strategy_chain:
            try:
                _LOGGER.info("Trying login strategy: %s", name)
                result = method(
                    email, password, prompt_mfa=prompt_mfa, return_on_mfa=return_on_mfa
                )

                if not (isinstance(result, tuple) and result[0] == "needs_mfa"):
                    self._load_profile()
                return result

            except GarminAuthenticationError:
                raise  # Bad credentials, fail immediately
            except (GarminTooManyRequestsError, GarminConnectionError) as e:
                _LOGGER.warning("Login strategy %s failed: %s", name, e)
                last_err = e
                continue
            except Exception as e:
                _LOGGER.warning("Login strategy %s failed: %s", name, e)
                last_err = e
                continue

        if isinstance(last_err, GarminTooManyRequestsError):
            raise last_err
        raise GarminConnectionError(
            f"All login strategies failed. Last error: {last_err}"
        )

    def _resume_login_chain(
        self, _client_state: Any, mfa_code: str
    ) -> Tuple[Optional[str], Any]:
        if hasattr(self, "_widget_session"):
            ticket = strategies.complete_mfa_widget(self, mfa_code)
            self._establish_session(
                ticket, sess=self._widget_session, service_url=f"{self._sso}/sso/embed"
            )
        elif hasattr(self, "_mfa_portal_web_session"):
            strategies.complete_mfa_portal_web(self, mfa_code)
        elif hasattr(self, "_mfa_cffi_session"):
            strategies.complete_mfa_portal(self, mfa_code)
        elif hasattr(self, "_mfa_session"):
            strategies.complete_mfa(self, mfa_code)
        else:
            raise GarminAuthenticationError("No pending MFA challenge to resume.")

        self._load_profile()
        return None, None

    # ------------------------------------------------------------------
    # Session establishment + DI token exchange
    # ------------------------------------------------------------------

    def _establish_session(
        self, ticket: str, sess: Any = None, service_url: Optional[str] = None
    ) -> None:
        del sess
        self._exchange_service_ticket(ticket, service_url=service_url)

    @staticmethod
    def _http_post(url: str, **kwargs: Any) -> Any:
        if HAS_CFFI:
            return cffi_requests.post(url, impersonate="chrome", **kwargs)
        return requests.post(url, **kwargs)

    def _exchange_service_ticket(
        self, ticket: str, service_url: Optional[str] = None
    ) -> None:
        svc_url = service_url or MOBILE_SSO_SERVICE_URL
        di_token = None
        di_refresh = None
        di_client_id = None
        had_auth_failure = False

        for client_id in DI_CLIENT_IDS:
            try:
                r = self._http_post(
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
            except _TRANSPORT_EXCEPTIONS:
                continue

            if r.status_code == 429:
                raise GarminTooManyRequestsError("DI token exchange rate limited")
            if not r.ok:
                if r.status_code < 500:
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
            except Exception:
                continue

            if not di_token:
                if had_auth_failure:
                    # We know the server specifically rejected the tickets
                    raise GarminAuthenticationError(
                        "Invalid Service Ticket or Client ID mismatch."
                    )
                else:
                    # We likely hit 500s or timeouts across the board
                    raise GarminConnectionError(
                        "Could not reach Garmin servers for token exchange."
                    )

        self.di_token = di_token
        self.di_refresh_token = di_refresh
        self.di_client_id = di_client_id

    def _refresh_di_token(self) -> None:
        if not self.di_refresh_token or not self.di_client_id:
            raise GarminAuthenticationError("No DI refresh token available")
        try:
            r = self._http_post(
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
            self._extract_client_id_from_jwt(self.di_token) or self.di_client_id
        )

    @staticmethod
    def _extract_client_id_from_jwt(token: str) -> Optional[str]:
        try:
            parts = token.split(".")
            if len(parts) < 2:
                return None
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode())
            return str(payload.get("client_id")) if payload.get("client_id") else None
        except Exception:
            return None

    def _token_expires_soon(self) -> bool:
        if not self.di_token:
            return False
        try:
            parts = str(self.di_token).split(".")
            if len(parts) >= 2:
                payload = json.loads(
                    base64.urlsafe_b64decode(
                        (parts[1] + "=" * (-len(parts[1]) % 4)).encode()
                    ).decode()
                )
                exp = payload.get("exp")
                if exp and time.time() > (int(exp) - 900):
                    return True
        except Exception:
            pass
        return False

    def _refresh_session(self) -> None:
        if not self.di_token:
            return
        self._refresh_di_token()
        with contextlib.suppress(Exception):
            self._dump_tokens(self._tokenstore_path)

    # ------------------------------------------------------------------
    # Profile & API execution
    # ------------------------------------------------------------------

    def _load_profile(self) -> None:
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

        merged = self.get_api_headers()
        merged.update(custom_headers)

        if self._api_session is None:
            self._api_session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
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
            if resp.status_code == 401:
                raise GarminAuthenticationError("Token expired and refresh failed.")
        elif not resp.ok:
            raise GarminConnectionError(
                f"API Error {resp.status_code}: {resp.text[:100]}"
            )

        return resp
