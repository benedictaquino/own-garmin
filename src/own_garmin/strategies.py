import logging
import re
from typing import Any, Callable, Optional, Tuple

from .constants import MOBILE_SSO_SERVICE_URL
from .exceptions import GarminAuthenticationError, GarminTooManyRequestsError

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

logger = logging.getLogger(__name__)


def portal_web_login_cffi(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Direct Integration Strategy: Mimics a browser login to Garmin SSO.
    Uses curl_cffi to bypass Cloudflare and obtain a Service Ticket (ST).
    """
    if not cffi_requests:
        raise ImportError("curl_cffi is required for the stealth login strategy.")

    # Create a session with a specific browser fingerprint
    session = cffi_requests.Session()
    session.impersonate = "chrome110"

    # 1. Initial GET to fetch CSRF and Flow Execution tokens
    params = {
        "service": "https://connect.garmin.com/modern/",
        "gauthHost": "https://sso.garmin.com/sso",
        "id": "gauth-widget",
        "embedWidget": "false",
    }

    response = session.get(MOBILE_SSO_SERVICE_URL, params=params, timeout=30)
    if response.status_code == 429:
        raise GarminTooManyRequestsError("Triggered Cloudflare rate limit.")

    # Extract hidden form fields required by Garmin's stateful SSO
    try:
        csrf = re.search(r'name="_csrf" value="(.+?)"', response.text).group(1)
        execution = re.search(r'name="execution" value="(.+?)"', response.text).group(1)
    except AttributeError:
        logger.error("Could not find CSRF or Execution tokens in SSO page.")
        raise GarminAuthenticationError(
            "SSO page structure changed or blocked by Cloudflare."
        )

    # 2. POST credentials with session state tokens
    data = {
        "username": email,
        "password": password,
        "embed": "false",
        "_csrf": csrf,
        "execution": execution,
        "_eventId": "submit",
    }

    login_response = session.post(
        MOBILE_SSO_SERVICE_URL,
        params=params,
        data=data,
        timeout=30,
        follow_redirects=True,
    )

    # 3. Handle Multi-Factor Authentication (MFA)
    if "mfa-code" in login_response.text or "mfa-pin" in login_response.url:
        # Save state to the client for CLI resume
        client._mfa_csrf = csrf
        client._mfa_execution = execution
        client._mfa_cffi_session = session

        if return_on_mfa:
            return "needs_mfa", None

        # If a prompt function was provided, handle MFA immediately
        if prompt_mfa:
            mfa_code = prompt_mfa()
            mfa_data = {
                "embed": "false",
                "mfa-code": mfa_code,
                "fromRedirect": "true",
                "_csrf": csrf,
                "execution": execution,
                "_eventId": "submit",
            }

            login_response = session.post(
                MOBILE_SSO_SERVICE_URL,
                params=params,
                data=mfa_data,
                timeout=30,
                follow_redirects=True,
            )
        else:
            raise GarminAuthenticationError(
                "MFA triggered but no prompt method provided."
            )

    # 4. Final Validation and Ticket Extraction
    # Success is indicated by a redirect URL containing 'ticket=ST-XXXXXX'
    if "ticket=" in login_response.url:
        ticket_match = re.search(r"ticket=(ST-[\w-]+)", login_response.url)
        if ticket_match:
            return ticket_match.group(1), None

    if "Invalid display name or password" in login_response.text:
        raise GarminAuthenticationError("Invalid email or password.")

    logger.debug(f"Login failed. Final URL: {login_response.url}")
    raise GarminAuthenticationError("Failed to retrieve Service Ticket from SSO.")
