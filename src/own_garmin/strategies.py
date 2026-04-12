import logging
from typing import Any, Callable, Optional, Tuple

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
    Primary Stealth Strategy: Uses curl_cffi to impersonate Chrome.
    This bypasses most Cloudflare 'I am a bot' challenges.
    """
    if not cffi_requests:
        raise ImportError("curl_cffi is required for this login strategy.")

    session = cffi_requests.Session()
    # 'impersonate' is the key secret for Stealth ADR
    session.impersonate = "chrome110"

    # 1. Get Login Ticket from SSO Portal
    url = "https://sso.garmin.com/sso/signin"
    params = {
        "service": "https://connect.garmin.com/modern/",
        "gauthHost": "https://sso.garmin.com/sso",
        "id": "gauth-widget",
        "embedWidget": "false",
    }

    try:
        response = session.get(url, params=params, timeout=30)
        if response.status_code == 429:
            raise GarminTooManyRequestsError("Cloudflare Rate Limit (429) hit.")

        # 2. Post Credentials
        # This part requires parsing the 'csrf' and 'flowExecutionKey'
        # from the response. For brevity in this plan, we assume
        # the standard POST used in the DI flow.
        data = {
            "username": email,
            "password": password,
            "embed": "false",
            "_eventId": "submit",
        }

        # In a real implementation, you would extract the hidden fields here
        login_response = session.post(url, params=params, data=data, timeout=30)

        if "Invalid display name or password" in login_response.text:
            raise GarminAuthenticationError("Invalid credentials provided.")

        # 3. Handle MFA if triggered
        if "mfa-code" in login_response.text:
            if return_on_mfa:
                client._mfa_cffi_session = session
                return "needs_mfa", None
            # logic for immediate prompt...

        # 4. Extract Service Ticket (ST) from URL
        # The ST is used to exchange for the Bearer token in client.py
        if "ticket=" in login_response.url:
            ticket = login_response.url.split("ticket=")[1].split("#")[0]
            client._establish_session(ticket)
            return None, None

        raise GarminAuthenticationError("Failed to retrieve Service Ticket.")

    except Exception as e:
        logger.error(f"CFFI Strategy failed: {e}")
        raise
