import base64

# Garmin Direct Integration (DI) Client IDs for Mobile SSO
# These mimic the actual IDs used by the Garmin Connect Mobile apps.
DI_CLIENT_IDS = [
    "0f89ff8b-da09-408a-bf7c-6e69661501bc",  # Android
    "736a444d-5a81-4275-816d-da6764f69707",  # iOS
    "b8f8888b-da09-408a-bf7c-6e69661501bc",
]

# OAuth / SSO Endpoints
MOBILE_SSO_SERVICE_URL = "https://sso.garmin.com/sso/embed"
DI_TOKEN_URL = "https://connectapi.garmin.com/oauth-service/oauth/exchange/user/2.0"
DI_GRANT_TYPE = "exchange_service_ticket"
SOCIAL_PROFILE_URL = "/userprofile-service/socialProfile"


def _build_basic_auth(client_id: str) -> str:
    """Builds the Basic Auth header for DI token exchange."""
    # Garmin uses empty password for these specific client IDs
    auth_str = f"{client_id}:"
    encoded = base64.b64encode(auth_str.encode()).decode()
    return f"Basic {encoded}"


def _native_headers(extra_headers: dict = None) -> dict:
    """Standard headers used to mimic the Garmin Connect Mobile app."""
    headers = {
        "User-Agent": "com.garmin.android.apps.connectmobile",
        "X-app-ver": "4.74",
        "X-m-with": "m",
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers
