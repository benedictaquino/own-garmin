import base64

# --------------------------------------------------------------------------------------
# OPTIONAL CURL_CFFI DETECTION
# --------------------------------------------------------------------------------------

try:
    import curl_cffi.requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException as CffiRequestException

    HAS_CFFI = True
except ImportError:
    cffi_requests = None  # type: ignore[assignment]
    CffiRequestException = None  # type: ignore[assignment]
    HAS_CFFI = False

# --------------------------------------------------------------------------------------
# SSO ENDPOINTS AND CLIENT IDENTIFIERS
# --------------------------------------------------------------------------------------

# Mobile SSO (Android Garmin Connect Mobile app flow).
MOBILE_SSO_CLIENT_ID = "GCM_ANDROID_DARK"
MOBILE_SSO_SERVICE_URL = "https://mobile.integration.garmin.com/gcm/android"
MOBILE_SSO_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13; sdk_gphone64_arm64 Build/TE1A.220922.025; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/132.0.0.0 "
    "Mobile Safari/537.36"
)

# Web portal (desktop browser flow that connect.garmin.com itself uses).
PORTAL_SSO_CLIENT_ID = "GarminConnect"
PORTAL_SSO_SERVICE_URL = "https://connect.garmin.com/app"
DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# --------------------------------------------------------------------------------------
# NATIVE API HEADERS (used for DI token exchange and authenticated API calls)
# --------------------------------------------------------------------------------------

NATIVE_API_USER_AGENT = "GCM-Android-5.23"
NATIVE_X_GARMIN_USER_AGENT = (
    "com.garmin.android.apps.connectmobile/5.23; ; Google/sdk_gphone64_arm64/google; "
    "Android/33; Dalvik/2.1.0"
)

# --------------------------------------------------------------------------------------
# DI OAUTH2 TOKEN EXCHANGE
# --------------------------------------------------------------------------------------

DI_TOKEN_URL = "https://diauth.garmin.com/di-oauth2-service/oauth/token"
DI_GRANT_TYPE = (
    "https://connectapi.garmin.com/di-oauth2-service/oauth/grant/service_ticket"
)

# Garmin rotates accepted DI client IDs each quarter.
DI_CLIENT_IDS = (
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2025Q2",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2024Q4",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI",
)

# --------------------------------------------------------------------------------------
# CLOUDFLARE WAF ANTI-RATE-LIMIT DELAY
# --------------------------------------------------------------------------------------

LOGIN_DELAY_MIN_S = 30.0
LOGIN_DELAY_MAX_S = 45.0

# --------------------------------------------------------------------------------------
# API URL TEMPLATES
# --------------------------------------------------------------------------------------

SOCIAL_PROFILE_URL = "/userprofile-service/socialProfile"

# Activities
ACTIVITIES_URL = "/activitylist-service/activities/search/activities"
ACTIVITY_URL = "/activity-service/activity"
ACTIVITY_DETAILS_URL = "/activity-service/activity/{activity_id}/details"
ACTIVITY_FIT_URL = "/download-service/files/activity/{activity_id}"

# --------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------------------------------------------


def _browser_headers() -> dict[str, str]:
    """Return standard desktop browser User-Agent header."""
    return {"User-Agent": DESKTOP_USER_AGENT}


def _build_basic_auth(client_id: str) -> str:
    """Build a Basic auth header value for the DI OAuth2 token endpoint."""
    return "Basic " + base64.b64encode(f"{client_id}:".encode()).decode()


def _native_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Headers for native (Android app) API calls and DI token exchange."""
    headers: dict[str, str] = {
        "User-Agent": NATIVE_API_USER_AGENT,
        "X-Garmin-User-Agent": NATIVE_X_GARMIN_USER_AGENT,
        "X-Garmin-Paired-App-Version": "10861",
        "X-Garmin-Client-Platform": "Android",
        "X-App-Ver": "10861",
        "X-Lang": "en",
        "X-GCExperience": "GC5",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra:
        headers.update(extra)
    return headers
