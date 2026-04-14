class GarminError(Exception):
    """Base exception for all Garmin related issues."""


class GarminAuthenticationError(GarminError):
    """Raised when credentials or tokens are invalid/expired."""


class GarminTooManyRequestsError(GarminError):
    """Raised on 429 status codes (Rate Limiting/Cloudflare)."""


class GarminConnectionError(GarminError):
    """Raised on transport layer failures."""
