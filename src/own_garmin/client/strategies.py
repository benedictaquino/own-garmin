import logging
import random
import re
import time
from typing import Any, Callable

import requests

try:
    from curl_cffi import requests as cffi_requests

    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

from .constants import (
    LOGIN_DELAY_MAX_S,
    LOGIN_DELAY_MIN_S,
    MOBILE_SSO_CLIENT_ID,
    MOBILE_SSO_SERVICE_URL,
    MOBILE_SSO_USER_AGENT,
    PORTAL_SSO_CLIENT_ID,
    PORTAL_SSO_SERVICE_URL,
    _browser_headers,
)
from .exceptions import (
    GarminAuthenticationError,
    GarminConnectionError,
    GarminTooManyRequestsError,
)

_LOGGER = logging.getLogger(__name__)

# Merged flexible regex
_CSRF_RE = re.compile(r'name="_?csrf"\s+value=["\'](.+?)["\']')
_TITLE_RE = re.compile(r"<title>(.+?)</title>")
_TICKET_RE = re.compile(r'embed\?ticket=([^"]+)"')

# --------------------------------------------------------------------------------------
# SSO EMBED WIDGET LOGIN
# --------------------------------------------------------------------------------------


def widget_login_cffi(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Callable[[], str] | None = None,
    return_on_mfa: bool = False,
) -> tuple[str | None, Any]:
    if not HAS_CFFI:
        raise GarminConnectionError("curl_cffi not installed; widget+cffi unavailable")

    sess: Any = cffi_requests.Session(impersonate="chrome", timeout=30)
    sso_base = f"{client._sso}/sso"
    sso_embed = f"{sso_base}/embed"
    embed_params = {
        "id": "gauth-widget",
        "embedWidget": "true",
        "gauthHost": sso_base,
    }
    signin_params = {
        **embed_params,
        "gauthHost": sso_embed,
        "service": sso_embed,
        "source": sso_embed,
        "redirectAfterAccountLoginUrl": sso_embed,
        "redirectAfterAccountCreationUrl": sso_embed,
    }

    r = sess.get(sso_embed, params=embed_params)
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget login returned 429 on embed page")
    if not r.ok:
        raise GarminConnectionError(
            f"Widget login: embed page returned HTTP {r.status_code}"
        )

    r = sess.get(
        f"{sso_base}/signin", params=signin_params, headers={"Referer": sso_embed}
    )
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget login returned 429 on sign-in page")
    csrf_match = _CSRF_RE.search(r.text)
    if not csrf_match:
        raise GarminConnectionError(
            "Widget login: could not find CSRF token in sign-in page"
        )

    r = sess.post(
        f"{sso_base}/signin",
        params=signin_params,
        headers={"Referer": r.url},
        data={
            "username": email,
            "password": password,
            "embed": "true",
            "_csrf": csrf_match.group(1),
        },
        timeout=30,
    )

    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget login returned 429")
    if not r.ok:
        raise GarminConnectionError(
            f"Widget login: credential POST returned HTTP {r.status_code}"
        )

    title_match = _TITLE_RE.search(r.text)
    title = title_match.group(1) if title_match else ""

    if "MFA" in title:
        client._widget_session = sess
        client._widget_signin_params = signin_params
        client._widget_last_resp = r
        client._pending_mfa = "widget"
        if return_on_mfa:
            return "needs_mfa", sess
        if prompt_mfa:
            mfa_code = prompt_mfa()
            ticket = complete_mfa_widget(client, mfa_code)
            client._establish_session(ticket, service_url=sso_embed)
            return None, None
        raise GarminAuthenticationError(
            "MFA Required but no prompt_mfa mechanism supplied"
        )

    if title != "Success":
        title_lower = title.lower()
        if any(
            hint in title_lower for hint in ("locked", "invalid", "error", "incorrect")
        ):
            raise GarminAuthenticationError(
                f"Widget login: authentication failed ('{title}')"
            )
        raise GarminConnectionError(f"Widget login: unexpected title '{title}'")

    ticket_match = _TICKET_RE.search(r.text)
    if not ticket_match:
        raise GarminConnectionError(
            "Widget login: could not find service ticket in response"
        )
    client._establish_session(ticket_match.group(1), service_url=sso_embed)
    return None, None


def complete_mfa_widget(client: Any, mfa_code: str) -> str:
    sess = client._widget_session
    r = client._widget_last_resp
    csrf_match = _CSRF_RE.search(r.text)
    if not csrf_match:
        raise GarminAuthenticationError("Widget MFA: could not find CSRF token")

    r = sess.post(
        f"{client._sso}/sso/verifyMFA/loginEnterMfaCode",
        params=client._widget_signin_params,
        headers={"Referer": r.url},
        data={
            "mfa-code": mfa_code,
            "embed": "true",
            "_csrf": csrf_match.group(1),
            "fromPage": "setupEnterMfaCode",
        },
        timeout=30,
    )
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget MFA returned 429")
    if not r.ok:
        raise GarminConnectionError(
            f"Widget MFA: verify endpoint returned HTTP {r.status_code}"
        )

    title_match = _TITLE_RE.search(r.text)
    title = title_match.group(1) if title_match else ""
    if title != "Success":
        raise GarminAuthenticationError(f"Widget MFA verification failed: '{title}'")

    ticket_match = _TICKET_RE.search(r.text)
    if not ticket_match:
        raise GarminAuthenticationError("Widget MFA: could not find service ticket")
    return ticket_match.group(1)


# --------------------------------------------------------------------------------------
# PORTAL WEB LOGIN
# --------------------------------------------------------------------------------------


def portal_web_login_cffi(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Callable[[], str] | None = None,
    return_on_mfa: bool = False,
) -> tuple[str | None, Any]:
    if not HAS_CFFI:
        raise GarminConnectionError("curl_cffi not installed; portal+cffi unavailable")

    impersonations = ["safari", "safari_ios", "chrome120", "edge101", "chrome"]
    last_err: Exception | None = None
    last_429: GarminTooManyRequestsError | None = None
    rate_limited_count = 0
    for imp in impersonations:
        try:
            _LOGGER.debug("Trying portal+cffi with impersonation=%s", imp)
            sess: Any = cffi_requests.Session(impersonate=imp)
            return _portal_web_login(
                client,
                sess,
                email,
                password,
                prompt_mfa=prompt_mfa,
                return_on_mfa=return_on_mfa,
            )
        except GarminAuthenticationError:
            raise
        except GarminTooManyRequestsError as e:
            _LOGGER.debug("portal+cffi(%s) 429: %s", imp, e)
            last_err = e
            last_429 = e
            rate_limited_count += 1
            continue
        except (GarminConnectionError, Exception) as e:
            _LOGGER.debug("portal+cffi(%s) failed: %s", imp, e)
            last_err = e
            continue

    if rate_limited_count == len(impersonations) and last_429 is not None:
        raise last_429
    if last_err is not None:
        raise GarminConnectionError("All cffi impersonations failed") from last_err
    raise GarminConnectionError("All cffi impersonations failed")


def portal_web_login_requests(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Callable[[], str] | None = None,
    return_on_mfa: bool = False,
) -> tuple[str | None, Any]:
    sess = requests.Session()
    return _portal_web_login(
        client,
        sess,
        email,
        password,
        prompt_mfa=prompt_mfa,
        return_on_mfa=return_on_mfa,
    )


def _portal_web_login(
    client: Any,
    sess: Any,
    email: str,
    password: str,
    prompt_mfa: Callable[[], str] | None = None,
    return_on_mfa: bool = False,
) -> tuple[str | None, Any]:
    signin_url = f"{client._sso}/portal/sso/en-US/sign-in"
    browser_hdrs = _browser_headers()

    get_resp = sess.get(
        signin_url,
        params={"clientId": PORTAL_SSO_CLIENT_ID, "service": PORTAL_SSO_SERVICE_URL},
        headers={
            **browser_hdrs,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=30,
    )
    if get_resp.status_code == 429:
        raise GarminTooManyRequestsError("Portal login GET returned 429")
    if not get_resp.ok:
        raise GarminConnectionError(
            f"Portal login GET failed: HTTP {get_resp.status_code}"
        )

    delay_s = random.uniform(LOGIN_DELAY_MIN_S, LOGIN_DELAY_MAX_S)
    _LOGGER.info(
        "Portal login: waiting %.0fs to avoid Cloudflare rate limiting...", delay_s
    )
    time.sleep(delay_s)

    login_url = f"{client._sso}/portal/api/login"
    login_params = {
        "clientId": PORTAL_SSO_CLIENT_ID,
        "locale": "en-US",
        "service": PORTAL_SSO_SERVICE_URL,
    }
    post_headers = {
        **browser_hdrs,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": client._sso,
        "Referer": (
            f"{signin_url}?clientId={PORTAL_SSO_CLIENT_ID}"
            f"&service={PORTAL_SSO_SERVICE_URL}"
        ),
    }

    r = sess.post(
        login_url,
        params=login_params,
        headers=post_headers,
        json={
            "username": email,
            "password": password,
            "rememberMe": True,
            "captchaToken": "",
        },
        timeout=30,
    )

    if r.status_code == 429:
        raise GarminTooManyRequestsError("Portal login returned 429")
    if not r.ok:
        raise GarminConnectionError(f"Portal login POST failed: HTTP {r.status_code}")

    try:
        res = r.json()
    except Exception as err:
        raise GarminConnectionError(
            f"Portal login returned non-JSON: {r.status_code}"
        ) from err

    resp_type = res.get("responseStatus", {}).get("type")
    if resp_type == "MFA_REQUIRED":
        client._mfa_method = res.get("customerMfaInfo", {}).get(
            "mfaLastMethodUsed", "email"
        )
        client._mfa_portal_web_session = sess
        client._mfa_portal_web_params = login_params
        client._mfa_portal_web_headers = post_headers
        client._pending_mfa = "portal_web"
        if return_on_mfa:
            return "needs_mfa", sess
        if prompt_mfa:
            mfa_code = prompt_mfa()
            complete_mfa_portal_web(client, mfa_code)
            return None, None
        raise GarminAuthenticationError(
            "MFA Required but no prompt_mfa mechanism supplied"
        )

    if resp_type == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket, service_url=PORTAL_SSO_SERVICE_URL)
        return None, None

    if resp_type == "INVALID_USERNAME_PASSWORD":
        raise GarminAuthenticationError("Invalid Username or Password")
    raise GarminConnectionError(f"Portal web login failed: {res}")


def complete_mfa_portal_web(client: Any, mfa_code: str) -> None:
    sess = client._mfa_portal_web_session
    mfa_json = {
        "mfaMethod": getattr(client, "_mfa_method", "email"),
        "mfaVerificationCode": mfa_code,
        "rememberMyBrowser": True,
        "reconsentList": [],
        "mfaSetup": False,
    }
    mfa_endpoints = [
        (
            f"{client._sso}/portal/api/mfa/verifyCode",
            client._mfa_portal_web_params,
            client._mfa_portal_web_headers,
        ),
        (
            f"{client._sso}/mobile/api/mfa/verifyCode",
            {
                "clientId": MOBILE_SSO_CLIENT_ID,
                "locale": "en-US",
                "service": MOBILE_SSO_SERVICE_URL,
            },
            client._mfa_portal_web_headers,
        ),
    ]

    failures = []
    for mfa_url, params, headers in mfa_endpoints:
        try:
            r = sess.post(
                mfa_url, params=params, headers=headers, json=mfa_json, timeout=30
            )
            if r.status_code == 429:
                failures.append(f"{mfa_url}: 429")
                continue
            if not r.ok:
                failures.append(f"{mfa_url}: {r.status_code}")
                continue
            res = r.json()
            if res.get("responseStatus", {}).get("type") == "SUCCESSFUL":
                ticket = res["serviceTicketId"]
                svc_url = (
                    PORTAL_SSO_SERVICE_URL
                    if "/portal/" in mfa_url
                    else MOBILE_SSO_SERVICE_URL
                )
                client._establish_session(ticket, service_url=svc_url)
                return
            failures.append(f"{mfa_url}: {res}")
        except Exception as e:
            failures.append(f"{mfa_url}: {e}")

    raise GarminAuthenticationError(f"MFA Verification failed: {failures}")


# --------------------------------------------------------------------------------------
# MOBILE SSO LOGIN
# --------------------------------------------------------------------------------------


def mobile_login_cffi(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Callable[[], str] | None = None,
    return_on_mfa: bool = False,
) -> tuple[str | None, Any]:
    if not HAS_CFFI:
        raise GarminConnectionError("curl_cffi not installed; mobile+cffi unavailable")

    sess: Any = cffi_requests.Session(impersonate="safari")
    signin_url = f"{client._sso}/mobile/sso/en_US/sign-in"
    get_resp = sess.get(
        signin_url,
        params={"clientId": MOBILE_SSO_CLIENT_ID, "service": MOBILE_SSO_SERVICE_URL},
        headers={
            "User-Agent": MOBILE_SSO_USER_AGENT,
            "accept": "text/html,application/xhtml+xml",
        },
        timeout=30,
    )
    if get_resp.status_code == 429:
        raise GarminTooManyRequestsError("Mobile portal login GET returned 429")
    if not get_resp.ok:
        raise GarminConnectionError(
            f"Mobile portal login GET failed: HTTP {get_resp.status_code}"
        )

    delay_s = random.uniform(LOGIN_DELAY_MIN_S, LOGIN_DELAY_MAX_S)
    _LOGGER.info("Mobile portal login: waiting %.0fs...", delay_s)
    time.sleep(delay_s)

    login_params = {
        "clientId": MOBILE_SSO_CLIENT_ID,
        "locale": "en-US",
        "service": MOBILE_SSO_SERVICE_URL,
    }
    post_headers = {
        "User-Agent": MOBILE_SSO_USER_AGENT,
        "content-type": "application/json",
        "origin": client._sso,
        "referer": (
            f"{signin_url}?clientId={MOBILE_SSO_CLIENT_ID}"
            f"&service={MOBILE_SSO_SERVICE_URL}"
        ),
    }
    r = sess.post(
        f"{client._sso}/mobile/api/login",
        params=login_params,
        headers=post_headers,
        json={
            "username": email,
            "password": password,
            "rememberMe": True,
            "captchaToken": "",
        },
        timeout=30,
    )
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Too many requests during mobile portal login")
    if not r.ok:
        raise GarminConnectionError(f"Mobile portal login POST failed: {r.status_code}")

    res = r.json()
    resp_type = res.get("responseStatus", {}).get("type")
    if resp_type == "MFA_REQUIRED":
        client._mfa_method = res.get("customerMfaInfo", {}).get(
            "mfaLastMethodUsed", "email"
        )
        client._mfa_cffi_session = sess
        client._mfa_cffi_params = login_params
        client._mfa_cffi_headers = post_headers
        client._pending_mfa = "mobile_cffi"
        if return_on_mfa:
            return "needs_mfa", sess
        if prompt_mfa:
            mfa_code = prompt_mfa()
            complete_mfa_mobile_cffi(client, mfa_code)
            return None, None
        raise GarminAuthenticationError("MFA Required")

    if resp_type == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket)
        return None, None

    if resp_type == "INVALID_USERNAME_PASSWORD":
        raise GarminAuthenticationError("Invalid Username or Password")
    raise GarminAuthenticationError(f"Mobile cffi login failed: {res}")


def complete_mfa_mobile_cffi(client: Any, mfa_code: str) -> None:
    sess = client._mfa_cffi_session
    r = sess.post(
        f"{client._sso}/mobile/api/mfa/verifyCode",
        params=client._mfa_cffi_params,
        headers=client._mfa_cffi_headers,
        json={
            "mfaMethod": getattr(client, "_mfa_method", "email"),
            "mfaVerificationCode": mfa_code,
            "rememberMyBrowser": True,
            "reconsentList": [],
            "mfaSetup": False,
        },
        timeout=30,
    )
    if not r.ok:
        raise GarminConnectionError(f"MFA Verification failed: HTTP {r.status_code}")
    res = r.json()
    if res.get("responseStatus", {}).get("type") == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket)
        return
    raise GarminAuthenticationError(f"MFA Verification failed: {res}")


def mobile_login_requests(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Callable[[], str] | None = None,
    return_on_mfa: bool = False,
) -> tuple[str | None, Any]:
    sess = requests.Session()
    sess.headers.update({"User-Agent": MOBILE_SSO_USER_AGENT})
    get_resp = sess.get(
        f"{client._sso}/mobile/sso/en_US/sign-in",
        params={"clientId": MOBILE_SSO_CLIENT_ID, "service": MOBILE_SSO_SERVICE_URL},
        timeout=30,
    )
    if not get_resp.ok:
        raise GarminConnectionError(f"Mobile login GET failed: {get_resp.status_code}")

    delay_s = random.uniform(LOGIN_DELAY_MIN_S, LOGIN_DELAY_MAX_S)
    _LOGGER.info("Mobile login: waiting %.0fs...", delay_s)
    time.sleep(delay_s)

    r = sess.post(
        f"{client._sso}/mobile/api/login",
        params={
            "clientId": MOBILE_SSO_CLIENT_ID,
            "locale": "en-US",
            "service": MOBILE_SSO_SERVICE_URL,
        },
        json={
            "username": email,
            "password": password,
            "rememberMe": True,
            "captchaToken": "",
        },
        timeout=30,
    )
    if not r.ok:
        raise GarminConnectionError(f"Login failed: {r.status_code}")

    res = r.json()
    resp_type = res.get("responseStatus", {}).get("type")
    if resp_type == "MFA_REQUIRED":
        client._mfa_method = res.get("customerMfaInfo", {}).get(
            "mfaLastMethodUsed", "email"
        )
        client._mfa_session = sess
        client._pending_mfa = "mobile_requests"
        if return_on_mfa:
            return "needs_mfa", client._mfa_session
        if prompt_mfa:
            mfa_code = prompt_mfa()
            complete_mfa_mobile_requests(client, mfa_code)
            return None, None
        raise GarminAuthenticationError("MFA Required")

    if resp_type == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket)
        return None, None

    raise GarminAuthenticationError(f"Login failed: {res}")


def complete_mfa_mobile_requests(client: Any, mfa_code: str) -> None:
    r = client._mfa_session.post(
        f"{client._sso}/mobile/api/mfa/verifyCode",
        params={
            "clientId": MOBILE_SSO_CLIENT_ID,
            "locale": "en-US",
            "service": MOBILE_SSO_SERVICE_URL,
        },
        json={
            "mfaMethod": getattr(client, "_mfa_method", "email"),
            "mfaVerificationCode": mfa_code,
            "rememberMyBrowser": True,
            "reconsentList": [],
            "mfaSetup": False,
        },
        timeout=30,
    )
    if not r.ok:
        raise GarminConnectionError(f"MFA Verification failed: {r.status_code}")
    res = r.json()
    if res.get("responseStatus", {}).get("type") == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket)
        return
    raise GarminAuthenticationError(f"MFA Verification failed: {res}")
