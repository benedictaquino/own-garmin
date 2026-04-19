import base64
import io
import json
import time
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from own_garmin.client import (
    GarminAuthenticationError,
    GarminClient,
    GarminConnectionError,
    GarminTooManyRequestsError,
)
from own_garmin.client.strategies import _CSRF_RE, _TICKET_RE, _TITLE_RE

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_jwt(payload: dict) -> str:
    """Build a minimal JWT string with the given payload."""
    b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"header.{b64}.sig"


def _make_zip(*entries: tuple[str, bytes]) -> bytes:
    """Build a ZIP archive in memory from (filename, content) pairs."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in entries:
            zf.writestr(name, content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_paths(mocker, tmp_path):
    """Mocks paths.session_dir to a temporary directory."""
    m_paths = mocker.patch("own_garmin.client.client.paths")
    m_paths.session_dir.return_value = tmp_path
    return tmp_path


@pytest.fixture
def mock_strategies(mocker):
    """Mocks the login strategies to prevent real network calls."""
    return mocker.patch("own_garmin.client.client.strategies")


@pytest.fixture
def authenticated_client(mock_paths):
    """Returns a GarminClient with a valid token pre-loaded from disk."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(
        json.dumps(
            {
                "di_token": "mock_access_token",
                "di_refresh_token": "mock_refresh_token",
                "di_client_id": "mock_client_id",
            }
        )
    )
    with patch.object(GarminClient, "_load_profile", return_value=None):
        return GarminClient()


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def test_init_resume_success(mock_paths, mock_strategies):
    """Scenario: Valid garmin_tokens.json exists, resume succeeds."""
    token_file = mock_paths / "garmin_tokens.json"
    token_data = {
        "di_token": "mock_access_token",
        "di_refresh_token": "mock_refresh_token",
        "di_client_id": "mock_client_id",
    }
    token_file.write_text(json.dumps(token_data))

    with patch.object(GarminClient, "_load_profile", return_value=None):
        client = GarminClient()

    assert client.di_token == "mock_access_token"
    assert mock_strategies.portal_web_login_cffi.call_count == 0


def test_init_resume_fails_login_succeeds(mock_paths, mock_strategies, monkeypatch):
    """Scenario: No tokens exist, login chain is triggered and persists tokens."""
    monkeypatch.setenv("GARMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "secret123")

    with patch.object(GarminClient, "_load_profile"):
        with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:

            def side_effect_with_self(instance, email, password, **kwargs):
                instance.di_token = "new_token"
                instance.di_refresh_token = "new_refresh"
                instance.di_client_id = "new_client"
                return None, None

            m_login.side_effect = side_effect_with_self
            client = GarminClient()  # noqa F841

    token_file = mock_paths / "garmin_tokens.json"
    assert token_file.exists()
    saved_data = json.loads(token_file.read_text())
    assert saved_data["di_token"] == "new_token"


def test_init_fails_on_missing_env_vars(mock_paths, monkeypatch, mocker):
    """Scenario: No session file and no environment credentials."""
    mocker.patch("own_garmin.client.client.load_dotenv")
    monkeypatch.delenv("GARMIN_EMAIL", raising=False)
    monkeypatch.delenv("GARMIN_PASSWORD", raising=False)
    mocker.patch("own_garmin.client.client.os.getenv", return_value=None)

    with pytest.raises(ValueError, match="Cannot perform fresh login"):
        GarminClient()


def test_init_prompt_mfa_callback(mock_paths, mock_strategies, monkeypatch):
    """Scenario: MFA required, prompt_mfa callback is invoked instead of input()."""
    monkeypatch.setenv("GARMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "secret123")

    mfa_called = []

    def fake_prompt_mfa():
        mfa_called.append(True)
        return "123456"

    with patch.object(GarminClient, "_load_profile"):
        with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:
            with patch.object(GarminClient, "_resume_login_chain") as m_resume:
                with patch.object(GarminClient, "_dump_tokens"):

                    def side_effect(instance, email, password, **kwargs):
                        instance.di_token = "token"
                        instance.di_refresh_token = "refresh"
                        instance.di_client_id = "client"
                        return ("needs_mfa", None)

                    m_login.side_effect = side_effect
                    GarminClient(prompt_mfa=fake_prompt_mfa)

    assert mfa_called, "prompt_mfa callback was not called"
    m_resume.assert_called_once_with("123456")


# ---------------------------------------------------------------------------
# Public API — list_activities / get_activity / get_activity_details
# ---------------------------------------------------------------------------


def test_list_activities_single_page(authenticated_client, mocker):
    """Single page (fewer than 200 results): one _connectapi call, returns the list."""
    activities = [{"activityId": i} for i in range(5)]
    mock_api = mocker.patch.object(
        authenticated_client, "_connectapi", side_effect=[activities]
    )

    result = authenticated_client.list_activities(date(2026, 1, 1), date(2026, 1, 2))

    assert result == activities
    mock_api.assert_called_once_with(
        "/activitylist-service/activities/search/activities",
        params={
            "startDate": "2026-01-01",
            "endDate": "2026-01-02",
            "start": 0,
            "limit": 200,
        },
    )


def test_list_activities_multi_page(authenticated_client, mocker):
    """Multi-page: concatenates results and advances offset correctly."""
    page1 = [{"activityId": i} for i in range(200)]
    page2 = [{"activityId": i} for i in range(200, 250)]
    mock_api = mocker.patch.object(
        authenticated_client, "_connectapi", side_effect=[page1, page2]
    )

    result = authenticated_client.list_activities(date(2026, 1, 1), date(2026, 3, 31))

    assert result == page1 + page2
    assert mock_api.call_count == 2
    mock_api.assert_any_call(
        "/activitylist-service/activities/search/activities",
        params={
            "startDate": "2026-01-01",
            "endDate": "2026-03-31",
            "start": 0,
            "limit": 200,
        },
    )
    mock_api.assert_any_call(
        "/activitylist-service/activities/search/activities",
        params={
            "startDate": "2026-01-01",
            "endDate": "2026-03-31",
            "start": 200,
            "limit": 200,
        },
    )


def test_list_activities_empty_first_page(authenticated_client, mocker):
    """Empty first response: returns [] with only one _connectapi call."""
    mock_api = mocker.patch.object(
        authenticated_client, "_connectapi", side_effect=[[]]
    )

    result = authenticated_client.list_activities(date(2026, 1, 1), date(2026, 1, 2))

    assert result == []
    mock_api.assert_called_once()


def test_get_activity_api_call(authenticated_client, mocker):
    """Verify get_activity calls _connectapi with the correct path."""
    mock_api = mocker.patch.object(authenticated_client, "_connectapi")

    authenticated_client.get_activity(12345)

    mock_api.assert_called_once_with("/activity-service/activity/12345")


def test_get_activity_details_api_call(authenticated_client, mocker):
    """Verify get_activity_details calls _connectapi with correct path and params."""
    mock_api = mocker.patch.object(authenticated_client, "_connectapi")

    authenticated_client.get_activity_details(12345)

    mock_api.assert_called_once_with(
        "/activity-service/activity/12345/details",
        params={"maxChartSize": 99999, "maxPolylineSize": 99999},
    )


# ---------------------------------------------------------------------------
# download_fit
# ---------------------------------------------------------------------------


def test_download_fit_happy_path(authenticated_client, mocker):
    """Verify download_fit extracts the .fit file bytes from the ZIP response."""
    fit_content = b"FIT file bytes"
    mock_resp = MagicMock()
    mock_resp.content = _make_zip(("activity_12345.fit", fit_content))
    mock_resp.status_code = 200
    mock_resp.ok = True

    mocker.patch.object(authenticated_client, "_request", return_value=mock_resp)

    assert authenticated_client.download_fit(12345) == fit_content


def test_download_fit_multiple_fit_files_warns_and_returns_first(
    authenticated_client, mocker, caplog
):
    """download_fit warns and returns the first .fit file when the ZIP has many."""
    import logging

    mock_resp = MagicMock()
    mock_resp.content = _make_zip(
        ("activity_a.fit", b"first"),
        ("activity_b.fit", b"second"),
    )
    mock_resp.status_code = 200
    mock_resp.ok = True

    mocker.patch.object(authenticated_client, "_request", return_value=mock_resp)

    with caplog.at_level(logging.WARNING, logger="own_garmin.client.client"):
        result = authenticated_client.download_fit(12345)

    assert result == b"first"
    assert ".fit files" in caplog.text


def test_download_fit_no_fit_files_raises(authenticated_client, mocker):
    """download_fit raises GarminConnectionError when the ZIP has no .fit files."""
    mock_resp = MagicMock()
    mock_resp.content = _make_zip(("README.txt", b"no fit here"))
    mock_resp.status_code = 200
    mock_resp.ok = True

    mocker.patch.object(authenticated_client, "_request", return_value=mock_resp)

    with pytest.raises(GarminConnectionError, match="no .fit files"):
        authenticated_client.download_fit(12345)


def test_download_fit_bad_zip_raises(authenticated_client, mocker):
    """download_fit raises GarminConnectionError when response is not a valid ZIP."""
    mock_resp = MagicMock()
    mock_resp.content = b"not a zip archive"
    mock_resp.status_code = 200
    mock_resp.ok = True

    mocker.patch.object(authenticated_client, "_request", return_value=mock_resp)

    with pytest.raises(GarminConnectionError, match="not a valid ZIP"):
        authenticated_client.download_fit(12345)


# ---------------------------------------------------------------------------
# _request — 429 / 401 handling
# ---------------------------------------------------------------------------


def test_request_429_raises_too_many_requests(authenticated_client):
    """A 429 response raises GarminTooManyRequestsError."""
    mock_resp = MagicMock()
    mock_resp.status_code = 429
    authenticated_client._api_session = MagicMock()
    authenticated_client._api_session.request.return_value = mock_resp

    with pytest.raises(GarminTooManyRequestsError):
        authenticated_client._request("GET", "/some/path")


def test_request_401_refreshes_and_retries(authenticated_client, mocker):
    """A 401 triggers _refresh_session and the request is retried once."""
    first = MagicMock()
    first.status_code = 401
    second = MagicMock()
    second.status_code = 200
    second.ok = True

    authenticated_client._api_session = MagicMock()
    authenticated_client._api_session.request.side_effect = [first, second]

    mock_refresh = mocker.patch.object(authenticated_client, "_refresh_session")

    resp = authenticated_client._request("GET", "/some/path")

    mock_refresh.assert_called_once()
    assert resp is second


def test_request_401_retry_non_ok_raises(authenticated_client, mocker):
    """A 401 retry returning a non-ok response raises GarminConnectionError."""
    first = MagicMock()
    first.status_code = 401
    second = MagicMock()
    second.status_code = 500
    second.ok = False
    second.text = "Internal Server Error"

    authenticated_client._api_session = MagicMock()
    authenticated_client._api_session.request.side_effect = [first, second]

    mocker.patch.object(authenticated_client, "_refresh_session")

    with pytest.raises(GarminConnectionError):
        authenticated_client._request("GET", "/some/path")


def test_request_401_retry_429_raises_too_many_requests(authenticated_client, mocker):
    """A 429 on the 401 retry raises GarminTooManyRequestsError, not ConnectionError."""
    first = MagicMock()
    first.status_code = 401
    second = MagicMock()
    second.status_code = 429

    authenticated_client._api_session = MagicMock()
    authenticated_client._api_session.request.side_effect = [first, second]

    mocker.patch.object(authenticated_client, "_refresh_session")

    with pytest.raises(GarminTooManyRequestsError):
        authenticated_client._request("GET", "/some/path")


def test_request_raises_on_authorization_header_override(authenticated_client):
    """Passing Authorization in custom headers raises ValueError."""
    with pytest.raises(ValueError, match="Authorization"):
        authenticated_client._request(
            "GET", "/some/path", headers={"Authorization": "Bearer other"}
        )


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------


def test_token_expires_soon_true(authenticated_client):
    """_token_expires_soon returns True when the token expiry is within 15 minutes."""
    authenticated_client.di_token = _make_jwt({"exp": int(time.time()) - 1})
    assert authenticated_client._token_expires_soon() is True


def test_token_expires_soon_false(authenticated_client):
    """_token_expires_soon returns False for a token expiring far in the future."""
    authenticated_client.di_token = _make_jwt({"exp": int(time.time()) + 3600})
    assert authenticated_client._token_expires_soon() is False


def test_refresh_session_persists_tokens(authenticated_client, mocker):
    """_refresh_session calls _refresh_di_token and writes updated tokens to disk."""
    mock_refresh = mocker.patch.object(authenticated_client, "_refresh_di_token")
    mock_dump = mocker.patch.object(authenticated_client, "_dump_tokens")

    authenticated_client._refresh_session()

    mock_refresh.assert_called_once()
    mock_dump.assert_called_once_with(authenticated_client._tokenstore_path)


def test_refresh_di_token_updates_fields(authenticated_client, mocker):
    """_refresh_di_token stores the new access and refresh tokens from the response."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "access_token": "new_access_token",
        "refresh_token": "new_refresh_token",
    }
    mocker.patch.object(GarminClient, "_di_post", return_value=mock_resp)

    authenticated_client.di_client_id = "mock_client_id"
    authenticated_client.di_refresh_token = "old_refresh"
    authenticated_client._refresh_di_token()

    assert authenticated_client.di_token == "new_access_token"
    assert authenticated_client.di_refresh_token == "new_refresh_token"


def test_refresh_di_token_429_raises(authenticated_client, mocker):
    """_refresh_di_token raises GarminTooManyRequestsError on a 429 response."""
    mock_resp = MagicMock()
    mock_resp.status_code = 429
    mock_resp.ok = False
    mocker.patch.object(GarminClient, "_di_post", return_value=mock_resp)

    authenticated_client.di_client_id = "mock_client_id"
    authenticated_client.di_refresh_token = "old_refresh"

    with pytest.raises(GarminTooManyRequestsError):
        authenticated_client._refresh_di_token()


# ---------------------------------------------------------------------------
# DI token exchange — client ID rotation
# ---------------------------------------------------------------------------


def test_exchange_service_ticket_rotates_client_ids(authenticated_client, mocker):
    """_exchange_service_ticket tries the next client ID after a 4xx failure."""
    first_resp = MagicMock()
    first_resp.status_code = 400
    first_resp.ok = False
    first_resp.text = "bad request"

    second_resp = MagicMock()
    second_resp.status_code = 200
    second_resp.ok = True
    second_resp.json.return_value = {
        "access_token": "token_from_second_client",
        "refresh_token": "refresh_token",
    }

    mock_post = mocker.patch.object(
        GarminClient, "_di_post", side_effect=[first_resp, second_resp]
    )

    authenticated_client._exchange_service_ticket("dummy_ticket")

    assert mock_post.call_count == 2
    assert authenticated_client.di_token == "token_from_second_client"


def test_exchange_service_ticket_all_fail_raises(authenticated_client, mocker):
    """_exchange_service_ticket raises GarminAuthenticationError when all IDs fail."""
    bad_resp = MagicMock()
    bad_resp.status_code = 401
    bad_resp.ok = False
    bad_resp.text = "unauthorized"

    from own_garmin.client.constants import DI_CLIENT_IDS

    mocker.patch.object(
        GarminClient, "_di_post", side_effect=[bad_resp] * len(DI_CLIENT_IDS)
    )

    with pytest.raises(GarminAuthenticationError):
        authenticated_client._exchange_service_ticket("dummy_ticket")


# ---------------------------------------------------------------------------
# Strategy regex patterns
# ---------------------------------------------------------------------------


def test_csrf_re_double_quotes():
    html = '<input name="_csrf" value="abc123" />'
    m = _CSRF_RE.search(html)
    assert m and m.group(2) == "abc123"


def test_csrf_re_single_quotes():
    html = "<input name='csrf' value='token456' />"
    m = _CSRF_RE.search(html)
    assert m and m.group(2) == "token456"


def test_csrf_re_mixed_quotes():
    html = "<input name=\"_csrf\" value='xyz789' />"
    m = _CSRF_RE.search(html)
    assert m and m.group(2) == "xyz789"


def test_csrf_re_no_match():
    assert _CSRF_RE.search("<input name='other' value='foo' />") is None


def test_title_re_success():
    html = "<html><head><title>Success</title></head></html>"
    m = _TITLE_RE.search(html)
    assert m and m.group(1) == "Success"


def test_title_re_mfa():
    html = "<title>MFA Required</title>"
    m = _TITLE_RE.search(html)
    assert m and "MFA" in m.group(1)


def test_title_re_no_match():
    assert _TITLE_RE.search("<p>no title here</p>") is None


def test_ticket_re_extracts_ticket_double_quote():
    html = 'src="https://sso.garmin.com/sso/embed?ticket=ST-1234567-abcdef"'
    m = _TICKET_RE.search(html)
    assert m and m.group(1) == "ST-1234567-abcdef"


def test_ticket_re_extracts_ticket_single_quote():
    html = "src='https://sso.garmin.com/sso/embed?ticket=ST-9999999-xyz'"
    m = _TICKET_RE.search(html)
    assert m and m.group(1) == "ST-9999999-xyz"


def test_ticket_re_no_match():
    assert _TICKET_RE.search('<a href="/other">') is None


# ---------------------------------------------------------------------------
# GARMIN_TOKENS_JSON side-loading
# ---------------------------------------------------------------------------


def test_init_with_tokens_json_env_sideloads(mock_paths, monkeypatch):
    """GARMIN_TOKENS_JSON side-loads tokens without invoking login strategies."""
    token_data = {
        "di_token": "env_access_token",
        "di_refresh_token": "env_refresh_token",
        "di_client_id": "env_client_id",
    }
    monkeypatch.setenv("GARMIN_TOKENS_JSON", json.dumps(token_data))

    with patch.object(GarminClient, "_load_profile", return_value=None):
        with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:
            client = GarminClient()

    assert client.di_token == "env_access_token"
    m_login.assert_not_called()


# ---------------------------------------------------------------------------
# export_session
# ---------------------------------------------------------------------------


def test_export_session_returns_json(authenticated_client):
    """export_session returns JSON with all three token fields."""
    raw = authenticated_client.export_session()
    parsed = json.loads(raw)
    assert parsed["di_token"] == "mock_access_token"
    assert parsed["di_refresh_token"] == "mock_refresh_token"
    assert parsed["di_client_id"] == "mock_client_id"


def test_export_session_raises_when_unauthenticated(mock_paths):
    """export_session raises GarminAuthenticationError when no token is set."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(
        json.dumps(
            {
                "di_token": "some_token",
                "di_refresh_token": "some_refresh",
                "di_client_id": "some_client",
            }
        )
    )
    with patch.object(GarminClient, "_load_profile", return_value=None):
        client = GarminClient()

    client.di_token = None
    with pytest.raises(GarminAuthenticationError, match="No active session"):
        client.export_session()


# ---------------------------------------------------------------------------
# Unwritable session dir
# ---------------------------------------------------------------------------


def test_init_session_dir_unwritable_keeps_working(mock_paths, monkeypatch):
    """Client constructs fine when session dir mkdir raises PermissionError."""
    monkeypatch.delenv("GARMIN_TOKENS_JSON", raising=False)
    monkeypatch.setenv("GARMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "secret123")

    original_mkdir = Path.mkdir

    def failing_mkdir(self, *args, **kwargs):
        if str(self) == str(mock_paths):
            raise PermissionError("read-only filesystem")
        return original_mkdir(self, *args, **kwargs)

    with patch.object(Path, "mkdir", failing_mkdir):
        with patch.object(GarminClient, "_load_profile", return_value=None):
            with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:

                def side_effect(instance, email, password, **kwargs):
                    instance.di_token = "fresh_token"
                    instance.di_refresh_token = "fresh_refresh"
                    instance.di_client_id = "fresh_client"
                    return None, None

                m_login.side_effect = side_effect
                client = GarminClient()

    assert client._tokenstore_path is None


# ---------------------------------------------------------------------------
# resume_session=False
# ---------------------------------------------------------------------------


def test_init_resume_session_false_skips_env_sideload(mock_paths, monkeypatch, mocker):
    """resume_session=False ignores GARMIN_TOKENS_JSON and forces fresh login."""
    token_data = {
        "di_token": "env_access_token",
        "di_refresh_token": "env_refresh_token",
        "di_client_id": "env_client_id",
    }
    monkeypatch.setenv("GARMIN_TOKENS_JSON", json.dumps(token_data))
    monkeypatch.setenv("GARMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "secret123")

    with patch.object(GarminClient, "_load_profile"):
        with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:

            def side_effect(instance, email, password, **kwargs):
                instance.di_token = "fresh_token"
                instance.di_refresh_token = "fresh_refresh"
                instance.di_client_id = "fresh_client"
                return None, None

            m_login.side_effect = side_effect
            client = GarminClient(resume_session=False)

    m_login.assert_called_once()
    assert client.di_token == "fresh_token"


def test_init_resume_session_false_skips_disk_resume(mock_paths, monkeypatch, mocker):
    """resume_session=False ignores a token file on disk and forces fresh login."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(
        json.dumps(
            {
                "di_token": "disk_token",
                "di_refresh_token": "disk_refresh",
                "di_client_id": "disk_client",
            }
        )
    )
    monkeypatch.setenv("GARMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "secret123")

    with patch.object(GarminClient, "_load_profile"):
        with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:

            def side_effect(instance, email, password, **kwargs):
                instance.di_token = "fresh_token"
                instance.di_refresh_token = "fresh_refresh"
                instance.di_client_id = "fresh_client"
                return None, None

            m_login.side_effect = side_effect
            client = GarminClient(resume_session=False)

    m_login.assert_called_once()
    assert client.di_token == "fresh_token"


def test_init_env_sideload_does_not_mkdir(mock_paths, monkeypatch, mocker):
    """When GARMIN_TOKENS_JSON side-load succeeds, the session dir is never
    touched — no mkdir and no _tokenstore_path set (Lambda-safe)."""
    token_data = {
        "di_token": "env_token",
        "di_refresh_token": "env_refresh",
        "di_client_id": "env_client",
    }
    monkeypatch.setenv("GARMIN_TOKENS_JSON", json.dumps(token_data))

    mkdir_calls: list[tuple] = []
    original_mkdir = Path.mkdir

    def tracking_mkdir(self, *args, **kwargs):
        mkdir_calls.append((str(self), args, kwargs))
        return original_mkdir(self, *args, **kwargs)

    with patch.object(Path, "mkdir", tracking_mkdir):
        with patch.object(GarminClient, "_load_profile", return_value=None):
            client = GarminClient()

    session_dir_mkdirs = [c for c in mkdir_calls if str(mock_paths) in c[0]]
    assert session_dir_mkdirs == []
    assert client._tokenstore_path is None


# ---------------------------------------------------------------------------
# _load_tokens_from_json shape validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["null", "[]", "42", '"str"'])
def test_load_tokens_from_json_rejects_non_object(mock_paths, raw):
    """_load_tokens_from_json raises ValueError for non-dict JSON."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(
        json.dumps(
            {
                "di_token": "tok",
                "di_refresh_token": "r",
                "di_client_id": "c",
            }
        )
    )
    with patch.object(GarminClient, "_load_profile", return_value=None):
        client = GarminClient()

    with pytest.raises(ValueError, match="must be a JSON object"):
        client._load_tokens_from_json(raw)


def test_init_malformed_tokens_json_falls_back(mock_paths, monkeypatch):
    """GARMIN_TOKENS_JSON='null' falls back to login rather than crashing."""
    monkeypatch.setenv("GARMIN_TOKENS_JSON", "null")
    monkeypatch.setenv("GARMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("GARMIN_PASSWORD", "secret123")

    with patch.object(GarminClient, "_load_profile"):
        with patch.object(GarminClient, "_login_chain", autospec=True) as m_login:

            def side_effect(instance, email, password, **kwargs):
                instance.di_token = "fresh_token"
                instance.di_refresh_token = "fresh_refresh"
                instance.di_client_id = "fresh_client"
                return None, None

            m_login.side_effect = side_effect
            client = GarminClient()

    m_login.assert_called_once()
    assert client.di_token == "fresh_token"
