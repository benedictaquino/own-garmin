import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from own_garmin.client import GarminClient, GarminTooManyRequestsError


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


def test_init_resume_success(mock_paths, mock_strategies):
    """Scenario: Valid garmin_tokens.json exists, resume succeeds."""
    token_file = mock_paths / "garmin_tokens.json"
    token_data = {
        "di_token": "mock_access_token",
        "di_refresh_token": "mock_refresh_token",
        "di_client_id": "mock_client_id",
    }
    token_file.write_text(json.dumps(token_data))

    # Mock the profile load to simulate a valid session
    with patch.object(GarminClient, "_load_profile", return_value=None):
        client = GarminClient()

    assert client.di_token == "mock_access_token"
    # Ensure no login strategies were triggered
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

    # Verify tokens were persisted to disk
    token_file = mock_paths / "garmin_tokens.json"
    assert token_file.exists()
    saved_data = json.loads(token_file.read_text())
    assert saved_data["di_token"] == "new_token"


def test_init_fails_on_missing_env_vars(mock_paths, monkeypatch, mocker):
    """Scenario: No session file and no environment credentials."""
    # Mock load_dotenv globally since it's imported inside the method
    mocker.patch("dotenv.load_dotenv")

    monkeypatch.delenv("GARMIN_EMAIL", raising=False)
    monkeypatch.delenv("GARMIN_PASSWORD", raising=False)

    # Also mock os.getenv in the module where it's used
    mocker.patch("own_garmin.client.client.os.getenv", return_value=None)

    with pytest.raises(ValueError, match="Cannot perform fresh login"):
        GarminClient()


def test_list_activities_api_call(mock_paths, mocker):
    """Verify list_activities calls the internal _connectapi with correct path."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(json.dumps({"di_token": "valid"}))

    with patch.object(GarminClient, "_load_profile"):
        client = GarminClient()
        mock_api = mocker.patch.object(client, "_connectapi")

        start = date(2026, 1, 1)
        end = date(2026, 1, 2)
        client.list_activities(start, end)

        # Verify the path matches the internal Garmin endpoint
        expected_path = (
            "/activitylist-service/activities/search/activities"
            "?startDate=2026-01-01&endDate=2026-01-02"
        )
        mock_api.assert_called_once_with(expected_path)


def test_get_activity_api_call(mock_paths, mocker):
    """Verify get_activity calls the internal _connectapi with correct path."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(json.dumps({"di_token": "valid"}))

    with patch.object(GarminClient, "_load_profile"):
        client = GarminClient()
        mock_api = mocker.patch.object(client, "_connectapi")

        client.get_activity(12345)

        expected_path = "/activity-service/activity/12345"
        mock_api.assert_called_once_with(expected_path)


def test_get_activity_details_api_call(mock_paths, mocker):
    """Verify get_activity_details calls _connectapi with correct path and params."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(json.dumps({"di_token": "valid"}))

    with patch.object(GarminClient, "_load_profile"):
        client = GarminClient()
        mock_api = mocker.patch.object(client, "_connectapi")

        client.get_activity_details(12345)

        expected_path = "/activity-service/activity/12345/details"
        mock_api.assert_called_once_with(
            expected_path, params={"maxChartSize": 99999, "maxPolylineSize": 99999}
        )


def test_request_429_raises_too_many_requests(mock_paths, mocker):
    """Verify a 429 response from the API raises GarminTooManyRequestsError."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(json.dumps({"di_token": "valid"}))

    with patch.object(GarminClient, "_load_profile"):
        client = GarminClient()

    mock_resp = MagicMock()
    mock_resp.status_code = 429
    client._api_session = MagicMock()
    client._api_session.request.return_value = mock_resp

    with pytest.raises(GarminTooManyRequestsError):
        client._request("GET", "/some/path")


def test_request_401_refreshes_and_retries(mock_paths, mocker):
    """Verify a 401 triggers _refresh_session and the request is retried once."""
    token_file = mock_paths / "garmin_tokens.json"
    token_file.write_text(json.dumps({"di_token": "valid"}))

    with patch.object(GarminClient, "_load_profile"):
        client = GarminClient()

    first = MagicMock()
    first.status_code = 401
    second = MagicMock()
    second.status_code = 200
    second.ok = True

    client._api_session = MagicMock()
    client._api_session.request.side_effect = [first, second]

    mock_refresh = mocker.patch.object(client, "_refresh_session")

    resp = client._request("GET", "/some/path")

    mock_refresh.assert_called_once()
    assert resp is second
