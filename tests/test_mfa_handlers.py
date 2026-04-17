import json
import time

import pytest
import requests as _requests

from own_garmin.client.mfa_handlers import NtfyMfaHandler


def _json_line(**kwargs) -> str:
    return json.dumps(kwargs)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_missing_topic_raises(monkeypatch):
    """No env var and no arg → ValueError."""
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    with pytest.raises(ValueError, match="NTFY_TOPIC"):
        NtfyMfaHandler()


def test_topic_from_env(monkeypatch):
    """Topic is read from NTFY_TOPIC env var when not passed directly."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    handler = NtfyMfaHandler()
    assert handler.topic == "fake-topic-uuid"


# ---------------------------------------------------------------------------
# Notification publish
# ---------------------------------------------------------------------------


def test_publishes_notification(monkeypatch, mocker):
    """get_mfa_code posts to ntfy.sh with the topic URL."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")

    valid_line = _json_line(event="message", message="123456")
    mock_get_resp = mocker.MagicMock()
    mock_get_resp.text = valid_line
    mock_get_resp.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_get_resp
    mock_requests.RequestException = _requests.RequestException

    handler = NtfyMfaHandler(
        poll_interval_s=0.01, timeout_s=5.0, base_url="https://ntfy.sh"
    )
    handler.get_mfa_code()

    mock_requests.post.assert_called_once()
    call_args = mock_requests.post.call_args
    assert "fake-topic-uuid" in call_args[0][0]


# ---------------------------------------------------------------------------
# Code extraction
# ---------------------------------------------------------------------------


def test_returns_first_valid_code(monkeypatch, mocker):
    """Returns the 6-digit code from the first matching message line."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")

    body = "\n".join(
        [
            _json_line(event="keepalive"),
            _json_line(event="message", message="123456"),
        ]
    )
    mock_get_resp = mocker.MagicMock()
    mock_get_resp.text = body
    mock_get_resp.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_get_resp
    mock_requests.RequestException = _requests.RequestException

    handler = NtfyMfaHandler(poll_interval_s=0.01, timeout_s=5.0)
    assert handler.get_mfa_code() == "123456"


def test_ignores_non_numeric_message(monkeypatch, mocker):
    """Non-numeric message body is ignored; valid code on next poll is returned."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")
    mocker.patch("own_garmin.client.mfa_handlers.time.sleep")

    first_body = _json_line(event="message", message="hello")
    second_body = _json_line(event="message", message="987654")

    mock_get_resp_1 = mocker.MagicMock()
    mock_get_resp_1.text = first_body
    mock_get_resp_1.raise_for_status.return_value = None

    mock_get_resp_2 = mocker.MagicMock()
    mock_get_resp_2.text = second_body
    mock_get_resp_2.raise_for_status.return_value = None

    mock_requests.get.side_effect = [mock_get_resp_1, mock_get_resp_2]
    mock_requests.RequestException = _requests.RequestException

    handler = NtfyMfaHandler(poll_interval_s=0.01, timeout_s=5.0)
    result = handler.get_mfa_code()

    assert result == "987654"
    assert mock_requests.get.call_count == 2


def test_ignores_wrong_length_code(monkeypatch, mocker):
    """A 5-digit body is ignored; a 6-digit code on next poll succeeds."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")
    mocker.patch("own_garmin.client.mfa_handlers.time.sleep")

    first_body = _json_line(event="message", message="12345")
    second_body = _json_line(event="message", message="123456")

    mock_get_resp_1 = mocker.MagicMock()
    mock_get_resp_1.text = first_body
    mock_get_resp_1.raise_for_status.return_value = None

    mock_get_resp_2 = mocker.MagicMock()
    mock_get_resp_2.text = second_body
    mock_get_resp_2.raise_for_status.return_value = None

    mock_requests.get.side_effect = [mock_get_resp_1, mock_get_resp_2]
    mock_requests.RequestException = _requests.RequestException

    handler = NtfyMfaHandler(poll_interval_s=0.01, timeout_s=5.0)
    result = handler.get_mfa_code()

    assert result == "123456"


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


def test_times_out(monkeypatch, mocker):
    """TimeoutError raised when no valid code is received within timeout_s."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")
    mocker.patch("own_garmin.client.mfa_handlers.time.sleep")

    mock_get_resp = mocker.MagicMock()
    mock_get_resp.text = ""
    mock_get_resp.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_get_resp
    mock_requests.RequestException = _requests.RequestException

    handler = NtfyMfaHandler(poll_interval_s=0.05, timeout_s=0.2)

    # We keep real time.time so the deadline logic works; only sleep is patched.
    deadline = time.time() + 1.0
    with pytest.raises(TimeoutError):
        handler.get_mfa_code()
    # Should have exited well within 1 second (not hanging forever).
    assert time.time() < deadline


# ---------------------------------------------------------------------------
# Network error resilience
# ---------------------------------------------------------------------------


def test_poll_network_error_is_swallowed(monkeypatch, mocker):
    """A RequestException on the first poll is swallowed; second poll returns code."""
    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")
    mocker.patch("own_garmin.client.mfa_handlers.time.sleep")

    mock_get_resp = mocker.MagicMock()
    mock_get_resp.text = _json_line(event="message", message="654321")
    mock_get_resp.raise_for_status.return_value = None

    mock_requests.get.side_effect = [
        _requests.RequestException("network error"),
        mock_get_resp,
    ]
    mock_requests.RequestException = _requests.RequestException

    handler = NtfyMfaHandler(poll_interval_s=0.01, timeout_s=5.0)
    result = handler.get_mfa_code()

    assert result == "654321"
    assert mock_requests.get.call_count == 2


# ---------------------------------------------------------------------------
# Publish HTTP error
# ---------------------------------------------------------------------------


def test_publish_http_error_is_logged_and_polling_continues(
    monkeypatch, mocker, caplog
):
    """HTTP error on publish POST is logged as warning; polling still succeeds."""
    import logging

    monkeypatch.setenv("NTFY_TOPIC", "fake-topic-uuid")
    mock_requests = mocker.patch("own_garmin.client.mfa_handlers.requests")
    mocker.patch("own_garmin.client.mfa_handlers.time.sleep")

    # publish POST returns a response whose raise_for_status raises HTTPError
    mock_post_resp = mocker.MagicMock()
    mock_post_resp.raise_for_status.side_effect = _requests.HTTPError("403 Forbidden")
    mock_requests.post.return_value = mock_post_resp
    mock_requests.RequestException = _requests.RequestException
    mock_requests.HTTPError = _requests.HTTPError

    # poll GET returns a valid 6-digit code
    mock_get_resp = mocker.MagicMock()
    mock_get_resp.text = _json_line(event="message", message="112233")
    mock_get_resp.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_get_resp

    handler = NtfyMfaHandler(poll_interval_s=0.01, timeout_s=5.0)

    with caplog.at_level(logging.WARNING, logger="own_garmin.client.mfa_handlers"):
        result = handler.get_mfa_code()

    assert result == "112233"
    assert "Failed to publish ntfy.sh MFA notification" in caplog.text
