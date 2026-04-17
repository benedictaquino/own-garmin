from __future__ import annotations

import json
import logging
import os
import time
from typing import Protocol

import requests

_LOGGER = logging.getLogger(__name__)

NTFY_BASE_URL = "https://ntfy.sh"
DEFAULT_POLL_INTERVAL_S = 5.0
DEFAULT_TIMEOUT_S = 300.0  # 5 minutes
_MFA_CODE_LEN = 6


class MfaHandler(Protocol):
    def get_mfa_code(self) -> str: ...


class InteractiveMfaHandler:
    def get_mfa_code(self) -> str:
        return input("\nEnter Garmin MFA code: ")


class NtfyMfaHandler:
    """Publish an MFA prompt to ntfy.sh and poll for a 6-digit reply."""

    def __init__(
        self,
        topic: str | None = None,
        *,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        base_url: str = NTFY_BASE_URL,
    ) -> None:
        self.topic = topic or os.environ.get("NTFY_TOPIC")
        if not self.topic:
            raise ValueError(
                "NTFY_TOPIC env var (or `topic` argument) is required for NtfyMfaHandler"  # noqa: E501
            )
        self.poll_interval_s = poll_interval_s
        self.timeout_s = timeout_s
        self.base_url = base_url.rstrip("/")

    def get_mfa_code(self) -> str:
        _LOGGER.info("Waiting for MFA code via ntfy.sh topic...")
        start = time.time()

        try:
            requests.post(
                f"{self.base_url}/{self.topic}",
                data=b"own-garmin: Enter MFA code",
                headers={"Title": "own-garmin MFA", "Priority": "high"},
                timeout=10,
            )
        except requests.RequestException as e:
            _LOGGER.warning("Failed to publish ntfy.sh MFA notification: %s", e)

        since = int(start)
        poll_url = f"{self.base_url}/{self.topic}/json?poll=1&since={since}"
        deadline = start + self.timeout_s

        while time.time() < deadline:
            code = self._poll_once(poll_url)
            if code is not None:
                return code
            time.sleep(self.poll_interval_s)

        raise TimeoutError(
            f"No MFA code received via ntfy.sh topic after {self.timeout_s}s"
        )

    def _poll_once(self, poll_url: str) -> str | None:
        try:
            resp = requests.get(poll_url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            _LOGGER.debug("ntfy poll failed: %s", e)
            return None

        for raw in resp.text.splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if msg.get("event") != "message":
                continue
            body = (msg.get("message") or "").strip()
            if len(body) == _MFA_CODE_LEN and body.isdigit():
                return body
        return None
