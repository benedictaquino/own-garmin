# Implementation Plan: Garmin Lakehouse (v1.1 — Cloud Ready & Remote MFA) - WIP DRAFT

## Context

V1 established the local medallion architecture (Bronze/Silver/DuckDB) for Garmin activity data. This "Cloud Ready" plan (v1.1) focuses on making the system headless-capable. The primary challenge is Garmin's MFA and session persistence in environments without a persistent local filesystem or interactive terminal. This requires decoupling the session from the disk and providing a non-interactive way to handle MFA challenges.

## Objectives

* **Headless Auth:** Decouple session persistence from the local filesystem to allow "side-loading" tokens via secrets.
* **Remote MFA:** Implement a non-interactive MFA flow using `ntfy.sh` for push-based code entry.
* **Secret Rotation Support:** Provide a way to "export" refreshed tokens so they can be saved back to a remote secret store or state manager.
* **Local Verification:** Ensure the remote MFA loop is robust and handles timeouts/errors by simulating a headless environment locally.

## New Components

### `mfa_handlers.py` — Remote MFA Logic

* `NtfyMfaHandler`: A polling-based handler that uses `ntfy.sh`.
  * Sends a notification to a private `NTFY_TOPIC`.
  * Polls the topic for a 6-digit response.
  * Timeout handling and security (UUID topics).

### Updated `client.py` — Session Injection

* Support `GARMIN_TOKENS_JSON` environment variable.
* Pluggable MFA handlers (defaulting to `input()` but switchable to `ntfy`).
* `export_session()` method for session state management.

### Updated `cli.py` — Headless Support

* `--remote-mfa`: Enable the `ntfy.sh` handler.
* `--export-session`: Print current tokens to stdout (for capture by external scripts).

## Task Breakdown

1. `10-remote-mfa-verification.md` — Session injection, remote MFA via `ntfy.sh`, and local headless simulation.

## Verification Workflow

1. **Start:** Run `own-garmin login --remote-mfa` locally with `NTFY_TOPIC` set and local session files removed.
2. **MFA:** User receives push notification on mobile via `ntfy.sh` and enters the code.
3. **Resume:** CLI resumes and prints updated session JSON.
4. **Confirm:** Re-run with the updated JSON injected via `GARMIN_TOKENS_JSON` to ensure successful side-loading.
