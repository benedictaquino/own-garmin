# Task 10: Remote MFA Verification (Headless Simulation)

Verify the `ntfy.sh` remote MFA loop and session injection/export locally to ensure they are "Cloud Ready" before setting up an orchestrator.

## Objective

* Implement `NtfyMfaHandler` for headless MFA entry.
* Support session side-loading via `GARMIN_TOKENS_JSON`.
* Verify the end-to-end "Push -> Poll -> Resume" flow using a local machine and a mobile device.

## Implementation Steps

### 1. Implement ntfy.sh Handler

* Create `src/own_garmin/client/mfa_handlers.py`.
* Implement `NtfyMfaHandler.get_mfa_code()`:
  1. Generate/Read `NTFY_TOPIC` from environment.
  2. Publish a notification: "🔓 own-garmin: Enter MFA code".
  3. Poll `https://ntfy.sh/<topic>/json?poll=1` in a loop (5s interval, 5m timeout).
  4. Extract the first 6-digit numeric message sent after the notification.

### 2. Enable CLI Hooks

* Add `--remote-mfa` flag to the `login` and `fetch` commands.
* Add `--export-session` flag to print the refreshed `GARMIN_TOKENS_JSON` to stdout.

### 3. Verification Protocol (Manual)

Run the following test to simulate a headless environment on your local machine:

1. Setup ntfy: Install the `ntfy` app on your phone and subscribe to a random topic (e.g., `garmin-mfa-test-123`).
2. Clear Local Session: Temporarily move your `garmin_tokens.json` to force a fresh login.
3. Run Simulation:

   ```bash
   export NTFY_TOPIC=garmin-mfa-test-123
   own-garmin login --remote-mfa --export-session
   ```

4. Expectation:
   * The CLI should print "Waiting for MFA code via ntfy.sh topic..." and hang.
   * A notification should appear on your phone.
5. Action: Tap the notification or open the topic and send "123456" (or the actual Garmin code).
6. Success Criteria:
   * The CLI automatically resumes within 5-10 seconds of you sending the code.
   * It prints the resulting JSON tokens to the terminal.
   * Subsequent runs using `export GARMIN_TOKENS_JSON='<the-output>'` skip the login step entirely.

## Definition of Done

* [ ] `GarminClient` resumes successfully using only `ntfy.sh` input.
* [ ] Refreshed sessions can be "sideloaded" via environment variables.
* [ ] No `input()` calls are triggered when `--remote-mfa` is active.
