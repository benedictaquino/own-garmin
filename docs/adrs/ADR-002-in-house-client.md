# ADR-002: In-House Implementation for Comprehensive Health Metrics Extraction

## Status

Implemented

## Context

The `own-garmin` project currently extracts activities, activity details, and FIT files, landing them in a local data lakehouse. The goal is to expand this pipeline to ingest all possible health and lifestyle data available from Garmin Connect (e.g., heart rate, sleep, stress, body composition, SpO2, HRV, and daily steps).

While open-source wrappers like `python-garminconnect` exist, a decision has been made **not** to adopt third-party wrapper libraries. Relying on an external wrapper introduces dependency bloat, potential version conflicts, and forces the project's architecture to conform to the library's design patterns.

Because Garmin does not provide an official, open public API for personal data, the required endpoints are undocumented and protected by complex authentication flows (including Cloudflare bot protection and SSO). Therefore, expanding the scope requires building a robust, custom API client directly into the `own-garmin` codebase.

## Decision

1. **Develop a Custom In-House Garmin Client:** We build a dedicated, lightweight HTTP client within `own-garmin` (`src/own_garmin/client/`) that handles authentication, session persistence, and token refresh. This gives us full control over the network layer, timeout configuration, and error handling. The client is decoupled from `python-garminconnect` and `garth`: it talks to Garmin's SSO and Connect API endpoints directly using `requests` and `curl_cffi` as low-level transports only.
2. **Use Open-Source as Documentation, Not Dependencies:** We leverage repositories like `python-garminconnect` strictly as reference material to map out Garmin's undocumented endpoint structures, required headers, and authentication sequences, translating them into our own implementation.
3. **Modular Per-Category Extractors:** Each Bronze category gets its own extractor module under `src/own_garmin/bronze/` (today: `activities.py`, `activity_details.py`, `fit.py`). Extractors iterate date ranges, pull raw payloads from the client, and write them to the Bronze layer. New health metrics (sleep, body battery, HRV, etc.) will follow the same pattern — see [ADR-003](ADR-003-expanding-scope.md) for the scope expansion.
4. **Preserve the Medallion Architecture:**
    * **Bronze Layer:** Raw JSON responses (and zipped FIT bytes for activities) are written as-is to preserve data fidelity and protect against unannounced schema changes by Garmin.
    * **Silver Layer:** [Polars](https://pola.rs/) reads Bronze, flattens and types the data, and writes hive-partitioned **Parquet** under `data/silver/{category}/`. [DuckDB](https://duckdb.org/) is used at query time to expose those Parquet files as SQL views (per ADR-001) — it is not used for the Bronze → Silver transformation itself.
5. **Explicit Rate-Limit and Retry Handling:** Since we manage the network requests directly, the client handles Garmin-specific failure modes explicitly:
    * `429 Too Many Requests` is surfaced as a typed `GarminTooManyRequestsError` so callers can choose how to respond rather than silently retrying.
    * `401 Unauthorized` triggers one automatic DI refresh-token exchange and a single retry before giving up.
    * Login strategies insert a jittered 30–45 s delay between attempts to avoid Cloudflare rate-limit heuristics.
    * The `ingest` CLI exposes a configurable `--sleep-sec` (default 0.5 s) between per-activity detail and FIT downloads to pace bulk scrapes.

## Consequences

### Positive

* **Zero Dependency Bloat:** `own-garmin` remains self-contained, reducing security risks and maintenance overhead associated with third-party upstream libraries.
* **Total Architectural Control:** Network requests, error handling, and data flow are optimized for the existing lakehouse architecture rather than working around a wrapper's return types.
* **Tailored Auth Flow:** We persist DI bearer and refresh tokens atomically to `session.json` (0600) and refresh them in-place, which keeps the Cloudflare footprint low compared to repeated full logins.

### Negative

* **High Maintenance Burden for Authentication:** Garmin frequently updates its SSO flow and Cloudflare protections. Without a community-maintained wrapper, the burden of reverse-engineering and fixing broken authentication flows falls entirely on the `own-garmin` maintainers. Today this is mitigated by a 5-strategy login chain (`portal+cffi`, `portal+requests`, `mobile+cffi`, `mobile+requests`, `widget+cffi`) so that a break in one path does not take down login entirely.
* **Manual Endpoint Updates:** If Garmin changes endpoint URLs or deprecates old APIs, we have to identify the changes and update the internal client ourselves.
* **Increased Upfront Development Time:** Building a reliable session manager that handles login handshakes, MFA (including the optional ntfy-based remote MFA flow), and cookie management is significantly more complex than calling a pre-built library.
