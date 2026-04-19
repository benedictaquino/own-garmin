# ADR-002: In-House Implementation for Comprehensive Health Metrics Extraction

## Status

Implemented

## Context

The `own-garmin` project currently extracts activities, activity details, and FIT files, landing them in a local data lakehouse. The goal is to expand this pipeline to ingest all possible health and lifestyle data available from Garmin Connect (e.g., heart rate, sleep, stress, body composition, SpO2, HRV, and daily steps).

While open-source wrappers like `python-garminconnect` exist, a decision has been made **not** to adopt third-party wrapper libraries. Relying on an external wrapper introduces dependency bloat, potential version conflicts, and forces the project's architecture to conform to the library's design patterns.

Because Garmin does not provide an official, open public API for personal data, the required endpoints are undocumented and protected by complex authentication flows (including Cloudflare bot protection and SSO). Therefore, expanding the scope requires building a robust, custom API client directly into the `own-garmin` codebase.

## Decision

1. **Develop a Custom In-House Garmin Client:** We will build a dedicated, lightweight HTTP client within `own-garmin` to handle authentication, session persistence, and token refreshes. This ensures full control over the network layer, timeout configurations, and error handling.
2. **Use Open-Source as Documentation, Not Dependencies:** We will leverage repositories like `python-garminconnect` and `garmin-health-data` strictly as reference material to map out Garmin's undocumented endpoint structures, required headers, and authentication sequences, translating these into our custom implementation.
3. **Modular Daily Metric Extraction:** We will implement dedicated extraction functions for each distinct health metric (e.g., sleep, body battery, resting heart rate). The extractor will iterate over defined date ranges and pull the raw JSON payloads.
4. **Preserve the Medallion Architecture:**
    * **Bronze Layer:** Raw JSON responses from the newly mapped health endpoints will be dumped directly into the Bronze layer to preserve data fidelity and protect against unannounced schema changes by Garmin.
    * **Silver Layer:** DuckDB will be used to extract, unnest, and flatten the complex JSON payloads into tabular formats, which will then be written as Apache Iceberg tables.
5. **Implement Custom Rate Limiting and Retry Logic:** Since we are managing the network requests directly, we will implement tailored exponential backoff and jitter strategies to safely scrape large historical date ranges without triggering Garmin's rate limits (HTTP 429) or Cloudflare blocks.

## Consequences

### Positive

* **Zero Dependency Bloat:** `own-garmin` remains self-contained, reducing security risks and maintenance overhead associated with third-party upstream libraries.
* **Total Architectural Control:** The network requests, error handling, and data flow can be optimized specifically for the existing data lakehouse architecture rather than working around a wrapper's return types.
* **Tailored Auth Flow:** We can implement authentication mechanisms (like caching session cookies or tokens) exactly how our infrastructure demands it.

### Negative

* **High Maintenance Burden for Authentication:** Garmin frequently updates its SSO flow and Cloudflare protections. Without a community-maintained wrapper, the burden of reverse-engineering and fixing broken authentication flows falls entirely on the `own-garmin` maintainers.
* **Manual Endpoint Updates:** If Garmin changes endpoint URLs or deprecates old APIs, we will need to manually identify the changes and update the internal client.
* **Increased Upfront Development Time:** Building a reliable session manager that handles login handshakes, MFA (if applicable), and cookie management is significantly more complex than calling a pre-built library.
