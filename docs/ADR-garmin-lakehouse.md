# ADR 001: Local Data Lakehouse Architecture for Garmin Health Data

## Status
**Proposed**

## Context
The goal is to build a personal health data platform that extracts data from Garmin Connect for long-term analysis. Traditional local solutions often use SQLite, which couples the raw data to a specific relational schema and makes reprocessing difficult if logic changes.

Furthermore, Garmin’s API is protected by aggressive Cloudflare anti-bot measures. The architecture must handle fragile authentication gracefully, ensure high-fidelity data preservation (Bronze layer), and provide a clean path to migrate from local storage to cloud object storage (S3/GCS) without rewriting core logic.

## Decision
We will implement a **Local Data Lakehouse** using a **Medallion Architecture**, focused on robust session management and decoupled storage.

### 1. Technical Stack
* **Ingestion:** `python-garminconnect` (v0.3.0+) utilizing the native mobile SSO flow to bypass Cloudflare challenges.
* **Storage Formats:** Immutable JSON for the Bronze layer; Partitioned Parquet for the Silver layer.
* **Processing Engine:** [Polars](https://pola.rs/) for high-performance data transformation and schema enforcement.
* **Query Engine:** [DuckDB](https://duckdb.org/) for SQL-based analytical interrogation of Parquet files.

### 2. Data Layers (Medallion Pattern)
* **Bronze (Raw):** Stores original, unmodified JSON responses from Garmin. This layer is strictly immutable. If parsing logic fails or Garmin adds new metrics, the source data remains preserved for re-processing.
    * *Pathing:* `/data/bronze/{category}/year={YYYY}/month={MM}/day={DD}.json`
* **Silver (Processed):** Flattened, cleaned, and typed Parquet files created by Polars. This layer converts Garmin’s internal units (e.g., semicircles, UTC offsets) into human-readable metrics.
    * *Pathing:* `/data/silver/{category}/` (Partitioned by date).

### 3. Key Design Patterns
* **Persistent Session Management:** Authentication will be encapsulated in a dedicated client wrapper. This wrapper must persist session tokens (e.g., in a local `session.json`). The system will prioritize refreshing an existing token over initiating a full login flow to minimize the footprint on Garmin’s authentication servers and avoid Cloudflare triggers.
* **File-Based Interface:** All components will interact with the data via standard file paths. By avoiding a database engine for storage, the system remains "cloud-ready"—transitioning to S3/GCS will eventually involve changing URI strings rather than refactoring SQL schemas.
* **Functional Transformation:** Transformations from Bronze to Silver will be treated as pure functions. Given a JSON input, the Polars logic will produce a deterministic Parquet output, allowing for easy testing and total "Silver" layer rebuilds.

## Consequences

### Positive
* **Resilience:** If transformation logic needs to change, we can rebuild the entire Silver layer from the local Bronze files without hitting Garmin's API or triggering rate limits.
* **Performance:** Polars and DuckDB provide near-instant query speeds on local Parquet files, far outperforming SQLite for analytical workloads.
* **Stealth:** Session persistence reduces the likelihood of account flagging or 403 Forbidden errors from Garmin.
* **Format Flexibility:** Parquet is natively compatible with modern BI tools and cloud storage.

### Negative / Risks
* **Data Mutations:** Parquet does not handle row-level updates easily. If Garmin retroactively updates an activity, the entire file/partition for that day must be replaced.
* **Storage Footprint:** Storing both raw JSON and compressed Parquet increases local disk usage compared to a single database file.

## Implementation Notes
* **Session Persistence:** The client wrapper must check for token expiration before every execution and save the updated token state immediately upon successful refresh.
* **Deduplication:** The processing layer must implement logic to handle overlapping data windows to ensure the Silver layer contains unique records.
