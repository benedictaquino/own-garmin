# Implementation Plan: Garmin Lakehouse (v1.1 — Cloud Ready)

## Context

V1 established the local medallion architecture (Bronze/Silver/DuckDB) for Garmin activity data. This "Cloud Ready" plan (v1.1) makes the system runnable in cloud environments. Three capabilities are needed:

1. **Headless Auth** — Garmin's MFA and session persistence in environments without a persistent filesystem or interactive terminal.
2. **S3 Storage** — Read/write bronze and silver data to S3 instead of local disk, so the pipeline can run serverless.
3. **Containerization** — Package the CLI in a Docker image for deployment to ECS/Fargate/Lambda.

## Objectives

* **Headless Auth:** Decouple session persistence from the local filesystem to allow "side-loading" tokens via secrets.
* **Remote MFA:** Implement a non-interactive MFA flow using `ntfy.sh` for push-based code entry.
* **Secret Rotation Support:** Provide a way to "export" refreshed tokens so they can be saved back to a remote secret store or state manager.
* **S3 Storage:** Make `OWN_GARMIN_DATA_DIR=s3://bucket/prefix` a drop-in swap — all bronze/silver I/O works transparently with S3 URIs.
* **Containerization:** A single Docker image that runs any CLI command against S3-backed data with injected credentials.

## New Components

### `storage.py` — I/O Abstraction Layer

Thin module that dispatches on `s3://` prefix. Local paths use stdlib (`pathlib`, `glob`, `shutil`); S3 paths use `boto3` (lazy import).

Public API:

```python
def is_s3(path: str) -> bool
def read_text(path: str) -> str
def write_text(path: str, data: str) -> None
def read_bytes(path: str) -> bytes
def write_bytes(path: str, data: bytes) -> None
def exists(path: str) -> bool
def list_files(pattern: str) -> list[str]
def rmtree(path: str) -> None
def write_partitioned_parquet(df, target: str, partition_by: list[str]) -> None
```

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

### Updated `paths.py` — S3-Safe Path Construction

* Guard `os.path.expanduser()` against `s3://` URIs in `data_root()`.

### Updated Bronze/Silver Modules — Storage Abstraction

All file I/O in `bronze/activities.py`, `bronze/activity_details.py`, `bronze/fit.py`, `silver/activities.py`, and `silver/fit_records.py` migrated from `pathlib`/`open()`/`glob` to `storage.*` calls.

### Updated `query.py` — S3-Aware DuckDB

* Load DuckDB `httpfs` extension when data root is S3.
* Use `storage.list_files()` for existence checks; DuckDB handles S3 parquet reads natively.

### `Dockerfile` + `.dockerignore` — Container Image

* Multi-stage build: `uv` builder stage + slim Python 3.12 runtime.
* Includes `--extra s3` (boto3) for cloud-ready operation.
* `ENTRYPOINT ["own-garmin"]` for direct CLI invocation.

## Task Breakdown

1. `10-remote-mfa-verification.md` — Session injection, remote MFA via `ntfy.sh`, and local headless simulation.
2. `11-storage-abstraction.md` — New `storage.py` module + `paths.py` guard + `pyproject.toml` optional dep.
3. `12-bronze-s3-migration.md` — Migrate bronze modules to use `storage.*` calls.
4. `13-silver-s3-migration.md` — Migrate silver modules to use `storage.*` calls.
5. `14-query-s3-support.md` — Add httpfs loading and `storage.list_files()` to query layer.
6. `15-containerization.md` — Dockerfile, .dockerignore, and end-to-end container verification.
7. `16-minio-e2e.md` — Local MinIO-backed end-to-end verification of the S3 path (storage, bronze, silver, query, container).

## Environment Variables

| Var | Default | Purpose |
|---|---|---|
| `GARMIN_EMAIL` | — | Garmin Connect login |
| `GARMIN_PASSWORD` | — | Garmin Connect login |
| `GARMIN_TOKENS_JSON` | — | Side-loaded session tokens (headless auth) |
| `NTFY_TOPIC` | — | ntfy.sh topic for remote MFA |
| `OWN_GARMIN_DATA_DIR` | `./data` | Data root — set to `s3://bucket/prefix` for cloud |
| `OWN_GARMIN_SESSION_DIR` | `~/.config/own-garmin/session` | Token persistence (local only) |
| `AWS_ACCESS_KEY_ID` | — | AWS credentials (or use instance profile) |
| `AWS_SECRET_ACCESS_KEY` | — | AWS credentials |
| `AWS_SESSION_TOKEN` | — | AWS temporary credentials |
| `AWS_REGION` | `us-east-1` | S3 region for DuckDB httpfs |
| `AWS_ENDPOINT_URL_S3` | — | Override S3 endpoint (e.g. `http://localhost:9000` for MinIO). Auto-honored by boto3 ≥ 1.34.52. |

## Crucial Considerations & Edge Cases

### 1. DuckDB AWS Authentication

DuckDB does not automatically inherit AWS credentials from the environment (unlike `boto3`). Loading `httpfs` alone is insufficient.

* **Fix:** Load DuckDB's `aws` extension alongside `httpfs`.
* **Implementation:** Run `LOAD aws;` then `CALL load_aws_credentials();` when initializing the DuckDB connection. This resolves standard AWS env vars (or IAM Task Roles on Fargate/Lambda) automatically — no manual `s3_access_key_id` parsing.

### 2. Lambda's Read-Only Filesystem

If deployed to AWS Lambda, the filesystem is read-only except for `/tmp` (512MB–10GB).

* **DuckDB temp spills:** Complex queries that spill to disk will crash if DuckDB writes to the working directory. Add a cloud fallback: `SET temp_directory='/tmp/duckdb_temp';` when running in a cloud environment.
* **Session dir fallback:** `OWN_GARMIN_SESSION_DIR` must fail gracefully or default to `/tmp` if `~/.config/` isn't writable in the container.

### 3. `fsspec` / `s3fs` Alternative to Raw `boto3`

Rather than hand-rolling `storage.py` on top of `pathlib` + `boto3`, consider `fsspec` (via the `s3fs` package).

* **Why:** Provides a unified, Pythonic filesystem interface. A single call works transparently for both `s3://bucket/data.fit` and `./data/data.fit`, eliminating the bespoke `read_text`/`read_bytes` dispatch logic.
* **Tradeoff:** Extra dependency, but significantly reduces boilerplate in `storage.py`.

### 4. Token Rotation — stdout/stderr Hygiene

`--export-session` prints refreshed tokens to stdout. In automated cloud execution (e.g. ECS cron), an orchestrator capturing stdout to route tokens back to AWS Secrets Manager is sensitive to log pollution.

* **Safety net:** Application logs must route strictly to **stderr**; only the exported token JSON goes to **stdout**. Mixing the two breaks the orchestrator's parser.

### 5. S3-Compatible Endpoints (MinIO) for Local Testing

Real AWS S3 costs money, leaks test data to a cloud account, and makes CI slow. A local **MinIO** container gives a production-grade S3-compatible endpoint with zero cost and hermetic test runs — strictly better than `moto` mocks (which re-implement the API surface and drift from real behavior) and lighter than LocalStack.

Two code surfaces need to accept a custom endpoint:

* **`boto3` (storage layer).** Since boto3 ≥ 1.34.52, the client auto-honors `AWS_ENDPOINT_URL_S3`. Bumping the `s3` optional dep floor to `boto3>=1.34.52` lets `storage.py` stay endpoint-agnostic — no code change needed, just set the env var.
* **DuckDB `httpfs` (query layer).** The extension needs `SET s3_endpoint`, `SET s3_url_style='path'`, and `SET s3_use_ssl=false` for MinIO over HTTP. Task 14 should read `AWS_ENDPOINT_URL_S3` and, when present, emit these settings after `load_aws_credentials()`.

MinIO defaults: access key `minioadmin`, secret `minioadmin`, endpoint `http://localhost:9000`. Treat these as test-only — never share a MinIO bucket between test and prod workflows.

### 6. Security of `ntfy.sh` Topics

Public `ntfy.sh` topics are unauthenticated — anyone who guesses the topic can subscribe. A UUID (128-bit entropy) is acceptable, but:

* **Treat `NTFY_TOPIC` as a highly sensitive secret**, identical in importance to the Garmin password. Store it in the same secret store, never log it, and never commit it.

## Verification Workflow

### Headless Auth

1. Run `own-garmin login --remote-mfa` with `NTFY_TOPIC` set and local session removed.
2. Receive push notification, enter MFA code via ntfy.sh.
3. CLI resumes and prints session JSON.
4. Re-run with `GARMIN_TOKENS_JSON` to confirm side-loading works.

### S3 Storage

1. `uv run pytest` — all existing tests pass (local regression).
2. `uv run pytest tests/test_storage.py` — new storage unit tests pass.
3. **MinIO-backed e2e (preferred local path — see Task 16):**
   1. `docker compose up -d minio` and bootstrap the test bucket.
   2. Export `AWS_ENDPOINT_URL_S3=http://localhost:9000`, `AWS_ACCESS_KEY_ID=minioadmin`, `AWS_SECRET_ACCESS_KEY=minioadmin`, `OWN_GARMIN_DATA_DIR=s3://own-garmin-test/garmin`.
   3. Run `own-garmin ingest --since 2026-01-01` + `own-garmin process` + `own-garmin query "SELECT count(*) FROM activities"`.
   4. Inspect objects via the MinIO console (`http://localhost:9001`) to confirm hive partitioning.
4. **Real S3 smoke test (optional):** repeat step 3 against a scratch AWS S3 bucket without `AWS_ENDPOINT_URL_S3` set — confirms nothing was MinIO-specific.

### Container

1. `docker build -t own-garmin .` succeeds.
2. `docker run --rm own-garmin --help` shows CLI help.
3. `docker run --rm -e OWN_GARMIN_DATA_DIR=s3://... -e AWS_... own-garmin process` completes.
