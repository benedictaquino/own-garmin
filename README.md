# Own Garmin

Personal Garmin Connect data pipeline — ingest, process, and query your activity history.

Pulls activity data from Garmin Connect into a local (or S3-backed) medallion lakehouse: raw JSON in Bronze, cleaned Parquet in Silver, queryable via DuckDB.

## Quickstart

```bash
uv sync
```

Set credentials:

```bash
export GARMIN_EMAIL=you@example.com
export GARMIN_PASSWORD=yourpassword
```

Run the pipeline:

```bash
uv run own-garmin login                  # authenticate and persist session token
uv run own-garmin ingest --since 2025-01-01   # pull activity summaries, details, and FIT files into bronze
uv run own-garmin process                # build Silver Parquet: activities summary + fit_records per-sample tables
uv run own-garmin query "SELECT activity_type, COUNT(*) AS n FROM activities GROUP BY 1 ORDER BY n DESC"
```

The `fit_records` view is also available in queries when FIT files have been ingested and processed.

### Remote MFA

If your Garmin account has MFA enabled, login is interactive by default. Two alternatives for non-interactive or remote environments:

- `own-garmin login --remote-mfa` — publishes an MFA prompt to an ntfy.sh topic and polls for the 6-digit reply. Requires the `NTFY_TOPIC` env var.
- `own-garmin login --export-session` — prints a `GARMIN_TOKENS_JSON` value to stdout after login. Pass this to subsequent container or CI runs via the `GARMIN_TOKENS_JSON` env var to skip the login flow entirely.

### Environment variables

| Var | Default | Purpose |
|---|---|---|
| `GARMIN_EMAIL` | — | Garmin Connect login |
| `GARMIN_PASSWORD` | — | Garmin Connect login |
| `OWN_GARMIN_DATA_DIR` | `./data` | Root for bronze/silver layers |
| `OWN_GARMIN_SESSION_DIR` | `~/.config/own-garmin/session` | Token persistence |

## Running via Docker

Build the image and run the pipeline without a local Python environment:

```bash
docker build -t own-garmin .
docker run --rm \
  -e GARMIN_EMAIL \
  -e GARMIN_PASSWORD \
  -v $(pwd)/data:/app/data \
  own-garmin ingest --since 2025-01-01
```

The image includes the `s3` extra, so `OWN_GARMIN_DATA_DIR=s3://…` works without any additional setup.

## Testing against MinIO

MinIO is a local S3-compatible server that lets you exercise the full S3 code path without an AWS account. (MinIO has its own test credentials, set below.)

### 1. Start MinIO and create the test bucket

```bash
docker compose up -d minio minio-bootstrap
```

### 2. Point the pipeline at MinIO

```bash
export AWS_ENDPOINT_URL_S3=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export OWN_GARMIN_DATA_DIR=s3://own-garmin-test/garmin
```

> These credentials are test-only. Never reuse a MinIO bucket for anything other than disposable e2e data.

### 3. Run the full pipeline

```bash
uv run own-garmin ingest --since 2026-01-01
uv run own-garmin process
uv run own-garmin query "SELECT activity_type, COUNT(*) AS n FROM activities GROUP BY 1 ORDER BY n DESC"
```

### 4. Inspect objects in the MinIO console

Open <http://localhost:9001> in your browser (login: `minioadmin` / `minioadmin`).

Expected objects under `own-garmin-test/garmin/`:

- `bronze/activities/year=YYYY/month=MM/day=DD.json`
- `silver/activities/year=YYYY/month=MM/data.parquet`

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — detailed architecture overview: layers, module map, storage abstraction, authentication flow, and deployment.
- [`docs/adrs/ADR-001-garmin-lakehouse.md`](docs/adrs/ADR-001-garmin-lakehouse.md) — original architecture decision record.
