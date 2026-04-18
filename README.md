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
uv run own-garmin ingest --since 2025-01-01   # pull raw JSON from Garmin Connect
uv run own-garmin process                # transform Bronze JSON → Silver Parquet
uv run own-garmin query "SELECT activity_type, COUNT(*) AS n FROM activities GROUP BY 1 ORDER BY n DESC"
```

### Environment variables

| Var | Default | Purpose |
|---|---|---|
| `GARMIN_EMAIL` | — | Garmin Connect login |
| `GARMIN_PASSWORD` | — | Garmin Connect login |
| `OWN_GARMIN_DATA_DIR` | `./data` | Root for bronze/silver layers |
| `OWN_GARMIN_SESSION_DIR` | `~/.config/own-garmin/session` | Token persistence |

## Testing against MinIO

MinIO is a local S3-compatible server that lets you exercise the full S3 code path without AWS credentials.

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

```bash
open http://localhost:9001   # login: minioadmin / minioadmin
```

Expected objects under `own-garmin-test/garmin/`:

- `bronze/activities/year=YYYY/month=MM/day=DD.json`
- `silver/activities/year=YYYY/month=MM/data.parquet`
