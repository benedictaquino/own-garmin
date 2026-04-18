# Task 16: MinIO-Backed End-to-End Verification

Stand up a local **MinIO** (S3-compatible) server and validate the full S3 code path — storage layer, bronze, silver, query, and container — without hitting AWS.

**Depends on:** Tasks 11–15 (storage abstraction through containerization)

## Objective

* Provide a hermetic, zero-cost way to exercise `OWN_GARMIN_DATA_DIR=s3://...` end-to-end.
* Catch divergences between local filesystem behavior and real object-store semantics (eventual listing, key-based enumeration, no directories) before running against AWS.
* Make the S3 path reproducible in CI without AWS credentials.

## Why MinIO (not moto or LocalStack)

* **`moto`** mocks the API in-process; it diverges from real S3 behavior and only covers Python callers — DuckDB's `httpfs` extension can't talk to it.
* **LocalStack** emulates the whole AWS surface (heavy; slow startup; free tier limits some services).
* **MinIO** is a production-grade S3-compatible server in a ~60 MB container. Both `boto3` and DuckDB `httpfs` speak to it transparently once the endpoint is pointed at it. This is the highest-fidelity local option.

## Files

* `docker-compose.yml` (new) — MinIO service + one-shot bucket bootstrap.
* `Makefile` or `scripts/minio-up.sh` (new, optional) — convenience wrapper for `docker compose up -d minio && bootstrap`.
* `pyproject.toml` (modify) — bump the `s3` extra floor to `boto3>=1.34.52` so `AWS_ENDPOINT_URL_S3` is auto-honored.
* `src/own_garmin/query.py` (modify) — apply DuckDB S3 endpoint settings when `AWS_ENDPOINT_URL_S3` is set.
* `tests/test_storage_minio.py` (new, optional) — integration test gated by `OWN_GARMIN_RUN_MINIO_TESTS=1`.
* `README.md` (modify) — short "Testing against MinIO" section.

## Implementation Steps

### 1. `docker-compose.yml`

Minimal service definition with a persistent volume and a one-shot `mc` bootstrap that creates the test bucket and a read-write policy.

```yaml
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Web console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 2s
      retries: 10

  minio-bootstrap:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb --ignore-existing local/own-garmin-test &&
      echo 'bucket ready';
      "

volumes:
  minio-data:
```

### 2. Bump `boto3` floor in `pyproject.toml`

```toml
[project.optional-dependencies]
s3 = ["boto3>=1.34.52"]
```

Rationale: boto3 ≥ 1.34.52 auto-honors `AWS_ENDPOINT_URL_S3`, so `storage.py` stays endpoint-agnostic. No code change in the storage layer.

### 3. DuckDB endpoint settings in `query.py`

Inside the `is_s3(data_root())` branch added by Task 14, after `load_aws_credentials()`:

```python
endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
if endpoint:
    # Strip scheme — DuckDB wants "host:port", not "http://host:port"
    host = endpoint.split("://", 1)[-1].rstrip("/")
    use_ssl = endpoint.startswith("https://")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
```

`path`-style URLs are required because MinIO does not route virtual-host-style buckets by default. The same settings are harmless against real AWS when `AWS_ENDPOINT_URL_S3` is unset.

### 4. E2E verification flow

Document the following in the README's "Testing against MinIO" section and run it as the acceptance check:

```bash
# 1. Start MinIO + create bucket
docker compose up -d minio minio-bootstrap

# 2. Point the pipeline at MinIO
export AWS_ENDPOINT_URL_S3=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export OWN_GARMIN_DATA_DIR=s3://own-garmin-test/garmin

# 3. Exercise the full pipeline (requires a valid Garmin session)
uv run own-garmin ingest --since 2026-01-01
uv run own-garmin process
uv run own-garmin query "SELECT activity_type, COUNT(*) AS n FROM activities GROUP BY 1 ORDER BY n DESC"

# 4. Inspect objects in the MinIO console
open http://localhost:9001  # login: minioadmin / minioadmin
```

Expected objects under `own-garmin-test/garmin/`:

* `bronze/activities/year=2026/month=*/day=*.json`
* `silver/activities/year=*/month=*/data.parquet` (hive-partitioned)

### 5. Container verification against MinIO

Validate the Dockerfile from Task 15 talks to MinIO when run on the same Docker network:

```bash
docker compose up -d minio minio-bootstrap
docker build -t own-garmin .
docker run --rm \
  --network $(docker compose ps -q minio | xargs docker inspect -f '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}') \
  -e AWS_ENDPOINT_URL_S3=http://minio:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_REGION=us-east-1 \
  -e OWN_GARMIN_DATA_DIR=s3://own-garmin-test/garmin \
  own-garmin query "SELECT count(*) FROM activities"
```

Key detail: inside the container, the endpoint host is `minio` (docker network DNS), not `localhost`.

### 6. Optional integration test

Add `tests/test_storage_minio.py` guarded by an env var so it's skipped by default:

```python
import os
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("OWN_GARMIN_RUN_MINIO_TESTS") != "1",
    reason="set OWN_GARMIN_RUN_MINIO_TESTS=1 after starting MinIO",
)
```

Cover: `write_bytes` → `read_bytes` round-trip, `list_files` with glob, `rmtree` cleanup, `write_partitioned_parquet` produces the expected hive keys. These mirror the existing local `test_storage.py` cases against a real S3 API.

## Verification Checklist

* [ ] `docker compose up -d minio minio-bootstrap` reports healthy and the test bucket exists.
* [ ] Storage ops (read/write/list/rmtree/partitioned parquet) work against `s3://own-garmin-test/...` with `AWS_ENDPOINT_URL_S3` set.
* [ ] `own-garmin ingest`, `process`, and `query` all succeed end-to-end against MinIO.
* [ ] DuckDB reads hive-partitioned parquet over `httpfs` with `s3_endpoint` pointed at MinIO.
* [ ] The same container image from Task 15 works against MinIO when placed on its Docker network.
* [ ] `uv run pytest` (without `OWN_GARMIN_RUN_MINIO_TESTS`) still passes — MinIO is opt-in, not a unit-test dependency.
* [ ] README documents the MinIO workflow.

## Acceptance Criteria

* [ ] `docker-compose.yml` launches MinIO + creates the test bucket with one command.
* [ ] `AWS_ENDPOINT_URL_S3` is the only knob users flip to redirect the pipeline to MinIO — no code branches on "is this MinIO?".
* [ ] `query.py` honors `AWS_ENDPOINT_URL_S3` for the DuckDB `httpfs` path-style + non-SSL settings.
* [ ] `pyproject.toml` `s3` extra requires `boto3>=1.34.52`.
* [ ] README has a runnable "Testing against MinIO" section.
* [ ] Full ingest → process → query pipeline verified against MinIO and visible in the MinIO console.

## Notes

* MinIO's default credentials (`minioadmin`/`minioadmin`) are test-only. Never reuse a MinIO bucket for anything other than disposable e2e data.
* If CI adopts this flow, run `docker compose down -v` between jobs to avoid cross-run parquet leaking into a fresh test.
* This task does **not** replace the real-AWS smoke test — keep at least one manual run against a scratch AWS S3 bucket before declaring v1.1 done, since MinIO does not perfectly model IAM, SSE, or regional routing edge cases.
