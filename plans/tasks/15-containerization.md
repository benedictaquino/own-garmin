# Task 15: Containerization

Create a Docker image that packages the `own-garmin` CLI for cloud execution.

**Depends on:** Tasks 11-14 (S3 storage support)

## Objective

* A single Docker image that can run any `own-garmin` CLI command.
* Pre-installs the S3 optional dependency (`boto3`) for cloud-ready operation.
* Minimal image size via multi-stage build.

## Files

* `Dockerfile` (new)
* `.dockerignore` (new)

## Implementation Steps

### 1. Create `.dockerignore`

```text
data/
.git/
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.claude/
tests/
plans/
docs/
*.md
!pyproject.toml
```

### 2. Create `Dockerfile`

Multi-stage build using uv for fast, lockfile-based installs:

```dockerfile
# --- Build stage ---
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra s3
COPY src/ src/

# --- Runtime stage ---
FROM python:3.12-slim-bookworm
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["own-garmin"]
```

Key decisions:

* **`ghcr.io/astral-sh/uv` builder**: matches project's existing uv tooling.
* **`--frozen`**: uses lockfile exactly, no resolution at build time.
* **`--extra s3`**: includes boto3 since the container is meant for cloud use.
* **`--no-dev`**: excludes pytest/ruff/pre-commit.
* **`python:3.12-slim-bookworm` runtime**: minimal base (~50MB), matches project's Python target.
* **`ENTRYPOINT ["own-garmin"]`**: container args become CLI args (e.g., `docker run own-garmin ingest --since 2024-01-01`).

### 3. Verify uv.lock exists

The Dockerfile uses `--frozen` which requires `uv.lock`. Verify this file is committed. If not, run `uv lock` first.

### 4. Lambda / read-only filesystem compatibility

If the image is targeted at AWS Lambda (or any runtime with a read-only root filesystem), only `/tmp` is writable — with a 512MB–10GB limit depending on configuration. Two places in the app write to disk and must tolerate this:

* **DuckDB spills** — Task 14 sets `temp_directory='/tmp/duckdb_temp'` when data root is S3, which handles this automatically.
* **Session persistence** — The default `OWN_GARMIN_SESSION_DIR=~/.config/own-garmin/session` will fail under Lambda. For Lambda deployments, set `OWN_GARMIN_SESSION_DIR=/tmp/own-garmin/session` explicitly via environment variable, and rely on `GARMIN_TOKENS_JSON` side-loading for the primary credential path (ephemeral `/tmp` state is a cache, not the source of truth).

Document this in the usage section below; no Dockerfile change is needed since `/tmp` exists on every Linux base image.

## Usage Examples

```bash
# Build
docker build -t own-garmin .

# Show help
docker run --rm own-garmin --help

# Ingest with session side-loading and S3 backend
docker run --rm \
  -e GARMIN_TOKENS_JSON="$(cat tokens.json)" \
  -e OWN_GARMIN_DATA_DIR=s3://my-bucket/garmin \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  own-garmin ingest --since 2024-01-01

# Process (silver rebuild)
docker run --rm \
  -e OWN_GARMIN_DATA_DIR=s3://my-bucket/garmin \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  own-garmin process

# Query
docker run --rm \
  -e OWN_GARMIN_DATA_DIR=s3://my-bucket/garmin \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  own-garmin query "SELECT count(*) FROM activities"

# Lambda-style execution (read-only root fs; only /tmp is writable)
docker run --rm \
  --read-only --tmpfs /tmp \
  -e GARMIN_TOKENS_JSON="$(cat tokens.json)" \
  -e OWN_GARMIN_SESSION_DIR=/tmp/own-garmin/session \
  -e OWN_GARMIN_DATA_DIR=s3://my-bucket/garmin \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  own-garmin process
```

## Acceptance Criteria

* [ ] `docker build -t own-garmin .` succeeds
* [ ] `docker run --rm own-garmin --help` prints CLI help
* [ ] Image size is under 300MB
* [ ] `boto3` is importable inside the container (`docker run --rm own-garmin python -c "import boto3"` — override entrypoint to verify)
* [ ] No test files, .git, or data/ included in the image
* [ ] Container runs under `--read-only --tmpfs /tmp` when `OWN_GARMIN_SESSION_DIR=/tmp/own-garmin/session` is set (Lambda-style execution works)
