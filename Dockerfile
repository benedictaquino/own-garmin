# --- Build stage ---
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
RUN uv sync --frozen --no-dev --extra s3

# --- Runtime stage ---
FROM python:3.12-slim-bookworm
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
RUN groupadd --system app && useradd --system --gid app --create-home app \
    && chown -R app:app /app
ENV PATH="/app/.venv/bin:$PATH"
USER app
ENTRYPOINT ["own-garmin"]
