# framework/tests/local/Dockerfile
# Local CDC/DDL tests (tests/local/) — real Spark+Delta, no Databricks.
# Needs a JVM for PySpark; nothing else in this repo does.
# See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir uv

# Keep the venv outside /workspace: docker-compose.yml bind-mounts the repo
# over /workspace at run time, which would otherwise shadow (hide) a venv
# built inside it during `docker build`.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /workspace
COPY pyproject.toml uv.lock ./
RUN uv sync --group test --group dev --no-install-project

COPY . .
RUN uv sync --group test --group dev

CMD ["pytest", "tests/local", "-v"]
