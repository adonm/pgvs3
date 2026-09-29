# Kind QA image: DuckDB 2.0-series ClickBench/SpatialBench inputs and service checks.
# Built by `just kind-bench` on the host and loaded into kind.
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# PGDG's PostgreSQL 18 client matches the server used in kind.
RUN . /etc/os-release \
 && install -d /usr/share/keyrings /etc/postgresql-common \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/keyrings/pgdg.asc \
 && echo "deb [signed-by=/usr/share/keyrings/pgdg.asc] http://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf \
 && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       postgresql-client-18 git \
 && rm -rf /var/lib/apt/lists/*

# DuckDB matches the pinned 2.0-series Python wheel in mise.toml.
ARG DUCKDB_PY
RUN test -n "$DUCKDB_PY" \
 && pip install --no-cache-dir "duckdb==$DUCKDB_PY"

WORKDIR /bench
COPY deploy/bench/suites/ /bench/suites/
COPY deploy/bench/harness/ /bench/harness/
