# Benchmark image: pgbench (OLTP), duckdb (TPC-H + ClickBench over DuckLake),
# OpenSearch Benchmark (Quickwit's ES API) and the pgvs3 stress load generator.
# Built by `just kind-bench` on the host and loaded into kind.
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# PGDG, not Debian's default client: the charts run postgres:18 and the
# numbers should come from a matching client. pgbench ships in the server
# package, so that comes too — never started (create_main_cluster is off).
RUN . /etc/os-release \
 && install -d /usr/share/keyrings /etc/postgresql-common \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/keyrings/pgdg.asc \
 && echo "deb [signed-by=/usr/share/keyrings/pgdg.asc] http://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      postgresql-client-18 postgresql-18 \
 && rm -rf /var/lib/apt/lists/*

# Keep the OSB driver fixed; DuckDB matches mise.toml via bench.sh.
ARG DUCKDB_PY
RUN test -n "$DUCKDB_PY" \
 && pip install --no-cache-dir "duckdb==$DUCKDB_PY" "opensearch-benchmark==2.4.0"

WORKDIR /bench
COPY deploy/bench/suites/ /bench/suites/
COPY deploy/bench/harness/ /bench/harness/
# The load generator for the stress suite; staged here by the just recipe.
COPY deploy/bench/pgvs3-bin /usr/local/bin/pgvs3
