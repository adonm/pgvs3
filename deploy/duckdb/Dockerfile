# syntax=docker/dockerfile:1.7
#
# Hardcharger 2.10.0 (ChargeWire 2.4.0) with the eight signed DuckDB v2.0.0
# extensions pgvs3 uses. Hardcharger embeds the exact official DuckDB v2.0.0
# (source id d54cbbe3df), so extension binaries from the official v2.0.0 feed
# load unchanged; the official CLI is still copied to /duckdb for one-shot
# non-daemon checks. The only non-extension runtime byte added for the modules
# is libexpat.so.1, which raquet needs; it comes from a throw-away pinned
# Debian build stage and is checked by hash.
#
# amd64 only in this increment. Arm64 hashes are not pinned.
#
# The build context is deploy/duckdb. Prepare .artifacts with
# `python3 fetch_artifacts.py` (or `just duckdb-image`). Nothing is downloaded
# or installed at container runtime.

ARG DEBIAN_DIGEST=sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

FROM --platform=linux/amd64 debian:bookworm-slim@${DEBIAN_DIGEST} AS expat
RUN apt-get update \
 && apt-get install -y --no-install-recommends libexpat1=2.5.0-1+deb12u5 \
 && rm -rf /var/lib/apt/lists/*
RUN echo "b342d94f6a71df596f19b1b0d4d3af9b883f0333027de6c3fc0dbc9ff8c45057  /usr/lib/x86_64-linux-gnu/libexpat.so.1.8.10" \
  | sha256sum -c -

FROM --platform=linux/amd64 duckdb/duckdb:2.0.0@sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221 AS official

FROM --platform=linux/amd64 debian:bookworm-slim@${DEBIAN_DIGEST} AS verify
ARG DUCKDB_PLATFORM=linux_amd64
COPY --from=official /duckdb /verify/duckdb
COPY .artifacts/extensions/v2.0.0/${DUCKDB_PLATFORM}/ /verify/extensions/
COPY EXTENSIONS.sha256 /verify/EXTENSIONS.sha256
RUN test "$(sha256sum /verify/duckdb | cut -d' ' -f1)" = "c41f957db5645d545a9caef430994cf262d24fad9297fef70c691395aadb43fb" \
 && cd /verify/extensions \
 && sha256sum -c /verify/EXTENSIONS.sha256

FROM --platform=linux/amd64 ghcr.io/gizmodata/hardcharger:2.10.0@sha256:35e7e1616b575ecf04f07b3308ca7435029fb26b24f0ec9ea38248375a27ed36

ARG DUCKDB_PLATFORM=linux_amd64
ARG EXTENSION_ROOT=/var/lib/hardcharger/.duckdb/extensions/v2.0.0/${DUCKDB_PLATFORM}

USER root

COPY --from=expat /usr/lib/x86_64-linux-gnu/libexpat.so.1.8.10 /usr/lib/x86_64-linux-gnu/libexpat.so.1
COPY --from=official /duckdb /duckdb
COPY --from=verify /verify/extensions/ ${EXTENSION_ROOT}/
COPY extensions.sql lake.sql extensions.lock EXTENSIONS.sha256 /opt/pgvs3-duckdb/
# /opt/pgvs3-duckdb/extensions is the extension lookup path for
# `SET extension_directories=[...]`; it is a symlink, not a second copy.
# init.sql here is the unrendered TEMPLATE: Hardcharger's DuckDB has no
# getenv(), so the real init SQL is rendered from the environment and mounted
# over this path (see deploy/duckdb/README.md). Starting without it fails at
# the template's own __PGVS3_* guard instead of attaching a bogus catalog.
COPY init.sql lake-only.sql /opt/pgvs3-duckdb/
RUN ln -s "${EXTENSION_ROOT}" /opt/pgvs3-duckdb/extensions \
 && chown -R 999:999 /var/lib/hardcharger/.duckdb \
 && chown -R 0:0 /opt/pgvs3-duckdb /duckdb

USER hardcharger

ENV HOME=/var/lib/hardcharger \
    DUCKDB_MEMORY_LIMIT=2GB \
    DUCKDB_THREADS=4 \
    DUCKDB_TEMP_DIRECTORY=/tmp/duckdb \
    PGVS3_ENDPOINT=127.0.0.1:8014 \
    PGVS3_PREFIX=ducklake \
    PGVS3_METADATA_SCHEMA=ducklake_pgvs3 \
    PGVS3_S3_ACCESS_KEY=pgvs3 \
    PGVS3_S3_SECRET_KEY=pgvs3 \
    PGVS3_S3_REGION=us-east-1 \
    PGVS3_S3_USE_SSL= \
    HARDCHARGER_HOST=0.0.0.0 \
    HARDCHARGER_PORT=31337 \
    HARDCHARGER_HEALTH_PORT=31338 \
    HARDCHARGER_ALLOW_UNSIGNED_EXTENSIONS=false \
    HARDCHARGER_INIT_SQL_COMMANDS_FILE=/opt/pgvs3-duckdb/init.sql \
    HARDCHARGER_DEFAULT_CATALOG=lake \
    HARDCHARGER_ADMIN_USERNAME=pgvs3 \
    HARDCHARGER_GRACEFUL_SHUTDOWN=true \
    HARDCHARGER_SHUTDOWN_GRACE_PERIOD_SECONDS=15 \
    HARDCHARGER_MEMORY_LIMIT=2GB

LABEL org.opencontainers.image.title="pgvs3-duckdb" \
      org.opencontainers.image.description="Hardcharger 2.10.0 (ChargeWire 2.4.0) on embedded DuckDB v2.0.0 with signed spatial/ducklake/postgres_scanner/fts/vss/httpfs/raquet/differential" \
      org.opencontainers.image.version="v2.0.0" \
      org.opencontainers.image.source="https://github.com/adonm/pgvs3" \
      io.pgvs3.duckdb.platform="linux_amd64" \
      io.pgvs3.duckdb.source="d54cbbe3df" \
      io.pgvs3.duckdb.base="ghcr.io/gizmodata/hardcharger:2.10.0@sha256:35e7e1616b575ecf04f07b3308ca7435029fb26b24f0ec9ea38248375a27ed36" \
      io.pgvs3.duckdb.cli="duckdb/duckdb:2.0.0@sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221"

WORKDIR /var/lib/hardcharger
EXPOSE 31337 31338
STOPSIGNAL SIGTERM
ENTRYPOINT ["hardcharger-server"]
