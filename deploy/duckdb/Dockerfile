# syntax=docker/dockerfile:1.7
#
# Official DuckDB 2.0.0 image plus nine signed extensions. The final image is
# the official distroless DuckDB base: no Python, shell, supervisor or CLI
# replacement. The only non-extension runtime byte added is libexpat.so.1,
# which raquet needs. It is taken from a throw-away pinned Debian build stage
# and checked by hash; the Debian stage is not part of the final image.
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

FROM --platform=linux/amd64 duckdb/duckdb:2.0.0@sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221

ARG DUCKDB_PLATFORM=linux_amd64

COPY --from=expat /usr/lib/x86_64-linux-gnu/libexpat.so.1.8.10 /usr/lib/x86_64-linux-gnu/libexpat.so.1
COPY --from=verify /verify/extensions/ /opt/duckdb/.duckdb/extensions/v2.0.0/${DUCKDB_PLATFORM}/
COPY --chown=65532:65532 extensions.sql lake.sql lake-only.sql init.sql extensions.lock EXTENSIONS.sha256 /opt/pgvs3-duckdb/

ENV HOME=/opt/duckdb \
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
    QUACK_BIND=0.0.0.0:9494 \
    QUACK_ALLOW_OTHER_HOSTNAME=true

LABEL org.opencontainers.image.title="pgvs3-duckdb" \
      org.opencontainers.image.description="Official DuckDB 2.0.0 with signed spatial/ducklake/postgres_scanner/fts/vss/quack/httpfs/raquet/differential and native Quack" \
      org.opencontainers.image.version="v2.0.0" \
      org.opencontainers.image.source="https://github.com/adonm/pgvs3" \
      io.pgvs3.duckdb.platform="linux_amd64" \
      io.pgvs3.duckdb.source="d54cbbe3df" \
      io.pgvs3.duckdb.base="duckdb/duckdb:2.0.0@sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221"

USER 65532:65532
WORKDIR /tmp
EXPOSE 9494
STOPSIGNAL SIGTERM
ENTRYPOINT ["/duckdb"]
CMD ["-no-agent", "-batch", "-bail", "-init", "/opt/pgvs3-duckdb/init.sql"]
