-- One-shot PG/S3 DuckLake attach without starting Quack. Used by
-- `just duckdb-no-daemon`; the CLI is the native /duckdb binary.
.output /dev/null
.read /opt/pgvs3-duckdb/lake.sql
.output
SELECT 'PGVS3_NO_DAEMON_OK';
