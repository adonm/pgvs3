-- Signed extension setup shared by the ChargeWire init and one-shot lake checks.
--
-- TEMPLATE. Render with deploy/duckdb/render_init.py before use: it replaces
-- the __DUCKDB_*__ sentinels with values from the environment. Every module is
-- baked into the image, so there is no INSTALL and unsigned loading stays off.
-- Hardcharger's DuckDB has no getenv(), so nothing here reads the environment.
SET extension_directories=['/opt/pgvs3-duckdb/extensions'];
SET autoinstall_known_extensions=false;
SET autoload_known_extensions=false;
SET allow_unsigned_extensions=false;
LOAD spatial;
LOAD ducklake;
LOAD postgres_scanner;
LOAD fts;
LOAD vss;
LOAD httpfs;
LOAD raquet;
LOAD differential;
SET threads=__DUCKDB_THREADS__;
SET memory_limit='__DUCKDB_MEMORY_LIMIT__';
SET temp_directory='__DUCKDB_TEMP_DIRECTORY__';
