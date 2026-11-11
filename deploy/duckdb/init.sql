-- ChargeWire startup SQL. TEMPLATE: render with
--   python3 deploy/duckdb/render_init.py --output /path/init.rendered.sql
-- and mount the rendered file over this path.
--
-- hardcharger-server runs init SQL through DuckDB's C++ Connection::Query:
-- pure SQL only (no .read/.output/.print) and no getenv(), which Hardcharger
-- deliberately omits. This file is exactly extensions.sql + lake.sql;
-- native_test.py enforces that, so edit those and regenerate.

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

-- DuckLake secret + attach, as a renderable template shared by the ChargeWire
-- init and the one-shot CLI check. Render with deploy/duckdb/render_init.py.
--
-- Hardcharger's embedded DuckDB deliberately omits getenv(), so no statement
-- here reads the environment: the renderer substitutes the __PGVS3_*__
-- sentinels with SQL-escaped literals and writes the result to a private path.
-- A template that reaches the server unrendered trips the first guard below,
-- which names the variable and never a value.

SELECT CASE WHEN position('__PGVS3_' IN '__PGVS3_URL__') = 1
  THEN error('PGVS3_URL is required (existing libpq-compatible database URL)') END;
SELECT CASE WHEN NOT regexp_matches('__PGVS3_URL__', '^postgres(ql)?://')
  THEN error('PGVS3_URL must use a postgres or postgresql URL scheme') END;
SELECT CASE WHEN position('__PGVS3_' IN '__PGVS3_BUCKET__') = 1
  THEN error('PGVS3_BUCKET is required; the bucket must already exist') END;
SELECT CASE WHEN NOT regexp_matches('__PGVS3_BUCKET__', '^[a-z0-9._-]{1,58}$')
  THEN error('PGVS3_BUCKET is not a safe bucket name') END;
SELECT CASE WHEN NOT regexp_matches('__PGVS3_PREFIX__', '^[A-Za-z0-9._/-]*$')
  THEN error('PGVS3_PREFIX contains an unsafe character') END;
SELECT CASE WHEN regexp_matches('__PGVS3_PREFIX__', '(^|/)\.\.?(/|$)')
  THEN error('PGVS3_PREFIX contains an unsafe path component') END;
SELECT CASE WHEN position('__PGVS3_' IN '__PGVS3_ENDPOINT__') = 1
  THEN error('PGVS3_ENDPOINT is required') END;
SELECT CASE WHEN position('__PGVS3_' IN '__PGVS3_S3_ACCESS_KEY__') = 1
  OR position('__PGVS3_' IN '__PGVS3_S3_SECRET_KEY__') = 1
  THEN error('PGVS3_S3_ACCESS_KEY and PGVS3_S3_SECRET_KEY must not be empty') END;
SELECT CASE WHEN NOT regexp_matches('__PGVS3_METADATA_SCHEMA__', '^[a-z_][a-z0-9_]*$')
  OR length('__PGVS3_METADATA_SCHEMA__') > 63
  THEN error('PGVS3_METADATA_SCHEMA must be a lower-case SQL identifier') END;

CREATE TEMP SECRET pgvs3_pg (
  TYPE postgres,
  URI '__PGVS3_URL__'
);
CREATE TEMP SECRET pgvs3_s3 (
  TYPE s3,
  KEY_ID '__PGVS3_S3_ACCESS_KEY__',
  SECRET '__PGVS3_S3_SECRET_KEY__',
  REGION '__PGVS3_S3_REGION__',
  ENDPOINT regexp_replace('__PGVS3_ENDPOINT__', '^https?://', ''),
  URL_STYLE 'path',
  USE_SSL CAST(
    CASE WHEN '__PGVS3_S3_USE_SSL__' = ''
      THEN CASE WHEN '__PGVS3_ENDPOINT__' LIKE 'https://%' THEN 'true' ELSE 'false' END
      ELSE '__PGVS3_S3_USE_SSL__'
    END AS BOOLEAN
  )
);
CREATE TEMP SECRET pgvs3_lake (
  TYPE ducklake,
  METADATA_PATH '',
  METADATA_SCHEMA '__PGVS3_METADATA_SCHEMA__',
  DATA_PATH 's3://__PGVS3_BUCKET__/' || CASE WHEN '__PGVS3_PREFIX__' = ''
      THEN ''
      ELSE '__PGVS3_PREFIX__' || '/'
    END,
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pgvs3_pg'}
);
ATTACH 'ducklake:pgvs3_lake' AS lake (AUTOMATIC_MIGRATION false);
SELECT 'PGVS3_LAKE_OK';
