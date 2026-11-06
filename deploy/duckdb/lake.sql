-- One-shot DuckLake setup, also read by init.sql before Quack starts.
-- Secrets are built from getenv() so SQL text and logs never contain the
-- PostgreSQL URL, credentials or token. A literal ATTACH URI leaks the URL,
-- so do not add one here.
.read /opt/pgvs3-duckdb/extensions.sql

SELECT CASE WHEN coalesce(getenv('PGVS3_URL'), '') = ''
  THEN error('PGVS3_URL is required (existing libpq-compatible database URL)') END;
SELECT CASE WHEN NOT regexp_matches(getenv('PGVS3_URL'), '^postgres(ql)?://')
  THEN error('PGVS3_URL must use a postgres or postgresql URL scheme') END;
SELECT CASE WHEN coalesce(getenv('PGVS3_BUCKET'), '') = ''
  THEN error('PGVS3_BUCKET is required; the bucket must already exist') END;
SELECT CASE WHEN NOT regexp_matches(getenv('PGVS3_BUCKET'), '^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$')
  THEN error('PGVS3_BUCKET is not a safe bucket name') END;
SELECT CASE WHEN NOT regexp_matches(coalesce(getenv('PGVS3_PREFIX'), ''), '^[A-Za-z0-9._/-]*$')
  THEN error('PGVS3_PREFIX contains an unsafe character') END;
SELECT CASE WHEN regexp_matches(coalesce(getenv('PGVS3_PREFIX'), ''), '(^|/)\.\.?(/|$)')
  THEN error('PGVS3_PREFIX contains an unsafe path component') END;
SELECT CASE WHEN coalesce(getenv('PGVS3_ENDPOINT'), '') = ''
  THEN error('PGVS3_ENDPOINT is required') END;
SELECT CASE WHEN coalesce(getenv('PGVS3_S3_ACCESS_KEY'), '') = ''
  OR coalesce(getenv('PGVS3_S3_SECRET_KEY'), '') = ''
  THEN error('PGVS3_S3_ACCESS_KEY and PGVS3_S3_SECRET_KEY must not be empty') END;
SELECT CASE WHEN NOT regexp_matches(coalesce(getenv('PGVS3_METADATA_SCHEMA'), ''), '^[a-z_][a-z0-9_]*$')
  OR length(getenv('PGVS3_METADATA_SCHEMA')) > 63
  THEN error('PGVS3_METADATA_SCHEMA must be a lower-case SQL identifier') END;

CREATE TEMP SECRET pgvs3_pg (
  TYPE postgres,
  URI getenv('PGVS3_URL')
);
CREATE TEMP SECRET pgvs3_s3 (
  TYPE s3,
  KEY_ID getenv('PGVS3_S3_ACCESS_KEY'),
  SECRET getenv('PGVS3_S3_SECRET_KEY'),
  REGION getenv('PGVS3_S3_REGION'),
  ENDPOINT regexp_replace(getenv('PGVS3_ENDPOINT'), '^https?://', ''),
  URL_STYLE 'path',
  USE_SSL CAST(coalesce(
    nullif(getenv('PGVS3_S3_USE_SSL'), ''),
    CASE WHEN getenv('PGVS3_ENDPOINT') LIKE 'https://%' THEN 'true' ELSE 'false' END
  ) AS BOOLEAN)
);
CREATE TEMP SECRET pgvs3_lake (
  TYPE ducklake,
  METADATA_PATH '',
  METADATA_SCHEMA getenv('PGVS3_METADATA_SCHEMA'),
  DATA_PATH 's3://' || getenv('PGVS3_BUCKET') || '/' ||
    CASE WHEN trim(getenv('PGVS3_PREFIX'), '/') = ''
      THEN ''
      ELSE trim(getenv('PGVS3_PREFIX'), '/') || '/'
    END,
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pgvs3_pg'}
);
ATTACH 'ducklake:pgvs3_lake' AS lake (AUTOMATIC_MIGRATION false);
USE lake;
SELECT 'PGVS3_LAKE_OK';
