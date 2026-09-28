-- pgvfs layout v1 (store::LAYOUT_VERSION). Any layout change requires a fresh DB.
--
-- The DuckDB pgvfs:// filesystem. Never shares a database with the S3
-- gateway's s3p schema: both inits refuse the other's layout.
--
-- Files are immutable once published: a write streams rows under a fresh
-- file_id and publishes (volume, path) -> file_id on close, replacing any
-- previous file there. Readers address rows by file_id only, so a file_id
-- names the same bytes forever (it is DuckDB's cache version tag).
--
-- Chunk rows are the s3p layout (crates/pgvs3/schema.sql): 8120-byte INLINE
-- payloads, one 8160-byte tuple per 8 KB page, no TOAST, 32 hash partitions.
CREATE SCHEMA IF NOT EXISTS pgvfs;

CREATE TABLE IF NOT EXISTS pgvfs.layout (version int4 NOT NULL);

CREATE SEQUENCE IF NOT EXISTS pgvfs.file_ids;

-- path collates "C": prefix listings are primary-key range scans in byte order.
CREATE TABLE IF NOT EXISTS pgvfs.files (
  volume     text COLLATE "C" NOT NULL,
  path       text COLLATE "C" NOT NULL,
  file_id    int8             NOT NULL UNIQUE,
  size       int8             NOT NULL,
  created_at timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (volume, path),
  CONSTRAINT file_shape CHECK (
    volume ~ '^[a-z0-9][a-z0-9._-]{0,62}$' AND octet_length(path) BETWEEN 1 AND 1024 AND
    size BETWEEN 0 AND 2147483648::int8 * 8120)
) WITH (autovacuum_vacuum_scale_factor = 0.02);

CREATE TABLE IF NOT EXISTS pgvfs.chunks (
  file_id int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea STORAGE EXTERNAL NOT NULL,
  PRIMARY KEY (file_id, no),
  CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
) PARTITION BY HASH (file_id);

DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS pgvfs.chunks_%s PARTITION OF pgvfs.chunks '
      'FOR VALUES WITH (MODULUS 32, REMAINDER %s) '
      'WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01, '
      'autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)',
      lpad(i::text, 2, '0'), i);
  END LOOP;
END $$;

-- A file becomes garbage in the same transaction that unpublishes it. Reads
-- take no snapshot across statements, so rows outlive the unpublish by a grace
-- period long enough for in-flight queries that already opened the file.
CREATE TABLE IF NOT EXISTS pgvfs.garbage (
  file_id   int8 PRIMARY KEY,
  queued_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS garbage_by_age ON pgvfs.garbage (queued_at, file_id);

-- Work bounded by chunk rows, not file size. Never erase a published file.
CREATE OR REPLACE FUNCTION pgvfs.reap_garbage(
  p_grace interval DEFAULT interval '10 minutes',
  p_max_rows int DEFAULT 65536
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  victim_file bigint;
  removed int := 0;
  batch_removed int;
BEGIN
  IF p_grace IS NULL OR p_max_rows IS NULL OR p_grace < interval '0 seconds' OR
     p_max_rows < 1 OR p_max_rows > 65536 THEN
    RAISE EXCEPTION 'invalid garbage budget';
  END IF;
  FOR attempt IN 1..1024 LOOP
    SELECT file_id INTO victim_file FROM pgvfs.garbage
      WHERE queued_at < now() - p_grace
      ORDER BY queued_at, file_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    EXIT WHEN victim_file IS NULL;
    IF EXISTS (SELECT 1 FROM pgvfs.files WHERE file_id = victim_file) THEN
      RAISE EXCEPTION 'garbage file % is still published', victim_file;
    END IF;
    WITH doomed AS (
      SELECT no FROM pgvfs.chunks WHERE file_id = victim_file
        ORDER BY no LIMIT (p_max_rows - removed)
    )
    DELETE FROM pgvfs.chunks c USING doomed d
      WHERE c.file_id = victim_file AND c.no = d.no;
    GET DIAGNOSTICS batch_removed = ROW_COUNT;
    removed := removed + batch_removed;
    IF batch_removed < p_max_rows - (removed - batch_removed) THEN
      DELETE FROM pgvfs.garbage WHERE file_id = victim_file;
    END IF;
    EXIT WHEN removed = p_max_rows;
  END LOOP;
  RETURN removed;
END $$;

CREATE OR REPLACE FUNCTION pgvfs.maintain() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pgvfs.reap_garbage();
END $$;
