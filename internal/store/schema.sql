-- Alpha layout v9 (db::LAYOUT_VERSION). Any layout change requires a fresh DB.
--
-- Object bytes are fixed-size INLINE rows: 8120-byte payloads stay inline
-- with toast_tuple_target = 8160 (heaptoast.c only externalises while
-- data_size > RelationGetToastTupleTarget - hoff; hoff = 24 with all-NOT-NULL
-- columns, so 8 + 4 + 4 + 8120 = 8136 <= 8136): one 8160-byte tuple per 8 KB
-- page, 99.6% fill, no TOAST, no toast-pointer indirection; slices are memcpy.
CREATE SCHEMA IF NOT EXISTS s3p;

CREATE TABLE IF NOT EXISTS s3p.layout (version int4 NOT NULL);

CREATE TABLE IF NOT EXISTS s3p.buckets (
  name       text COLLATE "C" PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- file_id is deliberately not UNIQUE: CopyObject shares immutable files
-- between object rows, and GC checks every reference before reaping.
-- Small objects (<= 64 KiB) live entirely in inline_data with file_id NULL:
-- one row read instead of a file_id + chunk lookup.
-- bucket/key collate "C": S3 lists keys in byte order, so the primary key
-- serves both point lookups and ordered LIST range scans.
CREATE SEQUENCE IF NOT EXISTS s3p.file_ids CACHE 64;

CREATE TABLE IF NOT EXISTS s3p.objects (
  bucket     text COLLATE "C" NOT NULL,
  key        text COLLATE "C" NOT NULL,
  file_id    bigint           DEFAULT nextval('s3p.file_ids'),  -- single file or first part
  size       int8             NOT NULL,
  sha256     bytea            NOT NULL,         -- integrity digest; multipart: of the part sha256s
  etag       text             NOT NULL,         -- S3 ETag: MD5, or MD5 of part MD5s + "-count"
  user_metadata text[]       NOT NULL DEFAULT '{}'::text[],
  content_type text          NOT NULL DEFAULT 'application/octet-stream',
  created_at timestamptz      NOT NULL DEFAULT now(),
  parts      int8[],                            -- multipart: part file_ids in order
  part_ends  int8[],                            -- multipart: cumulative end offsets
  inline_data bytea STORAGE EXTERNAL,           -- small object bytes; file_id is NULL
  PRIMARY KEY (bucket, key),
  FOREIGN KEY (bucket) REFERENCES s3p.buckets (name),
  CONSTRAINT object_shape CHECK (
     octet_length(key) BETWEEN 1 AND 1024 AND size >= 0 AND octet_length(sha256) = 32 AND
     etag ~ '^[0-9a-f]{32}(-[1-9][0-9]{0,4})?$' AND
     cardinality(user_metadata) <= 128 AND cardinality(user_metadata) % 2 = 0 AND
     octet_length(content_type) BETWEEN 1 AND 1024 AND
    ((inline_data IS NOT NULL AND file_id IS NULL AND parts IS NULL AND part_ends IS NULL AND
      octet_length(inline_data) <= 65536 AND octet_length(inline_data) = size)
     OR
     (inline_data IS NULL AND file_id IS NOT NULL AND parts IS NULL AND part_ends IS NULL AND
      size <= 2147483648::int8 * 8120)
     OR
     (inline_data IS NULL AND file_id IS NOT NULL AND parts IS NOT NULL AND part_ends IS NOT NULL AND
      cardinality(parts) > 0 AND array_ndims(parts) = 1 AND array_ndims(part_ends) = 1 AND
      array_lower(parts, 1) = 1 AND array_lower(part_ends, 1) = 1 AND
      cardinality(parts) = cardinality(part_ends) AND parts[1] = file_id AND
      part_ends[cardinality(part_ends)] = size AND
      array_position(parts, NULL) IS NULL AND array_position(part_ends, NULL) IS NULL))
  )
) WITH (autovacuum_vacuum_scale_factor = 0.02);
ALTER SEQUENCE s3p.file_ids OWNED BY s3p.objects.file_id;
CREATE INDEX IF NOT EXISTS objects_parts_idx ON s3p.objects USING gin (parts)
  WHERE parts IS NOT NULL;

-- One relation caps at MaxBlockNumber (0xFFFFFFFE) x 8 KB = 32 TiB
-- (storage/block.h): ~31.7 TiB of object data at one row per page. That is
-- pgvs3's capacity. Single-writer DuckLake does not need partitions to spread
-- concurrent inserts; every GET is a `file_id = $1` primary-key range.
CREATE TABLE IF NOT EXISTS s3p.chunks (
  file_id int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea STORAGE EXTERNAL NOT NULL,
  PRIMARY KEY (file_id, no),
  CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
) WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01,
        autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);

-- In-progress multipart uploads live in PostgreSQL, not gateway memory: any
-- gateway can take any part and uploads survive gateway restarts.
CREATE TABLE IF NOT EXISTS s3p.uploads (
  upload_id  text             PRIMARY KEY,
  bucket     text COLLATE "C" NOT NULL,
  key        text COLLATE "C" NOT NULL,
  -- Algorithm every part must carry and Complete must repeat (S3 composite).
  checksum_algorithm text CHECK (checksum_algorithm IN ('CRC32', 'CRC32C', 'CRC64NVME', 'SHA1', 'SHA256')),
  user_metadata text[]       NOT NULL DEFAULT '{}'::text[],
  content_type text          NOT NULL DEFAULT 'application/octet-stream',
  created_at timestamptz      NOT NULL DEFAULT now(),
  FOREIGN KEY (bucket) REFERENCES s3p.buckets (name),
  CONSTRAINT upload_metadata_shape CHECK (
    cardinality(user_metadata) <= 128 AND cardinality(user_metadata) % 2 = 0 AND
    octet_length(content_type) BETWEEN 1 AND 1024)
);
CREATE INDEX IF NOT EXISTS uploads_by_bucket_key ON s3p.uploads (bucket, key);

CREATE TABLE IF NOT EXISTS s3p.upload_parts (
  upload_id text  NOT NULL REFERENCES s3p.uploads ON DELETE CASCADE,
  part_no   int4  NOT NULL,
  file_id   int8  NOT NULL,
  size      int8  NOT NULL,
  sha256    bytea NOT NULL,
  md5       bytea NOT NULL,
  checksum  text,                                -- verified base64 value of the upload's algorithm
  PRIMARY KEY (upload_id, part_no),
  CONSTRAINT part_shape CHECK (part_no BETWEEN 1 AND 10000 AND size >= 0 AND
    size <= 2147483648::int8 * 8120 AND octet_length(sha256) = 32 AND
    octet_length(md5) = 16 AND (checksum IS NULL OR octet_length(checksum) BETWEEN 1 AND 128))
);
CREATE INDEX IF NOT EXISTS uploads_by_age ON s3p.uploads (created_at, upload_id);

-- A file becomes garbage in the *same transaction* that drops its final
-- reference. Reads started earlier retain their MVCC snapshot during cleanup.
-- Per-part checksums for completed multipart objects. file_id ties a row to
-- the current part so stale rows from an overwritten object are ignored.
CREATE TABLE IF NOT EXISTS s3p.object_part_checksums (
  bucket     text COLLATE "C" NOT NULL,
  key        text COLLATE "C" NOT NULL,
  part_no    int4 NOT NULL,
  file_id    int8 NOT NULL,
  algorithm  text NOT NULL,
  value      text NOT NULL,
  PRIMARY KEY (bucket, key, part_no),
  FOREIGN KEY (bucket, key) REFERENCES s3p.objects (bucket, key)
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS s3p.garbage (
  file_id   int8 PRIMARY KEY,
  queued_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS garbage_by_age ON s3p.garbage (queued_at, file_id);

-- One-round-trip replacement of a multipart part, including safe garbage.
CREATE OR REPLACE FUNCTION s3p.commit_part(
  p_upload text, p_no int4, p_file_id int8, p_size int8, p_sha bytea,
  p_md5 bytea, p_checksum text DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE old bigint;
BEGIN
  DELETE FROM s3p.upload_parts WHERE upload_id = p_upload AND part_no = p_no
    RETURNING file_id INTO old;
  IF old IS NOT NULL THEN
    INSERT INTO s3p.garbage (file_id)
    SELECT old WHERE NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.file_id = old OR old = ANY(o.parts))
      AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = old)
    ON CONFLICT DO NOTHING;
  END IF;
  INSERT INTO s3p.upload_parts (upload_id, part_no, file_id, size, sha256, md5, checksum)
  VALUES (p_upload, p_no, p_file_id, p_size, p_sha, p_md5, p_checksum);
END $$;

-- One-round-trip publish for a chunked object after COPY.
CREATE OR REPLACE FUNCTION s3p.publish_object(
  p_bucket text, p_key text, p_file_id int8, p_size int8, p_sha256 bytea,
  p_etag text, p_meta text[], p_ct text, p_cond text DEFAULT 'none',
  p_if_match text DEFAULT NULL
) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  old_file bigint;
  old_parts int8[];
  old_etag text;
  found_old boolean;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(p_bucket, 0) # hashtextextended(p_key, 1));
  SELECT file_id, parts, etag INTO old_file, old_parts, old_etag
    FROM s3p.objects WHERE bucket = p_bucket AND key = p_key FOR UPDATE;
  found_old := FOUND;
  IF p_cond = 'if_absent' AND found_old THEN
    RETURN 'precondition';
  END IF;
  IF p_cond = 'if_match' AND (NOT found_old OR old_etag <> p_if_match) THEN
    RETURN 'precondition';
  END IF;
  DELETE FROM s3p.object_part_checksums WHERE bucket = p_bucket AND key = p_key;
  INSERT INTO s3p.objects
    (bucket, key, file_id, size, sha256, etag, user_metadata, content_type,
     parts, part_ends, inline_data)
  VALUES
    (p_bucket, p_key, p_file_id, p_size, p_sha256, p_etag, p_meta, p_ct,
     NULL, NULL, NULL)
  ON CONFLICT (bucket, key) DO UPDATE SET
    file_id = EXCLUDED.file_id, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
    etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
    content_type = EXCLUDED.content_type, parts = NULL, part_ends = NULL,
    inline_data = NULL, created_at = now();
  IF found_old THEN
    INSERT INTO s3p.garbage (file_id)
    SELECT f FROM unnest(COALESCE(old_parts, ARRAY[]::int8[]) || old_file) AS f
    WHERE f IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.file_id = f OR f = ANY(o.parts))
      AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = f)
    ON CONFLICT DO NOTHING;
  END IF;
  PERFORM pg_notify('s3p_invalidate', p_bucket || chr(31) || p_key);
  RETURN 'ok';
END $$;

-- One-round-trip small PUT. Returns nobucket, precondition or ok; the body is
-- already verified and hashed by the caller.
CREATE OR REPLACE FUNCTION s3p.put_inline(
  p_bucket text, p_key text, p_data bytea, p_sha256 bytea, p_etag text,
  p_meta text[], p_ct text, p_cond text DEFAULT 'none', p_if_match text DEFAULT NULL,
  p_sync text DEFAULT 'on'
) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  old_file bigint;
  old_parts int8[];
  old_etag text;
  found_old boolean;
BEGIN
  IF p_cond = 'none' AND p_sync IN ('local','off','remote_write','remote_apply') THEN
    PERFORM set_config('synchronous_commit', p_sync, true);
  ELSE
    PERFORM set_config('synchronous_commit', 'on', true);
  END IF;
  IF octet_length(p_data) > 65536 THEN
    RAISE EXCEPTION 'inline object exceeds 64 KiB';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM s3p.buckets WHERE name = p_bucket) THEN
    RETURN 'nobucket';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(p_bucket, 0) # hashtextextended(p_key, 1));
  SELECT file_id, parts, etag INTO old_file, old_parts, old_etag
    FROM s3p.objects WHERE bucket = p_bucket AND key = p_key FOR UPDATE;
  found_old := FOUND;
  IF p_cond = 'if_absent' AND found_old THEN
    RETURN 'precondition';
  END IF;
  IF p_cond = 'if_match' AND (NOT found_old OR old_etag <> p_if_match) THEN
    RETURN 'precondition';
  END IF;
  DELETE FROM s3p.object_part_checksums WHERE bucket = p_bucket AND key = p_key;
  INSERT INTO s3p.objects
    (bucket, key, file_id, size, sha256, etag, user_metadata, content_type,
     parts, part_ends, inline_data)
  VALUES
    (p_bucket, p_key, NULL, octet_length(p_data), p_sha256, p_etag, p_meta, p_ct,
     NULL, NULL, p_data)
  ON CONFLICT (bucket, key) DO UPDATE SET
    file_id = NULL, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
    etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
    content_type = EXCLUDED.content_type, parts = NULL, part_ends = NULL,
    inline_data = EXCLUDED.inline_data, created_at = now();
  IF found_old THEN
    INSERT INTO s3p.garbage (file_id)
    SELECT f FROM unnest(COALESCE(old_parts, ARRAY[]::int8[]) || old_file) AS f
    WHERE f IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.file_id = f OR f = ANY(o.parts))
      AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = f)
    ON CONFLICT DO NOTHING;
  END IF;
  PERFORM pg_notify('s3p_invalidate', p_bucket || chr(31) || p_key);
  RETURN 'ok';
END $$;

-- Expire whole uploads atomically. A concurrent part writer holds KEY SHARE on
-- the upload row; Complete/Abort and expiry hold UPDATE on that same row.
CREATE OR REPLACE FUNCTION s3p.expire_uploads(
  p_grace interval DEFAULT interval '24 hours',
  p_max_uploads int DEFAULT 64,
  p_bucket text DEFAULT NULL
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  victim_upload text;
  removed int := 0;
BEGIN
  IF p_grace IS NULL OR p_max_uploads IS NULL OR
     p_grace < interval '0 seconds' OR p_max_uploads < 1 OR p_max_uploads > 256 THEN
    RAISE EXCEPTION 'invalid upload expiry budget';
  END IF;
  FOR attempt IN 1..p_max_uploads LOOP
    SELECT upload_id INTO victim_upload FROM s3p.uploads
      WHERE created_at < now() - p_grace AND (p_bucket IS NULL OR bucket = p_bucket)
      ORDER BY created_at, upload_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    EXIT WHEN victim_upload IS NULL;
    INSERT INTO s3p.garbage (file_id)
      SELECT file_id FROM s3p.upload_parts WHERE upload_id = victim_upload
      ON CONFLICT DO NOTHING;
    DELETE FROM s3p.uploads WHERE upload_id = victim_upload;
    removed := removed + 1;
  END LOOP;
  RETURN removed;
END $$;

-- Work bounded by chunk rows, not object size. Never erase a referenced file;
-- raise an error so a broken invariant is visible instead of losing bytes.
CREATE OR REPLACE FUNCTION s3p.reap_garbage(p_max_rows int DEFAULT 65536)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  victim_file bigint;
  removed int := 0;
  batch_removed int;
BEGIN
  IF p_max_rows IS NULL OR p_max_rows < 1 OR p_max_rows > 65536 THEN
    RAISE EXCEPTION 'invalid garbage budget';
  END IF;
  FOR attempt IN 1..1024 LOOP
    SELECT file_id INTO victim_file FROM s3p.garbage
      ORDER BY queued_at, file_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    EXIT WHEN victim_file IS NULL;
    IF EXISTS (SELECT 1 FROM s3p.objects WHERE file_id = victim_file) OR
       EXISTS (SELECT 1 FROM s3p.objects WHERE parts IS NOT NULL AND parts @> ARRAY[victim_file]) OR
       EXISTS (SELECT 1 FROM s3p.upload_parts WHERE file_id = victim_file) THEN
      -- A shared CopyObject/UploadPartCopy file can be queued by an older
      -- gateway. Drop the false-positive marker and continue.
      DELETE FROM s3p.garbage WHERE file_id = victim_file;
      CONTINUE;
    END IF;
    WITH doomed AS (
      SELECT no FROM s3p.chunks WHERE file_id = victim_file
        ORDER BY no LIMIT (p_max_rows - removed)
    )
    DELETE FROM s3p.chunks c USING doomed d
      WHERE c.file_id = victim_file AND c.no = d.no;
    GET DIAGNOSTICS batch_removed = ROW_COUNT;
    removed := removed + batch_removed;
    IF batch_removed < p_max_rows - (removed - batch_removed) THEN
      DELETE FROM s3p.garbage WHERE file_id = victim_file;
    END IF;
    EXIT WHEN removed = p_max_rows;
  END LOOP;
  RETURN removed;
END $$;

CREATE OR REPLACE FUNCTION s3p.maintain() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM s3p.expire_uploads();
  PERFORM s3p.reap_garbage();
END $$;
