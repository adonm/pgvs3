-- Alpha layout v8 (db::LAYOUT_VERSION). Any layout change requires a fresh DB.
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

-- bucket/key collate "C": S3 lists keys in byte order, so the primary key
-- serves both point lookups and ordered LIST range scans.
CREATE TABLE IF NOT EXISTS s3p.objects (
  bucket     text COLLATE "C" NOT NULL,
  key        text COLLATE "C" NOT NULL,
  file_id    bigserial        NOT NULL UNIQUE,  -- the single file, or the first part
  size       int8             NOT NULL,
  sha256     bytea            NOT NULL,         -- integrity digest; multipart: of the part sha256s
  etag       text             NOT NULL,         -- S3 ETag: MD5, or MD5 of part MD5s + "-count"
  user_metadata text[]       NOT NULL DEFAULT '{}'::text[],
  content_type text          NOT NULL DEFAULT 'application/octet-stream',
  created_at timestamptz      NOT NULL DEFAULT now(),
  parts      int8[],                            -- multipart: part file_ids in order
  part_ends  int8[],                            -- multipart: cumulative end offsets
  PRIMARY KEY (bucket, key),
  FOREIGN KEY (bucket) REFERENCES s3p.buckets (name),
  CONSTRAINT object_shape CHECK (
     octet_length(key) BETWEEN 1 AND 1024 AND size >= 0 AND octet_length(sha256) = 32 AND
     etag ~ '^[0-9a-f]{32}(-[1-9][0-9]{0,4})?$' AND
     cardinality(user_metadata) <= 128 AND cardinality(user_metadata) % 2 = 0 AND
     octet_length(content_type) BETWEEN 1 AND 1024 AND
    ((parts IS NULL AND part_ends IS NULL AND size <= 2147483648::int8 * 8120) OR
     (parts IS NOT NULL AND part_ends IS NOT NULL AND cardinality(parts) > 0 AND
      array_ndims(parts) = 1 AND array_ndims(part_ends) = 1 AND
      array_lower(parts, 1) = 1 AND array_lower(part_ends, 1) = 1 AND
      cardinality(parts) = cardinality(part_ends) AND parts[1] = file_id AND
      part_ends[cardinality(part_ends)] = size AND
      array_position(parts, NULL) IS NULL AND array_position(part_ends, NULL) IS NULL))
  )
) WITH (autovacuum_vacuum_scale_factor = 0.02);
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
  file_id   int8  NOT NULL UNIQUE,
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
CREATE TABLE IF NOT EXISTS s3p.garbage (
  file_id   int8 PRIMARY KEY,
  queued_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS garbage_by_age ON s3p.garbage (queued_at, file_id);

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
      RAISE EXCEPTION 'garbage file % is still referenced', victim_file;
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
