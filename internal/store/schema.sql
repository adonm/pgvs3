-- Alpha layout v10 (store.LayoutVersion). Any layout change requires a fresh DB.
--
-- Each bucket lives in its own PostgreSQL schema `s3b_<bucket>` containing
-- `objects`, `chunks`, `uploads`, `upload_parts`, `object_part_checksums` and
-- `garbage`. Isolating buckets into dedicated schemas gives:
--   1. 32 TiB chunk-table capacity and independent autovacuum per bucket;
--   2. Instant OS disk-space reclamation on bucket drop (`DROP SCHEMA CASCADE`);
--   3. Single-bucket logical snapshots via `pg_dump -n s3b_<bucket>`;
--   4. Fast server-side bucket cloning (`s3p.clone_bucket`).
--
-- Object bytes are fixed-size INLINE rows: 8120-byte payloads stay inline
-- with toast_tuple_target = 8160 (heaptoast.c only externalises while
-- data_size > RelationGetToastTupleTarget - hoff; hoff = 24 with all-NOT-NULL
-- columns, so 8 + 4 + 4 + 8120 = 8136 <= 8136): one 8160-byte tuple per 8 KB
-- page, 99.6% fill, no TOAST, no toast-pointer indirection; slices are memcpy.
CREATE SCHEMA IF NOT EXISTS s3p;

CREATE TABLE IF NOT EXISTS s3p.layout (version int4 NOT NULL);

CREATE SEQUENCE IF NOT EXISTS s3p.file_ids CACHE 64;

CREATE TABLE IF NOT EXISTS s3p.buckets (
  name        text COLLATE "C" PRIMARY KEY,
  schema_name text COLLATE "C" NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bucket_name_shape CHECK (name ~ '^[a-z0-9._-]{1,58}$')
);

CREATE OR REPLACE FUNCTION s3p.bucket_schema(p_bucket text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF p_bucket IS NULL OR p_bucket !~ '^[a-z0-9._-]{1,58}$' THEN
    RAISE EXCEPTION 'invalid bucket name: %', coalesce(p_bucket, '<null>')
      USING ERRCODE = '22023';
  END IF;
  RETURN 's3b_' || p_bucket;
END $$;

-- Idempotently creates a bucket's schema, tables, indexes and static SQL
-- helpers. Once the schema exists, subsequent calls return in one catalog check.
CREATE OR REPLACE FUNCTION s3p.ensure_bucket(p_bucket text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_schema text := s3p.bucket_schema(p_bucket);
BEGIN
  IF to_regclass(format('%I.objects', v_schema)) IS NOT NULL AND
     EXISTS (SELECT 1 FROM s3p.buckets WHERE name = p_bucket) THEN
    RETURN v_schema;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.bucket', 0) # hashtextextended(p_bucket, 1));

  INSERT INTO s3p.buckets (name, schema_name)
  VALUES (p_bucket, v_schema)
  ON CONFLICT (name) DO NOTHING;

  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

  EXECUTE format($ddl$
    CREATE TABLE IF NOT EXISTS %1$I.objects (
      key           text COLLATE "C" PRIMARY KEY,
      file_id       bigint,
      size          int8 NOT NULL,
      sha256        bytea NOT NULL,
      etag          text NOT NULL,
      user_metadata text[] NOT NULL DEFAULT '{}'::text[],
      content_type  text NOT NULL DEFAULT 'application/octet-stream',
      created_at    timestamptz NOT NULL DEFAULT now(),
      parts         int8[],
      part_ends     int8[],
      inline_data   bytea STORAGE EXTERNAL,
      CONSTRAINT object_shape CHECK (
        octet_length(key) BETWEEN 1 AND 1024 AND size >= 0 AND octet_length(sha256) = 32 AND
        etag ~ '^[0-9a-f]{32}(-[1-9][0-9]{0,4})?$' AND
        cardinality(user_metadata) <= 128 AND cardinality(user_metadata) %% 2 = 0 AND
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

    CREATE INDEX IF NOT EXISTS objects_file_id_idx ON %1$I.objects (file_id)
      WHERE file_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS objects_parts_idx ON %1$I.objects USING gin (parts)
      WHERE parts IS NOT NULL;

    CREATE TABLE IF NOT EXISTS %1$I.chunks (
      file_id int8 NOT NULL,
      no      int4 NOT NULL,
      data    bytea STORAGE EXTERNAL NOT NULL,
      PRIMARY KEY (file_id, no),
      CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
    ) WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);

    CREATE TABLE IF NOT EXISTS %1$I.uploads (
      upload_id          text PRIMARY KEY,
      key                text COLLATE "C" NOT NULL,
      checksum_algorithm text CHECK (checksum_algorithm IN ('CRC32', 'CRC32C', 'CRC64NVME', 'SHA1', 'SHA256')),
      user_metadata      text[] NOT NULL DEFAULT '{}'::text[],
      content_type       text NOT NULL DEFAULT 'application/octet-stream',
      created_at         timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT upload_metadata_shape CHECK (
        cardinality(user_metadata) <= 128 AND cardinality(user_metadata) %% 2 = 0 AND
        octet_length(content_type) BETWEEN 1 AND 1024)
    );
    CREATE INDEX IF NOT EXISTS uploads_by_key ON %1$I.uploads (key, upload_id);
    CREATE INDEX IF NOT EXISTS uploads_by_age ON %1$I.uploads (created_at, upload_id);

    CREATE TABLE IF NOT EXISTS %1$I.upload_parts (
      upload_id text NOT NULL REFERENCES %1$I.uploads ON DELETE CASCADE,
      part_no   int4 NOT NULL,
      file_id   int8 NOT NULL,
      size      int8 NOT NULL,
      sha256    bytea NOT NULL,
      md5       bytea NOT NULL,
      checksum  text,
      PRIMARY KEY (upload_id, part_no),
      CONSTRAINT part_shape CHECK (part_no BETWEEN 1 AND 10000 AND size >= 0 AND
        size <= 2147483648::int8 * 8120 AND octet_length(sha256) = 32 AND
        octet_length(md5) = 16 AND (checksum IS NULL OR octet_length(checksum) BETWEEN 1 AND 128))
    );
    CREATE INDEX IF NOT EXISTS upload_parts_file_id_idx ON %1$I.upload_parts (file_id);

    CREATE TABLE IF NOT EXISTS %1$I.object_part_checksums (
      key       text COLLATE "C" NOT NULL,
      part_no   int4 NOT NULL,
      file_id   int8 NOT NULL,
      algorithm text NOT NULL,
      value     text NOT NULL,
      PRIMARY KEY (key, part_no),
      FOREIGN KEY (key) REFERENCES %1$I.objects (key)
        ON UPDATE CASCADE ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS %1$I.garbage (
      file_id   int8 PRIMARY KEY,
      queued_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS garbage_by_age ON %1$I.garbage (queued_at, file_id);
  $ddl$, v_schema);

  EXECUTE format($fn$
    CREATE OR REPLACE FUNCTION %1$I.commit_part(
      p_upload text, p_no int4, p_file_id int8, p_size int8, p_sha bytea,
      p_md5 bytea, p_checksum text DEFAULT NULL
    ) RETURNS void LANGUAGE plpgsql AS $body$
    DECLARE old bigint;
    BEGIN
      DELETE FROM %1$I.upload_parts WHERE upload_id = p_upload AND part_no = p_no
        RETURNING file_id INTO old;
      IF old IS NOT NULL THEN
        INSERT INTO %1$I.garbage (file_id)
        SELECT old
        WHERE NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.file_id IS NOT NULL AND o.file_id = old)
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[old])
          AND NOT EXISTS (SELECT 1 FROM %1$I.upload_parts p WHERE p.file_id = old)
        ON CONFLICT DO NOTHING;
      END IF;
      INSERT INTO %1$I.upload_parts (upload_id, part_no, file_id, size, sha256, md5, checksum)
      VALUES (p_upload, p_no, p_file_id, p_size, p_sha, p_md5, p_checksum);
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.publish_object(
      p_key text, p_file_id int8, p_size int8, p_sha256 bytea,
      p_etag text, p_meta text[], p_ct text, p_cond text DEFAULT 'none',
      p_if_match text DEFAULT NULL
    ) RETURNS text LANGUAGE plpgsql AS $body$
    DECLARE
      old_file bigint;
      old_parts int8[];
      old_etag text;
      found_old boolean;
    BEGIN
      PERFORM pg_advisory_xact_lock(%2$L::int8 # hashtextextended(p_key, 1));
      SELECT file_id, parts, etag INTO old_file, old_parts, old_etag
        FROM %1$I.objects WHERE key = p_key FOR UPDATE;
      found_old := FOUND;
      IF p_cond = 'if_absent' AND found_old THEN
        RETURN 'precondition';
      END IF;
      IF p_cond = 'if_match' AND (NOT found_old OR old_etag <> p_if_match) THEN
        RETURN 'precondition';
      END IF;
      DELETE FROM %1$I.object_part_checksums WHERE key = p_key;
      INSERT INTO %1$I.objects
        (key, file_id, size, sha256, etag, user_metadata, content_type,
         parts, part_ends, inline_data)
      VALUES
        (p_key, p_file_id, p_size, p_sha256, p_etag, p_meta, p_ct,
         NULL, NULL, NULL)
      ON CONFLICT (key) DO UPDATE SET
        file_id = EXCLUDED.file_id, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
        etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
        content_type = EXCLUDED.content_type, parts = NULL, part_ends = NULL,
        inline_data = NULL, created_at = now();
      IF found_old AND old_file IS NOT NULL THEN
        INSERT INTO %1$I.garbage (file_id)
        SELECT f FROM unnest(COALESCE(old_parts, ARRAY[]::int8[]) || old_file) AS f
        WHERE f IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.file_id IS NOT NULL AND o.file_id = f)
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[f])
          AND NOT EXISTS (SELECT 1 FROM %1$I.upload_parts p WHERE p.file_id = f)
        ON CONFLICT DO NOTHING;
      END IF;
      RETURN 'ok';
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.put_inline(
      p_key text, p_data bytea, p_sha256 bytea, p_etag text,
      p_meta text[], p_ct text, p_cond text DEFAULT 'none', p_if_match text DEFAULT NULL,
      p_sync text DEFAULT 'on'
    ) RETURNS text LANGUAGE plpgsql AS $body$
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
      PERFORM pg_advisory_xact_lock(%2$L::int8 # hashtextextended(p_key, 1));
      SELECT file_id, parts, etag INTO old_file, old_parts, old_etag
        FROM %1$I.objects WHERE key = p_key FOR UPDATE;
      found_old := FOUND;
      IF p_cond = 'if_absent' AND found_old THEN
        RETURN 'precondition';
      END IF;
      IF p_cond = 'if_match' AND (NOT found_old OR old_etag <> p_if_match) THEN
        RETURN 'precondition';
      END IF;
      DELETE FROM %1$I.object_part_checksums WHERE key = p_key;
      INSERT INTO %1$I.objects
        (key, file_id, size, sha256, etag, user_metadata, content_type,
         parts, part_ends, inline_data)
      VALUES
        (p_key, NULL, octet_length(p_data), p_sha256, p_etag, p_meta, p_ct,
         NULL, NULL, p_data)
      ON CONFLICT (key) DO UPDATE SET
        file_id = NULL, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
        etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
        content_type = EXCLUDED.content_type, parts = NULL, part_ends = NULL,
        inline_data = EXCLUDED.inline_data, created_at = now();
      IF found_old AND old_file IS NOT NULL THEN
        INSERT INTO %1$I.garbage (file_id)
        SELECT f FROM unnest(COALESCE(old_parts, ARRAY[]::int8[]) || old_file) AS f
        WHERE f IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.file_id IS NOT NULL AND o.file_id = f)
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[f])
          AND NOT EXISTS (SELECT 1 FROM %1$I.upload_parts p WHERE p.file_id = f)
        ON CONFLICT DO NOTHING;
      END IF;
      RETURN 'ok';
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.expire_uploads(
      p_grace interval DEFAULT interval '24 hours',
      p_max_uploads int DEFAULT 64
    ) RETURNS int LANGUAGE plpgsql AS $body$
    DECLARE
      victim_upload text;
      removed int := 0;
    BEGIN
      FOR attempt IN 1..p_max_uploads LOOP
        SELECT upload_id INTO victim_upload FROM %1$I.uploads
          WHERE created_at < now() - p_grace
          ORDER BY created_at, upload_id LIMIT 1 FOR UPDATE SKIP LOCKED;
        EXIT WHEN victim_upload IS NULL;
        INSERT INTO %1$I.garbage (file_id)
          SELECT file_id FROM %1$I.upload_parts WHERE upload_id = victim_upload
          ON CONFLICT DO NOTHING;
        DELETE FROM %1$I.uploads WHERE upload_id = victim_upload;
        removed := removed + 1;
      END LOOP;
      RETURN removed;
    END $body$;
  $fn$, v_schema, hashtextextended(p_bucket, 0));

  RETURN v_schema;
END $$;

-- Drops a bucket schema (`DROP SCHEMA CASCADE`). When p_force is false,
-- refuses to drop a bucket that still has objects or in-progress uploads.
CREATE OR REPLACE FUNCTION s3p.drop_bucket(p_bucket text, p_force boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_schema text := s3p.bucket_schema(p_bucket);
  v_occupied boolean := false;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.bucket', 0) # hashtextextended(p_bucket, 1));
  IF NOT EXISTS (SELECT 1 FROM s3p.buckets WHERE name = p_bucket FOR UPDATE) AND
     to_regnamespace(format('%I', v_schema)) IS NULL THEN
    RETURN 'not_found';
  END IF;
  IF NOT p_force AND to_regclass(format('%I.objects', v_schema)) IS NOT NULL THEN
    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %1$I.objects) OR EXISTS (SELECT 1 FROM %1$I.uploads)',
      v_schema
    ) INTO v_occupied;
    IF v_occupied THEN
      RETURN 'not_empty';
    END IF;
  END IF;
  EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', v_schema);
  DELETE FROM s3p.buckets WHERE name = p_bucket;
  RETURN 'deleted';
END $$;

-- Clones all committed objects and chunks from p_src into p_dst inside
-- PostgreSQL without moving bytes over the network.
CREATE OR REPLACE FUNCTION s3p.clone_bucket(p_src text, p_dst text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_src text := s3p.bucket_schema(p_src);
  v_dst text := s3p.bucket_schema(p_dst);
  v_first text := LEAST(p_src, p_dst);
  v_second text := GREATEST(p_src, p_dst);
  v_occupied boolean := false;
  v_max_id bigint;
BEGIN
  IF p_src = p_dst THEN
    RETURN 'not_empty';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.bucket', 0) # hashtextextended(v_first, 1));
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.bucket', 0) # hashtextextended(v_second, 1));

  IF NOT EXISTS (SELECT 1 FROM s3p.buckets WHERE name = p_src) OR
     to_regclass(format('%I.objects', v_src)) IS NULL THEN
    RETURN 'not_found';
  END IF;

  PERFORM s3p.ensure_bucket(p_dst);
  EXECUTE format(
    'SELECT EXISTS (SELECT 1 FROM %1$I.objects) OR EXISTS (SELECT 1 FROM %1$I.uploads)',
    v_dst
  ) INTO v_occupied;
  IF v_occupied THEN
    RETURN 'not_empty';
  END IF;

  EXECUTE format(
    'INSERT INTO %1$I.chunks (file_id, no, data) SELECT file_id, no, data FROM %2$I.chunks',
    v_dst, v_src
  );
  EXECUTE format($sql$
    INSERT INTO %1$I.objects
      (key, file_id, size, sha256, etag, user_metadata, content_type,
       created_at, parts, part_ends, inline_data)
    SELECT key, file_id, size, sha256, etag, user_metadata, content_type,
           created_at, parts, part_ends, inline_data
    FROM %2$I.objects
  $sql$, v_dst, v_src);
  EXECUTE format($sql$
    INSERT INTO %1$I.object_part_checksums (key, part_no, file_id, algorithm, value)
    SELECT key, part_no, file_id, algorithm, value FROM %2$I.object_part_checksums
  $sql$, v_dst, v_src);
  EXECUTE format('SELECT max(file_id) FROM %1$I.chunks', v_dst) INTO v_max_id;
  IF v_max_id IS NOT NULL THEN
    PERFORM setval('s3p.file_ids', GREATEST((SELECT last_value FROM s3p.file_ids), v_max_id), true);
  END IF;
  RETURN 'ok';
END $$;

-- Reconciles `s3b_*` schemas restored via `pg_restore` (or dropped manually)
-- with `s3p.buckets` and ensures `s3p.file_ids` is ahead of all stored chunks.
CREATE OR REPLACE FUNCTION s3p.reconcile_buckets()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r record;
  v_bucket text;
  v_max bigint := 0;
  v_cur bigint;
BEGIN
  DELETE FROM s3p.buckets b
  WHERE to_regnamespace(format('%I', b.schema_name)) IS NULL;

  FOR r IN
    SELECT nspname FROM pg_namespace
    WHERE nspname ~ '^s3b_[a-z0-9._-]{1,58}$'
  LOOP
    IF to_regclass(format('%I.objects', r.nspname)) IS NULL OR
       to_regclass(format('%I.chunks', r.nspname)) IS NULL OR
       to_regclass(format('%I.garbage_by_age', r.nspname)) IS NULL THEN
      CONTINUE;
    END IF;
    v_bucket := substr(r.nspname, 5);
    INSERT INTO s3p.buckets (name, schema_name)
    VALUES (v_bucket, r.nspname)
    ON CONFLICT (name) DO NOTHING;
    EXECUTE format('SELECT COALESCE(max(file_id), 0) FROM %I.chunks', r.nspname) INTO v_cur;
    IF v_cur > v_max THEN
      v_max := v_cur;
    END IF;
  END LOOP;

  IF v_max > 0 THEN
    PERFORM setval('s3p.file_ids', GREATEST((SELECT last_value FROM s3p.file_ids), v_max), true);
  END IF;
END $$;
