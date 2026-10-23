-- Alpha layout v11 (store.LayoutVersion). Any layout change requires a fresh DB.
--
-- One PostgreSQL schema per namespace, prefixed by kind:
--
--   s3b_<name>  durable S3 bucket   (LOGGED, 64 KiB inline objects)
--   s3c_<name>  volatile cache DB   (UNLOGGED, one 8 KB page per value)
--   s3q_<name>  SQS queue           (LOGGED, one 8 KB page per message body)
--
-- Namespaces are isolated so each one owns its tables, autovacuum workers and
-- 32 TiB chunk-relation budget, can be snapshotted with `pg_dump -n`, dropped
-- with `DROP SCHEMA CASCADE`, and cloned inside PostgreSQL.
--
-- Object bytes are fixed-size INLINE rows: 8120-byte payloads stay inline
-- with toast_tuple_target = 8160 (heaptoast.c only externalises while
-- data_size > RelationGetToastTupleTarget - hoff; hoff = 24 with all-NOT-NULL
-- columns, so 8 + 4 + 4 + 8120 = 8136 <= 8136): one 8160-byte tuple per 8 KB
-- page, 99.6% fill, no TOAST, no toast-pointer indirection; slices are memcpy.
CREATE SCHEMA IF NOT EXISTS s3p;

CREATE TABLE IF NOT EXISTS s3p.layout (version int4 NOT NULL);

CREATE SEQUENCE IF NOT EXISTS s3p.file_ids CACHE 64;

CREATE TABLE IF NOT EXISTS s3p.namespaces (
  kind        text NOT NULL CHECK (kind IN ('bucket', 'cache', 'queue')),
  name        text COLLATE "C" NOT NULL,
  schema_name text COLLATE "C" NOT NULL UNIQUE,
  quota_bytes int8,
  attrs       jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (kind, name),
  CONSTRAINT namespace_name_shape CHECK (name ~ '^[a-z0-9._-]{1,58}$')
);

CREATE OR REPLACE FUNCTION s3p.namespace_schema(p_kind text, p_name text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF p_kind NOT IN ('bucket', 'cache', 'queue') THEN
    RAISE EXCEPTION 'unknown namespace kind: %', p_kind USING ERRCODE = '22023';
  END IF;
  IF p_name IS NULL OR p_name !~ '^[a-z0-9._-]{1,58}$' THEN
    RAISE EXCEPTION 'invalid namespace name: %', coalesce(p_name, '<null>')
      USING ERRCODE = '22023';
  END IF;
  RETURN CASE p_kind
    WHEN 'cache' THEN 's3c_' || p_name
    WHEN 'queue' THEN 's3q_' || p_name
    ELSE 's3b_' || p_name
  END;
END $$;

-- Idempotently creates a namespace's schema, tables, indexes and SQL helpers.
CREATE OR REPLACE FUNCTION s3p.ensure_namespace(
  p_kind text, p_name text, p_quota int8 DEFAULT NULL, p_attrs jsonb DEFAULT NULL
) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_persist text := CASE WHEN p_kind = 'cache' THEN 'UNLOGGED' ELSE '' END;
  v_inline int := CASE WHEN p_kind = 'bucket' THEN 65536 ELSE 8120 END;
BEGIN
  IF to_regclass(format('%I.objects', v_schema)) IS NOT NULL
     OR to_regclass(format('%I.messages', v_schema)) IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
      RETURN v_schema;
    END IF;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));

  INSERT INTO s3p.namespaces (kind, name, schema_name, quota_bytes, attrs)
  VALUES (p_kind, p_name, v_schema, p_quota, coalesce(p_attrs, '{}'::jsonb))
  ON CONFLICT (kind, name) DO UPDATE
    SET quota_bytes = coalesce(EXCLUDED.quota_bytes, s3p.namespaces.quota_bytes),
        attrs = s3p.namespaces.attrs || EXCLUDED.attrs;

  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

  IF p_kind = 'queue' THEN
    EXECUTE format($ddl$
      CREATE TABLE IF NOT EXISTS %1$I.messages (
        id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        msg_id             text NOT NULL UNIQUE,
        body               bytea NOT NULL,
        body_size          int4 NOT NULL CHECK (body_size >= 0 AND octet_length(body) <= body_size),
        attrs              bytea,
        visible_at         timestamptz NOT NULL DEFAULT now(),
        receipt            text,
        receive_count      int4 NOT NULL DEFAULT 0,
        first_received_at  timestamptz,
        sent_at            timestamptz NOT NULL DEFAULT now(),
        expires_at         timestamptz NOT NULL
      ) WITH (fillfactor = 80);
      CREATE INDEX IF NOT EXISTS messages_ready ON %1$I.messages (visible_at, id);
      CREATE INDEX IF NOT EXISTS messages_expiry ON %1$I.messages (expires_at);
      CREATE INDEX IF NOT EXISTS messages_receipt ON %1$I.messages (receipt)
        WHERE receipt IS NOT NULL;
    $ddl$, v_schema);
    RETURN v_schema;
  END IF;

  EXECUTE format($ddl$
    CREATE %2$s TABLE IF NOT EXISTS %1$I.objects (
      key           text COLLATE "C" PRIMARY KEY,
      file_id       bigint,
      size          int8 NOT NULL,
      sha256        bytea NOT NULL,
      etag          text NOT NULL,
      user_metadata text[] NOT NULL DEFAULT '{}'::text[],
      content_type  text NOT NULL DEFAULT 'application/octet-stream',
      created_at    timestamptz NOT NULL DEFAULT now(),
      expires_at    timestamptz,
      origin_etag   text,
      parts         int8[],
      part_ends     int8[],
      inline_data   bytea STORAGE EXTERNAL,
      CONSTRAINT object_shape CHECK (
        octet_length(key) BETWEEN 1 AND 1024 AND size >= 0 AND octet_length(sha256) = 32 AND
        etag ~ '^[0-9a-f]{32}(-[1-9][0-9]{0,4})?$' AND
        cardinality(user_metadata) <= 128 AND cardinality(user_metadata) %% 2 = 0 AND
        octet_length(content_type) BETWEEN 1 AND 1024 AND
        ((inline_data IS NOT NULL AND file_id IS NULL AND parts IS NULL AND part_ends IS NULL AND
          size <= %3$s AND octet_length(inline_data) <= size AND
          (size = 0 OR octet_length(inline_data) > 0))
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
    ) WITH (fillfactor = 85, toast_tuple_target = 8160,
            autovacuum_vacuum_scale_factor = 0.02);

    CREATE INDEX IF NOT EXISTS objects_file_id_idx ON %1$I.objects (file_id)
      WHERE file_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS objects_parts_idx ON %1$I.objects USING gin (parts)
      WHERE parts IS NOT NULL;
    CREATE INDEX IF NOT EXISTS objects_expires_idx ON %1$I.objects (expires_at)
      WHERE expires_at IS NOT NULL;

    CREATE %2$s TABLE IF NOT EXISTS %1$I.chunks (
      file_id int8 NOT NULL,
      no      int4 NOT NULL,
      data    bytea STORAGE EXTERNAL NOT NULL,
      PRIMARY KEY (file_id, no),
      CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
    ) WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);

    CREATE %2$s TABLE IF NOT EXISTS %1$I.garbage (
      file_id   int8 PRIMARY KEY,
      queued_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS garbage_by_age ON %1$I.garbage (queued_at, file_id);

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

    CREATE %2$s TABLE IF NOT EXISTS %1$I.object_part_checksums (
      key       text COLLATE "C" NOT NULL,
      part_no   int4 NOT NULL,
      file_id   int8 NOT NULL,
      algorithm text NOT NULL,
      value     text NOT NULL,
      PRIMARY KEY (key, part_no),
      FOREIGN KEY (key) REFERENCES %1$I.objects (key)
        ON UPDATE CASCADE ON DELETE CASCADE
    );
  $ddl$, v_schema, v_persist, v_inline);

  IF p_kind = 'bucket' THEN
    EXECUTE format($ddl$    $ddl$, v_schema);
  END IF;

  EXECUTE format($fn$
    CREATE OR REPLACE FUNCTION %1$I.publish_object(
      p_key text, p_file_id int8, p_size int8, p_sha256 bytea,
      p_etag text, p_meta text[], p_ct text, p_cond text DEFAULT 'none',
      p_if_match text DEFAULT NULL, p_expires_at timestamptz DEFAULT NULL,
      p_keep_ttl boolean DEFAULT false, p_origin_etag text DEFAULT NULL
    ) RETURNS text LANGUAGE plpgsql AS $body$
    DECLARE
      old_file bigint;
      old_parts int8[];
      old_etag text;
      old_expires timestamptz;
      found_old boolean;
      live_old boolean := false;
      new_expires timestamptz := p_expires_at;
    BEGIN
      PERFORM pg_advisory_xact_lock(%2$L::int8 # hashtextextended(p_key, 1));
      SELECT file_id, parts, etag, expires_at
        INTO old_file, old_parts, old_etag, old_expires
        FROM %1$I.objects WHERE key = p_key FOR UPDATE;
      found_old := FOUND;
      IF found_old AND (old_expires IS NULL OR old_expires > now()) THEN
        live_old := true;
      END IF;
      IF p_cond = 'if_absent' AND live_old THEN RETURN 'precondition'; END IF;
      IF p_cond = 'if_present' AND NOT live_old THEN RETURN 'precondition'; END IF;
      IF p_cond = 'if_match' AND (NOT live_old OR old_etag <> p_if_match) THEN
        RETURN 'precondition';
      END IF;
      IF p_keep_ttl AND live_old THEN new_expires := old_expires; END IF;
      DELETE FROM %1$I.object_part_checksums WHERE key = p_key;
      INSERT INTO %1$I.objects
        (key, file_id, size, sha256, etag, user_metadata, content_type,
         expires_at, origin_etag, parts, part_ends, inline_data)
      VALUES
        (p_key, p_file_id, p_size, p_sha256, p_etag, p_meta, p_ct,
         new_expires, p_origin_etag, NULL, NULL, NULL)
      ON CONFLICT (key) DO UPDATE SET
        file_id = EXCLUDED.file_id, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
        etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
        content_type = EXCLUDED.content_type, expires_at = EXCLUDED.expires_at,
        origin_etag = EXCLUDED.origin_etag,
        parts = NULL, part_ends = NULL, inline_data = NULL, created_at = now();
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
      p_key text, p_data bytea, p_size int8, p_sha256 bytea, p_etag text,
      p_meta text[], p_ct text, p_cond text DEFAULT 'none', p_if_match text DEFAULT NULL,
      p_sync text DEFAULT 'on', p_expires_at timestamptz DEFAULT NULL,
      p_keep_ttl boolean DEFAULT false, p_origin_etag text DEFAULT NULL
    ) RETURNS text LANGUAGE plpgsql AS $body$
    DECLARE
      old_file bigint;
      old_parts int8[];
      old_etag text;
      old_expires timestamptz;
      found_old boolean;
      live_old boolean := false;
      new_expires timestamptz := p_expires_at;
    BEGIN
      IF p_cond = 'none' AND p_sync IN ('local','off','remote_write','remote_apply') THEN
        PERFORM set_config('synchronous_commit', p_sync, true);
      ELSE
        PERFORM set_config('synchronous_commit', 'on', true);
      END IF;
      IF p_size > %3$s OR octet_length(p_data) > p_size THEN
        RAISE EXCEPTION 'inline object exceeds the %3$s-byte limit';
      END IF;
      PERFORM pg_advisory_xact_lock(%2$L::int8 # hashtextextended(p_key, 1));
      SELECT file_id, parts, etag, expires_at
        INTO old_file, old_parts, old_etag, old_expires
        FROM %1$I.objects WHERE key = p_key FOR UPDATE;
      found_old := FOUND;
      IF found_old AND (old_expires IS NULL OR old_expires > now()) THEN
        live_old := true;
      END IF;
      IF p_cond = 'if_absent' AND live_old THEN RETURN 'precondition'; END IF;
      IF p_cond = 'if_present' AND NOT live_old THEN RETURN 'precondition'; END IF;
      IF p_cond = 'if_match' AND (NOT live_old OR old_etag <> p_if_match) THEN
        RETURN 'precondition';
      END IF;
      IF p_keep_ttl AND live_old THEN new_expires := old_expires; END IF;
      DELETE FROM %1$I.object_part_checksums WHERE key = p_key;
      INSERT INTO %1$I.objects
        (key, file_id, size, sha256, etag, user_metadata, content_type,
         expires_at, origin_etag, parts, part_ends, inline_data)
      VALUES
        (p_key, NULL, p_size, p_sha256, p_etag, p_meta, p_ct,
         new_expires, p_origin_etag, NULL, NULL, p_data)
      ON CONFLICT (key) DO UPDATE SET
        file_id = NULL, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
        etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
        content_type = EXCLUDED.content_type, expires_at = EXCLUDED.expires_at,
        origin_etag = EXCLUDED.origin_etag,
        parts = NULL, part_ends = NULL, inline_data = EXCLUDED.inline_data,
        created_at = now();
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

    -- Expire keys in bounded batches. Returns the number removed so callers can
    -- chain passes until the backlog is drained.
    CREATE OR REPLACE FUNCTION %1$I.expire_objects(p_max int DEFAULT 4096)
    RETURNS int LANGUAGE plpgsql AS $body$
    DECLARE removed int := 0;
    BEGIN
      WITH victims AS (
        SELECT key FROM %1$I.objects
        WHERE expires_at IS NOT NULL AND expires_at <= now()
        ORDER BY expires_at LIMIT p_max
        FOR UPDATE SKIP LOCKED
      ), doomed AS (
        DELETE FROM %1$I.objects o USING victims v WHERE o.key = v.key
        RETURNING o.key, o.file_id, o.parts
      ), dead AS (
        SELECT file_id FROM doomed WHERE file_id IS NOT NULL
        UNION ALL
        SELECT unnest(parts) FROM doomed WHERE parts IS NOT NULL
      ), queued AS (
        INSERT INTO %1$I.garbage (file_id)
        SELECT DISTINCT d.file_id FROM dead d
        WHERE d.file_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.file_id IS NOT NULL AND o.file_id = d.file_id)
          AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[d.file_id])
          AND NOT EXISTS (SELECT 1 FROM %1$I.upload_parts p WHERE p.file_id = d.file_id)
        ON CONFLICT DO NOTHING
      )
      SELECT count(*)::int INTO removed FROM doomed;
      RETURN removed;
    END $body$;
  $fn$, v_schema, hashtextextended(v_schema, 0), v_inline);

  IF p_kind = 'bucket' THEN
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

      CREATE OR REPLACE FUNCTION %1$I.expire_uploads(
        p_grace interval DEFAULT interval '24 hours',
        p_max_uploads int DEFAULT 64
      ) RETURNS int LANGUAGE plpgsql AS $body$
      DECLARE victim_upload text; removed int := 0;
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
    $fn$, v_schema);
  END IF;

  RETURN v_schema;
END $$;

-- Drops a namespace schema (`DROP SCHEMA CASCADE`). When p_force is false,
-- refuses to drop a namespace that still holds data.
CREATE OR REPLACE FUNCTION s3p.drop_namespace(p_kind text, p_name text, p_force boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_table text := CASE WHEN p_kind = 'queue' THEN 'messages' ELSE 'objects' END;
  v_occupied boolean := false;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));
  IF NOT EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name FOR UPDATE) AND
     to_regnamespace(format('%I', v_schema)) IS NULL THEN
    RETURN 'not_found';
  END IF;
  IF NOT p_force AND to_regclass(format('%I.%I', v_schema, v_table)) IS NOT NULL THEN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I)', v_schema, v_table) INTO v_occupied;
    IF v_occupied THEN RETURN 'not_empty'; END IF;
  END IF;
  EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', v_schema);
  DELETE FROM s3p.namespaces WHERE kind = p_kind AND name = p_name;
  RETURN 'deleted';
END $$;

-- Clones a bucket or cache namespace inside PostgreSQL without moving bytes
-- through the gateway.
CREATE OR REPLACE FUNCTION s3p.clone_namespace(p_kind text, p_src text, p_dst text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_src text := s3p.namespace_schema(p_kind, p_src);
  v_dst text := s3p.namespace_schema(p_kind, p_dst);
  v_first text := LEAST(p_src, p_dst);
  v_second text := GREATEST(p_src, p_dst);
  v_occupied boolean := false;
  v_max_id bigint;
BEGIN
  IF p_src = p_dst THEN RETURN 'not_empty'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || v_first, 1));
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || v_second, 1));
  IF NOT EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_src) OR
     to_regclass(format('%I.objects', v_src)) IS NULL THEN
    RETURN 'not_found';
  END IF;
  PERFORM s3p.ensure_namespace(p_kind, p_dst, NULL, NULL);
  EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.objects)', v_dst) INTO v_occupied;
  IF v_occupied THEN RETURN 'not_empty'; END IF;
  EXECUTE format('INSERT INTO %1$I.chunks (file_id, no, data) SELECT file_id, no, data FROM %2$I.chunks', v_dst, v_src);
  EXECUTE format($sql$
    INSERT INTO %1$I.objects
      (key, file_id, size, sha256, etag, user_metadata, content_type,
       created_at, expires_at, origin_etag, parts, part_ends, inline_data)
    SELECT key, file_id, size, sha256, etag, user_metadata, content_type,
           created_at, expires_at, origin_etag, parts, part_ends, inline_data
    FROM %2$I.objects
  $sql$, v_dst, v_src);
  EXECUTE format('SELECT max(file_id) FROM %1$I.chunks', v_dst) INTO v_max_id;
  IF v_max_id IS NOT NULL THEN
    PERFORM setval('s3p.file_ids', GREATEST((SELECT last_value FROM s3p.file_ids), v_max_id), true);
  END IF;
  RETURN 'ok';
END $$;

-- Registers one on-disk namespace schema in the catalog. Explicit only:
-- a restore or an operator-created schema never becomes a live bucket by
-- accident. Returns 'adopted', 'exists' or 'missing'.
CREATE OR REPLACE FUNCTION s3p.adopt_namespace(p_kind text, p_name text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_table text := CASE WHEN p_kind = 'queue' THEN 'messages' ELSE 'objects' END;
  v_max bigint;
BEGIN
  IF to_regclass(format('%I.%I', v_schema, v_table)) IS NULL THEN
    RETURN 'missing';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));
  IF EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
    RETURN 'exists';
  END IF;
  INSERT INTO s3p.namespaces (kind, name, schema_name) VALUES (p_kind, p_name, v_schema);
  IF p_kind <> 'queue' THEN
    EXECUTE format('SELECT COALESCE(max(file_id), 0) FROM %I.chunks', v_schema) INTO v_max;
    IF v_max > 0 THEN
      PERFORM setval('s3p.file_ids',
        GREATEST((SELECT last_value FROM s3p.file_ids), v_max), true);
    END IF;
  END IF;
  RETURN 'adopted';
END $$;

-- Adopts every matching schema (optionally one kind) and prunes catalog rows
-- whose schema has disappeared. Returns the number newly adopted.
CREATE OR REPLACE FUNCTION s3p.adopt_namespaces(p_kind text DEFAULT NULL)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  r record;
  v_kind text;
  v_name text;
  v_prefix text;
  adopted int := 0;
BEGIN
  v_prefix := CASE p_kind
    WHEN 'cache' THEN 's3c_%'
    WHEN 'queue' THEN 's3q_%'
    WHEN 'bucket' THEN 's3b_%'
    ELSE 's3_%' END;
  FOR r IN
    SELECT nspname FROM pg_namespace
    WHERE nspname ~ '^s3[bcq]_[a-z0-9._-]{1,58}$'
      AND nspname LIKE v_prefix
    ORDER BY nspname
  LOOP
    v_kind := CASE substr(r.nspname, 1, 3)
      WHEN 's3c' THEN 'cache'
      WHEN 's3q' THEN 'queue'
      ELSE 'bucket' END;
    v_name := substr(r.nspname, 5);
    IF s3p.adopt_namespace(v_kind, v_name) = 'adopted' THEN
      adopted := adopted + 1;
    END IF;
  END LOOP;
  DELETE FROM s3p.namespaces n
  WHERE to_regnamespace(format('%I', n.schema_name)) IS NULL
    AND (p_kind IS NULL OR n.kind = p_kind);
  RETURN adopted;
END $$;

-- In-place migration from layout v10 (per-bucket schemas catalogued in
-- s3p.buckets) to v11 (kind-prefixed namespaces catalogued in s3p.namespaces).
-- Bucket tables and every byte stay in place; only the catalog, per-schema
-- helper functions and one new column change. Legacy objects stay readable
-- through the renamed v10_buckets table.
CREATE OR REPLACE FUNCTION s3p.migrate_v10()
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  r record;
  n int := 0;
BEGIN
  IF to_regclass('s3p.buckets') IS NULL THEN
    RAISE EXCEPTION 'layout v10 catalog (s3p.buckets) not found';
  END IF;

  FOR r IN SELECT name, schema_name, created_at FROM s3p.buckets ORDER BY name LOOP
    CONTINUE WHEN to_regclass(format('%I.objects', r.schema_name)) IS NULL;
    EXECUTE format(
      'ALTER TABLE %I.objects ADD COLUMN IF NOT EXISTS origin_etag text',
      r.schema_name);
    EXECUTE format(
      'ALTER TABLE %I.objects SET (fillfactor = 85, toast_tuple_target = 8160)',
      r.schema_name);
    EXECUTE format(
      'ALTER TABLE %I.chunks SET (toast_tuple_target = 8160, ' ||
      'autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.02, ' ||
      'autovacuum_vacuum_threshold = 1000)', r.schema_name);
    -- Re-run the namespace DDL so per-schema functions match this binary.
    PERFORM s3p.ensure_namespace('bucket', r.name, NULL, NULL);
    UPDATE s3p.namespaces SET created_at = r.created_at
     WHERE kind = 'bucket' AND name = r.name;
    n := n + 1;
  END LOOP;

  -- Bucket schemas restored without their catalog row become visible only
  -- after the operator asks; during migration they are part of the upgrade.
  FOR r IN
    SELECT nspname FROM pg_namespace
     WHERE nspname ~ '^s3b_[a-z0-9._-]{1,58}$'
       AND NOT EXISTS (SELECT 1 FROM s3p.buckets b WHERE b.schema_name = nspname)
     ORDER BY nspname
  LOOP
    EXECUTE format(
      'ALTER TABLE %I.objects ADD COLUMN IF NOT EXISTS origin_etag text',
      r.nspname);
    EXECUTE format(
      'ALTER TABLE %I.chunks SET (toast_tuple_target = 8160, ' ||
      'autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.02, ' ||
      'autovacuum_vacuum_threshold = 1000)', r.nspname);
    PERFORM s3p.ensure_namespace('bucket', substr(r.nspname, 5), NULL, NULL);
    n := n + 1;
  END LOOP;

  PERFORM s3p.adopt_namespaces('bucket');

  DROP FUNCTION IF EXISTS s3p.ensure_bucket(text);
  DROP FUNCTION IF EXISTS s3p.bucket_schema(text);
  DROP FUNCTION IF EXISTS s3p.clone_bucket(text, text);
  DROP FUNCTION IF EXISTS s3p.drop_bucket(text, boolean);
  DROP FUNCTION IF EXISTS s3p.reconcile_buckets();

  ALTER TABLE s3p.buckets RENAME TO v10_buckets;
  UPDATE s3p.layout SET version = 11;
  RETURN n;
END $$;
