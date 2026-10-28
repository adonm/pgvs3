--
-- pgvs3 schema baseline. Layout v25; apply only to an empty database.
--



SET LOCAL statement_timeout = 0;
SET LOCAL lock_timeout = 0;
SET LOCAL idle_in_transaction_session_timeout = 0;
SET LOCAL transaction_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', true);
SET LOCAL check_function_bodies = false;
SET LOCAL xmloption = content;
SET LOCAL client_min_messages = warning;
SET LOCAL row_security = off;

--
-- Name: s3p; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA s3p;


--
-- Name: adopt_namespace(text, text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.adopt_namespace(p_kind text, p_name text) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_schema  text := s3p.namespace_schema(p_kind, p_name);
  v_quota   int8;
  v_attrs   jsonb := '{}'::jsonb;
  v_created timestamptz := now();
  v_max     bigint;
BEGIN
  IF p_kind = 'queue' THEN
    IF to_regclass(format('%I.jobs', v_schema)) IS NULL AND
       to_regclass(format('%I.messages', v_schema)) IS NULL THEN
      RETURN 'missing';
    END IF;
  ELSIF to_regclass(format('%I.objects', v_schema)) IS NULL THEN
    RETURN 'missing';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns:cap', 0));
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));
  IF EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
    RETURN 'exists';
  END IF;
  IF (SELECT count(*) FROM s3p.namespaces) >= 100 THEN
    RETURN 'limit';
  END IF;
  IF to_regclass(format('%I.meta', v_schema)) IS NOT NULL THEN
    EXECUTE format('SELECT quota_bytes, coalesce(attrs, ''{}''::jsonb), coalesce(created_at, now()) FROM %I.meta LIMIT 1', v_schema)
      INTO v_quota, v_attrs, v_created;
  END IF;
  INSERT INTO s3p.namespaces (kind, name, schema_name, quota_bytes, attrs, created_at)
  VALUES (p_kind, p_name, v_schema, v_quota, coalesce(v_attrs, '{}'::jsonb), coalesce(v_created, now()));
  IF p_kind = 'queue' THEN
    PERFORM s3p.ensure_queue_schema(v_schema);
    EXECUTE format(
      'INSERT INTO s3p.job_index (id, queue, created_at)
       SELECT id, %L, created_at FROM %I.jobs
       ON CONFLICT (id) DO NOTHING', p_name, v_schema);
  ELSE
    PERFORM s3p.ensure_object_schema(v_schema, p_kind);
    EXECUTE format('SELECT COALESCE(max(file_id), 0) FROM %I.chunks', v_schema) INTO v_max;
    IF v_max > 0 THEN
      PERFORM setval('s3p.file_ids',
        GREATEST((SELECT last_value FROM s3p.file_ids), v_max), true);
    END IF;
  END IF;
  EXECUTE format($sql$
    INSERT INTO %1$I.meta (singleton, kind, name, quota_bytes, attrs, created_at)
    SELECT true, kind, name, quota_bytes, attrs, created_at
    FROM s3p.namespaces WHERE kind = $1 AND name = $2
    ON CONFLICT (singleton) DO UPDATE
      SET quota_bytes = EXCLUDED.quota_bytes,
          attrs = EXCLUDED.attrs
  $sql$, v_schema) USING p_kind, p_name;
  RETURN 'adopted';
END $_$;


--
-- Name: adopt_namespaces(text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.adopt_namespaces(p_kind text DEFAULT NULL::text) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
  r record;
  v_kind text;
  v_name text;
  v_prefix text;
  adopted int := 0;
BEGIN
  v_prefix := CASE p_kind
    WHEN 'cache' THEN 's3c_%'
    WHEN 'writeback' THEN 's3w_%'
    WHEN 'queue' THEN 's3q_%'
    WHEN 'bucket' THEN 's3b_%'
    ELSE 's3_%' END;
  FOR r IN
    SELECT p.nspname FROM pg_namespace p
    WHERE p.nspname ~ '^s3[bcqw]_[a-z0-9._-]{1,58}$'
      AND p.nspname LIKE v_prefix
      AND NOT EXISTS (SELECT 1 FROM s3p.namespaces n WHERE n.schema_name = p.nspname)
    ORDER BY p.nspname
  LOOP
    v_kind := CASE substr(r.nspname, 1, 3)
      WHEN 's3c' THEN 'cache'
      WHEN 's3w' THEN 'writeback'
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
END $_$;


--
-- Name: clone_namespace(text, text, text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.clone_namespace(p_kind text, p_src text, p_dst text) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_src text := s3p.namespace_schema(p_kind, p_src);
  v_dst text := s3p.namespace_schema(p_kind, p_dst);
  v_first text := LEAST(p_src, p_dst);
  v_second text := GREATEST(p_src, p_dst);
  v_occupied boolean := false;
  v_max_id bigint;
BEGIN
  IF p_src = p_dst THEN RETURN 'not_empty'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns:cap', 0));
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
       content_encoding, origin_modified, checksum_name, checksum_value, checksum_type,
       created_at, expires_at, origin_etag, parts, part_ends, inline_data)
    SELECT key, file_id, size, sha256, etag, user_metadata, content_type,
           content_encoding, origin_modified, checksum_name, checksum_value, checksum_type,
           created_at, expires_at, origin_etag, parts, part_ends, inline_data
    FROM %2$I.objects
  $sql$, v_dst, v_src);
  EXECUTE format($sql$
    INSERT INTO %1$I.object_part_checksums (key, part_no, file_id, algorithm, value)
    SELECT key, part_no, file_id, algorithm, value
    FROM %2$I.object_part_checksums
  $sql$, v_dst, v_src);
  EXECUTE format('SELECT max(file_id) FROM %1$I.chunks', v_dst) INTO v_max_id;
  IF v_max_id IS NOT NULL THEN
    PERFORM setval('s3p.file_ids', GREATEST((SELECT last_value FROM s3p.file_ids), v_max_id), true);
  END IF;
  RETURN 'ok';
END $_$;


--
-- Name: drop_namespace(text, text, boolean); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.drop_namespace(p_kind text, p_name text, p_force boolean DEFAULT false) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_table text := CASE WHEN p_kind = 'queue' THEN 'jobs' ELSE 'objects' END;
  v_legacy text := CASE WHEN p_kind = 'queue' THEN 'messages' ELSE NULL END;
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
  IF NOT p_force AND v_legacy IS NOT NULL AND
     to_regclass(format('%I.%I', v_schema, v_legacy)) IS NOT NULL THEN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I)', v_schema, v_legacy) INTO v_occupied;
    IF v_occupied THEN RETURN 'not_empty'; END IF;
  END IF;
  EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', v_schema);
  DELETE FROM s3p.namespaces WHERE kind = p_kind AND name = p_name;
  RETURN 'deleted';
END $$;


--
-- Name: ensure_namespace(text, text, bigint, jsonb); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.ensure_namespace(p_kind text, p_name text, p_quota bigint DEFAULT NULL::bigint, p_attrs jsonb DEFAULT NULL::jsonb) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_count  bigint;
BEGIN
  IF to_regclass(format('%I.objects', v_schema)) IS NOT NULL
     OR to_regclass(format('%I.messages', v_schema)) IS NOT NULL
     OR to_regclass(format('%I.jobs', v_schema)) IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
      RETURN v_schema;
    END IF;
  END IF;

  -- Namespace creation is capped. Hold a global creation lock so two
  -- different names cannot both pass the count at the limit.
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns:cap', 0));
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));

  IF p_quota IS NULL AND p_attrs IS NULL
     AND (to_regclass(format('%I.objects', v_schema)) IS NOT NULL
          OR to_regclass(format('%I.messages', v_schema)) IS NOT NULL
          OR to_regclass(format('%I.jobs', v_schema)) IS NOT NULL)
     AND EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
    RETURN v_schema;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
    SELECT count(*) INTO v_count FROM s3p.namespaces;
    IF v_count >= 100 THEN
      RAISE EXCEPTION 'namespace limit reached (100)' USING ERRCODE = '54000';
    END IF;
  END IF;

  INSERT INTO s3p.namespaces (kind, name, schema_name, quota_bytes, attrs)
  VALUES (p_kind, p_name, v_schema, p_quota, coalesce(p_attrs, '{}'::jsonb))
  ON CONFLICT (kind, name) DO UPDATE
    SET quota_bytes = coalesce(EXCLUDED.quota_bytes, s3p.namespaces.quota_bytes),
        attrs = s3p.namespaces.attrs || EXCLUDED.attrs;

  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);

  IF p_kind = 'queue' THEN
    PERFORM s3p.ensure_queue_schema(v_schema);
  ELSE
    PERFORM s3p.ensure_object_schema(v_schema, p_kind);
  END IF;

  EXECUTE format($sql$
    INSERT INTO %1$I.meta (singleton, kind, name, quota_bytes, attrs, created_at)
    SELECT true, kind, name, quota_bytes, attrs, created_at
    FROM s3p.namespaces WHERE kind = $1 AND name = $2
    ON CONFLICT (singleton) DO UPDATE
      SET quota_bytes = EXCLUDED.quota_bytes,
          attrs = EXCLUDED.attrs
  $sql$, v_schema) USING p_kind, p_name;

  RETURN v_schema;
END $_$;


--
-- Name: ensure_object_schema(text, text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.ensure_object_schema(p_schema text, p_kind text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_inline  int  := CASE WHEN p_kind IN ('bucket', 'writeback') THEN 65536 ELSE 8120 END;
  v_keymax  int  := CASE WHEN p_kind = 'bucket' THEN 1024 ELSE 2048 END;
BEGIN
  EXECUTE format($ddl$
    CREATE TABLE IF NOT EXISTS %1$I.objects (
      key           text COLLATE "C" PRIMARY KEY,
      file_id       bigint,
      size          int8 NOT NULL,
      sha256        bytea NOT NULL,
      etag          text NOT NULL,
      user_metadata text[] NOT NULL DEFAULT '{}'::text[],
      content_type  text NOT NULL DEFAULT 'application/octet-stream',
      content_encoding text NOT NULL DEFAULT '',
      origin_modified  timestamptz,
      checksum_name    text,
      checksum_value   text,
      checksum_type    text,
      created_at    timestamptz NOT NULL DEFAULT now(),
      expires_at    timestamptz,
      origin_etag   text,
      parts         int8[],
      part_ends     int8[],
      inline_data   bytea STORAGE EXTERNAL,
      CONSTRAINT object_shape CHECK (
        octet_length(key) BETWEEN 1 AND %3$s AND size >= 0 AND octet_length(sha256) = 32 AND
        etag ~ '^[0-9a-f]{32}(-[1-9][0-9]{0,4})?$' AND
        cardinality(user_metadata) <= 128 AND cardinality(user_metadata) %% 2 = 0 AND
        octet_length(content_type) BETWEEN 1 AND 1024 AND
        octet_length(content_encoding) <= 1024 AND
        ((inline_data IS NOT NULL AND file_id IS NULL AND parts IS NULL AND part_ends IS NULL AND
          size <= %2$s AND octet_length(inline_data) <= size AND
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

    -- Metadata-only parent records do not masquerade as zero-byte objects.
    -- Kept schema-local so flush/drop/export retain the same ownership boundary.
    CREATE TABLE IF NOT EXISTS %1$I.origin_metadata (
      key text COLLATE "C" PRIMARY KEY,
      info jsonb NOT NULL,
      validated_at timestamptz NOT NULL,
      expires_at timestamptz NOT NULL
    );
    CREATE INDEX IF NOT EXISTS origin_metadata_expiry ON %1$I.origin_metadata (expires_at);

    CREATE TABLE IF NOT EXISTS %1$I.chunks (
      file_id int8 NOT NULL,
      no      int4 NOT NULL,
      data    bytea STORAGE EXTERNAL NOT NULL,
      PRIMARY KEY (file_id, no),
      CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
    ) WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);

    CREATE TABLE IF NOT EXISTS %1$I.garbage (
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
      content_encoding   text NOT NULL DEFAULT '',
      created_at         timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT upload_metadata_shape CHECK (
        cardinality(user_metadata) <= 128 AND cardinality(user_metadata) %% 2 = 0 AND
        octet_length(content_type) BETWEEN 1 AND 1024 AND
        octet_length(content_encoding) <= 1024 AND
        octet_length(content_encoding) <= 1024)
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

    CREATE TABLE IF NOT EXISTS %1$I.meta (
      singleton   bool PRIMARY KEY DEFAULT true CHECK (singleton),
      kind        text NOT NULL,
      name        text NOT NULL,
      quota_bytes int8,
      attrs       jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at  timestamptz NOT NULL DEFAULT now()
    );
  $ddl$, p_schema, v_inline, v_keymax);

  EXECUTE format($fn$
    -- Queues candidate file_ids that are no longer referenced by any object or
    -- in-progress multipart upload in this namespace.
    CREATE OR REPLACE FUNCTION %1$I.queue_garbage(p_files int8[])
    RETURNS void LANGUAGE sql AS $body$
      INSERT INTO %1$I.garbage (file_id)
      SELECT DISTINCT f FROM unnest(p_files) AS f
      WHERE f IS NOT NULL AND f <> 0
        AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.file_id IS NOT NULL AND o.file_id = f)
        AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[f])
        AND NOT EXISTS (SELECT 1 FROM %1$I.upload_parts p WHERE p.file_id = f)
      ON CONFLICT DO NOTHING;
    $body$;

    -- PG18 OLD RETURNING gives garbage ownership without a pre-read on the
    -- unconditional hot path. Inline and chunked values use the same mutation.
    CREATE OR REPLACE FUNCTION %1$I.publish_value(
      p_key text, p_file_id int8, p_data bytea, p_size int8, p_sha256 bytea,
      p_etag text, p_meta text[], p_ct text, p_cond text DEFAULT 'none',
      p_if_match text DEFAULT NULL, p_expires_at timestamptz DEFAULT NULL,
      p_keep_ttl boolean DEFAULT false, p_origin_etag text DEFAULT NULL,
      p_expected_origin text DEFAULT NULL, p_skip_lock boolean DEFAULT false,
      p_s3_meta jsonb DEFAULT '{}'::jsonb
    ) RETURNS text LANGUAGE plpgsql AS $body$
    DECLARE
      old_file bigint;
      old_parts int8[];
      old_etag text;
      old_expires timestamptz;
      old_origin text;
      found_old boolean;
      live_old boolean := false;
      v_written int;
    BEGIN
      IF NOT p_skip_lock THEN
        PERFORM pg_advisory_xact_lock(hashtextextended(%2$L::text || E'\x1f' || p_key, 0));
      END IF;
      IF p_s3_meta ? 'fill_owner' THEN
        PERFORM 1 FROM s3p.cache_fill_leases
        WHERE namespace = p_s3_meta->>'fill_namespace'
          AND key = p_s3_meta->>'fill_key' AND owner = p_s3_meta->>'fill_owner'
          AND expires_at > clock_timestamp() FOR UPDATE;
        IF NOT FOUND THEN RETURN 'stale'; END IF;
      END IF;
      IF p_cond <> 'none' OR p_expected_origin IS NOT NULL THEN
        SELECT etag, expires_at, origin_etag
          INTO old_etag, old_expires, old_origin
          FROM %1$I.objects WHERE key = p_key FOR UPDATE;
        found_old := FOUND;
        live_old := found_old AND (old_expires IS NULL OR old_expires > now());
      END IF;
      IF p_cond = 'if_absent' AND live_old THEN RETURN 'precondition'; END IF;
      IF p_cond = 'if_present' AND NOT live_old THEN RETURN 'precondition'; END IF;
      IF p_cond = 'if_match' AND (NOT live_old OR old_etag <> p_if_match) THEN
        RETURN 'precondition';
      END IF;
      IF p_expected_origin IS NOT NULL AND found_old
         AND old_origin IS DISTINCT FROM p_expected_origin THEN
        RETURN 'stale';
      END IF;
      INSERT INTO %1$I.objects
        (key, file_id, size, sha256, etag, user_metadata, content_type,
         content_encoding, origin_modified, checksum_name, checksum_value, checksum_type,
         expires_at, origin_etag, parts, part_ends, inline_data)
      VALUES
        (p_key, p_file_id, p_size, p_sha256, p_etag, p_meta, p_ct,
         coalesce(p_s3_meta->>'content_encoding', ''),
         NULLIF(p_s3_meta->>'origin_modified', '')::timestamptz,
         NULLIF(p_s3_meta->>'checksum_name', ''),
         NULLIF(p_s3_meta->>'checksum_value', ''),
         NULLIF(p_s3_meta->>'checksum_type', ''),
          p_expires_at, p_origin_etag, NULL, NULL, p_data)
      ON CONFLICT (key) DO UPDATE SET
        file_id = EXCLUDED.file_id, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
        etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
        content_type = EXCLUDED.content_type, content_encoding = EXCLUDED.content_encoding,
        origin_modified = EXCLUDED.origin_modified,
        checksum_name = EXCLUDED.checksum_name, checksum_value = EXCLUDED.checksum_value,
        checksum_type = EXCLUDED.checksum_type,
        expires_at = CASE WHEN p_keep_ttl AND (objects.expires_at IS NULL OR objects.expires_at > now())
                          THEN objects.expires_at ELSE EXCLUDED.expires_at END,
        origin_etag = EXCLUDED.origin_etag,
        parts = NULL, part_ends = NULL, inline_data = EXCLUDED.inline_data, created_at = now()
      WHERE p_expected_origin IS NULL
         OR objects.origin_etag IS NOT DISTINCT FROM p_expected_origin
      RETURNING old.file_id, old.parts INTO old_file, old_parts;
      GET DIAGNOSTICS v_written = ROW_COUNT;
      IF v_written = 0 THEN
        RETURN 'stale';
      END IF;
      DELETE FROM %1$I.object_part_checksums WHERE key = p_key;
      DELETE FROM %1$I.origin_metadata WHERE key = p_key;
      IF old_file IS NOT NULL THEN
        PERFORM %1$I.queue_garbage(COALESCE(old_parts, ARRAY[]::int8[]) || old_file);
      END IF;
      RETURN 'ok';
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.publish_object(
      p_key text, p_file_id int8, p_size int8, p_sha256 bytea,
      p_etag text, p_meta text[], p_ct text, p_cond text DEFAULT 'none',
      p_if_match text DEFAULT NULL, p_expires_at timestamptz DEFAULT NULL,
      p_keep_ttl boolean DEFAULT false, p_origin_etag text DEFAULT NULL,
      p_expected_origin text DEFAULT NULL, p_skip_lock boolean DEFAULT false,
      p_s3_meta jsonb DEFAULT '{}'::jsonb
    ) RETURNS text LANGUAGE sql AS $body$
      SELECT %1$I.publish_value(p_key, p_file_id, NULL, p_size, p_sha256,
        p_etag, p_meta, p_ct, p_cond, p_if_match, p_expires_at,
        p_keep_ttl, p_origin_etag, p_expected_origin, p_skip_lock, p_s3_meta);
    $body$;

    CREATE OR REPLACE FUNCTION %1$I.put_inline(
      p_key text, p_data bytea, p_size int8, p_sha256 bytea, p_etag text,
      p_meta text[], p_ct text, p_cond text DEFAULT 'none', p_if_match text DEFAULT NULL,
      p_sync text DEFAULT 'on', p_expires_at timestamptz DEFAULT NULL,
      p_keep_ttl boolean DEFAULT false, p_origin_etag text DEFAULT NULL,
      p_expected_origin text DEFAULT NULL, p_skip_lock boolean DEFAULT false,
      p_s3_meta jsonb DEFAULT '{}'::jsonb
    ) RETURNS text LANGUAGE plpgsql AS $body$
    BEGIN
      IF p_cond = 'none' AND p_sync IN ('local','off','remote_write','remote_apply') THEN
        PERFORM set_config('synchronous_commit', p_sync, true);
      ELSE
        PERFORM set_config('synchronous_commit', 'on', true);
      END IF;
      IF p_size > %3$s OR octet_length(p_data) > p_size THEN
        RAISE EXCEPTION 'inline object exceeds the %3$s-byte limit';
      END IF;
      RETURN %1$I.publish_value(p_key, NULL, p_data, p_size, p_sha256,
        p_etag, p_meta, p_ct, p_cond, p_if_match, p_expires_at,
        p_keep_ttl, p_origin_etag, p_expected_origin, p_skip_lock, p_s3_meta);
    END $body$;

    -- Expire keys in bounded batches. Returns the number removed so callers can
    -- chain passes until the backlog is drained.
    CREATE OR REPLACE FUNCTION %1$I.expire_objects(p_max int DEFAULT 4096)
    RETURNS int LANGUAGE plpgsql AS $body$
    DECLARE
      removed int := 0;
      metadata_removed int := 0;
      dead_files int8[];
    BEGIN
      DELETE FROM %1$I.origin_metadata WHERE key IN (
        SELECT key FROM %1$I.origin_metadata WHERE expires_at <= now()
        ORDER BY expires_at LIMIT p_max FOR UPDATE SKIP LOCKED
      );
      GET DIAGNOSTICS metadata_removed = ROW_COUNT;
      WITH victims AS (
        SELECT key FROM %1$I.objects
        WHERE expires_at IS NOT NULL AND expires_at <= now()
        ORDER BY expires_at LIMIT p_max
        FOR UPDATE SKIP LOCKED
      ), doomed AS (
        DELETE FROM %1$I.objects o USING victims v WHERE o.key = v.key
        RETURNING o.file_id, o.parts
      ), dead AS (
        SELECT file_id FROM doomed WHERE file_id IS NOT NULL
        UNION ALL
        SELECT unnest(parts) FROM doomed WHERE parts IS NOT NULL
      )
      SELECT (SELECT count(*)::int FROM doomed), array_agg(file_id)
        INTO removed, dead_files FROM dead;
      IF dead_files IS NOT NULL THEN
        PERFORM %1$I.queue_garbage(dead_files);
      END IF;
      RETURN COALESCE(removed, 0) + metadata_removed;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.commit_part(
      p_upload text, p_no int4, p_file_id int8, p_size int8, p_sha bytea,
      p_md5 bytea, p_checksum text DEFAULT NULL
    ) RETURNS void LANGUAGE plpgsql AS $body$
    DECLARE old bigint;
    BEGIN
      DELETE FROM %1$I.upload_parts WHERE upload_id = p_upload AND part_no = p_no
        RETURNING file_id INTO old;
      IF old IS NOT NULL THEN
        PERFORM %1$I.queue_garbage(ARRAY[old]);
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
  $fn$, p_schema, p_schema, v_inline, v_keymax);
END $_$;


--
-- Name: ensure_queue_schema(text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.ensure_queue_schema(p_schema text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
BEGIN
  EXECUTE format($ddl$
    CREATE TABLE IF NOT EXISTS %1$I.jobs (
      seq                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      id                 text NOT NULL UNIQUE CHECK (octet_length(id) = 36),
      type               text NOT NULL,
      args               jsonb NOT NULL DEFAULT '[]'::jsonb,
      meta               jsonb,
      queue              text NOT NULL,
      state              text NOT NULL CHECK (state IN
                           ('scheduled','available','pending','active',
                            'completed','retryable','cancelled','discarded')),
      attempt            int4 NOT NULL DEFAULT 0 CHECK (attempt >= 0),
      max_attempts       int4 NOT NULL DEFAULT 3 CHECK (max_attempts >= 0),
      priority           int4 NOT NULL DEFAULT 0,
      timeout_ms         int4,
      created_at         timestamptz NOT NULL DEFAULT now(),
      scheduled_at       timestamptz,
      enqueued_at        timestamptz NOT NULL DEFAULT now(),
      started_at         timestamptz,
      completed_at       timestamptz,
      cancelled_at       timestamptz,
      discarded_at       timestamptz,
      worker_id          text,
      lease_token        text,
      lease_until        timestamptz,
      next_visible_at    timestamptz GENERATED ALWAYS AS (
        CASE WHEN state = 'active' THEN lease_until
             ELSE coalesce(scheduled_at, enqueued_at, created_at)
        END
      ) STORED,
      retry_initial_ms   int4 NOT NULL DEFAULT 1000,
      retry_coefficient  float8 NOT NULL DEFAULT 2.0,
      retry_max_ms       int4 NOT NULL DEFAULT 60000,
      retry_delay_ms     int4,
      result             jsonb,
      error              jsonb,
      expires_at         timestamptz,
      tags               text[] NOT NULL DEFAULT '{}'::text[]
    ) WITH (fillfactor = 80);
    -- One claim index covers available, due scheduled/retryable, and expired
    -- active leases. The generated next_visible_at is the claim time: lease_until
    -- while active, otherwise the scheduled/available time.
    CREATE INDEX IF NOT EXISTS jobs_claim ON %1$I.jobs
      (priority DESC, next_visible_at, seq)
      WHERE state IN ('available','scheduled','retryable','active');
    CREATE INDEX IF NOT EXISTS jobs_active ON %1$I.jobs (lease_until)
      WHERE state = 'active';
    CREATE INDEX IF NOT EXISTS jobs_state ON %1$I.jobs (state);
    CREATE INDEX IF NOT EXISTS jobs_expiry ON %1$I.jobs (expires_at)
      WHERE expires_at IS NOT NULL;
    CREATE INDEX IF NOT EXISTS jobs_terminal_age ON %1$I.jobs
      ((coalesce(completed_at, cancelled_at, discarded_at, created_at)))
      WHERE state IN ('completed','cancelled','discarded');
    CREATE TABLE IF NOT EXISTS %1$I.meta (
      singleton   bool PRIMARY KEY DEFAULT true CHECK (singleton),
      kind        text NOT NULL,
      name        text NOT NULL,
      quota_bytes int8,
      attrs       jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at  timestamptz NOT NULL DEFAULT now()
    );

    -- Shared queue/stream configuration. Jobs and streams live in the same
    -- queue namespace; stream retention/retry settings are independent of the
    -- job lifecycle.
    CREATE TABLE IF NOT EXISTS %1$I.stream_config (
      singleton          bool PRIMARY KEY DEFAULT true CHECK (singleton),
      max_retries        int4 NOT NULL DEFAULT 5 CHECK (max_retries >= 0),
      retention          interval NOT NULL DEFAULT interval '7 days'
                         CHECK (retention > interval '0'),
      current_segment    int2 NOT NULL DEFAULT 0
                         CHECK (current_segment >= 0 AND current_segment < 3),
      segment_started_at timestamptz NOT NULL DEFAULT now(),
      created_at         timestamptz NOT NULL DEFAULT now()
    );
    INSERT INTO %1$I.stream_config (singleton) VALUES (true)
      ON CONFLICT DO NOTHING;

    CREATE TABLE IF NOT EXISTS %1$I.stream_ticks (
      tick_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      snapshot   pg_snapshot NOT NULL DEFAULT pg_current_snapshot(),
      max_seq    bigint NOT NULL DEFAULT 0,
      created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS stream_ticks_created ON %1$I.stream_ticks (created_at);

    -- Three physical event segments rotate on retention/2 boundaries. The
    -- oldest segment is truncated only after every subscription cursor is past
    -- its seal transaction. This is the zero-bloat retention path.
    CREATE TABLE IF NOT EXISTS %1$I.stream_segments (
      segment    int2 PRIMARY KEY CHECK (segment >= 0 AND segment < 3),
      sealed_xid xid8,
      sealed_at  timestamptz
    );
    INSERT INTO %1$I.stream_segments (segment)
      VALUES (0), (1), (2) ON CONFLICT DO NOTHING;

    CREATE SEQUENCE IF NOT EXISTS %1$I.stream_event_seq;

    CREATE TABLE IF NOT EXISTS %1$I.stream_events (
      seq         bigint NOT NULL DEFAULT nextval('%1$I.stream_event_seq'::regclass),
      segment     int2 NOT NULL CHECK (segment >= 0 AND segment < 3),
      event_id    text NOT NULL CHECK (octet_length(event_id) BETWEEN 1 AND 1024),
      txid        xid8 NOT NULL DEFAULT pg_current_xact_id(),
      type        text NOT NULL DEFAULT 'default',
      payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
      headers     jsonb NOT NULL DEFAULT '{}'::jsonb,
      retry_count int4 NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
      created_at  timestamptz NOT NULL DEFAULT now()
    ) PARTITION BY LIST (segment);

    CREATE TABLE IF NOT EXISTS %1$I.stream_events_0
      PARTITION OF %1$I.stream_events FOR VALUES IN (0);
    CREATE TABLE IF NOT EXISTS %1$I.stream_events_1
      PARTITION OF %1$I.stream_events FOR VALUES IN (1);
    CREATE TABLE IF NOT EXISTS %1$I.stream_events_2
      PARTITION OF %1$I.stream_events FOR VALUES IN (2);

    CREATE INDEX IF NOT EXISTS stream_events_txid ON %1$I.stream_events (txid);
    CREATE INDEX IF NOT EXISTS stream_events_created ON %1$I.stream_events (created_at);
    CREATE INDEX IF NOT EXISTS stream_events_id ON %1$I.stream_events (event_id);
    CREATE INDEX IF NOT EXISTS stream_events_seq ON %1$I.stream_events (seq);

    CREATE TABLE IF NOT EXISTS %1$I.stream_subscriptions (
      consumer   text PRIMARY KEY CHECK (octet_length(consumer) BETWEEN 1 AND 128),
      last_tick  bigint NOT NULL,
      next_tick  bigint NOT NULL DEFAULT 0,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS %1$I.stream_batches (
      batch_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      consumer  text NOT NULL REFERENCES %1$I.stream_subscriptions(consumer) ON DELETE CASCADE,
      member    text NOT NULL DEFAULT '' CHECK (octet_length(member) <= 128),
      start_tick bigint NOT NULL,
      end_tick   bigint NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      acked_at   timestamptz
    );
    CREATE UNIQUE INDEX IF NOT EXISTS stream_batches_active
      ON %1$I.stream_batches (consumer, member) WHERE acked_at IS NULL;

    CREATE TABLE IF NOT EXISTS %1$I.stream_batch_events (
      batch_id bigint NOT NULL REFERENCES %1$I.stream_batches(batch_id) ON DELETE CASCADE,
      seq      bigint NOT NULL,
      PRIMARY KEY (batch_id, seq)
    );

    CREATE TABLE IF NOT EXISTS %1$I.stream_retry_events (
      retry_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      batch_id    bigint NOT NULL,
      event_id    text NOT NULL,
      type        text NOT NULL,
      payload     jsonb NOT NULL,
      headers     jsonb NOT NULL DEFAULT '{}'::jsonb,
      retry_count int4 NOT NULL CHECK (retry_count >= 0),
      due_at      timestamptz NOT NULL,
      created_at  timestamptz NOT NULL DEFAULT now(),
      UNIQUE (batch_id, event_id)
    );
    CREATE INDEX IF NOT EXISTS stream_retries_due ON %1$I.stream_retry_events (due_at);

    CREATE TABLE IF NOT EXISTS %1$I.stream_dead_letters (
      dead_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      batch_id    bigint NOT NULL,
      event_id    text NOT NULL,
      type        text NOT NULL,
      payload     jsonb NOT NULL,
      headers     jsonb NOT NULL DEFAULT '{}'::jsonb,
      retry_count int4 NOT NULL CHECK (retry_count >= 0),
      reason      text NOT NULL DEFAULT '',
      created_at  timestamptz NOT NULL DEFAULT now(),
      UNIQUE (batch_id, event_id)
    );
    CREATE INDEX IF NOT EXISTS stream_dead_letters_age ON %1$I.stream_dead_letters (created_at);

    INSERT INTO %1$I.stream_ticks (max_seq)
      SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM %1$I.stream_ticks);
  $ddl$, p_schema);
  EXECUTE format($fn$
    CREATE OR REPLACE FUNCTION %1$I.stream_new_tick() RETURNS int8
    LANGUAGE sql AS $body$
      INSERT INTO %1$I.stream_ticks (max_seq)
        VALUES ((SELECT coalesce(max(seq), 0) FROM %1$I.stream_events))
        RETURNING tick_id;
    $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_latest_tick() RETURNS int8
    LANGUAGE plpgsql AS $body$
    DECLARE tick int8;
    BEGIN
      SELECT coalesce(max(t.tick_id), 0) INTO tick FROM %1$I.stream_ticks t;
      IF tick = 0 THEN
        tick := %1$I.stream_new_tick();
      END IF;
      RETURN tick;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_append(
      p_ids text[], p_types text[], p_payloads text[], p_headers text[]
    ) RETURNS SETOF %1$I.stream_events
    LANGUAGE plpgsql AS $body$
    DECLARE v_segment int2;
    BEGIN
      PERFORM pg_advisory_xact_lock_shared(hashtextextended(%2$L, 0));
      SELECT c.current_segment INTO v_segment FROM %1$I.stream_config c;
      RETURN QUERY
        WITH inserted AS (
          INSERT INTO %1$I.stream_events (segment, event_id, type, payload, headers)
          SELECT v_segment, i.id, i.type, i.payload::jsonb, i.headers::jsonb
          FROM unnest(p_ids, p_types, p_payloads, p_headers)
            WITH ORDINALITY AS i(id, type, payload, headers, ord)
          ORDER BY i.ord
          RETURNING *
        )
        SELECT * FROM inserted ORDER BY seq;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_subscribe(p_consumer text, p_from_tick int8)
    RETURNS %1$I.stream_subscriptions
    LANGUAGE plpgsql AS $body$
    DECLARE
      target int8;
      sub %1$I.stream_subscriptions;
    BEGIN
      PERFORM pg_advisory_xact_lock_shared(hashtextextended(%2$L, 0));
      IF p_from_tick IS NULL THEN
        target := %1$I.stream_latest_tick();
      ELSIF p_from_tick = 0 THEN
        SELECT coalesce(min(t.tick_id), 0) INTO target FROM %1$I.stream_ticks t;
        IF target = 0 THEN
          target := %1$I.stream_new_tick();
        END IF;
      ELSE
        IF NOT EXISTS (SELECT 1 FROM %1$I.stream_ticks t WHERE t.tick_id = p_from_tick) THEN
          RAISE EXCEPTION 'stream tick not found' USING ERRCODE = 'PV004';
        END IF;
        target := p_from_tick;
      END IF;
      INSERT INTO %1$I.stream_subscriptions (consumer, last_tick, next_tick)
        VALUES (p_consumer, target, target)
        ON CONFLICT (consumer) DO NOTHING
        RETURNING * INTO sub;
      IF NOT FOUND THEN
        SELECT * INTO sub FROM %1$I.stream_subscriptions s WHERE s.consumer = p_consumer;
      END IF;
      RETURN sub;
    END $body$;
  $fn$, p_schema, p_schema);
  EXECUTE format($fn$
    CREATE OR REPLACE FUNCTION %1$I.stream_batch(p_batch int8)
    RETURNS TABLE (batch_id int8, consumer text, member text, start_tick int8, end_tick int8,
                   batch_created_at timestamptz, seq int8, event_id text, type text,
                   payload jsonb, headers jsonb, retry_count int4, created_at timestamptz)
    LANGUAGE sql AS $body$
      SELECT r.* FROM (
        SELECT b.batch_id, b.consumer, b.member, b.start_tick, b.end_tick,
               b.created_at AS batch_created_at,
               NULL::int8 AS seq, NULL::text AS event_id, NULL::text AS type,
               NULL::jsonb AS payload, NULL::jsonb AS headers,
               NULL::int4 AS retry_count, NULL::timestamptz AS created_at
        FROM %1$I.stream_batches b
        WHERE b.batch_id = p_batch
        UNION ALL
        SELECT be.batch_id, NULL::text, NULL::text, NULL::int8, NULL::int8, NULL::timestamptz,
               e.seq, e.event_id, e.type, e.payload, e.headers, e.retry_count, e.created_at
        FROM %1$I.stream_batch_events be
        JOIN %1$I.stream_events e ON e.seq = be.seq
        WHERE be.batch_id = p_batch
      ) r
      ORDER BY r.seq NULLS FIRST;
    $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_receive(p_consumer text, p_member text, p_limit int4)
    RETURNS TABLE (batch_id int8, consumer text, member text, start_tick int8, end_tick int8,
                   batch_created_at timestamptz, seq int8, event_id text, type text,
                   payload jsonb, headers jsonb, retry_count int4, created_at timestamptz)
    LANGUAGE plpgsql AS $body$
    #variable_conflict use_column
    DECLARE
      last_seen int8;
      next_seen int8;
      start_at int8;
      end_at int8;
      batch int8;
      seqs int8[];
      found_count int8;
      visible boolean;
    BEGIN
      PERFORM pg_advisory_xact_lock_shared(hashtextextended(%2$L, 0));
      SELECT s.last_tick INTO last_seen FROM %1$I.stream_subscriptions s
        WHERE s.consumer = p_consumer FOR UPDATE;
      IF NOT FOUND THEN
        last_seen := %1$I.stream_latest_tick();
        INSERT INTO %1$I.stream_subscriptions (consumer, last_tick, next_tick)
          VALUES (p_consumer, last_seen, last_seen);
      END IF;
      SELECT b.batch_id INTO batch FROM %1$I.stream_batches b
        WHERE b.consumer = p_consumer AND b.member = p_member AND b.acked_at IS NULL;
      IF FOUND THEN
        RETURN QUERY SELECT * FROM %1$I.stream_batch(batch);
        RETURN;
      END IF;
      SELECT s.next_tick INTO next_seen FROM %1$I.stream_subscriptions s
        WHERE s.consumer = p_consumer;
      IF p_member = '' AND next_seen > last_seen THEN
        RAISE EXCEPTION 'stream has active cooperative batches' USING ERRCODE = 'PV003';
      END IF;
      start_at := greatest(next_seen, last_seen);
      SELECT coalesce(max(t.tick_id), 0) INTO end_at
        FROM %1$I.stream_ticks t WHERE t.tick_id > start_at;
      IF end_at = 0 THEN
        SELECT EXISTS (
          SELECT 1
          FROM %1$I.stream_events e, %1$I.stream_ticks lt,
               (SELECT pg_current_snapshot() AS snapshot) cur
          WHERE lt.tick_id = start_at
            AND pg_visible_in_snapshot(e.txid, cur.snapshot)
            AND NOT pg_visible_in_snapshot(e.txid, lt.snapshot)
            AND (e.txid >= pg_snapshot_xmax(lt.snapshot)
                 OR e.txid IN (SELECT * FROM pg_snapshot_xip(lt.snapshot)
                               EXCEPT SELECT * FROM pg_snapshot_xip(cur.snapshot)))
        ) INTO visible;
        IF NOT visible THEN
          RETURN QUERY SELECT NULL::int8, p_consumer, p_member, start_at, start_at,
            NULL::timestamptz, NULL::int8, NULL::text, NULL::text, NULL::jsonb,
            NULL::jsonb, NULL::int4, NULL::timestamptz;
          RETURN;
        END IF;
        end_at := %1$I.stream_new_tick();
      END IF;
      SELECT coalesce(array_agg(x.seq ORDER BY x.seq), '{}'::int8[]), count(*)
        INTO seqs, found_count
        FROM (
          SELECT e.seq
          FROM %1$I.stream_events e, %1$I.stream_ticks lt, %1$I.stream_ticks ct
          WHERE lt.tick_id = start_at AND ct.tick_id = end_at
            AND pg_visible_in_snapshot(e.txid, ct.snapshot)
            AND NOT pg_visible_in_snapshot(e.txid, lt.snapshot)
            AND (e.txid >= pg_snapshot_xmax(lt.snapshot)
                 OR e.txid IN (SELECT * FROM pg_snapshot_xip(lt.snapshot)
                               EXCEPT SELECT * FROM pg_snapshot_xip(ct.snapshot)))
          ORDER BY e.seq
          LIMIT p_limit + 1
        ) x;
      IF found_count > p_limit THEN
        RAISE EXCEPTION 'stream batch exceeds the requested ceiling' USING ERRCODE = 'PV005';
      END IF;
      IF found_count = 0 THEN
        UPDATE %1$I.stream_subscriptions s
          SET next_tick = end_at,
              last_tick = CASE WHEN p_member = '' OR NOT EXISTS (
                            SELECT 1 FROM %1$I.stream_batches b
                            WHERE b.consumer = p_consumer AND b.acked_at IS NULL)
                           THEN end_at ELSE s.last_tick END,
              updated_at = now()
          WHERE s.consumer = p_consumer;
        RETURN QUERY SELECT NULL::int8, p_consumer, p_member, start_at, end_at,
          NULL::timestamptz, NULL::int8, NULL::text, NULL::text, NULL::jsonb,
          NULL::jsonb, NULL::int4, NULL::timestamptz;
        RETURN;
      END IF;
      INSERT INTO %1$I.stream_batches (consumer, member, start_tick, end_tick)
        VALUES (p_consumer, p_member, start_at, end_at)
        RETURNING batch_id INTO batch;
      UPDATE %1$I.stream_subscriptions s
        SET next_tick = end_at, updated_at = now()
        WHERE s.consumer = p_consumer;
      INSERT INTO %1$I.stream_batch_events (batch_id, seq)
        SELECT batch, unnest(seqs);
      RETURN QUERY SELECT * FROM %1$I.stream_batch(batch);
    END $body$;
  $fn$, p_schema, p_schema);
  EXECUTE format($fn$
    CREATE OR REPLACE FUNCTION %1$I.stream_ack(p_batch int8) RETURNS int8
    LANGUAGE plpgsql AS $body$
    DECLARE
      owner text;
      end_at int8;
      acked timestamptz;
      chained int8[];
      chain_end int8;
    BEGIN
      SELECT b.consumer, b.end_tick, b.acked_at INTO owner, end_at, acked
        FROM %1$I.stream_batches b WHERE b.batch_id = p_batch FOR UPDATE;
      IF NOT FOUND OR acked IS NOT NULL THEN
        RAISE EXCEPTION 'stream batch not found' USING ERRCODE = 'PV001';
      END IF;
      UPDATE %1$I.stream_batches b SET acked_at = now() WHERE b.batch_id = p_batch;
      WITH RECURSIVE chain AS (
        SELECT b.batch_id, b.end_tick
        FROM %1$I.stream_batches b
        WHERE b.consumer = owner AND b.acked_at IS NOT NULL
          AND b.start_tick = (SELECT s.last_tick FROM %1$I.stream_subscriptions s
                              WHERE s.consumer = owner)
        UNION ALL
        SELECT n.batch_id, n.end_tick
        FROM chain c
        JOIN %1$I.stream_batches n
          ON n.consumer = owner AND n.acked_at IS NOT NULL AND n.start_tick = c.end_tick
      )
      SELECT coalesce(array_agg(c.batch_id), '{}'::int8[]), max(c.end_tick)
        INTO chained, chain_end
        FROM chain c;
      IF cardinality(chained) > 0 THEN
        UPDATE %1$I.stream_subscriptions s
          SET last_tick = chain_end, next_tick = GREATEST(s.next_tick, chain_end),
              updated_at = now()
          WHERE s.consumer = owner;
        DELETE FROM %1$I.stream_batches b WHERE b.batch_id = ANY(chained);
      END IF;
      RETURN end_at;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_nack(
      p_batch int8, p_event text, p_retry_secs float8, p_reason text
    ) RETURNS text
    LANGUAGE plpgsql AS $body$
    DECLARE
      msg_type text;
      msg_payload jsonb;
      msg_headers jsonb;
      msg_retry int4;
      max_retry int4;
      next_retry int4;
    BEGIN
      PERFORM 1 FROM %1$I.stream_batches b
        WHERE b.batch_id = p_batch AND b.acked_at IS NULL FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'stream batch not found' USING ERRCODE = 'PV001';
      END IF;
      SELECT e.type, e.payload, e.headers, e.retry_count
        INTO msg_type, msg_payload, msg_headers, msg_retry
        FROM %1$I.stream_batch_events be
        JOIN %1$I.stream_events e ON e.seq = be.seq
        WHERE be.batch_id = p_batch AND e.event_id = p_event;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'stream message not in batch' USING ERRCODE = 'PV002';
      END IF;
      max_retry := coalesce((SELECT c.max_retries FROM %1$I.stream_config c), 5);
      next_retry := msg_retry + 1;
      IF next_retry > max_retry THEN
        INSERT INTO %1$I.stream_dead_letters
          (batch_id, event_id, type, payload, headers, retry_count, reason)
        VALUES (p_batch, p_event, msg_type, msg_payload, msg_headers, next_retry, p_reason)
        ON CONFLICT (batch_id, event_id) DO NOTHING;
        RETURN 'dead_letter';
      END IF;
      INSERT INTO %1$I.stream_retry_events
        (batch_id, event_id, type, payload, headers, retry_count, due_at)
      VALUES (p_batch, p_event, msg_type, msg_payload, msg_headers, next_retry,
              now() + make_interval(secs => p_retry_secs))
      ON CONFLICT (batch_id, event_id) DO NOTHING;
      RETURN 'retryable';
    END $body$;
  $fn$, p_schema, p_schema);
  EXECUTE format($fn$
    CREATE OR REPLACE FUNCTION %1$I.stream_process_retries(p_max int4) RETURNS int8
    LANGUAGE plpgsql AS $body$
    DECLARE
      v_segment int2;
      moved int8;
    BEGIN
      PERFORM pg_advisory_xact_lock_shared(hashtextextended(%2$L, 0));
      SELECT c.current_segment INTO v_segment FROM %1$I.stream_config c;
      WITH due AS (
        DELETE FROM %1$I.stream_retry_events
        WHERE retry_id IN (
          SELECT r.retry_id FROM %1$I.stream_retry_events r
          WHERE r.due_at <= now() ORDER BY r.due_at LIMIT p_max)
        RETURNING event_id, type, payload, headers, retry_count
      ), inserted AS (
        INSERT INTO %1$I.stream_events (segment, event_id, type, payload, headers, retry_count)
        SELECT v_segment, d.event_id, d.type, d.payload, d.headers, d.retry_count FROM due d
        RETURNING 1
      )
      SELECT count(*) INTO moved FROM inserted;
      RETURN moved;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_prune_ticks(p_max int4) RETURNS int8
    LANGUAGE sql AS $body$
      WITH gone AS (
        DELETE FROM %1$I.stream_ticks t
        WHERE t.tick_id <> (SELECT min(m.tick_id) FROM %1$I.stream_ticks m)
          AND t.created_at < now() - (SELECT c.retention FROM %1$I.stream_config c)
          AND NOT EXISTS (SELECT 1 FROM %1$I.stream_subscriptions s WHERE s.last_tick = t.tick_id)
          AND NOT EXISTS (SELECT 1 FROM %1$I.stream_batches b
                          WHERE b.start_tick = t.tick_id OR b.end_tick = t.tick_id)
          AND t.tick_id IN (
            SELECT x.tick_id FROM %1$I.stream_ticks x
            WHERE x.tick_id <> (SELECT min(m.tick_id) FROM %1$I.stream_ticks m)
            ORDER BY x.tick_id LIMIT p_max)
        RETURNING 1
      )
      SELECT count(*) FROM gone;
    $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_rotate() RETURNS int8
    LANGUAGE plpgsql AS $body$
    DECLARE
      current_seg int2;
      target int2;
      is_due boolean;
      events int8;
      pinned boolean;
      batched boolean;
    BEGIN
      PERFORM pg_advisory_xact_lock(hashtextextended(%2$L, 0));
      SELECT c.current_segment, (c.segment_started_at + c.retention / 2.0) <= now()
        INTO current_seg, is_due
        FROM %1$I.stream_config c FOR UPDATE;
      IF NOT FOUND OR NOT is_due THEN
        RETURN 0;
      END IF;
      target := ((current_seg + 1) %% 3)::int2;
      SELECT
        (SELECT count(*) FROM %1$I.stream_events e WHERE e.segment = target),
        EXISTS (
          SELECT 1 FROM %1$I.stream_segments seg
          WHERE seg.segment = target
            AND (seg.sealed_xid IS NULL OR EXISTS (
              SELECT 1 FROM %1$I.stream_subscriptions s
              LEFT JOIN %1$I.stream_ticks t ON t.tick_id = s.last_tick
              WHERE t.tick_id IS NULL OR pg_snapshot_xmin(t.snapshot) <= seg.sealed_xid))),
        EXISTS (
          SELECT 1 FROM %1$I.stream_batch_events be
          JOIN %1$I.stream_events e ON e.seq = be.seq
          WHERE e.segment = target)
        INTO events, pinned, batched;
      IF events > 0 AND (pinned OR batched) THEN
        RETURN 0;
      END IF;
      BEGIN
        EXECUTE 'LOCK TABLE %1$I.stream_events_' || target::text
          || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
      EXCEPTION WHEN lock_not_available THEN
        RETURN 0;
      END;
      EXECUTE 'TRUNCATE TABLE %1$I.stream_events_' || target::text;
      DELETE FROM %1$I.stream_ticks t
        WHERE t.tick_id <> (SELECT min(m.tick_id) FROM %1$I.stream_ticks m)
          AND NOT EXISTS (SELECT 1 FROM %1$I.stream_subscriptions s WHERE s.last_tick = t.tick_id)
          AND NOT EXISTS (SELECT 1 FROM %1$I.stream_batches b
                          WHERE b.start_tick = t.tick_id OR b.end_tick = t.tick_id)
          AND pg_snapshot_xmin(t.snapshot) <= (
            SELECT seg.sealed_xid FROM %1$I.stream_segments seg WHERE seg.segment = target);
      UPDATE %1$I.stream_segments seg SET sealed_xid = pg_current_xact_id(), sealed_at = now()
        WHERE seg.segment = current_seg;
      UPDATE %1$I.stream_segments seg SET sealed_xid = NULL, sealed_at = NULL
        WHERE seg.segment = target;
      UPDATE %1$I.stream_config c SET current_segment = target, segment_started_at = now();
      DELETE FROM %1$I.stream_ticks t
        WHERE t.tick_id <> (SELECT min(m.tick_id) FROM %1$I.stream_ticks m)
          AND t.created_at < now() - (SELECT c.retention FROM %1$I.stream_config c)
          AND NOT EXISTS (SELECT 1 FROM %1$I.stream_subscriptions s WHERE s.last_tick = t.tick_id)
          AND NOT EXISTS (SELECT 1 FROM %1$I.stream_batches b
                          WHERE b.start_tick = t.tick_id OR b.end_tick = t.tick_id);
      RETURN events;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_replay_dead_letter(p_dead int8) RETURNS text
    LANGUAGE plpgsql AS $body$
    DECLARE
      v_segment int2;
      ev_id text;
      ev_type text;
      ev_payload jsonb;
      ev_headers jsonb;
    BEGIN
      PERFORM pg_advisory_xact_lock_shared(hashtextextended(%2$L, 0));
      SELECT c.current_segment INTO v_segment FROM %1$I.stream_config c;
      SELECT d.event_id, d.type, d.payload, d.headers
        INTO ev_id, ev_type, ev_payload, ev_headers
        FROM %1$I.stream_dead_letters d WHERE d.dead_id = p_dead FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'stream message not in batch' USING ERRCODE = 'PV002';
      END IF;
      INSERT INTO %1$I.stream_events (segment, event_id, type, payload, headers, retry_count)
        VALUES (v_segment, ev_id, ev_type, ev_payload, ev_headers, 0);
      DELETE FROM %1$I.stream_dead_letters d WHERE d.dead_id = p_dead;
      RETURN ev_id;
    END $body$;

    CREATE OR REPLACE FUNCTION %1$I.stream_replay_dead_letters() RETURNS int8
    LANGUAGE plpgsql AS $body$
    DECLARE
      v_segment int2;
      replayed int8;
    BEGIN
      PERFORM pg_advisory_xact_lock_shared(hashtextextended(%2$L, 0));
      SELECT c.current_segment INTO v_segment FROM %1$I.stream_config c;
      WITH gone AS (
        DELETE FROM %1$I.stream_dead_letters
        RETURNING event_id, type, payload, headers
      ), inserted AS (
        INSERT INTO %1$I.stream_events (segment, event_id, type, payload, headers, retry_count)
        SELECT v_segment, g.event_id, g.type, g.payload, g.headers, 0 FROM gone g
        RETURNING 1
      )
      SELECT count(*) INTO replayed FROM inserted;
      RETURN replayed;
    END $body$;
  $fn$, p_schema, p_schema);
END $_$;


--
-- Name: namespace_schema(text, text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.namespace_schema(p_kind text, p_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $_$
BEGIN
  IF p_kind NOT IN ('bucket', 'cache', 'writeback', 'queue') THEN
    RAISE EXCEPTION 'unknown namespace kind: %', p_kind USING ERRCODE = '22023';
  END IF;
  IF p_name IS NULL OR p_name !~ '^[a-z0-9._-]{1,58}$' THEN
    RAISE EXCEPTION 'invalid namespace name: %', coalesce(p_name, '<null>')
      USING ERRCODE = '22023';
  END IF;
  RETURN CASE p_kind
    WHEN 'cache' THEN 's3c_' || p_name
    WHEN 'writeback' THEN 's3w_' || p_name
    WHEN 'queue' THEN 's3q_' || p_name
    ELSE 's3b_' || p_name
  END;
END $_$;


--
-- Name: reconcile_namespaces(); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.reconcile_namespaces() RETURNS integer
    LANGUAGE sql
    AS $$
  SELECT s3p.adopt_namespaces(NULL);
$$;


--
-- Name: tune_queue_schema(text); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.tune_queue_schema(p_schema text) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  EXECUTE format(
    'ALTER TABLE %I.jobs SET (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02)',
    p_schema);
END $$;


--
-- Name: upgrade_queue_schemas(); Type: FUNCTION; Schema: s3p; Owner: -
--

CREATE FUNCTION s3p.upgrade_queue_schemas() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  r record;
  n int := 0;
BEGIN
  FOR r IN SELECT schema_name FROM s3p.namespaces WHERE kind = 'queue' LOOP
    PERFORM s3p.ensure_queue_schema(r.schema_name);
    n := n + 1;
  END LOOP;
  RETURN n;
END $$;


--
-- Name: file_ids; Type: SEQUENCE; Schema: s3p; Owner: -
--

CREATE SEQUENCE s3p.file_ids
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 64;


SET LOCAL default_table_access_method = heap;

--
-- Name: job_index; Type: TABLE; Schema: s3p; Owner: -
--

CREATE TABLE s3p.job_index (
    id text NOT NULL COLLATE pg_catalog."C",
    kind text DEFAULT 'queue'::text NOT NULL,
    queue text NOT NULL COLLATE pg_catalog."C",
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT job_index_kind_check CHECK ((kind = 'queue'::text))
);


--
-- Name: layout; Type: TABLE; Schema: s3p; Owner: -
--

CREATE TABLE s3p.layout (
    version integer NOT NULL
);


--
-- Name: namespaces; Type: TABLE; Schema: s3p; Owner: -
--

CREATE TABLE s3p.namespaces (
    kind text NOT NULL,
    name text NOT NULL COLLATE pg_catalog."C",
    schema_name text NOT NULL COLLATE pg_catalog."C",
    quota_bytes bigint,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT namespace_name_shape CHECK ((name ~ '^[a-z0-9._-]{1,58}$'::text)),
    CONSTRAINT namespaces_kind_check CHECK ((kind = ANY (ARRAY['bucket'::text, 'cache'::text, 'writeback'::text, 'queue'::text])))
);


--
-- Name: cache_fill_leases; Type: TABLE; Schema: s3p; Owner: -
--

CREATE TABLE s3p.cache_fill_leases (
  namespace   text NOT NULL,
  key         text NOT NULL,
  owner       text NOT NULL,
  expires_at  timestamptz NOT NULL,
  PRIMARY KEY (namespace, key)
);
CREATE INDEX cache_fill_leases_expiry ON s3p.cache_fill_leases (expires_at);


--
-- Name: pending_uploads; Type: TABLE; Schema: s3p; Owner: -
--

CREATE TABLE s3p.pending_uploads (
    kind text DEFAULT 'cache'::text NOT NULL,
    namespace text NOT NULL COLLATE pg_catalog."C",
    key text NOT NULL COLLATE pg_catalog."C",
    token text DEFAULT ''::text NOT NULL,
    size bigint DEFAULT 0 NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt timestamp with time zone DEFAULT now() NOT NULL,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    base_etag text,
    base_missing boolean DEFAULT true NOT NULL,
    lease_owner text,
    lease_until timestamp with time zone,
    dead boolean DEFAULT false NOT NULL,
    CONSTRAINT pending_uploads_kind_check CHECK ((kind = 'cache'::text))
);


--
-- Name: job_index job_index_pkey; Type: CONSTRAINT; Schema: s3p; Owner: -
--

ALTER TABLE ONLY s3p.job_index
    ADD CONSTRAINT job_index_pkey PRIMARY KEY (id);


--
-- Name: namespaces namespaces_pkey; Type: CONSTRAINT; Schema: s3p; Owner: -
--

ALTER TABLE ONLY s3p.namespaces
    ADD CONSTRAINT namespaces_pkey PRIMARY KEY (kind, name);


--
-- Name: namespaces namespaces_schema_name_key; Type: CONSTRAINT; Schema: s3p; Owner: -
--

ALTER TABLE ONLY s3p.namespaces
    ADD CONSTRAINT namespaces_schema_name_key UNIQUE (schema_name);


--
-- Name: pending_uploads pending_uploads_pkey; Type: CONSTRAINT; Schema: s3p; Owner: -
--

ALTER TABLE ONLY s3p.pending_uploads
    ADD CONSTRAINT pending_uploads_pkey PRIMARY KEY (namespace, key);


--
-- Name: job_index_queue; Type: INDEX; Schema: s3p; Owner: -
--

CREATE INDEX job_index_queue ON s3p.job_index USING btree (queue);


--
-- Name: pending_uploads_claim; Type: INDEX; Schema: s3p; Owner: -
--

CREATE INDEX pending_uploads_claim ON s3p.pending_uploads USING btree (namespace, next_attempt, lease_until);


--
-- Name: pending_uploads_due; Type: INDEX; Schema: s3p; Owner: -
--

CREATE INDEX pending_uploads_due ON s3p.pending_uploads USING btree (namespace, next_attempt);


--
-- Name: pending_uploads_fk_idx; Type: INDEX; Schema: s3p; Owner: -
--

CREATE INDEX pending_uploads_fk_idx ON s3p.pending_uploads USING btree (kind, namespace);


--
-- Name: job_index job_index_kind_queue_fkey; Type: FK CONSTRAINT; Schema: s3p; Owner: -
--

ALTER TABLE ONLY s3p.job_index
    ADD CONSTRAINT job_index_kind_queue_fkey FOREIGN KEY (kind, queue) REFERENCES s3p.namespaces(kind, name) ON DELETE CASCADE;


--
-- Name: pending_uploads pending_uploads_kind_namespace_fkey; Type: FK CONSTRAINT; Schema: s3p; Owner: -
--

ALTER TABLE ONLY s3p.pending_uploads
    ADD CONSTRAINT pending_uploads_kind_namespace_fkey FOREIGN KEY (kind, namespace) REFERENCES s3p.namespaces(kind, name) ON DELETE CASCADE;


INSERT INTO s3p.layout (version) VALUES (25);

-- End of pgvs3 schema baseline.
