--
-- pgvs3 cache-only schema baseline (Layout v26; apply only to an empty database).
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

CREATE SCHEMA s3p;

CREATE SEQUENCE s3p.file_ids
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE s3p.layout (
    version integer NOT NULL
);

CREATE TABLE s3p.namespaces (
    kind text NOT NULL,
    name text NOT NULL,
    schema_name text NOT NULL,
    quota_bytes bigint,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (kind, name),
    UNIQUE (schema_name),
    CONSTRAINT namespaces_kind_check CHECK (kind IN ('bucket', 'cache'))
);

CREATE TABLE s3p.cache_fill_leases (
    namespace text NOT NULL,
    key text NOT NULL,
    owner text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE INDEX cache_fill_leases_expiry ON s3p.cache_fill_leases (expires_at);

CREATE FUNCTION s3p.namespace_schema(p_kind text, p_name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $$
DECLARE
  v_prefix text;
BEGIN
  v_prefix := CASE p_kind
    WHEN 'cache' THEN 's3c_'
    WHEN 'bucket' THEN 's3b_'
    ELSE NULL END;
  IF v_prefix IS NULL THEN
    RAISE EXCEPTION 'invalid namespace kind %', p_kind;
  END IF;
  IF p_name !~ '^[a-z0-9][a-z0-9._-]{0,57}$' OR p_name LIKE '%..%' THEN
    RAISE EXCEPTION 'invalid namespace name %', p_name;
  END IF;
  RETURN v_prefix || p_name;
END $$;

CREATE FUNCTION s3p.ensure_object_schema(p_schema text, p_kind text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_inline  int  := 8120;
  v_keymax  int  := 2048;
BEGIN
  EXECUTE format($ddl$
    CREATE TABLE IF NOT EXISTS %1$I.objects (
      key              text COLLATE "C" PRIMARY KEY,
      file_id          bigint,
      size             int8 NOT NULL,
      sha256           bytea NOT NULL,
      etag             text NOT NULL,
      user_metadata    text[] NOT NULL DEFAULT '{}'::text[],
      content_type     text NOT NULL DEFAULT 'application/octet-stream',
      content_encoding text NOT NULL DEFAULT '',
      origin_modified  timestamptz,
      checksum_name    text,
      checksum_value   text,
      checksum_type    text,
      created_at       timestamptz NOT NULL DEFAULT now(),
      expires_at       timestamptz,
      origin_etag      text,
      parts            int8[],
      part_ends        int8[],
      inline_data      bytea STORAGE EXTERNAL,
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
          size <= 2147483648::int8 * 8120))
      )
    ) WITH (fillfactor = 85, toast_tuple_target = 8160,
            autovacuum_vacuum_scale_factor = 0.02);

    CREATE INDEX IF NOT EXISTS objects_file_id_idx ON %1$I.objects (file_id)
      WHERE file_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS objects_expires_idx ON %1$I.objects (expires_at)
      WHERE expires_at IS NOT NULL;

    CREATE TABLE IF NOT EXISTS %1$I.origin_metadata (
      key          text COLLATE "C" PRIMARY KEY,
      info         jsonb NOT NULL,
      validated_at timestamptz NOT NULL,
      expires_at   timestamptz NOT NULL
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
    CREATE OR REPLACE FUNCTION %1$I.queue_garbage(p_files int8[])
    RETURNS void LANGUAGE sql AS $body$
      INSERT INTO %1$I.garbage (file_id)
      SELECT DISTINCT f FROM unnest(p_files) AS f
      WHERE f IS NOT NULL AND f <> 0
        AND NOT EXISTS (SELECT 1 FROM %1$I.objects o WHERE o.file_id IS NOT NULL AND o.file_id = f)
      ON CONFLICT DO NOTHING;
    $body$;

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
      RETURNING old.file_id INTO old_file;
      GET DIAGNOSTICS v_written = ROW_COUNT;
      IF v_written = 0 THEN
        RETURN 'stale';
      END IF;
      DELETE FROM %1$I.origin_metadata WHERE key = p_key;
      IF old_file IS NOT NULL THEN
        PERFORM %1$I.queue_garbage(ARRAY[old_file]);
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
        RETURNING o.file_id
      )
      SELECT (SELECT count(*)::int FROM doomed), array_agg(file_id) FILTER (WHERE file_id IS NOT NULL)
        INTO removed, dead_files FROM doomed;
      IF dead_files IS NOT NULL THEN
        PERFORM %1$I.queue_garbage(dead_files);
      END IF;
      RETURN COALESCE(removed, 0) + metadata_removed;
    END $body$;
  $fn$, p_schema, p_schema, v_inline, v_keymax);
END $_$;

CREATE FUNCTION s3p.ensure_namespace(p_kind text, p_name text, p_quota bigint DEFAULT NULL::bigint, p_attrs jsonb DEFAULT NULL::jsonb) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_count  int;
BEGIN
  IF p_quota IS NULL AND p_attrs IS NULL THEN
    IF EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
      IF to_regclass(format('%I.objects', v_schema)) IS NOT NULL THEN
        RETURN v_schema;
      END IF;
    END IF;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns:cap', 0));
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));
  IF p_quota IS NULL AND p_attrs IS NULL
     AND to_regclass(format('%I.objects', v_schema)) IS NOT NULL
     AND EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
    RETURN v_schema;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name) THEN
    SELECT count(*) INTO v_count FROM s3p.namespaces;
    IF v_count >= 100 THEN
      RAISE EXCEPTION 'too many namespaces (limit 100)' USING ERRCODE = '53000';
    END IF;
  END IF;
  INSERT INTO s3p.namespaces (kind, name, schema_name, quota_bytes, attrs)
  VALUES (p_kind, p_name, v_schema, p_quota, coalesce(p_attrs, '{}'::jsonb))
  ON CONFLICT (kind, name) DO UPDATE
    SET quota_bytes = coalesce(EXCLUDED.quota_bytes, s3p.namespaces.quota_bytes),
        attrs = s3p.namespaces.attrs || EXCLUDED.attrs;

  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);
  PERFORM s3p.ensure_object_schema(v_schema, p_kind);
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

CREATE FUNCTION s3p.drop_namespace(p_kind text, p_name text, p_force boolean DEFAULT false) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_schema text := s3p.namespace_schema(p_kind, p_name);
  v_occupied boolean := false;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('s3p.ns', 0) # hashtextextended(p_kind || ':' || p_name, 1));
  IF NOT EXISTS (SELECT 1 FROM s3p.namespaces WHERE kind = p_kind AND name = p_name FOR UPDATE) AND
     to_regnamespace(format('%I', v_schema)) IS NULL THEN
    RETURN 'not_found';
  END IF;
  IF NOT p_force AND to_regclass(format('%I.objects', v_schema)) IS NOT NULL THEN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.objects)', v_schema) INTO v_occupied;
    IF v_occupied THEN RETURN 'not_empty'; END IF;
  END IF;
  EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', v_schema);
  DELETE FROM s3p.namespaces WHERE kind = p_kind AND name = p_name;
  RETURN 'deleted';
END $$;

INSERT INTO s3p.layout (version) VALUES (26);
