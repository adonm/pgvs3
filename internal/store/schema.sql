CREATE TABLE IF NOT EXISTS cache_layout (
    version integer NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_objects (
    id               bigserial PRIMARY KEY,
    bucket           text NOT NULL,
    key              text NOT NULL,
    etag             text NOT NULL,
    size             bigint NOT NULL CHECK (size >= 0),
    content_type     text NOT NULL DEFAULT 'application/octet-stream',
    content_encoding text NOT NULL DEFAULT '',
    last_modified    timestamptz NOT NULL DEFAULT now(),
    user_meta        text[] NOT NULL DEFAULT '{}'::text[],
    filled_at        timestamptz NOT NULL DEFAULT now(),
    last_access      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket, key)
);

CREATE INDEX IF NOT EXISTS cache_objects_lru_idx ON cache_objects (last_access, id);

CREATE TABLE IF NOT EXISTS cache_chunks (
    object_id bigint NOT NULL REFERENCES cache_objects(id) ON DELETE CASCADE,
    idx       integer NOT NULL CHECK (idx >= 0),
    data      bytea STORAGE EXTERNAL NOT NULL,
    PRIMARY KEY (object_id, idx)
) WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01);
