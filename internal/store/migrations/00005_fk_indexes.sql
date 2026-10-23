-- +goose Up
-- Supporting indexes for the (kind, namespace) foreign keys on the cache
-- tables. Without these, dropping or renaming a namespace scans both tables.
CREATE INDEX IF NOT EXISTS origin_listing_fk_idx
  ON s3p.origin_listing (kind, namespace);
CREATE INDEX IF NOT EXISTS pending_uploads_fk_idx
  ON s3p.pending_uploads (kind, namespace);

-- +goose Down
DROP INDEX IF EXISTS s3p.pending_uploads_fk_idx;
DROP INDEX IF EXISTS s3p.origin_listing_fk_idx;
