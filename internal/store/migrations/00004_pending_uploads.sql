-- +goose Up
CREATE TABLE IF NOT EXISTS s3p.pending_uploads (
  kind         text NOT NULL DEFAULT 'cache' CHECK (kind = 'cache'),
  namespace    text COLLATE "C" NOT NULL,
  key          text COLLATE "C" NOT NULL,
  token        text NOT NULL DEFAULT '',
  attempts     int NOT NULL DEFAULT 0,
  next_attempt timestamptz NOT NULL DEFAULT now(),
  last_error   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace, key),
  FOREIGN KEY (kind, namespace) REFERENCES s3p.namespaces (kind, name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS pending_uploads_due
  ON s3p.pending_uploads (namespace, next_attempt);

-- +goose Down
DROP TABLE IF EXISTS s3p.pending_uploads;
