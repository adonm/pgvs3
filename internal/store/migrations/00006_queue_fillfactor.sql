-- +goose Up
-- Queue rows are updated on every receive (visible_at, receipt). Leave more
-- free space per page and vacuum them more eagerly to reduce page splits and
-- dead-tuple pressure under lease churn.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION s3p.tune_queue_schema(p_schema text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format(
    'ALTER TABLE %I.messages SET (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02)',
    p_schema);
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT schema_name FROM s3p.namespaces WHERE kind = 'queue' LOOP
    PERFORM s3p.tune_queue_schema(r.schema_name);
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION s3p.tune_queue_schema(p_schema text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format(
    'ALTER TABLE %I.messages RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor); ALTER TABLE %I.messages SET (fillfactor = 80)',
    p_schema, p_schema);
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT schema_name FROM s3p.namespaces WHERE kind = 'queue' LOOP
    PERFORM s3p.tune_queue_schema(r.schema_name);
  END LOOP;
END $$;
-- +goose StatementEnd
