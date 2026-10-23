-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
  v int;
BEGIN
  SELECT max(version) INTO v FROM s3p.layout;
  IF v = 10 THEN
    PERFORM s3p.migrate_v10();
  END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
-- v10 migration is irreversible.
