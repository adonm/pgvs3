-- +goose Up
INSERT INTO s3p.layout (version)
SELECT 11
WHERE NOT EXISTS (SELECT 1 FROM s3p.layout);

-- +goose Down
DELETE FROM s3p.layout WHERE version = 11;
