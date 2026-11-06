-- Native startup for the official DuckDB CLI. The image entrypoint is
-- /duckdb; keep stdin open (docker run -d -i --init, or K8s stdin: true with
-- stdinOnce: false) or the CLI reaches EOF and the in-process Quack server
-- exits. Quack is a one-shot CLI command, not a daemon.
.output /dev/null
.read /opt/pgvs3-duckdb/lake.sql

SELECT CASE WHEN length(coalesce(getenv('QUACK_TOKEN'), '')) < 4
  OR regexp_matches(coalesce(getenv('QUACK_TOKEN'), ''), '\s')
  THEN error('QUACK_TOKEN must be at least 4 non-whitespace characters') END;
SELECT CASE WHEN coalesce(getenv('QUACK_BIND'), '') = ''
  THEN error('QUACK_BIND is required') END;
SELECT CASE WHEN coalesce(getenv('QUACK_ALLOW_OTHER_HOSTNAME'), '') = ''
  THEN error('QUACK_ALLOW_OTHER_HOSTNAME is required') END;

CALL quack_serve(
  'quack:' || getenv('QUACK_BIND'),
  token=getenv('QUACK_TOKEN'),
  allow_other_hostname=CAST(getenv('QUACK_ALLOW_OTHER_HOSTNAME') AS BOOLEAN),
  disable_ssl=true
);

.output
.print PGVS3_READY
.print warning Quack sessions start in the memory catalog; persistent tables must be qualified as lake.main.<table> (for example CREATE TABLE lake.main.t ...). Use q.query(...) or quack_query(...) for arbitrary qualified SQL.
