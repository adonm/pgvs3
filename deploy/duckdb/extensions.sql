-- Signed extension setup shared by the server and one-shot lake checks.
-- There is no INSTALL here: every module is baked into the image and loaded
-- with autoinstall/autoload/unsigned loading disabled.
SET autoinstall_known_extensions=false;
SET autoload_known_extensions=false;
SET allow_unsigned_extensions=false;
SET threads=getenv('DUCKDB_THREADS');
SET memory_limit=getenv('DUCKDB_MEMORY_LIMIT');
SET temp_directory=getenv('DUCKDB_TEMP_DIRECTORY');
LOAD spatial;
LOAD ducklake;
LOAD postgres_scanner;
LOAD fts;
LOAD vss;
LOAD httpfs;
LOAD quack;
LOAD raquet;
LOAD differential;
