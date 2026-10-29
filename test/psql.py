#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
from urllib.parse import unquote, urlsplit

url = urlsplit(sys.argv[1])
routes = json.loads(Path(os.environ["PGVS3_TEST_SQL_ROUTES"]).read_text())
container = routes[str(url.port)]
os.execvp("docker", ["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", unquote(url.path[1:]), *sys.argv[2:]])
