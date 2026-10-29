#!/usr/bin/env bash

test_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

test_run() {
  cd "$test_root" || return
  test_name=$1
  test_started=$SECONDS
  mkdir -p .tmp/verify
  PGVS3_TEST_RUN_DIR=$(mktemp -d "$test_root/.tmp/verify/$test_name.XXXXXX")
  PGVS3_TEST_BIN="$PGVS3_TEST_RUN_DIR/pgvs3"
  export PGVS3_TEST_RUN_DIR PGVS3_TEST_BIN
  test_pids=()
  test_containers=()
  test_networks=()
  PGVS3_TEST_SQL_ROUTES="$PGVS3_TEST_RUN_DIR/sql-routes.json"
  PGVS3_TEST_PSQL="$test_root/test/psql.py"
  export PGVS3_TEST_SQL_ROUTES PGVS3_TEST_PSQL
  printf '{}\n' > "$PGVS3_TEST_SQL_ROUTES"
  exec {test_stdout}>&1 {test_stderr}>&2
  trap 'test_finish "$?"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM HUP
}

test_finish() {
  local status=$1 log container network
  trap - EXIT
  exec 1>&"$test_stdout" 2>&"$test_stderr"
  exec {test_stdout}>&- {test_stderr}>&-
  if [ "${#test_pids[@]}" -gt 0 ]; then
    kill "${test_pids[@]}" 2>/dev/null || true
    wait "${test_pids[@]}" 2>/dev/null || true
  fi
  for container in "${test_containers[@]}"; do
    if [ "$status" -ne 0 ]; then
      docker logs --tail 100 "$container" > "$PGVS3_TEST_RUN_DIR/$container.log" 2>&1 || true
    fi
    docker rm -fv "$container" >/dev/null 2>&1 || true
  done
  for network in "${test_networks[@]}"; do
    docker network rm "$network" >/dev/null 2>&1 || true
  done
  if [ "$status" -ne 0 ]; then
    printf '%s failed (exit %s); artifacts: %s\n' "$test_name" "$status" "$PGVS3_TEST_RUN_DIR" >&2
    while IFS= read -r -d '' log; do
      printf '\n%s\n' "$log" >&2
      tail -n 100 "$log" >&2
    done < <(find "$PGVS3_TEST_RUN_DIR" -name '*.log' -type f -print0)
  else
    if [ "${test_keep_artifacts:-false}" = true ]; then
      printf '%s ok (%ss); artifacts: %s\n' "$test_name" "$((SECONDS - test_started))" "$PGVS3_TEST_RUN_DIR"
    else
      printf '%s ok (%ss)\n' "$test_name" "$((SECONDS - test_started))"
      rm -rf "$PGVS3_TEST_RUN_DIR"
    fi
  fi
  exit "$status"
}

test_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'
}

test_sql_route() {
  python3 - "$1" "$2" <<'PY'
import json, os, sys
from pathlib import Path
path=Path(os.environ['PGVS3_TEST_SQL_ROUTES'])
routes=json.loads(path.read_text())
routes[sys.argv[1]]=sys.argv[2]
path.write_text(json.dumps(routes)+'\n')
PY
}

test_postgres() {
  local buffers=$1 label=${2:-} container="pgvs3-${PGVS3_TEST_RUN_DIR##*/}" ready=false port
  if [ "$#" -ge 2 ]; then shift 2; else shift; fi
  [ -z "$label" ] || container+="-$label"
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
  test_containers+=("$container")
  docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
    -p "127.0.0.1:$port:5432" postgres:18 -c max_connections=256 -c "shared_buffers=$buffers" "$@" \
    >> "$PGVS3_TEST_RUN_DIR/postgres-start.log" 2>&1
  for _ in $(seq 1 120); do
    if docker exec "$container" pg_isready -q -h 127.0.0.1 -U postgres >/dev/null 2>&1; then ready=true; break; fi
    sleep 0.25
  done
  [ "$ready" = true ] || return 1
  local address
  address=$(docker port "$container" 5432/tcp)
  PGVS3_TEST_PG_PORT=${address##*:}
  PGVS3_TEST_PG_CONTAINER=$container
  PGVS3_TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$PGVS3_TEST_PG_PORT/postgres?sslmode=disable"
  export PGVS3_TEST_PG_PORT PGVS3_TEST_PG_CONTAINER PGVS3_TEST_DATABASE_URL
  test_sql_route "$PGVS3_TEST_PG_PORT" "$container"
}

test_standby() {
  local primary=$1 label=$2 buffers=${3:-256MB} port ready=false
  local container="pgvs3-${PGVS3_TEST_RUN_DIR##*/}-$label" app="pgvs3_${label//-/_}"
  local network="pgvs3-${PGVS3_TEST_RUN_DIR##*/}-$label"
  test_networks+=("$network")
  docker network create "$network" >> "$PGVS3_TEST_RUN_DIR/postgres-start.log"
  docker network connect "$network" "$primary"
  docker exec "$primary" bash -ec 'printf "\nhost replication postgres all scram-sha-256\n" >> "$PGDATA/pg_hba.conf"'
  docker exec "$primary" psql -U postgres -c 'SELECT pg_reload_conf()' >> "$PGVS3_TEST_RUN_DIR/postgres-start.log"
  port=$(test_port)
  test_containers+=("$container")
  docker run -d --name "$container" --network "$network" \
    -p "127.0.0.1:$port:5432" -e PGPASSWORD=postgres -e "REPL_PRIMARY=$primary" -e "REPL_NAME=$app" \
    -e PGDATA=/var/lib/postgresql/18/docker -e "REPL_BUFFERS=$buffers" postgres:18 bash -ec '
      mkdir -p "$PGDATA"
      chown postgres:postgres "$PGDATA"
      chmod 700 "$PGDATA"
      if [ ! -s "$PGDATA/PG_VERSION" ]; then
        gosu postgres pg_basebackup -d "host=$REPL_PRIMARY user=postgres password=postgres application_name=$REPL_NAME" \
          -D "$PGDATA" -X stream -R -C -S "$REPL_NAME" -c fast
      fi
      exec gosu postgres postgres -c max_connections=256 -c hot_standby=on -c "shared_buffers=$REPL_BUFFERS" -c hot_standby_feedback=on
    ' >> "$PGVS3_TEST_RUN_DIR/postgres-start.log" 2>&1
  for _ in $(seq 1 240); do
    if [ "$(docker exec "$container" psql -U postgres -At -c 'SELECT pg_is_in_recovery()' 2>/dev/null)" = t ]; then ready=true; break; fi
    [ "$(docker inspect --format '{{.State.Running}}' "$container")" = true ] || return 1
    sleep 0.25
  done
  [ "$ready" = true ] || return 1
  PGVS3_TEST_STANDBY_CONTAINER=$container
  PGVS3_TEST_STANDBY_PORT=$port
  PGVS3_TEST_STANDBY_URL="postgres://postgres:postgres@127.0.0.1:$port/postgres?sslmode=disable"
  PGVS3_TEST_STANDBY_APP=$app
  export PGVS3_TEST_STANDBY_CONTAINER PGVS3_TEST_STANDBY_PORT PGVS3_TEST_STANDBY_URL PGVS3_TEST_STANDBY_APP
  test_sql_route "$port" "$container"
}

test_build_gateway() {
  mise exec go -- go build -o "$PGVS3_TEST_BIN" ./cmd/pgvs3 > "$PGVS3_TEST_RUN_DIR/build.log" 2>&1
}

test_start_gateway() {
  local label=$1 url=$2 port=$3
  shift 3
  "$PGVS3_TEST_BIN" --url "$url" serve --addr "127.0.0.1:$port" "$@" \
    > "$PGVS3_TEST_RUN_DIR/$label.log" 2>&1 &
  test_gateway_pid=$!
  test_pids+=("$test_gateway_pid")
  for _ in $(seq 1 120); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; then return 0; fi
    kill -0 "$test_gateway_pid" 2>/dev/null || return 1
    sleep 0.25
  done
  return 1
}
