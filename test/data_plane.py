#!/usr/bin/env python3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import date, timedelta
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
RUN = Path(os.environ["PGVS3_TEST_RUN_DIR"])
BIN = os.environ["PGVS3_TEST_BIN"]
DATABASE = os.environ["PGVS3_TEST_DB_URL"]
A = os.environ["PGVS3_TEST_ENDPOINT_A"]
B = os.environ["PGVS3_TEST_ENDPOINT_B"]
ORIGIN = os.environ["PGVS3_TEST_ORIGIN_ENDPOINT"]
PSQL = os.environ["PGVS3_TEST_PSQL"]


def eventually(description, fn, seconds=60):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (OSError, HTTPError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            last = error
        time.sleep(0.05)
    raise AssertionError(f"{description}: {last}")


def sql(statement, database=DATABASE):
    return subprocess.check_output([PSQL, database, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement], text=True, stderr=subprocess.PIPE, timeout=30).strip()


DUCKDB_IMAGE = os.environ.get("PGVS3_DUCKDB_IMAGE", "pgvs3-duckdb:2.0.0")
DUCKDB_EXTENSION_VERSION = "v2.0.0"
DUCKDB_EXTENSION_PLATFORM = "linux_amd64"


def sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def compute_names(run_name):
    """Stable, unique names for the disposable compute scenario. The bucket,
    prefix and PostgreSQL metadata schema survive only for this run and are
    used again by the restarted container."""
    slug = re.sub(r"[^a-z0-9]+", "", Path(run_name).name.lower())[:24] or "run"
    return {
        "slug": slug,
        "bucket": f"compute-{slug}"[:63],
        "prefix": f"compute/{slug}/lake",
        "schema": f"compute_{slug}"[:63],
    }


def parse_s3_keys(body):
    root = ET.fromstring(body)
    return [node.text for node in root.findall("{*}Contents/{*}Key") if node.text]


def parse_s3_objects(body):
    """(key, size) pairs from a ListObjectsV2 response."""
    root = ET.fromstring(body)
    objects = []
    for node in root.findall("{*}Contents"):
        key = node.findtext("{*}Key")
        if key:
            objects.append((key, int(node.findtext("{*}Size") or 0)))
    return objects


def duckdb_client_config(extension_directory):
    """Offline, signed host-client configuration. The caller copies quack and
    httpfs plus their .info sidecars out of the image into this directory."""
    return {
        "extension_directory": str(extension_directory),
        "autoinstall_known_extensions": "false",
        "autoload_known_extensions": "false",
        "allow_unsigned_extensions": "false",
    }


def timing_summary(samples):
    values = [round(float(value), 3) for value in samples]
    if not values:
        return {"ms": [], "first_ms": None, "repeat_ms": [], "median_ms": None,
                "min_ms": None, "max_ms": None}
    return {
        "ms": values,
        "first_ms": values[0],
        "repeat_ms": values[1:],
        "median_ms": round(statistics.median(values), 3),
        "min_ms": min(values),
        "max_ms": max(values),
    }


def docker_inspect(target, template):
    return subprocess.check_output(["docker", "inspect", "--format", template, target],
                                   text=True, stderr=subprocess.PIPE, timeout=30).strip()


def command(*args):
    return subprocess.check_output([BIN, *args], text=True, stderr=subprocess.STDOUT, timeout=120).strip()


def request(base, path, method="GET", body=None, headers=None, want=200, timeout=120):
    try:
        response = urlopen(Request(base + path, data=body, method=method, headers=headers or {}), timeout=timeout)
    except HTTPError as error:
        response = error
    with response:
        data = response.read()
        assert response.status == want, (method, path, response.status, data[:500])
        return response.headers, data


def check_body(base, path, body, cache=None, span=None, etag=None):
    headers = {}
    expected = body
    if span:
        start, end = span
        headers["Range"] = f"bytes={start}-{end}"
        expected = body[start:end + 1]
    received, data = request(base, path, headers=headers, want=206 if span else 200)
    assert hashlib.sha256(data).digest() == hashlib.sha256(expected).digest(), path
    assert int(received["Content-Length"]) == len(expected), received
    if span:
        assert received["Content-Range"] == f"bytes {span[0]}-{span[1]}/{len(body)}", received
    if cache:
        assert received.get("X-Cache") == cache, received
    if etag:
        assert received["ETag"] == etag, received
    return received


@contextmanager
def gateway(label, database, read_only=False):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    with (RUN / f"{label}.log").open("wb") as log:
        args = [BIN, "serve", "--url", database, "--addr", f"127.0.0.1:{port}", "--pool-max", "8", "--log-level", "warn"]
        if read_only:
            args.append("--read-only")
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        try:
            def ready():
                assert process.poll() is None, f"{label} exited; see {log.name}"
                try:
                    return request(endpoint, "/readyz", timeout=2)[0] is not None
                except AssertionError:
                    return False
            eventually(f"{label} readiness", ready)
            yield endpoint
        finally:
            process.terminate()
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


class OriginProxy:
    def __init__(self, latency=0.0, keep_alive=False):
        """latency: seconds added before every forwarded request, standing in
        for an object store's time to first byte. keep_alive: reuse client
        connections, as an object store does, instead of closing each one."""
        self.latency = latency
        self.keep_alive = keep_alive
        self.writes_blocked = False
        self.counts = Counter()
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            disable_nagle_algorithm = True

            def log_message(self, *args):
                pass

            def serve(self):
                path = self.path.split("?", 1)[0]
                with owner.lock:
                    owner.counts[(self.command, path)] += 1
                    blocked = owner.writes_blocked and self.command in ("PUT", "POST", "DELETE")
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    chunks = []
                    while True:
                        size = int(self.rfile.readline().split(b";", 1)[0], 16)
                        if not size:
                            while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                                pass
                            break
                        chunks.append(self.rfile.read(size))
                        assert self.rfile.read(2) == b"\r\n"
                    body = b"".join(chunks)
                else:
                    body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if blocked:
                    status, headers, data = 503, {}, b"origin fixture temporarily unavailable"
                else:
                    if owner.latency:
                        time.sleep(owner.latency)
                    target = urlsplit(ORIGIN)
                    connection = HTTPConnection(target.hostname, target.port, timeout=120)
                    try:
                        headers = {key: value for key, value in self.headers.items() if key.lower() not in ("host", "connection", "content-length", "transfer-encoding", "expect")}
                        headers["Content-Length"] = str(len(body))
                        connection.request(self.command, self.path, body, headers)
                        response = connection.getresponse()
                        status, headers, data = response.status, dict(response.headers), response.read()
                    finally:
                        connection.close()
                self.send_response(status)
                for key, value in headers.items():
                    if key.lower() not in ("connection", "transfer-encoding", "content-length", "server", "date"):
                        self.send_header(key, value)
                length = headers.get("Content-Length", "0") if self.command == "HEAD" else str(len(data))
                self.send_header("Content-Length", length)
                if not owner.keep_alive:
                    self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    try:
                        self.wfile.write(data)
                    except (BrokenPipeError, ConnectionResetError):
                        pass

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = serve

        class Server(ThreadingHTTPServer):
            request_queue_size = 128

            def handle_error(self, request, client_address):
                if not isinstance(sys.exc_info()[1], ConnectionError):
                    super().handle_error(request, client_address)

        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def reads(self, bucket, key):
        path = f"/upstream/{bucket}/{key}"
        with self.lock:
            return sum(self.counts[(method, path)] for method in ("GET", "HEAD"))

    def reads_under(self, prefix):
        with self.lock:
            return sum(n for (method, path), n in self.counts.items() if method in ("GET", "HEAD") and path.startswith(prefix))

    def methods_under(self, prefix):
        with self.lock:
            return {method: sum(n for (m, path), n in self.counts.items()
                                if m == method and path.startswith(prefix))
                    for method in ("GET", "HEAD")}


def configure_origin(proxy, bucket, namespace, endpoints=(A, B), database=DATABASE, upstream="upstream", strip=False,
                     range_chunk=65536, extra=()):
    """strip: the cache bucket maps onto a same-named origin bucket with
    identical keys (strip_bucket), as when fronting an existing bucket."""
    if strip:
        upstream = bucket
    request(ORIGIN, f"/{upstream}", "PUT", b"")
    request(ORIGIN, f"/{upstream}/probe" if strip else f"/{upstream}/{bucket}/probe", "PUT", b"probe")
    config = RUN / f"{namespace}-origin.json"
    s3 = {"bucket": upstream, "endpoint": proxy.endpoint, "access_key": "test", "secret_key": "test", "bucket_lookup_type": "path"}
    if strip:
        s3["strip_bucket"] = True
    config.write_text(json.dumps({"type": "s3", "config": s3}))
    command("origin", "--url", database, "--bucket", bucket, "--cache-namespace", namespace,
            "--cache-ttl", "1h", "--range-cache-chunk", str(range_chunk), *extra, "set", str(config))
    for endpoint in endpoints:
        def visible():
            try:
                return request(endpoint, f"/{bucket}/probe")[1] == b"probe"
            except AssertionError:
                return False
        eventually(f"origin config on {endpoint}", visible)


def pending(namespace, database=DATABASE):
    return int(sql(f"SELECT count(*) FROM s3p.pending_uploads WHERE namespace='{namespace}'", database))


def readme_sql(heading):
    section = (ROOT / "README.md").read_text().split(heading + "\n", 1)[1]
    return re.search(r"(?ms)^```sql\n(.*?)^```", section).group(1)


def duckdb_session(endpoint, bucket):
    import duckdb
    connection = duckdb.connect()
    source = readme_sql("### Use from DuckDB 2.0+")
    source = source.replace("ENDPOINT '127.0.0.1:8014'", f"ENDPOINT '{endpoint[7:]}'").replace("s3://lake/", f"s3://{bucket}/")
    setup, _ = source.split("COPY (", 1)
    connection.execute(setup)
    connection.execute("SET enable_external_file_cache=false; SET parquet_metadata_cache=false")
    return connection, source


def parquet_fixture(name="events", rows=1000000):
    import duckdb
    path = RUN / f"{name}.parquet"
    source = readme_sql("### Use from DuckDB 2.0+")
    copy = "COPY (" + source.split("COPY (", 1)[1].split(";", 1)[0]
    copy = copy.replace("s3://lake/events.parquet", str(path)).replace("range(1000000)", f"range({rows})")
    with duckdb.connect() as connection:
        connection.execute(copy)
    data = path.read_bytes()
    assert 10 << 20 <= len(data) <= 1 << 30, len(data)
    return data


def multipart(endpoint, path, body):
    _, data = request(endpoint, path + "?uploads", "POST", b"")
    upload = ET.fromstring(data).findtext("{*}UploadId")
    assert upload, data
    parts = []
    middle = len(body) // 2
    for number, part in enumerate((body[:middle], body[middle:]), 1):
        headers, _ = request(endpoint, f"{path}?uploadId={quote(upload)}&partNumber={number}", "PUT", part)
        parts.append(f"<Part><PartNumber>{number}</PartNumber><ETag>{headers['ETag']}</ETag></Part>")
    complete = ("<CompleteMultipartUpload>" + "".join(parts) + "</CompleteMultipartUpload>").encode()
    _, data = request(endpoint, path + "?uploadId=" + quote(upload), "POST", complete)
    etag = ET.fromstring(data).findtext("{*}ETag")
    assert etag, data
    return etag


def replay(database, lsn):
    eventually(f"WAL replay through {lsn}", lambda: sql(f"SELECT coalesce(pg_last_wal_replay_lsn()>='{lsn}'::pg_lsn,false)", database) == "t")


def replication():
    sync = os.environ["PGVS3_TEST_SYNC_URL"]
    asynchronous = os.environ["PGVS3_TEST_ASYNC_URL"]
    restored = os.environ["PGVS3_TEST_RESTORE_URL"]
    app = os.environ["PGVS3_TEST_SYNC_APP"]
    evidence = {"complete": False, "events": [], "policy": {"synchronous_commit": "on", "synchronous_standby_names": f"FIRST 1 ({app})"}}

    def record(event, **values):
        evidence["events"].append({"event": event, **values})
        (RUN / "replication.json").write_text(json.dumps(evidence, indent=2) + "\n")

    assert sql("SELECT pg_is_in_recovery()", sync) == "t"
    assert sql("SELECT pg_is_in_recovery()", asynchronous) == "t"
    sql(f"ALTER SYSTEM SET synchronous_standby_names='FIRST 1 ({app})'")
    sql("SELECT pg_reload_conf()")
    eventually("synchronous standby selection", lambda: sql(f"SELECT count(*) FROM pg_stat_replication WHERE application_name='{app}' AND sync_state='sync'") == "1")
    record("physical topology", server=sql("SHOW server_version"),
           replication=json.loads(sql("SELECT coalesce(json_agg(json_build_object('application',application_name,'state',state,'sync',sync_state,'flush_lsn',flush_lsn,'replay_lsn',replay_lsn)),'[]') FROM pg_stat_replication")))
    body = parquet_fixture()
    large = os.urandom(1 << 20) * 80
    with OriginProxy() as proxy, ExitStack() as stack, ExitStack() as old_writers:
        peer = old_writers.enter_context(gateway("replica-peer", DATABASE))
        request(A, "/replica-proof", "PUT", b"")
        request(A, "/replica-proof/native.parquet", "PUT", body, {"Content-Type": "application/vnd.apache.parquet", "x-amz-meta-proof": "replicated"})
        request(A, "/replica-proof/sequential.bin", "PUT", large)
        native_etag = multipart(A, "/replica-proof/multipart.parquet", body)
        configure_origin(proxy, "replica-edge", "replica-origin", (A, peer))
        for key in ("whole.parquet", "ranges.parquet"):
            request(ORIGIN, f"/upstream/replica-edge/{key}", "PUT", body, {"Content-Type": "application/vnd.apache.parquet"})
        whole = check_body(A, "/replica-edge/whole.parquet", body)
        span = (len(body) - 65536, len(body) - 1)
        check_body(A, "/replica-edge/ranges.parquet", body, span=span)
        proxy.writes_blocked = True
        pending_headers, _ = request(A, "/replica-edge/pending.parquet", "PUT", body)
        pending_etag = multipart(peer, "/replica-edge/pending-multipart.parquet", body)
        assert pending("replica-origin") == 2
        assert sql("SELECT count(*) FROM \"s3w_replica-origin\".objects WHERE origin_etag LIKE 'pending:%' AND expires_at IS NULL") == "2"
        old_token = sql("SELECT token FROM s3p.pending_uploads WHERE namespace='replica-origin' AND key='replica-edge/pending.parquet'")
        lsn = sql("SELECT pg_current_wal_flush_lsn()")
        replay(sync, lsn)
        replay(asynchronous, lsn)
        reader = stack.enter_context(gateway("physical-reader", sync, read_only=True))
        origin_reads = {key: proxy.reads("replica-edge", key) for key in ("whole.parquet", "ranges.parquet", "pending.parquet", "pending-multipart.parquet")}
        native = check_body(reader, "/replica-proof/native.parquet", body)
        assert native["x-amz-meta-proof"] == "replicated" and native["Content-Type"] == "application/vnd.apache.parquet", native
        check_body(reader, "/replica-proof/sequential.bin", large)
        check_body(reader, "/replica-proof/multipart.parquet", body, etag=native_etag)
        check_body(reader, "/replica-edge/whole.parquet", body, cache="HIT", etag=whole["ETag"])
        check_body(reader, "/replica-edge/ranges.parquet", body, span=span, cache="HIT")
        check_body(reader, "/replica-edge/pending.parquet", body, cache="HIT", etag=pending_headers["ETag"])
        check_body(reader, "/replica-edge/pending-multipart.parquet", body, cache="HIT", etag=pending_etag)
        request(reader, "/replica-proof/rejected", "PUT", b"no", want=403)
        assert {key: proxy.reads("replica-edge", key) for key in origin_reads} == origin_reads
        record("standby reads", replay_lsn=sql("SELECT pg_last_wal_replay_lsn()", sync), barrier_lsn=lsn,
               parquet_bytes=len(body), sequential_bytes=len(large), multipart_etag=native_etag,
               pending_bodies=2, shared_hit_origin_requests=0)

        backup = RUN / "pending-backup.dump"
        with backup.open("wb") as output:
            subprocess.run(["docker", "exec", os.environ["PGVS3_TEST_PG_CONTAINER"], "pg_dump", "-U", "postgres", "-d", "pgvs3", "-Fc"], stdout=output, check=True)
        with backup.open("rb") as source, (RUN / "restore.log").open("wb") as log:
            subprocess.run(["docker", "exec", "-i", os.environ["PGVS3_TEST_RESTORE_CONTAINER"], "pg_restore", "-U", "postgres", "-d", "pgvs3_restored", "--single-transaction", "--exit-on-error"], stdin=source, stdout=log, stderr=subprocess.STDOUT, check=True)
        restored_reader = stack.enter_context(gateway("restored-reader", restored, read_only=True))
        check_body(restored_reader, "/replica-proof/native.parquet", body, etag=native["ETag"])
        check_body(restored_reader, "/replica-edge/pending.parquet", body, cache="HIT", etag=pending_headers["ETag"])
        check_body(restored_reader, "/replica-edge/pending-multipart.parquet", body, cache="HIT", etag=pending_etag)
        assert pending("replica-origin", restored) == 2
        record("consistent restore", dump_bytes=backup.stat().st_size, pending_bodies=2, doctor=command("doctor", "--url", restored))
        configure_origin(proxy, "replica-edge", "replica-origin", (restored_reader,), database=restored, upstream="restore-upstream")

        sql("ALTER SYSTEM SET primary_conninfo='host=127.0.0.1 port=1 user=postgres connect_timeout=1'", asynchronous)
        sql("SELECT pg_reload_conf()", asynchronous)
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_wal_receiver", asynchronous)
        eventually("asynchronous receiver disconnected", lambda: sql("SELECT count(*) FROM pg_stat_wal_receiver", asynchronous) == "0")
        async_lsn = sql("SELECT pg_last_wal_replay_lsn()", asynchronous)
        request(A, "/replica-proof/async-gap", "PUT", b"acknowledged with synchronous peer only")
        ack_lsn = sql("SELECT pg_current_wal_flush_lsn()")
        replay(sync, ack_lsn)
        assert sql(f"SELECT pg_last_wal_replay_lsn()<'{ack_lsn}'::pg_lsn", asynchronous) == "t"
        target = urlsplit(A)
        interrupted = HTTPConnection(target.hostname, target.port, timeout=30)
        interrupted.putrequest("PUT", "/replica-proof/interrupted.bin")
        interrupted.putheader("Content-Length", str(len(large)))
        interrupted.endheaders()
        interrupted.send(large[:8 << 20])
        eventually("in-flight COPY before primary loss", lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='pgvs3' AND state='active' AND query LIKE 'COPY %'") != "0")
        record("promotion boundary", acknowledged_lsn=ack_lsn, asynchronous_replay_lsn=async_lsn,
               old_pending_token=old_token, interrupted_copy="active, incomplete client body")
        subprocess.run(["docker", "kill", "--signal=KILL", os.environ["PGVS3_TEST_PG_CONTAINER"]], check=True, stdout=subprocess.DEVNULL)
        interrupted.close()
        os.kill(int(os.environ["PGVS3_TEST_PID_A"]), signal.SIGKILL)
        old_writers.close()
        assert sql("SELECT pg_promote(true,60)", sync) == "t"
        sql("ALTER SYSTEM SET synchronous_standby_names=''", sync)
        sql("SELECT pg_reload_conf()", sync)
        writer = stack.enter_context(gateway("promoted-writer", sync))
        second = stack.enter_context(gateway("promoted-peer", sync))
        check_body(writer, "/replica-proof/native.parquet", body, etag=native["ETag"])
        check_body(second, "/replica-proof/multipart.parquet", body, etag=native_etag)
        request(writer, "/replica-proof/interrupted.bin", want=404)
        assert request(writer, "/replica-proof/async-gap")[1] == b"acknowledged with synchronous peer only"
        check_body(writer, "/replica-edge/pending.parquet", body, cache="HIT", etag=pending_headers["ETag"])
        check_body(writer, "/replica-edge/pending-multipart.parquet", body, cache="HIT", etag=pending_etag)
        assert pending("replica-origin", sync) == 2
        sql("UPDATE s3p.pending_uploads SET lease_until=now()-interval '1 second' WHERE namespace='replica-origin'", sync)
        newer = parquet_fixture("newer", 1000001)
        newer_headers, _ = request(second, "/replica-edge/pending.parquet", "PUT", newer)
        assert sql(f"SELECT count(*) FROM s3p.pending_uploads WHERE namespace='replica-origin' AND token='{old_token}'", sync) == "0"
        record("synchronous promotion", server_in_recovery=sql("SELECT pg_is_in_recovery()", sync),
               pending_bodies=2, old_token_replaced=True, lease_expiry="injected after fencing old writer fleet")
        restored_writer = stack.enter_context(gateway("restored-writer", restored))
        sql("UPDATE s3p.pending_uploads SET lease_until=now()-interval '1 second' WHERE namespace='replica-origin'", restored)
        proxy.writes_blocked = False
        eventually("promoted outbox drain", lambda: pending("replica-origin", sync) == 0)
        eventually("restored outbox drain", lambda: pending("replica-origin", restored) == 0)
        check_body(second, "/replica-edge/pending.parquet", newer, etag=newer_headers["ETag"])
        check_body(ORIGIN, "/upstream/replica-edge/pending.parquet", newer)
        check_body(ORIGIN, "/upstream/replica-edge/pending-multipart.parquet", body)
        check_body(restored_writer, "/replica-edge/pending.parquet", body, etag=pending_headers["ETag"])
        check_body(ORIGIN, "/restore-upstream/replica-edge/pending.parquet", body)
        check_body(ORIGIN, "/restore-upstream/replica-edge/pending-multipart.parquet", body)
        request(writer, "/replica-edge/pending-multipart.parquet", "DELETE", want=204)
        request(second, "/replica-edge/pending-multipart.parquet", want=404)
        record("recovery and fences", origin_versions="newer PUT retained; deleted multipart not resurrected",
               restored_origin="isolated restore-upstream mirror", pending_bodies=0,
               reap=command("reap", "--url", sync, "--grace", "0s", "--all"), doctor=command("doctor", "--url", sync))
        sys.path.insert(0, str(ROOT / "deploy/bench"))
        from capacity import Capacity
        fixtures = SimpleNamespace(args=SimpleNamespace(psql=PSQL), endpoints={"after": second},
                                   sql=lambda side, statement: sql(statement, sync))
        snapshot = Capacity(fixtures, {}).snapshot("after")
        assert snapshot["metrics"]["pgvs3_spool_peak_bytes"] >= len(newer), snapshot
        assert snapshot["pending"]["rows"] == 0
        record("capacity SQL and telemetry", snapshot=snapshot)
        assert sql("SELECT pg_promote(true,60)", asynchronous) == "t"
        async_reader = stack.enter_context(gateway("async-promoted-reader", asynchronous, read_only=True))
        request(async_reader, "/replica-proof/async-gap", want=404)
        check_body(async_reader, "/replica-proof/native.parquet", body, etag=native["ETag"])
        check_body(async_reader, "/replica-edge/pending.parquet", body, cache="HIT", etag=pending_headers["ETag"])
        record("asynchronous gap", acknowledged_marker="absent, as expected for unreceived WAL", baseline_bytes="preserved",
               replay_lsn_before_promotion=async_lsn, acknowledged_lsn=ack_lsn)
    evidence["complete"] = True
    (RUN / "replication.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"replication": "passed", "events": evidence["events"]}), flush=True)


def analytics():
    groups = [(i, 10000) for i in range(5)]
    records = []
    request(A, "/lake", "PUT", b"")
    writer, source = duckdb_session(A, "lake")
    assert writer.execute(source[source.index("COPY ("):]).fetchall() == groups
    writer.close()
    _, body = request(B, "/lake/events.parquet")
    assert 10 << 20 <= len(body) <= 1 << 30, len(body)
    with OriginProxy() as proxy:
        configure_origin(proxy, "edge-lake", "analytical-origin")
        for key in ("cold.parquet", "ranges.parquet"):
            request(ORIGIN, f"/upstream/edge-lake/{key}", "PUT", body, {"Content-Type": "application/vnd.apache.parquet", "x-amz-meta-purpose": "analytical-contract"})
        cold = check_body(A, "/edge-lake/cold.parquet", body)
        reads = proxy.reads("edge-lake", "cold.parquet")
        check_body(B, "/edge-lake/cold.parquet", body, cache="HIT", etag=cold["ETag"])
        assert proxy.reads("edge-lake", "cold.parquet") == reads
        span = (len(body) - 65536, len(body) - 1)
        check_body(A, "/edge-lake/ranges.parquet", body, span=span)
        range_reads = proxy.reads("edge-lake", "ranges.parquet")
        check_body(B, "/edge-lake/ranges.parquet", body, span=span, cache="HIT")
        assert proxy.reads("edge-lake", "ranges.parquet") == range_reads
        for bucket, key, role in (("lake", "events.parquet", "native"), ("edge-lake", "cold.parquet", "origin-cache")):
            reader, _ = duckdb_session(B, bucket)
            result = reader.execute(f"SELECT group_id,count(*) FROM read_parquet('s3://{bucket}/{key}') GROUP BY group_id ORDER BY group_id LIMIT 5").fetchall()
            assert result == groups, result
            assert reader.execute(f"SELECT count(*),sum(id) FROM read_parquet('s3://{bucket}/{key}')").fetchone() == (1000000, 499999500000)
            reader.close()
            if role == "origin-cache":
                assert proxy.reads(bucket, key) == reads
            records.append({"role": role, "bytes": len(body), "rows": 1000000, "reader": "gateway B; DuckDB caches disabled", "sha256": hashlib.sha256(body).hexdigest()})
        proxy.writes_blocked = True
        start = time.monotonic()
        headers, _ = request(A, "/edge-lake/pending.parquet", "PUT", body)
        ack_seconds = time.monotonic() - start
        assert pending("analytical-origin") == 1
        assert sql("SELECT expires_at IS NULL FROM \"s3w_analytical-origin\".objects WHERE key='edge-lake/pending.parquet'") == "t"
        request(ORIGIN, "/upstream/edge-lake/pending.parquet", want=404)
        check_body(B, "/edge-lake/pending.parquet", body, cache="HIT", etag=headers["ETag"])
        proxy.writes_blocked = False
        drain_start = time.monotonic()
        eventually("analytical write-back drain", lambda: pending("analytical-origin") == 0)
        check_body(ORIGIN, "/upstream/edge-lake/pending.parquet", body)
        check_body(B, "/edge-lake/pending.parquet", body, cache="HIT", etag=headers["ETag"])
        records.append({"role": "origin-staging", "copy_of_record": "PostgreSQL pending body -> origin", "ack_seconds": ack_seconds,
                        "after_ack_drain_seconds": time.monotonic() - drain_start, "shared_range_origin_requests_after_warmup": 0})
        for bucket, catalog in (("lake", "ducklake_catalog"), ("edge-lake", "ducklake_origin_catalog")):
            if catalog != "ducklake_catalog":
                sql(f"CREATE DATABASE {catalog}")
            source = readme_sql("### Use with DuckLake (catalog and objects on one cluster)")
            source = source.replace("PORT 5432", f"PORT {os.environ['PGVS3_TEST_PG_PORT']}").replace("DATABASE 'ducklake_catalog'", f"DATABASE '{catalog}'").replace("s3://lake/", f"s3://{bucket}/")
            writer, _ = duckdb_session(A, bucket)
            result = writer.execute(source).fetchall()
            assert result == [(42, date(2026, 9, 1), 1.0)], result
            writer.close()
            reader, _ = duckdb_session(B, bucket)
            reader.execute(source.split("CREATE TABLE", 1)[0])
            assert reader.execute("SELECT * FROM lake.events").fetchall() == result
            reader.close()
            _, listing = request(B, f"/{bucket}?list-type=2&prefix=ducklake/")
            files = [node.text for node in ET.fromstring(listing).findall("{*}Contents/{*}Key") if node.text.endswith(".parquet")]
            assert files, f"DuckLake {bucket} stored no object files"
            records.append({"role": bucket, "ducklake_catalog": catalog, "catalog_object_transaction": "separate",
                            "reconnected_result": [42, "2026-09-01", 1.0], "table_object_keys": files})
    (RUN / "analytics.json").write_text(json.dumps({"complete": True, "records": records}, indent=2) + "\n")
    print(f"native: {len(body)}-byte Parquet, 1,000,000 rows; query through gateway B with client caches disabled")
    print("origin-cache: shared whole-object and aligned footer hits; zero origin requests after warmup")
    print(f"origin-staging: PostgreSQL-only pending body -> origin; ACK {ack_seconds:.3f}s, after-ACK drain {records[2]['after_ack_drain_seconds']:.3f}s")
    print("DuckLake: native and origin-backed tables reopened through gateway B; catalog/object commits remain separate", flush=True)


def guides():
    import duckdb
    from documentation import GUIDES, check_links, snippets

    assert RUN.resolve().parent == (ROOT / ".tmp/verify").resolve(), "guide mutations require the disposable harness"
    assert urlsplit(DATABASE).hostname == "127.0.0.1"
    assert not (RUN / "guides.json").exists(), "never overwrite an earlier guide trial"
    blocks = snippets()
    evidence = {"complete": False, "checks": [], "snippets": {}, "links": check_links(),
                "postgres": sql("SHOW server_version"), "duckdb": duckdb.__version__,
                "scope": "functional snippets/failure windows, not performance acceptance or production isolation",
                "gateway_sha256": hashlib.sha256(Path(BIN).read_bytes()).hexdigest(),
                "documents": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in GUIDES.glob("*.md") if path.name != "validation.md"},
                "tools": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                          for name in ("test/data_plane.py", "test/documentation.py", "test/harness.sh", "test/e2e.sh",
                                       "test/loadgen/main.go", "deploy/bench/capacity.py", "deploy/bench/stack.sh", "justfile")}}
    env = dict(os.environ, PGVS3_URL=DATABASE, PGVS3_ENDPOINT=A, PGVS3_ORIGIN_ENDPOINT=ORIGIN,
               PSQL=PSQL, PATH=str(RUN) + os.pathsep + os.environ["PATH"], PGVS3_GUIDE_DURATION="2s")

    def save():
        (RUN / "guides.json").write_text(json.dumps(evidence, indent=2, default=str) + "\n")

    def record(name, **values):
        evidence["checks"].append({"name": name, **values})
        save()
        print(f"guides: {name}", flush=True)

    def source(name, language):
        block = blocks[name]
        assert block["language"] == language, name
        row = evidence["snippets"].setdefault(name, {key: block[key] for key in ("path", "language", "sha256")} | {"uses": 0})
        row["uses"] += 1
        save()
        return block["source"]

    def pg(name, database=DATABASE):
        output = sql(source(name, "sql"), database)
        (RUN / f"guide-{name}-{evidence['snippets'][name]['uses']}.log").write_text(output + "\n")
        return output

    def script(name, language="sh", endpoint=A, want=0):
        code = source(name, language)
        argv = ["bash", "-euc", code] if language == "sh" else [sys.executable, "-c", code]
        result = subprocess.run(argv, cwd=RUN, env=env | {"PGVS3_ENDPOINT": endpoint},
                                capture_output=True, text=True, timeout=90)
        log = RUN / f"guide-{name}-{evidence['snippets'][name]['uses']}.log"
        log.write_text(result.stdout + result.stderr)
        assert result.returncode == want, (name, result.returncode, f"see {log}")
        return result.stdout

    def api(endpoint, path, body=None, want=200, method=None):
        method = method or ("GET" if body is None else "POST")
        _, data = request(endpoint, "/ojs/v1" + path, method,
                          None if body is None else json.dumps(body).encode(),
                          {"Content-Type": "application/openjobspec+json"}, want=want)
        return json.loads(data) if data else None

    def keys(prefix):
        _, data = request(B, "/guide-analytics?list-type=2&prefix=" + quote(prefix))
        return sorted(node.text for node in ET.fromstring(data).findall("{*}Contents/{*}Key"))

    def catalog_url():
        return urlsplit(DATABASE)._replace(path="/ducklake_catalog").geturl()

    def candidate_sql(code, endpoint):
        return code.replace("ENDPOINT '127.0.0.1:8014'", f"ENDPOINT '{endpoint[7:]}'").replace(
            "PORT 5432", f"PORT {os.environ['PGVS3_TEST_PG_PORT']}").replace("DATABASE 'pgvs3_bench'", "DATABASE 'pgvs3'")

    save()
    try:
        assert sql("SELECT to_regnamespace('guide_app') IS NULL") == "t", "refuse existing application demo data"
        assert sql("SELECT count(*) FROM s3p.namespaces WHERE name LIKE 'guide-%'") == "0", "refuse existing demo namespaces"
        sql("CREATE EXTENSION pg_stat_statements")
        pg("app-fixture")
        query = source("oltp-plan", "sql").split("\n", 1)[1]
        before_rows = sql(query)
        before = json.loads(pg("oltp-plan"))[0]
        pg("oltp-index")
        after = json.loads(pg("oltp-plan"))[0]
        assert sql(query) == before_rows
        assert [int(line.split("|", 1)[0]) for line in before_rows.splitlines()] == list(range(990042, 800041, -10000))
        assert "orders_customer_recent" in json.dumps(after)
        buffers = lambda result: sum(result["Plan"].get(key, 0) for key in ("Shared Hit Blocks", "Shared Read Blocks"))
        assert buffers(after) < buffers(before)
        settings = "SELECT json_build_object('work_mem',current_setting('work_mem'),'parallel',current_setting('max_parallel_workers_per_gather'));"
        scoped = sql(settings + source("local-budgets", "sql") + settings).splitlines()
        assert scoped[0] == scoped[-1] and "1000000|4999500000" in scoped
        record("OLTP index and transaction-local budgets", rows=20, index="orders_customer_recent",
               control_buffers=buffers(before), candidate_buffers=buffers(after),
               control_plan=before, candidate_plan=after, settings_restored=json.loads(scoped[-1]))

        request(A, "/guide-analytics", "PUT", b"")
        with duckdb.connect() as writer:
            writer.execute(candidate_sql(source("extract-setup", "sql"), A))
            writer.execute(source("extract-copy", "sql"))
            evidence["extensions"] = writer.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded ORDER BY 1").fetchall()
        _, body = request(B, "/guide-analytics/orders/snapshot-v1.parquet")
        assert 10 << 20 <= len(body) <= 1 << 30, len(body)
        reader, _ = duckdb_session(B, "guide-analytics")
        try:
            assert reader.execute(source("extract-check", "sql")).fetchone() == (1000000, 499999500000, 4999500000)
            assert reader.execute("SELECT sum(length(payload)) FROM read_parquet('s3://guide-analytics/orders/snapshot-v1.parquet')").fetchone() == (32000000,)
        finally:
            reader.close()
        script("extract-manifest")
        script("extract-manifest", want=22)
        manifest = json.loads(request(B, "/guide-analytics/current.json")[1])
        assert manifest["generation"] == "v1" and manifest["rows"] == 1000000
        sql("UPDATE guide_app.orders SET amount_cents=amount_cents+1 WHERE id=42; DELETE FROM guide_app.orders WHERE id=43")
        reader, _ = duckdb_session(B, "guide-analytics")
        try:
            assert reader.execute(source("extract-check", "sql")).fetchone() == (1000000, 499999500000, 4999500000)
        finally:
            reader.close()
        assert sql("SELECT count(*),sum(amount_cents) FROM guide_app.orders") == "999999|4999499958"
        record("PostgreSQL extract, immutable generation and conditional discovery", rows=1000000, bytes=len(body),
               sha256=hashlib.sha256(body).hexdigest(), manifest_repeat_status=412,
               source_update_and_delete="old extract unchanged; not CDC", client_caches="external file + Parquet metadata disabled")

        probe = RUN / "probe.sql"
        probe.write_text(source("oltp-canary", "sql"))
        container = os.environ["PGVS3_TEST_PG_CONTAINER"]
        subprocess.run(["docker", "exec", container, "mkdir", "/tmp/guide-canary"], check=True)
        subprocess.run(["docker", "cp", str(probe), container + ":/tmp/guide-canary/probe.sql"], check=True)
        subprocess.run(["mise", "exec", "go", "--", "go", "build", "-o", str(RUN / "loadgen"), "./test/loadgen"],
                       cwd=ROOT, check=True, capture_output=True, timeout=120)
        evidence["loadgen_sha256"] = hashlib.sha256((RUN / "loadgen").read_bytes()).hexdigest()
        evidence["pgbench"] = subprocess.check_output(["docker", "exec", container, "pgbench", "--version"], text=True, timeout=10).strip()
        (RUN / "orders.parquet").write_bytes(body)
        (RUN / "canary").mkdir()
        canaries = {}
        for label in ("control", "mixed"):
            args = ["docker", "exec", container, "pgbench", "-U", "postgres", "-d", "pgvs3", "-n", "-M", "prepared",
                    "-c", "2", "-j", "2", "-T", "2", "-l", "--log-prefix", "/tmp/guide-canary/" + label,
                    "-f", "/tmp/guide-canary/probe.sql"]
            with (RUN / ("guide-pgbench-" + label + ".log")).open("w") as output:
                process = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT)
                try:
                    if label == "mixed":
                        assert process.poll() is None
                        measured = json.loads(script("mixed-object-load"))
                        assert measured["errors"] == 0 and measured["requests"] > 0
                    assert process.wait(timeout=30) == 0
                finally:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=10)
                    copied = subprocess.run(["docker", "cp", container + ":/tmp/guide-canary/.", str(RUN / "canary")], capture_output=True)
                    assert copied.returncode == 0, "retain raw pgbench logs, including failed trials"
        for label in ("control", "mixed"):
            rows = [line.split() for path in (RUN / "canary").glob(label + ".*") for line in path.read_text().splitlines()]
            assert len(rows) > 20 and all(len(row) >= 6 and row[2].isdigit() for row in rows)
            canaries[label] = {"logged_transactions": len(rows), "failed_or_skipped": 0, "latency_unit": "microseconds, column 3"}
        record("same-cluster pgbench + existing object loadgen canary", pgbench=canaries, object_load=measured,
               duration_seconds=2, performance_gates_evaluated=False,
               scope="tooling/concurrency smoke on one fixture; not application tail-latency isolation")

        lake_source = candidate_sql(source("lake-options", "sql"), A)
        writer, _ = duckdb_session(A, "guide-analytics")
        try:
            options = writer.execute(lake_source).fetchall()
            configured = {row[0]: row[2] for row in options}
            assert {key: configured[key] for key in ("parquet_compression", "parquet_row_group_size", "target_file_size", "data_inlining_row_limit")} == {
                "parquet_compression": "zstd", "parquet_row_group_size": "122880", "target_file_size": "256000000", "data_inlining_row_limit": "10"}
            evidence["extensions"] = []
            for name, version, path in writer.execute("SELECT extension_name,extension_version,install_path FROM duckdb_extensions() WHERE loaded ORDER BY 1").fetchall():
                artifact = {"name": name, "version": version}
                if path and Path(path).is_file():
                    with Path(path).open("rb") as library:
                        artifact["sha256"] = hashlib.file_digest(library, "sha256").hexdigest()
                evidence["extensions"].append(artifact)
            parquet = keys("ducklake/")
            assert parquet
            layout = writer.execute("SELECT DISTINCT compression,row_group_num_rows FROM parquet_metadata(?)", ["s3://guide-analytics/" + parquet[0]]).fetchall()
            assert all(codec == "ZSTD" and 0 < rows <= 122880 for codec, rows in layout), layout
            inline = source("lake-inlining", "sql")
            create, flush = inline.split("CALL ducklake_flush_inlined_data", 1)
            before_keys = keys("ducklake/")
            assert writer.execute(create).fetchall() == [(42,)]
            assert keys("ducklake/") == before_keys, "tiny insert should be catalog-only"
            writer.execute("CALL ducklake_flush_inlined_data" + flush)
            assert len(keys("ducklake/")) > len(before_keys)
            assert writer.execute("SELECT * FROM lake.inline_probe").fetchall() == [(42,)]
            spatio_sql = source("lake-spatiotemporal", "sql")
            assert writer.execute(spatio_sql).fetchall() == [(3240, 256, 72)]
        finally:
            writer.close()
        assert sql("SELECT count(*) FILTER (WHERE min_value IS NOT NULL OR max_value IS NOT NULL) FROM guide_lake.ducklake_file_column_stats s JOIN guide_lake.ducklake_column c USING (table_id, column_id) WHERE c.column_name='wide_payload'", catalog_url()) == "0"
        reader, _ = duckdb_session(B, "guide-analytics")
        try:
            reader.execute(candidate_sql(lake_source.split("CALL lake.set_option", 1)[0], B))
            assert reader.execute("SELECT count(*),sum(id),sum(length(payload)) FROM lake.orders").fetchone() == (1000000, 499999500000, 32000000)
            assert reader.execute("SELECT * FROM lake.inline_probe").fetchall() == [(42,)]
            assert reader.execute("WITH matched AS (" + spatio_sql.split("WITH matched AS (", 1)[1]).fetchone() == (3240, 256, 72)
        finally:
            reader.close()
        record("DuckLake candidate syntax, skewed-hotspot 32-bit curve sort / hash-32 compaction and catalog reopen", options=options,
               parquet_codec_row_groups=layout, inlining="no new object until explicit flush",
               skewed_curve_and_hash32=[3240, 256, 72], wide_payload_minmax_stats_skipped=True, table_rows=1000000)

        with OriginProxy() as proxy:
            script("cache-origin-seed")
            config = json.loads(source("cache-origin-config", "json"))
            config["config"]["endpoint"] = proxy.endpoint
            (RUN / "origin.json").write_text(json.dumps(config))
            script("cache-origin-attach")
            request(ORIGIN, "/upstream/guide-edge/probe", "PUT", b"guide probe")
            for endpoint in (A, B):
                def visible():
                    try:
                        return request(endpoint, "/guide-edge/probe")[1] == b"guide probe"
                    except AssertionError:
                        return False
                eventually("guide origin discovery", visible)
            assert script("cache-read").rstrip().endswith("version-one")
            reads = proxy.reads("guide-edge", "version.txt")
            warm = script("cache-read", endpoint=B)
            assert "x-cache: hit" in warm.lower() and warm.rstrip().endswith("version-one")
            assert proxy.reads("guide-edge", "version.txt") == reads
            request(ORIGIN, "/upstream/guide-edge/ranges.parquet", "PUT", body)
            span = (len(body) - 65536, len(body) - 1)
            check_body(A, "/guide-edge/ranges.parquet", body, span=span)
            range_reads = proxy.reads("guide-edge", "ranges.parquet")
            check_body(B, "/guide-edge/ranges.parquet", body, span=span, cache="HIT")
            assert proxy.reads("guide-edge", "ranges.parquet") == range_reads
            with gateway("guide-read-only", DATABASE, read_only=True) as read_only:
                check_body(read_only, "/guide-edge/version.txt", b"version-one", cache="HIT")
                assert proxy.reads("guide-edge", "version.txt") == reads
                request(read_only, "/guide-analytics/rejected", "PUT", b"no", want=403)
                api(read_only, "/workers/fetch", {"queues": ["guide-jobs"]}, want=403)
            script("cache-external-change")
            assert script("cache-read", endpoint=B).rstrip().endswith("version-one")
            script("cache-revalidate")
            eventually("guide TTL=0 revalidation", lambda: script("cache-read", endpoint=B).rstrip().endswith("version-two"))
            record("shared hits, read-only guard and mutable-origin freshness", whole_hit_extra_origin_reads=0,
                   range_hit_extra_origin_reads=0, stale_inside_ttl=True, revalidated="version-two",
                   read_only_scope="explicit read-only gateway on primary; physical standby proof is just fault")

            proxy.writes_blocked = True
            headers, _ = request(A, "/guide-edge/pending.parquet", "PUT", body)
            assert pending("guide-origin") == 1
            request(ORIGIN, "/upstream/guide-edge/pending.parquet", want=404)
            check_body(B, "/guide-edge/pending.parquet", body, cache="HIT", etag=headers["ETag"])
            assert sql("SELECT expires_at IS NULL FROM \"s3w_guide-origin\".objects WHERE key='guide-edge/pending.parquet'") == "t"
            assert "guide-origin|1|" in pg("pending-diagnostics")
            script("demo-cleanup-namespaces", want=1)
            assert pending("guide-origin") == 1 and "guide-analytics" in sql("SELECT name FROM s3p.namespaces")
            sys.path.insert(0, str(ROOT / "deploy/bench"))
            from capacity import Capacity
            fixtures = SimpleNamespace(args=SimpleNamespace(psql=PSQL), endpoints={"after": A}, sql=lambda side, statement: sql(statement))
            snapshot = Capacity(fixtures, {}).snapshot("after")
            assert snapshot["pending"]["rows"] == 1 and snapshot["metrics"]["pgvs3_pending_uploads_bytes"] == len(body)
            proxy.writes_blocked = False
            eventually("guide pending body drain", lambda: pending("guide-origin") == 0)
            check_body(ORIGIN, "/upstream/guide-edge/pending.parquet", body)
            check_body(B, "/guide-edge/pending.parquet", body, etag=headers["ETag"])
            record("origin outage, pending-body protection and verified drain", pending_body_nonexpiring=True,
                   cleanup_refused_pending=True, bytes=len(body), public_etag=headers["ETag"], capacity_while_pending=snapshot)

        pg("outbox-schema")
        intent = source("outbox-commit", "sql")
        sql(intent.replace("COMMIT;", "ROLLBACK;"))
        event_id = "018f0000-0000-7000-8000-000000000042"
        assert sql("SELECT paid FROM guide_app.orders WHERE id=42") == "f"
        assert sql("SELECT count(*) FROM guide_app.outbox") == "0"
        api(A, "/jobs/" + event_id, want=404)
        sql(intent)
        assert sql("SELECT paid FROM guide_app.orders WHERE id=42") == "t"
        api(B, "/jobs/" + event_id, want=404)
        job = json.loads(sql("SELECT job FROM guide_app.outbox"))
        api(A, "/jobs", job, want=201)
        output = json.loads(script("outbox-relay", "python", endpoint=B))
        assert output["published"] == event_id
        assert sql("SELECT published_at IS NOT NULL FROM guide_app.outbox") == "t"
        api(A, "/jobs", {"type": "invalid.args", "args": {}, "queue": "guide-jobs"}, want=400)
        claim = json.loads(source("job-fetch", "json"))
        first = api(A, "/workers/fetch", claim)["jobs"]
        assert len(first) == 1 and first[0]["id"] == event_id
        pg("job-effect")
        api(A, "/workers/heartbeat", {"worker_id": "guide-worker", "active_jobs": [event_id],
                                    "lease_tokens": {event_id: first[0]["lease_token"]}, "visibility_timeout_ms": 1})
        time.sleep(0.02)
        second = api(B, "/workers/fetch", claim)["jobs"][0]
        assert second["id"] == event_id and second["lease_token"] != first[0]["lease_token"] and second["attempt"] == 2
        stale = api(A, "/workers/ack", {"job_id": event_id, "lease_token": first[0]["lease_token"]}, want=409)
        assert stale["error"]["code"] == "stale_lease"
        pg("job-effect")
        assert sql("SELECT fulfillment_count FROM guide_app.orders WHERE id=42") == "1"
        api(B, "/workers/ack", {"job_id": event_id, "lease_token": second["lease_token"], "result": {"applied": True}})
        record("outbox rollback, ambiguous enqueue and effect-before-ACK redelivery", enqueue_reconciliation="POST 409 + GET envelope match",
               old_lease_status=409, effect_count=1, receipt_count=1, args_object_status=400)

        collision = job | {"id": "018f0000-0000-7000-8000-000000000099", "args": [{"version": 1, "order_id": 99}]}
        encoded = json.dumps(collision).replace("'", "''")
        sql(f"INSERT INTO guide_app.outbox(event_id,job) VALUES ('{collision['id']}','{encoded}'::jsonb)")
        api(A, "/jobs", collision | {"args": [{"version": 99, "order_id": 99}]}, want=201)
        script("outbox-relay", "python", want=1)
        assert sql(f"SELECT published_at IS NULL FROM guide_app.outbox WHERE event_id='{collision['id']}'") == "t"
        api(A, "/jobs/" + collision["id"], method="DELETE")
        record("relay rejects a mismatched retained UUID", published=False, failure="payload mismatch, not idempotent enqueue success")

        due = api(A, "/jobs", {"type": "guide.scheduled", "args": [], "queue": "guide-jobs", "run_at": "+200ms"}, want=201)["job"]
        assert due["state"] == "scheduled"
        start = time.monotonic()
        scheduled = api(B, "/workers/fetch", claim)["jobs"][0]
        scheduled_seconds = time.monotonic() - start
        assert scheduled["id"] == due["id"] and scheduled_seconds < 5
        api(B, "/workers/nack", {"job_id": scheduled["id"], "lease_token": scheduled["lease_token"], "error": {"retryable": True}})
        retried = api(A, "/workers/fetch", claim)["jobs"][0]
        assert retried["id"] == scheduled["id"] and retried["attempt"] == 2
        api(A, "/workers/ack", {"job_id": retried["id"], "lease_token": retried["lease_token"]})
        sql(f"UPDATE \"s3q_guide-jobs\".jobs SET completed_at=now()-interval '25 hours' WHERE id='{event_id}' AND state='completed'")
        def pruned():
            command("reap", "--url", DATABASE, "--all")
            return sql(f"SELECT count(*) FROM s3p.job_index WHERE id='{event_id}'") == "0"
        eventually("guide terminal retention after maintenance-lock contention", pruned)
        api(A, "/jobs/" + event_id, want=404)
        api(B, "/jobs", job, want=201)
        reused = api(A, "/workers/fetch", claim)["jobs"][0]
        assert reused["id"] == event_id
        pg("job-effect")
        assert sql("SELECT fulfillment_count,(SELECT count(*) FROM guide_app.receipts) FROM guide_app.orders WHERE id=42") == "1|1"
        api(A, "/workers/ack", {"job_id": event_id, "lease_token": reused["lease_token"]})
        record("lazy scheduled/retry claims and retention-bounded ID reuse", scheduled_without_forced_maintenance=True,
               scheduled_fetch_seconds=scheduled_seconds,
               retry_attempt=2, prune_policy="normal 24h retention; fixture advances only its own terminal row's completed_at by 25h",
               reused_id_status=201, effect_count_after_reuse=1)
        stream = json.loads(script("stream-roundtrip", "python"))
        assert stream == {"processed": 2, "redelivered_same_batch": True}
        assert api(B, "/queues/guide-events/receive", {"consumer": "audit", "max": 100})["count"] == 0
        record("snapshot stream ceiling, redelivery and ACK", too_small_status=413, **stream)

        script("health-diagnostics")
        for name in ("settings-diagnostics", "activity-diagnostics", "statements-diagnostics", "storage-diagnostics", "replication-diagnostics"):
            pg(name)
        metrics = request(A, "/metrics")[1].decode()
        for name in ("pgvs3_pg_pool_empty_acquire_wait_seconds_total", "pgvs3_pg_pool_empty_acquire_count_total",
                     "pgvs3_chunk_pool_empty_acquire_wait_seconds_total", "pgvs3_s3_admission_waiting_requests",
                     "pgvs3_pending_uploads_bytes", "pgvs3_pending_uploads_oldest_seconds", "pgvs3_spool_peak_bytes"):
            assert re.search(r"^" + name + r" [0-9.eE+-]+$", metrics, re.M), name
        help_result = subprocess.run([BIN, "serve", "--help"], capture_output=True, text=True, timeout=30)
        assert help_result.returncode == 0 and "s3-queue-wait" in help_result.stderr and "default 1s" in help_result.stderr, (
            help_result.returncode, help_result.stderr)
        record("PostgreSQL 18 diagnostics, optional pg_stat_statements and actual metric/flag names", metrics_checked=7)
        script("demo-cleanup-namespaces")
        pg("demo-cleanup-catalog", catalog_url())
        pg("demo-cleanup-app")
        assert sql("SELECT to_regnamespace('guide_app') IS NULL") == "t"
        assert sql("SELECT count(*) FROM s3p.namespaces WHERE name LIKE 'guide-%'") == "0"
        assert blocks.keys() == evidence["snippets"].keys(), blocks.keys() - evidence["snippets"].keys()
        record("guarded owned-demo cleanup", namespace_deletions="only named guide-* / guide_app / guide_lake demo data", snippets=len(blocks))
        evidence["complete"] = True
        save()
        print(f"guides: {len(blocks)} bound snippets, {len(evidence['checks'])} scenario checks passed; evidence: {RUN / 'guides.json'}", flush=True)
    except BaseException as error:
        message = re.sub(r"postgres(?:ql)?://[^/\s]+@", "postgres://<redacted>@", str(error))
        evidence["error"] = {"type": type(error).__name__, "message": message}
        save()
        raise


RAQUET_POINTS = ((-3.7038, 40.4168), (-3.9, 40.2), (-3.5, 40.6), (-9.0, 37.0))
RAQUET_REGION = "'POLYGON((-4 40, -3.5 40, -3.5 40.5, -4 40.5, -4 40))'::GEOMETRY"


def raquet_queries(url):
    """(name, sql) pairs: sparse point lookups, one region and a full scan."""
    points = [(f"point {lon},{lat}", f"SELECT ST_RasterValue(block, band_1, ST_Point({lon}, {lat}), metadata) "
                                     f"FROM read_raquet_at('{url}', {lon}, {lat})") for lon, lat in RAQUET_POINTS]
    return points + [
        ("region", f"SELECT unnest(ST_RegionStats(band_1, block, {RAQUET_REGION}, metadata)) FROM read_raquet('{url}', {RAQUET_REGION})"),
        ("scan", f"SELECT count(*), sum((ST_RasterSummaryStats(band_1, metadata)).sum) FROM read_raquet('{url}')"),
    ]


def raquet_run(connection, queries):
    """Results and HTTP GET count/bytes per query, from DuckDB's HTTP log."""
    out = {}
    connection.execute("CALL enable_logging('HTTP')")
    for name, query in queries:
        connection.execute("CALL truncate_duckdb_logs()")
        rows = connection.execute(query).fetchall()
        gets, received, statuses = connection.execute(
            "SELECT count(*), coalesce(sum(TRY_CAST(response.headers['Content-Length'] AS BIGINT)), 0), "
            "histogram(response.status::VARCHAR) "
            "FROM duckdb_logs_parsed('HTTP') WHERE request.type = 'GET'").fetchone()
        out[name] = {"rows": rows, "gets": gets, "bytes": received, "statuses": dict(statuses or {})}
    return out


def raquet():
    import duckdb
    import raquet_fixture
    path = RUN / "raster.parquet"
    metadata = raquet_fixture.write(path, count=8, row_group=4)
    body = path.read_bytes()
    with duckdb.connect() as local:
        local.execute("INSTALL raquet FROM community; LOAD raquet")
        oracle = {name: local.execute(query).fetchall() for name, query in raquet_queries(str(path))}
    for (lon, lat), (name, _) in zip(RAQUET_POINTS, raquet_queries("")):
        assert oracle[name] == [(raquet_fixture.value(lon, lat),)], (name, oracle[name])
    pixels = metadata["width"] * metadata["height"]
    assert oracle["scan"] == [(metadata["tiling"]["num_blocks"], metadata["bands"][0]["STATISTICS_MEAN"] * pixels)], oracle["scan"]

    def session(endpoint, bucket):
        connection, _ = duckdb_session(endpoint, bucket)
        connection.execute("INSTALL raquet FROM community; LOAD raquet")
        return connection

    def check(role, endpoint, bucket, key):
        with session(endpoint, bucket) as connection:
            observed = raquet_run(connection, raquet_queries(f"s3://{bucket}/{key}"))
        for name, result in observed.items():
            assert result["rows"] == oracle[name], (role, name, result["rows"], oracle[name])
        for name, _ in raquet_queries("")[:len(RAQUET_POINTS)]:
            assert 0 < observed[name]["bytes"] < len(body) // 4, (role, name, observed[name], len(body))
        return observed

    records = []
    request(A, "/raster", "PUT", b"")
    request(A, "/raster/raster.parquet", "PUT", body, {"Content-Type": "application/vnd.apache.parquet"})
    native = check("native", B, "raster", "raster.parquet")
    records.append({"role": "native", "queries": native})
    with OriginProxy() as proxy:
        configure_origin(proxy, "edge-raster", "raster-origin")
        request(ORIGIN, "/upstream/edge-raster/raster.parquet", "PUT", body, {"Content-Type": "application/vnd.apache.parquet"})
        cold = check("origin-cache cold", A, "edge-raster", "raster.parquet")
        reads = proxy.reads("edge-raster", "raster.parquet")
        assert reads > 0
        warm = check("origin-cache warm", B, "edge-raster", "raster.parquet")
        assert proxy.reads("edge-raster", "raster.parquet") == reads, "gateway B re-read the origin"
        for name in native:
            sequence = [(role[name]["gets"], role[name]["bytes"]) for role in (native, cold, warm)]
            assert len(set(sequence)) == 1, ("cache tier changed the client's ranged reads", name, sequence)
        records.append({"role": "origin-cache", "origin_reads_cold": reads, "origin_reads_after_warm": 0, "queries": warm})
        request(A, "/edge-raster/written.parquet", "PUT", body, {"Content-Type": "application/vnd.apache.parquet"})
        written = check("write-through", B, "edge-raster", "written.parquet")
        assert proxy.reads("edge-raster", "written.parquet") == 0, "write-through object was pulled from the origin"
        eventually("raquet write-back drain", lambda: pending("raster-origin") == 0)
        check_body(ORIGIN, "/upstream/edge-raster/written.parquet", body)
        records.append({"role": "write-through", "origin_reads": 0, "queries": written})
    point = native[raquet_queries("")[0][0]]
    (RUN / "raquet.json").write_text(json.dumps({"complete": True, "bytes": len(body), "tiling": metadata["tiling"],
                                                  "records": records}, indent=2, default=str) + "\n")
    print(f"raquet: {len(body)}-byte raster, {metadata['tiling']['num_blocks']} tiles; point lookup {point['gets']} GETs / "
          f"{point['bytes']} bytes; native, origin-cache (zero origin reads on B) and write-through match the local oracle", flush=True)


def check_file(base, path, digest, size):
    """Stream an object and compare it with a local file's SHA-256."""
    with urlopen(Request(base + path), timeout=600) as response:
        assert response.status == 200 and int(response.headers["Content-Length"]) == size, (path, response.headers)
        received = hashlib.sha256()
        while chunk := response.read(1 << 20):
            received.update(chunk)
    assert received.hexdigest() == digest, path


def ortho():
    """The RaQuet queries on a real multi-GB orthophoto (test/raquet_ortho.py)."""
    import duckdb
    import raquet_ortho
    sheets = int(os.environ.get("PGVS3_ORTHO_SHEETS", "4"))
    path = raquet_ortho.build(sheets)
    meta, size = raquet_ortho.metadata(path), path.stat().st_size
    digest = raquet_ortho.sha256(path)
    queries = lambda url: raquet_ortho.queries(url, meta)
    point_names = [name for name, _ in queries("") if name.startswith(("point", "overview"))]
    with duckdb.connect() as local:
        local.execute("INSTALL raquet FROM community; LOAD raquet")
        oracle = {name: local.execute(query).fetchall() for name, query in queries(str(path))}
    assert all(len(oracle[name]) == 1 and None not in oracle[name][0] for name in point_names), oracle

    def check(role, endpoint, bucket, key, proxy=None):
        """Every query matches the local oracle; returns per-query GETs,
        bytes and (with a proxy) origin reads."""
        connection, _ = duckdb_session(endpoint, bucket)
        observed = {}
        with connection:
            connection.execute("INSTALL raquet FROM community; LOAD raquet")
            for name, query in queries(f"s3://{bucket}/{key}"):
                before = proxy.reads(bucket, key) if proxy else 0
                observed[name] = raquet_run(connection, [(name, query)])[name]
                observed[name]["origin_reads"] = (proxy.reads(bucket, key) - before) if proxy else 0
        for name, result in observed.items():
            assert raquet_ortho.same(result["rows"], oracle[name]), (role, name, result["rows"], oracle[name])
            del result["rows"]
        for name in point_names:
            assert 0 < observed[name]["bytes"] < size // 20, (role, name, observed[name], size)
        return observed

    records = []
    started = time.monotonic()
    request(A, "/ortho", "PUT", b"")
    raquet_ortho.upload(A, "/ortho/ortho.parquet", path)
    check_file(B, "/ortho/ortho.parquet", digest, size)
    records.append({"role": "native", "upload_and_verify_s": round(time.monotonic() - started, 1),
                    "queries": check("native", B, "ortho", "ortho.parquet")})
    with OriginProxy() as proxy:
        configure_origin(proxy, "edge-ortho", "ortho-origin")
        raquet_ortho.upload(ORIGIN, "/upstream/edge-ortho/ortho.parquet", path)
        cold = check("origin-cache cold", A, "edge-ortho", "ortho.parquet", proxy)
        assert sum(q["origin_reads"] for q in cold.values()) > 0
        warm = check("origin-cache warm", B, "edge-ortho", "ortho.parquet", proxy)
        for name in point_names:
            assert warm[name]["origin_reads"] == 0, ("gateway B re-read the origin", name, warm[name])
            assert (warm[name]["gets"], warm[name]["bytes"]) == (cold[name]["gets"], cold[name]["bytes"]), name
        records.append({"role": "origin-cache cold", "queries": cold})
        records.append({"role": "origin-cache warm", "queries": warm})
        raquet_ortho.upload(A, "/edge-ortho/written.parquet", path)
        written = check("write-through", B, "edge-ortho", "written.parquet", proxy)
        assert all(q["origin_reads"] == 0 for q in written.values()), ("write-through object was pulled from the origin", written)
        eventually("ortho write-back drain", lambda: pending("ortho-origin") == 0, seconds=600)
        check_file(ORIGIN, "/upstream/edge-ortho/written.parquet", digest, size)
        records.append({"role": "write-through", "queries": written})
    (RUN / "ortho.json").write_text(json.dumps({"complete": True, "sheets": sheets, "bytes": size, "sha256": digest,
                                                 "tiling": meta["tiling"], "records": records}, indent=2) + "\n")
    point = records[0]["queries"]["point 0"]
    print(f"ortho: {sheets}x{sheets} LINZ sheets, {size / (1 << 30):.2f} GiB RaQuet, {meta['tiling']['num_blocks']} native tiles; "
          f"point lookup {point['gets']} GETs / {point['bytes']} bytes; warm origin reads "
          f"region={warm['region']['origin_reads']} scan={warm['scan']['origin_reads']}; native, origin-cache and "
          f"write-through match the local oracle", flush=True)


ICEBERG_ROWS = 20000


def iceberg_batch(day):
    import pyarrow as pa
    ids = range(day * ICEBERG_ROWS, (day + 1) * ICEBERG_ROWS)
    return pa.table({"id": pa.array(ids, pa.int64()), "day": pa.array([date(2026, 1, 1) + timedelta(day)] * ICEBERG_ROWS, pa.date32()),
                     "kind": pa.array([f"k{i % 7}" for i in ids]), "value": pa.array([float(i % 1000) for i in ids])})


def iceberg_expected(days):
    ids = range(days * ICEBERG_ROWS)
    totals = {}
    for i in ids:
        count, value = totals.get(f"k{i % 7}", (0, 0.0))
        totals[f"k{i % 7}"] = (count + 1, value + i % 1000)
    last = range((days - 1) * ICEBERG_ROWS, days * ICEBERG_ROWS)
    return {"count": [(len(ids),)], "partition": [(ICEBERG_ROWS, float(sum(i % 1000 for i in last)))],
            "aggregate": [(kind, *totals[kind]) for kind in sorted(totals)]}


def iceberg_queries(location, days):
    scan = f"iceberg_scan('{location}')"
    return [("snapshots", f"SELECT count(*) FROM iceberg_snapshots('{location}')"),
            ("count", f"SELECT count(*) FROM {scan}"),
            ("partition", f"SELECT count(*), sum(value) FROM {scan} WHERE day = DATE '{date(2026, 1, 1) + timedelta(days - 1)}'"),
            ("aggregate", f"SELECT kind, count(*), sum(value) FROM {scan} GROUP BY kind ORDER BY kind")]


def iceberg():
    """An Iceberg table with one snapshot per daily append, read by DuckDB
    straight from a latency-injected origin and through the cache tier."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType
    days = int(os.environ.get("PGVS3_ICEBERG_COMMITS", "100"))
    latency = float(os.environ.get("PGVS3_ICEBERG_ORIGIN_MS", "20")) / 1000
    repeat = int(os.environ.get("PGVS3_ICEBERG_REPEAT", "3"))
    bucket, identifier = "ice", "bench.events"

    def catalog(endpoint):
        return SqlCatalog("pgvs3", uri=f"sqlite:///{RUN / 'iceberg-catalog.db'}", warehouse=f"s3://{bucket}/warehouse",
                          **{"s3.endpoint": endpoint, "s3.access-key-id": "test", "s3.secret-access-key": "test", "s3.region": "us-east-1"})

    def run(role, endpoint, location, days, proxy):
        """Per query: rows, client GETs, origin reads and wall time of each
        repetition (the first is the cold one on a fresh cache)."""
        import duckdb
        connection, _ = duckdb_session(endpoint, bucket)
        observed = {}
        with connection:
            connection.execute("INSTALL iceberg; LOAD iceberg; CALL enable_logging('HTTP')")
            for name, query in iceberg_queries(location, days):
                times, reads = [], proxy.reads_under(f"/{bucket}/")
                for _ in range(repeat):
                    connection.execute("CALL truncate_duckdb_logs()")
                    started = time.perf_counter()
                    rows = connection.execute(query).fetchall()
                    times.append(round((time.perf_counter() - started) * 1000, 1))
                    if len(times) == 1:
                        gets = connection.execute("SELECT count(*) FROM duckdb_logs_parsed('HTTP') WHERE request.type = 'GET'").fetchone()[0]
                        first_reads = proxy.reads_under(f"/{bucket}/") - reads
                observed[name] = {"rows": rows, "gets": gets, "origin_reads": first_reads, "ms": times}
        expected = iceberg_expected(days)
        for name, result in observed.items():
            want = [(days,)] if name == "snapshots" else expected[name]
            assert result["rows"] == want, (role, name, result["rows"], want)
            del result["rows"]
        return observed

    with OriginProxy(latency, keep_alive=True) as proxy:
        configure_origin(proxy, bucket, "ice-origin", strip=True)
        writer = catalog(ORIGIN)
        writer.create_namespace("bench")
        table = writer.create_table(identifier, partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "day")),
                                    schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "day", DateType()),
                                                  NestedField(3, "kind", StringType()), NestedField(4, "value", DoubleType())))
        started = time.monotonic()
        for day in range(days):
            table.append(iceberg_batch(day))
        build_s = round(time.monotonic() - started, 1)
        location = table.metadata_location
        records = [{"role": "direct", "queries": run("direct", proxy.endpoint, location, days, proxy)}]
        cold = run("origin-cache cold", B, location, days, proxy)
        warm = run("origin-cache warm", A, location, days, proxy)
        for name in warm:
            assert warm[name]["origin_reads"] == 0, ("gateway A re-read the origin", name, warm[name])
            assert cold[name]["gets"] == warm[name]["gets"] == records[0]["queries"][name]["gets"], name
        records += [{"role": "origin-cache cold", "queries": cold}, {"role": "origin-cache warm", "queries": warm}]

        def appends(endpoint, first, count):
            table = catalog(endpoint).load_table(identifier)
            times = []
            for day in range(first, first + count):
                started = time.perf_counter()
                table.append(iceberg_batch(day))
                times.append(round((time.perf_counter() - started) * 1000, 1))
            return table.metadata_location, times

        location, through = appends(A, days, 3)
        written = run("write-through", B, location, days + 3, proxy)
        assert all(q["origin_reads"] == 0 for q in written.values()), ("write-through snapshot was pulled from the origin", written)
        records.append({"role": "write-through", "append_ms": through, "queries": written})
        eventually("iceberg write-back drain", lambda: pending("ice-origin") == 0)
        _, direct = appends(proxy.endpoint, days + 3, 3)
        records.append({"role": "direct append", "append_ms": direct})
    (RUN / "iceberg.json").write_text(json.dumps({"complete": True, "commits": days, "rows_per_commit": ICEBERG_ROWS,
                                                   "origin_latency_ms": latency * 1000, "build_s": build_s,
                                                   "records": records}, indent=2) + "\n")
    median = lambda values: sorted(values)[len(values) // 2]
    summary = "; ".join(f"{name} {records[0]['queries'][name]['ms'][0]:.0f}/{cold[name]['ms'][0]:.0f}/{median(warm[name]['ms']):.0f} ms"
                        for name in warm)
    print(f"iceberg: {days} snapshots, {latency * 1000:.0f} ms origin latency; direct/cold/warm {summary}; "
          f"append direct {median(direct):.0f} ms vs through pgvs3 {median(through):.0f} ms; all roles match", flush=True)


TRINO_IMAGE = "trinodb/trino:484"
NESSIE_IMAGE = "ghcr.io/projectnessie/nessie:0.109.0"
TRINO_MONTHS = [date(1992 + m // 12, m % 12 + 1, 1) for m in range(84)]
TRINO_MONTH = ("SELECT count(*), sum(CAST(extendedprice AS DECIMAL(18,2))) FROM {t} "
               "WHERE shipdate >= DATE '1995-06-01' AND shipdate < DATE '1995-07-01'")
TRINO_Q1 = ("SELECT returnflag, linestatus, count(*), sum(CAST(quantity AS DECIMAL(18,2))), "
            "sum(CAST(extendedprice AS DECIMAL(18,2))) FROM {t} WHERE shipdate <= DATE '1998-09-02' "
            "GROUP BY 1, 2 ORDER BY 1, 2")
# (name, table, query): "lineitem" is the month-by-month commit log of small
# files, "big" one CTAS of large files.
TRINO_QUERIES = [
    ("count", "lineitem", "SELECT count(*) FROM {t}"),
    ("month", "lineitem", TRINO_MONTH),
    ("order", "lineitem", "SELECT count(*), sum(CAST(quantity AS DECIMAL(18,2))) FROM {t} WHERE orderkey = 3000000"),
    ("q1", "lineitem", TRINO_Q1),
    ("big order", "big", "SELECT count(*), sum(CAST(quantity AS DECIMAL(18,2))) FROM {t} WHERE orderkey = 30000000"),
    ("big month", "big", TRINO_MONTH),
    ("big q1", "big", TRINO_Q1),
]

TRINO_PAIRED_CATALOGS = {"pg": "via_a", "alluxio": "alluxio"}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def container(name, image, *args):
    """A host-network container, so it reaches the gateways and the origin
    proxy on 127.0.0.1; its log is kept in the run directory."""
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.check_output(["docker", "run", "-d", "--name", name, "--network", "host", *args, image], text=True)
    try:
        yield
    finally:
        with (RUN / f"{name.rsplit('-', 1)[-1]}.log").open("wb") as log:
            subprocess.run(["docker", "logs", name], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def copy_client_extensions(image, extension_directory):
    """Copy only the signed quack/httpfs artifacts and .info sidecars out of
    the image for the host Python 2.0 client. No runtime INSTALL is used."""
    import_target = Path(extension_directory) / DUCKDB_EXTENSION_VERSION / DUCKDB_EXTENSION_PLATFORM
    import_target.mkdir(parents=True, exist_ok=True)
    helper = f"pgvs3-{RUN.name}-extcopy"
    subprocess.run(["docker", "rm", "-f", helper], capture_output=True)
    subprocess.check_output(["docker", "create", "--name", helper, image], text=True,
                            stderr=subprocess.STDOUT)
    try:
        for extension in ("httpfs", "quack", "spatial", "raquet"):
            for suffix in ("", ".info"):
                source = (f"{helper}:/opt/duckdb/.duckdb/extensions/"
                          f"{DUCKDB_EXTENSION_VERSION}/{DUCKDB_EXTENSION_PLATFORM}/"
                          f"{extension}.duckdb_extension{suffix}")
                subprocess.check_output(["docker", "cp", source, str(import_target) + "/"],
                                        text=True, stderr=subprocess.STDOUT)
    finally:
        subprocess.run(["docker", "rm", "-f", helper], capture_output=True)
    return Path(extension_directory)


def host_client(extension_directory, port=None, token=None):
    """The host analytics uv client, using signed image artifacts offline and
    an existing connection for every functional/benchmark query."""
    import duckdb
    connection = duckdb.connect(config=duckdb_client_config(extension_directory))
    connection.execute("LOAD httpfs; LOAD quack;")
    connection.execute("SET enable_external_file_cache=false; SET parquet_metadata_cache=false;")
    if port is not None:
        connection.execute(
            f"ATTACH 'quack:127.0.0.1:{port}' AS q (TYPE quack, TOKEN {sql_str(token)})"
        )
    return connection


def q_sql(client, statement):
    """Execute SQL on the native Quack server, not a new CLI process."""
    return client.execute("FROM q.query(" + sql_str(statement) + ")").fetchall()


def q_exec(client, statement):
    started = time.perf_counter()
    rows = q_sql(client, statement)
    return round((time.perf_counter() - started) * 1000, 3), rows


def q_timed(client, statement, repeat=3):
    samples, rows = [], None
    for _ in range(repeat):
        started = time.perf_counter()
        rows = q_sql(client, statement)
        samples.append((time.perf_counter() - started) * 1000)
    return timing_summary(samples), rows


def wait_compute(extension_directory, port, token):
    def ready():
        try:
            connection = host_client(extension_directory, port, token)
            try:
                return connection.execute("SELECT 1").fetchone() == (1,)
            finally:
                connection.close()
        except Exception:
            return False
    eventually("compute Quack readiness", ready, 120)


def compute_environment(names, endpoint, port, token):
    return {
        "PGVS3_URL": DATABASE,
        "PGVS3_BUCKET": names["bucket"],
        "PGVS3_PREFIX": names["prefix"],
        "PGVS3_ENDPOINT": endpoint,
        "PGVS3_S3_ACCESS_KEY": os.environ["PGVS3_ACCESS_KEY"],
        "PGVS3_S3_SECRET_KEY": os.environ["PGVS3_SECRET_KEY"],
        "PGVS3_S3_USE_SSL": "false",
        "PGVS3_METADATA_SCHEMA": names["schema"],
        "QUACK_TOKEN": token,
        "QUACK_BIND": f"127.0.0.1:{port}",
        "QUACK_ALLOW_OTHER_HOSTNAME": "false",
        "DUCKDB_MEMORY_LIMIT": "1GB",
        "DUCKDB_THREADS": "4",
        "DUCKDB_TEMP_DIRECTORY": "/tmp/duckdb",
    }


def compute_container_args(environment):
    # Native DuckDB is PID 1 by default: without -i it sees stdin EOF and exits,
    # and without an init it ignores SIGTERM. One host Docker init plus open
    # stdin keeps Quack alive and turns docker stop into a normal 143 exit.
    args = ["-i", "--init", "--read-only", "--tmpfs", "/tmp:rw,size=2g"]
    for key, value in environment.items():
        args.extend(["-e", f"{key}={value}"])
    return args


def stop_compute(name):
    started = time.perf_counter()
    subprocess.run(["docker", "stop", "--time", "20", name], check=True,
                   capture_output=True, text=True, timeout=40)
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    state = json.loads(docker_inspect(name, "{{json .State}}"))
    logs = subprocess.run(["docker", "logs", name], text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=30).stdout
    assert state["ExitCode"] == 143, ("compute SIGTERM exit", state)
    assert "PGVS3_READY" in logs, logs[-2000:]
    assert "warning Quack sessions start in the memory catalog" in logs, logs[-2000:]
    return {"sigterm": True, "exit_code": state["ExitCode"], "stop_ms": elapsed,
            "ready_log": True, "memory_catalog_warning": True}


def client_s3_secret(client, endpoint):
    host = endpoint.split("://", 1)[-1]
    client.execute(
        "CREATE SECRET pgvs3_native (TYPE s3, "
        f"KEY_ID {sql_str(os.environ['PGVS3_ACCESS_KEY'])}, "
        f"SECRET {sql_str(os.environ['PGVS3_SECRET_KEY'])}, "
        "REGION 'us-east-1', "
        f"ENDPOINT {sql_str(host)}, URL_STYLE 'path', USE_SSL false)"
    )


def lake_parquet_objects(bucket, prefix):
    _, listing = request(B, f"/{bucket}?list-type=2&prefix={quote(prefix.rstrip('/') + '/')}",
                         timeout=60)
    return [(key, size) for key, size in parse_s3_objects(listing) if key.endswith(".parquet")]


def lake_object_keys(bucket, prefix):
    return [key for key, _ in lake_parquet_objects(bucket, prefix)]


@contextmanager
def raquet_local(extension_directory):
    """Signed spatial/raquet artifacts copied from the image, loaded offline as
    a local oracle for the Quack measurements."""
    import duckdb
    connection = duckdb.connect(config=duckdb_client_config(extension_directory))
    try:
        connection.execute("LOAD spatial; LOAD raquet;")
        yield connection
    finally:
        connection.close()


def parquet_digest(client, bucket, keys):
    paths = "[" + ",".join(sql_str(f"s3://{bucket}/{key}") for key in keys) + "]"
    return client.execute(
        f"SELECT count(*), sum(id), sum(hash(id,payload)::HUGEINT) FROM read_parquet({paths})"
    ).fetchone()


def differential_local(name):
    statement = """
SET autoinstall_known_extensions=false;
SET autoload_known_extensions=false;
LOAD differential;
SET differential_repository='/tmp/compute-differential/repo';
ATTACH 'differential:compute' AS d;
USE d;
CREATE TABLE t (id INTEGER);
INSERT INTO t VALUES (1),(2),(3);
CHECKPOINT;
CALL differential_create_snapshot('d', name='v1');
SELECT count(*) FROM differential_snapshots('d')
  WHERE snapshot_name='v1' AND publication_state='local_published' AND NOT retired;
USE memory;
DETACH d;
ATTACH 'differential:compute' AS d2;
SELECT count(*) FROM d2.main.t;
DETACH d2;
"""
    result = subprocess.run(
        ["docker", "exec", "-i", name, "/duckdb", "-no-agent",
         "-batch", "-noheader", "-csv", "-bail"],
        input=statement, text=True, capture_output=True, timeout=90,
    )
    assert result.returncode == 0, (result.stdout[-1000:], result.stderr[-1000:])
    assert "1" in result.stdout.splitlines() and "3" in result.stdout.splitlines(), result.stdout
    return {"baked": True, "local_snapshot": True, "reopened_rows": 3,
            "remote_ack": False,
            "note": "local native snapshot only; differential is not the stateless lake backend"}


def unsupported_index(client, statement):
    try:
        q_sql(client, statement)
        return {"supported": True}
    except Exception as error:
        return {"supported": False, "error": str(error).splitlines()[0][:300]}


def compute_large(client, names, extension_directory):
    """The opt-in large datasets: a 1M-row DuckLake table and the real LINZ
    Wellington orthophoto. Loads and timings go through the live Quack server;
    local signed spatial/raquet artifacts are only the value oracle."""
    import hashlib
    import raquet_ortho

    create_ms, _ = q_exec(
        client,
        "CREATE TABLE lake.main.large_events AS "
        "SELECT i::BIGINT AS id, (i % 1000)::INT AS group_id, i::DOUBLE AS val, "
        "'payload-' || md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload "
        "FROM range(1000000) t(i)",
    )
    flush_ms, _ = q_exec(
        client,
        "CALL ducklake_flush_inlined_data('lake', table_name => 'large_events')",
    )
    load_ms = round(create_ms + flush_ms, 3)
    lake_files, lake_bytes = q_sql(
        client,
        "SELECT count(*), coalesce(sum(data_file_size_bytes), 0) "
        "FROM ducklake_list_files('lake', 'large_events')",
    )[0]
    table_prefix = f"{names['prefix']}/main/large_events"
    objects = lake_parquet_objects(names["bucket"], table_prefix)
    object_bytes = sum(size for _, size in objects)
    assert lake_files == len(objects), (lake_files, objects)
    assert object_bytes == lake_bytes, (object_bytes, lake_bytes)
    assert 35 << 20 <= object_bytes <= 55 << 20, object_bytes

    projection_timing, projection_rows = q_timed(
        client, "SELECT sum(val), count(*) FROM lake.main.large_events")
    assert projection_rows == [(499999500000.0, 1000000)], projection_rows
    range_timing, range_rows = q_timed(
        client,
        "SELECT sum(val) FROM lake.main.large_events "
        "WHERE id BETWEEN 500000 AND 509999",
    )
    assert range_rows == [(5049995000.0,)], range_rows
    point_timing, point_rows = q_timed(
        client,
        "SELECT payload FROM lake.main.large_events WHERE id = 777777",
    )
    payload = ("payload-" + hashlib.md5(b"777777").hexdigest()
               + hashlib.md5(b"777778").hexdigest())
    assert point_rows == [(payload,)], (point_rows, payload)

    path = raquet_ortho.build(4)
    size = path.stat().st_size
    metadata = raquet_ortho.metadata(path)
    digest = raquet_ortho.sha256(path)
    key = "ortho-4x4.parquet"
    started = time.perf_counter()
    raquet_ortho.upload(A, f"/{names['bucket']}/{key}", path)
    upload_ms = round((time.perf_counter() - started) * 1000, 1)
    headers, _ = request(B, f"/{names['bucket']}/{key}", "HEAD", timeout=600)
    assert int(headers["Content-Length"]) == size, headers
    url = f"s3://{names['bucket']}/{key}"

    west, south, east, north = metadata["bounds"]
    lon, lat = (west + east) / 2, (south + north) / 2
    x0, x1 = west + (east - west) * 0.35, west + (east - west) * 0.65
    y0, y1 = south + (north - south) * 0.35, south + (north - south) * 0.65
    region = (f"'POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, "
              f"{x0} {y1}, {x0} {y0}))'::GEOMETRY")
    lookup = lambda zoom="": (
        f"SELECT {raquet_ortho.rgb(lon, lat)} FROM "
        f"read_raquet_at('{url}', {lon!r}, {lat!r}{zoom})")
    with raquet_local(extension_directory) as oracle:
        local_metadata = json.loads(oracle.execute(
            "SELECT metadata FROM read_raquet_metadata(?)", [str(path)]).fetchone()[0])
        assert local_metadata == metadata, "cached ortho metadata changed"
        local_native = oracle.execute(
            f"SELECT {raquet_ortho.rgb(lon, lat)} FROM "
            f"read_raquet_at('{path}', {lon!r}, {lat!r})").fetchall()
        local_overview = oracle.execute(
            f"SELECT {raquet_ortho.rgb(lon, lat)} FROM "
            f"read_raquet_at('{path}', {lon!r}, {lat!r}, 16)").fetchall()
        local_matched = oracle.execute(
            f"SELECT count(*) FROM read_raquet('{path}', {region})").fetchone()[0]
        local_region = oracle.execute(
            f"SELECT unnest(ST_RegionStats(band_1, block, {region}, metadata)) "
            f"FROM read_raquet('{path}', {region})").fetchall()

    metadata_timing, metadata_rows = q_timed(
        client, f"SELECT metadata FROM read_raquet_metadata('{url}')")
    assert json.loads(metadata_rows[0][0]) == metadata, "Quack metadata differs"
    native_timing, native_rows = q_timed(client, lookup())
    assert native_rows == local_native, (native_rows, local_native)
    overview_timing, overview_rows = q_timed(client, lookup(", 16"))
    assert overview_rows == local_overview, (overview_rows, local_overview)
    region_ms, region_rows = q_exec(
        client, f"SELECT count(*) FROM read_raquet('{url}', {region})")
    assert region_rows == [(local_matched,)], (region_rows, local_matched)
    region_stats_ms, server_region = q_exec(
        client,
        f"SELECT unnest(ST_RegionStats(band_1, block, {region}, metadata)) "
        f"FROM read_raquet('{url}', {region})",
    )
    assert raquet_ortho.same(server_region, local_region), (server_region, local_region)

    mib = 1 << 20
    return {
        "ducklake": {
            "table": "q.lake.main.large_events",
            "server_table": "lake.main.large_events",
            "rows": 1000000,
            "load_ms": load_ms,
            "create_ms": create_ms,
            "flush_ms": flush_ms,
            "ducklake_stored_bytes": int(lake_bytes),
            "s3_listing": {
                "endpoint": "gateway B",
                "prefix": table_prefix,
                "objects": len(objects),
                "bytes": object_bytes,
                "mib": round(object_bytes / mib, 1),
            },
            "queries": {
                "columnar_projection": {
                    "sql": "SELECT sum(val), count(*) FROM lake.main.large_events",
                    "result": {"sum_val": projection_rows[0][0],
                               "count": projection_rows[0][1]},
                    "timing": projection_timing,
                },
                "range_1pct": {
                    "sql": "SELECT sum(val) FROM lake.main.large_events "
                           "WHERE id BETWEEN 500000 AND 509999",
                    "result": {"sum_val": range_rows[0][0]},
                    "timing": range_timing,
                },
                "point_lookup": {
                    "sql": "SELECT payload FROM lake.main.large_events "
                           "WHERE id = 777777",
                    "result": {"payload": point_rows[0][0]},
                    "timing": point_timing,
                },
            },
        },
        "ortho": {
            "file": str(path),
            "bytes": size,
            "gib": round(size / (1 << 30), 3),
            "sha256": digest,
            "upload": {
                "endpoint": "gateway A",
                "target": f"{names['bucket']}/{key}",
                "ms": upload_ms,
                "mib_s": round(size / mib / (upload_ms / 1000), 1),
            },
            "tiling": metadata["tiling"],
            "metadata": {"timing": metadata_timing, "matches": True},
            "native_z21": {
                "lon": lon,
                "lat": lat,
                "rgb": list(native_rows[0]),
                "timing": native_timing,
            },
            "overview_z16": {
                "rgb": list(overview_rows[0]),
                "timing": overview_timing,
            },
            "region_9pct": {
                "predicate": "read_raquet(url, region) uses ST_Intersects",
                "ms": region_ms,
                "stats_ms": region_stats_ms,
                "matched_tiles": region_rows[0][0],
                "expected_matched_tiles": local_matched,
                "stats": server_region,
                "matches": True,
            },
        },
    }


def compute_bench(client, names, extension_directory):
    q_exec(client, "SET enable_external_file_cache=false; SET parquet_metadata_cache=false")
    points_build_ms, _ = q_exec(
        client,
        "CREATE TABLE lake.main.bench_points AS "
        "SELECT i AS id, ST_Point((i % 100)::DOUBLE, (i // 100)::DOUBLE) AS geom "
        "FROM range(5000) t(i)",
    )
    spatial_lake, lake_spatial = q_timed(
        client,
        "SELECT count(*) FROM lake.main.bench_points "
        "WHERE ST_Within(geom, ST_MakeEnvelope(10,10,50,50))",
    )
    points_copy_ms, _ = q_exec(
        client, "CREATE TABLE native_points AS SELECT * FROM lake.main.bench_points"
    )
    points_index_ms, _ = q_exec(
        client, "CREATE INDEX native_points_rtree ON native_points USING RTREE (geom)"
    )
    spatial_statement = (
        "SELECT count(*) FROM native_points "
        "WHERE ST_Within(geom, ST_MakeEnvelope(10,10,50,50))"
    )
    spatial_native, native_spatial = q_timed(client, spatial_statement)
    assert lake_spatial == native_spatial, (lake_spatial, native_spatial)
    unsupported_spatial = unsupported_index(
        client, "CREATE INDEX lake_points_rtree ON lake.main.bench_points USING RTREE (geom)"
    )
    q_exec(client, "INSERT INTO lake.main.bench_points VALUES (5000, ST_Point(500,500))")
    lakes_after = q_sql(client, "SELECT count(*) FROM lake.main.bench_points")[0][0]
    native_after = q_sql(client, "SELECT count(*) FROM native_points")[0][0]
    assert lakes_after == 5001 and native_after == 5000, (lakes_after, native_after)

    vec_build_ms, _ = q_exec(
        client,
        "CREATE TABLE lake.main.bench_vecs AS "
        "SELECT i AS id, list_transform(range(32), lambda j: sin(i*0.01+j*0.1)::FLOAT) AS vec "
        "FROM range(5000) t(i)",
    )
    query = [math.sin(42 * 0.01 + j * 0.1) for j in range(32)]
    literal = "[" + ",".join(f"{value:.9g}" for value in query) + "]::FLOAT[32]"
    vector_lake, lake_ids = q_timed(
        client,
        "SELECT id FROM lake.main.bench_vecs "
        f"ORDER BY array_cosine_distance(vec::FLOAT[32], {literal}) LIMIT 10",
    )
    columns = ", ".join(f"vec[{j}]::FLOAT" for j in range(1, 33))
    vector_copy_ms, _ = q_exec(
        client,
        f"CREATE TABLE native_vecs AS SELECT id, array_value({columns}) AS vec "
        "FROM lake.main.bench_vecs",
    )
    vector_index_ms, _ = q_exec(
        client,
        "CREATE INDEX native_vec_hnsw ON native_vecs USING HNSW (vec) WITH (metric='cosine')",
    )
    vector_statement = (
        "SELECT id FROM native_vecs "
        f"ORDER BY array_cosine_distance(vec, {literal}) LIMIT 10"
    )
    vector_native, native_ids = q_timed(client, vector_statement)
    recall_hits = len({row[0] for row in lake_ids} & {row[0] for row in native_ids})
    assert recall_hits == 10, (recall_hits, lake_ids, native_ids)
    recall = recall_hits / 10
    unsupported_vector = unsupported_index(
        client, "CREATE INDEX lake_vec_hnsw ON lake.main.bench_vecs USING HNSW (vec)"
    )

    docs_build_ms, _ = q_exec(
        client, "CREATE TABLE native_docs (id INTEGER NOT NULL, body VARCHAR)"
    )
    q_exec(client, "INSERT INTO native_docs SELECT i, 'doc ' || i FROM range(20) t(i)")
    fts_build_ms, _ = q_exec(
        client,
        "PRAGMA create_fts_index('native_docs','id','body', incremental=true, overwrite=1)",
    )
    fts_statement = (
        "SELECT count(*) FROM (SELECT *, "
        "fts_main_native_docs.match_bm25(id,'zebra') AS score FROM native_docs) "
        "WHERE score IS NOT NULL"
    )
    fts_before, _ = q_timed(client, fts_statement)
    q_exec(client, "INSERT INTO native_docs VALUES (100, 'zebra')")
    fts_insert = q_sql(client, fts_statement)[0][0]
    q_exec(client, "DELETE FROM native_docs WHERE id=100")
    fts_delete = q_sql(client, fts_statement)[0][0]
    q_exec(client, "INSERT INTO native_docs VALUES (101, 'zebra')")
    q_exec(client, "UPDATE native_docs SET body='wombat' WHERE id=101")
    fts_after_update = q_sql(client, fts_statement)[0][0]
    wombat = q_sql(client, fts_statement.replace("'zebra'", "'wombat'"))[0][0]
    assert fts_insert == 1 and fts_delete == 0, (fts_insert, fts_delete)

    return {
        "enabled": True,
        "datasets": {"points": 5000, "vectors": 5000, "dimensions": 32, "documents": 20},
        "spatial": {
            "lake_scan": spatial_lake,
            "native_rtree": {"copy_ms": points_copy_ms, "index_ms": points_index_ms,
                             "query": spatial_native},
            "lake_build_ms": points_build_ms,
            "rows": lake_spatial[0][0],
            "lake_after_insert": lakes_after,
            "native_copy_after_insert": native_after,
            "native_copy_stale_rows": lakes_after - native_after,
            "unsupported_lake_index": unsupported_spatial,
        },
        "vector": {
            "lake_scan": vector_lake,
            "native_hnsw": {"copy_ms": vector_copy_ms, "index_ms": vector_index_ms,
                            "query": vector_native},
            "lake_build_ms": vec_build_ms,
            "recall_at_10": recall_hits,
            "recall": recall,
            "unsupported_lake_index": unsupported_vector,
        },
        "fts": {
            "native_build_ms": round(docs_build_ms + fts_build_ms, 3),
            "query": fts_before,
            "insert_visible": fts_insert,
            "delete_removed": fts_delete,
            "zebra_after_update": fts_after_update,
            "wombat_after_update": wombat,
            "update_maintained": fts_after_update == 0 and wombat == 1,
            "note": "incremental INSERT/DELETE maintained; UPDATE remains stale in DuckDB 2.0.0",
        },
        "large": compute_large(client, names, extension_directory),
        "caveat": "native copies are ephemeral memory objects, not lake indexes; timings are "
                  "native Quack round-trip samples, not storage or throughput claims",
    }


def compute():
    """The actual deployment shape: one existing pgvs3 PostgreSQL database,
    native S3 buckets through gateways A/B, the non-root/read-only DuckDB image
    with native Quack, and a restart with no persistent local database."""
    names = compute_names(RUN.name)
    image = DUCKDB_IMAGE
    extension_directory = RUN / "duckdb-client-extensions"
    copy_client_extensions(image, extension_directory)
    import duckdb
    image_id = docker_inspect(image, "{{.Id}}")
    parts = urlsplit(DATABASE)
    report = {
        "preamble": False,
        "success": False,
        "complete": False,
        "image": {"tag": image, "id": image_id},
        "providers": {
            "compute": "pgvs3-duckdb 2.0.0 image, native Quack",
            "object_store": "pgvs3 native bucket through gateways A/B",
            "catalog": "PostgreSQL 18 pgvs3 database",
            "client": "host Python duckdb 2.0.0 with signed image quack/httpfs",
        },
        "config": {
            "pg_host": parts.hostname,
            "pg_port": parts.port,
            "pg_database": parts.path.lstrip("/"),
            "bucket": names["bucket"],
            "prefix": names["prefix"],
            "metadata_schema": names["schema"],
            "endpoint_a": A,
            "endpoint_b": B,
            "readonly_rootfs": True,
            "nonroot_user": "65532:65532",
            "duckdb_memory_limit": "1GB",
            "duckdb_threads": 4,
        },
        "versions": {},
        "functional": {},
        "restart": {},
        "negative": {},
        "bench": {"enabled": False},
        "raw_samples": {},
    }

    def save():
        (RUN / "compute.json").write_text(json.dumps(report, indent=2) + "\n")

    save()
    request(A, f"/{names['bucket']}", "PUT", b"")
    token = os.urandom(16).hex()
    port = free_port()
    env = compute_environment(names, A, port, token)
    name = f"pgvs3-{RUN.name}-compute-a"
    with container(name, image, *compute_container_args(env)):
        wait_compute(extension_directory, port, token)
        client = host_client(extension_directory, port, token)
        try:
            client_s3_secret(client, B)
            server_version = q_sql(client, "SELECT version()")[0][0]
            server_modules = q_sql(
                client,
                "SELECT extension_name, extension_version, installed_from, loaded "
                "FROM duckdb_extensions() WHERE extension_name IN "
                "('spatial','ducklake','postgres_scanner','fts','vss','quack','httpfs',"
                "'raquet','differential') ORDER BY 1",
            )
            client_version = duckdb.__version__
            client_modules = client.execute(
                "SELECT extension_name, extension_version, installed_from, loaded "
                "FROM duckdb_extensions() WHERE extension_name IN ('quack','httpfs') ORDER BY 1"
            ).fetchall()
            report["versions"] = {
                "server_duckdb": server_version,
                "client_duckdb": client_version,
                "server_modules": [dict(zip(("name", "version", "origin", "loaded"), row))
                                   for row in server_modules],
                "client_modules": [dict(zip(("name", "version", "origin", "loaded"), row))
                                   for row in client_modules],
            }
            assert server_version.startswith("v2.0.0"), server_version
            assert all(row[1] for row in server_modules), server_modules
            assert all(row[1] for row in client_modules), client_modules
            assert all(row[3] for row in server_modules), server_modules

            schema_tables = int(sql(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = "
                f"{sql_str(names['schema'])}"
            ))
            namespace_rows = int(sql(
                f"SELECT count(*) FROM s3p.namespaces WHERE name = {sql_str(names['bucket'])}"
            ))
            assert schema_tables > 0, schema_tables
            assert namespace_rows == 1, namespace_rows

            native_key = "native/compute-object.txt"
            request(A, f"/{names['bucket']}/{native_key}", "PUT", b"native-object")
            _, native_body = request(B, f"/{names['bucket']}/{native_key}")
            assert native_body == b"native-object", native_body

            client.execute("CREATE TABLE q.lake.main.events (id INTEGER, payload VARCHAR)")
            client.execute(
                "INSERT INTO q.lake.main.events SELECT i, 'value-' || i FROM range(100) t(i)"
            )
            q_exec(client, "CALL ducklake_flush_inlined_data('lake', table_name => 'events')")
            lake_rows = client.execute(
                "SELECT count(*), sum(id), sum(hash(id,payload)::HUGEINT) "
                "FROM q.lake.main.events"
            ).fetchone()
            assert lake_rows[0] == 100 and lake_rows[1] == 4950, lake_rows
            keys = lake_object_keys(names["bucket"], names["prefix"])
            assert keys, "DuckLake stored no Parquet keys"
            parquet_rows = parquet_digest(client, names["bucket"], keys)
            assert parquet_rows == lake_rows, (parquet_rows, lake_rows)

            report["functional"] = {
                "explicit_bucket_created": True,
                "metadata_schema_tables": schema_tables,
                "same_database_native_namespace": namespace_rows,
                "native_put_get_after_ducklake": True,
                "qualified_read": "q.lake.main.events",
                "qualified_create_insert": True,
                "rows": lake_rows[0],
                "id_sum": lake_rows[1],
                "hash": str(lake_rows[2]),
                "parquet_keys": keys,
                "parquet_rows": parquet_rows[0],
                "parquet_hash": str(parquet_rows[2]),
                "raquet_functions": q_sql(
                    client,
                    "SELECT count(DISTINCT function_name) FROM duckdb_functions() "
                    "WHERE function_name IN ('read_raquet_metadata','read_raquet_at')",
                )[0][0],
                "differential": differential_local(name),
                "qualified_note": "q.lake.main.* writes are durable; unqualified "
                                  "q.<table> writes land in the ephemeral memory catalog",
            }
            assert report["functional"]["raquet_functions"] == 2
            report["raw_samples"]["functional"] = {
                "lake_rows": lake_rows,
                "parquet_rows": parquet_rows,
                "keys": keys,
            }
            save()

            stopping = stop_compute(name)
        finally:
            client.close()
    report["restart"]["shutdown"] = stopping

    port2 = free_port()
    env2 = compute_environment(names, B, port2, token)
    name2 = f"pgvs3-{RUN.name}-compute-b"
    with container(name2, image, *compute_container_args(env2)):
        wait_compute(extension_directory, port2, token)
        client2 = host_client(extension_directory, port2, token)
        try:
            client_s3_secret(client2, B)
            rows_after = client2.execute(
                "SELECT count(*), sum(id), sum(hash(id,payload)::HUGEINT) "
                "FROM q.lake.main.events"
            ).fetchone()
            assert rows_after == lake_rows, (rows_after, lake_rows)
            client2.execute(
                "INSERT INTO q.lake.main.events VALUES (100, 'after-restart')"
            )
            q_exec(client2, "CALL ducklake_flush_inlined_data('lake', table_name => 'events')")
            rows_after_write = client2.execute(
                "SELECT count(*), sum(id), sum(hash(id,payload)::HUGEINT) "
                "FROM q.lake.main.events"
            ).fetchone()
            assert rows_after_write[0] == 101, rows_after_write
            keys_after = lake_object_keys(names["bucket"], names["prefix"])
            parquet_after = parquet_digest(client2, names["bucket"], keys_after)
            assert parquet_after == rows_after_write, (parquet_after, rows_after_write)
            report["restart"].update({
                "endpoint": "gateway B",
                "fresh_container": True,
                "persistent_local_database": False,
                "rows_before_write": rows_after,
                "rows_after_write": rows_after_write,
                "parquet_keys_after": keys_after,
                "parquet_rows_after": parquet_after,
            })
            report["raw_samples"]["restart"] = {
                "rows_before_write": rows_after,
                "rows_after_write": rows_after_write,
                "keys_after": keys_after,
            }
            if os.environ.get("PGVS3_DUCKDB_BENCH") == "1":
                report["bench"] = compute_bench(client2, names, extension_directory)
                report["raw_samples"]["bench"] = report["bench"]
            else:
                report["bench"] = {"enabled": False,
                                   "reason": "set PGVS3_DUCKDB_BENCH=1 for the opt-in sample"}
            report["image"].update({
                "architecture": docker_inspect(image, "{{.Architecture}}"),
                "user": docker_inspect(name2, "{{.Config.User}}"),
                "readonly_rootfs": docker_inspect(
                    name2, "{{.HostConfig.ReadonlyRootfs}}"
                ).lower() == "true",
                "mounts": int(docker_inspect(name2, "{{len .Mounts}}")),
            })
            save()
            report["restart"]["shutdown_b"] = stop_compute(name2)
        finally:
            client2.close()

    missing_bucket = f"compute-absent-{names['slug']}"[:63]
    request(B, f"/{missing_bucket}", "GET", want=404)
    missing_bucket_error = None
    probe_client = host_client(extension_directory)
    try:
        client_s3_secret(probe_client, B)
        try:
            probe_client.execute(
                f"SELECT * FROM read_parquet({sql_str(f's3://{missing_bucket}/absent.parquet')})"
            )
        except Exception as error:
            missing_bucket_error = str(error).splitlines()[0][:300]
    finally:
        probe_client.close()
    assert missing_bucket_error is not None, "missing bucket unexpectedly readable"
    request(B, f"/{missing_bucket}", "GET", want=404)

    missing_database = f"compute_missing_{names['slug']}"[:63]
    missing_url = urlunsplit((parts.scheme, parts.netloc, "/" + missing_database,
                              parts.query, parts.fragment))
    missing_env = dict(env)
    missing_env["PGVS3_URL"] = missing_url
    missing_env["PGVS3_METADATA_SCHEMA"] = f"{names['schema']}_missing"[:63]
    missing_args = ["docker", "run", "--rm", "--read-only", "--tmpfs", "/tmp:rw,size=1g"]
    for key, value in missing_env.items():
        missing_args.extend(["-e", f"{key}={value}"])
    # Native lake-only check: no Quack token/server, just secret + attach.
    missing_args.extend([
        image, "-no-agent", "-batch", "-bail",
        "-init", "/opt/pgvs3-duckdb/lake-only.sql",
    ])
    missing = subprocess.run(missing_args, text=True, capture_output=True, timeout=120)
    missing_text = missing.stdout + missing.stderr
    assert missing.returncode != 0, missing_text
    assert "PGVS3_LAKE_OK" not in missing_text, missing_text
    assert DATABASE not in missing_text, missing_text
    assert "postgres:postgres" not in missing_text, missing_text
    missing_database_count = int(sql(
        f"SELECT count(*) FROM pg_database WHERE datname = {sql_str(missing_database)}"
    ))
    assert missing_database_count == 0, missing_database_count

    report["negative"] = {
        "missing_bucket": missing_bucket,
        "missing_bucket_404": True,
        "missing_bucket_error": missing_bucket_error,
        "no_implicit_bucket_recreation": True,
        "missing_database": missing_database,
        "missing_database_exit": missing.returncode,
        "missing_database_created": missing_database_count,
        "credentials_redacted": True,
    }

    report["success"] = True
    report["complete"] = True
    save()
    print(
        f"compute: image {image_id[:19]} DuckDB {server_version} client {client_version}; "
        f"schema {names['schema']} rows {lake_rows[0]} -> {rows_after_write[0]}; "
        f"keys {len(keys)} -> {len(keys_after)}; native ops, restart, absence checks passed",
        flush=True,
    )


def trino_query(server, statement):
    """Runs one statement over Trino's client protocol; returns the rows and
    the coordinator's elapsed milliseconds."""
    headers = {"X-Trino-User": "pgvs3"}
    response = json.load(urlopen(Request(server + "/v1/statement", statement.encode(), headers), timeout=600))
    rows = []
    while True:
        rows += response.get("data", [])
        if "error" in response:
            raise AssertionError((statement, response["error"]["message"]))
        if "nextUri" not in response:
            return rows, response["stats"]["elapsedTimeMillis"]
        response = json.load(urlopen(Request(response["nextUri"], headers=headers), timeout=600))


def trino_observation(times, method_runs):
    """One query's timings and origin GET/HEAD counts. The first run is
    separate from its repeats, and the total covers every repetition because a
    later run can still miss after a first-run hit."""
    first = method_runs[0]
    total = {method: sum(run.get(method, 0) for run in method_runs) for method in ("GET", "HEAD")}
    return {"origin_reads": sum(first.values()), "origin_reads_total": sum(total.values()),
            "origin_methods": {"first": first, "total": total, "runs": method_runs},
            "first_ms": times[0], "repeat_ms": times[1:], "ms": times}


def trino_config(latency, repeat, scale, range_chunk, fill):
    """The effective values the gateways and origin setter received. Read-cache
    bytes bound local cache residency, not process RSS or in-flight bodies."""
    gateway_fill = int(os.environ.get("PGVS3_FILL_CONCURRENCY", "8"))
    return {
        "gateway_read_cache_bytes": int(os.environ.get("PGVS3_READ_CACHE_BYTES", "0")),
        "gateway_pool_max": int(os.environ.get("PGVS3_POOL_MAX", "8")),
        "gateway_fill_concurrency": gateway_fill,
        "origin_set_fill_concurrency": int(fill) if fill else gateway_fill,
        "repeat": repeat,
        "range_chunk": range_chunk,
        "origin_latency_ms": latency * 1000,
        "big_scale": scale,
    }


def trino_identity():
    """The source revision, gateway binary, host and pinned engine images; no
    credentials or connection strings."""
    try:
        commit = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True,
                                         stderr=subprocess.DEVNULL).strip()
        dirty = bool(subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True,
                                             stderr=subprocess.DEVNULL).strip())
    except (OSError, subprocess.CalledProcessError):
        commit, dirty = None, None
    return {"git_commit": commit, "git_dirty": dirty,
            "binary_sha256": hashlib.sha256(Path(BIN).read_bytes()).hexdigest(),
            "host": {"platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count()},
            "images": {"trino": TRINO_IMAGE, "nessie": NESSIE_IMAGE}}


def trino_paired(execute, proxy, expected, cycles, prefix, checkpoint, queries=TRINO_QUERIES):
    """ABBA paired warm runs against the same fixture. The caller computes
    paired ratios/CIs offline; this only preserves the block sequence."""
    providers = {
        "pg": {"catalog": TRINO_PAIRED_CATALOGS["pg"],
               "description": "pgvs3 gateway-local read cache (gateway A); Trino filesystem cache disabled"},
        "alluxio": {"catalog": TRINO_PAIRED_CATALOGS["alluxio"],
                    "description": "Alluxio unified metadata+data cache"},
    }
    paired = {"enabled": True, "cycles": cycles, "providers": providers,
              "alluxio_head_expected_per_sample": 2, "complete": False, "queries": {}}
    checkpoint(paired)
    for query, kind, template in queries:
        blocks, samples = [], []
        for cycle in range(cycles):
            order = ("pg", "alluxio", "alluxio", "pg") if cycle % 2 == 0 else ("alluxio", "pg", "pg", "alluxio")
            block = {"cycle": cycle + 1, "order": list(order), "samples": []}
            for position, provider in enumerate(order, 1):
                before = proxy.methods_under(prefix)
                rows, elapsed = execute(provider, query, kind, template)
                after = proxy.methods_under(prefix)
                methods = {method: after[method] - before[method] for method in ("GET", "HEAD")}
                assert rows == expected[query], (query, provider, rows, expected[query])
                if provider == "pg":
                    assert methods == {"GET": 0, "HEAD": 0}, ("pg paired origin read", query, methods)
                else:
                    assert methods["GET"] == 0, ("alluxio paired GET", query, methods)
                sample = {"cycle": cycle + 1, "position": position, "provider": provider,
                          "ms": elapsed, "origin_methods": methods}
                block["samples"].append(sample)
                samples.append(sample)
            blocks.append(block)
        paired["queries"][query] = {"blocks": blocks, "samples": samples}
        checkpoint(paired)
    paired["complete"] = True
    checkpoint(paired)
    return paired


def trino_profile(pprof, run, expected, runs=20, seconds=20):
    """Capture CPU/allocs while an extra warm query loop runs. This is a
    diagnostic beside the timed roles, never part of their medians."""
    base = pprof if pprof.startswith(("http://", "https://")) else "http://" + pprof
    paths = {kind: RUN / f"trino-{kind}.pprof" for kind in ("cpu", "allocs")}
    endpoints = {"cpu": "profile", "allocs": "allocs"}

    def capture(kind):
        paths[kind].write_bytes(urlopen(f"{base}/debug/pprof/{endpoints[kind]}?seconds={seconds}",
                                        timeout=seconds + 60).read())

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(capture, kind) for kind in paths]
        queries = run("profile warm", "via_a", expected, runs=runs)
        for future in futures:
            future.result()
    metrics_path = RUN / "gateway-a-metrics.txt"
    metrics_path.write_bytes(request(A, "/metrics")[1])
    assert all(q["origin_reads_total"] == 0 for q in queries.values()), ("profile warm read the origin", queries)
    return {"enabled": True, "diagnostic": True, "role": "origin-cache warm", "catalog": "via_a",
            "runs": runs, "cpu_seconds": seconds, "allocs_seconds": seconds,
            "cpu_profile": str(paths["cpu"]), "allocs_profile": str(paths["allocs"]),
            "metrics": str(metrics_path), "queries": queries}


def trino():
    """Trino 484 with a Nessie catalog whose version store is the same
    PostgreSQL: a TPC-H lineitem table committed month by month (many
    snapshots, small files) and one CTAS of a larger scale (large files), read
    straight from a latency-injected origin, through the cache tier, and through
    Trino's own Alluxio cache."""
    latency = float(os.environ.get("PGVS3_TRINO_ORIGIN_MS", "20")) / 1000
    repeat = int(os.environ.get("PGVS3_TRINO_REPEAT", "3"))
    scale = os.environ.get("PGVS3_TRINO_BIG_SCALE", "sf10")
    range_chunk = int(os.environ.get("PGVS3_TRINO_RANGE_CHUNK", str(1 << 20)))
    fill = os.environ.get("PGVS3_TRINO_FILL_CONCURRENCY")
    paired_cycles = int(os.environ.get("PGVS3_TRINO_PAIRED_CYCLES", "0"))
    profile_enabled = bool(os.environ.get("PGVS3_TEST_PPROF_A")) and os.environ.get("PGVS3_TRINO_PROFILE") == "1"
    bucket, prefix = "trino", f"pgvs3-{RUN.name}"
    build, through, appended = TRINO_MONTHS[:78], TRINO_MONTHS[78:81], TRINO_MONTHS[81:]
    report = {"complete": False, "months": len(TRINO_MONTHS), "big_scale": scale,
              "config": trino_config(latency, repeat, scale, range_chunk, fill),
              "paired": {"enabled": False}, "profile": {"enabled": False},
              "identity": trino_identity(), "records": []}

    def save():
        (RUN / "trino.json").write_text(json.dumps(report, indent=2) + "\n")

    def checkpoint(record):
        report["records"].append(record)
        save()

    save()
    pg = urlsplit(DATABASE)
    nessie_port, trino_port = free_port(), free_port()
    server = f"http://127.0.0.1:{trino_port}"
    etc = RUN / "trino-etc"
    (etc / "catalog").mkdir(parents=True, exist_ok=True)
    (etc / "config.properties").write_text(
        f"coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port={trino_port}\n"
        f"discovery.uri={server}\n"
        "cache-manager.config-files=etc/cache-manager-alluxio.properties,etc/cache-manager-memory.properties\n")
    (etc / "cache-manager-alluxio.properties").write_text(
        "cache-manager.name=alluxio\nfs.cache.directories=/cache\nfs.cache.max-sizes=4GB\n")
    (etc / "cache-manager-memory.properties").write_text("cache-manager.name=memory\nfs.memory-cache.max-size=1GB\n")
    (etc / "node.properties").write_text("node.environment=pgvs3\nnode.data-dir=/data/trino\n")
    (etc / "jvm.config").write_text("\n".join([
        "-server", "-Xmx8G", "-XX:G1HeapRegionSize=32M", "-XX:+ExplicitGCInvokesConcurrent", "-XX:+ExitOnOutOfMemoryError",
        "-XX:-OmitStackTraceInFastThrow", "-XX:ReservedCodeCacheSize=256M", "-Djdk.attach.allowAttachSelf=true",
        "-Djdk.nio.maxCachedBufferSize=2000000"]) + "\n")
    (etc / "catalog" / "tpch.properties").write_text("connector.name=tpch\n")

    def catalog(name, endpoint, cache=None):
        """cache: None disables the engine metadata cache, "trino" keeps
        Trino's coordinator MemoryBlobCache (engine metadata plus small data
        files on a single coordinator+worker), "alluxio" replaces the engine
        cache with Alluxio's unified metadata+data cache."""
        lines = ["connector.name=iceberg", "iceberg.catalog.type=nessie",
                 f"iceberg.nessie-catalog.uri=http://127.0.0.1:{nessie_port}/api/v2", "iceberg.nessie-catalog.ref=main",
                 f"iceberg.nessie-catalog.default-warehouse-dir=s3://{bucket}/warehouse", "fs.native-s3.enabled=true",
                 f"s3.endpoint={endpoint}", "s3.region=us-east-1", "s3.path-style-access=true",
                 "s3.aws-access-key=test", "s3.aws-secret-key=test"]
        if cache is None:
            lines.append("iceberg.metadata-cache.enabled=false")
        if cache == "alluxio":
            lines.append("fs.cache.enabled=true")
        (etc / "catalog" / f"{name}.properties").write_text("\n".join(lines) + "\n")

    def table(name, kind="lineitem", schema="bench"):
        return f"{name}.{schema}.{kind}"

    def oracle(months, kind):
        if kind == "big":
            return f"tpch.{scale}.lineitem"
        return f"(SELECT * FROM tpch.sf1.lineitem WHERE shipdate < DATE '{months[-1] + timedelta(days=31):%Y-%m-01}')"

    def insert(name, month):
        started = time.perf_counter()
        trino_query(server, f"INSERT INTO {table(name)} SELECT * FROM tpch.sf1.lineitem "
                            f"WHERE shipdate >= DATE '{month}' AND shipdate < DATE '{month}' + INTERVAL '1' MONTH")
        return round((time.perf_counter() - started) * 1000, 1)

    def run(role, name, expected, runs=repeat, schema="bench"):
        """Per query: coordinator milliseconds of each run (the first is the
        cold one for a fresh cache), first-run milliseconds and repeats, and
        origin GET/HEAD counts during the first run, each run and across all
        runs."""
        observed = {}
        for query, kind, template in TRINO_QUERIES:
            times, method_runs = [], []
            for _ in range(runs):
                before = proxy.methods_under(f"/{bucket}/")
                rows, elapsed = trino_query(server, template.format(t=table(name, kind, schema)))
                after = proxy.methods_under(f"/{bucket}/")
                assert rows == expected[query], (role, query, rows, expected[query])
                times.append(elapsed)
                method_runs.append({method: after[method] - before[method] for method in ("GET", "HEAD")})
            observed[query] = trino_observation(times, method_runs)
        return observed

    nessie_env = {"QUARKUS_HTTP_PORT": nessie_port, "QUARKUS_MANAGEMENT_PORT": free_port(),
                  "NESSIE_VERSION_STORE_TYPE": "JDBC2", "NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE": "postgresql",
                  "QUARKUS_DATASOURCE_POSTGRESQL_JDBC_URL": f"jdbc:postgresql://{pg.hostname}:{pg.port}/nessie",
                  "QUARKUS_DATASOURCE_POSTGRESQL_USERNAME": pg.username,
                  "QUARKUS_DATASOURCE_POSTGRESQL_PASSWORD": pg.password}
    with OriginProxy(latency, keep_alive=True) as proxy, ExitStack() as stack:
        configure_origin(proxy, bucket, "trino-origin", strip=True, range_chunk=range_chunk,
                         extra=("--fill-concurrency", fill) if fill else ())
        for name, endpoint, cache in [("direct", proxy.endpoint, None), ("via_b", B, None), ("via_a", A, None),
                                      ("alluxio", proxy.endpoint, "alluxio"), ("direct_cached", proxy.endpoint, "trino"),
                                      ("via_a_cached", A, "trino")]:
            catalog(name, endpoint, cache)
        stack.enter_context(container(f"{prefix}-nessie", NESSIE_IMAGE,
                                      *(arg for key, value in nessie_env.items() for arg in ("-e", f"{key}={value}"))))
        eventually("nessie readiness", lambda: request(f"http://127.0.0.1:{nessie_port}", "/api/v2/config", timeout=2), 120)
        stack.enter_context(container(f"{prefix}-trino", TRINO_IMAGE, "-v", f"{etc}:/etc/trino:ro",
                                      "--tmpfs", "/cache:rw,mode=1777,size=4g"))
        eventually("trino readiness",
                   lambda: not json.load(urlopen(server + "/v1/info", timeout=2))["starting"] and trino_query(server, "SELECT 1"), 180)

        trino_query(server, "CREATE SCHEMA direct.bench")
        trino_query(server, f"CREATE TABLE {table('direct')} WITH (partitioning = ARRAY['month(shipdate)']) "
                            "AS SELECT * FROM tpch.sf1.lineitem WITH NO DATA")
        started = time.monotonic()
        for month in build:
            insert("direct", month)
        build_s = round(time.monotonic() - started, 1)
        started = time.monotonic()
        trino_query(server, f"CREATE TABLE {table('direct', 'big')} AS SELECT * FROM tpch.{scale}.lineitem")
        big_s = round(time.monotonic() - started, 1)
        layout = {kind: trino_query(server, f'SELECT count(*), sum(file_size_in_bytes), max(file_size_in_bytes) '
                                            f'FROM direct.bench."{kind}$files"')[0][0] for kind in ("lineitem", "big")}
        oracles = {}

        def expect(months):
            out = {}
            for query, kind, template in TRINO_QUERIES:
                if kind == "big" and query in oracles:
                    out[query] = oracles[query]
                    continue
                out[query] = trino_query(server, template.format(t=oracle(months, kind)))[0]
                if kind == "big":
                    oracles[query] = out[query]
            return out

        expected = expect(build)
        run("warm-up", "direct", expected, runs=1)
        checkpoint({"role": "direct", "queries": run("direct", "direct", expected)})
        cold = run("origin-cache cold", "via_b", expected)
        checkpoint({"role": "origin-cache cold", "queries": cold})
        warm = run("origin-cache warm", "via_a", expected)
        checkpoint({"role": "origin-cache warm", "queries": warm})
        assert all(q["origin_reads_total"] == 0 for q in warm.values()), ("gateway A re-read the origin", warm)
        checkpoint({"role": "alluxio", "queries": run("alluxio", "alluxio", expected)})
        checkpoint({"role": "direct, trino metadata cache", "includes_small_data_files": True,
                    "queries": run("direct cached", "direct_cached", expected)})
        checkpoint({"role": "origin-cache warm, trino metadata cache", "includes_small_data_files": True,
                    "queries": run("warm cached", "via_a_cached", expected)})

        if paired_cycles:
            def paired_query(provider, query, kind, template):
                catalog_name = TRINO_PAIRED_CATALOGS[provider]
                return trino_query(server, template.format(t=table(catalog_name, kind)))

            def checkpoint_paired(paired):
                report["paired"] = paired
                save()

            report["paired"] = trino_paired(paired_query, proxy, expected, paired_cycles, f"/{bucket}/", checkpoint_paired)
            paired_metrics = RUN / "gateway-a-metrics-paired.txt"
            paired_metrics.write_bytes(request(A, "/metrics")[1])
            report["paired"]["metrics"] = str(paired_metrics)
            save()

        if profile_enabled:
            report["profile"] = trino_profile(os.environ["PGVS3_TEST_PPROF_A"], run, expected)
            save()

        # Layout: rebuild the 78-month build cohort into one current snapshot
        # and manifest, and sort the large table by the month filter's column
        # so row-group statistics prune it. Historical Nessie commits remain,
        # but planning reads the current snapshot's manifests. Gateway B has
        # not read these files yet. Comparing this with `expected` keeps the
        # layout comparison on the same rows; the write-through and appended
        # inserts run afterwards.
        trino_query(server, "CREATE SCHEMA direct.tuned")
        started = time.monotonic()
        trino_query(server, f"CREATE TABLE {table('direct', schema='tuned')} WITH (partitioning = ARRAY['month(shipdate)']) "
                            f"AS SELECT * FROM {table('direct')}")
        trino_query(server, f"CREATE TABLE {table('direct', 'big', 'tuned')} WITH (sorted_by = ARRAY['shipdate']) "
                            f"AS SELECT * FROM tpch.{scale}.lineitem")
        tuned_s = round(time.monotonic() - started, 1)
        tuned = {}
        for role, catalog_name in (("direct", "direct"), ("cold", "via_b"), ("warm", "via_a")):
            queries = run(f"tuned {role}", catalog_name, expected, schema="tuned")
            tuned[role] = queries
            checkpoint({"role": f"tuned {role}", "queries": queries})
        assert all(q["origin_reads_total"] == 0 for q in tuned["warm"].values()), ("tuned warm read the origin", tuned["warm"])

        through_ms = [insert("via_a", month) for month in through]
        written = run("write-through", "via_b", expect(build + through), runs=1)
        checkpoint({"role": "write-through", "insert_ms": through_ms, "queries": written})
        assert all(q["origin_reads_total"] == 0 for q in written.values()), ("write-through commit was pulled from the origin", written)
        eventually("trino write-back drain", lambda: pending("trino-origin") == 0, 120)
        direct_ms = [insert("direct", month) for month in appended]
        checkpoint({"role": "direct insert", "insert_ms": direct_ms})
        final = expect(TRINO_MONTHS)
        run("final direct", "direct", final, runs=1)
        run("final cache", "via_b", final, runs=1)

        nessie_rows = int(sql("SELECT count(*) FROM objs2", DATABASE.replace("/pgvs3?", "/nessie?")))
    records = report["records"]
    median = lambda values: sorted(values)[len(values) // 2]
    roles = {record["role"]: record["queries"] for record in records if "queries" in record}
    summary = "; ".join(f"{query} {median(roles['direct'][query]['ms']):.0f}/{cold[query]['ms'][0]:.0f}/"
                        f"{median(warm[query]['ms']):.0f}/{median(roles['alluxio'][query]['ms']):.0f} ms"
                        for query, _, _ in TRINO_QUERIES)
    tuned_summary = "; ".join(f"{query} {median(tuned['direct'][query]['ms']):.0f}/{tuned['cold'][query]['ms'][0]:.0f}/"
                              f"{median(tuned['warm'][query]['ms']):.0f} ms" for query, _, _ in TRINO_QUERIES)
    files = ", ".join(f"{kind} {n} files / {total / (1 << 20):.0f} MiB" for kind, (n, total, _) in layout.items())
    report.update({"complete": True,
                   "files": {kind: {"count": n, "bytes": total, "max_bytes": largest}
                             for kind, (n, total, largest) in layout.items()},
                   "origin_latency_ms": latency * 1000, "range_chunk": range_chunk,
                   "build_s": build_s, "big_build_s": big_s, "tuned_build_s": tuned_s,
                   "tuned_months": len(build), "nessie_objects": nessie_rows})
    save()
    print(f"trino: {len(TRINO_MONTHS)} Nessie commits; {files}; "
          f"{latency * 1000:.0f} ms origin latency; direct/cold/warm/alluxio {summary}; insert direct "
          f"{median(direct_ms):.0f} ms vs through pgvs3 {median(through_ms):.0f} ms; rebuilt and sorted "
          f"direct/cold/warm {tuned_summary}; all roles match TPC-H", flush=True)


if __name__ == "__main__":
    scenarios = {"analytics": analytics, "replication": replication, "guides": guides, "raquet": raquet, "ortho": ortho,
                 "iceberg": iceberg, "trino": trino, "compute": compute}
    for name in sys.argv[1:]:
        scenarios[name]()
