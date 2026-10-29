#!/usr/bin/env python3
from collections import Counter
from contextlib import ExitStack, contextmanager
from datetime import date
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
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
    def __init__(self):
        self.writes_blocked = False
        self.counts = Counter()
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

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
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    try:
                        self.wfile.write(data)
                    except (BrokenPipeError, ConnectionResetError):
                        pass

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = serve

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
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


def configure_origin(proxy, bucket, namespace, endpoints=(A, B), database=DATABASE, upstream="upstream"):
    request(ORIGIN, f"/{upstream}", "PUT", b"")
    request(ORIGIN, f"/{upstream}/{bucket}/probe", "PUT", b"probe")
    config = RUN / f"{namespace}-origin.json"
    config.write_text(json.dumps({"type": "s3", "config": {"bucket": upstream, "endpoint": proxy.endpoint, "access_key": "test", "secret_key": "test", "bucket_lookup_type": "path"}}))
    command("origin", "--url", database, "--bucket", bucket, "--cache-namespace", namespace,
            "--cache-ttl", "1h", "--range-cache-chunk", "65536", "set", str(config))
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


if __name__ == "__main__":
    {"analytics": analytics, "replication": replication}[sys.argv[1]]()
