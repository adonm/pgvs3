import json
import os
import platform
import re
import subprocess
import threading
import time
from urllib.request import urlopen


def lsn_bytes(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def metrics(text):
    return {name: float(value) for name, value in re.findall(r"^(pgvs3_(?:pg_pool|chunk_pool|spool|pending_uploads|s3_admission)\w*) ([0-9.eE+-]+)$", text, re.M)}


def metric_delta(before, after):
    counters = {name: value - before.get(name, 0) for name, value in after.items() if name.endswith("_total")}
    if any(value < 0 for value in counters.values()):
        raise RuntimeError("gateway restarted or metric counters went backwards during a sample")
    return counters


def host_info():
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (OSError, ValueError):
        memory = None
    return {"platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count(),
            "memory_bytes": memory}


class Capacity:
    def __init__(self, fixtures, replicas):
        self.fixtures, self.replicas = fixtures, replicas

    def query(self, database, statement, timeout=60):
        result = subprocess.run([self.fixtures.args.psql, database, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement],
                                check=True, capture_output=True, text=True, timeout=timeout)
        return result.stdout.strip()

    def replica(self, side):
        database = self.replicas.get(side)
        if not database:
            return None
        result = json.loads(self.query(database, "SELECT json_build_object('in_recovery',pg_is_in_recovery(),'receive_lsn',pg_last_wal_receive_lsn(),'replay_lsn',pg_last_wal_replay_lsn(),'system_identifier',(SELECT system_identifier::text FROM pg_control_system()))"))
        if not result["in_recovery"] or not result["replay_lsn"]:
            raise RuntimeError(f"capacity fixture is not an active physical standby: {result}")
        return result

    def gateway_metrics(self, side):
        with urlopen(self.fixtures.endpoints[side] + "/metrics", timeout=15) as response:
            values = metrics(response.read().decode())
        if "pgvs3_pg_pool_acquire_count_total" not in values or "pgvs3_spool_bytes" not in values:
            raise RuntimeError("missing required gateway capacity metrics")
        return values

    def settings(self, side):
        settings = self.fixtures.sql(side, "SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('server_version','block_size','shared_buffers','work_mem','max_connections','max_wal_size','wal_compression','full_page_writes','wal_level','synchronous_commit','synchronous_standby_names','checkpoint_timeout')")
        return json.loads(settings)

    def snapshot(self, side):
        namespaces = json.loads(self.fixtures.sql(side, "SELECT coalesce(json_agg(schema_name),'[]') FROM s3p.namespaces WHERE kind<>'queue'"))
        logical = " UNION ALL ".join(f"SELECT coalesce(sum(size),0)::bigint AS bytes FROM {self.ident(schema)}.objects" for schema in namespaces) or "SELECT 0::bigint AS bytes"
        statement = """SELECT json_build_object(
          'insert_lsn',pg_current_wal_insert_lsn(),'flush_lsn',pg_current_wal_flush_lsn(),
          'database_bytes',pg_database_size(current_database()),
          'logical_bytes',(SELECT coalesce(sum(bytes),0) FROM (""" + logical + """) logical),
          'pending',(SELECT json_build_object('rows',count(*),'bytes',coalesce(sum(size),0),'dead',count(*) FILTER (WHERE dead)) FROM s3p.pending_uploads),
          'relations',(SELECT coalesce(json_agg(json_build_object('schema',n.nspname,'relation',c.relname,
            'heap_bytes',pg_relation_size(c.oid),'index_bytes',pg_indexes_size(c.oid),'total_bytes',pg_total_relation_size(c.oid)) ORDER BY n.nspname,c.relname),'[]')
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind='r' AND (left(n.nspname,4) IN ('s3b_','s3c_','s3w_') OR n.nspname='s3p')),
          'checkpoints',(SELECT to_jsonb(c) FROM pg_stat_checkpointer c))"""
        result = json.loads(self.fixtures.sql(side, statement))
        result["metrics"] = self.gateway_metrics(side)
        result["replica"] = self.replica(side)
        return result

    @staticmethod
    def ident(value):
        return '"' + value.replace('"', '""') + '"'

    def catch_up(self, side, lsn):
        if not self.replicas.get(side):
            return None
        start = time.monotonic()
        deadline = start + 120
        while True:
            replica = self.replica(side)
            if lsn_bytes(replica["replay_lsn"]) >= lsn_bytes(lsn):
                return {"barrier_lsn": lsn, "replay_lsn": replica["replay_lsn"], "seconds": time.monotonic() - start}
            if time.monotonic() >= deadline:
                raise RuntimeError(f"replica did not replay through {lsn}")
            time.sleep(0.05)

    def prepare(self, side):
        start = time.monotonic()
        timeout = self.fixtures.args.maintenance_timeout
        self.fixtures.sql(side, "CHECKPOINT", timeout=timeout)
        barrier = self.fixtures.sql(side, "SELECT pg_current_wal_flush_lsn()")
        self.catch_up(side, barrier)
        if self.replicas.get(side):
            self.query(self.replicas[side], "CHECKPOINT", timeout=timeout)
        return {"policy": "matched primary checkpoint, replay barrier and standby restartpoint outside timed work", "seconds": time.monotonic() - start}

    def result(self, before, acknowledged, after, observer, drain_seconds, catchup):
        wal = lsn_bytes(after["insert_lsn"]) - lsn_bytes(before["insert_lsn"])
        ack_wal = lsn_bytes(acknowledged["insert_lsn"]) - lsn_bytes(before["insert_lsn"])
        if not 0 <= ack_wal <= wal:
            raise RuntimeError("PostgreSQL WAL position went backwards during a sample")
        if observer.errors:
            raise RuntimeError(f"capacity observation failed: {observer.errors}")
        return {"before": before, "after_loadgen": acknowledged, "after_drain": after,
                "wal_insert_bytes": wal, "wal_at_loadgen_completion_bytes": ack_wal,
                "metric_deltas": metric_delta(before["metrics"], after["metrics"]),
                "observed_peaks": observer.peaks, "observation_count": observer.samples,
                "process_spool_high_water_bytes": after["metrics"].get("pgvs3_spool_peak_bytes"),
                "after_loadgen_drain_seconds": drain_seconds, "replica_catch_up": catchup,
                "limits": "WAL is isolated-cluster LSN advancement including page headers/padding; gauge peaks are sampled lower bounds, cross-node position gaps include probe skew; spool high water is reserved payload since gateway start"}

    def lifecycle(self, side, prefix):
        f = self.fixtures
        quote = f.quote
        before = self.snapshot(side)
        if before["pending"]["rows"]:
            raise RuntimeError("refusing lifecycle cleanup with pending acknowledged bodies")
        folder = prefix + "/"
        keys = set()
        for namespace in json.loads(f.sql(side, "SELECT coalesce(json_agg(schema_name),'[]') FROM s3p.namespaces WHERE kind<>'queue'")):
            match = f.args.bucket + "/" + folder if namespace.startswith(("s3c_", "s3w_")) else folder
            raw = f.sql(side, f"SELECT coalesce(json_agg(key),'[]') FROM {self.ident(namespace)}.objects WHERE left(key,length({quote(match)}))={quote(match)}")
            for key in json.loads(raw):
                key = key.removeprefix(f.args.bucket + "/") if namespace.startswith(("s3c_", "s3w_")) else key
                key = key.split(".__range__.", 1)[0]
                if key.startswith(folder):
                    keys.add(key)
        from urllib.parse import quote as urlquote
        from urllib.request import Request
        for key in sorted(keys):
            with urlopen(Request(f.endpoints[side] + "/" + f.args.bucket + "/" + urlquote(key, safe="/"), method="DELETE"), timeout=120) as response:
                response.read()
        deleted = self.snapshot(side)
        if deleted["pending"]["rows"]:
            raise RuntimeError("refusing a lifecycle expiry drill with pending acknowledged bodies")
        for namespace in json.loads(f.sql(side, "SELECT coalesce(json_agg(schema_name),'[]') FROM s3p.namespaces WHERE kind<>'queue'")):
            match = f.args.bucket + "/" + folder if namespace.startswith(("s3c_", "s3w_")) else folder
            predicate = f"left(key,length({quote(match)}))={quote(match)}"
            f.sql(side, f"UPDATE {self.ident(namespace)}.objects SET expires_at=now()-interval '1 second' WHERE {predicate} AND expires_at IS NOT NULL")
            f.sql(side, f"UPDATE {self.ident(namespace)}.origin_metadata SET expires_at=now()-interval '1 second' WHERE {predicate}")
        expired = self.snapshot(side)
        binary = {"before": f.args.before_binary, "after": f.args.after_binary}[side]
        start = time.monotonic()
        deadline = start + f.args.maintenance_timeout
        while True:
            result = subprocess.run([binary, "reap", "--url", f.databases[side], "--grace", "0s", "--all"], check=True, capture_output=True,
                                    text=True, timeout=max(1, deadline - time.monotonic()))
            if re.search(r"\bgarbage_files=0\b", result.stdout):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"resource reaper did not drain: {result.stdout}")
            time.sleep(0.1)
        reap_seconds = time.monotonic() - start
        reaped = self.snapshot(side)
        start = time.monotonic()
        for namespace in json.loads(f.sql(side, "SELECT coalesce(json_agg(schema_name),'[]') FROM s3p.namespaces WHERE kind<>'queue'")):
            for relation in ("objects", "chunks", "origin_metadata"):
                f.sql(side, f"VACUUM (ANALYZE) {self.ident(namespace)}.{relation}", timeout=f.args.maintenance_timeout)
        vacuum_seconds = time.monotonic() - start
        vacuumed = self.snapshot(side)
        self.catch_up(side, vacuumed["insert_lsn"])
        prepared = self.prepare(side) if f.args.checkpoint_writes else None
        return {"keys_deleted": len(keys), "before": before, "after_delete": deleted, "after_expiry": expired, "after_reap": reaped, "after_vacuum": vacuumed,
                "maintenance": {"reap_seconds": reap_seconds, "vacuum_seconds": vacuum_seconds, "reap_result": result.stdout.strip(), "after_vacuum_checkpoint": prepared},
                "note": "Expiry deadlines are advanced only for this fixture's committed rows, never pending bodies; VACUUM makes dead space reusable, but need not shrink relation files"}


class Observer:
    def __init__(self, capacity, side):
        self.capacity, self.side = capacity, side
        self.stop = threading.Event()
        self.peaks, self.errors, self.samples = {}, [], 0
        self.thread = threading.Thread(target=self.observe, daemon=True)

    def sample(self):
        values = self.capacity.gateway_metrics(self.side)
        replica = self.capacity.replica(self.side)
        if replica:
            insert = self.capacity.fixtures.sql(self.side, "SELECT pg_current_wal_insert_lsn()")
            values["replay_lag_bytes"] = max(0, lsn_bytes(insert) - lsn_bytes(replica["replay_lsn"]))
        for key, value in values.items():
            if not key.endswith("_total"):
                self.peaks[key] = max(self.peaks.get(key, 0), value)
        self.samples += 1

    def observe(self):
        while not self.stop.wait(0.1):
            try:
                self.sample()
            except Exception as error:
                self.errors.append(str(error))
                return

    def __enter__(self):
        self.sample()
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=90)
        if self.thread.is_alive():
            raise RuntimeError("capacity observer did not stop")
        self.sample()
