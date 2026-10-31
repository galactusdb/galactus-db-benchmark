"""Galactus DB, Neo4j and Memgraph, measured side by side.

Every engine gets the same machine, the same CPU and memory limits, the same
synthetic graph, the same Python Bolt driver and the same Cypher text wherever
the engines accept it. Engines run one at a time on a fresh volume, and the
order rotates each pass so none always goes first or last.

Usage:
  python bench.py                        # all engines, 100,000 people, 3 passes
  python bench.py --engines gdb-resident,memgraph --passes 1
  python bench.py --scale 10000 --passes 1 --smoke   # quick check that it all runs

Results go to results/<timestamp>/:
  environment.json  host, Docker, images, limits and each server's effective settings
  raw.jsonl         every measurement as it was taken
  results.csv       one row per pass, engine, workload and metric
  summary.csv       the median of the passes, which the report charts
Then run `python report.py` to build report.html from the newest results.
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import pathlib
import platform
import random
import re
import statistics
import subprocess
import threading
import time

import neo4j
from neo4j import GraphDatabase

# The official Python driver only talks to servers whose Bolt agent starts with
# "Neo4j/". Galactus DB reports "GDB/<version>", so switch off that one check.
# The wire protocol is unchanged, and the patch applies to every engine alike.
import neo4j._sync.io as _neo4j_io  # noqa: E402

for _mod in (_neo4j_io, _neo4j_io._bolt3, _neo4j_io._bolt4, _neo4j_io._bolt5, _neo4j_io._common):
    if hasattr(_mod, "check_supported_server_product"):
        _mod.check_supported_server_product = lambda agent: None

HERE = pathlib.Path(__file__).resolve().parent
PASSWORD = os.environ.get("BENCH_PASSWORD", "benchmark-password-1")

# Container limits, identical for every engine within a profile. Neo4j's heap and
# page cache are what `neo4j-admin server memory-recommendation --memory=<mem>
# --docker` prints for that memory, so the JVM is sized the way Neo4j advises.
PROFILES = {
    "standard": {"BENCH_CPUS": "8", "BENCH_MEM": "4g", "NEO4J_HEAP": "2g", "NEO4J_PAGECACHE": "512m"},
    "restricted": {"BENCH_CPUS": "2", "BENCH_MEM": "1g", "NEO4J_HEAP": "307m", "NEO4J_PAGECACHE": "52m"},
}
LIMITS = dict(PROFILES["standard"])

# --- The statements --------------------------------------------------------
# Shared by every engine unless an engine lists its own text under "queries".

Q_CREATE_NODES = "UNWIND $rows AS r CREATE (:Person {id: r.id, name: r.name, age: r.age, city: r.city})"
Q_CREATE_RELS = ("UNWIND $pairs AS p MATCH (a:Person {id: p.a}) MATCH (b:Person {id: p.b}) "
                 "CREATE (a)-[:KNOWS {w: p.w}]->(b)")
Q_SEEK = "MATCH (p:Person {id: $id}) RETURN p.name AS name"
Q_SCAN = "MATCH (p:Person) WHERE p.age % 7 = 0 RETURN count(p) AS c"
Q_TWO_HOP = "MATCH (a:Person {id: $id})-[:KNOWS]->()-[:KNOWS]->(c) RETURN count(DISTINCT c) AS c"
Q_SP = ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
        "MATCH p = shortestPath((a)-[:KNOWS*..6]-(b)) RETURN length(p) AS hops")
Q_ASP = ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
         "MATCH p = allShortestPaths((a)-[:KNOWS*..6]-(b)) RETURN length(p) AS hops, count(p) AS paths")
Q_PROJECT = "CALL gds.graph.project('bench', 'Person', {KNOWS: {orientation: 'UNDIRECTED', properties: 'w'}})"
Q_WSP = ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
         "CALL gds.shortestPath.dijkstra.stream('bench', {sourceNode: a, targetNode: b, "
         "relationshipWeightProperty: 'w'}) YIELD totalCost RETURN totalCost AS cost")
Q_READY = "MATCH (p:Person {id: $id}) RETURN p.name AS name"

ENGINES = {
    "gdb-paged": {
        "label": "Galactus DB (Paged)",
        "uri": "bolt://127.0.0.1:27687",
        "user": "gdb",
        "database": "bench",
        "setup": ["CREATE DATABASE bench"],
        "schema": ["CREATE INDEX FOR (p:Person) ON (p.id)"],
    },
    "gdb-resident": {
        "label": "Galactus DB (Resident)",
        "uri": "bolt://127.0.0.1:27690",
        "user": "gdb",
        "database": "bench",
        "setup": ["CREATE DATABASE bench", "ALTER DATABASE bench SET ENGINE RESIDENT"],
        "schema": ["CREATE INDEX FOR (p:Person) ON (p.id)"],
    },
    "neo4j": {
        "label": "Neo4j Community",
        "uri": "bolt://127.0.0.1:27688",
        "user": "neo4j",
        "database": "neo4j",
        "setup": [],
        "schema": ["CREATE INDEX person_id FOR (p:Person) ON (p.id)", "CALL db.awaitIndexes(300)"],
    },
    "memgraph": {
        "label": "Memgraph Community",
        "uri": "bolt://127.0.0.1:27689",
        # Memgraph Community runs without users and with one database.
        "user": None,
        "database": None,
        "setup": [],
        "schema": ["CREATE INDEX ON :Person(id)"],
        # Memgraph has no shortestPath(), allShortestPaths() or GDS procedures.
        # These are its built-in equivalents, which return the same answers.
        "queries": {
            "Q_SP": ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
                     "MATCH p = (a)-[:KNOWS *BFS ..6]-(b) RETURN size(p) AS hops"),
            "Q_ASP": ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
                      "MATCH p = (a)-[:KNOWS *ALLSHORTEST ..6 (r, n | 1) total]-(b) "
                      "RETURN size(p) AS hops, count(p) AS paths"),
            "Q_PROJECT": None,
            "Q_WSP": ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
                      "MATCH p = (a)-[:KNOWS *WSHORTEST (r, n | r.w) total]-(b) RETURN total AS cost"),
        },
    },
}

CITIES = ["Avonmere", "Brackwater", "Coldharbour", "Dunlow", "Eastwick", "Fennick", "Glenhollow", "Harrowgate"]
UNITS = {"B": 1, "KB": 1e3, "KiB": 2**10, "MB": 1e6, "MiB": 2**20, "GB": 1e9, "GiB": 2**30}


def person(i):
    return {"id": i, "name": f"person-{i}", "age": 18 + (i * 7919) % 70, "city": CITIES[i % len(CITIES)]}


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else None


def sh(*args, check=False):
    r = subprocess.run(list(args), capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"{' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def compose(*args):
    r = subprocess.run(["docker", "compose", "-f", str(HERE / "docker-compose.yml"), *args],
                       capture_output=True, text=True, env={**os.environ, **LIMITS})
    if r.returncode != 0:
        raise SystemExit(f"docker compose {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def container_of(engine):
    return sh("docker", "compose", "-f", str(HERE / "docker-compose.yml"), "--profile", engine,
              "ps", "-q", engine)


def mem_bytes(container):
    out = sh("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container)
    m = re.match(r"([\d.]+)\s*([KMG]i?B|B)", out)
    return int(float(m.group(1)) * UNITS[m.group(2)]) if m else None


class MemPeak(threading.Thread):
    """Samples the container's memory once a second and keeps the peak."""

    def __init__(self, container):
        super().__init__(daemon=True)
        self.container, self.peak, self._halt = container, 0, threading.Event()

    def run(self):
        while not self._halt.is_set():
            self.peak = max(self.peak, mem_bytes(self.container) or 0)
            self._halt.wait(1.0)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self._halt.set()
        self.join(timeout=5)


class Run:
    """One engine, one pass: start fresh, load, measure, restart, stop."""

    def __init__(self, engine, args, pass_no, sink):
        self.engine, self.cfg, self.a, self.pass_no, self.sink = engine, ENGINES[engine], args, pass_no, sink
        self.rows = []

    # -- plumbing -----------------------------------------------------------
    def q(self, name):
        return self.cfg.get("queries", {}).get(name, globals()[name])

    def driver(self, **kw):
        auth = None if self.cfg["user"] is None else (self.cfg["user"], PASSWORD)
        return GraphDatabase.driver(self.cfg["uri"], auth=auth, **kw)

    def session(self, drv):
        db = self.cfg["database"]
        return drv.session() if db is None else drv.session(database=db)

    def metric(self, workload, metric, value, unit, better, **extra):
        row = {"pass": self.pass_no, "engine": self.engine, "workload": workload,
               "metric": metric, "value": value, "unit": unit, "better": better}
        self.rows.append(row)
        self.sink({**row, **extra})
        print(f"  {self.engine:13s} {workload:22s} {metric:16s} {value:12,.2f} {unit}")

    def answers(self, workload, results):
        """Fingerprint of every answer, so the report can show the engines agreed."""
        digest = hashlib.sha256(json.dumps(results, sort_keys=True).encode()).hexdigest()[:16]
        self.sink({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                   "kind": "answers", "sha": digest, "n": len(results), "sample": results[:5]})

    def latency(self, workload, statement, params, key, warm):
        """Times each statement on one session; the first `warm` runs are not counted."""
        lat, got = [], []
        with self.session(self.drv) as s:
            for i, p in enumerate(params):
                t0 = time.perf_counter()
                rec = s.run(statement, **p).data()
                dt_ = time.perf_counter() - t0
                if i >= warm:
                    lat.append(dt_ * 1000)
                    got.append(key(rec))
        self.metric(workload, "p50_ms", pct(lat, 50), "ms", "lower", p95_ms=pct(lat, 95), n=len(lat))
        self.metric(workload, "p95_ms", pct(lat, 95), "ms", "lower")
        self.answers(workload, got)

    # -- lifecycle -----------------------------------------------------------
    def start(self):
        compose("--profile", self.engine, "down", "-v")
        compose("--profile", self.engine, "up", "-d", self.engine)
        self.container = container_of(self.engine)
        self.wait_bolt()
        if self.cfg["setup"]:
            with self.driver() as d, d.session() as s:
                for stmt in self.cfg["setup"]:
                    s.run(stmt).consume()
        self.drv = self.driver(max_connection_pool_size=self.a.clients + 8)
        with self.session(self.drv) as s:
            for stmt in self.cfg["schema"]:
                s.run(stmt).consume()

    def wait_bolt(self, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with self.driver(connection_timeout=2) as d, d.session() as s:
                    s.run("RETURN 1").consume()
                    return
            except Exception:
                time.sleep(0.25)
        raise SystemExit(f"{self.engine} did not accept Bolt connections within {timeout} s")

    def stop(self):
        self.drv.close()
        compose("--profile", self.engine, "down", "-v")

    # -- workloads -------------------------------------------------------------
    def bulk_load(self, batch=1000):
        n = self.a.scale
        rng = random.Random(42)
        with MemPeak(self.container) as mem, self.session(self.drv) as s:
            t0 = time.perf_counter()
            for start in range(0, n, batch):
                s.run(self.q("Q_CREATE_NODES"), rows=[person(i) for i in range(start, min(start + batch, n))]).consume()
            t_nodes = time.perf_counter() - t0
            t1 = time.perf_counter()
            pairs = []
            for i in range(n):
                for _ in range(2):
                    pairs.append({"a": i, "b": rng.randrange(n), "w": rng.randint(1, 10)})
                if len(pairs) >= batch:
                    s.run(self.q("Q_CREATE_RELS"), pairs=pairs).consume()
                    pairs = []
            if pairs:
                s.run(self.q("Q_CREATE_RELS"), pairs=pairs).consume()
            t_rels = time.perf_counter() - t1
        self.metric("bulk_load", "nodes_per_s", n / t_nodes, "nodes/s", "higher")
        self.metric("bulk_load", "rels_per_s", 2 * n / t_rels, "rels/s", "higher")
        self.metric("bulk_load", "peak_mem_mb", mem.peak / 1e6, "MB", "lower")

    def idle_memory(self, settle=15):
        time.sleep(settle)
        self.metric("memory", "loaded_idle_mb", mem_bytes(self.container) / 1e6, "MB", "lower")

    def pairs(self, salt, count):
        rng = random.Random(f"{self.a.seed}-{salt}")
        return [{"a": rng.randrange(self.a.scale), "b": rng.randrange(self.a.scale)} for _ in range(count)]

    def ids(self, salt, count):
        rng = random.Random(f"{self.a.seed}-{salt}")
        return [{"id": rng.randrange(self.a.scale)} for _ in range(count)]

    def index_seek(self):
        self.latency("index_seek", self.q("Q_SEEK"), self.ids("seek", self.k(1000) + 50),
                     lambda r: r[0]["name"] if r else None, warm=50)

    def two_hop(self):
        self.latency("two_hop", self.q("Q_TWO_HOP"), self.ids("hop", self.k(300) + 20),
                     lambda r: r[0]["c"] if r else None, warm=20)

    def filtered_scan(self):
        self.latency("filtered_scan", self.q("Q_SCAN"), [{}] * (self.k(10) + 2),
                     lambda r: r[0]["c"] if r else None, warm=2)

    def shortest_path(self):
        self.latency("shortest_path", self.q("Q_SP"), self.pairs("sp", self.k(100) + 10),
                     lambda r: r[0]["hops"] if r else None, warm=10)

    def all_shortest_paths(self):
        self.latency("all_shortest_paths", self.q("Q_ASP"), self.pairs("asp", self.k(100) + 10),
                     lambda r: [r[0]["hops"], r[0]["paths"]] if r else None, warm=10)

    def weighted_shortest_path(self):
        project = self.q("Q_PROJECT")
        if project:
            # Neo4j and Galactus DB answer weighted paths from a named projection,
            # built once; Memgraph searches the stored graph directly.
            with self.session(self.drv) as s:
                t0 = time.perf_counter()
                s.run(project).consume()
                self.sink({"pass": self.pass_no, "engine": self.engine, "workload": "weighted_shortest_path",
                           "kind": "projection", "ms": (time.perf_counter() - t0) * 1000})
        self.latency("weighted_shortest_path", self.q("Q_WSP"), self.pairs("wsp", self.k(100) + 10),
                     lambda r: round(float(r[0]["cost"]), 6) if r else None, warm=10)

    def concurrent_read(self):
        clients, seconds = self.a.clients, self.a.seconds
        counts, lats, stop = [0] * clients, [[] for _ in range(clients)], threading.Event()

        def worker(k):
            rng = random.Random(f"{self.a.seed}-read-{k}")
            with self.session(self.drv) as s:
                while not stop.is_set():
                    t0 = time.perf_counter()
                    s.run(self.q("Q_SEEK"), id=rng.randrange(self.a.scale)).consume()
                    lats[k].append((time.perf_counter() - t0) * 1000)
                    counts[k] += 1

        threads = [threading.Thread(target=worker, args=(k,)) for k in range(clients)]
        with MemPeak(self.container) as mem:
            t0 = time.perf_counter()
            for t in threads:
                t.start()
            time.sleep(seconds)
            stop.set()
            for t in threads:
                t.join()
            elapsed = time.perf_counter() - t0
        flat = [x for l in lats for x in l]
        self.metric("concurrent_read", "reads_per_s", sum(counts) / elapsed, "reads/s", "higher", clients=clients)
        self.metric("concurrent_read", "p50_ms", pct(flat, 50), "ms", "lower")
        self.metric("concurrent_read", "peak_mem_mb", mem.peak / 1e6, "MB", "lower")

    def restart(self, stop_timeout=60):
        """Clean stop, then time from `docker start` to the first correct answer."""
        self.drv.close()
        sh("docker", "stop", "-t", str(stop_timeout), self.container, check=True)
        probe = self.a.scale - 1
        want = person(probe)["name"]
        t0 = time.perf_counter()
        sh("docker", "start", self.container, check=True)
        while True:
            try:
                with self.driver(connection_timeout=1) as d, self.session(d) as s:
                    rec = s.run(Q_READY, id=probe).single()
                    if rec and rec["name"] == want:
                        break
            except Exception:
                pass
            if time.perf_counter() - t0 > 900:
                raise SystemExit(f"{self.engine} did not answer within 900 s of restarting")
            time.sleep(0.02)
        self.metric("restart", "ready_s", time.perf_counter() - t0, "s", "lower")
        self.drv = self.driver(max_connection_pool_size=self.a.clients + 8)

    def k(self, n):
        return max(3, n // 20) if self.a.smoke else n

    def state(self):
        s = json.loads(sh("docker", "inspect", self.container) or "[{}]")[0].get("State", {})
        return {"running": s.get("Running"), "oom_killed": s.get("OOMKilled"), "exit_code": s.get("ExitCode")}

    def attempt(self, workload, fn):
        """Runs one workload. A failure is recorded as "did not finish" and the run goes on."""
        try:
            fn()
        except Exception as e:  # an engine running out of memory is a result, not a crash of the suite
            st = self.state()
            reason = "out of memory (killed)" if st["oom_killed"] else f"{type(e).__name__}: {str(e)[:200]}"
            self.rows.append({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                              "metric": "did_not_finish", "value": 1, "unit": reason, "better": "lower"})
            self.sink({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                       "kind": "failure", "reason": reason, "state": st})
            print(f"  {self.engine:13s} {workload:22s} DID NOT FINISH: {reason}")
            if not st["running"]:
                sh("docker", "start", self.container)
                try:
                    self.wait_bolt(timeout=300)
                except SystemExit:
                    pass
            self.drv.close()
            self.drv = self.driver(max_connection_pool_size=self.a.clients + 8)

    def all(self):
        self.start()
        try:
            self.attempt("bulk_load", self.bulk_load)
            self.attempt("memory", lambda: self.idle_memory(settle=2 if self.a.smoke else 15))
            for name in ("index_seek", "two_hop", "filtered_scan", "shortest_path", "all_shortest_paths",
                         "weighted_shortest_path", "concurrent_read", "restart"):
                self.attempt(name, getattr(self, name))
        finally:
            self.stop()
        return self.rows


# --- What the run was ---------------------------------------------------------

def image_facts(image):
    raw = sh("docker", "image", "inspect", image)
    if not raw:
        return {"image": image}
    i = json.loads(raw)[0]
    return {"image": image, "id": i["Id"], "repo_digests": i.get("RepoDigests", []),
            "size_mb": round(i["Size"] / 1e6, 1), "created": i.get("Created")}


def server_facts(engine):
    """Read back from the running server: version and the settings that matter."""
    cfg = ENGINES[engine]
    r = Run(engine, argparse.Namespace(clients=1), 0, lambda row: None)
    out = {}
    with r.driver() as d, r.session(d) as s:
        info = s.run("RETURN 1").consume().server
        out["agent"], out["bolt"] = info.agent, ".".join(map(str, info.protocol_version))
        if engine == "neo4j":
            c = s.run("CALL dbms.components() YIELD name, versions, edition RETURN *").single()
            out["version"], out["edition"] = c["versions"][0], c["edition"]
            out["settings"] = {x["name"]: x["value"] for x in s.run(
                "CALL dbms.listConfig() YIELD name, value WHERE name STARTS WITH 'server.memory' "
                "OR name STARTS WITH 'db.tx_log' OR name STARTS WITH 'db.checkpoint' RETURN name, value")}
        elif engine == "memgraph":
            out["version"] = s.run("SHOW VERSION").single()[0]
            out["settings"] = {x["name"]: x["current_value"] for x in s.run("SHOW CONFIG")
                               if x["name"].startswith(("storage", "memory", "query"))}
        else:
            out["version"] = info.agent.split("/")[-1]
            for line in sh("docker", "logs", container_of(engine)).splitlines():
                if "Bolt server listening" in line:
                    try:
                        out["settings"] = {k: v for k, v in json.loads(line).items()
                                           if k not in ("timestamp", "level", "message", "service_id")}
                    except json.JSONDecodeError:
                        pass
            out["engine"] = cfg["setup"][-1]
    return out


def cpu_name():
    """The processor's marketing name, which platform.processor() does not give on Windows or Linux."""
    try:
        if platform.system() == "Windows":
            return sh("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name").splitlines()[0].strip()
        if platform.system() == "Darwin":
            return sh("sysctl", "-n", "machdep.cpu.brand_string")
        for line in open("/proc/cpuinfo", encoding="utf-8"):
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except (OSError, IndexError):
        pass
    return platform.processor()


def host_facts():
    info = json.loads(sh("docker", "info", "--format", "{{json .}}") or "{}")
    return {
        "platform": platform.platform(), "processor": cpu_name(), "logical_cpus": os.cpu_count(),
        "python": platform.python_version(), "neo4j_python_driver": neo4j.__version__,
        "docker": {k: info.get(k) for k in ("ServerVersion", "OperatingSystem", "KernelVersion", "NCPU", "MemTotal")},
        "other_running_containers": sh("docker", "ps", "--format", "{{.Names}} ({{.Image}})").splitlines(),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engines", default=",".join(ENGINES), help="comma-separated: " + ", ".join(ENGINES))
    ap.add_argument("--scale", type=int, default=100_000, help="people in the graph; each knows two others")
    ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--clients", type=int, default=16, help="clients in the concurrent read phase")
    ap.add_argument("--seconds", type=int, default=10, help="length of the concurrent read phase")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--smoke", action="store_true", help="few samples: prove every statement runs")
    ap.add_argument("--profiles", default="standard,restricted",
                    help="limit profiles to run: standard (8 CPUs, 4 GB), restricted (2 CPUs, 1 GB)")
    a = ap.parse_args()
    engines = a.engines.split(",")
    for e in engines:
        if e not in ENGINES:
            raise SystemExit(f"unknown engine {e!r}; choose from {', '.join(ENGINES)}")

    stamp = HERE / "results" / dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    for profile in a.profiles.split(","):
        if profile not in PROFILES:
            raise SystemExit(f"unknown profile {profile!r}; choose from {', '.join(PROFILES)}")
        LIMITS.clear()
        LIMITS.update(PROFILES[profile])
        print(f"\n#### {profile}: {LIMITS['BENCH_CPUS']} CPUs, {LIMITS['BENCH_MEM']} memory per engine")
        run_profile(a, engines, profile, stamp / profile)
    print(f"\nResults: {stamp}\nNext: python report.py {stamp}")


def run_profile(a, engines, profile, out):
    out.mkdir(parents=True)
    raw = open(out / "raw.jsonl", "a", encoding="utf-8")

    def sink(row):
        raw.write(json.dumps(row) + "\n")
        raw.flush()

    env = {"started": dt.datetime.now().isoformat(timespec="seconds"), "params": vars(a),
           "profile": profile, "limits": dict(LIMITS),
           "bench_sha256": hashlib.sha256((HERE / "bench.py").read_bytes()).hexdigest(),
           "compose_sha256": hashlib.sha256((HERE / "docker-compose.yml").read_bytes()).hexdigest(),
           "host": host_facts(), "engines": {}}
    if env["host"]["other_running_containers"]:
        print("WARNING: other containers are running and will share the machine:",
              env["host"]["other_running_containers"])

    rows = []
    for p in range(1, a.passes + 1):
        shift = (p - 1) % len(engines)
        for e in engines[shift:] + engines[:shift]:
            print(f"== pass {p}: {e}")
            run = Run(e, a, p, sink)
            if e not in env["engines"]:
                # Record the exact image, limits and server settings once, from the live server.
                run.start()
                cid = run.container
                hc = json.loads(sh("docker", "inspect", cid))[0]["HostConfig"]
                env["engines"][e] = {
                    "label": ENGINES[e]["label"],
                    "image": image_facts(json.loads(sh("docker", "inspect", cid))[0]["Config"]["Image"]),
                    "limits": {"cpus": hc["NanoCpus"] / 1e9, "memory_bytes": hc["Memory"]},
                    "server": server_facts(e),
                    "setup": ENGINES[e]["setup"], "schema": ENGINES[e]["schema"],
                    "statements": {k: run.q(k) for k in ("Q_CREATE_NODES", "Q_CREATE_RELS", "Q_SEEK", "Q_TWO_HOP",
                                                         "Q_SCAN", "Q_SP", "Q_ASP", "Q_PROJECT", "Q_WSP")},
                }
                run.stop()
            rows += run.all()
    raw.close()

    limits = {json.dumps(v["limits"], sort_keys=True) for v in env["engines"].values()}
    env["limits_identical"] = len(limits) == 1
    if not env["limits_identical"]:
        print("WARNING: engines ran under different limits:", limits)
    (out / "environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")

    fields = ["pass", "engine", "workload", "metric", "value", "unit", "better"]
    with open(out / "results.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    groups = {}
    for r in rows:
        groups.setdefault((r["engine"], r["workload"], r["metric"], r["unit"], r["better"]), []).append(r["value"])
    with open(out / "summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["engine", "workload", "metric", "median", "min", "max", "passes", "unit", "better"])
        for (e, wl, m, u, b), vals in groups.items():
            w.writerow([e, wl, m, round(statistics.median(vals), 4), round(min(vals), 4), round(max(vals), 4),
                        len(vals), u, b])


if __name__ == "__main__":
    main()
