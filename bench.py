"""Galactus DB, Neo4j, Memgraph, FalkorDB and ArangoDB, measured side by side.

Every engine gets the same machine, the same CPU and memory limits, the same
synthetic graph and the same Cypher text wherever the engines accept it. The
Bolt engines share one Python Bolt driver; FalkorDB is driven over its own
Redis protocol, and ArangoDB runs the same work in AQL over HTTP. Engines run one at a time on a fresh volume, and the order
rotates each pass so none always goes first or last.

Usage:
  python bench.py                        # all engines, 100,000 people, 3 passes
  python bench.py --engines gdb-resident,memgraph --passes 1
  python bench.py --scale 10000 --passes 1 --smoke   # quick check that it all runs
  python bench.py --inside               # the client beside each engine, on its Docker network

Results go to runs/<timestamp>/ (or --out <folder>/<timestamp>/):
  environment.json  host, Docker, images, limits and each server's effective settings
  raw.jsonl         every measurement as it was taken
  results.csv       one row per pass, engine, workload and metric
  summary.csv       the median of the passes, which the report charts
Then run `python report.py` to build report.html from the newest results.
"""
import argparse
import base64
import csv
import datetime as dt
import hashlib
import json
import multiprocessing
import os
import pathlib
import platform
import random
import re
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time

try:  # only --client driver needs it; the client container runs without it
    import neo4j
    import neo4j._sync.io as _neo4j_io
    from neo4j import GraphDatabase
except ImportError:
    neo4j = GraphDatabase = None
else:
    # The official Python driver only talks to servers whose Bolt agent starts with
    # "Neo4j/". Galactus DB reports "GDB/<version>", so switch off that one check.
    # The wire protocol is unchanged, and the patch applies to every engine alike.
    for _mod in (_neo4j_io, _neo4j_io._bolt3, _neo4j_io._bolt4, _neo4j_io._bolt5, _neo4j_io._common):
        if hasattr(_mod, "check_supported_server_product"):
            _mod.check_supported_server_product = lambda agent: None

HERE = pathlib.Path(__file__).resolve().parent
PASSWORD = os.environ.get("BENCH_PASSWORD", "benchmark-password-1")

# Container limits, identical for every engine within a profile. Neo4j's heap and
# page cache are what `neo4j-admin server memory-recommendation --memory=<mem>
# --docker` prints for that memory, so the JVM is sized the way Neo4j advises.
# ArangoDB is told the container's memory in bytes, since it cannot see the limit.
PROFILES = {
    "standard": {"BENCH_CPUS": "8", "BENCH_MEM": "4g", "NEO4J_HEAP": "2g", "NEO4J_PAGECACHE": "512m",
                 "ARANGODB_MEMORY": str(4 * 2**30)},
    "restricted": {"BENCH_CPUS": "2", "BENCH_MEM": "1g", "NEO4J_HEAP": "307m", "NEO4J_PAGECACHE": "52m",
                   "ARANGODB_MEMORY": str(2**30)},
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
Q_ROUND_TRIP = "RETURN 1 AS one"

# ArangoDB has no Cypher: it is queried in AQL, its own language. Each statement below asks
# for the same work and returns the same answer as the Cypher above, with the same names
# for the parameters (@id is $id) and the returned fields. People are documents in a
# Person collection and relationships are edges in a KNOWS edge collection.
AQL_ONE = "FOR a IN Person FILTER a.id == @a FOR b IN Person FILTER b.id == @b "
AQL = {
    "Q_CREATE_NODES": "FOR r IN @rows INSERT {id: r.id, name: r.name, age: r.age, city: r.city} INTO Person",
    "Q_CREATE_RELS": ("FOR p IN @pairs FOR a IN Person FILTER a.id == p.a FOR b IN Person FILTER b.id == p.b "
                      "INSERT {_from: a._id, _to: b._id, w: p.w} INTO KNOWS"),
    "Q_SEEK": "FOR p IN Person FILTER p.id == @id RETURN {name: p.name}",
    "Q_SCAN": "FOR p IN Person FILTER p.age % 7 == 0 COLLECT WITH COUNT INTO c RETURN {c}",
    # A traversal's default uniqueness (no edge twice on one path) is Cypher's.
    "Q_TWO_HOP": ("FOR a IN Person FILTER a.id == @id "
                  "RETURN {c: LENGTH(FOR c IN 2..2 OUTBOUND a KNOWS RETURN DISTINCT c._id)}"),
    # SHORTEST_PATH and ALL_SHORTEST_PATHS take no hop limit, so the limit of 6 is applied
    # to the answer. Every pair is chosen within 6 hops, so it never removes one.
    "Q_SP": (AQL_ONE + "LET p = (FOR v IN ANY SHORTEST_PATH a TO b KNOWS RETURN 1) "
             "FILTER LENGTH(p) > 1 AND LENGTH(p) <= 7 RETURN {hops: LENGTH(p) - 1}"),
    "Q_ASP": (AQL_ONE + "LET ps = (FOR p IN ANY ALL_SHORTEST_PATHS a TO b KNOWS RETURN LENGTH(p.edges)) "
              "FILTER LENGTH(ps) > 0 AND ps[0] <= 6 RETURN {hops: ps[0], paths: LENGTH(ps)}"),
    "Q_PROJECT": None,
    "Q_WSP": (AQL_ONE + "LET ws = (FOR v, e IN ANY SHORTEST_PATH a TO b KNOWS OPTIONS {weightAttribute: 'w'} "
              "RETURN e.w) FILTER LENGTH(ws) > 0 RETURN {cost: SUM(ws)}"),
    "Q_READY": "FOR p IN Person FILTER p.id == @id RETURN {name: p.name}",
    "Q_ROUND_TRIP": "RETURN {one: 1}",
}

def port(engine, default):
    """The host port an engine is published on. If a default is taken on your machine, set
    BENCH_PORT_ and the engine's name, as in BENCH_PORT_GDB_RESIDENT; docker-compose.yml
    reads the same variables."""
    return int(os.environ.get("BENCH_PORT_" + engine.upper().replace("-", "_"), default))


ENGINES = {
    "gdb-paged": {
        "label": "Galactus DB (Paged)",
        "port": port("gdb-paged", 27687),
        "user": "gdb",
        "database": "bench",
        "setup": ["CREATE DATABASE bench"],
        "schema": ["CREATE INDEX FOR (p:Person) ON (p.id)"],
    },
    "gdb-resident": {
        "label": "Galactus DB (Resident)",
        "port": port("gdb-resident", 27690),
        "user": "gdb",
        "database": "bench",
        "setup": ["CREATE DATABASE bench", "ALTER DATABASE bench SET ENGINE RESIDENT"],
        "schema": ["CREATE INDEX FOR (p:Person) ON (p.id)"],
    },
    "neo4j": {
        "label": "Neo4j Community",
        "port": port("neo4j", 27688),
        "user": "neo4j",
        "database": "neo4j",
        "setup": [],
        "schema": ["CREATE INDEX person_id FOR (p:Person) ON (p.id)", "CALL db.awaitIndexes(300)"],
    },
    "memgraph": {
        "label": "Memgraph Community",
        "port": port("memgraph", 27689),
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
    "falkordb": {
        "label": "FalkorDB",
        # FalkorDB is a Redis module. It is driven over the Redis protocol with the command its
        # client libraries send (GRAPH.QUERY ... --compact); see RespSession below.
        "protocol": "resp",
        "port": port("falkordb", 26379),
        # The image runs without a password, and a graph is a named key rather than a database.
        "user": None,
        "database": "bench",
        "setup": [],
        "schema": ["CREATE INDEX FOR (p:Person) ON (p.id)"],
        # FalkorDB takes shortestPath() in WITH or RETURN and allShortestPaths() once both ends
        # are bound, and answers weighted paths with its own algo.SPpaths procedure.
        "queries": {
            "Q_SP": ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
                     "WITH shortestPath((a)-[:KNOWS*..6]-(b)) AS p RETURN length(p) AS hops"),
            "Q_ASP": ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) WITH a, b "
                      "MATCH p = allShortestPaths((a)-[:KNOWS*..6]-(b)) "
                      "RETURN length(p) AS hops, count(p) AS paths"),
            "Q_PROJECT": None,
            "Q_WSP": ("MATCH (a:Person {id: $a}), (b:Person {id: $b}) "
                      "CALL algo.SPpaths({sourceNode: a, targetNode: b, relTypes: ['KNOWS'], "
                      "relDirection: 'both', weightProp: 'w'}) YIELD pathWeight RETURN pathWeight AS cost"),
        },
    },
}
# FalkorDB keeps writes in memory between snapshots unless Redis's append-only file is on.
# This is the same server with that file on, so every write reaches a log on disk, as it
# does in the other engines. Only loading and restarting differ from the entry above.
ENGINES["falkordb-aof"] = {**ENGINES["falkordb"], "label": "FalkorDB (append-only file)",
                           "port": port("falkordb-aof", 26380)}
ENGINES["arangodb"] = {
    "label": "ArangoDB Community",
    # ArangoDB answers AQL over HTTP with JSON: driven by a small client that posts each
    # statement to its cursor API, as its client libraries do; see HttpSession below.
    "protocol": "http",
    "port": port("arangodb", 28529),
    "user": "root",
    "database": "bench",
    # A database, a document collection and an edge collection are created through the
    # HTTP API (AQL cannot create them), then a persistent index on id like the others'.
    "setup": ['POST /_api/database {"name": "bench"}',
              'POST /_db/bench/_api/collection {"name": "Person"}',
              'POST /_db/bench/_api/collection {"name": "KNOWS", "type": 3}'],
    "schema": ['POST /_db/bench/_api/index?collection=Person {"type": "persistent", "fields": ["id"]}'],
    "queries": AQL,
}
INNER_PORTS = {"bolt": 7687, "resp": 6379, "http": 8529}

CITIES = ["Avonmere", "Brackwater", "Coldharbour", "Dunlow", "Eastwick", "Fennick", "Glenhollow", "Harrowgate"]
UNITS = {"B": 1, "KB": 1e3, "KiB": 2**10, "MB": 1e6, "MiB": 2**20, "GB": 1e9, "GiB": 2**30}


def person(i):
    return {"id": i, "name": f"person-{i}", "age": 18 + (i * 7919) % 70, "city": CITIES[i % len(CITIES)]}


def knows(n):
    """The relationships, from a fixed seed: each person knows two others, weight 1 to 10."""
    rng = random.Random(42)
    for i in range(n):
        for _ in range(2):
            yield {"a": i, "b": rng.randrange(n), "w": rng.randint(1, 10)}


_NEIGHBOURS = {}


def neighbours(n):
    """Undirected adjacency of the generated graph, used only to choose path endpoints."""
    if n not in _NEIGHBOURS:
        adj = [[] for _ in range(n)]
        for r in knows(n):
            adj[r["a"]].append(r["b"])
            adj[r["b"]].append(r["a"])
        _NEIGHBOURS[n] = adj
    return _NEIGHBOURS[n]


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else None


def sh(*args, check=False):
    r = subprocess.run(list(args), capture_output=True, text=True, encoding="utf-8", errors="replace")
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


# --- Minimal clients -----------------------------------------------------------
# A client library's own work is part of every latency it measures, and the
# libraries differ: the official Neo4j driver does far more per query than a Redis
# client does. With --client minimal every engine is driven by one of the two small
# clients below, which do the least their protocol allows: send the statement and
# its parameters, read the rows. Each offers the few calls of the Neo4j driver this
# file uses, so every workload is the same code for every engine and client.

class Rows:
    def __init__(self, rows):
        self.rows = rows

    def data(self):
        return self.rows

    def single(self):
        return self.rows[0] if self.rows else None

    def consume(self):
        return None


class Connection:
    """A TCP connection that a session owns for its lifetime."""

    def __init__(self, address, connection_timeout):
        # A connection opened with a timeout is a readiness probe: it must not wait
        # for ever on a server that accepts the connection and then says nothing.
        self.sock = socket.create_connection(address, timeout=connection_timeout or 30)
        self.sock.settimeout(max(10, connection_timeout) if connection_timeout else None)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = self.sock.makefile("rb")

    def read(self, n):
        data = self.reader.read(n)
        if len(data) < n:
            raise ConnectionError("the server closed the connection")
        return data

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.reader.close()
        self.sock.close()


class MinimalDriver:
    def __init__(self, session, address, auth, default=None, connection_timeout=None, **_):
        self.make, self.address, self.auth = session, address, auth
        self.default, self.timeout = default, connection_timeout

    def session(self, database=None):
        return self.make(self.address, self.auth, database or self.default, self.timeout)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


# FalkorDB speaks the Redis protocol. This sends what its client libraries send:
# GRAPH.QUERY <graph> "CYPHER name=value ... <statement>" --compact.

def cypher_literal(v):
    """A parameter value as Cypher text, the way FalkorDB's clients pass parameters."""
    if isinstance(v, str):
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(cypher_literal(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}:{cypher_literal(x)}" for k, x in v.items()) + "}"
    return repr(v)


def compact_value(cell):
    """One value of a --compact reply: [type, value]."""
    kind, v = cell
    if kind == 1:
        return None
    if kind == 4:
        return v == "true"
    if kind == 5:
        return float(v)
    if kind == 6:
        return [compact_value(x) for x in v]
    if kind == 10:
        return {v[i]: compact_value(v[i + 1]) for i in range(0, len(v), 2)}
    return v  # text and whole numbers arrive as they are


class RespSession(Connection):
    """One connection to one graph."""

    def __init__(self, address, auth, graph, connection_timeout):
        super().__init__(address, connection_timeout)
        self.graph = graph

    def command(self, *args):
        parts = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            parts.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.sock.sendall(b"".join(parts))
        return self.reply()

    def reply(self):
        line = self.reader.readline()
        if not line:
            raise ConnectionError("the server closed the connection")
        kind, rest = line[:1], line[1:-2]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            return None if rest == b"-1" else self.read(int(rest) + 2)[:-2].decode()
        if kind == b"*":
            return [self.reply() for _ in range(int(rest))]
        raise RuntimeError(f"unexpected reply {line!r}")

    def run(self, statement, **params):
        if params:
            statement = "CYPHER " + " ".join(f"{k}={cypher_literal(v)}" for k, v in params.items()) + " " + statement
        reply = self.command("GRAPH.QUERY", self.graph, statement, "--compact")
        if len(reply) < 3:  # a statement that returns nothing answers with its statistics alone
            return Rows([])
        names = [h[1] for h in reply[0]]
        return Rows([dict(zip(names, map(compact_value, row))) for row in reply[1]])


# Bolt 5, for Galactus DB, Neo4j and Memgraph: one RUN and one PULL per statement,
# as an auto-commit transaction, which is what the Neo4j driver's session.run sends.

def pack(v):
    """A value in PackStream, Bolt's encoding."""
    if v is None:
        return b"\xc0"
    if v is True:
        return b"\xc3"
    if v is False:
        return b"\xc2"
    if isinstance(v, int):
        if -16 <= v < 128:
            return struct.pack(">b", v)
        for marker, fmt, bits in ((0xC8, ">b", 7), (0xC9, ">h", 15), (0xCA, ">i", 31), (0xCB, ">q", 63)):
            if -(1 << bits) <= v < (1 << bits):
                return bytes([marker]) + struct.pack(fmt, v)
    if isinstance(v, float):
        return b"\xc1" + struct.pack(">d", v)
    if isinstance(v, str):
        data = v.encode()
        return sized(len(data), 0x80, 0xD0) + data
    if isinstance(v, (list, tuple)):
        return sized(len(v), 0x90, 0xD4) + b"".join(map(pack, v))
    if isinstance(v, dict):
        return sized(len(v), 0xA0, 0xD8) + b"".join(pack(k) + pack(x) for k, x in v.items())
    raise TypeError(f"cannot send {type(v).__name__} over Bolt")


def sized(n, tiny, marker):
    if n < 16:
        return bytes([tiny + n])
    if n < 256:
        return bytes([marker, n])
    return bytes([marker + 1]) + struct.pack(">H", n) if n < 65536 else bytes([marker + 2]) + struct.pack(">I", n)


def unpack(data, at=0):
    """One PackStream value from data, and the position after it."""
    m = data[at]
    at += 1
    if m < 0x80:
        return m, at
    if m >= 0xF0:
        return m - 256, at
    if m == 0xC0:
        return None, at
    if m in (0xC2, 0xC3):
        return m == 0xC3, at
    if m == 0xC1:
        return struct.unpack_from(">d", data, at)[0], at + 8
    if 0xC8 <= m <= 0xCB:
        fmt, size = ((">b", 1), (">h", 2), (">i", 4), (">q", 8))[m - 0xC8]
        return struct.unpack_from(fmt, data, at)[0], at + size
    if 0xB0 <= m <= 0xBF:  # a structure: its tag, then its fields
        tag, at, fields = data[at], at + 1, []
        for _ in range(m - 0xB0):
            v, at = unpack(data, at)
            fields.append(v)
        return (tag, fields), at
    for tiny, marker, kind in ((0x80, 0xD0, "text"), (0x90, 0xD4, "list"), (0xA0, 0xD8, "map"), (None, 0xCC, "bytes")):
        if tiny is not None and tiny <= m < tiny + 16:
            n = m - tiny
        elif marker <= m <= marker + 2:
            width = 1 << (m - marker)
            n = int.from_bytes(data[at:at + width], "big")
            at += width
        else:
            continue
        if kind in ("text", "bytes"):
            raw = bytes(data[at:at + n])
            return (raw.decode() if kind == "text" else raw), at + n
        items = []
        for _ in range(n * (2 if kind == "map" else 1)):
            v, at = unpack(data, at)
            items.append(v)
        return (dict(zip(items[::2], items[1::2])) if kind == "map" else items), at
    raise RuntimeError(f"unexpected PackStream marker {m:#x}")


HELLO, LOGON, RESET, RUN, PULL, SUCCESS, RECORD = 0x01, 0x6A, 0x0F, 0x10, 0x3F, 0x70, 0x71


class BoltSession(Connection):
    """One Bolt connection to one database."""

    def __init__(self, address, auth, database, connection_timeout):
        super().__init__(address, connection_timeout)
        self.extra = {"db": database} if database else {}
        # Offer Bolt 5.4 down to 5.0, then 4.4.
        self.sock.sendall(b"\x60\x60\xb0\x17" + bytes([0, 4, 4, 5, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0]))
        agreed = self.read(4)
        self.version = (agreed[3], agreed[2])
        if self.version < (4, 4):
            raise ConnectionError(f"the server offered no Bolt version this client speaks: {agreed!r}")
        hello = {"user_agent": "graph-benchmark/1.0"}
        if self.version >= (5, 3):
            hello["bolt_agent"] = {"product": "graph-benchmark/1.0"}
        logon = {"scheme": "basic", "principal": auth[0], "credentials": auth[1]} if auth else {"scheme": "none"}
        if self.version >= (5, 1):
            self.sock.sendall(self.message(HELLO, hello) + self.message(LOGON, logon))
            self.agent = self.expect_success().get("server")
            self.expect_success()
        else:
            self.sock.sendall(self.message(HELLO, {**hello, **logon}))
            self.agent = self.expect_success().get("server")

    @staticmethod
    def message(tag, *fields):
        data = bytes([0xB0 + len(fields), tag]) + b"".join(map(pack, fields))
        chunks = [struct.pack(">H", len(data[i:i + 65535])) + data[i:i + 65535] for i in range(0, len(data), 65535)]
        return b"".join(chunks) + b"\x00\x00"

    def receive(self):
        parts = []
        while True:
            (n,) = struct.unpack(">H", self.read(2))
            if n:
                parts.append(self.read(n))
            elif parts:  # an empty chunk ends a message; one on its own is a keep-alive
                return unpack(b"".join(parts))[0]

    def expect_success(self):
        tag, fields = self.receive()
        if tag != SUCCESS:
            raise RuntimeError(self.failure(tag, fields))
        return fields[0]

    def failure(self, tag, fields, pending=0):
        """Reads past the replies still owed, clears the failure, and describes it."""
        for _ in range(pending):
            self.receive()
        self.sock.sendall(self.message(RESET))
        self.receive()
        info = fields[0] if fields else {}
        return f"{info.get('code', f'reply {tag:#x}')}: {info.get('message', '')}"

    def run(self, statement, **params):
        self.sock.sendall(self.message(RUN, statement, params, self.extra) + self.message(PULL, {"n": -1}))
        tag, fields = self.receive()
        if tag != SUCCESS:
            raise RuntimeError(self.failure(tag, fields, pending=1))
        names, rows = fields[0].get("fields", []), []
        while True:
            tag, fields = self.receive()
            if tag == RECORD:
                rows.append(dict(zip(names, fields[0])))
            elif tag == SUCCESS:
                return Rows(rows)
            else:
                raise RuntimeError(self.failure(tag, fields))


# ArangoDB, over HTTP/1.1 with one kept-alive connection: each AQL statement is posted to
# the database's cursor API with its parameters as bind variables, as ArangoDB's client
# libraries do, and the rows come back in the reply.

class HttpSession(Connection):
    """One kept-alive HTTP connection to one database."""

    def __init__(self, address, auth, database, connection_timeout):
        super().__init__(address, connection_timeout)
        self.head = f"Host: {address[0]}:{address[1]}\r\n"
        if auth:
            self.head += "Authorization: Basic " + base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode() + "\r\n"
        self.base = f"/_db/{database or '_system'}"

    def request(self, method, path, body=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.sock.sendall(f"{method} {path} HTTP/1.1\r\n{self.head}Content-Type: application/json\r\n"
                          f"Content-Length: {len(data)}\r\n\r\n".encode() + data)
        status = self.reader.readline().split(b" ", 2)
        if len(status) < 2:
            raise ConnectionError("the server closed the connection")
        length, chunked = 0, False
        while True:
            line = self.reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            name = name.strip().lower()
            if name == "content-length":
                length = int(value)
            elif name == "transfer-encoding" and "chunked" in value.lower():
                chunked = True
        if chunked:
            raw = b""
            while True:
                n = int(self.reader.readline().split(b";")[0], 16)
                raw += self.read(n + 2)[:-2]
                if n == 0:
                    break
        else:
            raw = self.read(length) if length else b""
        out = json.loads(raw) if raw else {}
        if int(status[1]) >= 400:
            raise RuntimeError(f"{int(status[1])} {out.get('errorNum', '')}: {out.get('errorMessage', raw[:200])}")
        return out

    def run(self, statement, **params):
        method, _, rest = statement.partition(" ")
        if method in ("GET", "POST", "PUT", "DELETE") and rest.startswith("/"):
            # A call to the HTTP API itself, used only to set up the database.
            path, _, body = rest.partition(" ")
            return Rows([self.request(method, path, json.loads(body) if body else None)])
        req = {"query": statement, "batchSize": 10000}
        if params:
            req["bindVars"] = params
        out = self.request("POST", self.base + "/_api/cursor", req)
        rows = out.get("result", [])
        while out.get("hasMore"):
            out = self.request("POST", f"{self.base}/_api/cursor/{out['id']}")
            rows += out["result"]
        return Rows(rows)


# --- The timed work ---------------------------------------------------------------
# Everything that is timed is one of these jobs. They run in this process, or with
# --inside in a container on the engine's own Docker network (see Worker), so that
# no port forwarding sits between client and engine. A target says where the engine
# is and which client reaches it.

def open_driver(target, **kw):
    auth = tuple(target["auth"]) if target["auth"] else None
    address = (target["address"][0], target["address"][1])
    if target["kind"] == "resp":
        return MinimalDriver(RespSession, address, auth, target["database"], **kw)
    if target["kind"] == "http":
        return MinimalDriver(HttpSession, address, auth, **kw)
    if target["kind"] == "bolt":
        return MinimalDriver(BoltSession, address, auth, **kw)
    if GraphDatabase is None:
        raise SystemExit("--client driver needs the Neo4j Python driver: pip install -r requirements.txt")
    return GraphDatabase.driver("bolt://%s:%d" % address, auth=auth, **kw)


def open_session(target, drv):
    db = target["database"]
    return drv.session() if db is None else drv.session(database=db)


def job_load(target, nodes, rels, scale, batch):
    """Creates the people, then the relationships, in batches on one session."""
    with open_driver(target) as d, open_session(target, d) as s:
        t0 = time.perf_counter()
        for start in range(0, scale, batch):
            s.run(nodes, rows=[person(i) for i in range(start, min(start + batch, scale))]).consume()
        t_nodes = time.perf_counter() - t0
        t1 = time.perf_counter()
        pairs = []
        for r in knows(scale):
            pairs.append(r)
            if len(pairs) >= batch:
                s.run(rels, pairs=pairs).consume()
                pairs = []
        if pairs:
            s.run(rels, pairs=pairs).consume()
        return {"nodes_s": t_nodes, "rels_s": time.perf_counter() - t1}


def job_latency(target, statement, params, warm):
    """Times each statement on one session; the first `warm` runs are not counted."""
    lat, got = [], []
    with open_driver(target) as d, open_session(target, d) as s:
        for i, p in enumerate(params):
            t0 = time.perf_counter()
            rec = s.run(statement, **p).data()
            dt_ = time.perf_counter() - t0
            if i >= warm:
                lat.append(dt_ * 1000)
                got.append(rec)
    return {"ms": lat, "records": got}


def reader(target, statement, k, begin, seconds, seed, scale):
    """One client of the concurrent phase: reads from `begin` until the window closes."""
    rng, lat, end = random.Random(f"{seed}-read-{k}"), [], begin + seconds
    with open_driver(target) as d, open_session(target, d) as s:
        s.run(statement, id=rng.randrange(scale)).consume()  # connected before the window opens
        time.sleep(max(0.0, begin - time.time()))
        started = time.time()
        if end - started < seconds / 2:
            raise RuntimeError("a client process started too late to measure")
        while time.time() < end:
            t0 = time.perf_counter()
            s.run(statement, id=rng.randrange(scale)).consume()
            lat.append((time.perf_counter() - t0) * 1000)
    return len(lat) / (end - started), lat


def job_concurrent(target, statement, clients, seconds, seed, scale):
    """Index lookups from several clients at once. Each client is its own process with its
    own connection, so the clients do not queue for one Python interpreter."""
    lead = 2 if multiprocessing.get_start_method() == "fork" else 3 + 0.3 * clients
    begin = time.time() + lead
    with multiprocessing.Pool(clients) as pool:
        parts = pool.starmap(reader, [(target, statement, k, begin, seconds, seed, scale) for k in range(clients)],
                             chunksize=1)
    return {"per_s": sum(rate for rate, _ in parts), "p50_ms": pct([x for _, lat in parts for x in lat], 50)}


JOBS = {"load": job_load, "latency": job_latency, "concurrent": job_concurrent}


def serve():
    """`bench.py --worker`: runs the jobs sent on standard input, one JSON line each."""
    for line in sys.stdin:
        req = json.loads(line)
        try:
            out = {"ok": JOBS[req["job"]](**req["args"])}
        except Exception as e:
            out = {"error": str(e)[:300] or type(e).__name__}
        print(json.dumps(out), flush=True)


class Worker:
    """The jobs, run by `bench.py --worker` in the client container."""

    def __init__(self, container):
        self.proc = subprocess.Popen(
            ["docker", "exec", "-i", container, "python", "-u", "/bench/bench.py", "--worker"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")

    def call(self, job, **args):
        self.proc.stdin.write(json.dumps({"job": job, "args": args}) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError("the client container stopped answering")
        out = json.loads(line)
        if "error" in out:
            raise RuntimeError(out["error"])
        return out["ok"]

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()


WORKLOADS = ("bulk_load", "memory", "round_trip", "index_seek", "two_hop", "filtered_scan", "shortest_path",
             "all_shortest_paths", "weighted_shortest_path", "concurrent_read", "restart")


class Run:
    """One engine, one pass: start fresh, load, measure, restart, stop."""

    def __init__(self, engine, args, pass_no, sink):
        self.engine, self.cfg, self.a, self.pass_no = engine, ENGINES[engine], args, pass_no
        self.keep = set(getattr(args, "workloads", None) or WORKLOADS)
        # Only the selected workloads are recorded; the graph is always loaded first.
        self.sink = lambda row: sink(row) if row.get("workload") in self.keep else None
        self.rows = []
        self.worker = None

    # -- plumbing -----------------------------------------------------------
    def q(self, name):
        return self.cfg.get("queries", {}).get(name, globals()[name])

    def target(self, inside=False):
        """Where the engine is and which client reaches it: from this machine by its published
        port, or from the client container by its name on the Docker network."""
        protocol = self.cfg.get("protocol", "bolt")
        kind = protocol if protocol != "bolt" else "bolt" if self.a.client == "minimal" else "driver"
        return {"kind": kind,
                "address": [self.engine, INNER_PORTS[protocol]] if inside else ["127.0.0.1", self.cfg["port"]],
                "auth": None if self.cfg["user"] is None else [self.cfg["user"], PASSWORD],
                "database": self.cfg["database"]}

    def driver(self, **kw):
        return open_driver(self.target(), **kw)

    def session(self, drv):
        return open_session(self.target(), drv)

    def work(self, job, **args):
        """Runs a timed job in this process or, with --inside, in the client container."""
        if self.worker is None:
            return JOBS[job](self.target(), **args)
        return self.worker.call(job, target=self.target(inside=True), **args)

    def metric(self, workload, metric, value, unit, better, **extra):
        row = {"pass": self.pass_no, "engine": self.engine, "workload": workload,
               "metric": metric, "value": value, "unit": unit, "better": better}
        if workload in self.keep:
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
        out = self.work("latency", statement=statement, params=params, warm=warm)
        lat, got = out["ms"], [key(rec) for rec in out["records"]]
        self.metric(workload, "p50_ms", pct(lat, 50), "ms", "lower", p95_ms=pct(lat, 95), n=len(lat))
        self.metric(workload, "p95_ms", pct(lat, 95), "ms", "lower")
        self.answers(workload, got)

    # -- lifecycle -----------------------------------------------------------
    def start(self):
        self.down()
        compose("--profile", self.engine, "up", "-d", self.engine)
        self.container = container_of(self.engine)
        self.wait_ready()
        if getattr(self.a, "inside", False):
            compose("--profile", "client", "up", "-d", "client")
            self.worker = Worker(container_of("client"))
        if self.cfg["setup"]:
            with self.driver() as d, d.session() as s:
                for stmt in self.cfg["setup"]:
                    s.run(stmt).consume()
        self.drv = self.driver(max_connection_pool_size=self.a.clients + 8)
        with self.session(self.drv) as s:
            for stmt in self.cfg["schema"]:
                s.run(stmt).consume()

    def wait_ready(self, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with self.driver(connection_timeout=2) as d, d.session() as s:
                    s.run("RETURN 1").consume()
                    return
            except Exception:
                time.sleep(0.25)
        raise SystemExit(f"{self.engine} did not accept connections within {timeout} s")

    def stop(self):
        self.drv.close()
        if self.worker:
            self.worker.close()
            self.worker = None
        self.down()

    def down(self):
        compose("--profile", self.engine, "--profile", "client", "down", "-v")

    # -- workloads -------------------------------------------------------------
    def bulk_load(self, batch=1000):
        n = self.a.scale
        with MemPeak(self.container) as mem:
            took = self.work("load", nodes=self.q("Q_CREATE_NODES"), rels=self.q("Q_CREATE_RELS"),
                             scale=n, batch=batch)
        self.metric("bulk_load", "nodes_per_s", n / took["nodes_s"], "nodes/s", "higher")
        self.metric("bulk_load", "rels_per_s", 2 * n / took["rels_s"], "rels/s", "higher")
        self.metric("bulk_load", "peak_mem_mb", mem.peak / 1e6, "MB", "lower")

    def idle_memory(self, settle=15):
        time.sleep(settle)
        self.metric("memory", "loaded_idle_mb", mem_bytes(self.container) / 1e6, "MB", "lower")

    def pairs(self, salt, count):
        rng = random.Random(f"{self.a.seed}-{salt}")
        return [{"a": rng.randrange(self.a.scale), "b": rng.randrange(self.a.scale)} for _ in range(count)]

    def linked_pairs(self, salt, count):
        """Pairs with a path of at most 6 hops between them: a random walk of 2 to 6 steps
        from a random person. Random pairs in a sparse graph are rarely within 6 hops, and a
        search that finds nothing is not the work these tests are about."""
        rng, adj, out = random.Random(f"{self.a.seed}-{salt}"), neighbours(self.a.scale), []
        while len(out) < count:
            a = b = rng.randrange(self.a.scale)
            for _ in range(rng.randint(2, 6)):
                b = rng.choice(adj[b])
            if b != a:
                out.append({"a": a, "b": b})
        return out

    def ids(self, salt, count):
        rng = random.Random(f"{self.a.seed}-{salt}")
        return [{"id": rng.randrange(self.a.scale)} for _ in range(count)]

    def round_trip(self):
        """An empty query: what the protocol, the client and the path to the engine cost."""
        self.latency("round_trip", self.q("Q_ROUND_TRIP"), [{}] * (self.k(1000) + 50),
                     lambda r: r[0]["one"] if r else None, warm=50)

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
        self.latency("shortest_path", self.q("Q_SP"), self.linked_pairs("sp", self.k(100) + 10),
                     lambda r: r[0]["hops"] if r else None, warm=10)

    def all_shortest_paths(self):
        self.latency("all_shortest_paths", self.q("Q_ASP"), self.linked_pairs("asp", self.k(100) + 10),
                     lambda r: [r[0]["hops"], r[0]["paths"]] if r else None, warm=10)

    def weighted_shortest_path(self):
        project, built = self.q("Q_PROJECT"), 0.0
        if project:
            # Neo4j and Galactus DB answer weighted paths from a named projection,
            # built once; Memgraph and FalkorDB search the stored graph directly.
            built = self.work("latency", statement=project, params=[{}], warm=0)["ms"][0]
        self.metric("weighted_shortest_path", "projection_ms", built, "ms", "lower")
        self.latency("weighted_shortest_path", self.q("Q_WSP"), self.pairs("wsp", self.k(100) + 10),
                     lambda r: round(float(r[0]["cost"]), 6) if r else None, warm=10)

    def concurrent_read(self):
        with MemPeak(self.container) as mem:
            out = self.work("concurrent", statement=self.q("Q_SEEK"), clients=self.a.clients,
                            seconds=self.a.seconds, seed=self.a.seed, scale=self.a.scale)
        self.metric("concurrent_read", "reads_per_s", out["per_s"], "reads/s", "higher", clients=self.a.clients)
        self.metric("concurrent_read", "p50_ms", out["p50_ms"], "ms", "lower")
        self.metric("concurrent_read", "peak_mem_mb", mem.peak / 1e6, "MB", "lower")

    def restart(self, stop_timeout=60):
        """Clean stop, then time from `docker start` to the first correct answer. Always asked
        from this machine: a stopped container has no name on the Docker network to ask."""
        self.drv.close()
        sh("docker", "stop", "-t", str(stop_timeout), self.container, check=True)
        probe = self.a.scale - 1
        want = person(probe)["name"]
        t0 = time.perf_counter()
        sh("docker", "start", self.container, check=True)
        while True:
            try:
                with self.driver(connection_timeout=1) as d, self.session(d) as s:
                    rec = s.run(self.q("Q_READY"), id=probe).single()
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
        """Runs one workload and says whether it finished. A failure is recorded as "did not
        finish" and the run goes on."""
        kept = len(self.rows)
        try:
            fn()
        except Exception as e:  # an engine running out of memory is a result, not a crash of the suite
            del self.rows[kept:]  # a failed workload has no measurements, only the failure
            st = self.state()
            reason = "out of memory (killed)" if st["oom_killed"] else f"{type(e).__name__}: {str(e)[:200]}"
            if workload in self.keep:
                self.rows.append({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                                  "metric": "did_not_finish", "value": 1, "unit": reason, "better": "lower"})
            self.sink({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                       "kind": "failure", "reason": reason, "state": st})
            print(f"  {self.engine:13s} {workload:22s} DID NOT FINISH: {reason}")
            if not st["running"]:
                sh("docker", "start", self.container)
                try:
                    self.wait_ready(timeout=300)
                except SystemExit:
                    pass
            self.drv.close()
            self.drv = self.driver(max_connection_pool_size=self.a.clients + 8)
            return False
        return True

    def skip(self, workload, reason):
        """Records a workload that could not be run as did not finish."""
        self.rows.append({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                          "metric": "did_not_finish", "value": 1, "unit": reason, "better": "lower"})
        self.sink({"pass": self.pass_no, "engine": self.engine, "workload": workload,
                   "kind": "failure", "reason": reason, "state": self.state()})
        print(f"  {self.engine:13s} {workload:22s} DID NOT FINISH: {reason}")

    def all(self):
        self.start()
        try:
            loaded = self.attempt("bulk_load", self.bulk_load)
            for name in WORKLOADS[1:]:
                if name not in self.keep:
                    continue
                if not loaded:
                    # Whatever the server holds is not the graph, so nothing timed on it counts.
                    self.skip(name, "the graph did not load")
                elif name == "memory":
                    self.attempt("memory", lambda: self.idle_memory(settle=2 if self.a.smoke else 15))
                else:
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
    r = Run(engine, argparse.Namespace(clients=1, client="driver"), 0, lambda row: None)
    out = {}
    if cfg.get("protocol") == "resp":
        with r.driver() as d, r.session(d) as s:
            module = next(dict(zip(m[::2], m[1::2])) for m in s.command("MODULE", "LIST") if "graph" in m)
            redis = dict(l.split(":", 1) for l in s.command("INFO", "server").splitlines() if ":" in l)
            ver = module["ver"]
            out["version"] = f"{ver // 10000}.{ver // 100 % 100}.{ver % 100}"
            out["agent"] = f"Redis {redis.get('redis_version')}, module graph"
            out["protocol"] = "Redis protocol (RESP2), GRAPH.QUERY --compact"
            out["settings"] = {k: v for k, v in s.command("GRAPH.CONFIG", "GET", "*")}
            for name in ("save", "appendonly", "appendfsync", "maxmemory"):
                out["settings"]["redis " + name] = s.command("CONFIG", "GET", name)[1]
        return out
    if cfg.get("protocol") == "http":
        with r.driver() as d, d.session() as s:
            v = s.request("GET", "/_api/version?details=true")
            out["version"], out["edition"] = v["version"], v.get("license")
            out["agent"] = f"ArangoDB {v['version']} ({v['details'].get('mode', '')})"
            out["protocol"] = "HTTP/1.1 with JSON, AQL through the cursor API"
            out["settings"] = {"ARANGODB_OVERRIDE_DETECTED_TOTAL_MEMORY": LIMITS.get("ARANGODB_MEMORY"),
                               "ARANGODB_OVERRIDE_DETECTED_NUMBER_OF_CORES": LIMITS.get("BENCH_CPUS")}
        # The options API is closed to password logins, so the values are read from arangod in the
        # same container, which computes them from the same detected memory and cores. Nothing
        # in the configuration file sets any of them.
        opts = json.loads(sh("docker", "exec", container_of(engine), "arangod", "--dump-options") or "{}")
        out["settings"].update({k: opts[k]["default"] for k in sorted(opts) if k in (
            "rocksdb.sync-interval", "rocksdb.block-cache-size", "rocksdb.total-write-buffer-size",
            "cache.size", "query.memory-limit", "query.global-memory-limit")})
        return out
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
        "python": platform.python_version(), "neo4j_python_driver": neo4j.__version__ if neo4j else None,
        "docker": {k: info.get(k) for k in ("ServerVersion", "OperatingSystem", "KernelVersion", "NCPU", "MemTotal")},
        "other_running_containers": sh("docker", "ps", "--format", "{{.Names}} ({{.Image}})").splitlines(),
    }


def main():
    if sys.argv[1:] == ["--worker"]:
        return serve()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engines", default=",".join(ENGINES), help="comma-separated: " + ", ".join(ENGINES))
    ap.add_argument("--scale", type=int, default=100_000, help="people in the graph; each knows two others")
    ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--clients", type=int, default=16, help="clients in the concurrent read phase")
    ap.add_argument("--seconds", type=int, default=10, help="length of the concurrent read phase")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--client", choices=("driver", "minimal"), default="driver",
                    help="driver: the official Neo4j Python driver for the Bolt engines. minimal: a small "
                         "client in this file for every engine, so no client library's own work is measured. "
                         "FalkorDB and ArangoDB are always driven by the minimal client for their protocol")
    ap.add_argument("--smoke", action="store_true", help="few samples: prove every statement runs")
    ap.add_argument("--profiles", default="standard,restricted",
                    help="limit profiles to run: standard (8 CPUs, 4 GB), restricted (2 CPUs, 1 GB)")
    ap.add_argument("--out", default=os.environ.get("BENCH_RESULTS", str(HERE / "runs")),
                    help="folder for results (default runs/, or BENCH_RESULTS)")
    ap.add_argument("--workloads", type=lambda v: v.split(","),
                    help="run only these workloads (comma-separated): " + ", ".join(WORKLOADS))
    ap.add_argument("--into", help="an earlier run folder: replace these engines' results for these "
                                   "workloads in it, and record which image re-measured them")
    ap.add_argument("--inside", action="store_true",
                    help="send the timed queries from a container on the engines' Docker network instead of "
                         "from this machine, so no port forwarding sits between client and engine. Uses the "
                         "minimal clients")
    a = ap.parse_args()
    if a.inside:
        a.client = "minimal"
    for wl in a.workloads or ():
        if wl not in WORKLOADS:
            raise SystemExit(f"unknown workload {wl!r}; choose from {', '.join(WORKLOADS)}")
    engines = a.engines.split(",")
    for e in engines:
        if e not in ENGINES:
            raise SystemExit(f"unknown engine {e!r}; choose from {', '.join(ENGINES)}")

    stamp = (pathlib.Path(a.into) if a.into
             else pathlib.Path(a.out) / dt.datetime.now().strftime("%Y-%m-%d_%H%M%S"))
    for profile in a.profiles.split(","):
        if profile not in PROFILES:
            raise SystemExit(f"unknown profile {profile!r}; choose from {', '.join(PROFILES)}")
        LIMITS.clear()
        LIMITS.update(PROFILES[profile])
        print(f"\n#### {profile}: {LIMITS['BENCH_CPUS']} CPUs, {LIMITS['BENCH_MEM']} memory per engine")
        (merge_profile if a.into else run_profile)(a, engines, profile, stamp / profile)
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
    if a.inside:
        env["client_image"] = image_facts(os.environ.get("BENCH_CLIENT_IMAGE", "python:3.13-slim"))
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
                env["engines"][e] = engine_facts(run)
            rows += run.all()
    raw.close()

    limits = {json.dumps(v["limits"], sort_keys=True) for v in env["engines"].values()}
    env["limits_identical"] = len(limits) == 1
    if not env["limits_identical"]:
        print("WARNING: engines ran under different limits:", limits)
    (out / "environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")
    write_tables(out, rows)


def engine_facts(run):
    """The exact image, limits and server settings, read once from the live server."""
    e = run.engine
    run.start()
    try:
        info = json.loads(sh("docker", "inspect", run.container))[0]
        hc = info["HostConfig"]
        return {
            "label": ENGINES[e]["label"],
            "image": image_facts(info["Config"]["Image"]),
            "limits": {"cpus": hc["NanoCpus"] / 1e9, "memory_bytes": hc["Memory"]},
            "server": server_facts(e),
            "setup": ENGINES[e]["setup"], "schema": ENGINES[e]["schema"],
            "statements": {k: run.q(k) for k in ("Q_CREATE_NODES", "Q_CREATE_RELS", "Q_SEEK", "Q_TWO_HOP",
                                                 "Q_SCAN", "Q_SP", "Q_ASP", "Q_PROJECT", "Q_WSP")},
        }
    finally:
        run.stop()


def merge_profile(a, engines, profile, out):
    """Re-measures some workloads for some engines and puts them into an earlier run.

    The earlier rows for those engines and workloads are replaced, every other row is
    kept, and environment.json records the image, code and date of the re-measurement.
    """
    if not (out / "environment.json").exists():
        raise SystemExit(f"no earlier run in {out}")
    env = json.loads((out / "environment.json").read_text(encoding="utf-8"))
    if any(env["limits"].get(k, v) != v for k, v in LIMITS.items()):  # runs before a setting existed lack it
        raise SystemExit(f"{out} ran under {env['limits']}, not {LIMITS}")
    for key in ("scale", "client", "inside", "clients", "seconds", "seed"):
        if key in env["params"] and env["params"][key] != getattr(a, key):
            raise SystemExit(f"{out} ran with --{key} {env['params'][key]}, not {getattr(a, key)}")
    workloads = a.workloads or list(WORKLOADS)
    replaced = {(e, w) for e in engines for w in workloads}

    def kept(r):
        return (r.get("engine"), r.get("workload")) not in replaced

    with open(out / "results.csv", encoding="utf-8") as f:
        rows = [{**r, "pass": int(r["pass"]), "value": float(r["value"]) if r["value"] else None}
                for r in csv.DictReader(f) if kept(r)]
    raw_lines = [line for line in (out / "raw.jsonl").read_text(encoding="utf-8").splitlines()
                 if kept(json.loads(line))]
    with open(out / "raw.jsonl", "w", encoding="utf-8") as raw:
        raw.writelines(line + "\n" for line in raw_lines)

        def sink(row):
            raw.write(json.dumps(row) + "\n")
            raw.flush()

        for e in engines:
            facts = engine_facts(Run(e, a, 0, sink))
            if e not in env["engines"]:
                # An engine the earlier run did not have joins it, measured under the same limits.
                if any(facts["limits"] != v["limits"] for v in env["engines"].values()):
                    raise SystemExit(f"{e} limits differ from the earlier run")
                env["engines"][e] = {**facts, "added": {
                    "at": dt.datetime.now().isoformat(timespec="seconds"), "workloads": workloads,
                    "passes": a.passes, "bench_sha256": hashlib.sha256((HERE / "bench.py").read_bytes()).hexdigest()}}
                continue
            if facts["limits"] != env["engines"][e]["limits"]:
                raise SystemExit(f"{e} limits differ from the earlier run")
            env["engines"][e].setdefault("remeasured", []).append({
                "at": dt.datetime.now().isoformat(timespec="seconds"), "workloads": workloads,
                "passes": a.passes, "image": facts["image"], "server": facts["server"],
                "bench_sha256": hashlib.sha256((HERE / "bench.py").read_bytes()).hexdigest()})
        for p in range(1, a.passes + 1):
            shift = (p - 1) % len(engines)
            for e in engines[shift:] + engines[:shift]:
                print(f"== pass {p}: {e} ({', '.join(workloads)})")
                rows += Run(e, a, p, sink).all()
    (out / "environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")
    write_tables(out, rows)


def write_tables(out, rows):
    fields = ["pass", "engine", "workload", "metric", "value", "unit", "better"]
    with open(out / "results.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    # A workload that failed in any pass has no median: the report shows it as did not finish.
    failed = {(r["engine"], r["workload"]) for r in rows if r["metric"] == "did_not_finish"}
    groups = {}
    for r in rows:
        if (r["engine"], r["workload"]) in failed:
            continue
        groups.setdefault((r["engine"], r["workload"], r["metric"], r["unit"], r["better"]), []).append(r["value"])
    with open(out / "summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["engine", "workload", "metric", "median", "min", "max", "passes", "unit", "better"])
        for (e, wl, m, u, b), vals in groups.items():
            w.writerow([e, wl, m, round(statistics.median(vals), 4), round(min(vals), 4), round(max(vals), 4),
                        len(vals), u, b])


if __name__ == "__main__":
    main()
