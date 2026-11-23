# Graph database benchmark

Galactus DB, Neo4j, Memgraph, FalkorDB, ArangoDB and Dgraph, measured side by side on your own machine. One
command runs every engine through the same workloads, writes the results as CSV
and builds a single HTML page of charts.

```sh
pip install -r requirements.txt
python bench.py          # under an hour for both profiles
python report.py         # writes runs/<run>/report.html
```

You need Docker with Compose and Python 3.9 or later. The first run pulls the images:
Galactus DB, Neo4j Community (with the Graph Data Science plugin), Memgraph Community,
FalkorDB, ArangoDB Community and Dgraph.

## How the comparison is kept fair

- **One engine at a time.** Each engine starts on a fresh volume, runs every
  workload and is removed before the next starts, so no engine shares the
  machine with another. The order rotates on each pass.
- **Identical limits.** Every engine gets the same CPU and memory limit, set in
  `docker-compose.yml`. Two profiles run by default:

  | Profile | CPUs | Memory |
  |---|---|---|
  | Standard | 8 | 4 GB |
  | Restricted | 2 | 1 GB |

- **Community editions and their defaults.** The only settings changed are
  listed in `docker-compose.yml` with the reason beside each: Neo4j's heap and
  page cache are the values `neo4j-admin server memory-recommendation` prints
  for each memory limit, Neo4j loads the Graph Data Science plugin for weighted
  paths, and Memgraph's telemetry is off. FalkorDB runs its server image as it
  comes, without the image's 1 second query timeout so that every query can
  finish. ArangoDB sizes its caches from the memory and cores it detects, and
  inside a container it sees the whole host, so it is told the container's
  limits through `ARANGODB_OVERRIDE_DETECTED_TOTAL_MEMORY` and
  `ARANGODB_OVERRIDE_DETECTED_NUMBER_OF_CORES`, the variables its documentation
  gives for containers. Dgraph's cache defaults to 4 GB, the whole of the standard
  limit, so it is set to half the container's memory, as Neo4j's heap is. Each
  server's effective settings are read back from the running server and printed
  in the report.
- **Same data, same Cypher.** The graph is generated from a fixed seed: 100,000
  people, each knowing two others, with a weight of 1 to 10 on every
  relationship. Every engine runs the same statement text wherever it accepts
  it. The differences are all in one place, `ENGINES` in `bench.py`, and the
  report lists every statement each engine ran.
- **Like-for-like clients.** One Python process drives every engine. Galactus
  DB, Neo4j and Memgraph speak Bolt and share the official Neo4j Python driver.
  FalkorDB is a Redis module, so it is driven over the Redis protocol by a small
  client in `bench.py` that sends what FalkorDB's own clients send. ArangoDB
  answers over HTTP, so it is driven by a small HTTP client in `bench.py` that
  posts each statement to its cursor API, as ArangoDB's own clients do. Dgraph's
  own clients speak gRPC over one long-lived connection, and its HTTP API closes
  the connection after every request, so it is driven by a small gRPC client in
  `bench.py`: the HTTP/2 frames and protocol-buffer fields one call needs. A client
  library's work is part of every latency it measures, and the Neo4j driver does
  far more per query than a Redis client, so to compare engines across
  protocols run with `--client minimal`: every engine is then driven by a small
  client that only sends the statement and reads the rows.
- **The client beside the engine.** With `--inside`, the timed queries are sent
  from a container on the engine's own Docker network, using the minimal
  clients. On Docker Desktop a query from the host crosses a forwarded port,
  which costs every engine the same few tenths of a millisecond and hides the
  differences between fast queries. The report shows what an empty query costs
  each engine, so you can see how much of any latency is the path and not the
  engine.
- **Clients that do not queue.** In the concurrent test each of the 16 clients
  is its own process with its own connection, so the test measures the engine
  and not one Python interpreter.
- **Like-for-like durability.** Galactus DB, Neo4j, Memgraph and ArangoDB log
  every write to disk by default. Dgraph runs with its default, `--survive
  process`, which keeps every write through a crash of the server process. FalkorDB keeps writes in memory between snapshots unless
  Redis's append-only file is on, so it runs twice: as it comes (`falkordb`),
  and with the append-only file on (`falkordb-aof`).
- **The same answers.** Every timed query's answer is fingerprinted, and the
  report shows whether all engines returned the same results.
- **Warm-up, then medians.** Each latency test runs a few untimed queries
  first. The report charts the median of the passes and shows the range on hover.
- **Failures are results.** If an engine runs out of memory or errors under a
  profile, the workload is recorded as "did not finish" with the reason, and
  the run carries on.

Galactus DB runs twice: **Paged**, its default engine, which keeps the pages in
use in memory and opens straight from disk, and **Resident**, which keeps the
whole graph in memory as Memgraph and FalkorDB do. Both run the published image with its
defaults, including buffered durability with a 50 ms commit-log sync.

## What is measured

| Group | Workload | Metric |
|---|---|---|
| Shortest paths | Shortest path between two people up to 6 hops apart | median latency |
| | All shortest paths between two people up to 6 hops apart | median latency |
| | Weighted shortest path (Dijkstra) between two random people | median latency |
| | Building the projection a weighted search runs on, where an engine uses one | once, in ms |
| Lookups and traversals | An empty query, `RETURN 1` | median latency |
| | Index lookup by id | median latency |
| | Distinct friends of friends | median latency |
| | Count of people matching a predicate on an unindexed property | median latency |
| | Index lookups from 16 concurrent clients | reads per second |
| Loading | People and relationships created in batches of 1,000 | per second |
| Memory | Peak while loading, at rest with the graph loaded, peak under concurrent reads | MB |
| Start-up | Clean stop, then `docker start` to the first correct answer | seconds |

For the two unweighted path tests, each pair is chosen by a random walk of 2 to 6
steps from a random person, so a path always exists within the 6-hop limit; random
pairs in a graph this sparse are almost never that close. Weighted paths have no
hop limit, so they use two random people.

A workload that fails in any pass is shown as did not finish, with the number of
passes it failed in, rather than as a median of the passes that completed. If the
graph itself does not load, the rest of that pass is recorded as did not finish
too: nothing timed on part of a graph is a measurement of it.

Memgraph has no `shortestPath()`, `allShortestPaths()` or Graph Data Science
procedures, so it runs its own built-in equivalents (`*BFS`, `*ALLSHORTEST` and
`*WSHORTEST`), which return the same answers. Neo4j and Galactus DB run the same
Graph Data Science call for weighted paths, from a projection built once.

FalkorDB takes `shortestPath()` in a `WITH` clause and `allShortestPaths()` once
both ends are bound, so those two statements are reworded for it, and it answers
weighted paths with its own `algo.SPpaths` procedure.

ArangoDB has no Cypher, so every statement is written in AQL, its own query
language, asking for the same work and returning the same answer: people are
documents in a `Person` collection with a persistent index on `id`, and
relationships are edges in a `KNOWS` edge collection. Paths use its built-in
`SHORTEST_PATH` and `ALL_SHORTEST_PATHS`, with `weightAttribute` for weighted
paths. Those take no hop limit, so the 6-hop limit is applied to the answer;
every pair is chosen within 6 hops, so it never removes one.

Dgraph is queried in DQL, its own language, so every statement is written in DQL
too: people are nodes with an `int` index on `id`, and relationships are `knows`
edges with the weight as a facet, searched in both directions through the
reverse edge. Paths use its `shortest` block, which returns each path's length,
or total weight with `@facets(w)`. Dgraph has no all-shortest-paths, so it is
asked for the two shortest paths within 6 hops, the least that shows whether the
shortest has a tie; no pair in the graph has more than one shortest path. The
filtered scan computes `age % 7` with `math()` and counts the zeros. Relationships
are created with upsert blocks that look up both ends by `id`. Dgraph runs its
two processes, Zero and Alpha, in one container under one set of limits, as its
standalone image does, with a stop handler so that a stop is clean.

## Results

Our own results, measured on an Intel Core i9-13900K (32 logical CPUs, 31 GB of RAM)
with Docker Desktop 28.0.1 on WSL 2. Your machine will give different figures; the
suite is here so you can measure them. The raw results stay out of this repository.

Each table is one run, so every figure in it was measured the same way. Galactus DB
2.5.3 and FalkorDB 6.0.4 were measured on 21 November 2026, ArangoDB 3.12.13
Community and Dgraph v25.5.1 on 23 November 2026, all with `--client minimal
--inside`: a small client for each server's own protocol, in a container beside it.
Bold marks the best figure in each row. "Ran out of memory" means the container was
killed at its memory limit; "graph did not load" means the graph failed to load in a
pass, so nothing was timed on it.

**100,000 people, 8 CPUs and 4 GB of RAM for each container.** Medians of three passes.

| Workload | Galactus DB Paged 2.5.3 | Galactus DB Resident 2.5.3 | FalkorDB 6.0.4 | ArangoDB 3.12.13 | Dgraph 25.5.1 |
|---|---|---|---|---|---|
| Index lookup | 0.077 ms | **0.073 ms** | 0.112 ms | 0.252 ms | 0.491 ms |
| Friends of friends | 0.080 ms | **0.074 ms** | 0.156 ms | 0.486 ms | 0.547 ms |
| Shortest path, up to 6 hops | 0.105 ms | **0.089 ms** | 0.151 ms | 0.564 ms | 1.23 ms |
| All shortest paths, up to 6 hops | 0.118 ms | **0.093 ms** | 0.189 ms | 0.585 ms | 5.45 ms |
| Weighted shortest path | 7.18 ms | **7.11 ms** | 53.0 ms | 8.73 ms | ran out of memory |
| Projection for weighted paths, built once | 283 ms | **219 ms** | – | – | – |
| Filtered scan of every person | 6.81 ms | 6.84 ms | **2.84 ms** | 11.6 ms | 182 ms |
| Lookups a second, 16 clients | 109,832 | **113,558** | 47,295 | 35,547 | 23,816 |
| Empty query | 0.067 ms | **0.067 ms** | 0.077 ms | 0.126 ms | 0.374 ms |
| Nodes loaded a second | 98,626 | 112,432 | **165,180** | 134,635 | 28,553 |
| Relationships loaded a second | 67,218 | **111,893** | 22,046 | 61,251 | 37,365 |
| Memory with the graph loaded | 166 MB | 81 MB | **80 MB** | 554 MB | 1,556 MB |
| Peak memory while loading | 166 MB | **92 MB** | 113 MB | 552 MB | 1,657 MB |
| Peak memory, 16 clients | 213 MB | 145 MB | **137 MB** | 2,231 MB | 1,462 MB |
| Restart to the first answer | 0.87 s | 0.85 s | **0.82 s** | 1.91 s | 8.55 s |
**100,000 people, 2 CPUs and 1 GB of RAM for each container.** Medians of three passes.

| Workload | Galactus DB Paged 2.5.3 | Galactus DB Resident 2.5.3 | FalkorDB 6.0.4 | ArangoDB 3.12.13 | Dgraph 25.5.1 |
|---|---|---|---|---|---|
| Index lookup | 0.077 ms | **0.073 ms** | 0.110 ms | 0.230 ms | graph did not load |
| Friends of friends | 0.080 ms | **0.074 ms** | 0.154 ms | 0.477 ms | graph did not load |
| Shortest path, up to 6 hops | 0.103 ms | **0.087 ms** | 0.148 ms | 0.577 ms | graph did not load |
| All shortest paths, up to 6 hops | 0.107 ms | **0.096 ms** | 0.184 ms | 0.583 ms | ran out of memory |
| Weighted shortest path | 7.15 ms | **6.84 ms** | 53.7 ms | ran out of memory | ran out of memory |
| Projection for weighted paths, built once | 281 ms | **219 ms** | – | – | – |
| Filtered scan of every person | 11.8 ms | 7.06 ms | **2.80 ms** | 11.7 ms | ran out of memory |
| Lookups a second, 16 clients | 28,177 | 30,225 | **47,409** | 9,571 | graph did not load |
| Empty query | 0.067 ms | **0.067 ms** | 0.081 ms | 0.126 ms | graph did not load |
| Nodes loaded a second | 103,387 | 110,898 | **164,973** | 137,024 | ran out of memory |
| Relationships loaded a second | 68,245 | **109,755** | 22,026 | 62,457 | ran out of memory |
| Memory with the graph loaded | 167 MB | 80 MB | **71 MB** | 511 MB | graph did not load |
| Peak memory while loading | 154 MB | 83 MB | **81 MB** | 512 MB | ran out of memory |
| Peak memory, 16 clients | 218 MB | 142 MB | **92 MB** | 664 MB | graph did not load |
| Restart to the first answer | 0.87 s | **0.84 s** | 0.85 s | 1.12 s | graph did not load |
**1,000,000 people, 8 CPUs and 4 GB of RAM for each container.** Medians of three passes.

| Workload | Galactus DB Paged 2.5.3 | Galactus DB Resident 2.5.3 | FalkorDB 6.0.4 | ArangoDB 3.12.13 | Dgraph 25.5.1 |
|---|---|---|---|---|---|
| Index lookup | 0.079 ms | **0.074 ms** | 0.115 ms | 0.240 ms | graph did not load |
| Friends of friends | 0.082 ms | **0.075 ms** | 0.151 ms | 0.572 ms | graph did not load |
| Shortest path, up to 6 hops | 0.114 ms | **0.093 ms** | 0.158 ms | 0.688 ms | graph did not load |
| All shortest paths, up to 6 hops | 0.136 ms | **0.106 ms** | 0.236 ms | 0.719 ms | graph did not load |
| Weighted shortest path | 153 ms | 157 ms | 1,164 ms | **38.5 ms** | graph did not load |
| Projection for weighted paths, built once | 2,788 ms | **2,488 ms** | – | – | – |
| Filtered scan of every person | 63.1 ms | 74.2 ms | **30.1 ms** | 111 ms | graph did not load |
| Lookups a second, 16 clients | 108,338 | **112,439** | 46,881 | 34,171 | graph did not load |
| Empty query | **0.068 ms** | 0.069 ms | 0.078 ms | 0.124 ms | graph did not load |
| Nodes loaded a second | 94,319 | 97,819 | **157,542** | 131,638 | ran out of memory |
| Relationships loaded a second | 53,508 | **93,048** | 20,143 | 54,704 | ran out of memory |
| Memory with the graph loaded | **500 MB** | 627 MB | 553 MB | 1,447 MB | graph did not load |
| Peak memory while loading | **578 MB** | 893 MB | 628 MB | 1,550 MB | ran out of memory |
| Peak memory, 16 clients | 805 MB | 1,011 MB | **607 MB** | 2,463 MB | graph did not load |
| Restart to the first answer | **0.87 s** | 2.24 s | 2.16 s | 2.21 s | graph did not load |
**1,000,000 people, 2 CPUs and 1 GB of RAM for each container.** Medians of three passes.

| Workload | Galactus DB Paged 2.5.3 | Galactus DB Resident 2.5.3 | FalkorDB 6.0.4 | ArangoDB 3.12.13 | Dgraph 25.5.1 |
|---|---|---|---|---|---|
| Index lookup | **0.079 ms** | graph did not load | 0.113 ms | graph did not load | graph did not load |
| Friends of friends | **0.082 ms** | graph did not load | 0.151 ms | graph did not load | graph did not load |
| Shortest path, up to 6 hops | **0.127 ms** | graph did not load | 0.155 ms | graph did not load | graph did not load |
| All shortest paths, up to 6 hops | **0.148 ms** | graph did not load | 0.212 ms | graph did not load | graph did not load |
| Weighted shortest path | **155 ms** | graph did not load | 1,081 ms | graph did not load | graph did not load |
| Projection for weighted paths, built once | 3,004 ms | graph did not load | – | – | – |
| Filtered scan of every person | 126 ms | graph did not load | **30.1 ms** | graph did not load | graph did not load |
| Lookups a second, 16 clients | 26,639 | graph did not load | **45,301** | graph did not load | graph did not load |
| Empty query | **0.068 ms** | graph did not load | 0.077 ms | graph did not load | graph did not load |
| Nodes loaded a second | 92,577 | ran out of memory | **158,991** | ran out of memory | ran out of memory |
| Relationships loaded a second | **52,451** | ran out of memory | 20,417 | ran out of memory | ran out of memory |
| Memory with the graph loaded | **497 MB** | graph did not load | 541 MB | graph did not load | graph did not load |
| Peak memory while loading | **559 MB** | ran out of memory | 562 MB | ran out of memory | ran out of memory |
| Peak memory, 16 clients | 755 MB | graph did not load | **565 MB** | graph did not load | graph did not load |
| Restart to the first answer | **0.84 s** | graph did not load | 2.22 s | graph did not load | graph did not load |
### Through the Neo4j Python driver

Galactus DB 2.4.3 (measured on 14 November 2026), Neo4j 5.26.32 Community and
Memgraph 3.14.0 Community (1 November 2026), on the same machine, with the default
`--client driver` from the host: every query crossed Docker Desktop's forwarded port
and went through the Neo4j Python driver, which is why every latency here is higher
than in the tables above. Compare figures within a table, not across them.

**100,000 people, 8 CPUs and 4 GB of RAM for each container, through the Neo4j Python driver from the host.** Medians of three passes.

| Workload | Galactus DB Paged 2.4.3 | Galactus DB Resident 2.4.3 | Neo4j 5.26.32 | Memgraph 3.14.0 |
|---|---|---|---|---|
| Index lookup | 0.477 ms | **0.474 ms** | 1.22 ms | 0.491 ms |
| Friends of friends | 0.481 ms | **0.476 ms** | 1.13 ms | 0.479 ms |
| Shortest path, up to 6 hops | 0.518 ms | **0.477 ms** | 1.46 ms | 2.29 ms |
| All shortest paths, up to 6 hops | 0.543 ms | **0.512 ms** | 1.31 ms | 3.70 ms |
| Weighted shortest path | 23.1 ms | **22.9 ms** | 26.1 ms | 417 ms |
| Filtered scan of every person | 8.27 ms | **8.26 ms** | 14.9 ms | 10.4 ms |
| Lookups a second, 16 clients | 4,103 | **4,117** | 3,005 | 3,849 |
| Nodes loaded a second | 71,374 | **77,798** | 38,109 | 63,341 |
| Relationships loaded a second | 58,600 | **88,159** | 36,457 | 73,854 |
| Memory with the graph loaded | 169 MB | **97 MB** | 3,043 MB | 252 MB |
| Peak memory while loading | 146 MB | **99 MB** | 3,049 MB | 428 MB |
| Peak memory, 16 clients | 628 MB | 568 MB | 3,174 MB | **415 MB** |
| Restart to the first answer | 0.76 s | **0.75 s** | 14.88 s | 1.08 s |
**100,000 people, 2 CPUs and 1 GB of RAM for each container, through the Neo4j Python driver from the host.** Medians of three passes.

| Workload | Galactus DB Paged 2.4.3 | Galactus DB Resident 2.4.3 | Neo4j 5.26.32 | Memgraph 3.14.0 |
|---|---|---|---|---|
| Index lookup | 0.481 ms | **0.461 ms** | 1.21 ms | 0.504 ms |
| Friends of friends | 0.482 ms | **0.470 ms** | 1.19 ms | 0.493 ms |
| Shortest path, up to 6 hops | 0.537 ms | **0.513 ms** | 1.38 ms | 2.26 ms |
| All shortest paths, up to 6 hops | 0.554 ms | **0.513 ms** | 1.31 ms | 3.23 ms |
| Weighted shortest path | **22.5 ms** | 22.7 ms | ran out of memory | 377 ms |
| Filtered scan of every person | 12.4 ms | **7.76 ms** | 17.7 ms | 10.5 ms |
| Lookups a second, 16 clients | 4,096 | 4,109 | ran out of memory | **4,276** |
| Nodes loaded a second | 72,670 | **77,652** | 33,564 | 70,803 |
| Relationships loaded a second | 58,293 | **85,178** | 33,053 | 78,607 |
| Memory with the graph loaded | 173 MB | **96 MB** | 1,031 MB | 250 MB |
| Peak memory while loading | 170 MB | **98 MB** | 1,031 MB | 395 MB |
| Peak memory, 16 clients | 631 MB | 570 MB | ran out of memory | **434 MB** |
| Restart to the first answer | 0.73 s | **0.72 s** | 15.62 s | 1.13 s |

## Options

```sh
python bench.py --profiles restricted              # one profile
python bench.py --engines gdb-resident,memgraph    # some engines
python bench.py --client minimal                   # minimal clients for every engine
python bench.py --inside                           # and the client beside each engine
python bench.py --scale 1000000 --passes 5         # a larger graph, more passes
python bench.py --scale 10000 --passes 1 --smoke   # check it all runs, in a few minutes
python bench.py --out D:/benchmark/runs            # results somewhere other than runs/
```

To re-measure some workloads in an earlier run, for example after a new release of
one engine, name the workloads and the run. The earlier results for those engines
and workloads are replaced, the rest are kept, and the report states which image
re-measured them and when.

```sh
python bench.py --engines gdb-paged,gdb-resident --workloads weighted_shortest_path --into runs/<run>
```

Image versions are pinned in `docker-compose.yml` and can be changed with
`GALACTUS_TAG`, `NEO4J_TAG`, `MEMGRAPH_TAG`, `FALKORDB_TAG`, `ARANGODB_TAG` and `DGRAPH_TAG`; the client
container's image is `BENCH_CLIENT_IMAGE`. If a host port is taken on your
machine, set `BENCH_PORT_` and the engine's name, as in
`BENCH_PORT_GDB_RESIDENT`.

## Output

Each run writes `runs/<timestamp>/<profile>/`:

| File | Contents |
|---|---|
| `environment.json` | Host, Docker, image digests, limits and each server's effective settings |
| `raw.jsonl` | Every measurement as it was taken, plus answer fingerprints and failures |
| `results.csv` | One row per pass, engine, workload and metric |
| `summary.csv` | Median, minimum and maximum across passes |

`python report.py` reads the newest run (or the folder you name) and writes
`report.html` beside it: one page with no external files, in light or dark to
match your system.

For results you can compare, close other heavy applications first. The suite
warns if other containers are running.

## Licence

Apache-2.0. See `LICENSE`.
