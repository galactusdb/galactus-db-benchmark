# Graph database benchmark

Galactus DB, Neo4j, Memgraph, FalkorDB and ArangoDB, measured side by side on your own machine. One
command runs every engine through the same workloads, writes the results as CSV
and builds a single HTML page of charts.

```sh
pip install -r requirements.txt
python bench.py          # under an hour for both profiles
python report.py         # writes runs/<run>/report.html
```

You need Docker with Compose and Python 3.9 or later. The first run pulls the images:
Galactus DB, Neo4j Community (with the Graph Data Science plugin), Memgraph Community,
FalkorDB and ArangoDB Community.

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
  gives for containers. Each server's effective settings are read back from the
  running server and printed in the report.
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
  posts each statement to its cursor API, as ArangoDB's own clients do. A client
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
  every write to disk by default. FalkorDB keeps writes in memory between snapshots unless
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
`GALACTUS_TAG`, `NEO4J_TAG`, `MEMGRAPH_TAG`, `FALKORDB_TAG` and `ARANGODB_TAG`; the client
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
