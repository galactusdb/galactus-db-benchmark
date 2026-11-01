# Graph database benchmark

Galactus DB, Neo4j and Memgraph, measured side by side on your own machine. One
command runs every engine through the same workloads, writes the results as CSV
and builds a single HTML page of charts.

```sh
pip install -r requirements.txt
python bench.py          # about 30 minutes for both profiles
python report.py         # writes results/<run>/report.html
```

You need Docker with Compose and Python 3.9 or later. The first run pulls the images:
Galactus DB, Neo4j Community (with the Graph Data Science plugin) and Memgraph Community.

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
  paths, and Memgraph's telemetry is off. Each server's effective settings are
  read back from the running server and printed in the report.
- **Same client, same data, same Cypher.** One Python process drives every
  engine through the official Neo4j Python driver. The graph is generated from
  a fixed seed: 100,000 people, each knowing two others, with a weight of 1 to
  10 on every relationship. Every engine runs the same statement text wherever
  it accepts it. The differences are all in one place, `ENGINES` in `bench.py`,
  and the report lists every statement each engine ran.
- **The same answers.** Every timed query's answer is fingerprinted, and the
  report shows whether all engines returned the same results.
- **Warm-up, then medians.** Each latency test runs a few untimed queries
  first. The report charts the median of the passes and shows the range on hover.
- **Failures are results.** If an engine runs out of memory or errors under a
  profile, the workload is recorded as "did not finish" with the reason, and
  the run carries on.

Galactus DB runs twice: **Paged**, its default engine, which keeps the pages in
use in memory and opens straight from disk, and **Resident**, which keeps the
whole graph in memory as Memgraph does. Both run the published image with its
defaults, including buffered durability with a 50 ms commit-log sync.

## What is measured

| Group | Workload | Metric |
|---|---|---|
| Shortest paths | Shortest path between two random people, up to 6 hops | median latency |
| | All shortest paths between two random people, up to 6 hops | median latency |
| | Weighted shortest path (Dijkstra) between two random people | median latency |
| Lookups and traversals | Index lookup by id | median latency |
| | Distinct friends of friends | median latency |
| | Count of people matching a predicate on an unindexed property | median latency |
| | Index lookups from 16 concurrent clients | reads per second |
| Loading | People and relationships created in batches of 1,000 | per second |
| Memory | Peak while loading, at rest with the graph loaded, peak under concurrent reads | MB |
| Start-up | Clean stop, then `docker start` to the first correct answer | seconds |

Memgraph has no `shortestPath()`, `allShortestPaths()` or Graph Data Science
procedures, so it runs its own built-in equivalents (`*BFS`, `*ALLSHORTEST` and
`*WSHORTEST`), which return the same answers. Neo4j and Galactus DB run the same
Graph Data Science call for weighted paths, from a projection built once.

## Options

```sh
python bench.py --profiles restricted              # one profile
python bench.py --engines gdb-resident,memgraph    # some engines
python bench.py --scale 1000000 --passes 5         # a larger graph, more passes
python bench.py --scale 10000 --passes 1 --smoke   # check it all runs, in a few minutes
python bench.py --out D:/benchmark/runs            # results somewhere other than results/
```

Image versions are pinned in `docker-compose.yml` and can be changed with
`GALACTUS_TAG`, `NEO4J_TAG` and `MEMGRAPH_TAG`.

## Output

Each run writes `results/<timestamp>/<profile>/`:

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
