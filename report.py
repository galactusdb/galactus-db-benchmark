"""Builds report.html, one self-contained page, from a bench.py results folder.

Usage:
  python report.py                       # newest folder under runs/
  python report.py runs/2026-10-31_101500
"""
import csv
import datetime as dt
import html
import json
import pathlib
import sys
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent
ENGINE_ORDER = ["gdb-paged", "gdb-resident", "neo4j", "memgraph"]
PROFILE_NAMES = {"standard": "Standard", "restricted": "Restricted"}

# Each chart: (workload, metric, title, what it measures).
SECTIONS = [
    ("Shortest paths", [
        ("shortest_path", "p50_ms", "Shortest path", "Fewest hops between two people up to 6 hops apart. Median latency."),
        ("all_shortest_paths", "p50_ms", "All shortest paths", "Every equally short path between two people up to 6 hops apart. Median latency."),
        ("weighted_shortest_path", "p50_ms", "Weighted shortest path", "Lowest total weight between two random people (Dijkstra). Median latency."),
    ]),
    ("Lookups and traversals", [
        ("index_seek", "p50_ms", "Index lookup", "One person by indexed id. Median latency."),
        ("two_hop", "p50_ms", "Two-hop traversal", "Distinct friends of friends of one person. Median latency."),
        ("filtered_scan", "p50_ms", "Filtered scan", "Count every person matching a predicate on an unindexed property. Median latency."),
        ("concurrent_read", "reads_per_s", "Concurrent reads", "Index lookups per second from 16 clients at once."),
    ]),
    ("Loading", [
        ("bulk_load", "nodes_per_s", "Bulk load: nodes", "People created per second, in batches of 1,000."),
        ("bulk_load", "rels_per_s", "Bulk load: relationships", "Weighted relationships created per second, in batches of 1,000."),
    ]),
    ("Memory", [
        ("bulk_load", "peak_mem_mb", "Peak memory while loading", "Highest container memory sampled during the bulk load."),
        ("memory", "loaded_idle_mb", "Memory with the graph loaded", "Container memory at rest after the load."),
        ("concurrent_read", "peak_mem_mb", "Peak memory under concurrent reads", "Highest container memory during the 16-client phase."),
    ]),
    ("Start-up", [
        ("restart", "ready_s", "Restart to first answer", "From docker start, after a clean stop, to the first correct lookup on the loaded graph."),
    ]),
]


def fmt(v, unit):
    if v is None:
        return "–"
    if unit == "ms":
        return f"{v:.2f} ms" if v < 10 else f"{v:,.1f} ms"
    if unit == "s":
        return f"{v:.2f} s" if v < 10 else f"{v:,.1f} s"
    if unit == "MB":
        return f"{v:,.0f} MB"
    return f"{v:,.0f} {unit}"


def load_profile(folder):
    env = json.loads((folder / "environment.json").read_text(encoding="utf-8"))
    summary = {}
    with open(folder / "summary.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            for k in ("median", "min", "max"):
                r[k] = float(r[k])
            summary[(r["engine"], r["workload"], r["metric"])] = r
    answers, failures = defaultdict(dict), defaultdict(list)
    for line in (folder / "raw.jsonl").read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row.get("kind") == "answers":
            answers[(row["workload"], row["pass"])][row["engine"]] = row["sha"]
        elif row.get("kind") == "failure":
            failures[(row["engine"], row["workload"])].append(row["reason"])
    agreed = defaultdict(lambda: True)
    for (workload, _), by_engine in answers.items():
        if len(set(by_engine.values())) > 1:
            agreed[workload] = False
    return env, summary, agreed, failures, {w for (w, _) in answers}


def chart(engines, labels, summary, failures, agreed, checked, workload, metric, title, note, passes=1):
    rows = [(e, summary.get((e, workload, metric))) for e in engines]
    have = [r for _, r in rows if r]
    if not have and not any(failures.get((e, workload)) for e in engines):
        return ""
    better = have[0]["better"] if have else "lower"
    unit = have[0]["unit"] if have else ""
    top = max((r["max"] for r in have), default=1) or 1
    best = (min if better == "lower" else max)((r["median"] for r in have), default=None)
    bars = []
    for i, (e, r) in enumerate(rows):
        slot = ENGINE_ORDER.index(e) + 1
        if r is None or r["passes"] == "0":
            failed = failures.get((e, workload)) or ["not measured"]
            times = f" in {len(failed)} of {passes} passes" if failures.get((e, workload)) else ""
            bars.append(f'<div class="bar-row"><span class="bar-name">{html.escape(labels[e])}</span>'
                        f'<span class="dnf">Did not finish{times}: {html.escape(failed[0])}</span></div>')
            continue
        width = max(0.6, 100 * r["median"] / top)
        tip = (f'{labels[e]}: {fmt(r["median"], unit)} median of {r["passes"]} passes '
               f'(range {fmt(r["min"], unit)} to {fmt(r["max"], unit)})')
        lead = ' <b class="lead">best</b>' if r["median"] == best and len(have) > 1 else ""
        bars.append(
            f'<div class="bar-row" tabindex="0" data-tip="{html.escape(tip)}">'
            f'<span class="bar-name">{html.escape(labels[e])}</span>'
            f'<div class="bar-track"><span class="bar s{slot}" style="width:{width:.2f}%"></span></div>'
            f'<span class="bar-value">{fmt(r["median"], unit)}{lead}</span></div>')
    check = ""
    if workload in checked:
        check = ('<p class="check ok">✓ Every engine returned the same answers</p>' if agreed[workload]
                 else '<p class="check bad">Answers differed between engines; see raw.jsonl</p>')
    arrow = "Lower is better" if better == "lower" else "Higher is better"
    return (f'<article class="card"><header><h4>{html.escape(title)}</h4><span class="better">{arrow}</span></header>'
            f'<p class="note">{html.escape(note)}</p><div class="bars">{"".join(bars)}</div>{check}</article>')


def profile_section(name, folder, first):
    env, summary, agreed, failures, checked = load_profile(folder)
    engines = [e for e in ENGINE_ORDER if e in env["engines"]]
    labels = {e: env["engines"][e]["label"] for e in engines}
    lim = env["limits"]
    out = [f'<section class="profile" id="p-{name}" {"" if first else "hidden"}>',
           f'<p class="limits">Each engine ran alone under <b>{lim["BENCH_CPUS"]} CPUs</b> and '
           f'<b>{lim["BENCH_MEM"].upper().replace("G", " GB")}</b> of memory. '
           f'Graph: {env["params"]["scale"]:,} people, each knowing two others. '
           f'Medians of {env["params"]["passes"]} pass{"" if env["params"]["passes"] == 1 else "es"}, '
           f'each on a fresh volume.</p>']
    for e in engines:
        for m in env["engines"][e].get("remeasured", []):
            titles = [t for _, cs in SECTIONS for w, _, t, _ in cs if w in m["workloads"]]
            out.append(f'<p class="limits">{html.escape(labels[e])}: {html.escape(", ".join(titles))} '
                       f're-measured on {started(m["at"])} with <code>{html.escape(m["image"]["image"])}</code> '
                       f'({html.escape(str(m["server"].get("version", "")))}), {m["passes"]} passes, '
                       f'same limits and graph. Every other result is from the run above.</p>')
    for heading, charts in SECTIONS:
        cards = "".join(chart(engines, labels, summary, failures, agreed, checked, *c,
                             passes=env["params"]["passes"]) for c in charts)
        if cards:
            out.append(f'<h3>{heading}</h3><div class="grid">{cards}</div>')
    # Image size is a property of the image, not a measurement of the run.
    sizes = {e: env["engines"][e]["image"].get("size_mb") for e in engines}
    if all(sizes.values()):
        fake = {(e, "image", "size_mb"): {"median": sizes[e], "min": sizes[e], "max": sizes[e], "passes": "1",
                                          "unit": "MB", "better": "lower"} for e in engines}
        out.append('<h3>Footprint</h3><div class="grid">' + chart(engines, labels, fake, {}, agreed, set(), "image", "size_mb",
                                                "Container image on disk", "Size of the image each engine ran.")
                   + "</div>")
    out.append(table(engines, labels, summary))
    out.append(setup(engines, env))
    out.append("</section>")
    return "".join(out), env


def table(engines, labels, summary):
    head = "".join(f"<th>{html.escape(labels[e])}</th>" for e in engines)
    rows = []
    for _, charts in SECTIONS:
        for workload, metric, title, _ in charts:
            cells = []
            for e in engines:
                r = summary.get((e, workload, metric))
                cells.append(f"<td>{fmt(r['median'], r['unit']) if r else 'Did not finish'}</td>")
            rows.append(f"<tr><th scope='row'>{html.escape(title)}</th>{''.join(cells)}</tr>")
    return (f'<details class="table"><summary>All results as a table</summary><div class="scroll"><table>'
            f'<thead><tr><th>Workload</th>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div></details>')


def started(iso):
    d = dt.datetime.fromisoformat(iso)
    return d.strftime("%d/%m/%Y at %H:%M")


def setup(engines, env):
    blocks = []
    for e in engines:
        x = env["engines"][e]
        srv = x["server"]
        digest = (x["image"].get("repo_digests") or [x["image"].get("id", "")])[0]
        settings = "".join(f"<li><code>{html.escape(str(k))}</code> = {html.escape(str(v))}</li>"
                           for k, v in sorted((srv.get("settings") or {}).items()))
        stmts = "".join(f"<dt>{k}</dt><dd><code>{html.escape(v or 'not used')}</code></dd>"
                        for k, v in x["statements"].items())
        blocks.append(
            f'<details class="engine"><summary><b>{html.escape(x["label"])}</b> '
            f'{html.escape(str(srv.get("version", "")))} · <code>{html.escape(x["image"]["image"])}</code></summary>'
            f'<p>Image digest <code>{html.escape(digest)}</code>, {x["image"].get("size_mb")} MB on disk. '
            f'Bolt {html.escape(str(srv.get("bolt", "")))}. Limits: {x["limits"]["cpus"]:g} CPUs, '
            f'{x["limits"]["memory_bytes"] / 2**30:g} GiB.</p>'
            f'<p>Set-up: <code>{html.escape("; ".join(x["setup"] + x["schema"]))}</code></p>'
            f'<h5>Statements</h5><dl>{stmts}</dl>'
            f'<h5>Server settings, read back from the running server</h5><ul class="settings">{settings}</ul></details>')
    h = env["host"]
    return (f'<h3>How it was run</h3><p class="host">Host: {html.escape(h["processor"] or h["platform"])}, '
            f'{h["logical_cpus"]} logical CPUs, Docker {html.escape(str(h["docker"]["ServerVersion"]))} on '
            f'{html.escape(str(h["docker"]["OperatingSystem"]))}, Python {h["python"]} with the Neo4j Python '
            f'driver {h["neo4j_python_driver"]}. Started {started(env["started"])}.</p>' + "".join(blocks))


CSS = """
:root{color-scheme:light;--bg:#f6f6f3;--surface:#fcfcfb;--line:#e2e1dc;--text:#0b0b0b;--muted:#52514e;
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;--ok:#00791e;--bad:#c4302b;--accent:#2a78d6}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--bg:#111110;--surface:#1a1a19;
--line:#33332f;--text:#f0efec;--muted:#c3c2b7;--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--ok:#4cc46a;
--bad:#ff7b72;--accent:#3987e5}}
:root[data-theme="dark"]{color-scheme:dark;--bg:#111110;--surface:#1a1a19;--line:#33332f;--text:#f0efec;--muted:#c3c2b7;
--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--ok:#4cc46a;--bad:#ff7b72;--accent:#3987e5}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
font:15px/1.6 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{max-width:1180px;margin:0 auto;padding:40px 16px 80px}h1{font-size:2rem;line-height:1.2;margin:0 0 8px}
h3{font-size:1.25rem;margin:40px 0 14px}h4{font-size:1rem;margin:0}h5{margin:16px 0 6px;font-size:.85rem}
.lede{color:var(--muted);max-width:760px;margin:0 0 24px}
.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:24px 0 8px}.tabs button{font:inherit;padding:8px 16px;
border:1px solid var(--line);background:var(--surface);color:var(--text);border-radius:6px;cursor:pointer}
.tabs button[aria-pressed="true"]{border-color:var(--accent);box-shadow:inset 0 0 0 1px var(--accent)}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;margin:16px 0 0;padding:0;list-style:none;color:var(--muted);font-size:.85rem}
.legend i{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;vertical-align:-1px}
.limits{color:var(--muted)}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px}
.card{min-width:0;background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:18px}
.card header{display:flex;justify-content:space-between;gap:12px;align-items:baseline}
.better{font-size:.75rem;color:var(--muted);white-space:nowrap}.note{font-size:.8rem;color:var(--muted);margin:4px 0 14px}
.bar-row{display:grid;grid-template-columns:minmax(0,1fr) 8.5em;gap:2px 10px;align-items:center;padding:4px 0;outline:none}
.bar-row:focus-visible{box-shadow:0 0 0 2px var(--accent);border-radius:4px}
.bar-name{grid-column:1/-1;font-size:.8rem;color:var(--muted)}.bar-track{min-width:0}
.bar{display:block;height:14px;border-radius:0 4px 4px 0}.bar-value{font-size:.8rem;white-space:nowrap;font-variant-numeric:tabular-nums}
.s1{background:var(--s1)}.s2{background:var(--s2)}.s3{background:var(--s3)}.s4{background:var(--s4)}
.lead{font-size:.7rem;font-weight:600;color:var(--text);border:1px solid var(--line);border-radius:4px;padding:0 5px;margin-left:4px}
.dnf{grid-column:1/-1;font-size:.78rem;color:var(--muted);font-style:italic}
.check{font-size:.78rem;margin:12px 0 0}.check.ok{color:var(--ok)}.check.bad{color:var(--bad)}
details{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px 16px;margin:10px 0}
summary{cursor:pointer}.scroll{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:.85rem;margin-top:10px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
code{font:.8rem ui-monospace,SFMono-Regular,Consolas,monospace;overflow-wrap:anywhere}dl{margin:0}dt{font-weight:600;font-size:.8rem;margin-top:8px}
dd{margin:2px 0 0}.settings{columns:2 280px;font-size:.8rem;padding-left:18px}.host,.method{color:var(--muted);font-size:.9rem}
#tip{position:fixed;pointer-events:none;background:var(--text);color:var(--bg);font-size:.78rem;padding:6px 9px;
border-radius:6px;max-width:300px;opacity:0;transition:opacity .1s}
@media (max-width:560px){.grid{grid-template-columns:minmax(0,1fr)}.card header{flex-wrap:wrap}}
"""

JS = """
const tip=document.getElementById('tip');
function show(el,x,y){tip.textContent=el.dataset.tip;tip.style.opacity=1;
 const w=tip.offsetWidth;tip.style.left=Math.min(x+12,innerWidth-w-8)+'px';tip.style.top=(y+14)+'px'}
document.querySelectorAll('[data-tip]').forEach(el=>{
 el.addEventListener('mousemove',e=>show(el,e.clientX,e.clientY));
 el.addEventListener('mouseleave',()=>tip.style.opacity=0);
 el.addEventListener('focus',()=>{const r=el.getBoundingClientRect();show(el,r.left,r.bottom)});
 el.addEventListener('blur',()=>tip.style.opacity=0)});
document.querySelectorAll('.tabs button').forEach(b=>b.addEventListener('click',()=>{
 document.querySelectorAll('.tabs button').forEach(x=>x.setAttribute('aria-pressed',x===b));
 document.querySelectorAll('.profile').forEach(s=>s.hidden=s.id!=='p-'+b.dataset.p)}));
"""


def main():
    root = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else max((HERE / "runs").iterdir())
    profiles = [p for p in ("standard", "restricted") if (root / p / "summary.csv").exists()]
    if not profiles:
        raise SystemExit(f"no results in {root}")
    sections, envs = [], []
    for i, p in enumerate(profiles):
        body, env = profile_section(p, root / p, i == 0)
        sections.append(body)
        envs.append(env)
    first = envs[0]
    engines = [e for e in ENGINE_ORDER if e in first["engines"]]
    versions = ", ".join(f'{first["engines"][e]["label"]} {first["engines"][e]["server"].get("version", "")}'
                         for e in engines)
    legend = "".join(f'<li><i class="s{ENGINE_ORDER.index(e) + 1}"></i>{html.escape(first["engines"][e]["label"])}</li>'
                     for e in engines)
    tabs = "".join(
        f'<button type="button" data-p="{p}" aria-pressed="{"true" if i == 0 else "false"}">{PROFILE_NAMES[p]}: '
        f'{envs[i]["limits"]["BENCH_CPUS"]} CPUs, {envs[i]["limits"]["BENCH_MEM"].upper().replace("G", " GB")}</button>'
        for i, p in enumerate(profiles))
    page = f"""<!doctype html><html lang="en-GB"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Graph database benchmark</title>
<style>{CSS}</style></head><body><main>
<h1>Graph database benchmark</h1>
<p class="lede">{html.escape(versions)}. The same graph, the same Bolt driver and the same Cypher wherever every
engine accepts it, with each engine running alone under identical container limits. Bars show the median of the
passes; hover or focus a bar for the range.</p>
<p class="method">Galactus DB runs twice: Paged, its default engine, which keeps the pages in use in memory, and
Resident, which keeps the whole graph in memory like Memgraph. Neo4j's heap and page cache are the values
<code>neo4j-admin server memory-recommendation</code> gives for each memory limit. Every other setting is the
engine's own default, and each server's effective settings are listed at the end.</p>
<div class="tabs" role="group" aria-label="Container limits">{tabs}</div>
<ul class="legend">{legend}</ul>
{"".join(sections)}
</main><div id="tip" role="status"></div><script>{JS}</script></body></html>"""
    out = root / "report.html"
    out.write_text(page, encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
