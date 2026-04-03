"""
generate_etl_diagram.py
━━━━━━━━━━━━━━━━━━━━━━━
Reads gold_transformer.ipynb and silver_cleaner.ipynb from the same directory
as this script, extracts SQL via sqlglot, builds a lineage graph, and writes
etl_diagram.html next to the script.

Usage:
    pip install sqlglot networkx
    python generate_etl_diagram.py
"""

import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ── 0. Config ─────────────────────────────────────────────────────────────────

HERE = Path(__file__).parent

NOTEBOOK_PATHS = [
    HERE / "gold_transformer.ipynb",
    HERE / "silver_cleaner.ipynb",
]

OUTPUT_HTML = HERE / "etl_diagram.html"
SQL_DIALECT = "spark"   # spark | tsql | hive | trino

# ── 1. Load notebooks ─────────────────────────────────────────────────────────

def load_notebook(path: Path) -> tuple[str, list[dict]]:
    """Return (notebook_name, cells) from a local .ipynb file."""
    if not path.exists():
        print(f"  ⚠️  Not found, skipping: {path}")
        return path.stem, []
    with open(path, "r", encoding="utf-8") as fh:
        nb = json.load(fh)
    cells = nb.get("cells", [])
    print(f"  ✅ {path.name} — {len(cells)} cells")
    return path.stem, cells

print("Loading notebooks...")
all_notebook_cells: list[tuple[str, list[dict]]] = []
for nb_path in NOTEBOOK_PATHS:
    name, cells = load_notebook(nb_path)
    if cells:
        all_notebook_cells.append((name, cells))

if not all_notebook_cells:
    sys.exit("❌ No notebooks loaded. Make sure the .ipynb files are in the same directory.")

print(f"✔ Loaded {len(all_notebook_cells)} notebook(s)\n")

# ── 2. SQL Extraction ─────────────────────────────────────────────────────────

@dataclass
class SQLFragment:
    source_cell_index: int
    raw_sql: str
    extraction_method: str   # magic | spark_sql | pandas_sql | raw_string
    notebook_name: str = ""

_SPARK_SQL_RE = re.compile(
    r'spark\.sql\s*\(\s*(?:f?"""([\s\S]*?)"""|f?\'\'\'([\s\S]*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*\)',
    re.IGNORECASE,
)
_PANDAS_SQL_RE = re.compile(
    r'pd(?:andas)?\.read_sql(?:_query)?\s*\(\s*(?:f?"""([\s\S]*?)"""|f?\'\'\'([\s\S]*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*',
    re.IGNORECASE,
)
_MAGIC_SQL_RE = re.compile(r'^%%sql\s*\n([\s\S]+)', re.MULTILINE | re.IGNORECASE)
_INLINE_SQL_RE = re.compile(
    r'(?:SELECT|INSERT\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)|WITH\s+\w+\s+AS)[\s\S]{20,}?(?:FROM|WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|;)',
    re.IGNORECASE,
)

def extract_sql(cells: list[dict], notebook_name: str) -> list[SQLFragment]:
    fragments = []
    for idx, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue
        src_lines = cell.get("source", [])
        src = "".join(src_lines) if isinstance(src_lines, list) else src_lines

        # 1. %%sql magic
        m = _MAGIC_SQL_RE.search(src)
        if m:
            fragments.append(SQLFragment(idx, m.group(1).strip(), "magic", notebook_name))
            continue

        # 2. spark.sql(...)
        for m in _SPARK_SQL_RE.finditer(src):
            sql = next(g for g in m.groups() if g is not None)
            fragments.append(SQLFragment(idx, sql.strip(), "spark_sql", notebook_name))

        # 3. pd.read_sql(...)
        for m in _PANDAS_SQL_RE.finditer(src):
            sql = next((g for g in m.groups() if g is not None), None)
            if sql:
                fragments.append(SQLFragment(idx, sql.strip(), "pandas_sql", notebook_name))

        # 4. Raw inline SQL fallback
        if not any(f.source_cell_index == idx and f.notebook_name == notebook_name for f in fragments):
            for m in _INLINE_SQL_RE.finditer(src):
                fragments.append(SQLFragment(idx, m.group(0).strip(), "raw_string", notebook_name))

    return fragments

print("Extracting SQL...")
sql_fragments: list[SQLFragment] = []
for nb_name, nb_cells in all_notebook_cells:
    frags = extract_sql(nb_cells, nb_name)
    sql_fragments.extend(frags)
    print(f"  {nb_name}: {len(frags)} fragment(s)")

print(f"✔ Total SQL fragments: {len(sql_fragments)}\n")

# ── 3. sqlglot Parsing ────────────────────────────────────────────────────────

try:
    import sqlglot
    import sqlglot.expressions as exp
except ImportError:
    sys.exit("❌ sqlglot not installed. Run: pip install sqlglot")

@dataclass
class TableLineage:
    source_tables: list[str] = field(default_factory=list)
    target_table: str | None = None
    ctes: list[str] = field(default_factory=list)
    operations: list[str] = field(default_factory=list)
    cell_index: int = -1
    raw_sql: str = ""
    notebook_name: str = ""
    parse_error: str | None = None

def normalize_table(t) -> str:
    db = t.args.get("db")
    db = db.name if db is not None else None
    name = t.name or ""
    parts = [p for p in [db, name] if p]
    return ".".join(parts)

def parse_fragment(frag: SQLFragment) -> TableLineage:
    lin = TableLineage(
        cell_index=frag.source_cell_index,
        raw_sql=frag.raw_sql,
        notebook_name=frag.notebook_name,
    )
    try:
        stmts = sqlglot.parse(frag.raw_sql, dialect=SQL_DIALECT, error_level=sqlglot.ErrorLevel.WARN)
    except Exception as e:
        lin.parse_error = str(e)
        return lin

    for stmt in stmts:
        if stmt is None:
            continue

        if isinstance(stmt, exp.Select):
            lin.operations.append("SELECT")
        elif isinstance(stmt, exp.Insert):
            lin.operations.append("INSERT")
            tgt = stmt.find(exp.Table)
            if tgt:
                lin.target_table = normalize_table(tgt)
        elif isinstance(stmt, exp.Create):
            lin.operations.append("CREATE")
            tgt = stmt.find(exp.Table)
            if tgt:
                lin.target_table = normalize_table(tgt)
        elif isinstance(stmt, exp.Merge):
            lin.operations.append("MERGE")

        for cte in stmt.find_all(exp.CTE):
            lin.ctes.append(cte.alias)

        joins = list(stmt.find_all(exp.Join))
        if joins:
            lin.operations.append(f"JOIN×{len(joins)}")

        cte_names = set(lin.ctes)
        for table in stmt.find_all(exp.Table):
            name = normalize_table(table)
            if name and name not in cte_names and name != lin.target_table:
                if name not in lin.source_tables:
                    lin.source_tables.append(name)

    lin.operations = list(dict.fromkeys(lin.operations))
    return lin

print("Parsing SQL with sqlglot...")
parsed: list[TableLineage] = [parse_fragment(f) for f in sql_fragments]

ok  = [l for l in parsed if not l.parse_error]
bad = [l for l in parsed if l.parse_error]
print(f"  ✅ {len(ok)} parsed successfully")
if bad:
    print(f"  ⚠️  {len(bad)} parse error(s):")
    for l in bad:
        print(f"     [{l.notebook_name}] cell {l.cell_index}: {l.parse_error[:80]}")
print()

# ── 4. Build Lineage Graph ────────────────────────────────────────────────────

try:
    import networkx as nx
except ImportError:
    sys.exit("❌ networkx not installed. Run: pip install networkx")

G = nx.DiGraph()

NODE_COLORS = {
    "source":    "#00C9A7",
    "transform": "#4F8EF7",
    "target":    "#F76E4F",
    "cte":       "#A78BFA",
}

for i, lin in enumerate(parsed):
    if lin.parse_error:
        continue

    ops_label    = " | ".join(lin.operations) if lin.operations else "SELECT"
    transform_id = f"__xform_{i}__"

    G.add_node(transform_id,
               label=ops_label,
               node_type="transform",
               cell=lin.cell_index,
               notebook=lin.notebook_name,
               sql_preview=lin.raw_sql[:200])

    for src in lin.source_tables:
        if not G.has_node(src):
            G.add_node(src, label=src, node_type="source", cell=-1, notebook="", sql_preview="")
        G.add_edge(src, transform_id, label="reads")

    for cte in lin.ctes:
        cte_id = f"CTE:{cte}"
        if not G.has_node(cte_id):
            G.add_node(cte_id, label=f"CTE\n{cte}", node_type="cte", cell=-1, notebook="", sql_preview="")
        G.add_edge(cte_id, transform_id, label="via CTE")

    if lin.target_table:
        if not G.has_node(lin.target_table):
            G.add_node(lin.target_table, label=lin.target_table, node_type="target", cell=-1, notebook="", sql_preview="")
        G.add_edge(transform_id, lin.target_table, label="writes")
    else:
        result_id = f"__result_{i}__"
        G.add_node(result_id, label=f"Result\nCell {lin.cell_index}", node_type="target", cell=lin.cell_index, notebook=lin.notebook_name, sql_preview="")
        G.add_edge(transform_id, result_id, label="returns")

print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

# ── 5. Hierarchical Layout ────────────────────────────────────────────────────

def hierarchical_layout(G, width=3000, height=2200):
    try:
        generations = list(nx.topological_generations(G))
    except nx.NetworkXUnfeasible:
        generations = [list(G.nodes())]

    n_layers = len(generations)
    margin_x = 200
    margin_y = 60
    usable_w = width  - margin_x * 2
    node_h   = 60     # fixed vertical slot per node regardless of canvas height

    pos = {}
    for layer_i, layer_nodes in enumerate(generations):
        x = margin_x + (usable_w / max(n_layers - 1, 1)) * layer_i
        n = len(layer_nodes)
        total_h = n * node_h
        start_y = margin_y
        for node_i, node in enumerate(sorted(layer_nodes)):
            y = start_y + node_i * node_h + node_h / 2
            pos[node] = (x, y)
    return pos
  
positions = hierarchical_layout(G)

# ── 6. Build graph JSON for D3 ────────────────────────────────────────────────

def build_graph_json(G, positions):
    nodes = []
    for node_id, data in G.nodes(data=True):
        x, y = positions.get(node_id, (400, 300))
        nodes.append({
            "id":      node_id,
            "label":   data.get("label", node_id),
            "type":    data.get("node_type", "source"),
            "color":   NODE_COLORS.get(data.get("node_type", "source"), "#888"),
            "x":       x,
            "y":       y,
            "cell":    data.get("cell", -1),
            "notebook": data.get("notebook", ""),
            "preview": data.get("sql_preview", ""),
        })
    links = []
    for src, tgt, data in G.edges(data=True):
        links.append({"source": src, "target": tgt, "label": data.get("label", "")})
    return {"nodes": nodes, "links": links}

graph_json = json.dumps(build_graph_json(G, positions), ensure_ascii=False)
notebook_names = ", ".join(nb for nb, _ in all_notebook_cells)

# ── 7. Render HTML ────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>ETL Lineage — {notebook_names}</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@300;500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg:        #0D0F14;
      --surface:   #161922;
      --border:    #252A38;
      --text:      #E2E8F0;
      --muted:     #64748B;
      --accent:    #4F8EF7;
      --source:    #00C9A7;
      --transform: #4F8EF7;
      --target:    #F76E4F;
      --cte:       #A78BFA;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: var(--bg);
      color: var(--text);
      font-family: 'DM Sans', sans-serif;
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }}
    header {{
      padding: 14px 24px;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      gap: 16px;
      flex-shrink: 0;
    }}
    header h1 {{
      font-size: 15px;
      font-weight: 700;
      letter-spacing: .04em;
      text-transform: uppercase;
      color: var(--accent);
    }}
    header span {{
      font-size: 13px;
      color: var(--muted);
      font-family: 'JetBrains Mono', monospace;
    }}
    .legend {{
      display: flex;
      gap: 20px;
      margin-left: auto;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: var(--muted);
    }}
    .legend-dot {{
      width: 10px;
      height: 10px;
      border-radius: 2px;
    }}
    #canvas {{
      flex: 1;
      overflow: hidden;
      position: relative;
    }}
    svg {{
      width: 100%;
      height: 100%;
      cursor: grab;
    }}
    svg:active {{ cursor: grabbing; }}
    .node rect, .node ellipse, .node polygon {{
      stroke-width: 1.5px;
      filter: drop-shadow(0 2px 8px rgba(0,0,0,.5));
      transition: filter .2s;
    }}
    .node:hover rect, .node:hover ellipse, .node:hover polygon {{
      filter: drop-shadow(0 0 14px currentColor);
      cursor: pointer;
    }}
    .node text {{
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      fill: #fff;
      pointer-events: none;
      dominant-baseline: middle;
      text-anchor: middle;
    }}
    .link {{
      stroke: #2d3550;
      stroke-width: 1.5px;
      fill: none;
    }}
    .link-label {{
      font-size: 9px;
      fill: var(--muted);
      font-family: 'JetBrains Mono', monospace;
    }}
    .arrowhead {{ fill: #2d3550; }}
    #tooltip {{
      position: absolute;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px 14px;
      max-width: 380px;
      font-size: 12px;
      line-height: 1.6;
      pointer-events: none;
      opacity: 0;
      transition: opacity .15s;
      z-index: 100;
    }}
    #tooltip.visible {{ opacity: 1; }}
    #tooltip h3 {{
      font-size: 11px;
      font-weight: 600;
      letter-spacing: .06em;
      text-transform: uppercase;
      color: var(--accent);
      margin-bottom: 6px;
    }}
    #tooltip .nb-tag {{
      color: #4F8EF7;
      font-size: 11px;
      margin-bottom: 2px;
    }}
    #tooltip .cell-tag {{
      color: var(--muted);
      font-size: 11px;
    }}
    #tooltip pre {{
      font-family: 'JetBrains Mono', monospace;
      font-size: 10px;
      color: var(--muted);
      white-space: pre-wrap;
      word-break: break-all;
      margin-top: 6px;
      border-top: 1px solid var(--border);
      padding-top: 6px;
      max-height: 160px;
      overflow-y: auto;
    }}
    .controls {{
      position: absolute;
      bottom: 20px;
      right: 20px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .ctrl-btn {{
      width: 34px;
      height: 34px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      color: var(--text);
      font-size: 16px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: background .15s;
    }}
    .ctrl-btn:hover {{ background: var(--border); }}
  </style>
</head>
<body>
<header>
  <h1>ETL Lineage</h1>
  <span>{notebook_names}</span>
  <div class="legend">
    <div class="legend-item"><div class="legend-dot" style="background:var(--source)"></div>Source</div>
    <div class="legend-item"><div class="legend-dot" style="background:var(--transform);border-radius:50%"></div>Transform</div>
    <div class="legend-item"><div class="legend-dot" style="background:var(--target)"></div>Target</div>
    <div class="legend-item"><div class="legend-dot" style="background:var(--cte);transform:rotate(45deg)"></div>CTE</div>
  </div>
</header>
<div id="canvas">
  <svg id="svg"></svg>
  <div id="tooltip"></div>
  <div class="controls">
    <button class="ctrl-btn" id="btn-zoom-in">+</button>
    <button class="ctrl-btn" id="btn-zoom-out">−</button>
    <button class="ctrl-btn" id="btn-reset" style="font-size:12px">⌖</button>
  </div>
</div>

<script>
const GRAPH = {graph_json};

const svg     = d3.select("#svg");
const tooltip = document.getElementById("tooltip");
const W = document.getElementById("canvas").clientWidth;
const H = document.getElementById("canvas").clientHeight;

const zoom = d3.zoom().scaleExtent([0.1, 4]).on("zoom", e => g.attr("transform", e.transform));
svg.call(zoom);
const g = svg.append("g");

// Arrow marker
svg.append("defs").append("marker")
  .attr("id", "arrow")
  .attr("viewBox", "0 -5 10 10")
  .attr("refX", 22).attr("refY", 0)
  .attr("markerWidth", 6).attr("markerHeight", 6)
  .attr("orient", "auto")
  .append("path").attr("d", "M0,-5L10,0L0,5").attr("class", "arrowhead");

const nodeMap = {{}};
GRAPH.nodes.forEach(n => nodeMap[n.id] = n);

// Edges
const linkG = g.append("g");
GRAPH.links.forEach(link => {{
  const s = nodeMap[link.source], t = nodeMap[link.target];
  if (!s || !t) return;
  linkG.append("path")
    .attr("class", "link")
    .attr("d", cubic(s.x, s.y, t.x, t.y))
    .attr("marker-end", "url(#arrow)");
  if (link.label) {{
    linkG.append("text")
      .attr("class", "link-label")
      .attr("x", (s.x + t.x) / 2)
      .attr("y", (s.y + t.y) / 2 - 6)
      .attr("text-anchor", "middle")
      .text(link.label);
  }}
}});

// Nodes
const nodeG = g.append("g");
GRAPH.nodes.forEach(n => {{
  const ng = nodeG.append("g").attr("class", "node")
    .attr("transform", `translate(${{n.x}},${{n.y}})`);

  const lines = n.label.split("\\n");
  const w = Math.max(130, lines.reduce((a, l) => Math.max(a, l.length * 7.5), 0) + 28);
  const h = lines.length * 18 + 18;

  if (n.type === "transform") {{
    ng.append("ellipse")
      .attr("rx", w / 2).attr("ry", h / 2)
      .attr("fill", "#111827").attr("stroke", n.color);
  }} else if (n.type === "cte") {{
    const s = Math.min(w, h) * 0.55;
    ng.append("polygon")
      .attr("points", `0,${{-s}} ${{s}},0 0,${{s}} ${{-s}},0`)
      .attr("fill", "#120f1e").attr("stroke", n.color).attr("stroke-width", 1.5);
  }} else {{
    ng.append("rect")
      .attr("x", -w/2).attr("y", -h/2)
      .attr("width", w).attr("height", h)
      .attr("rx", 6).attr("fill", "#0a1120").attr("stroke", n.color);
  }}

  lines.forEach((line, i) => {{
    ng.append("text")
      .attr("y", (i - (lines.length - 1) / 2) * 18)
      .text(line)
      .style("font-weight", i === 0 ? "600" : "400")
      .style("fill",        i === 0 ? "#fff" : "#94a3b8");
  }});

  // Color pip
  ng.append("circle").attr("cx", -w/2 + 8).attr("cy", -h/2 + 8).attr("r", 4).attr("fill", n.color);

  // Tooltip
  ng.on("mouseover", (event) => {{
    tooltip.innerHTML =
      `<h3>${{n.type}}</h3>` +
      `<div>${{n.label.replace(/\\n/g, " ")}}</div>` +
      (n.notebook ? `<div class="nb-tag">📓 ${{n.notebook}}</div>` : "") +
      (n.cell >= 0 ? `<div class="cell-tag">Cell #${{n.cell}}</div>` : "") +
      (n.preview  ? `<pre>${{n.preview}}</pre>` : "");
    tooltip.className = "visible";
    move(event);
  }}).on("mousemove", move).on("mouseout", () => tooltip.className = "");
}});

// Fit on load
const bb = g.node().getBBox();
if (bb.width > 0) {{
  const sc = Math.min(0.9, W / bb.width, H / bb.height);
  svg.call(zoom.transform,
    d3.zoomIdentity
      .translate((W - bb.width * sc) / 2 - bb.x * sc,
                 (H - bb.height * sc) / 2 - bb.y * sc)
      .scale(sc));
}}

document.getElementById("btn-zoom-in").onclick  = () => svg.transition().call(zoom.scaleBy, 1.4);
document.getElementById("btn-zoom-out").onclick = () => svg.transition().call(zoom.scaleBy, 0.7);
document.getElementById("btn-reset").onclick = () => {{
  const bb = g.node().getBBox();
  const sc = Math.min(0.9, W / bb.width, H / bb.height);
  svg.transition().call(zoom.transform,
    d3.zoomIdentity
      .translate((W - bb.width * sc) / 2 - bb.x * sc,
                 (H - bb.height * sc) / 2 - bb.y * sc)
      .scale(sc));
}};

function cubic(x1, y1, x2, y2) {{
  const dx = (x2 - x1) * 0.5;
  return `M${{x1}},${{y1}} C${{x1+dx}},${{y1}} ${{x2-dx}},${{y2}} ${{x2}},${{y2}}`;
}}
function move(event) {{
  const r = document.getElementById("canvas").getBoundingClientRect();
  let x = event.clientX - r.left + 14;
  let y = event.clientY - r.top  + 14;
  if (x + 400 > W) x -= 420;
  tooltip.style.left = x + "px";
  tooltip.style.top  = y + "px";
}}
</script>
</body>
</html>"""

# ── 8. Save ───────────────────────────────────────────────────────────────────

OUTPUT_HTML.write_text(HTML, encoding="utf-8")
print(f"✅ Diagram saved: {OUTPUT_HTML}")
print(f"   Open in browser: file://{OUTPUT_HTML.resolve()}")