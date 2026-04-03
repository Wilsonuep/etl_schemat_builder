"""
generate_etl_diagram.py
━━━━━━━━━━━━━━━━━━━━━━━
Reads gold_transformer.ipynb and silver_cleaner.ipynb from the same directory,
extracts SQL via sqlglot, and writes two separate HTML files with interactive
force-directed diagrams, swim-lane backgrounds, and edge highlighting.

Usage:
    pip install sqlglot networkx
    python generate_etl_diagram.py
"""

import json
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

SQL_DIALECT = "spark"

NODE_COLORS = {
    "source":    "#00C9A7",
    "transform": "#4F8EF7",
    "target":    "#F76E4F",
    "cte":       "#A78BFA",
}

NOTEBOOK_EDGE_COLORS = [
    "#4F8EF7",
    "#F7A24F",
    "#A78BFA",
    "#4FF7B0",
]

# ── 1. Load notebooks ─────────────────────────────────────────────────────────

def load_notebook(path):
    if not path.exists():
        print(f"  ⚠️  Not found, skipping: {path}")
        return path.stem, []
    with open(path, "r", encoding="utf-8") as fh:
        nb = json.load(fh)
    cells = nb.get("cells", [])
    print(f"  ✅ {path.name} — {len(cells)} cells")
    return path.stem, cells

print("Loading notebooks...")
all_notebook_cells = []
for nb_path in NOTEBOOK_PATHS:
    name, cells = load_notebook(nb_path)
    if cells:
        all_notebook_cells.append((name, cells))

if not all_notebook_cells:
    sys.exit("❌ No notebooks loaded.")

print(f"✔ Loaded {len(all_notebook_cells)} notebook(s)\n")

# ── 2. SQL Extraction ─────────────────────────────────────────────────────────

@dataclass
class SQLFragment:
    source_cell_index: int
    raw_sql: str
    extraction_method: str
    notebook_name: str = ""

_SPARK_SQL_RE = re.compile(
    r'spark\.sql\s*\(\s*(?:f?"""([\s\S]*?)"""|f?\'\'\'([\s\S]*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*\)',
    re.IGNORECASE,
)
_PANDAS_SQL_RE = re.compile(
    r'pd(?:andas)?\.read_sql(?:_query)?\s*\(\s*(?:f?"""([\s\S]*?)"""|f?\'\'\'([\s\S]*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*',
    re.IGNORECASE,
)
_MAGIC_SQL_RE  = re.compile(r'^%%sql\s*\n([\s\S]+)', re.MULTILINE | re.IGNORECASE)
_INLINE_SQL_RE = re.compile(
    r'(?:SELECT|INSERT\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)|WITH\s+\w+\s+AS)[\s\S]{20,}?(?:FROM|WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|;)',
    re.IGNORECASE,
)

def extract_sql(cells, notebook_name):
    fragments = []
    for idx, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue
        src_lines = cell.get("source", [])
        src = "".join(src_lines) if isinstance(src_lines, list) else src_lines

        m = _MAGIC_SQL_RE.search(src)
        if m:
            fragments.append(SQLFragment(idx, m.group(1).strip(), "magic", notebook_name))
            continue
        for m in _SPARK_SQL_RE.finditer(src):
            sql = next(g for g in m.groups() if g is not None)
            fragments.append(SQLFragment(idx, sql.strip(), "spark_sql", notebook_name))
        for m in _PANDAS_SQL_RE.finditer(src):
            sql = next((g for g in m.groups() if g is not None), None)
            if sql:
                fragments.append(SQLFragment(idx, sql.strip(), "pandas_sql", notebook_name))
        if not any(f.source_cell_index == idx and f.notebook_name == notebook_name for f in fragments):
            for m in _INLINE_SQL_RE.finditer(src):
                fragments.append(SQLFragment(idx, m.group(0).strip(), "raw_string", notebook_name))
    return fragments

# ── 3. sqlglot Parsing ────────────────────────────────────────────────────────

try:
    import sqlglot
    import sqlglot.expressions as exp
except ImportError:
    sys.exit("❌ sqlglot not installed. Run: pip install sqlglot")

@dataclass
class TableLineage:
    source_tables: list = field(default_factory=list)
    target_table: str = None
    ctes: list = field(default_factory=list)
    operations: list = field(default_factory=list)
    cell_index: int = -1
    raw_sql: str = ""
    notebook_name: str = ""
    parse_error: str = None

def normalize_table(t):
    db = t.args.get("db")
    db = db.name if db is not None else None
    name = t.name or ""
    return ".".join(p for p in [db, name] if p)

def parse_fragment(frag):
    lin = TableLineage(cell_index=frag.source_cell_index,
                       raw_sql=frag.raw_sql, notebook_name=frag.notebook_name)
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
            if tgt: lin.target_table = normalize_table(tgt)
        elif isinstance(stmt, exp.Create):
            lin.operations.append("CREATE")
            tgt = stmt.find(exp.Table)
            if tgt: lin.target_table = normalize_table(tgt)
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

# ── 4. Build graph ────────────────────────────────────────────────────────────

try:
    import networkx as nx
except ImportError:
    sys.exit("❌ networkx not installed. Run: pip install networkx")

def build_graph(lineages, edge_color):
    G = nx.DiGraph()
    for i, lin in enumerate(lineages):
        if lin.parse_error:
            continue
        ops_label    = " | ".join(lin.operations) if lin.operations else "SELECT"
        transform_id = f"__xform_{i}__"
        G.add_node(transform_id, label=ops_label, node_type="transform",
                   cell=lin.cell_index, notebook=lin.notebook_name,
                   sql_preview=lin.raw_sql[:300])

        for src in lin.source_tables:
            if not G.has_node(src):
                G.add_node(src, label=src, node_type="source", cell=-1, notebook="", sql_preview="")
            G.add_edge(src, transform_id, label="reads", color=edge_color)

        for cte in lin.ctes:
            cte_id = f"CTE:{cte}"
            if not G.has_node(cte_id):
                G.add_node(cte_id, label=f"CTE: {cte}", node_type="cte", cell=-1, notebook="", sql_preview="")
            G.add_edge(cte_id, transform_id, label="via CTE", color=NODE_COLORS["cte"])

        if lin.target_table:
            if not G.has_node(lin.target_table):
                G.add_node(lin.target_table, label=lin.target_table, node_type="target",
                           cell=-1, notebook="", sql_preview="")
            G.add_edge(transform_id, lin.target_table, label="writes", color=edge_color)
        else:
            result_id = f"__result_{i}__"
            G.add_node(result_id, label=f"Result / Cell {lin.cell_index}",
                       node_type="target", cell=lin.cell_index,
                       notebook=lin.notebook_name, sql_preview="")
            G.add_edge(transform_id, result_id, label="returns", color=edge_color)
    return G

# ── 5. Serialize with layer hints for JS force layout ─────────────────────────

LAYER_ORDER = {"source": 0, "cte": 1, "transform": 2, "target": 3}

def graph_to_json(G):
    nodes, links = [], []
    for node_id, data in G.nodes(data=True):
        nodes.append({
            "id":       node_id,
            "label":    data.get("label", node_id),
            "type":     data.get("node_type", "source"),
            "color":    NODE_COLORS.get(data.get("node_type", "source"), "#888"),
            "layer":    LAYER_ORDER.get(data.get("node_type", "source"), 0),
            "cell":     data.get("cell", -1),
            "notebook": data.get("notebook", ""),
            "preview":  data.get("sql_preview", ""),
        })
    for src, tgt, data in G.edges(data=True):
        links.append({
            "source": src, "target": tgt,
            "label":  data.get("label", ""),
            "color":  data.get("color", "#4F8EF7"),
        })
    return json.dumps({"nodes": nodes, "links": links}, ensure_ascii=False)

# ── 6. HTML render ────────────────────────────────────────────────────────────

def render_html(nb_name, graph_json_str, edge_color, ok_count, err_count, frag_count):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>ETL Lineage — {nb_name}</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@300;500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg:        #0D0F14;
      --surface:   #161922;
      --surface2:  #1e2330;
      --border:    #252A38;
      --text:      #E2E8F0;
      --muted:     #64748B;
      --accent:    {edge_color};
      --source:    #00C9A7;
      --transform: #4F8EF7;
      --target:    #F76E4F;
      --cte:       #A78BFA;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: var(--bg); color: var(--text);
      font-family: 'DM Sans', sans-serif;
      height: 100vh; display: flex; flex-direction: column; overflow: hidden;
    }}
    header {{
      padding: 11px 20px; background: var(--surface);
      border-bottom: 1px solid var(--border);
      display: flex; align-items: center; gap: 14px; flex-shrink: 0;
    }}
    .h-title {{ font-size: 12px; font-weight: 700; letter-spacing: .07em; text-transform: uppercase; color: var(--accent); }}
    .h-nb    {{ font-size: 12px; font-family: 'JetBrains Mono', monospace; color: var(--muted); }}
    .h-stats {{ margin-left: auto; display: flex; gap: 20px; }}
    .stat    {{ display: flex; flex-direction: column; align-items: center; font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }}
    .stat strong {{ font-size: 17px; font-weight: 700; color: var(--text); line-height: 1.1; }}

    /* Hint bar */
    #hint {{
      padding: 5px 20px; background: #0f111a;
      border-bottom: 1px solid var(--border);
      font-size: 11px; color: var(--muted);
      display: flex; gap: 24px; flex-shrink: 0;
    }}
    #hint span {{ display: flex; align-items: center; gap: 6px; }}
    #hint kbd {{
      background: var(--surface2); border: 1px solid var(--border);
      border-radius: 3px; padding: 1px 5px; font-size: 10px;
      font-family: 'JetBrains Mono', monospace; color: var(--text);
    }}

    #canvas {{ flex: 1; overflow: hidden; position: relative; }}
    svg {{ width: 100%; height: 100%; cursor: grab; }}
    svg:active {{ cursor: grabbing; }}

    /* Swim-lane labels */
    .lane-label {{
      font-family: 'DM Sans', sans-serif;
      font-size: 11px; font-weight: 700;
      letter-spacing: .08em; text-transform: uppercase;
      opacity: 0.35;
    }}

    /* Edges */
    .link {{
      fill: none; stroke-width: 1.8px; opacity: 0.3;
      transition: opacity .15s, stroke-width .15s;
    }}
    .link.highlighted {{ opacity: 1 !important; stroke-width: 3px; }}
    .link.dimmed      {{ opacity: 0.05 !important; }}
    .link-label {{
      font-size: 9px; fill: var(--muted);
      font-family: 'JetBrains Mono', monospace;
      opacity: 0;
      transition: opacity .15s;
    }}
    .link-label.visible {{ opacity: 1; }}

    /* Nodes */
    .node {{ cursor: grab; }}
    .node:active {{ cursor: grabbing; }}
    .node .shape {{
      stroke-width: 1.5px;
      filter: drop-shadow(0 2px 8px rgba(0,0,0,.6));
      transition: filter .2s, stroke-width .15s, opacity .15s;
    }}
    .node.highlighted .shape {{
      stroke-width: 2.5px;
      filter: drop-shadow(0 0 18px currentColor);
    }}
    .node.dimmed .shape {{ opacity: 0.25; }}
    .node text {{
      font-family: 'JetBrains Mono', monospace; font-size: 10.5px;
      fill: #fff; pointer-events: none;
      dominant-baseline: middle; text-anchor: middle;
      transition: opacity .15s;
    }}
    .node.dimmed text {{ opacity: 0.25; }}

    /* Legend */
    #legend {{
      position: absolute; top: 14px; left: 14px;
      background: var(--surface); border: 1px solid var(--border);
      border-left: 3px solid var(--accent);
      border-radius: 10px; padding: 13px 15px; z-index: 50; min-width: 165px;
    }}
    #legend h4 {{
      font-size: 10px; font-weight: 700; letter-spacing: .08em;
      text-transform: uppercase; color: var(--muted); margin-bottom: 9px;
    }}
    .leg-row {{
      display: flex; align-items: center; gap: 9px;
      margin-bottom: 7px; font-size: 12px; color: var(--text);
    }}
    .leg-row:last-child {{ margin-bottom: 0; }}
    .l-rect    {{ width: 20px; height: 12px; border-radius: 3px; border: 1.5px solid; background: #080f1a; flex-shrink: 0; }}
    .l-ell     {{ width: 24px; height: 13px; border-radius: 50%; border: 1.5px solid var(--transform); background: #0f1726; flex-shrink: 0; }}
    .l-dia     {{ width: 12px; height: 12px; border: 1.5px solid var(--cte); background: #120f1e; transform: rotate(45deg); flex-shrink: 0; margin: 0 4px; }}
    hr.ld {{ border: none; border-top: 1px solid var(--border); margin: 9px 0; }}
    .leg-edge  {{ display: flex; align-items: center; gap: 0; margin-bottom: 7px; font-size: 12px; color: var(--text); }}
    .leg-edge:last-child {{ margin-bottom: 0; }}
    .l-line    {{ width: 20px; height: 2px; border-radius: 1px; flex-shrink: 0; }}
    .l-arr     {{ width:0; height:0; border-top:4px solid transparent; border-bottom:4px solid transparent; border-left:7px solid; flex-shrink:0; margin-right:8px; }}

    /* Tooltip */
    #tooltip {{
      position: absolute;
      background: var(--surface2); border: 1px solid var(--border);
      border-left: 3px solid var(--accent);
      border-radius: 8px; padding: 12px 14px; max-width: 420px;
      font-size: 12px; line-height: 1.6;
      pointer-events: none; opacity: 0; transition: opacity .15s;
      z-index: 200; box-shadow: 0 8px 32px rgba(0,0,0,.5);
    }}
    #tooltip.visible {{ opacity: 1; }}
    #tooltip h3 {{ font-size: 10px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; color: var(--accent); margin-bottom: 4px; }}
    #tooltip .tt-name {{ font-size: 13px; font-weight: 600; color: #fff; margin-bottom: 4px; }}
    #tooltip .tt-nb   {{ color: #4F8EF7; font-size: 11px; margin-bottom: 2px; }}
    #tooltip .tt-cell {{ color: var(--muted); font-size: 11px; margin-bottom: 6px; }}
    #tooltip pre {{
      font-family: 'JetBrains Mono', monospace; font-size: 10px; color: var(--muted);
      white-space: pre-wrap; word-break: break-all;
      margin-top: 8px; border-top: 1px solid var(--border); padding-top: 8px;
      max-height: 200px; overflow-y: auto;
    }}

    /* Controls */
    .controls {{
      position: absolute; bottom: 18px; right: 18px;
      display: flex; flex-direction: column; gap: 5px;
    }}
    .ctrl-btn {{
      width: 32px; height: 32px;
      background: var(--surface); border: 1px solid var(--border);
      border-radius: 6px; color: var(--text); font-size: 15px; cursor: pointer;
      display: flex; align-items: center; justify-content: center;
      transition: background .15s, border-color .15s;
    }}
    .ctrl-btn:hover {{ background: var(--surface2); border-color: var(--accent); }}
  </style>
</head>
<body>
<header>
  <div class="h-title">ETL Lineage</div>
  <div class="h-nb">📓 {nb_name}</div>
  <div class="h-stats">
    <div class="stat"><strong>{frag_count}</strong>Queries</div>
    <div class="stat"><strong>{ok_count}</strong>Parsed</div>
    <div class="stat" style="color:#F76E4F"><strong>{err_count}</strong>Errors</div>
  </div>
</header>
<div id="hint">
  <span>🖱 <kbd>Drag</kbd> nodes to reposition</span>
  <span>🔍 <kbd>Scroll</kbd> to zoom</span>
  <span>✨ <kbd>Hover</kbd> a node to highlight its connections</span>
  <span>⌖ <kbd>Double-click</kbd> canvas to reset view</span>
</div>

<div id="canvas">
  <svg id="svg"></svg>

  <div id="legend">
    <h4>Node types</h4>
    <div class="leg-row"><div class="l-rect" style="border-color:#00C9A7"></div>Source table</div>
    <div class="leg-row"><div class="l-ell"></div>Transform</div>
    <div class="leg-row"><div class="l-rect" style="border-color:#F76E4F"></div>Target table</div>
    <div class="leg-row"><div class="l-dia"></div>CTE</div>
    <hr class="ld"/>
    <h4>Connections</h4>
    <div class="leg-edge">
      <div class="l-line" style="background:{edge_color}"></div>
      <div class="l-arr"  style="border-left-color:{edge_color}"></div>reads / writes
    </div>
    <div class="leg-edge">
      <div class="l-line" style="background:#A78BFA"></div>
      <div class="l-arr"  style="border-left-color:#A78BFA"></div>via CTE
    </div>
    <hr class="ld"/>
    <h4>Swim lanes</h4>
    <div class="leg-row" style="font-size:11px;color:var(--muted)">Left → Right flow</div>
    <div class="leg-row" style="font-size:11px;color:#00C9A7">① Source</div>
    <div class="leg-row" style="font-size:11px;color:#A78BFA">② CTE</div>
    <div class="leg-row" style="font-size:11px;color:#4F8EF7">③ Transform</div>
    <div class="leg-row" style="font-size:11px;color:#F76E4F">④ Target</div>
  </div>

  <div id="tooltip"></div>
  <div class="controls">
    <button class="ctrl-btn" id="btn-zi"  title="Zoom in">+</button>
    <button class="ctrl-btn" id="btn-zo"  title="Zoom out">−</button>
    <button class="ctrl-btn" id="btn-fit" title="Fit view" style="font-size:10px">⌖</button>
  </div>
</div>

<script>
const GRAPH = {graph_json_str};
const canvas  = document.getElementById("canvas");
const svg     = d3.select("#svg");
const tooltip = document.getElementById("tooltip");

// ── Zoom ────────────────────────────────────────────────────────────────────
const zoom = d3.zoom().scaleExtent([0.04, 6]).on("zoom", e => root.attr("transform", e.transform));
svg.call(zoom);
svg.on("dblclick.zoom", null).on("dblclick", fitView);
const root = svg.append("g");

// ── Swim-lane bands ──────────────────────────────────────────────────────────
const LANES = [
  {{ label: "① Source",    color: "#00C9A7", layer: 0 }},
  {{ label: "② CTE",       color: "#A78BFA", layer: 1 }},
  {{ label: "③ Transform", color: "#4F8EF7", layer: 2 }},
  {{ label: "④ Target",    color: "#F76E4F", layer: 3 }},
];
const laneG = root.append("g").attr("class", "lanes");

// ── Defs: per-color arrowheads ────────────────────────────────────────────────
const defs = svg.append("defs");
[...new Set(GRAPH.links.map(l => l.color))].forEach(color => {{
  const id = "arr-" + color.replace("#","");
  defs.append("marker")
    .attr("id", id)
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 28).attr("refY", 0)
    .attr("markerWidth", 7).attr("markerHeight", 7)
    .attr("orient", "auto")
    .append("path").attr("d", "M0,-5L10,0L0,5").attr("fill", color);
}});

// ── Build adjacency for highlight ────────────────────────────────────────────
const neighbors = {{}};
const edgeIndex  = {{}};
GRAPH.nodes.forEach(n => neighbors[n.id] = new Set());
GRAPH.links.forEach(l => {{
  neighbors[l.source].add(l.target);
  neighbors[l.target].add(l.source);
}});

// ── Edge layer ───────────────────────────────────────────────────────────────
const linkG = root.append("g").attr("class", "link-layer");
const linkSel = linkG.selectAll("path")
  .data(GRAPH.links).join("path")
  .attr("class", "link")
  .attr("stroke", d => d.color)
  .attr("marker-end", d => `url(#arr-${{d.color.replace("#","")}})`);

const linkLabelSel = linkG.selectAll("text")
  .data(GRAPH.links).join("text")
  .attr("class", "link-label")
  .attr("text-anchor", "middle")
  .text(d => d.label);

// ── Node layer ───────────────────────────────────────────────────────────────
const nodeG   = root.append("g").attr("class", "node-layer");
const nodeSel = nodeG.selectAll("g.node")
  .data(GRAPH.nodes).join("g")
  .attr("class", "node")
  .call(d3.drag()
    .on("start", (event, d) => {{
      if (!event.active) sim.alphaTarget(0.15).restart();
      d.fx = d.x; d.fy = d.y;
    }})
    .on("drag",  (event, d) => {{ d.fx = event.x; d.fy = event.y; }})
    .on("end",   (event, d) => {{
      if (!event.active) sim.alphaTarget(0);
      d.fx = null; d.fy = null;
    }})
  );

// Draw shapes
nodeSel.each(function(n) {{
  const g   = d3.select(this);
  const lines = n.label.split("\\n");
  n._w = Math.max(145, lines.reduce((a, l) => Math.max(a, l.length * 7.6), 0) + 32);
  n._h = lines.length * 19 + 20;

  if (n.type === "transform") {{
    g.append("ellipse").attr("class","shape")
      .attr("rx", n._w/2).attr("ry", n._h/2)
      .attr("fill","#0f1726").attr("stroke", n.color);
  }} else if (n.type === "cte") {{
    const s = 36;
    g.append("polygon").attr("class","shape")
      .attr("points", `0,${{-s}} ${{s}},0 0,${{s}} ${{-s}},0`)
      .attr("fill","#120f1e").attr("stroke", n.color).attr("stroke-width",1.5);
    n._w = s * 2.2; n._h = s * 2;
  }} else {{
    g.append("rect").attr("class","shape")
      .attr("x",-n._w/2).attr("y",-n._h/2)
      .attr("width",n._w).attr("height",n._h)
      .attr("rx",7).attr("fill","#080f1a").attr("stroke", n.color);
  }}

  // Text
  lines.forEach((line, i) => {{
    g.append("text")
      .attr("y", (i - (lines.length-1)/2) * 19)
      .text(line)
      .style("font-weight", i===0 ? "600" : "400")
      .style("fill",        i===0 ? "#fff" : "#94a3b8");
  }});

  // Color pip
  if (n.type !== "cte") {{
    g.append("circle")
      .attr("cx",-n._w/2+8).attr("cy",-n._h/2+8)
      .attr("r",4).attr("fill",n.color);
  }}
}});

// Hover to highlight
nodeSel
  .on("mouseover", function(event, n) {{
    // Highlight connected edges & nodes
    const connected = neighbors[n.id];
    linkSel
      .classed("highlighted", d => d.source.id===n.id || d.target.id===n.id)
      .classed("dimmed",      d => d.source.id!==n.id && d.target.id!==n.id);
    linkLabelSel
      .classed("visible",     d => d.source.id===n.id || d.target.id===n.id);
    nodeSel
      .classed("highlighted", d => d.id===n.id || connected.has(d.id))
      .classed("dimmed",      d => d.id!==n.id && !connected.has(d.id));

    // Tooltip
    tooltip.innerHTML =
      `<h3>${{n.type}}</h3>` +
      `<div class="tt-name">${{n.label.replace(/\\n/g," ")}}</div>` +
      (n.notebook ? `<div class="tt-nb">📓 ${{n.notebook}}</div>` : "") +
      (n.cell >= 0 ? `<div class="tt-cell">Cell #${{n.cell}}</div>` : "") +
      (n.preview  ? `<pre>${{n.preview}}</pre>` : "");
    tooltip.className = "visible";
    posTip(event);
  }})
  .on("mousemove", posTip)
  .on("mouseout", () => {{
    linkSel.classed("highlighted dimmed", false);
    linkLabelSel.classed("visible", false);
    nodeSel.classed("highlighted dimmed", false);
    tooltip.className = "";
  }});

// ── Force simulation ─────────────────────────────────────────────────────────
const N_LAYERS = 4;
const LAYER_W  = 320;   // horizontal spacing between layers

const sim = d3.forceSimulation(GRAPH.nodes)
  .force("link", d3.forceLink(GRAPH.links)
    .id(d => d.id)
    .distance(d => {{
      // longer distance between source→transform and transform→target
      const sl = d.source.layer ?? 0, tl = d.target.layer ?? 0;
      return Math.abs(sl - tl) > 1 ? 260 : 180;
    }})
    .strength(0.8))
  .force("charge",  d3.forceManyBody().strength(-600).distanceMax(500))
  .force("collide", d3.forceCollide(d => Math.max(d._w ?? 80, d._h ?? 40) / 2 + 14))
  // Pin each node to its horizontal lane
  .force("x", d3.forceX(d => 180 + (d.layer ?? 0) * LAYER_W).strength(0.9))
  .force("y", d3.forceY(() => canvas.clientHeight / 2).strength(0.04))
  .alphaDecay(0.025)
  .on("tick", ticked);

// Draw swim-lane backgrounds after first positions known
sim.on("end", drawLanes);

function ticked() {{
  // Update edge paths
  linkSel.attr("d", d => {{
    const sx = d.source.x ?? 0, sy = d.source.y ?? 0;
    const tx = d.target.x ?? 0, ty = d.target.y ?? 0;
    const dx = Math.abs(tx - sx) * 0.45;
    return `M${{sx}},${{sy}} C${{sx+dx}},${{sy}} ${{tx-dx}},${{ty}} ${{tx}},${{ty}}`;
  }});
  linkLabelSel
    .attr("x", d => ((d.source.x??0) + (d.target.x??0)) / 2)
    .attr("y", d => ((d.source.y??0) + (d.target.y??0)) / 2 - 7);
  // Update node positions
  nodeSel.attr("transform", d => `translate(${{d.x??0}},${{d.y??0}})`);
}}

function drawLanes() {{
  const allX = GRAPH.nodes.map(n => n.x ?? 0);
  const allY = GRAPH.nodes.map(n => n.y ?? 0);
  const minY = Math.min(...allY) - 60;
  const maxY = Math.max(...allY) + 60;

  LANES.forEach((lane, i) => {{
    const cx = 180 + i * LAYER_W;
    const lw = LAYER_W * 0.85;

    laneG.append("rect")
      .attr("x", cx - lw/2).attr("y", minY)
      .attr("width", lw).attr("height", maxY - minY)
      .attr("rx", 12)
      .attr("fill", lane.color)
      .attr("opacity", 0.04);

    laneG.append("rect")
      .attr("x", cx - lw/2).attr("y", minY)
      .attr("width", lw).attr("height", 28)
      .attr("rx", 12)
      .attr("fill", lane.color)
      .attr("opacity", 0.12);

    laneG.append("text")
      .attr("class","lane-label")
      .attr("x", cx).attr("y", minY + 18)
      .attr("text-anchor","middle")
      .attr("fill", lane.color)
      .text(lane.label);
  }});

  fitView();
}}

// ── Fit view ─────────────────────────────────────────────────────────────────
function fitView() {{
  const W = canvas.clientWidth, H = canvas.clientHeight;
  const bb = root.node().getBBox();
  if (!bb.width) return;
  const sc = Math.min(0.9, W / (bb.width + 80), H / (bb.height + 80));
  svg.transition().duration(400).call(zoom.transform,
    d3.zoomIdentity
      .translate((W - bb.width*sc)/2 - bb.x*sc, (H - bb.height*sc)/2 - bb.y*sc)
      .scale(sc));
}}

document.getElementById("btn-zi").onclick  = () => svg.transition().duration(250).call(zoom.scaleBy, 1.4);
document.getElementById("btn-zo").onclick  = () => svg.transition().duration(250).call(zoom.scaleBy, 0.72);
document.getElementById("btn-fit").onclick = fitView;

function posTip(event) {{
  const r = canvas.getBoundingClientRect();
  const W = canvas.clientWidth;
  let x = event.clientX - r.left + 16;
  let y = event.clientY - r.top  + 16;
  if (x + 440 > W) x = event.clientX - r.left - 450;
  tooltip.style.left = x + "px";
  tooltip.style.top  = y + "px";
}}
</script>
</body>
</html>"""

# ── 7. Generate one HTML per notebook ─────────────────────────────────────────

print("Building diagrams...\n")

for nb_idx, (nb_name, nb_cells) in enumerate(all_notebook_cells):
    edge_color = NOTEBOOK_EDGE_COLORS[nb_idx % len(NOTEBOOK_EDGE_COLORS)]

    frags     = extract_sql(nb_cells, nb_name)
    lineages  = [parse_fragment(f) for f in frags]
    ok_count  = sum(1 for l in lineages if not l.parse_error)
    err_count = sum(1 for l in lineages if l.parse_error)

    for l in lineages:
        if l.parse_error:
            print(f"  ⚠️  [{nb_name}] cell {l.cell_index}: {l.parse_error[:80]}")

    G   = build_graph(lineages, edge_color)
    gj  = graph_to_json(G)
    html = render_html(nb_name, gj, edge_color, ok_count, err_count, len(frags))

    out_path = HERE / f"etl_diagram_{nb_name}.html"
    out_path.write_text(html, encoding="utf-8")

    print(f"  📓 {nb_name}")
    print(f"     {len(frags)} fragments → {ok_count} parsed, {err_count} errors")
    print(f"     {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    print(f"     ✅ {out_path.name}\n")

print("Done.")
