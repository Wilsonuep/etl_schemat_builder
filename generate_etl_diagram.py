"""
generate_etl_diagram.py
━━━━━━━━━━━━━━━━━━━━━━━
Interactive CLI to generate ETL lineage diagrams from .ipynb notebooks.

Usage:
    pip install sqlglot networkx
    python generate_etl_diagram.py

Menu flow:
    Include full scope?
    ├── Yes (full scope)
    │   ├── Full diagram        → one HTML, all notebooks combined
    │   └── Breakdown by target
    │       ├── Full scope      → one HTML per target (all notebooks)
    │       ├── Gold notebook   → one HTML per gold target (includes silver steps, no silver targets)
    │       └── Specific target → pick from list → one HTML, all notebooks, one target
    └── Let me choose (pick one notebook)
        ├── Full diagram        → one HTML for chosen notebook
        └── Breakdown by target → one HTML per target in chosen notebook
"""

import json, re, sys
from dataclasses import dataclass, field
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
# 0. CONFIG
# ══════════════════════════════════════════════════════════════════════════════

HERE          = Path(__file__).parent
NOTEBOOKS_DIR = HERE / "notebooks"
OUTPUT_DIR    = HERE / "diagrams"
OUTPUT_DIR.mkdir(exist_ok=True)

NOTEBOOK_PATHS = [
    NOTEBOOKS_DIR / "gold_transformer.ipynb",
    NOTEBOOKS_DIR / "silver_cleaner.ipynb",
]

# Which notebook contains the "final" gold-layer targets
GOLD_NOTEBOOK = "gold_transformer"

SQL_DIALECT = "spark"

NODE_COLORS = {
    "source":    "#00C9A7",
    "transform": "#4F8EF7",
    "target":    "#F76E4F",
    "cte":       "#A78BFA",
}

NOTEBOOK_EDGE_COLORS = {
    "gold_transformer": "#4F8EF7",
    "silver_cleaner":   "#F7A24F",
}
DEFAULT_EDGE_COLOR = "#4F8EF7"

# Schema → horizontal lane index (left to right)
# Extend this when adding new schemas
SCHEMA_LANES = {
    "dbo":    0,
    "silver": 1,
    "gold":   2,
}
SCHEMA_COLORS = {
    "dbo":    "#00C9A7",
    "silver": "#F7A24F",
    "gold":   "#4F8EF7",
}

# ══════════════════════════════════════════════════════════════════════════════
# 1. CLI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def hr(char="─", w=58): print(char * w)

def ask(prompt, options, allow_name=False):
    print(f"\n  {prompt}")
    for i, opt in enumerate(options, 1):
        print(f"    [{i}] {opt}")
    while True:
        raw = input("  › ").strip()
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                return idx
        elif allow_name:
            matches = [i for i, o in enumerate(options) if raw.lower() in o.lower()]
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                print(f"  ✗  Ambiguous — matched: {[options[m] for m in matches]}")
                continue
        print(f"  ✗  Enter a number 1–{len(options)}"
              + (" or type part of the name" if allow_name else ""))

# ══════════════════════════════════════════════════════════════════════════════
# 2. NOTEBOOK LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_notebook(path):
    if not path.exists():
        print(f"  ⚠️  Not found: {path}")
        return path.stem, []
    with open(path, "r", encoding="utf-8") as fh:
        nb = json.load(fh)
    cells = nb.get("cells", [])
    print(f"  ✅ {path.name} — {len(cells)} cells")
    return path.stem, cells

# ══════════════════════════════════════════════════════════════════════════════
# 3. SQL EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SQLFragment:
    source_cell_index: int
    raw_sql: str
    extraction_method: str
    notebook_name: str = ""

_SPARK_SQL_RE = re.compile(
    r'spark\.sql\s*\(\s*(?:f?"""([\s\S]*?)"""|f?\'\'\'([\s\S]*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*\)',
    re.IGNORECASE)
_PANDAS_SQL_RE = re.compile(
    r'pd(?:andas)?\.read_sql(?:_query)?\s*\(\s*(?:f?"""([\s\S]*?)"""|f?\'\'\'([\s\S]*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*',
    re.IGNORECASE)
_MAGIC_SQL_RE  = re.compile(r'^%%sql\s*\n([\s\S]+)', re.MULTILINE | re.IGNORECASE)
_INLINE_SQL_RE = re.compile(
    r'(?:SELECT|INSERT\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)|WITH\s+\w+\s+AS)'
    r'[\s\S]{20,}?(?:FROM|WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|;)',
    re.IGNORECASE)

def extract_sql(cells, notebook_name):
    fragments = []
    for idx, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", [])
        src = "".join(src) if isinstance(src, list) else src
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
        if not any(f.source_cell_index == idx and f.notebook_name == notebook_name
                   for f in fragments):
            for m in _INLINE_SQL_RE.finditer(src):
                fragments.append(SQLFragment(idx, m.group(0).strip(), "raw_string", notebook_name))
    return fragments

# ══════════════════════════════════════════════════════════════════════════════
# 4. SQLGLOT PARSING
# ══════════════════════════════════════════════════════════════════════════════

try:
    import sqlglot
    import sqlglot.expressions as exp
except ImportError:
    sys.exit("❌  sqlglot not installed. Run: pip install sqlglot")

@dataclass
class TableLineage:
    source_tables: list = field(default_factory=list)
    target_table:  str  = None
    ctes:          list = field(default_factory=list)
    operations:    list = field(default_factory=list)
    cell_index:    int  = -1
    raw_sql:       str  = ""
    notebook_name: str  = ""
    parse_error:   str  = None

def normalize_table(t):
    db   = t.args.get("db")
    db   = db.name if db is not None else None
    name = t.name or ""
    return ".".join(p for p in [db, name] if p)

def parse_fragment(frag):
    lin = TableLineage(cell_index=frag.source_cell_index,
                       raw_sql=frag.raw_sql, notebook_name=frag.notebook_name)
    try:
        stmts = sqlglot.parse(frag.raw_sql, dialect=SQL_DIALECT,
                              error_level=sqlglot.ErrorLevel.WARN)
    except Exception as e:
        lin.parse_error = str(e)
        return lin
    for stmt in stmts:
        if stmt is None: continue
        if isinstance(stmt, exp.Select):   lin.operations.append("SELECT")
        elif isinstance(stmt, exp.Insert):
            lin.operations.append("INSERT")
            tgt = stmt.find(exp.Table)
            if tgt: lin.target_table = normalize_table(tgt)
        elif isinstance(stmt, exp.Create):
            lin.operations.append("CREATE")
            tgt = stmt.find(exp.Table)
            if tgt: lin.target_table = normalize_table(tgt)
        elif isinstance(stmt, exp.Merge):  lin.operations.append("MERGE")
        for cte in stmt.find_all(exp.CTE): lin.ctes.append(cte.alias)
        joins = list(stmt.find_all(exp.Join))
        if joins: lin.operations.append(f"JOIN×{len(joins)}")
        cte_names = set(lin.ctes)
        for table in stmt.find_all(exp.Table):
            name = normalize_table(table)
            if name and name not in cte_names and name != lin.target_table:
                if name not in lin.source_tables:
                    lin.source_tables.append(name)
    lin.operations = list(dict.fromkeys(lin.operations))
    return lin

# ══════════════════════════════════════════════════════════════════════════════
# 5. GRAPH BUILDING
# ══════════════════════════════════════════════════════════════════════════════

try:
    import networkx as nx
except ImportError:
    sys.exit("❌  networkx not installed. Run: pip install networkx")

def build_graph(lineages, notebook_edge_map=None):
    G = nx.DiGraph()
    def ec(nb_name):
        return (notebook_edge_map or {}).get(nb_name, DEFAULT_EDGE_COLOR)

    for i, lin in enumerate(lineages):
        if lin.parse_error: continue
        ops_label    = " | ".join(lin.operations) if lin.operations else "SELECT"
        transform_id = f"__xform_{lin.notebook_name}_{i}__"
        color        = ec(lin.notebook_name)

        if not G.has_node(transform_id):
            G.add_node(transform_id, label=ops_label, node_type="transform",
                       cell=lin.cell_index, notebook=lin.notebook_name,
                       sql_preview=lin.raw_sql[:300])

        for src in lin.source_tables:
            if not G.has_node(src):
                G.add_node(src, label=src, node_type="source",
                           cell=-1, notebook="", sql_preview="")
            G.add_edge(src, transform_id, label="reads", color=color)

        for cte in lin.ctes:
            cte_id = f"CTE:{lin.notebook_name}:{cte}"
            if not G.has_node(cte_id):
                G.add_node(cte_id, label=f"CTE: {cte}", node_type="cte",
                           cell=-1, notebook=lin.notebook_name, sql_preview="")
            G.add_edge(cte_id, transform_id, label="via CTE", color=NODE_COLORS["cte"])

        if lin.target_table:
            if not G.has_node(lin.target_table):
                G.add_node(lin.target_table, label=lin.target_table, node_type="target",
                           cell=-1, notebook="", sql_preview="")
            G.add_edge(transform_id, lin.target_table, label="writes", color=color)
        else:
            result_id = f"__result_{lin.notebook_name}_{i}__"
            G.add_node(result_id, label=f"Result / Cell {lin.cell_index}",
                       node_type="target", cell=lin.cell_index,
                       notebook=lin.notebook_name, sql_preview="")
            G.add_edge(transform_id, result_id, label="returns", color=color)
    return G

def subgraph_for_target(G_full, target_id):
    if not G_full.has_node(target_id): return None
    nodes = nx.ancestors(G_full, target_id)
    nodes.add(target_id)
    return G_full.subgraph(nodes).copy()

def subgraph_for_target_hide_silver_targets(G_full, target_id, silver_targets):
    if not G_full.has_node(target_id): return None
    nodes = nx.ancestors(G_full, target_id)
    nodes.add(target_id)
    nodes -= silver_targets
    return G_full.subgraph(nodes).copy()

def get_targets_for_notebook(G, notebook_name):
    result = []
    for node_id, data in G.nodes(data=True):
        if data.get("node_type") != "target": continue
        for pred in nx.ancestors(G, node_id):
            if G.nodes[pred].get("notebook") == notebook_name:
                result.append(node_id)
                break
    return sorted(set(result))

def graph_to_json(G):
    nodes, links = [], []
    for node_id, data in G.nodes(data=True):
        nodes.append({
            "id":       node_id,
            "label":    data.get("label", node_id),
            "type":     data.get("node_type", "source"),
            "color":    NODE_COLORS.get(data.get("node_type", "source"), "#888"),
            "cell":     data.get("cell", -1),
            "notebook": data.get("notebook", ""),
            "preview":  data.get("sql_preview", ""),
        })
    for src, tgt, data in G.edges(data=True):
        links.append({
            "source": src, "target": tgt,
            "label":  data.get("label", ""),
            "color":  data.get("color", DEFAULT_EDGE_COLOR),
        })
    return json.dumps({"nodes": nodes, "links": links}, ensure_ascii=False)

# ══════════════════════════════════════════════════════════════════════════════
# 6. HTML RENDERER
# ══════════════════════════════════════════════════════════════════════════════

def render_html(title, subtitle, graph_json_str, accent_color="#4F8EF7", stats=None):
    stats      = stats or {}
    frag_count = stats.get("frags",  "—")
    ok_count   = stats.get("ok",     "—")
    err_count  = stats.get("errors",  0)

    schema_lanes_js  = json.dumps(SCHEMA_LANES)
    schema_colors_js = json.dumps(SCHEMA_COLORS)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>{title}</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@300;500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg:#0D0F14; --surface:#161922; --surface2:#1e2330; --border:#252A38;
      --text:#E2E8F0; --muted:#64748B; --accent:{accent_color};
      --source:#00C9A7; --transform:#4F8EF7; --target:#F76E4F; --cte:#A78BFA;
    }}
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;
          height:100vh;display:flex;flex-direction:column;overflow:hidden}}
    header{{padding:11px 20px;background:var(--surface);border-bottom:1px solid var(--border);
            display:flex;align-items:center;gap:14px;flex-shrink:0}}
    .h-title{{font-size:12px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--accent)}}
    .h-sub{{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--muted)}}
    .h-stats{{margin-left:auto;display:flex;gap:20px}}
    .stat{{display:flex;flex-direction:column;align-items:center;font-size:10px;
           color:var(--muted);text-transform:uppercase;letter-spacing:.04em}}
    .stat strong{{font-size:17px;font-weight:700;color:var(--text);line-height:1.1}}
    #hint{{padding:5px 20px;background:#0a0c12;border-bottom:1px solid var(--border);
           font-size:11px;color:var(--muted);display:flex;gap:24px;flex-shrink:0}}
    #hint kbd{{background:var(--surface2);border:1px solid var(--border);border-radius:3px;
               padding:1px 5px;font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text)}}
    #canvas{{flex:1;overflow:hidden;position:relative}}
    svg{{width:100%;height:100%;cursor:grab}}
    svg:active{{cursor:grabbing}}
    .lane-label{{font-family:'DM Sans',sans-serif;font-size:12px;font-weight:700;
                 letter-spacing:.1em;text-transform:uppercase;opacity:.5}}
    .link{{fill:none;stroke-width:1.8px;opacity:.28;transition:opacity .15s,stroke-width .15s}}
    .link.hl{{opacity:1!important;stroke-width:3px}}
    .link.dim{{opacity:.04!important}}
    .link-label{{font-size:9px;fill:var(--muted);font-family:'JetBrains Mono',monospace;opacity:0}}
    .link-label.hl{{opacity:1}}
    .node{{cursor:grab}}.node:active{{cursor:grabbing}}
    .node .shape{{stroke-width:1.5px;filter:drop-shadow(0 2px 8px rgba(0,0,0,.6));
                  transition:filter .2s,stroke-width .15s,opacity .15s}}
    .node.hl .shape{{stroke-width:2.5px;filter:drop-shadow(0 0 18px currentColor)}}
    .node.dim .shape{{opacity:.18}}
    .node text{{font-family:'JetBrains Mono',monospace;font-size:10.5px;fill:#fff;
                pointer-events:none;dominant-baseline:middle;text-anchor:middle}}
    .node.dim text{{opacity:.18}}
    #legend{{position:absolute;top:14px;left:14px;background:var(--surface);
             border:1px solid var(--border);border-left:3px solid var(--accent);
             border-radius:10px;padding:13px 15px;z-index:50;min-width:168px}}
    #legend h4{{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
                color:var(--muted);margin-bottom:9px}}
    .lr{{display:flex;align-items:center;gap:9px;margin-bottom:7px;font-size:12px;color:var(--text)}}
    .lr:last-child{{margin-bottom:0}}
    .l-rect{{width:20px;height:12px;border-radius:3px;border:1.5px solid;background:#080f1a;flex-shrink:0}}
    .l-ell{{width:24px;height:13px;border-radius:50%;border:1.5px solid var(--transform);background:#0f1726;flex-shrink:0}}
    .l-dia{{width:12px;height:12px;border:1.5px solid var(--cte);background:#120f1e;transform:rotate(45deg);flex-shrink:0;margin:0 4px}}
    hr.ld{{border:none;border-top:1px solid var(--border);margin:9px 0}}
    .le{{display:flex;align-items:center;gap:0;margin-bottom:7px;font-size:12px;color:var(--text)}}
    .le:last-child{{margin-bottom:0}}
    .l-ln{{width:20px;height:2px;border-radius:1px;flex-shrink:0}}
    .l-ar{{width:0;height:0;border-top:4px solid transparent;border-bottom:4px solid transparent;
           border-left:7px solid;flex-shrink:0;margin-right:8px}}
    #tooltip{{position:absolute;background:var(--surface2);border:1px solid var(--border);
              border-left:3px solid var(--accent);border-radius:8px;padding:12px 14px;
              max-width:420px;font-size:12px;line-height:1.6;pointer-events:none;
              opacity:0;transition:opacity .15s;z-index:200;box-shadow:0 8px 32px rgba(0,0,0,.5)}}
    #tooltip.vis{{opacity:1}}
    #tooltip h3{{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
                 color:var(--accent);margin-bottom:4px}}
    .tt-name{{font-size:13px;font-weight:600;color:#fff;margin-bottom:4px}}
    .tt-nb{{color:#4F8EF7;font-size:11px;margin-bottom:2px}}
    .tt-cell{{color:var(--muted);font-size:11px;margin-bottom:6px}}
    #tooltip pre{{font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--muted);
                  white-space:pre-wrap;word-break:break-all;margin-top:8px;
                  border-top:1px solid var(--border);padding-top:8px;max-height:200px;overflow-y:auto}}
    .controls{{position:absolute;bottom:18px;right:18px;display:flex;flex-direction:column;gap:5px}}
    .ctrl-btn{{width:32px;height:32px;background:var(--surface);border:1px solid var(--border);
               border-radius:6px;color:var(--text);font-size:15px;cursor:pointer;
               display:flex;align-items:center;justify-content:center;
               transition:background .15s,border-color .15s}}
    .ctrl-btn:hover{{background:var(--surface2);border-color:var(--accent)}}
  </style>
</head>
<body>
<header>
  <div class="h-title">{title}</div>
  <div class="h-sub">{subtitle}</div>
  <div class="h-stats">
    <div class="stat"><strong>{frag_count}</strong>Queries</div>
    <div class="stat"><strong>{ok_count}</strong>Parsed</div>
    <div class="stat" style="color:#F76E4F"><strong>{err_count}</strong>Errors</div>
  </div>
</header>
<div id="hint">
  <span><kbd>Hover</kbd> node → highlight connections</span>
  <span><kbd>Drag</kbd> node → reposition</span>
  <span><kbd>Scroll</kbd> → zoom</span>
  <span><kbd>Dbl-click</kbd> canvas → fit view</span>
</div>
<div id="canvas">
  <svg id="svg"></svg>
  <div id="legend">
    <h4>Nodes</h4>
    <div class="lr"><div class="l-rect" style="border-color:#00C9A7"></div>Source</div>
    <div class="lr"><div class="l-ell"></div>Transform</div>
    <div class="lr"><div class="l-rect" style="border-color:#F76E4F"></div>Target</div>
    <div class="lr"><div class="l-dia"></div>CTE</div>
    <hr class="ld"/>
    <h4>Edges</h4>
    <div class="le">
      <div class="l-ln" style="background:{accent_color}"></div>
      <div class="l-ar" style="border-left-color:{accent_color}"></div>reads / writes
    </div>
    <div class="le">
      <div class="l-ln" style="background:#A78BFA"></div>
      <div class="l-ar" style="border-left-color:#A78BFA"></div>via CTE
    </div>
    <hr class="ld"/>
    <h4>Schema lanes</h4>
    <div class="lr" style="font-size:11px;color:#00C9A7">← dbo (raw)</div>
    <div class="lr" style="font-size:11px;color:#F7A24F">── silver</div>
    <div class="lr" style="font-size:11px;color:#4F8EF7">── gold →</div>
  </div>
  <div id="tooltip"></div>
  <div class="controls">
    <button class="ctrl-btn" id="bzi">+</button>
    <button class="ctrl-btn" id="bzo">−</button>
    <button class="ctrl-btn" id="bfit" style="font-size:10px">⌖</button>
  </div>
</div>
<script>
const GRAPH        = {graph_json_str};
const SCHEMA_LANES = {schema_lanes_js};
const SCHEMA_COLORS= {schema_colors_js};
const canvas  = document.getElementById("canvas");
const svg     = d3.select("#svg");
const tooltip = document.getElementById("tooltip");

const zoom = d3.zoom().scaleExtent([0.04,6]).on("zoom",e=>root.attr("transform",e.transform));
svg.call(zoom);
svg.on("dblclick.zoom",null).on("dblclick",fitView);
const root = svg.append("g");

const defs = svg.append("defs");
[...new Set(GRAPH.links.map(l=>l.color))].forEach(color=>{{
  defs.append("marker")
    .attr("id","arr-"+color.replace("#",""))
    .attr("viewBox","0 -5 10 10").attr("refX",28).attr("refY",0)
    .attr("markerWidth",7).attr("markerHeight",7).attr("orient","auto")
    .append("path").attr("d","M0,-5L10,0L0,5").attr("fill",color);
}});

// ── Schema-aware lane assignment ─────────────────────────────────────────────
const LAYER_W = 320;

function schemaOf(node) {{
  const dot = node.label.indexOf(".");
  if (dot > -1) return node.label.slice(0, dot).toLowerCase();
  // Transform nodes: infer from notebook name
  if (node.type === "transform") {{
    if (node.notebook.includes("silver")) return "silver";
    if (node.notebook.includes("gold"))   return "gold";
  }}
  return "dbo";
}}

function laneX(node) {{
  const schema = schemaOf(node);
  const idx    = SCHEMA_LANES[schema] ?? 0;
  return 200 + idx * LAYER_W;
}}

// Pre-compute schema on each node so drawLanes can use it
GRAPH.nodes.forEach(n => {{ n._schema = schemaOf(n); }});

// ── Adjacency for highlight ──────────────────────────────────────────────────
const nbrs={{}};
GRAPH.nodes.forEach(n=>nbrs[n.id]=new Set());
GRAPH.links.forEach(l=>{{
  nbrs[l.source]&&nbrs[l.source].add(l.target);
  nbrs[l.target]&&nbrs[l.target].add(l.source);
}});

const laneG=root.append("g");
const linkG=root.append("g");
const nodeG=root.append("g");

// ── Edges ────────────────────────────────────────────────────────────────────
const linkSel=linkG.selectAll("path").data(GRAPH.links).join("path")
  .attr("class","link").attr("stroke",d=>d.color)
  .attr("marker-end",d=>`url(#arr-${{d.color.replace("#","")}})`);

const lblSel=linkG.selectAll("text").data(GRAPH.links).join("text")
  .attr("class","link-label").attr("text-anchor","middle").text(d=>d.label);

// ── Nodes ────────────────────────────────────────────────────────────────────
const nodeSel=nodeG.selectAll("g.node").data(GRAPH.nodes).join("g")
  .attr("class","node")
  .call(d3.drag()
    .on("start",(e,d)=>{{if(!e.active)sim.alphaTarget(0.15).restart();d.fx=d.x;d.fy=d.y;}})
    .on("drag", (e,d)=>{{d.fx=e.x;d.fy=e.y;}})
    .on("end",  (e,d)=>{{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null;}}));

nodeSel.each(function(n){{
  const g=d3.select(this);
  const lines=n.label.split("\\n");
  n._w=Math.max(150,lines.reduce((a,l)=>Math.max(a,l.length*7.6),0)+32);
  n._h=lines.length*19+20;
  if(n.type==="transform"){{
    g.append("ellipse").attr("class","shape")
      .attr("rx",n._w/2).attr("ry",n._h/2)
      .attr("fill","#0f1726").attr("stroke",n.color);
  }}else if(n.type==="cte"){{
    const s=34;
    g.append("polygon").attr("class","shape")
      .attr("points",`0,${{-s}} ${{s}},0 0,${{s}} ${{-s}},0`)
      .attr("fill","#120f1e").attr("stroke",n.color).attr("stroke-width",1.5);
    n._w=s*2.1;n._h=s*2;
  }}else{{
    g.append("rect").attr("class","shape")
      .attr("x",-n._w/2).attr("y",-n._h/2)
      .attr("width",n._w).attr("height",n._h)
      .attr("rx",7).attr("fill","#080f1a").attr("stroke",n.color);
  }}
  lines.forEach((line,i)=>{{
    g.append("text").attr("y",(i-(lines.length-1)/2)*19).text(line)
      .style("font-weight",i===0?"600":"400")
      .style("fill",i===0?"#fff":"#94a3b8");
  }});
  if(n.type!=="cte"){{
    g.append("circle").attr("cx",-n._w/2+8).attr("cy",-n._h/2+8)
      .attr("r",4).attr("fill",n.color);
  }}
}});

// ── Hover highlight ──────────────────────────────────────────────────────────
nodeSel
  .on("mouseover",(event,n)=>{{
    const conn=nbrs[n.id]||new Set();
    linkSel.classed("hl",d=>d.source.id===n.id||d.target.id===n.id)
           .classed("dim",d=>d.source.id!==n.id&&d.target.id!==n.id);
    lblSel.classed("hl",d=>d.source.id===n.id||d.target.id===n.id);
    nodeSel.classed("hl",d=>d.id===n.id||conn.has(d.id))
           .classed("dim",d=>d.id!==n.id&&!conn.has(d.id));
    tooltip.innerHTML=
      `<h3>${{n.type}}</h3>`+
      `<div class="tt-name">${{n.label.replace(/\\n/g," ")}}</div>`+
      (n.notebook?`<div class="tt-nb">📓 ${{n.notebook}}</div>`:"")+
      (n.cell>=0?`<div class="tt-cell">Cell #${{n.cell}}</div>`:"")+
      (n.preview?`<pre>${{n.preview}}</pre>`:"");
    tooltip.className="vis";moveTip(event);
  }})
  .on("mousemove",moveTip)
  .on("mouseout",()=>{{
    linkSel.classed("hl dim",false);lblSel.classed("hl",false);
    nodeSel.classed("hl dim",false);tooltip.className="";
  }});

// ── Force simulation ─────────────────────────────────────────────────────────
const sim=d3.forceSimulation(GRAPH.nodes)
  .force("link",d3.forceLink(GRAPH.links).id(d=>d.id).distance(160).strength(0.7))
  .force("charge",d3.forceManyBody().strength(-550).distanceMax(480))
  .force("collide",d3.forceCollide(d=>Math.max(d._w??80,d._h??40)/2+16))
  .force("x",d3.forceX(d=>laneX(d)).strength(0.88))
  .force("y",d3.forceY(()=>canvas.clientHeight/2).strength(0.03))
  .alphaDecay(0.022)
  .on("tick",ticked)
  .on("end",drawLanes);

function ticked(){{
  linkSel.attr("d",d=>{{
    const[sx,sy,tx,ty]=[d.source.x??0,d.source.y??0,d.target.x??0,d.target.y??0];
    const dx=Math.abs(tx-sx)*0.45;
    return`M${{sx}},${{sy}} C${{sx+dx}},${{sy}} ${{tx-dx}},${{ty}} ${{tx}},${{ty}}`;
  }});
  lblSel.attr("x",d=>((d.source.x??0)+(d.target.x??0))/2)
        .attr("y",d=>((d.source.y??0)+(d.target.y??0))/2-7);
  nodeSel.attr("transform",d=>`translate(${{d.x??0}},${{d.y??0}})`);
}}

function drawLanes(){{
  const ys=GRAPH.nodes.map(n=>n.y??0);
  const minY=Math.min(...ys)-60, maxY=Math.max(...ys)+60;

  // Draw one band per schema that actually appears in this graph
  const presentSchemas=[...new Set(GRAPH.nodes.map(n=>n._schema))]
    .filter(s=>SCHEMA_LANES[s]!==undefined)
    .sort((a,b)=>SCHEMA_LANES[a]-SCHEMA_LANES[b]);

  presentSchemas.forEach(schema=>{{
    const idx   = SCHEMA_LANES[schema];
    const color = SCHEMA_COLORS[schema] ?? "#888";
    const cx    = 200 + idx * LAYER_W;
    const lw    = LAYER_W * 0.84;

    laneG.append("rect")
      .attr("x",cx-lw/2).attr("y",minY)
      .attr("width",lw).attr("height",maxY-minY)
      .attr("rx",12).attr("fill",color).attr("opacity",0.04);
    laneG.append("rect")
      .attr("x",cx-lw/2).attr("y",minY)
      .attr("width",lw).attr("height",28)
      .attr("rx",12).attr("fill",color).attr("opacity",0.14);
    laneG.append("text").attr("class","lane-label")
      .attr("x",cx).attr("y",minY+19)
      .attr("text-anchor","middle").attr("fill",color)
      .text(schema.toUpperCase());
  }});
  fitView();
}}

function fitView(){{
  const W=canvas.clientWidth,H=canvas.clientHeight;
  const bb=root.node().getBBox();
  if(!bb.width)return;
  const sc=Math.min(0.9,W/(bb.width+80),H/(bb.height+80));
  svg.transition().duration(400).call(zoom.transform,
    d3.zoomIdentity.translate((W-bb.width*sc)/2-bb.x*sc,(H-bb.height*sc)/2-bb.y*sc).scale(sc));
}}

document.getElementById("bzi").onclick=()=>svg.transition().duration(240).call(zoom.scaleBy,1.4);
document.getElementById("bzo").onclick=()=>svg.transition().duration(240).call(zoom.scaleBy,0.72);
document.getElementById("bfit").onclick=fitView;

function moveTip(event){{
  const r=canvas.getBoundingClientRect(),W=canvas.clientWidth;
  let x=event.clientX-r.left+16,y=event.clientY-r.top+16;
  if(x+440>W)x=event.clientX-r.left-450;
  tooltip.style.left=x+"px";tooltip.style.top=y+"px";
}}
</script>
</body>
</html>"""

# ══════════════════════════════════════════════════════════════════════════════
# 7. SAVE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def safe_filename(s):
    return re.sub(r'[^\w\-]', '_', s)

def save_html(path, html):
    Path(path).write_text(html, encoding="utf-8")
    print(f"     ✅  {Path(path).name}")

def make_html(G, title, subtitle, accent_color, stats=None):
    return render_html(title=title, subtitle=subtitle,
                       graph_json_str=graph_to_json(G),
                       accent_color=accent_color, stats=stats)

# ══════════════════════════════════════════════════════════════════════════════
# 8. MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run():
    print()
    hr("═")
    print("  ETL LINEAGE DIAGRAM BUILDER")
    hr("═")

    print("\n  Loading notebooks...")
    hr()
    all_nb = []
    for p in NOTEBOOK_PATHS:
        name, cells = load_notebook(p)
        if cells:
            all_nb.append((name, cells))
    if not all_nb:
        sys.exit("\n❌  No notebooks loaded. Check NOTEBOOKS_DIR.")

    print("\n  Parsing SQL...")
    hr()
    nb_lineages = {}
    nb_stats    = {}
    for name, cells in all_nb:
        frags    = extract_sql(cells, name)
        lineages = [parse_fragment(f) for f in frags]
        nb_lineages[name] = lineages
        nb_stats[name] = {
            "frags":  len(frags),
            "ok":     sum(1 for l in lineages if not l.parse_error),
            "errors": sum(1 for l in lineages if l.parse_error),
        }
        s = nb_stats[name]
        print(f"  {name}: {s['frags']} fragments, {s['ok']} parsed, {s['errors']} errors")

    all_lineages = [l for lins in nb_lineages.values() for l in lins]
    G_all  = build_graph(all_lineages, NOTEBOOK_EDGE_COLORS)
    G_per  = {
        name: build_graph(lins, {name: NOTEBOOK_EDGE_COLORS.get(name, DEFAULT_EDGE_COLOR)})
        for name, lins in nb_lineages.items()
    }

    silver_nb      = [n for n in nb_lineages if n != GOLD_NOTEBOOK]
    silver_targets = set()
    for nb in silver_nb:
        for node_id, data in G_per.get(nb, nx.DiGraph()).nodes(data=True):
            if data.get("node_type") == "target":
                silver_targets.add(node_id)

    gold_targets = get_targets_for_notebook(G_all, GOLD_NOTEBOOK)

    # ── Q1: Scope ─────────────────────────────────────────────────────────────
    scope = ask(
        "Include full scope (all notebooks)?",
        ["Yes — full scope",
         "No  — let me pick a specific notebook"]
    )

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH A: Full scope
    # ══════════════════════════════════════════════════════════════════════════
    if scope == 0:
        view = ask(
            "What do you want to generate?",
            ["Full diagram  (one file, everything combined)",
             "Breakdown by target table"]
        )

        if view == 0:
            print(); hr()
            print("  Generating full combined diagram..."); hr()
            combined_stats = {
                "frags":  sum(s["frags"]  for s in nb_stats.values()),
                "ok":     sum(s["ok"]     for s in nb_stats.values()),
                "errors": sum(s["errors"] for s in nb_stats.values()),
            }
            html = make_html(G_all,
                title="ETL Lineage — Full Scope",
                subtitle=f"{'  +  '.join(nb_lineages)} · {G_all.number_of_nodes()} nodes",
                accent_color=DEFAULT_EDGE_COLOR, stats=combined_stats)
            save_html(OUTPUT_DIR / "etl_full_scope.html", html)

        elif view == 1:
            breakdown = ask(
                "Which targets should be included in the breakdown?",
                ["Full scope  — one diagram per target (all notebooks)",
                 "Gold notebook only  — gold targets, silver steps included, silver targets hidden",
                 "Specific target  — pick one target from the list"]
            )

            if breakdown == 0:
                all_targets = sorted(n for n, d in G_all.nodes(data=True)
                                     if d.get("node_type") == "target")
                print(); hr()
                print(f"  Generating {len(all_targets)} target diagram(s)..."); hr()
                for tgt in all_targets:
                    sub = subgraph_for_target(G_all, tgt)
                    if not sub or sub.number_of_nodes() == 0: continue
                    ec = NOTEBOOK_EDGE_COLORS.get(GOLD_NOTEBOOK, DEFAULT_EDGE_COLOR) \
                         if tgt in gold_targets else DEFAULT_EDGE_COLOR
                    html = make_html(sub,
                        title=f"ETL — {tgt}",
                        subtitle=f"Full scope · {sub.number_of_nodes()} nodes",
                        accent_color=ec,
                        stats={"frags":"—","ok":sub.number_of_nodes(),"errors":0})
                    save_html(OUTPUT_DIR / f"etl_full__{safe_filename(tgt)}.html", html)

            elif breakdown == 1:
                print(); hr()
                print(f"  Generating {len(gold_targets)} gold target diagram(s)...")
                print(f"  (Silver steps included; silver target nodes hidden)"); hr()
                for tgt in sorted(gold_targets):
                    sub = subgraph_for_target_hide_silver_targets(G_all, tgt, silver_targets)
                    if not sub or sub.number_of_nodes() == 0: continue
                    ec = NOTEBOOK_EDGE_COLORS.get(GOLD_NOTEBOOK, DEFAULT_EDGE_COLOR)
                    html = make_html(sub,
                        title=f"ETL — {tgt}",
                        subtitle=f"Gold view · {sub.number_of_nodes()} nodes",
                        accent_color=ec,
                        stats={"frags":"—","ok":sub.number_of_nodes(),"errors":0})
                    save_html(OUTPUT_DIR / f"etl_gold__{safe_filename(tgt)}.html", html)

            elif breakdown == 2:
                all_targets = sorted(n for n, d in G_all.nodes(data=True)
                                     if d.get("node_type") == "target")
                print()
                print("  Available targets:")
                for i, t in enumerate(all_targets, 1):
                    print(f"    [{i:>3}] {t}")
                print()
                tgt = None
                while tgt is None:
                    raw = input("  › Enter number or table name: ").strip()
                    try:
                        if raw.isdigit():
                            idx = int(raw) - 1
                            if 0 <= idx < len(all_targets):
                                tgt = all_targets[idx]
                            else:
                                print(f"  ✗  Number out of range (1–{len(all_targets)})")
                        else:
                            matches = [t for t in all_targets if raw.lower() in t.lower()]
                            if len(matches) == 1:   tgt = matches[0]
                            elif len(matches) == 0: print(f"  ✗  No target matches '{raw}'")
                            else:
                                print(f"  ✗  Ambiguous — matched:")
                                for m in matches: print(f"       {m}")
                    except Exception as ex:
                        print(f"  ✗  Invalid input: {ex}")

                print(); hr()
                print(f"  Generating diagram for: {tgt}"); hr()
                sub = subgraph_for_target(G_all, tgt)
                if sub and sub.number_of_nodes() > 0:
                    ec = NOTEBOOK_EDGE_COLORS.get(GOLD_NOTEBOOK, DEFAULT_EDGE_COLOR) \
                         if tgt in gold_targets else DEFAULT_EDGE_COLOR
                    html = make_html(sub,
                        title=f"ETL — {tgt}",
                        subtitle=f"Root trace · {sub.number_of_nodes()} nodes",
                        accent_color=ec,
                        stats={"frags":"—","ok":sub.number_of_nodes(),"errors":0})
                    save_html(OUTPUT_DIR / f"etl_target__{safe_filename(tgt)}.html", html)
                else:
                    print(f"  ⚠️  No lineage found for {tgt}")

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH B: Pick a specific notebook
    # ══════════════════════════════════════════════════════════════════════════
    elif scope == 1:
        nb_names  = list(nb_lineages.keys())
        chosen_nb = nb_names[ask(
            "Which notebook?",
            [f"{n}  ({'✅' if (NOTEBOOKS_DIR/f'{n}.ipynb').exists() else '❌'})"
             for n in nb_names]
        )]
        G_chosen  = G_per[chosen_nb]
        ec_chosen = NOTEBOOK_EDGE_COLORS.get(chosen_nb, DEFAULT_EDGE_COLOR)

        view = ask(
            "What do you want to generate?",
            [f"Full diagram  — all targets in {chosen_nb}",
             f"Breakdown by target  — one file per target in {chosen_nb}"]
        )

        if view == 0:
            print(); hr()
            print(f"  Generating full diagram for {chosen_nb}..."); hr()
            html = make_html(G_chosen,
                title=f"ETL Lineage — {chosen_nb}",
                subtitle=f"{G_chosen.number_of_nodes()} nodes · {G_chosen.number_of_edges()} edges",
                accent_color=ec_chosen, stats=nb_stats[chosen_nb])
            save_html(OUTPUT_DIR / f"etl_notebook__{safe_filename(chosen_nb)}.html", html)

        elif view == 1:
            targets_in_nb = sorted(n for n, d in G_chosen.nodes(data=True)
                                   if d.get("node_type") == "target")
            if not targets_in_nb:
                print(f"  ⚠️  No targets found in {chosen_nb}")
            else:
                print(); hr()
                print(f"  Generating {len(targets_in_nb)} diagram(s) for {chosen_nb}..."); hr()
                for tgt in targets_in_nb:
                    sub = subgraph_for_target(G_chosen, tgt)
                    if not sub or sub.number_of_nodes() == 0: continue
                    html = make_html(sub,
                        title=f"ETL — {tgt}",
                        subtitle=f"{chosen_nb} · {sub.number_of_nodes()} nodes",
                        accent_color=ec_chosen,
                        stats={"frags":"—","ok":sub.number_of_nodes(),"errors":0})
                    fname = f"etl_{safe_filename(chosen_nb)}__{safe_filename(tgt)}.html"
                    save_html(OUTPUT_DIR / fname, html)

    print()
    hr("═")
    print(f"  Done. Diagrams saved to: {OUTPUT_DIR}")
    hr("═")
    print()


if __name__ == "__main__":
    run()
