# ETL Lineage Diagram Builder

Reads Fabric/Synapse `.ipynb` notebooks, extracts SQL queries, parses them with **sqlglot**, and generates interactive HTML lineage diagrams with a D3.js force layout.

---

## Setup

```bash
pip install sqlglot networkx
```

Place your exported `.ipynb` files in the `notebooks/` folder, then run:

```bash
python generate_etl_diagram.py
```

---

## Repo structure

```
etl_schemat_builder/
├── generate_etl_diagram.py   # main script
├── notebooks/
│   ├── gold_transformer.ipynb
│   └── silver_cleaner.ipynb
└── diagrams/                 # all output HTML files (auto-created)
```

---

## How it works

### 1. SQL extraction

Every `code` cell in each notebook is scanned for SQL via four patterns:

| Pattern | Example |
|---|---|
| `%%sql` magic cell | `%%sql\nSELECT * FROM silver.orders` |
| `spark.sql(...)` | `spark.sql("INSERT INTO gold.orders SELECT ...")` |
| `pd.read_sql(...)` | `pd.read_sql("SELECT id FROM dim_customer", conn)` |
| Raw inline SQL string | Multi-line SQL assigned to a variable |

### 2. Parsing with sqlglot

Each fragment is parsed into a `TableLineage` object containing:

- **source tables** — all `FROM` / `JOIN` references (excluding CTEs)
- **target table** — the `CREATE TABLE`, `INSERT INTO`, or `CREATE OR REPLACE TABLE` destination
- **CTEs** — named `WITH` clauses
- **operations** — `SELECT`, `CREATE`, `INSERT`, `MERGE`, `JOIN×N`

### 3. Graph building

Nodes and edges are assembled into a `networkx.DiGraph`:

| Node type | Shape | Color | Description |
|---|---|---|---|
| Source | Rectangle | 🟢 Teal | Raw input table |
| Transform | Ellipse | 🔵 Blue | SQL operation (one per cell) |
| Target | Rectangle | 🔴 Orange | Output table written to |
| CTE | Diamond | 🟣 Purple | Named intermediate expression |

Edge colors are **per notebook** — blue for `gold_transformer`, amber for `silver_cleaner` — so you can trace which notebook produced which edges in cross-notebook diagrams.

### 4. HTML diagram

Each output file is a self-contained HTML page with:

- **Schema-aware swim lanes** — nodes are pinned to horizontal bands by their table schema prefix. `dbo.*` tables sit on the far left, `silver.*` in the middle, `gold.*` on the far right. Only schemas that actually appear in the diagram are rendered as bands.
- **Force-directed layout** — within each lane, nodes float freely vertically and the D3 simulation separates overlapping nodes. The lane X-force keeps schemas grouped while the charge force spreads nodes apart.
- **Hover to highlight** — hovering a node fades all unconnected edges and nodes; only direct connections remain visible; edge labels (`reads` / `writes` / `via CTE`) appear on hover
- **Drag to reposition** — any node can be dragged to untangle the layout manually
- **Zoom / pan** — scroll to zoom, drag canvas to pan, double-click to fit view

---

## Menu

The script asks two to three questions depending on your choices.

```
Q1  Include full scope (all notebooks)?
    [1] Yes — full scope
    [2] No  — let me pick a specific notebook
```

---

### Full scope

```
Q2  What do you want to generate?
    [1] Full diagram
    [2] Breakdown by target table
```

**[1] Full diagram** — combines all notebooks into one graph:
```
diagrams/etl_full_scope.html
```

**[2] Breakdown by target:**

```
Q3  Which targets?
    [1] Full scope          — one file per target, all notebooks
    [2] Gold notebook only  — gold targets, silver steps included, silver targets hidden
    [3] Specific target     — pick from list
```

| Option | What it generates | Output filename |
|---|---|---|
| Full scope | One diagram per every target table, tracing back through all notebooks | `etl_full__<target>.html` |
| Gold notebook only | One diagram per gold-layer target. Silver transform steps and raw source tables are included so the full data path is visible, but silver target nodes are stripped to keep the view clean | `etl_gold__<target>.html` |
| Specific target | Prompts for a target by number or name (fuzzy match, with try/catch on bad input); traces all ancestors back through every notebook to the raw source | `etl_target__<target>.html` |

---

### Specific notebook

```
Q2  Which notebook?
    [1] gold_transformer  ✅
    [2] silver_cleaner    ✅

Q3  What do you want to generate?
    [1] Full diagram
    [2] Breakdown by target
```

| Option | What it generates | Output filename |
|---|---|---|
| Full diagram | One diagram for the chosen notebook only | `etl_notebook__<name>.html` |
| Breakdown by target | One diagram per target table in the chosen notebook | `etl_<name>__<target>.html` |

---

## Configuration

All config is at the top of `generate_etl_diagram.py`:

| Variable | Description |
|---|---|
| `NOTEBOOK_PATHS` | List of `.ipynb` files to process |
| `GOLD_NOTEBOOK` | Name of the notebook containing final (gold-layer) targets |
| `SQL_DIALECT` | sqlglot dialect: `spark`, `tsql`, `hive`, `trino` |
| `NOTEBOOK_EDGE_COLORS` | Edge color per notebook name |
| `SCHEMA_LANES` | Maps schema prefix → horizontal lane index (left = 0) |
| `SCHEMA_COLORS` | Maps schema prefix → swim-lane band color |
| `OUTPUT_DIR` | Where HTML files are written (`diagrams/` by default) |

### Adding a new notebook

```python
NOTEBOOK_PATHS = [
    NOTEBOOKS_DIR / "gold_transformer.ipynb",
    NOTEBOOKS_DIR / "silver_cleaner.ipynb",
    NOTEBOOKS_DIR / "bronze_ingestor.ipynb",   # add here
]

NOTEBOOK_EDGE_COLORS = {
    "gold_transformer": "#4F8EF7",
    "silver_cleaner":   "#F7A24F",
    "bronze_ingestor":  "#4FF7B0",             # and here
}
```

### Adding a new schema lane

```python
SCHEMA_LANES = {
    "dbo":    0,
    "bronze": 1,   # new
    "silver": 2,
    "gold":   3,
}
SCHEMA_COLORS = {
    "dbo":    "#00C9A7",
    "bronze": "#F7965A",   # new
    "silver": "#F7A24F",
    "gold":   "#4F8EF7",
}
```

Lane index controls left-to-right order. Only schemas that appear in a given diagram are rendered as bands — unused lanes are skipped automatically.

---

## Diagram controls

| Action | Result |
|---|---|
| Hover a node | Highlights direct connections; fades everything else |
| Drag a node | Repositions it; force simulation adjusts neighbours |
| Scroll | Zoom in / out |
| Drag canvas | Pan |
| Double-click canvas | Fit entire graph to view |
| `+` / `−` buttons | Step zoom |
| `⌖` button | Fit view |
