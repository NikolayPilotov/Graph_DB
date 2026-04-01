# Graph Native Lab

This folder contains a separate, isolated lab case for a graph-native benchmark:

- Case B: shortest path from a seed address to any address with a target label
- edge constraints: transfer time must stay inside a time window
- result: path plus hop count

The baseline benchmark in `src/` is left unchanged. This module creates its own:

- PostgreSQL tables: `case_b_transfer_edges`, `case_b_address_labels`
- Neo4j relationships and labels: `:CASE_B_TRANSFER`, `:CaseBLabel`, `:HAS_CASE_B_LABEL`

## Prerequisite

Run the baseline data preparation once so the normalized transaction file exists:

```powershell
py -m src.main prepare-data --source-path "D:\path\to\your\dataset.parquet"
```

## Commands

```powershell
py -m src.graph_native_lab.main prepare-case-b
py -m src.graph_native_lab.main load-postgres
py -m src.graph_native_lab.main load-neo4j
py -m src.graph_native_lab.main benchmark-case-b
py -m src.graph_native_lab.main plot-case-b
```

Or all at once:

```powershell
py -m src.graph_native_lab.main run-all
```

## Generated artifacts

- `data/postgres/case_b_transfer_edges.csv`
- `data/postgres/case_b_address_labels.csv`
- `data/neo4j/case_b_addresses.csv`
- `data/neo4j/case_b_transfer_edges.csv`
- `data/neo4j/case_b_address_labels.csv`
- `data/workloads/case_b_shortest_path_targets.jsonl`
- `results/raw/case_b_shortest_path_to_label_*.jsonl`
- `results/summary/case_b_shortest_path_to_label_*.csv`
- `results/summary/case_b_shortest_path_to_label_*.json`
- `results/summary/case_b_shortest_path_to_label_*_plot_summary.csv`
- `results/summary/case_b_shortest_path_to_label_*_plot_summary.json`
- `results/figures/case_b_shortest_path_to_label_*_summary.png`
- `results/figures/case_b_shortest_path_to_label_*_summary.svg`
- `results/figures/case_b_shortest_path_to_label_*_boxplot.png`
- `results/figures/case_b_shortest_path_to_label_*_boxplot.svg`
