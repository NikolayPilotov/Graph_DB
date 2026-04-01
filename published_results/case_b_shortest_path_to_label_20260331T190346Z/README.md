# Published Case B Result Snapshot

This folder publishes one local `graph_native_lab` run so the repository contains visible Case B evidence on GitHub.

## What this run is

- Case: `case_b_shortest_path_to_label`
- Source dataset scale: normalized Avalanche sample derived from the local `last_50k_tx.parquet` workflow
- Transfer edges in Case B projection: `49,989`
- Addresses in Case B projection: `12,267`
- Labeled target addresses: `122`
- Generated workloads: `45`

## Run settings

- Compose profile: `compose.light.yaml`
- `max_hops = 4`
- `window_hours = 12`
- `warmup_runs = 1`
- `repeat_runs = 5`

## Main result

From `case_b_shortest_path_to_label_20260331T190346Z.csv`:

- Neo4j mean latency: `0.008342500030994415` seconds
- PostgreSQL mean latency: `4.159289080044255` seconds
- Neo4j ops/sec: `119.8681445951162`
- PostgreSQL ops/sec: `0.2404257027475859`

This is a small, low-footprint proof-of-concept run, not the final large-scale benchmark campaign.

## Files

- Raw benchmark log: `case_b_shortest_path_to_label_20260331T190346Z.jsonl`
- Summary tables: `case_b_shortest_path_to_label_20260331T190346Z.csv`, `case_b_shortest_path_to_label_20260331T190346Z.json`
- Plot summaries: `case_b_shortest_path_to_label_20260331T190346Z_plot_summary.csv`, `case_b_shortest_path_to_label_20260331T190346Z_plot_summary.json`
- Figures: `case_b_shortest_path_to_label_20260331T190346Z_summary.png`, `case_b_shortest_path_to_label_20260331T190346Z_boxplot.png`
