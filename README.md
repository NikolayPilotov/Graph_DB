# Avalanche PostgreSQL vs Neo4j Benchmark

This project builds a local benchmark pipeline for comparing PostgreSQL and Neo4j on Avalanche transaction data. It prepares a shared logical model, bulk-loads both databases, executes read and mutation benchmarks, saves all raw latency values in seconds, and generates comparison charts.

## Prerequisites

- Python 3.11+
- Docker Desktop with `docker compose`
- A local Avalanche transactions source file in Parquet, JSON, or JSONL format

## Project layout

```text
compose.yaml
.env.example
pyproject.toml
README.md
src/
data/
results/
```

The CLI writes prepared data into `data/` and benchmark outputs into `results/`.

## Setup

1. Create and activate a virtual environment.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -U pip
pip install -e .
```

2. Copy the environment template and adjust credentials or paths if needed.

```powershell
Copy-Item .env.example .env
```

3. Start PostgreSQL and Neo4j.

```powershell
docker compose up -d
```

4. Confirm the services are up.

```powershell
docker compose ps
```

PostgreSQL is exposed on `localhost:55432`. Neo4j Browser is exposed on [http://localhost:7474](http://localhost:7474) and Bolt on `localhost:7687`.

## CLI commands

You can run commands either through `python -m src.main` or the installed `avalanche-benchmark` entrypoint.

### Prepare data

```powershell
python -m src.main prepare-data --source-path D:\path\to\avalanche_transactions.parquet
```

### Load PostgreSQL

```powershell
python -m src.main load-postgres
```

### Load Neo4j

```powershell
python -m src.main load-neo4j
```

### Benchmark reads

```powershell
python -m src.main benchmark-reads
```

### Benchmark mutations

```powershell
python -m src.main benchmark-mutations
```

### Plot results

```powershell
python -m src.main plot-results
```

### Mermaid visualizer

```powershell
python -m src.main visualize-mermaid
```

To load Mermaid code from a file instead of the built-in sample:

```powershell
python -m src.main visualize-mermaid --mermaid-file D:\path\to\diagram.mmd
```

### Run everything

```powershell
python -m src.main run-all --source-path D:\path\to\avalanche_transactions.parquet
```

## Generated files

Data preparation outputs:

- `data/raw/normalized_transactions.parquet`
- `data/raw/normalized_transactions.csv`
- `data/postgres/transactions.csv`
- `data/postgres/addresses.csv`
- `data/postgres/blocks.csv`
- `data/neo4j/transactions.csv`
- `data/neo4j/addresses.csv`
- `data/neo4j/blocks.csv`
- `data/neo4j/sent_edges.csv`
- `data/neo4j/to_edges.csv`
- `data/neo4j/in_block_edges.csv`
- `data/workloads/read_targets.json`
- `data/workloads/insert_annotations.jsonl`
- `data/workloads/update_annotations.jsonl`
- `data/workloads/delete_annotations.jsonl`

Benchmark outputs:

- `results/raw/*.jsonl`
- `results/summary/*.csv`
- `results/summary/*.json`
- `results/figures/*.png`
- `results/figures/*.svg`
- `results/figures/*.html`

Published snapshot artifacts:

- `published_results/case_b_shortest_path_to_label_20260331T190346Z/`

The repository also includes a separate graph-native Case B lab in `src/graph_native_lab` plus one published low-footprint result snapshot for that case.

## Timing model

All benchmark timings are recorded in seconds using `time.perf_counter()`.

Raw benchmark rows store float-second fields:

- `latency_seconds`
- `total_time_seconds`

Summary outputs store:

- `mean_seconds`
- `median_seconds`
- `p50_seconds`
- `p95_seconds`
- `min_seconds`
- `max_seconds`
- `ops_per_second`

Values are rounded only for terminal display and plots. Raw JSONL records keep the original floating-point precision.

## Cold vs warm runs

Version 1 focuses on warm runs. If you want a simple cold-run comparison:

1. Stop the databases.
2. Start them again with `docker compose up -d`.
3. Run the benchmark commands into a separate results directory.

Example:

```powershell
docker compose down
docker compose up -d
python -m src.main benchmark-reads --results-dir .\results\cold
python -m src.main benchmark-mutations --results-dir .\results\cold
```

## Notes

- `value_num` and `gas_price_num` are kept as exact decimal strings in both databases for cross-database consistency.
- The mutation benchmark uses a separate annotation layer so blockchain transaction data remains immutable.
- The workload generator writes deterministic benchmark targets based on a shared seed.
