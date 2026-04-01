from __future__ import annotations

from time import perf_counter
from typing import Any

import pandas as pd
from neo4j import Record
from psycopg.rows import tuple_row

from src.benchmark_common import measured_items
from src.config import Settings
from src.graph_native_lab.common import CASE_B_NAME, case_b_raw_output_path, case_b_summary_paths, case_b_paths
from src.graph_native_lab.queries import case_b_cypher, case_b_sql
from src.load_neo4j import get_driver
from src.load_postgres import get_connection
from src.utils import append_jsonl, console, read_jsonl, utc_now_iso, write_json


def _build_result_record(
    *,
    db: str,
    run_number: int,
    latency_seconds: float,
    success: bool,
    target: dict[str, Any],
) -> dict[str, Any]:
    return {
        "suite": "graph_native_lab",
        "db": db,
        "case": CASE_B_NAME,
        "run_number": run_number,
        "latency_seconds": latency_seconds,
        "total_time_seconds": latency_seconds,
        "success": success,
        "timestamp": utc_now_iso(),
        "target": target,
    }


def _summarize(path) -> pd.DataFrame:
    frame = pd.DataFrame(read_jsonl(path))
    if frame.empty:
        raise ValueError("No Case B benchmark results were recorded.")
    successful = frame.loc[frame["success"] == True].copy()
    if successful.empty:
        raise ValueError("Case B benchmark ran, but no successful results were recorded.")

    rows: list[dict[str, object]] = []
    for db, group in successful.groupby("db", sort=True):
        latencies = group["latency_seconds"].astype(float)
        total = latencies.sum()
        rows.append(
            {
                "suite": "graph_native_lab",
                "db": db,
                "case": CASE_B_NAME,
                "count": int(len(latencies)),
                "mean_seconds": float(latencies.mean()),
                "median_seconds": float(latencies.median()),
                "p50_seconds": float(latencies.quantile(0.50)),
                "p95_seconds": float(latencies.quantile(0.95)),
                "min_seconds": float(latencies.min()),
                "max_seconds": float(latencies.max()),
                "ops_per_second": float(len(latencies) / total) if total > 0 else None,
            }
        )
    return pd.DataFrame(rows)


def benchmark_case_b(settings: Settings) -> pd.DataFrame:
    settings.ensure_directories()
    workloads = read_jsonl(case_b_paths(settings).workloads)
    if not workloads:
        raise ValueError("Case B workloads were not found. Run prepare-case-b first.")

    output_path = case_b_raw_output_path(settings)
    summary_csv_path, summary_json_path = case_b_summary_paths(settings)
    sql = case_b_sql()

    warmups, measured = measured_items(workloads, settings.warmup_runs, settings.repeat_runs)
    console.print(
        f"Benchmarking Case B with {len(warmups)} warmups and {len(measured)} measured runs per database."
    )

    with get_connection(settings) as pg_conn, get_driver(settings) as neo4j_driver:
        pg_conn.autocommit = True
        with pg_conn.cursor(row_factory=tuple_row) as pg_cursor, neo4j_driver.session(
            database=settings.neo4j_database
        ) as neo4j_session:
            for target in warmups:
                pg_cursor.execute(sql, target)
                pg_cursor.fetchone()

                neo4j_session.run(case_b_cypher(int(target["max_hops"])), target).single()

            for run_number, target in enumerate(measured, start=1):
                pg_start = perf_counter()
                pg_cursor.execute(sql, target)
                pg_row = pg_cursor.fetchone()
                pg_latency = perf_counter() - pg_start
                append_jsonl(
                    output_path,
                    _build_result_record(
                        db="postgres",
                        run_number=run_number,
                        latency_seconds=pg_latency,
                        success=pg_row is not None,
                        target=target,
                    ),
                )

                neo_start = perf_counter()
                neo_row: Record | None = neo4j_session.run(
                    case_b_cypher(int(target["max_hops"])),
                    target,
                ).single()
                neo_latency = perf_counter() - neo_start
                append_jsonl(
                    output_path,
                    _build_result_record(
                        db="neo4j",
                        run_number=run_number,
                        latency_seconds=neo_latency,
                        success=neo_row is not None,
                        target=target,
                    ),
                )

                if run_number == 1 or run_number == len(measured) or run_number % 10 == 0:
                    console.print(f"Completed Case B run {run_number}/{len(measured)} for both databases")

    summary = _summarize(output_path)
    summary.to_csv(summary_csv_path, index=False)
    write_json(summary_json_path, summary.to_dict(orient="records"))
    console.print(f"Saved Case B raw results to {output_path}")
    console.print(f"Saved Case B summaries to {summary_csv_path} and {summary_json_path}")
    return summary
