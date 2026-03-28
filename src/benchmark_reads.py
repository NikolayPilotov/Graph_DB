from __future__ import annotations

from time import perf_counter
from typing import Any

from neo4j import Result
from psycopg.rows import tuple_row
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from src.benchmark_common import benchmark_output_path, build_result_record, load_read_targets, measured_items
from src.config import Settings
from src.load_neo4j import get_driver
from src.load_postgres import get_connection
from src.queries_cypher import READ_CYPHER
from src.queries_sql import READ_SQL
from src.utils import append_jsonl, console


READ_CASES = list(READ_SQL.keys())


def _sql_params(case_name: str, target: dict[str, Any], settings: Settings) -> dict[str, Any]:
    params = dict(target)
    if case_name != "transaction_by_hash":
        params["limit"] = settings.result_limit
    return params


def _run_postgres_query(cursor, case_name: str, target: dict[str, Any], settings: Settings) -> bool:
    cursor.execute(READ_SQL[case_name], _sql_params(case_name, target, settings))
    rows = cursor.fetchall()
    return len(rows) > 0


def _run_neo4j_query(session, case_name: str, target: dict[str, Any], settings: Settings) -> bool:
    params = _sql_params(case_name, target, settings)
    result: Result = session.run(READ_CYPHER[case_name], params)
    rows = list(result)
    return len(rows) > 0


def benchmark_reads(settings: Settings) -> None:
    settings.ensure_directories()
    workloads = load_read_targets(settings)
    output_path = benchmark_output_path(settings, "read_benchmarks")

    total_steps = len(READ_CASES) * settings.repeat_runs * 2

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Benchmarking reads", total=total_steps)

        with get_connection(settings) as pg_conn, get_driver(settings) as neo4j_driver:
            with pg_conn.cursor(row_factory=tuple_row) as pg_cursor, neo4j_driver.session(
                database=settings.neo4j_database
            ) as neo4j_session:
                for case_name in READ_CASES:
                    warmups, measured = measured_items(workloads.get(case_name, []), settings.warmup_runs, settings.repeat_runs)

                    for target in warmups:
                        _run_postgres_query(pg_cursor, case_name, target, settings)
                        _run_neo4j_query(neo4j_session, case_name, target, settings)

                    for run_number, target in enumerate(measured, start=1):
                        for db_name in ("postgres", "neo4j"):
                            start = perf_counter()
                            success = False
                            try:
                                if db_name == "postgres":
                                    success = _run_postgres_query(pg_cursor, case_name, target, settings)
                                else:
                                    success = _run_neo4j_query(neo4j_session, case_name, target, settings)
                            finally:
                                elapsed = perf_counter() - start

                            append_jsonl(
                                output_path,
                                build_result_record(
                                    db=db_name,
                                    case=case_name,
                                    run_number=run_number,
                                    latency_seconds=elapsed,
                                    success=success,
                                    target=target,
                                ),
                            )
                            progress.advance(task_id)

    console.print(f"Read benchmark results saved to {output_path}")
