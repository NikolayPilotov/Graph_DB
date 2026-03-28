from __future__ import annotations

from time import perf_counter
from typing import Any

from psycopg.rows import tuple_row
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from src.benchmark_common import benchmark_output_path, build_result_record, load_mutation_targets, measured_items
from src.config import Settings
from src.load_neo4j import get_driver
from src.load_postgres import _reset_annotation_sequence, get_connection
from src.queries_cypher import MUTATION_CYPHER
from src.queries_sql import MUTATION_SQL
from src.utils import append_jsonl, console
from src.workload_generator import MUTATION_CASE_DELETE, MUTATION_CASE_INSERT, MUTATION_CASE_UPDATE


MUTATION_CASES = [MUTATION_CASE_INSERT, MUTATION_CASE_UPDATE, MUTATION_CASE_DELETE]


def _clear_postgres_annotations(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE address_annotations RESTART IDENTITY")
    _reset_annotation_sequence(conn)


def _clear_neo4j_annotations(session) -> None:
    session.run("MATCH (annotation:Annotation) DETACH DELETE annotation").consume()


def _insert_postgres_annotation(cursor, payload: dict[str, Any]) -> bool:
    cursor.execute(MUTATION_SQL[MUTATION_CASE_INSERT], payload)
    row = cursor.fetchone()
    return row is not None


def _insert_neo4j_annotation(session, payload: dict[str, Any]) -> bool:
    row = session.run(MUTATION_CYPHER[MUTATION_CASE_INSERT], payload).single()
    return row is not None


def _preseed_case(case_name: str, items: list[dict[str, Any]], pg_conn, pg_cursor, neo4j_session) -> None:
    _clear_postgres_annotations(pg_conn)
    _clear_neo4j_annotations(neo4j_session)

    if case_name == MUTATION_CASE_INSERT:
        return

    for payload in items:
        _insert_postgres_annotation(pg_cursor, payload)
        _insert_neo4j_annotation(neo4j_session, payload)
    _reset_annotation_sequence(pg_conn)


def _run_postgres_mutation(cursor, case_name: str, payload: dict[str, Any]) -> bool:
    cursor.execute(MUTATION_SQL[case_name], payload)
    row = cursor.fetchone()
    return row is not None


def _run_neo4j_mutation(session, case_name: str, payload: dict[str, Any]) -> bool:
    row = session.run(MUTATION_CYPHER[case_name], payload).single()
    return row is not None


def benchmark_mutations(settings: Settings) -> None:
    settings.ensure_directories()
    workloads = load_mutation_targets(settings)
    output_path = benchmark_output_path(settings, "mutation_benchmarks")
    insert_seed_rows = workloads[MUTATION_CASE_INSERT]

    total_steps = len(MUTATION_CASES) * settings.repeat_runs * 2

    with Progress(
        TextColumn("[bold magenta]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Benchmarking mutations", total=total_steps)

        with get_connection(settings) as pg_conn, get_driver(settings) as neo4j_driver:
            pg_conn.autocommit = True
            with pg_conn.cursor(row_factory=tuple_row) as pg_cursor, neo4j_driver.session(
                database=settings.neo4j_database
            ) as neo4j_session:
                for case_name in MUTATION_CASES:
                    workload_items = workloads[case_name]
                    warmups, measured = measured_items(workload_items, settings.warmup_runs, settings.repeat_runs)
                    if case_name == MUTATION_CASE_INSERT:
                        preseed_items: list[dict[str, Any]] = []
                    else:
                        needed_ids = {item["annotation_id"] for item in (warmups + measured)}
                        preseed_items = [row for row in insert_seed_rows if row["annotation_id"] in needed_ids]
                    _preseed_case(case_name, preseed_items, pg_conn, pg_cursor, neo4j_session)

                    for payload in warmups:
                        if case_name == MUTATION_CASE_INSERT:
                            _run_postgres_mutation(pg_cursor, case_name, payload)
                            _run_neo4j_mutation(neo4j_session, case_name, payload)
                        elif case_name == MUTATION_CASE_UPDATE:
                            _run_postgres_mutation(pg_cursor, case_name, payload)
                            _run_neo4j_mutation(neo4j_session, case_name, payload)
                        else:
                            _run_postgres_mutation(pg_cursor, case_name, payload)
                            _run_neo4j_mutation(neo4j_session, case_name, payload)

                    for run_number, payload in enumerate(measured, start=1):
                        for db_name in ("postgres", "neo4j"):
                            start = perf_counter()
                            success = False
                            try:
                                if db_name == "postgres":
                                    success = _run_postgres_mutation(pg_cursor, case_name, payload)
                                else:
                                    success = _run_neo4j_mutation(neo4j_session, case_name, payload)
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
                                    target=payload,
                                ),
                            )
                            progress.advance(task_id)

    console.print(f"Mutation benchmark results saved to {output_path}")
