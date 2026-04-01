from __future__ import annotations

from pathlib import Path

import typer

from src.config import Settings, get_settings
from src.graph_native_lab.benchmark_case_b import benchmark_case_b
from src.graph_native_lab.load_case_b_neo4j import load_case_b_neo4j
from src.graph_native_lab.load_case_b_postgres import load_case_b_postgres
from src.graph_native_lab.plot_case_b import plot_case_b
from src.graph_native_lab.prepare_case_b import prepare_case_b


app = typer.Typer(add_completion=False, no_args_is_help=True)


def _build_settings(
    *,
    source_path: Path | None = None,
    data_dir: Path | None = None,
    results_dir: Path | None = None,
    warmup_runs: int | None = None,
    repeat_runs: int | None = None,
    row_limit: int | None = None,
    seed: int | None = None,
) -> Settings:
    overrides: dict[str, object] = {}
    if source_path is not None:
        overrides["BENCH_SOURCE_PATH"] = source_path
    if data_dir is not None:
        overrides["DATA_DIR"] = data_dir
    if results_dir is not None:
        overrides["RESULTS_DIR"] = results_dir
    if warmup_runs is not None:
        overrides["WARMUP_RUNS"] = warmup_runs
    if repeat_runs is not None:
        overrides["REPEAT_RUNS"] = repeat_runs
    if row_limit is not None:
        overrides["ROW_LIMIT"] = row_limit
    if seed is not None:
        overrides["WORKLOAD_SEED"] = seed
    return get_settings(**overrides)


@app.command("prepare-case-b")
def prepare_case_b_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
    seed: int | None = typer.Option(None, "--seed"),
    max_hops: int = typer.Option(6, "--max-hops"),
    window_hours: int = typer.Option(24, "--window-hours"),
) -> None:
    settings = _build_settings(
        data_dir=data_dir,
        results_dir=results_dir,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
        seed=seed,
    )
    prepare_case_b(settings, max_hops=max_hops, window_hours=window_hours)


@app.command("load-postgres")
def load_postgres_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
) -> None:
    settings = _build_settings(data_dir=data_dir, results_dir=results_dir)
    load_case_b_postgres(settings)


@app.command("load-neo4j")
def load_neo4j_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
) -> None:
    settings = _build_settings(data_dir=data_dir, results_dir=results_dir)
    load_case_b_neo4j(settings)


@app.command("benchmark-case-b")
def benchmark_case_b_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
) -> None:
    settings = _build_settings(
        data_dir=data_dir,
        results_dir=results_dir,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
    )
    benchmark_case_b(settings)


@app.command("plot-case-b")
def plot_case_b_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    raw_file: Path | None = typer.Option(None, "--raw-file"),
) -> None:
    settings = _build_settings(
        data_dir=data_dir,
        results_dir=results_dir,
    )
    plot_case_b(settings, raw_file=raw_file)


@app.command("run-all")
def run_all_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
    seed: int | None = typer.Option(None, "--seed"),
    max_hops: int = typer.Option(6, "--max-hops"),
    window_hours: int = typer.Option(24, "--window-hours"),
) -> None:
    settings = _build_settings(
        data_dir=data_dir,
        results_dir=results_dir,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
        seed=seed,
    )
    prepare_case_b(settings, max_hops=max_hops, window_hours=window_hours)
    load_case_b_postgres(settings)
    load_case_b_neo4j(settings)
    benchmark_case_b(settings)
    plot_case_b(settings)


def run() -> None:
    app()


if __name__ == "__main__":
    run()
