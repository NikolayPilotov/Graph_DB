from __future__ import annotations

from pathlib import Path

import typer

from src.benchmark_mutations import benchmark_mutations
from src.benchmark_reads import benchmark_reads
from src.config import Settings, get_settings
from src.load_neo4j import load_neo4j
from src.load_postgres import load_postgres
from src.mermaid_visualizer import visualize_mermaid
from src.plot_results import plot_results
from src.prepare_data import prepare_data
from src.utils import console


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
    mode: str | None = None,
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
    if mode is not None:
        overrides["BENCHMARK_MODE"] = mode
    return get_settings(**overrides)


@app.command("prepare-data")
def prepare_data_command(
    source_path: Path | None = typer.Option(None, "--source-path"),
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    row_limit: int | None = typer.Option(None, "--row-limit"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
    seed: int | None = typer.Option(None, "--seed"),
) -> None:
    settings = _build_settings(
        source_path=source_path,
        data_dir=data_dir,
        results_dir=results_dir,
        row_limit=row_limit,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
        seed=seed,
    )
    prepare_data(settings, source_path=source_path, row_limit=row_limit or settings.row_limit)


@app.command("load-postgres")
def load_postgres_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
) -> None:
    settings = _build_settings(data_dir=data_dir, results_dir=results_dir)
    load_postgres(settings)


@app.command("load-neo4j")
def load_neo4j_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
) -> None:
    settings = _build_settings(data_dir=data_dir, results_dir=results_dir)
    load_neo4j(settings)


@app.command("benchmark-reads")
def benchmark_reads_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
    mode: str = typer.Option("warm", "--mode"),
) -> None:
    settings = _build_settings(
        data_dir=data_dir,
        results_dir=results_dir,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
        mode=mode,
    )
    benchmark_reads(settings)


@app.command("benchmark-mutations")
def benchmark_mutations_command(
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
    mode: str = typer.Option("warm", "--mode"),
) -> None:
    settings = _build_settings(
        data_dir=data_dir,
        results_dir=results_dir,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
        mode=mode,
    )
    benchmark_mutations(settings)


@app.command("plot-results")
def plot_results_command(
    results_dir: Path | None = typer.Option(None, "--results-dir"),
) -> None:
    settings = _build_settings(results_dir=results_dir)
    plot_results(settings)


@app.command("run-all")
def run_all_command(
    source_path: Path | None = typer.Option(None, "--source-path"),
    data_dir: Path | None = typer.Option(None, "--data-dir"),
    results_dir: Path | None = typer.Option(None, "--results-dir"),
    row_limit: int | None = typer.Option(None, "--row-limit"),
    warmup_runs: int | None = typer.Option(None, "--warmup-runs"),
    repeat_runs: int | None = typer.Option(None, "--repeat-runs"),
    seed: int | None = typer.Option(None, "--seed"),
    mode: str = typer.Option("warm", "--mode"),
) -> None:
    settings = _build_settings(
        source_path=source_path,
        data_dir=data_dir,
        results_dir=results_dir,
        row_limit=row_limit,
        warmup_runs=warmup_runs,
        repeat_runs=repeat_runs,
        seed=seed,
        mode=mode,
    )
    prepare_data(settings, source_path=source_path, row_limit=row_limit or settings.row_limit)
    load_postgres(settings)
    load_neo4j(settings)
    benchmark_reads(settings)
    benchmark_mutations(settings)
    summary = plot_results(settings)
    console.print(f"Completed run-all with {len(summary)} summary rows.")


@app.command("visualize-mermaid")
def visualize_mermaid_command(
    mermaid_file: Path | None = typer.Option(None, "--mermaid-file"),
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8765, "--port"),
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser"),
) -> None:
    visualize_mermaid(
        mermaid_file=mermaid_file,
        host=host,
        port=port,
        open_browser=open_browser,
    )


def run() -> None:
    app()


if __name__ == "__main__":
    run()
