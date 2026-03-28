from __future__ import annotations

import json
from pathlib import Path

import matplotlib
import pandas as pd
import plotly.express as px

from src.benchmark_common import CASE_DISPLAY_NAMES, load_raw_results
from src.config import Settings
from src.utils import console


matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _case_order(cases: list[str]) -> list[str]:
    known = [case for case in CASE_DISPLAY_NAMES if case in cases]
    unknown = sorted(case for case in cases if case not in CASE_DISPLAY_NAMES)
    return known + unknown


def _display_case(series: pd.Series) -> pd.Series:
    return series.map(lambda value: CASE_DISPLAY_NAMES.get(value, value))


def summarize_results(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("No raw benchmark results were found.")

    successful = frame.loc[frame["success"] == True].copy()
    if successful.empty:
        raise ValueError("No successful benchmark runs were found in the raw results.")

    grouped = successful.groupby(["db", "case"], dropna=False)
    summary = grouped["latency_seconds"].agg(
        mean_seconds="mean",
        median_seconds="median",
        min_seconds="min",
        max_seconds="max",
    ).reset_index()
    summary["p50_seconds"] = grouped["latency_seconds"].quantile(0.50).values
    summary["p95_seconds"] = grouped["latency_seconds"].quantile(0.95).values
    summary["ops_per_second"] = grouped["latency_seconds"].apply(lambda values: len(values) / values.sum()).values
    summary["run_count"] = grouped["latency_seconds"].count().values
    summary["case_display"] = _display_case(summary["case"])
    return summary


def _save_summary(summary: pd.DataFrame, settings: Settings) -> tuple[Path, Path]:
    csv_path = settings.summary_results_dir / "benchmark_summary.csv"
    json_path = settings.summary_results_dir / "benchmark_summary.json"
    summary.to_csv(csv_path, index=False)
    json_path.write_text(summary.to_json(orient="records", indent=2), encoding="utf-8")
    return csv_path, json_path


def _plot_grouped_bars(summary: pd.DataFrame, metric: str, title: str, filename: str, settings: Settings) -> None:
    ordered_cases = _case_order(summary["case"].drop_duplicates().tolist())
    metric_table = summary.pivot(index="case", columns="db", values=metric).reindex(ordered_cases)
    metric_table.index = [CASE_DISPLAY_NAMES.get(case, case) for case in metric_table.index]

    fig, ax = plt.subplots(figsize=(12, 6))
    metric_table.plot(kind="bar", ax=ax)
    ax.set_title(title)
    ax.set_ylabel(metric)
    ax.set_xlabel("Benchmark case")
    ax.tick_params(axis="x", rotation=25)
    ax.legend(title="Database")
    fig.tight_layout()

    png_path = settings.figures_dir / f"{filename}.png"
    svg_path = settings.figures_dir / f"{filename}.svg"
    fig.savefig(png_path, dpi=200)
    fig.savefig(svg_path)
    plt.close(fig)

    html_path = settings.figures_dir / f"{filename}.html"
    plotly_fig = px.bar(
        summary,
        x="case_display",
        y=metric,
        color="db",
        barmode="group",
        title=title,
    )
    plotly_fig.write_html(html_path)


def _plot_boxplot(frame: pd.DataFrame, settings: Settings) -> None:
    successful = frame.loc[frame["success"] == True].copy()
    successful["case_display"] = _display_case(successful["case"])
    successful["label"] = successful["db"] + " | " + _display_case(successful["case"])

    ordered_labels = []
    for case_name in _case_order(successful["case"].drop_duplicates().tolist()):
        display = CASE_DISPLAY_NAMES.get(case_name, case_name)
        for db_name in sorted(successful["db"].drop_duplicates()):
            label = f"{db_name} | {display}"
            if label in successful["label"].values:
                ordered_labels.append(label)

    data = [
        successful.loc[successful["label"] == label, "latency_seconds"].tolist()
        for label in ordered_labels
    ]

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.boxplot(data, labels=ordered_labels, showfliers=True)
    ax.set_title("Latency distribution by database and case")
    ax.set_ylabel("latency_seconds")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()

    png_path = settings.figures_dir / "latency_distribution_boxplot.png"
    svg_path = settings.figures_dir / "latency_distribution_boxplot.svg"
    fig.savefig(png_path, dpi=200)
    fig.savefig(svg_path)
    plt.close(fig)

    html_path = settings.figures_dir / "latency_distribution_boxplot.html"
    plotly_fig = px.box(
        successful,
        x="case_display",
        y="latency_seconds",
        color="db",
        title="Latency distribution by database and case",
    )
    plotly_fig.write_html(html_path)


def plot_results(settings: Settings) -> pd.DataFrame:
    settings.ensure_directories()
    frame = load_raw_results(settings)
    summary = summarize_results(frame)
    csv_path, json_path = _save_summary(summary, settings)

    _plot_grouped_bars(summary, "mean_seconds", "Mean latency comparison", "mean_seconds_comparison", settings)
    _plot_grouped_bars(summary, "p95_seconds", "P95 latency comparison", "p95_seconds_comparison", settings)
    _plot_grouped_bars(summary, "ops_per_second", "Operations per second comparison", "ops_per_second_comparison", settings)
    _plot_boxplot(frame, settings)

    console.print(f"Summary CSV saved to {csv_path}")
    console.print(f"Summary JSON saved to {json_path}")
    console.print(f"Figures saved to {settings.figures_dir}")
    return summary
