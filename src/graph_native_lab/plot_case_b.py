from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

from src.config import Settings
from src.graph_native_lab.common import CASE_B_NAME
from src.utils import console, read_jsonl, write_json


matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _resolve_raw_path(settings: Settings, raw_file: Path | None) -> Path:
    if raw_file is not None:
        resolved = Path(raw_file)
        if not resolved.exists():
            raise FileNotFoundError(f"Case B raw results file does not exist: {resolved}")
        return resolved

    candidates = sorted(settings.raw_results_dir.glob(f"{CASE_B_NAME}_*.jsonl"))
    if not candidates:
        raise FileNotFoundError("No Case B raw result files were found. Run benchmark-case-b first.")
    return candidates[-1]


def _load_frame(raw_path: Path) -> pd.DataFrame:
    frame = pd.DataFrame(read_jsonl(raw_path))
    if frame.empty:
        raise ValueError(f"Case B raw results file is empty: {raw_path}")
    successful = frame.loc[frame["success"] == True].copy()
    if successful.empty:
        raise ValueError(f"Case B raw results contain no successful runs: {raw_path}")
    return successful


def summarize_case_b_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for db, group in frame.groupby("db", sort=True):
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


def _summary_paths(settings: Settings, raw_path: Path) -> tuple[Path, Path]:
    stem = raw_path.stem
    return (
        settings.summary_results_dir / f"{stem}_plot_summary.csv",
        settings.summary_results_dir / f"{stem}_plot_summary.json",
    )


def _plot_summary_figure(summary: pd.DataFrame, settings: Settings, raw_path: Path) -> tuple[Path, Path]:
    ordered = summary.sort_values("db").reset_index(drop=True)
    labels = ordered["db"].tolist()
    mean_values = ordered["mean_seconds"].tolist()
    p95_values = ordered["p95_seconds"].tolist()
    ops_values = ordered["ops_per_second"].tolist()

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    metrics = [
        ("Mean seconds", mean_values),
        ("P95 seconds", p95_values),
        ("Ops per second", ops_values),
    ]
    for ax, (title, values) in zip(axes, metrics, strict=True):
        ax.bar(labels, values)
        ax.set_title(title)
        ax.tick_params(axis="x", rotation=20)

    fig.suptitle("Case B shortest path benchmark summary")
    fig.tight_layout()

    stem = raw_path.stem
    png_path = settings.figures_dir / f"{stem}_summary.png"
    svg_path = settings.figures_dir / f"{stem}_summary.svg"
    fig.savefig(png_path, dpi=200)
    fig.savefig(svg_path)
    plt.close(fig)
    return png_path, svg_path


def _plot_boxplot(frame: pd.DataFrame, settings: Settings, raw_path: Path) -> tuple[Path, Path]:
    ordered = frame.sort_values("db")
    labels = []
    data = []
    for db, group in ordered.groupby("db", sort=True):
        labels.append(db)
        data.append(group["latency_seconds"].astype(float).tolist())

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.boxplot(data, labels=labels, showfliers=True)
    ax.set_title("Case B latency distribution")
    ax.set_ylabel("latency_seconds")
    fig.tight_layout()

    stem = raw_path.stem
    png_path = settings.figures_dir / f"{stem}_boxplot.png"
    svg_path = settings.figures_dir / f"{stem}_boxplot.svg"
    fig.savefig(png_path, dpi=200)
    fig.savefig(svg_path)
    plt.close(fig)
    return png_path, svg_path


def plot_case_b(settings: Settings, *, raw_file: Path | None = None) -> pd.DataFrame:
    settings.ensure_directories()
    raw_path = _resolve_raw_path(settings, raw_file)
    frame = _load_frame(raw_path)
    summary = summarize_case_b_frame(frame)

    csv_path, json_path = _summary_paths(settings, raw_path)
    summary.to_csv(csv_path, index=False)
    write_json(json_path, summary.to_dict(orient="records"))

    summary_png, summary_svg = _plot_summary_figure(summary, settings, raw_path)
    box_png, box_svg = _plot_boxplot(frame, settings, raw_path)

    console.print(f"Case B plot summary CSV saved to {csv_path}")
    console.print(f"Case B plot summary JSON saved to {json_path}")
    console.print(f"Case B summary figure saved to {summary_png} and {summary_svg}")
    console.print(f"Case B boxplot saved to {box_png} and {box_svg}")
    return summary

