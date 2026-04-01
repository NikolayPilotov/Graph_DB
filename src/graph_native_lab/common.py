from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.config import Settings


CASE_B_NAME = "case_b_shortest_path_to_label"
TARGET_LABEL_EXCHANGE = "exchange"
DEFAULT_MAX_HOPS = 6
DEFAULT_WINDOW_HOURS = 24


@dataclass(slots=True)
class CaseBPaths:
    postgres_transfer_edges: Path
    postgres_address_labels: Path
    neo4j_addresses: Path
    neo4j_transfer_edges: Path
    neo4j_address_labels: Path
    workloads: Path
    metadata: Path


def case_b_paths(settings: Settings) -> CaseBPaths:
    return CaseBPaths(
        postgres_transfer_edges=settings.postgres_data_dir / "case_b_transfer_edges.csv",
        postgres_address_labels=settings.postgres_data_dir / "case_b_address_labels.csv",
        neo4j_addresses=settings.neo4j_data_dir / "case_b_addresses.csv",
        neo4j_transfer_edges=settings.neo4j_data_dir / "case_b_transfer_edges.csv",
        neo4j_address_labels=settings.neo4j_data_dir / "case_b_address_labels.csv",
        workloads=settings.workloads_dir / "case_b_shortest_path_targets.jsonl",
        metadata=settings.raw_data_dir / "case_b_metadata.json",
    )


def case_b_raw_output_path(settings: Settings) -> Path:
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return settings.raw_results_dir / f"{CASE_B_NAME}_{stamp}.jsonl"


def case_b_summary_paths(settings: Settings) -> tuple[Path, Path]:
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return (
        settings.summary_results_dir / f"{CASE_B_NAME}_{stamp}.csv",
        settings.summary_results_dir / f"{CASE_B_NAME}_{stamp}.json",
    )

