from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import Settings
from src.utils import console, write_json, write_jsonl


READ_CASE_TRANSACTION_BY_HASH = "transaction_by_hash"
READ_CASE_SENT_BY_ADDRESS = "transactions_sent_by_address"
READ_CASE_RECEIVED_BY_ADDRESS = "transactions_received_by_address"
READ_CASE_IN_BLOCK = "transactions_in_one_block"
READ_CASE_TWO_HOP = "two_hop_traversal_from_address"

MUTATION_CASE_INSERT = "insert_annotation"
MUTATION_CASE_UPDATE = "update_annotation"
MUTATION_CASE_DELETE = "delete_annotation"


@dataclass(slots=True)
class WorkloadPaths:
    read_targets: Path
    insert_annotations: Path
    update_annotations: Path
    delete_annotations: Path


def _target_count(settings: Settings) -> int:
    return max(settings.warmup_runs + settings.repeat_runs + 10, 32)


def _sample_values(values: list[str], count: int, seed: int) -> list[str]:
    if not values:
        return []
    unique_values = list(dict.fromkeys(values))
    if len(unique_values) <= count:
        return unique_values
    rng = random.Random(seed)
    return rng.sample(unique_values, count)


def _timestamp_at(index: int) -> str:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return base.replace(microsecond=index % 1_000_000).isoformat()


def generate_workloads(transactions_df: pd.DataFrame, settings: Settings) -> WorkloadPaths:
    settings.ensure_directories()
    target_count = _target_count(settings)

    hashes = transactions_df["transaction_hash"].dropna().astype(str).tolist()
    sent_addresses = (
        transactions_df.loc[transactions_df["from_address"].notna(), "from_address"].astype(str).tolist()
    )
    received_addresses = (
        transactions_df.loc[transactions_df["to_address"].notna(), "to_address"].astype(str).tolist()
    )
    block_hashes = transactions_df.loc[transactions_df["block_hash"].notna(), "block_hash"].astype(str).tolist()

    sent_counts = transactions_df.groupby("from_address", dropna=True)["transaction_hash"].count()
    recv_counts = transactions_df.groupby("to_address", dropna=True)["transaction_hash"].count()
    two_hop_candidates = sent_counts[sent_counts > 0].index.astype(str).tolist()
    sent_candidates = sent_counts[sent_counts > 0].index.astype(str).tolist()
    recv_candidates = recv_counts[recv_counts > 0].index.astype(str).tolist()

    read_targets = {
        READ_CASE_TRANSACTION_BY_HASH: [
            {"transaction_hash": value}
            for value in _sample_values(hashes, target_count, settings.workload_seed + 1)
        ],
        READ_CASE_SENT_BY_ADDRESS: [
            {"address": value}
            for value in _sample_values(sent_candidates, target_count, settings.workload_seed + 2)
        ],
        READ_CASE_RECEIVED_BY_ADDRESS: [
            {"address": value}
            for value in _sample_values(recv_candidates, target_count, settings.workload_seed + 3)
        ],
        READ_CASE_IN_BLOCK: [
            {"block_hash": value}
            for value in _sample_values(block_hashes, target_count, settings.workload_seed + 4)
        ],
        READ_CASE_TWO_HOP: [
            {"address": value}
            for value in _sample_values(two_hop_candidates, target_count, settings.workload_seed + 5)
        ],
    }

    all_addresses = _sample_values(sent_addresses + received_addresses, target_count, settings.workload_seed + 6)
    mutations_needed = target_count
    if not all_addresses:
        raise ValueError("No address values are available to build mutation workloads.")
    while len(all_addresses) < mutations_needed:
        all_addresses.extend(all_addresses)
    all_addresses = all_addresses[:mutations_needed]

    insert_rows: list[dict[str, object]] = []
    update_rows: list[dict[str, object]] = []
    delete_rows: list[dict[str, object]] = []

    annotation_start = 1_000_000
    for index, address in enumerate(all_addresses):
        annotation_id = annotation_start + index
        created_at = _timestamp_at(index)
        updated_at = _timestamp_at(index + 10_000)
        insert_rows.append(
            {
                "annotation_id": annotation_id,
                "address": address,
                "label": f"label_{index % 5}",
                "risk_score": index % 100,
                "note": f"Generated benchmark annotation {index}",
                "source": "benchmark_seed",
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        update_rows.append(
            {
                "annotation_id": annotation_id,
                "address": address,
                "label": f"updated_label_{index % 5}",
                "risk_score": (index * 7) % 100,
                "note": f"Updated benchmark annotation {index}",
                "source": "benchmark_update",
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
        delete_rows.append(
            {
                "annotation_id": annotation_id,
                "address": address,
                "deleted_at": updated_at,
            }
        )

    paths = WorkloadPaths(
        read_targets=settings.workloads_dir / "read_targets.json",
        insert_annotations=settings.workloads_dir / "insert_annotations.jsonl",
        update_annotations=settings.workloads_dir / "update_annotations.jsonl",
        delete_annotations=settings.workloads_dir / "delete_annotations.jsonl",
    )

    write_json(paths.read_targets, read_targets)
    write_jsonl(paths.insert_annotations, insert_rows)
    write_jsonl(paths.update_annotations, update_rows)
    write_jsonl(paths.delete_annotations, delete_rows)

    console.print(
        "Saved workloads:",
        str(paths.read_targets),
        str(paths.insert_annotations),
        str(paths.update_annotations),
        str(paths.delete_annotations),
    )

    return paths
