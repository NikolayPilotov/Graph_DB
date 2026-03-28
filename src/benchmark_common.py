from __future__ import annotations

from datetime import datetime
from itertools import cycle, islice
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.config import Settings
from src.utils import read_json, read_jsonl, utc_now_iso


CASE_DISPLAY_NAMES = {
    "transaction_by_hash": "Transaction by hash",
    "transactions_sent_by_address": "Sent transactions by address",
    "transactions_received_by_address": "Received transactions by address",
    "transactions_in_one_block": "Transactions in one block",
    "two_hop_traversal_from_address": "Two-hop traversal from address",
    "insert_annotation": "Insert annotation",
    "update_annotation": "Update annotation",
    "delete_annotation": "Delete annotation",
}


def benchmark_output_path(settings: Settings, prefix: str) -> Path:
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return settings.raw_results_dir / f"{prefix}_{stamp}.jsonl"


def measured_items(items: list[dict[str, Any]], warmup_runs: int, repeat_runs: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not items:
        raise ValueError("Workload items are empty.")
    warmups = list(islice(cycle(items), warmup_runs))
    measured = list(islice(cycle(items[warmup_runs:] or items), repeat_runs))
    if len(measured) < repeat_runs:
        measured = list(islice(cycle(items), repeat_runs))
    return warmups, measured


def load_read_targets(settings: Settings) -> dict[str, list[dict[str, Any]]]:
    payload = read_json(settings.workloads_dir / "read_targets.json")
    return {str(key): list(value) for key, value in payload.items()}


def load_mutation_targets(settings: Settings) -> dict[str, list[dict[str, Any]]]:
    return {
        "insert_annotation": read_jsonl(settings.workloads_dir / "insert_annotations.jsonl"),
        "update_annotation": read_jsonl(settings.workloads_dir / "update_annotations.jsonl"),
        "delete_annotation": read_jsonl(settings.workloads_dir / "delete_annotations.jsonl"),
    }


def build_result_record(
    *,
    db: str,
    case: str,
    run_number: int,
    latency_seconds: float,
    success: bool,
    target: dict[str, Any],
) -> dict[str, Any]:
    return {
        "db": db,
        "case": case,
        "run_number": run_number,
        "latency_seconds": latency_seconds,
        "total_time_seconds": latency_seconds,
        "success": success,
        "timestamp": utc_now_iso(),
        "target": target,
    }


def load_raw_results(settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted(settings.raw_results_dir.glob("*.jsonl")):
        rows.extend(read_jsonl(path))
    return pd.DataFrame(rows)
