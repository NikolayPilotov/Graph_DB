from __future__ import annotations

import random
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd

from src.benchmark_common import measured_items
from src.config import Settings
from src.graph_native_lab.common import (
    CASE_B_NAME,
    DEFAULT_MAX_HOPS,
    DEFAULT_WINDOW_HOURS,
    TARGET_LABEL_EXCHANGE,
    CaseBPaths,
    case_b_paths,
)
from src.utils import console, safe_text, timed, write_json, write_jsonl


@dataclass(slots=True)
class CaseBPrepared:
    paths: CaseBPaths
    transfer_edge_count: int
    address_count: int
    labeled_address_count: int
    workload_count: int


def _load_normalized_transactions(settings: Settings) -> pd.DataFrame:
    parquet_path = settings.raw_data_dir / "normalized_transactions.parquet"
    csv_path = settings.raw_data_dir / "normalized_transactions.csv"
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(
        "Normalized transactions were not found. Run `py -m src.main prepare-data` first."
    )


def _build_transfer_edges(transactions_df: pd.DataFrame) -> pd.DataFrame:
    edges = transactions_df.loc[
        transactions_df["from_address"].notna() & transactions_df["to_address"].notna(),
        [
            "from_address",
            "to_address",
            "transaction_hash",
            "block_timestamp",
            "value_num",
            "transaction_type",
        ],
    ].copy()
    edges = edges.rename(columns={"from_address": "src_address", "to_address": "dst_address"})
    edges = edges.loc[edges["src_address"] != edges["dst_address"]].reset_index(drop=True)
    edges["block_timestamp"] = edges["block_timestamp"].astype(str)
    edges["value_num"] = edges["value_num"].map(safe_text)
    edges["transaction_type"] = edges["transaction_type"].map(
        lambda value: None if pd.isna(value) else int(value)
    )
    return edges


def _pick_exchange_labels(transfer_edges: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    indegree = transfer_edges.groupby("dst_address")["transaction_hash"].count().sort_values(ascending=False)
    candidate_addresses = indegree.loc[indegree >= 2].index.astype(str).tolist()
    if not candidate_addresses:
        candidate_addresses = indegree.index.astype(str).tolist()
    if not candidate_addresses:
        raise ValueError("No destination addresses are available to build Case B labels.")

    total_addresses = len(set(transfer_edges["src_address"]).union(set(transfer_edges["dst_address"])))
    label_count = min(max(16, int(total_addresses * 0.01)), max(16, len(candidate_addresses)))
    label_count = min(label_count, len(candidate_addresses))

    rng = random.Random(settings.workload_seed + 200)
    if len(candidate_addresses) <= label_count:
        selected = candidate_addresses
    else:
        selected = rng.sample(candidate_addresses, label_count)
    return pd.DataFrame({"address": sorted(selected), "label": TARGET_LABEL_EXCHANGE})


def _build_addresses(transfer_edges: pd.DataFrame, labels_df: pd.DataFrame) -> pd.DataFrame:
    addresses = set(transfer_edges["src_address"]).union(set(transfer_edges["dst_address"]))
    addresses.update(labels_df["address"].astype(str).tolist())
    return pd.DataFrame({"address": sorted(addresses)})


def _iso_plus_hours(timestamp: pd.Timestamp, hours: int) -> str:
    return (timestamp + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _find_shortest_path(
    *,
    seed: str,
    edges_by_src: dict[str, list[tuple[str, pd.Timestamp]]],
    target_addresses: set[str],
    t0: pd.Timestamp,
    t1: pd.Timestamp,
    max_hops: int,
) -> list[str] | None:
    queue: deque[tuple[str, list[str]]] = deque([(seed, [seed])])
    while queue:
        node, path = queue.popleft()
        if len(path) - 1 >= max_hops:
            continue
        for dst_address, timestamp in edges_by_src.get(node, []):
            if timestamp < t0 or timestamp >= t1:
                continue
            if dst_address in path:
                continue
            next_path = path + [dst_address]
            if dst_address in target_addresses:
                return next_path
            queue.append((dst_address, next_path))
    return None


def _generate_workloads(
    transfer_edges: pd.DataFrame,
    labels_df: pd.DataFrame,
    settings: Settings,
    *,
    max_hops: int,
    window_hours: int,
) -> list[dict[str, object]]:
    target_addresses = set(labels_df["address"].astype(str).tolist())
    target_count = max(settings.warmup_runs + settings.repeat_runs + 10, 32)

    working = transfer_edges.copy()
    working["parsed_timestamp"] = pd.to_datetime(working["block_timestamp"], utc=True)
    working = working.dropna(subset=["parsed_timestamp"]).sort_values("parsed_timestamp").reset_index(drop=True)

    edges_by_src: dict[str, list[tuple[str, pd.Timestamp]]] = defaultdict(list)
    for row in working.itertuples(index=False):
        edges_by_src[str(row.src_address)].append((str(row.dst_address), row.parsed_timestamp))

    candidate_seeds = sorted(set(working["src_address"].astype(str).tolist()) - target_addresses)
    rng = random.Random(settings.workload_seed + 300)
    rng.shuffle(candidate_seeds)

    workloads: list[dict[str, object]] = []
    seen_keys: set[tuple[str, str, str]] = set()

    for seed in candidate_seeds:
        outgoing = edges_by_src.get(seed, [])
        if not outgoing:
            continue
        for dst_address, timestamp in outgoing[:3]:
            t0 = timestamp
            t1 = timestamp + timedelta(hours=window_hours)
            path = _find_shortest_path(
                seed=seed,
                edges_by_src=edges_by_src,
                target_addresses=target_addresses,
                t0=t0,
                t1=t1,
                max_hops=max_hops,
            )
            if not path:
                continue
            key = (seed, t0.strftime("%Y-%m-%dT%H:%M:%SZ"), path[-1])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            workloads.append(
                {
                    "seed": seed,
                    "target_label": TARGET_LABEL_EXCHANGE,
                    "target_address": path[-1],
                    "t0": t0.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "t1": _iso_plus_hours(t0, window_hours),
                    "max_hops": max_hops,
                    "expected_hops": len(path) - 1,
                    "expected_path": path,
                }
            )
            if len(workloads) >= target_count:
                return workloads

    if len(workloads) < target_count:
        full_start = working["parsed_timestamp"].min()
        full_end = working["parsed_timestamp"].max() + timedelta(seconds=1)
        for seed in candidate_seeds:
            path = _find_shortest_path(
                seed=seed,
                edges_by_src=edges_by_src,
                target_addresses=target_addresses,
                t0=full_start,
                t1=full_end,
                max_hops=max_hops,
            )
            if not path:
                continue
            key = (seed, full_start.strftime("%Y-%m-%dT%H:%M:%SZ"), path[-1])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            workloads.append(
                {
                    "seed": seed,
                    "target_label": TARGET_LABEL_EXCHANGE,
                    "target_address": path[-1],
                    "t0": full_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "t1": full_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "max_hops": max_hops,
                    "expected_hops": len(path) - 1,
                    "expected_path": path,
                }
            )
            if len(workloads) >= target_count:
                break

    if not workloads:
        raise ValueError("Case B workload generation failed. No reachable labeled targets were found.")
    return workloads


def prepare_case_b(
    settings: Settings,
    *,
    max_hops: int = DEFAULT_MAX_HOPS,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> CaseBPrepared:
    settings.ensure_directories()
    paths = case_b_paths(settings)

    with timed("Load normalized transactions for Case B"):
        transactions_df = _load_normalized_transactions(settings)

    with timed("Build Case B transfer projection"):
        transfer_edges_df = _build_transfer_edges(transactions_df)
        labels_df = _pick_exchange_labels(transfer_edges_df, settings)
        addresses_df = _build_addresses(transfer_edges_df, labels_df)
        workloads = _generate_workloads(
            transfer_edges_df,
            labels_df,
            settings,
            max_hops=max_hops,
            window_hours=window_hours,
        )

    with timed("Write Case B lab artifacts"):
        transfer_edges_df.to_csv(paths.postgres_transfer_edges, index=False)
        labels_df.to_csv(paths.postgres_address_labels, index=False)
        addresses_df.to_csv(paths.neo4j_addresses, index=False)
        transfer_edges_df.to_csv(paths.neo4j_transfer_edges, index=False)
        labels_df.to_csv(paths.neo4j_address_labels, index=False)
        write_jsonl(paths.workloads, workloads)
        write_json(
            paths.metadata,
            {
                "case": CASE_B_NAME,
                "transfer_edge_count": len(transfer_edges_df),
                "address_count": len(addresses_df),
                "labeled_address_count": len(labels_df),
                "workload_count": len(workloads),
                "max_hops": max_hops,
                "window_hours": window_hours,
                "paths": {key: str(value) for key, value in asdict(paths).items()},
            },
        )

    console.print(
        f"Prepared Case B lab data with {len(transfer_edges_df):,} transfer edges, "
        f"{len(labels_df):,} labeled addresses, and {len(workloads):,} workloads."
    )

    return CaseBPrepared(
        paths=paths,
        transfer_edge_count=len(transfer_edges_df),
        address_count=len(addresses_df),
        labeled_address_count=len(labels_df),
        workload_count=len(workloads),
    )

