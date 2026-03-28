from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import Settings
from src.utils import console, timed
from src.workload_generator import WorkloadPaths, generate_workloads


REQUIRED_TRANSACTION_COLUMNS = [
    "block_hash",
    "block_timestamp",
    "transaction_hash",
    "transaction_index",
    "nonce",
    "from_address",
    "to_address",
    "value_str",
    "value_num",
    "input",
    "gas",
    "gas_price_str",
    "gas_price_num",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "transaction_type",
]


@dataclass(slots=True)
class PreparedPaths:
    normalized_parquet: Path
    normalized_csv: Path
    postgres_transactions: Path
    postgres_addresses: Path
    postgres_blocks: Path
    neo4j_transactions: Path
    neo4j_addresses: Path
    neo4j_blocks: Path
    neo4j_sent_edges: Path
    neo4j_to_edges: Path
    neo4j_in_block_edges: Path
    workloads: WorkloadPaths


def detect_source_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".json":
        return "json"
    raise ValueError(f"Unsupported source file type: {path.suffix}")


def _load_json_records(path: Path, source_type: str) -> list[dict[str, Any]]:
    if source_type == "jsonl":
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        list_values = [value for value in payload.values() if isinstance(value, list)]
        if len(list_values) == 1:
            return list_values[0]
        return [payload]
    raise ValueError("JSON payload must be a list of rows, JSONL lines, or a dict containing a row list.")


def load_source_file(path: Path, source_type: str) -> pd.DataFrame:
    if source_type == "parquet":
        return pd.read_parquet(path)
    records = _load_json_records(path, source_type)
    return pd.json_normalize(records, sep=".")


def _nested_lookup(value: Any, child: str) -> Any:
    if isinstance(value, dict):
        return value.get(child)
    return None


def _series_from_options(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for candidate in candidates:
        if candidate in frame.columns:
            return frame[candidate]
        if "." in candidate:
            root, child = candidate.split(".", 1)
            if root in frame.columns:
                return frame[root].apply(lambda value: _nested_lookup(value, child))
    return pd.Series([None] * len(frame), index=frame.index, dtype="object")


def _normalize_text_series(series: pd.Series, lowercase: bool = False) -> pd.Series:
    output = series.astype("object").where(series.notna(), None)
    output = output.map(lambda value: None if value is None else str(value).strip())
    output = output.replace({"": None})
    if lowercase:
        output = output.map(lambda value: value.lower() if isinstance(value, str) else value)
    return output


def _normalize_int_series(series: pd.Series) -> pd.Series:
    def parse_value(value: Any) -> int | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        if text.startswith("0x"):
            return int(text, 16)
        try:
            decimal_value = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"Cannot parse integer-like value: {text!r}") from exc
        if decimal_value != decimal_value.to_integral_value():
            raise ValueError(f"Value is not an integer: {text!r}")
        return int(decimal_value)

    return series.map(parse_value, na_action="ignore").astype("Int64")


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    formatted = parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted.where(parsed.notna(), None)


def _coerce_required_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = pd.DataFrame(index=frame.index)
    normalized["block_hash"] = _normalize_text_series(_series_from_options(frame, ["block_hash"]))
    normalized["block_timestamp"] = _normalize_timestamp_series(_series_from_options(frame, ["block_timestamp"]))
    normalized["transaction_hash"] = _normalize_text_series(_series_from_options(frame, ["transaction_hash"]))
    normalized["transaction_index"] = _normalize_int_series(_series_from_options(frame, ["transaction_index"]))
    normalized["nonce"] = _normalize_int_series(_series_from_options(frame, ["nonce"]))
    normalized["from_address"] = _normalize_text_series(
        _series_from_options(frame, ["from_address"]), lowercase=True
    )
    normalized["to_address"] = _normalize_text_series(_series_from_options(frame, ["to_address"]), lowercase=True)
    normalized["value_str"] = _normalize_text_series(
        _series_from_options(frame, ["value_str", "value.string_value", "value.stringvalue"])
    )
    normalized["value_num"] = _normalize_text_series(
        _series_from_options(frame, ["value_num", "value.bignumeric_value", "value.bignumericvalue"])
    )
    normalized["input"] = _normalize_text_series(_series_from_options(frame, ["input"]))
    normalized["gas"] = _normalize_int_series(_series_from_options(frame, ["gas"]))
    normalized["gas_price_str"] = _normalize_text_series(
        _series_from_options(frame, ["gas_price_str", "gas_price.string_value", "gas_price.stringvalue"])
    )
    normalized["gas_price_num"] = _normalize_text_series(
        _series_from_options(
            frame,
            ["gas_price_num", "gas_price.bignumeric_value", "gas_price.bignumericvalue"],
        )
    )
    normalized["max_fee_per_gas"] = _normalize_int_series(_series_from_options(frame, ["max_fee_per_gas"]))
    normalized["max_priority_fee_per_gas"] = _normalize_int_series(
        _series_from_options(frame, ["max_priority_fee_per_gas"])
    )
    normalized["transaction_type"] = _normalize_int_series(_series_from_options(frame, ["transaction_type"]))
    return normalized


def _build_addresses(transactions_df: pd.DataFrame) -> pd.DataFrame:
    values = pd.concat([transactions_df["from_address"], transactions_df["to_address"]], ignore_index=True)
    values = values.dropna().astype(str).drop_duplicates().sort_values()
    return pd.DataFrame({"address": values.tolist()})


def _build_blocks(transactions_df: pd.DataFrame) -> pd.DataFrame:
    blocks = (
        transactions_df.loc[transactions_df["block_hash"].notna(), ["block_hash", "block_timestamp"]]
        .drop_duplicates(subset=["block_hash"])
        .sort_values(["block_timestamp", "block_hash"], na_position="last")
        .reset_index(drop=True)
    )
    return blocks


def _build_neo4j_transaction_nodes(transactions_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "transaction_hash",
        "block_timestamp",
        "transaction_index",
        "nonce",
        "value_str",
        "value_num",
        "input",
        "gas",
        "gas_price_str",
        "gas_price_num",
        "max_fee_per_gas",
        "max_priority_fee_per_gas",
        "transaction_type",
    ]
    return transactions_df.loc[:, columns].copy()


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False)


def prepare_data(settings: Settings, source_path: Path | None = None, row_limit: int | None = None) -> PreparedPaths:
    settings.ensure_directories()
    resolved_source_value = source_path or settings.bench_source_path
    if resolved_source_value is None:
        raise ValueError("A source path must be provided through --source-path or BENCH_SOURCE_PATH.")
    resolved_source = Path(resolved_source_value)
    if not resolved_source.exists():
        raise FileNotFoundError(f"Source file does not exist: {resolved_source}")

    source_type = detect_source_type(resolved_source)
    console.print(f"Detected source type: {source_type}")

    with timed("Load source file"):
        raw_frame = load_source_file(resolved_source, source_type)

    raw_frame.columns = [str(column).strip().lower() for column in raw_frame.columns]
    if row_limit:
        raw_frame = raw_frame.head(row_limit).copy()

    with timed("Normalize transactions"):
        transactions_df = _coerce_required_columns(raw_frame)
        transactions_df = transactions_df.loc[transactions_df["transaction_hash"].notna()].copy()
        transactions_df = transactions_df.drop_duplicates(subset=["transaction_hash"]).reset_index(drop=True)
        transactions_df = transactions_df.loc[:, REQUIRED_TRANSACTION_COLUMNS]

    addresses_df = _build_addresses(transactions_df)
    blocks_df = _build_blocks(transactions_df)
    neo4j_transactions_df = _build_neo4j_transaction_nodes(transactions_df)

    sent_edges_df = (
        transactions_df.loc[
            transactions_df["from_address"].notna(),
            ["from_address", "transaction_hash"],
        ]
        .rename(columns={"from_address": "address"})
        .drop_duplicates()
        .reset_index(drop=True)
    )
    to_edges_df = (
        transactions_df.loc[
            transactions_df["to_address"].notna(),
            ["transaction_hash", "to_address"],
        ]
        .rename(columns={"to_address": "address"})
        .drop_duplicates()
        .reset_index(drop=True)
    )
    in_block_edges_df = (
        transactions_df.loc[
            transactions_df["block_hash"].notna(),
            ["transaction_hash", "block_hash"],
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    normalized_parquet = settings.raw_data_dir / "normalized_transactions.parquet"
    normalized_csv = settings.raw_data_dir / "normalized_transactions.csv"
    postgres_transactions = settings.postgres_data_dir / "transactions.csv"
    postgres_addresses = settings.postgres_data_dir / "addresses.csv"
    postgres_blocks = settings.postgres_data_dir / "blocks.csv"
    neo4j_transactions = settings.neo4j_data_dir / "transactions.csv"
    neo4j_addresses = settings.neo4j_data_dir / "addresses.csv"
    neo4j_blocks = settings.neo4j_data_dir / "blocks.csv"
    neo4j_sent_edges = settings.neo4j_data_dir / "sent_edges.csv"
    neo4j_to_edges = settings.neo4j_data_dir / "to_edges.csv"
    neo4j_in_block_edges = settings.neo4j_data_dir / "in_block_edges.csv"

    with timed("Write normalized data"):
        transactions_df.to_parquet(normalized_parquet, index=False)
        _write_csv(transactions_df, normalized_csv)
        _write_csv(transactions_df, postgres_transactions)
        _write_csv(addresses_df, postgres_addresses)
        _write_csv(blocks_df, postgres_blocks)
        _write_csv(neo4j_transactions_df, neo4j_transactions)
        _write_csv(addresses_df, neo4j_addresses)
        _write_csv(blocks_df, neo4j_blocks)
        _write_csv(sent_edges_df, neo4j_sent_edges)
        _write_csv(to_edges_df, neo4j_to_edges)
        _write_csv(in_block_edges_df, neo4j_in_block_edges)

    workloads = generate_workloads(transactions_df, settings)

    console.print(
        f"Prepared {len(transactions_df):,} transactions, "
        f"{len(addresses_df):,} addresses, {len(blocks_df):,} blocks."
    )

    return PreparedPaths(
        normalized_parquet=normalized_parquet,
        normalized_csv=normalized_csv,
        postgres_transactions=postgres_transactions,
        postgres_addresses=postgres_addresses,
        postgres_blocks=postgres_blocks,
        neo4j_transactions=neo4j_transactions,
        neo4j_addresses=neo4j_addresses,
        neo4j_blocks=neo4j_blocks,
        neo4j_sent_edges=neo4j_sent_edges,
        neo4j_to_edges=neo4j_to_edges,
        neo4j_in_block_edges=neo4j_in_block_edges,
        workloads=workloads,
    )
