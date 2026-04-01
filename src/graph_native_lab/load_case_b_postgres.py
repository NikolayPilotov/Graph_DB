from __future__ import annotations

from time import perf_counter

from src.config import Settings
from src.graph_native_lab.common import case_b_paths
from src.load_postgres import _copy_csv, get_connection
from src.utils import console


DDL_STATEMENTS = [
    "DROP TABLE IF EXISTS case_b_address_labels",
    "DROP TABLE IF EXISTS case_b_transfer_edges",
    """
    CREATE TABLE case_b_transfer_edges (
        src_address TEXT NOT NULL,
        dst_address TEXT NOT NULL,
        transaction_hash TEXT NOT NULL,
        block_timestamp TEXT NOT NULL,
        value_num TEXT,
        transaction_type BIGINT
    )
    """,
    """
    CREATE TABLE case_b_address_labels (
        address TEXT NOT NULL,
        label TEXT NOT NULL
    )
    """,
    "CREATE INDEX idx_case_b_transfer_src ON case_b_transfer_edges (src_address)",
    "CREATE INDEX idx_case_b_transfer_dst ON case_b_transfer_edges (dst_address)",
    "CREATE INDEX idx_case_b_transfer_timestamp ON case_b_transfer_edges (block_timestamp)",
    "CREATE INDEX idx_case_b_transfer_src_dst_ts ON case_b_transfer_edges (src_address, dst_address, block_timestamp)",
    "CREATE INDEX idx_case_b_labels_address_label ON case_b_address_labels (address, label)",
]


def load_case_b_postgres(settings: Settings) -> None:
    settings.ensure_directories()
    paths = case_b_paths(settings)
    start = perf_counter()

    with get_connection(settings) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for statement in DDL_STATEMENTS:
                cursor.execute(statement)

        copy_steps = [
            (
                "case_b_transfer_edges",
                ["src_address", "dst_address", "transaction_hash", "block_timestamp", "value_num", "transaction_type"],
                paths.postgres_transfer_edges,
            ),
            ("case_b_address_labels", ["address", "label"], paths.postgres_address_labels),
        ]

        for table, columns, path in copy_steps:
            step_start = perf_counter()
            _copy_csv(conn, table, columns, path)
            console.print(f"Loaded PostgreSQL {table} in {perf_counter() - step_start:.6f} seconds")

    console.print(f"PostgreSQL Case B load finished in {perf_counter() - start:.6f} seconds")

