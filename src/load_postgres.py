from __future__ import annotations

from pathlib import Path
from time import perf_counter

import psycopg

from src.config import Settings
from src.utils import console


DDL_STATEMENTS = [
    "DROP TABLE IF EXISTS address_annotations",
    "DROP TABLE IF EXISTS transactions",
    "DROP TABLE IF EXISTS blocks",
    "DROP TABLE IF EXISTS addresses",
    """
    CREATE TABLE addresses (
        address TEXT PRIMARY KEY
    )
    """,
    """
    CREATE TABLE blocks (
        block_hash TEXT PRIMARY KEY,
        block_timestamp TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE transactions (
        transaction_hash TEXT PRIMARY KEY,
        block_hash TEXT,
        from_address TEXT,
        to_address TEXT,
        block_timestamp TIMESTAMPTZ,
        transaction_index BIGINT,
        nonce BIGINT,
        value_str TEXT,
        value_num TEXT,
        input TEXT,
        gas BIGINT,
        gas_price_str TEXT,
        gas_price_num TEXT,
        max_fee_per_gas BIGINT,
        max_priority_fee_per_gas BIGINT,
        transaction_type BIGINT
    )
    """,
    """
    CREATE TABLE address_annotations (
        annotation_id BIGSERIAL PRIMARY KEY,
        address TEXT NOT NULL,
        label TEXT NOT NULL,
        risk_score INTEGER,
        note TEXT,
        source TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ
    )
    """,
    "CREATE INDEX idx_transactions_block_hash ON transactions (block_hash)",
    "CREATE INDEX idx_transactions_from_address ON transactions (from_address)",
    "CREATE INDEX idx_transactions_to_address ON transactions (to_address)",
    "CREATE INDEX idx_transactions_block_timestamp ON transactions (block_timestamp)",
    "CREATE INDEX idx_address_annotations_address ON address_annotations (address)",
]


def get_connection(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.postgres_dsn)


def _copy_csv(conn: psycopg.Connection, table: str, columns: list[str], path: Path) -> None:
    statement = (
        f"COPY {table} ({', '.join(columns)}) FROM STDIN "
        "WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    )
    with conn.cursor() as cursor:
        with cursor.copy(statement) as copy:
            with path.open("r", encoding="utf-8") as handle:
                while chunk := handle.read(1024 * 1024):
                    copy.write(chunk)


def _reset_annotation_sequence(conn: psycopg.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT setval(
                pg_get_serial_sequence('address_annotations', 'annotation_id'),
                COALESCE((SELECT MAX(annotation_id) FROM address_annotations), 1),
                true
            )
            """
        )


def load_postgres(settings: Settings) -> None:
    settings.ensure_directories()
    start = perf_counter()

    with get_connection(settings) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for statement in DDL_STATEMENTS:
                cursor.execute(statement)

        copy_steps = [
            ("addresses", ["address"], settings.postgres_data_dir / "addresses.csv"),
            ("blocks", ["block_hash", "block_timestamp"], settings.postgres_data_dir / "blocks.csv"),
            (
                "transactions",
                [
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
                ],
                settings.postgres_data_dir / "transactions.csv",
            ),
        ]

        for table, columns, path in copy_steps:
            step_start = perf_counter()
            _copy_csv(conn, table, columns, path)
            console.print(f"Loaded PostgreSQL {table} in {perf_counter() - step_start:.6f} seconds")

        _reset_annotation_sequence(conn)

    console.print(f"PostgreSQL load finished in {perf_counter() - start:.6f} seconds")
