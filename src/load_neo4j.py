from __future__ import annotations

from time import perf_counter

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from src.config import Settings
from src.utils import console


CONSTRAINTS = [
    "CREATE CONSTRAINT address_unique IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE",
    "CREATE CONSTRAINT block_unique IF NOT EXISTS FOR (b:Block) REQUIRE b.block_hash IS UNIQUE",
    "CREATE CONSTRAINT transaction_unique IF NOT EXISTS FOR (t:Transaction) REQUIRE t.transaction_hash IS UNIQUE",
    "CREATE CONSTRAINT annotation_unique IF NOT EXISTS FOR (a:Annotation) REQUIRE a.annotation_id IS UNIQUE",
]


def get_driver(settings: Settings):
    return GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))


def _run_query(session, query: str) -> None:
    session.run(query).consume()


def _load_nodes_and_edges(session, settings: Settings) -> None:
    queries = [
        (
            "Address nodes",
            f"""
            LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url("addresses.csv")}' AS row
            CALL {{
                WITH row
                MERGE (:Address {{address: row.address}})
            }} IN TRANSACTIONS OF 1000 ROWS
            """,
        ),
        (
            "Block nodes",
            f"""
            LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url("blocks.csv")}' AS row
            CALL {{
                WITH row
                MERGE (block:Block {{block_hash: row.block_hash}})
                SET block.block_timestamp = CASE WHEN row.block_timestamp = '' THEN NULL ELSE row.block_timestamp END
            }} IN TRANSACTIONS OF 1000 ROWS
            """,
        ),
        (
            "Transaction nodes",
            f"""
            LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url("transactions.csv")}' AS row
            CALL {{
                WITH row
                MERGE (tx:Transaction {{transaction_hash: row.transaction_hash}})
                SET
                    tx.block_timestamp = CASE WHEN row.block_timestamp = '' THEN NULL ELSE row.block_timestamp END,
                    tx.transaction_index = CASE WHEN row.transaction_index = '' THEN NULL ELSE toInteger(row.transaction_index) END,
                    tx.nonce = CASE WHEN row.nonce = '' THEN NULL ELSE toInteger(row.nonce) END,
                    tx.value_str = CASE WHEN row.value_str = '' THEN NULL ELSE row.value_str END,
                    tx.value_num = CASE WHEN row.value_num = '' THEN NULL ELSE row.value_num END,
                    tx.input = CASE WHEN row.input = '' THEN NULL ELSE row.input END,
                    tx.gas = CASE WHEN row.gas = '' THEN NULL ELSE toInteger(row.gas) END,
                    tx.gas_price_str = CASE WHEN row.gas_price_str = '' THEN NULL ELSE row.gas_price_str END,
                    tx.gas_price_num = CASE WHEN row.gas_price_num = '' THEN NULL ELSE row.gas_price_num END,
                    tx.max_fee_per_gas = CASE WHEN row.max_fee_per_gas = '' THEN NULL ELSE toInteger(row.max_fee_per_gas) END,
                    tx.max_priority_fee_per_gas = CASE WHEN row.max_priority_fee_per_gas = '' THEN NULL ELSE toInteger(row.max_priority_fee_per_gas) END,
                    tx.transaction_type = CASE WHEN row.transaction_type = '' THEN NULL ELSE toInteger(row.transaction_type) END
            }} IN TRANSACTIONS OF 1000 ROWS
            """,
        ),
        (
            "SENT edges",
            f"""
            LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url("sent_edges.csv")}' AS row
            CALL {{
                WITH row
                MATCH (a:Address {{address: row.address}})
                MATCH (tx:Transaction {{transaction_hash: row.transaction_hash}})
                MERGE (a)-[:SENT]->(tx)
            }} IN TRANSACTIONS OF 1000 ROWS
            """,
        ),
        (
            "TO edges",
            f"""
            LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url("to_edges.csv")}' AS row
            CALL {{
                WITH row
                MATCH (tx:Transaction {{transaction_hash: row.transaction_hash}})
                MATCH (a:Address {{address: row.address}})
                MERGE (tx)-[:TO]->(a)
            }} IN TRANSACTIONS OF 1000 ROWS
            """,
        ),
        (
            "IN_BLOCK edges",
            f"""
            LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url("in_block_edges.csv")}' AS row
            CALL {{
                WITH row
                MATCH (tx:Transaction {{transaction_hash: row.transaction_hash}})
                MATCH (b:Block {{block_hash: row.block_hash}})
                MERGE (tx)-[:IN_BLOCK]->(b)
            }} IN TRANSACTIONS OF 1000 ROWS
            """,
        ),
    ]

    for label, query in queries:
        step_start = perf_counter()
        _run_query(session, query)
        console.print(f"Loaded Neo4j {label} in {perf_counter() - step_start:.6f} seconds")


def load_neo4j(settings: Settings) -> None:
    settings.ensure_directories()
    start = perf_counter()

    with get_driver(settings) as driver:
        with driver.session(database=settings.neo4j_database) as session:
            for statement in CONSTRAINTS:
                session.run(statement).consume()

            try:
                session.run("MATCH (n) DETACH DELETE n").consume()
            except Neo4jError as exc:
                raise RuntimeError("Neo4j delete failed. Confirm the database is reachable.") from exc

            _load_nodes_and_edges(session, settings)

    console.print(f"Neo4j load finished in {perf_counter() - start:.6f} seconds")
