from __future__ import annotations

from time import perf_counter

from src.config import Settings
from src.graph_native_lab.common import case_b_paths
from src.load_neo4j import get_driver
from src.utils import console


CONSTRAINTS = [
    "CREATE CONSTRAINT address_unique IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE",
    "CREATE CONSTRAINT case_b_label_unique IF NOT EXISTS FOR (l:CaseBLabel) REQUIRE l.label IS UNIQUE",
]


def _run_query(session, query: str) -> None:
    session.run(query).consume()


def load_case_b_neo4j(settings: Settings) -> None:
    settings.ensure_directories()
    paths = case_b_paths(settings)
    start = perf_counter()

    queries = [
        "MATCH ()-[r:CASE_B_TRANSFER]-() DELETE r",
        "MATCH (:Address)-[r:HAS_CASE_B_LABEL]->(:CaseBLabel) DELETE r",
        "MATCH (n:CaseBLabel) DETACH DELETE n",
        f"""
        LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url(paths.neo4j_addresses.name)}' AS row
        CALL {{
            WITH row
            MERGE (:Address {{address: row.address}})
        }} IN TRANSACTIONS OF 1000 ROWS
        """,
        f"""
        LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url(paths.neo4j_transfer_edges.name)}' AS row
        CALL {{
            WITH row
            MATCH (src:Address {{address: row.src_address}})
            MATCH (dst:Address {{address: row.dst_address}})
            MERGE (src)-[r:CASE_B_TRANSFER {{transaction_hash: row.transaction_hash}}]->(dst)
            SET
                r.block_timestamp = row.block_timestamp,
                r.value_num = CASE WHEN row.value_num = '' THEN NULL ELSE row.value_num END,
                r.transaction_type = CASE WHEN row.transaction_type = '' THEN NULL ELSE toInteger(row.transaction_type) END
        }} IN TRANSACTIONS OF 1000 ROWS
        """,
        f"""
        LOAD CSV WITH HEADERS FROM '{settings.neo4j_csv_url(paths.neo4j_address_labels.name)}' AS row
        CALL {{
            WITH row
            MATCH (a:Address {{address: row.address}})
            MERGE (l:CaseBLabel {{label: row.label}})
            MERGE (a)-[:HAS_CASE_B_LABEL]->(l)
        }} IN TRANSACTIONS OF 1000 ROWS
        """,
    ]

    with get_driver(settings) as driver:
        with driver.session(database=settings.neo4j_database) as session:
            for statement in CONSTRAINTS:
                session.run(statement).consume()

            labels = [
                "Cleanup old CASE_B_TRANSFER edges",
                "Cleanup old Case B label relationships",
                "Cleanup old Case B label nodes",
                "Address nodes",
                "CASE_B_TRANSFER edges",
                "Case B labels",
            ]
            for label, query in zip(labels, queries, strict=True):
                step_start = perf_counter()
                _run_query(session, query)
                console.print(f"Loaded Neo4j {label} in {perf_counter() - step_start:.6f} seconds")

    console.print(f"Neo4j Case B load finished in {perf_counter() - start:.6f} seconds")
