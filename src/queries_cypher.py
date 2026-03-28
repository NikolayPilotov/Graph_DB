from __future__ import annotations

from src.workload_generator import (
    MUTATION_CASE_DELETE,
    MUTATION_CASE_INSERT,
    MUTATION_CASE_UPDATE,
    READ_CASE_IN_BLOCK,
    READ_CASE_RECEIVED_BY_ADDRESS,
    READ_CASE_SENT_BY_ADDRESS,
    READ_CASE_TRANSACTION_BY_HASH,
    READ_CASE_TWO_HOP,
)


READ_CYPHER: dict[str, str] = {
    READ_CASE_TRANSACTION_BY_HASH: """
        MATCH (tx:Transaction {transaction_hash: $transaction_hash})
        RETURN tx
    """,
    READ_CASE_SENT_BY_ADDRESS: """
        MATCH (:Address {address: $address})-[:SENT]->(tx:Transaction)
        RETURN tx
        ORDER BY tx.block_timestamp DESC, tx.transaction_index DESC
        LIMIT $limit
    """,
    READ_CASE_RECEIVED_BY_ADDRESS: """
        MATCH (tx:Transaction)-[:TO]->(:Address {address: $address})
        RETURN tx
        ORDER BY tx.block_timestamp DESC, tx.transaction_index DESC
        LIMIT $limit
    """,
    READ_CASE_IN_BLOCK: """
        MATCH (:Block {block_hash: $block_hash})<-[:IN_BLOCK]-(tx:Transaction)
        RETURN tx
        ORDER BY tx.transaction_index ASC
        LIMIT $limit
    """,
    READ_CASE_TWO_HOP: """
        MATCH (:Address {address: $address})-[:SENT]->(:Transaction)-[:TO]->(counterparty:Address)
        WHERE counterparty.address <> $address
        RETURN counterparty.address AS counterparty_address, count(*) AS transaction_count
        ORDER BY transaction_count DESC, counterparty_address ASC
        LIMIT $limit
    """,
}


MUTATION_CYPHER: dict[str, str] = {
    MUTATION_CASE_INSERT: """
        MATCH (address:Address {address: $address})
        CREATE (annotation:Annotation {
            annotation_id: $annotation_id,
            label: $label,
            risk_score: $risk_score,
            note: $note,
            source: $source,
            created_at: $created_at,
            updated_at: $updated_at
        })
        MERGE (address)-[:HAS_ANNOTATION]->(annotation)
        RETURN annotation.annotation_id AS annotation_id
    """,
    MUTATION_CASE_UPDATE: """
        MATCH (annotation:Annotation {annotation_id: $annotation_id})
        SET annotation.label = $label,
            annotation.risk_score = $risk_score,
            annotation.note = $note,
            annotation.source = $source,
            annotation.updated_at = $updated_at
        RETURN annotation.annotation_id AS annotation_id
    """,
    MUTATION_CASE_DELETE: """
        MATCH (annotation:Annotation {annotation_id: $annotation_id})
        WITH annotation, annotation.annotation_id AS annotation_id
        DETACH DELETE annotation
        RETURN annotation_id
    """,
}
