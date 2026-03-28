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


READ_SQL: dict[str, str] = {
    READ_CASE_TRANSACTION_BY_HASH: """
        SELECT
            transaction_hash,
            block_hash,
            from_address,
            to_address,
            block_timestamp,
            transaction_index,
            nonce,
            value_str,
            value_num,
            input,
            gas,
            gas_price_str,
            gas_price_num,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            transaction_type
        FROM transactions
        WHERE transaction_hash = %(transaction_hash)s
    """,
    READ_CASE_SENT_BY_ADDRESS: """
        SELECT
            transaction_hash,
            to_address,
            block_hash,
            block_timestamp,
            transaction_index
        FROM transactions
        WHERE from_address = %(address)s
        ORDER BY block_timestamp DESC NULLS LAST, transaction_index DESC NULLS LAST
        LIMIT %(limit)s
    """,
    READ_CASE_RECEIVED_BY_ADDRESS: """
        SELECT
            transaction_hash,
            from_address,
            block_hash,
            block_timestamp,
            transaction_index
        FROM transactions
        WHERE to_address = %(address)s
        ORDER BY block_timestamp DESC NULLS LAST, transaction_index DESC NULLS LAST
        LIMIT %(limit)s
    """,
    READ_CASE_IN_BLOCK: """
        SELECT
            transaction_hash,
            from_address,
            to_address,
            transaction_index,
            block_timestamp
        FROM transactions
        WHERE block_hash = %(block_hash)s
        ORDER BY transaction_index ASC NULLS LAST
        LIMIT %(limit)s
    """,
    READ_CASE_TWO_HOP: """
        SELECT
            to_address AS counterparty_address,
            COUNT(*) AS transaction_count
        FROM transactions
        WHERE from_address = %(address)s
          AND to_address IS NOT NULL
          AND to_address <> %(address)s
        GROUP BY to_address
        ORDER BY transaction_count DESC, counterparty_address ASC
        LIMIT %(limit)s
    """,
}


MUTATION_SQL: dict[str, str] = {
    MUTATION_CASE_INSERT: """
        INSERT INTO address_annotations (
            annotation_id,
            address,
            label,
            risk_score,
            note,
            source,
            created_at,
            updated_at
        )
        VALUES (
            %(annotation_id)s,
            %(address)s,
            %(label)s,
            %(risk_score)s,
            %(note)s,
            %(source)s,
            %(created_at)s,
            %(updated_at)s
        )
        RETURNING annotation_id
    """,
    MUTATION_CASE_UPDATE: """
        UPDATE address_annotations
        SET
            label = %(label)s,
            risk_score = %(risk_score)s,
            note = %(note)s,
            source = %(source)s,
            updated_at = %(updated_at)s
        WHERE annotation_id = %(annotation_id)s
        RETURNING annotation_id
    """,
    MUTATION_CASE_DELETE: """
        DELETE FROM address_annotations
        WHERE annotation_id = %(annotation_id)s
        RETURNING annotation_id
    """,
}
