from __future__ import annotations


def case_b_sql() -> str:
    return """
        WITH RECURSIVE bfs AS (
          SELECT
            e.dst_address AS node,
            1 AS depth,
            ARRAY[%(seed)s::text, e.dst_address] AS path
          FROM case_b_transfer_edges e
          WHERE e.src_address = %(seed)s
            AND e.block_timestamp >= %(t0)s
            AND e.block_timestamp < %(t1)s

          UNION ALL

          SELECT
            e.dst_address AS node,
            b.depth + 1 AS depth,
            b.path || e.dst_address
          FROM bfs b
          JOIN case_b_transfer_edges e
            ON e.src_address = b.node
          WHERE b.depth < %(max_hops)s
            AND e.block_timestamp >= %(t0)s
            AND e.block_timestamp < %(t1)s
            AND NOT (e.dst_address = ANY(b.path))
        )
        SELECT
          b.path AS address_path,
          b.depth AS hops,
          b.node AS target_address
        FROM bfs b
        JOIN case_b_address_labels ann
          ON ann.address = b.node
         AND ann.address = %(target_address)s
         AND ann.label = %(target_label)s
        ORDER BY b.depth ASC, array_to_string(b.path, '>') ASC
        LIMIT 1
    """


def case_b_cypher(max_hops: int) -> str:
    return f"""
        MATCH (seed:Address {{address: $seed}})
        MATCH (target:Address {{address: $target_address}})-[:HAS_CASE_B_LABEL]->(:CaseBLabel {{label: $target_label}})
        MATCH p = shortestPath((seed)-[:CASE_B_TRANSFER*1..{max_hops}]->(target))
        WHERE p IS NOT NULL
          AND all(rel IN relationships(p) WHERE rel.block_timestamp >= $t0 AND rel.block_timestamp < $t1)
        RETURN [n IN nodes(p) | n.address] AS address_path,
               length(p) AS hops,
               target.address AS target_address
        ORDER BY hops ASC, target_address ASC
        LIMIT 1
    """
