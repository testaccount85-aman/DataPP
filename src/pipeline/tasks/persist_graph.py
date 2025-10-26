"""Persist user-campaign relationships into Neo4j."""
from __future__ import annotations

from typing import Iterable

from neo4j import Transaction

from src.db.connections import neo4j_driver
from src.db.schemas import MessageRecord
from src.utils.logging import logger
from src.utils.monitoring import detect_missing_relationships


QUERY = """
UNWIND $rows AS row
MERGE (u:User {id: row.user_id})
MERGE (c:Campaign {id: row.campaign_id})
MERGE (u)-[r:INTERACTED_WITH]->(c)
SET r.last_timestamp = row.timestamp
RETURN count(*) as relationships
"""


def upsert_graph(records: Iterable[MessageRecord]) -> int:
    records_list = list(records)
    if not records_list:
        return 0

    anomalies = detect_missing_relationships(records_list)
    if anomalies:
        logger.warning("graph_anomalies_detected", details=dict(anomalies))

    rows = [record.dict() for record in records_list]
    with neo4j_driver.session() as session:
        result = session.execute_write(lambda tx: _run_query(tx, rows))
    logger.bind(upserted=result).info("neo4j_relationships_upserted")
    return result


def _run_query(tx: Transaction, rows: list[dict[str, str]]) -> int:
    res = tx.run(QUERY, rows=rows)
    return res.single()["relationships"]


__all__ = ["upsert_graph"]
