"""Aggregate engagement metrics into SQLite."""
from __future__ import annotations

from typing import Iterable

from src.db.connections import sqlite_conn
from src.db.schemas import MessageRecord, as_sqlite_row, update_campaign_stats_sql
from src.utils.logging import logger


def aggregate(records: Iterable[MessageRecord]) -> int:
    records_list = list(records)
    if not records_list:
        return 0

    cursor = sqlite_conn.cursor()
    cursor.executemany(
        "INSERT INTO engagement(user_id, campaign_id, ts) VALUES (?, ?, ?)",
        [as_sqlite_row(record) for record in records_list],
    )
    cursor.execute(update_campaign_stats_sql())
    sqlite_conn.commit()
    logger.bind(processed=len(records_list)).info("sqlite_engagements_aggregated")
    return len(records_list)


__all__ = ["aggregate"]
