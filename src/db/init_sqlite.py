"""Helpers to initialize SQLite analytics database."""
from __future__ import annotations

import sqlite3

from src.db import schemas
from src.utils.logging import logger


def initialize_sqlite(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for statement in schemas.SQLITE_DDL:
        cursor.execute(statement)
    conn.commit()
    logger.bind(db="sqlite").info("sqlite_initialized")


__all__ = ["initialize_sqlite"]
