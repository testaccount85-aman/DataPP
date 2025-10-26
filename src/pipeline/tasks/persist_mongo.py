"""Persist message metadata into MongoDB."""
from __future__ import annotations

from typing import Iterable

from src.db.connections import mongo_db
from src.db.schemas import MessageRecord
from src.utils.logging import logger


COLLECTION_NAME = "conversations"


def write_messages(records: Iterable[MessageRecord]) -> int:
    docs = [record.dict() for record in records]
    if not docs:
        return 0
    mongo_db[COLLECTION_NAME].insert_many(docs, ordered=False)
    logger.bind(collection=COLLECTION_NAME, inserted=len(docs)).info("mongo_messages_upserted")
    return len(docs)


__all__ = ["write_messages", "COLLECTION_NAME"]
