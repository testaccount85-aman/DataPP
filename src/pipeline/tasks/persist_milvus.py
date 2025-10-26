"""Persist embeddings into Milvus."""
from __future__ import annotations

from typing import Iterable

import numpy as np

from src.db.connections import collection
from src.db.schemas import MessageRecord
from src.utils.logging import logger


def upsert_embeddings(pairs: Iterable[tuple[MessageRecord, np.ndarray]]) -> int:
    records = list(pairs)
    if not records:
        return 0

    user_ids = [record.user_id for record, _ in records]
    message_ids = [record.message_id for record, _ in records]
    vectors = [vector.tolist() for _, vector in records]

    insert_result = collection.insert([user_ids, message_ids, vectors])
    collection.flush()
    logger.bind(inserted=insert_result.insert_count).info("milvus_embeddings_upserted")
    return insert_result.insert_count


__all__ = ["upsert_embeddings"]
