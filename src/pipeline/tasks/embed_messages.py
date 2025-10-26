"""Embed ingested messages using the shared embedder."""
from __future__ import annotations

from typing import Iterable, List

import numpy as np

from src.db.schemas import MessageRecord
from src.models.embedder import encode_texts
from src.utils.logging import logger
from src.utils.monitoring import detect_empty_embeddings


def embed_records(records: Iterable[MessageRecord]) -> List[tuple[MessageRecord, np.ndarray]]:
    records_list = list(records)
    if not records_list:
        return []

    vectors = encode_texts([record.message for record in records_list])
    empty = detect_empty_embeddings(vectors)
    if empty:
        logger.warning("empty_embedding_vectors_detected", count=empty)

    paired: List[tuple[MessageRecord, np.ndarray]] = [
        (records_list[idx], vectors[idx]) for idx in range(len(records_list))
    ]
    logger.bind(processed=len(paired)).info("messages_embedded")
    return paired


__all__ = ["embed_records"]
