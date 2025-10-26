"""SentenceTransformer embedder wrapper."""
from __future__ import annotations

from functools import lru_cache
from typing import Iterable, List

import numpy as np
from sentence_transformers import SentenceTransformer

from src.utils import config
from src.utils.logging import logger


@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    logger.bind(model=config.EMBEDDER_MODEL).info("loading_embedder")
    return SentenceTransformer(config.EMBEDDER_MODEL)


def encode_texts(texts: Iterable[str]) -> np.ndarray:
    texts_list: List[str] = list(texts)
    if not texts_list:
        raise ValueError("No texts provided for embedding")

    model = get_model()
    vectors = model.encode(texts_list, convert_to_numpy=True, normalize_embeddings=True)
    if vectors.size == 0:
        logger.error("empty_embeddings", count=len(texts_list))
        raise ValueError("Embedding model returned empty vectors")

    # Validate vector norms
    zero_vectors = np.where(np.linalg.norm(vectors, axis=1) == 0)[0]
    if zero_vectors.size:
        logger.warning("zero_length_embeddings", indices=zero_vectors.tolist())
        raise ValueError("Zero length embeddings detected")

    return vectors


__all__ = ["get_model", "encode_texts"]
