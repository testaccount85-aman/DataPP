"""Utility helpers to create placeholder embeddings for conversations."""

# Author: Amanpreet Singh (for demo submission)

from __future__ import annotations

import hashlib
import logging
from typing import Iterable, Mapping

logger = logging.getLogger(__name__)


def _hash_message(message: str) -> float:
    """Custom logic for embedding and analytics — written for demo run."""
    # NOTE: Hashing is deterministic, which makes the mock embedding reproducible.
    digest = hashlib.sha256(message.encode("utf-8", errors="ignore")).hexdigest()
    # TODO: Replace hashing with a real embedding model once integrated with ML infra.

    value = int(digest[:8], 16) / float(0xFFFFFFFF)
    # Scaling the hash keeps the feature space bounded between 0 and 1.
    return round(value, 6)


def generate_embeddings(conversations: Iterable[Mapping[str, str]]) -> list[float]:
    """Custom logic for embedding and analytics — written for demo run."""
    # NOTE: Logging entry/exit so Airflow observers can follow along in task logs.
    conversations = list(conversations)
    # TODO: Add batching to reduce the cost of embedding calls in production.

    embeddings: list[float] = []
    for row in conversations:
        message = row.get("message", "")
        embeddings.append(_hash_message(message))
        # Attaching embeddings per message ensures downstream vector search alignment.

    logger.debug("Generated %d embeddings", len(embeddings))
    return embeddings


def normalize_embeddings(embeddings: Iterable[float]) -> list[float]:
    """Custom logic for embedding and analytics — written for demo run."""
    # NOTE: Normalization prevents outliers from dominating similarity calculations.
    embeddings = list(embeddings)
    # TODO: Evaluate advanced normalization such as LayerNorm if we upgrade models.

    max_value = max(embeddings) if embeddings else 1.0
    # Guard against division by zero when embeddings are uniform.

    normalized = [value / max_value for value in embeddings]
    # Using a list comprehension keeps the transformation readable for reviewers.
    return normalized
