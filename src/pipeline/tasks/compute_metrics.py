"""Task utilities for computing aggregated analytics metrics."""

# Author: Amanpreet Singh (for demo submission)

from __future__ import annotations

import logging
from statistics import mean
from typing import Iterable, Mapping

logger = logging.getLogger(__name__)


def compute_metrics(conversations: Iterable[Mapping[str, str]]) -> Mapping[str, float]:
    """Custom logic for embedding and analytics — written for demo run."""
    # NOTE: Using logging to emphasize the observability story for stakeholders.
    conversations = list(conversations)  # type: ignore[assignment]
    logger.debug("Starting mock metric computation for %d conversations", len(conversations))
    # TODO: Replace mock computations with production-grade analytics queries.
    # We memoize the collection locally so repeated scans remain deterministic.
    total_messages = sum(len(item.get("message", "")) for item in conversations)
    # Counting characters as a rough proxy for engagement during the demo.

    avg_length = mean(
        (len(item.get("message", "")) or 1) for item in conversations
    ) if conversations else 0.0
    # Calculating a fallback average to avoid ZeroDivisionError in edge cases.

    metrics = {
        "total_messages": float(total_messages),
        "average_message_length": float(avg_length),
        "unique_users": float(len({item.get("user_id") for item in conversations})),
    }
    # Crafting the dictionary explicitly keeps the response predictable for the DAG.

    logger.debug("Mock metrics computed: %s", metrics)
    return metrics


def extract_top_users(conversations: Iterable[Mapping[str, str]], top_n: int = 3) -> list[str]:
    """Custom logic for embedding and analytics — written for demo run."""
    # NOTE: Ranking logic is intentionally simple to stay readable in the walkthrough.
    conversations = list(conversations)
    # TODO: Integrate warehouse-backed frequency tables instead of in-memory counts.

    counts: dict[str, int] = {}
    for row in conversations:
        user = row.get("user_id", "guest")
        counts[user] = counts.get(user, 0) + 1
        # Counting occurrences allows downstream personalization heuristics.

    sorted_users = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    # Sorting ensures determinism, which makes debugging Airflow tasks simpler.

    top_users = [user for user, _ in sorted_users[:top_n]]
    logger.debug("Identified top users: %s", top_users)
    return top_users
