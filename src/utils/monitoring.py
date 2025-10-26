"""Monitoring helpers for instrumentation and anomaly detection."""
from __future__ import annotations

import functools
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Iterable, ParamSpec, TypeVar

from src.utils.logging import logger

T = TypeVar("T")
P = ParamSpec("P")


@dataclass(slots=True)
class TaskMetrics:
    latency_ms: float
    processed: int
    anomalies: Counter[str]

    def to_log(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "latency_ms": round(self.latency_ms, 2),
            "processed": self.processed,
        }
        if self.anomalies:
            payload["anomalies"] = dict(self.anomalies)
        return payload


def timed(name: str) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to measure execution time and log around functions."""

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                elapsed = (time.perf_counter() - start) * 1000
                logger.bind(metric="latency", task=name).info("task_completed", latency_ms=elapsed)

        return wrapper

    return decorator


def count_records(items: Iterable[Any]) -> int:
    return sum(1 for _ in items)


def detect_empty_embeddings(vectors: Iterable[Iterable[float]]) -> int:
    return sum(1 for vector in vectors if not any(vector))


def detect_missing_relationships(records: Iterable[Any]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for record in records:
        if not getattr(record, "user_id", None):
            counter["missing_user_id"] += 1
        if not getattr(record, "campaign_id", None):
            counter["missing_campaign_id"] += 1
    return counter


def metrics_summary(latency_ms: float, processed: int, anomalies: Counter[str]) -> TaskMetrics:
    return TaskMetrics(latency_ms=latency_ms, processed=processed, anomalies=anomalies)


__all__ = [
    "timed",
    "count_records",
    "detect_empty_embeddings",
    "detect_missing_relationships",
    "metrics_summary",
    "TaskMetrics",
]
