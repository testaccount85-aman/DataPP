"""Configuration helpers for environment driven settings."""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def _get_env(key: str, default: Any) -> Any:
    value = os.getenv(key)
    return value if value not in (None, "") else default


MONGO_URI: str = str(_get_env("MONGO_URI", "mongodb://mongo:27017"))
MONGO_DB: str = str(_get_env("MONGO_DB", "personalization"))
MONGO_COLLECTION: str = str(_get_env("MONGO_COLLECTION", "conversations"))

MILVUS_HOST: str = str(_get_env("MILVUS_HOST", "milvus"))
MILVUS_PORT: int = int(_get_env("MILVUS_PORT", 19530))
MILVUS_COLLECTION: str = str(_get_env("MILVUS_COLLECTION", "message_embeddings"))
MILVUS_INDEX_METRIC: str = str(_get_env("MILVUS_INDEX_METRIC", "IP"))

NEO4J_URI: str = str(_get_env("NEO4J_URI", "bolt://neo4j:7687"))
NEO4J_USER: str = str(_get_env("NEO4J_USER", "neo4j"))
NEO4J_PASSWORD: str = str(_get_env("NEO4J_PASSWORD", "password"))

REDIS_HOST: str = str(_get_env("REDIS_HOST", "redis"))
REDIS_PORT: int = int(_get_env("REDIS_PORT", 6379))
REDIS_DB: int = int(_get_env("REDIS_DB", 0))
REDIS_TTL_SECONDS: int = int(_get_env("REDIS_TTL_SECONDS", 60))

SQLITE_PATH: str = str(_get_env("SQLITE_PATH", "/data/analytics.db"))

EMBEDDER_MODEL: str = str(
    _get_env("EMBEDDER_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
)

VECTOR_DIM_FALLBACK: int = int(_get_env("VECTOR_DIM", 384))


@lru_cache(maxsize=1)
def get_vector_dim() -> int:
    """Derive vector dimension from the embedder lazily.

    Loading the model during import makes Airflow DAG parsing slow. To avoid
    that we use a lazy lookup that can be called at runtime when the model is
    already required. When the model cannot be loaded (e.g. optional runtime),
    fall back to the configured default dimension.
    """

    if os.getenv("SKIP_EMBEDDER_LOAD", "false").lower() == "true":
        return VECTOR_DIM_FALLBACK

    try:
        from src.models.embedder import get_model

        model = get_model()
        dimension = getattr(model, "get_sentence_embedding_dimension", None)
        if callable(dimension):
            dim_value = dimension()
            if isinstance(dim_value, int):
                return dim_value
        if isinstance(dimension, int):
            return dimension
    except Exception:
        pass
    return VECTOR_DIM_FALLBACK


__all__ = [
    "MONGO_URI",
    "MONGO_DB",
    "MONGO_COLLECTION",
    "MILVUS_HOST",
    "MILVUS_PORT",
    "MILVUS_COLLECTION",
    "MILVUS_INDEX_METRIC",
    "NEO4J_URI",
    "NEO4J_USER",
    "NEO4J_PASSWORD",
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_DB",
    "REDIS_TTL_SECONDS",
    "SQLITE_PATH",
    "EMBEDDER_MODEL",
    "VECTOR_DIM_FALLBACK",
    "get_vector_dim",
]
