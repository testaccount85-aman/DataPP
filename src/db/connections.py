"""Centralized database connection management."""
from __future__ import annotations

import contextlib

from neo4j import GraphDatabase, basic_auth
from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections
from pymilvus.exceptions import MilvusException
from pymongo import MongoClient
from redis import Redis
import sqlite3

from src.db.init_sqlite import initialize_sqlite
from src.utils import config
from src.utils.logging import logger


mongo_client = MongoClient(config.MONGO_URI)
mongo_db = mongo_client[config.MONGO_DB]

redis_client = Redis(
    host=config.REDIS_HOST,
    port=config.REDIS_PORT,
    db=config.REDIS_DB,
    decode_responses=True,
    socket_connect_timeout=5,
)

sqlite_conn = sqlite3.connect(config.SQLITE_PATH, check_same_thread=False)
initialize_sqlite(sqlite_conn)

neo4j_driver = GraphDatabase.driver(
    config.NEO4J_URI, auth=basic_auth(config.NEO4J_USER, config.NEO4J_PASSWORD)
)


def _ensure_milvus_connection() -> None:
    connections.connect(alias="default", host=config.MILVUS_HOST, port=str(config.MILVUS_PORT))


def _create_collection_if_missing() -> Collection:
    try:
        _ensure_milvus_connection()
        existing = Collection(name=config.MILVUS_COLLECTION, using="default")
        logger.bind(db="milvus", collection=config.MILVUS_COLLECTION).info("milvus_collection_loaded")
        return existing
    except MilvusException:
        vector_dim = config.get_vector_dim()
        fields = [
            FieldSchema(name="user_id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="message_id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=vector_dim),
        ]
        schema = CollectionSchema(fields, description="Message embedding store")
        collection = Collection(
            name=config.MILVUS_COLLECTION,
            schema=schema,
            using="default",
            shards_num=2,
        )
        logger.bind(db="milvus", collection=config.MILVUS_COLLECTION).info("milvus_collection_created")
        return collection


collection = _create_collection_if_missing()

with contextlib.suppress(MilvusException):
    index_params = {
        "index_type": "HNSW",
        "metric_type": config.MILVUS_INDEX_METRIC,
        "params": {"M": 16, "efConstruction": 200},
    }
    collection.create_index(field_name="embedding", index_params=index_params)
    logger.bind(index="HNSW").info("milvus_index_ready")

with contextlib.suppress(MilvusException):
    collection.load()


__all__ = [
    "mongo_client",
    "mongo_db",
    "redis_client",
    "sqlite_conn",
    "neo4j_driver",
    "collection",
]
