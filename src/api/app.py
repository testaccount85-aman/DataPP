"""FastAPI application exposing health and recommendation endpoints."""
from __future__ import annotations

import json
from typing import Any, Dict, List

import numpy as np
from fastapi import FastAPI, HTTPException
from pymilvus import CollectionNotExistException

from src.db.connections import (
    collection,
    mongo_db,
    neo4j_driver,
    redis_client,
    sqlite_conn,
)
from src.utils import config
from src.utils.logging import logger

app = FastAPI(title="Hybrid Recommendations API", version="0.1.0")


def _milvus_user_embeddings(user_id: str) -> list[list[float]]:
    try:
        collection.load()
    except CollectionNotExistException as exc:
        logger.error("milvus_collection_missing", error=str(exc))
        raise HTTPException(status_code=500, detail="Vector store unavailable") from exc

    results = collection.query(
        expr=f'user_id == "{user_id}"',
        output_fields=["embedding", "message_id"],
        consistency_level="Bounded",
    )
    return [row["embedding"] for row in results if row.get("embedding")]


def _compute_profile_vector(embeddings: list[list[float]]) -> np.ndarray:
    if not embeddings:
        raise HTTPException(status_code=404, detail="User has no embeddings")
    stacked = np.array(embeddings)
    return stacked.mean(axis=0)


def _search_similar_users(vector: np.ndarray, limit: int) -> list[str]:
    search_params = {"metric_type": config.MILVUS_INDEX_METRIC, "params": {"ef": 128}}
    results = collection.search(
        data=[vector.tolist()],
        anns_field="embedding",
        param=search_params,
        limit=limit * 5,
        output_fields=["user_id"],
    )
    similar: Dict[str, float] = {}
    for hits in results:
        for hit in hits:
            uid = hit.entity.get("user_id")
            if not uid:
                continue
            score = float(hit.distance)
            similar[uid] = max(similar.get(uid, score), score)
    ordered = [uid for uid, _ in sorted(similar.items(), key=lambda item: item[1], reverse=True)]
    return ordered[:limit]


def _campaigns_for_users(user_ids: list[str]) -> list[str]:
    if not user_ids:
        return []
    query = """
    UNWIND $uids AS uid
    MATCH (:User {id: uid})-[:INTERACTED_WITH]->(c:Campaign)
    RETURN DISTINCT c.id AS campaign_id
    """
    with neo4j_driver.session() as session:
        result = session.run(query, uids=user_ids)
        return [record["campaign_id"] for record in result]


def _rank_campaigns(campaign_ids: list[str], limit: int) -> list[dict[str, Any]]:
    if not campaign_ids:
        return []
    cursor = sqlite_conn.cursor()
    ranked: List[dict[str, Any]] = []
    for cid in campaign_ids:
        cursor.execute(
            "SELECT engagements FROM campaign_stats WHERE campaign_id = ?",
            (cid,),
        )
        row = cursor.fetchone()
        ranked.append({"campaign_id": cid, "score": int(row[0]) if row else 0})
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:limit]


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        mongo_db.command("ping")
        redis_client.ping()
        with neo4j_driver.session() as session:
            session.run("RETURN 1")
        sqlite_conn.execute("SELECT 1")
        collection.num_entities  # Trigger lazy connection
        return {"status": "ok"}
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("healthcheck_failed", error=str(exc))
        raise HTTPException(status_code=503, detail="Service unavailable") from exc


@app.get("/recommendations/{user_id}")
def recommendations(user_id: str, k: int = 5) -> dict[str, Any]:
    if k <= 0:
        raise HTTPException(status_code=400, detail="k must be positive")

    cache_key = f"rec:{user_id}:{k}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    embeddings = _milvus_user_embeddings(user_id)
    profile_vector = _compute_profile_vector(embeddings)
    similar_users = [uid for uid in _search_similar_users(profile_vector, limit=k + 5) if uid != user_id]
    similar_users = similar_users[:k]

    campaigns = _campaigns_for_users(similar_users or [user_id])
    ranked = _rank_campaigns(campaigns, limit=k)

    response = {
        "user_id": user_id,
        "recommended_campaigns": ranked,
        "similar_users": similar_users,
    }
    redis_client.setex(cache_key, config.REDIS_TTL_SECONDS, json.dumps(response))
    return response
