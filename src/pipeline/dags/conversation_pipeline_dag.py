"""Airflow DAG orchestrating the end-to-end conversation pipeline."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List

import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.db.schemas import MessageRecord
from src.pipeline.tasks.aggregate_analytics import aggregate
from src.pipeline.tasks.embed_messages import embed_records
from src.pipeline.tasks.ingest_messages import load_conversations
from src.pipeline.tasks.persist_graph import upsert_graph
from src.pipeline.tasks.persist_milvus import upsert_embeddings
from src.pipeline.tasks.persist_mongo import write_messages
from src.utils.logging import logger

DATA_PATH = Path("/opt/airflow/data/conversations.csv")


def _records_to_json(records: List[MessageRecord]) -> str:
    temp_file = NamedTemporaryFile("w", delete=False, suffix=".json")
    json.dump([record.dict() for record in records], temp_file)
    temp_file.flush()
    temp_file.close()
    return temp_file.name


def _pairs_to_json(pairs: list[tuple[MessageRecord, list[float]]]) -> str:
    temp_file = NamedTemporaryFile("w", delete=False, suffix=".json")
    payload = [
        {"record": pair[0].dict(), "embedding": pair[1]} for pair in pairs
    ]
    json.dump(payload, temp_file)
    temp_file.flush()
    temp_file.close()
    return temp_file.name


def _load_records(path: str) -> list[MessageRecord]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return [MessageRecord(**item) for item in data]


def _load_pairs(path: str) -> list[tuple[MessageRecord, list[float]]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return [(MessageRecord(**item["record"]), item["embedding"]) for item in data]


with DAG(
    dag_id="conversation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["personalization", "marketing"],
) as dag:

    def ingest_task(**context):
        records = load_conversations(DATA_PATH)
        path = _records_to_json(records)
        logger.bind(records=len(records)).info("dag_ingest_complete", path=path)
        context["ti"].xcom_push(key="records_path", value=path)
        return len(records)

    def embed_task(**context):
        path = context["ti"].xcom_pull(key="records_path")
        records = _load_records(path)
        pairs = embed_records(records)
        payload = [(record, vector.tolist()) for record, vector in pairs]
        temp_path = _pairs_to_json(payload)
        context["ti"].xcom_push(key="pairs_path", value=temp_path)
        logger.bind(pairs=len(payload)).info("dag_embed_complete", path=temp_path)
        return len(payload)

    def mongo_task(**context):
        path = context["ti"].xcom_pull(key="records_path")
        records = _load_records(path)
        return write_messages(records)

    def milvus_task(**context):
        path = context["ti"].xcom_pull(key="pairs_path")
        pairs = _load_pairs(path)
        formatted = [(record, np.array(vector)) for record, vector in pairs]
        return upsert_embeddings(formatted)

    def graph_task(**context):
        path = context["ti"].xcom_pull(key="records_path")
        records = _load_records(path)
        return upsert_graph(records)

    def aggregate_task(**context):
        path = context["ti"].xcom_pull(key="records_path")
        records = _load_records(path)
        return aggregate(records)

    ingest = PythonOperator(task_id="ingest", python_callable=ingest_task)
    embed = PythonOperator(task_id="embed", python_callable=embed_task)
    mongo = PythonOperator(task_id="persist_mongo", python_callable=mongo_task)
    milvus = PythonOperator(task_id="persist_milvus", python_callable=milvus_task)
    graph = PythonOperator(task_id="persist_graph", python_callable=graph_task)
    analytics = PythonOperator(task_id="aggregate", python_callable=aggregate_task)

    ingest >> embed >> [mongo, milvus, graph] >> analytics
