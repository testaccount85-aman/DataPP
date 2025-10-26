"""Airflow DAG orchestrating the personalization data pipeline."""

# Author: Amanpreet Singh (for demo submission)

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipeline.tasks.compute_metrics import compute_metrics, extract_top_users
from src.pipeline.tasks.generate_embeddings import generate_embeddings, normalize_embeddings

import logging

logger = logging.getLogger(__name__)

DATA_PATH = Path("data/conversations.csv")


def _load_conversations(**_: Any) -> list[dict[str, str]]:
    """Custom logic for embedding and analytics — written for demo run."""
    logger.info("Starting task: load conversations from CSV")
    # NOTE: Using a local CSV keeps the demo self-contained for reviewers.
    # TODO: Replace with a database connector when moving beyond the prototype.

    rows: list[dict[str, str]] = []
    with DATA_PATH.open("r", encoding="utf-8") as handle:
        headers = handle.readline().strip().split(",")
        # Parsing headers manually to avoid bringing in heavy dependencies for the demo.
        for line in handle:
            values = [value.strip() for value in line.strip().split(",")]
            rows.append(dict(zip(headers, values)))
            # Building dictionaries allows downstream tasks to reference keys intuitively.
    logger.info("Completed task: load conversations from CSV")
    return rows


def _generate_embeddings(**context: Any) -> list[float]:
    """Custom logic for embedding and analytics — written for demo run."""
    logger.info("Starting task: generate embeddings")
    # NOTE: Pulling upstream XCom into Python keeps the pipeline approachable in the demo.
    conversations = context["ti"].xcom_pull(task_ids="load_conversations")
    # TODO: Wire this step to a feature store when we move beyond CSV inputs.

    embeddings = generate_embeddings(conversations)
    normalized = normalize_embeddings(embeddings)
    # Normalization ensures the vector store receives consistent magnitudes for scoring.
    logger.info("Completed task: generate embeddings")
    return normalized


def _compute_metrics(**context: Any) -> dict[str, float]:
    """Custom logic for embedding and analytics — written for demo run."""
    logger.info("Starting task: compute metrics")
    # NOTE: Metrics focus on storytelling rather than statistical rigor for now.
    conversations = context["ti"].xcom_pull(task_ids="load_conversations")
    # TODO: Replace with a call to the analytics warehouse once available.

    metrics = compute_metrics(conversations)
    logger.info("Completed task: compute metrics")
    return metrics


def _summarize_users(**context: Any) -> list[str]:
    """Custom logic for embedding and analytics — written for demo run."""
    logger.info("Starting task: summarize top users")
    # NOTE: Reporting top users demonstrates how personalization segments might emerge.
    conversations = context["ti"].xcom_pull(task_ids="load_conversations")
    # TODO: Parameterize the `top_n` to explore more audience cohorts dynamically.

    top_users = extract_top_users(conversations)
    logger.info("Completed task: summarize top users")
    return top_users


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
}

with DAG(
    dag_id="conversation_pipeline_v2",
    description="Demo Airflow pipeline for conversational personalization",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["demo", "personalization"],
) as dag:
    load_conversations = PythonOperator(
        task_id="load_conversations",
        python_callable=_load_conversations,
    )

    generate_embeddings_task = PythonOperator(
        task_id="generate_embeddings",
        python_callable=_generate_embeddings,
        provide_context=True,
    )

    compute_metrics_task = PythonOperator(
        task_id="compute_metrics",
        python_callable=_compute_metrics,
        provide_context=True,
    )

    summarize_users_task = PythonOperator(
        task_id="summarize_users",
        python_callable=_summarize_users,
        provide_context=True,
    )

    load_conversations >> [generate_embeddings_task, compute_metrics_task, summarize_users_task]
