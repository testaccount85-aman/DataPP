# DataPP

Personalization pipeline demo connecting conversational analytics with vector search primitives. The repository includes Airflow orchestration assets, lightweight task utilities, and sample data for experimentation.

## Getting Started

1. Create a virtual environment with Python 3.10 or newer.
2. Install Airflow locally or use an Astronomer/Managed Airflow sandbox for quick validation.
3. Set the `PYTHONPATH` to include the repository root before triggering the DAG so the task modules resolve.

## Local DAG Execution

```bash
airflow dags trigger conversation_pipeline_v2
```

The DAG loads the mock CSV, produces hashed embeddings, calculates summary metrics, and emits a ranked list of active users for personalization experiments.

## Data Inputs

The `data/conversations.csv` file contains intentionally messy conversational snippets. Typos and emojis help validate that the text processing logic is resilient to real-world noise.

## Author Notes

Airflow was chosen for this demonstration because its scheduling semantics and task dependency visualization make it easy for stakeholders to reason about data freshness. Vector search here relies on deterministic hashing as a stand-in for real embeddings, highlighting how similarity lookup would work once connected to a production-grade model.

Code developed manually for demonstration, with selective automation for scaffolding.

Author: Amanpreet Singh (for demo submission)
