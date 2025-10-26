# Personalization Multi-DB Prototype

This repository contains a production-style prototype for an AI-driven marketing personalization platform. It ingests omnichannel conversation data, generates sentence-transformer embeddings, distributes the resulting features across multiple databases, and exposes a hybrid retrieval API that combines vector similarity, graph traversal, and aggregate engagement metrics.

## Features

- **Multi-database topology** featuring MongoDB, Milvus, Neo4j, Redis, and SQLite.
- **Batch orchestration** with Apache Airflow covering ingestion → embedding → persistence → analytics aggregation.
- **Hybrid retrieval API** built with FastAPI, providing cached recommendations that blend vector similarity, graph relationships, and engagement frequency.
- **Observability** via structured Loguru logging, anomaly detection helpers, and health checks spanning all dependencies.
- **Containerized deployment** with Docker Compose bringing up the complete stack locally.

## Quickstart

1. **Generate sample data** (300 synthetic conversation rows plus seed catalogs):
   ```bash
   python -m src.pipeline.generators.make_sample_data
   ```

2. **Launch services**:
   ```bash
   docker compose up -d
   ```

3. **Trigger the Airflow DAG**:
   - Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (credentials `admin` / `admin`).
   - Locate the `conversation_pipeline` DAG and trigger a manual run.

4. **Query the API** once the DAG completes:
   ```bash
   curl http://localhost:8000/health
   curl "http://localhost:8000/recommendations/user_001?k=5"
   ```

## Architecture Overview

- `src/pipeline/dags/conversation_pipeline_dag.py` orchestrates ingestion, embedding, and persistence across MongoDB, Milvus, and Neo4j before aggregating into SQLite.
- `src/api/app.py` performs hybrid retrieval with Redis caching and multi-store coordination.
- `src/db/connections.py` centralizes database connections and ensures Milvus indexes exist.
- `src/utils` contains configuration, logging, and monitoring helpers.

For detailed diagrams and rationale, see [`architecture_diagram.md`](architecture_diagram.md) and [`architecture.md`](architecture.md).

## Testing & Validation

- **Health check**: `/health` verifies MongoDB, Milvus, Neo4j, Redis, and SQLite availability.
- **Anomaly detection**: missing identifiers and empty embeddings are surfaced in logs.
- **Caching**: Redis caches recommendation responses for the TTL defined by `REDIS_TTL_SECONDS`.

## Troubleshooting

| Symptom | Resolution |
| --- | --- |
| Milvus healthcheck fails | Ensure Docker Desktop has >4 CPUs & >8GB RAM allocated. Restart stack after increasing resources. |
| FastAPI container exits | Check `docker compose logs fastapi` for missing dependencies or migration errors. |
| Airflow cannot import DAG | Confirm the repository path is mounted (default `./src/pipeline/dags:/opt/airflow/dags`). |

## Ports

| Service | Port |
| --- | --- |
| FastAPI | 8000 |
| Airflow Webserver | 8080 |
| MongoDB | 27017 |
| Redis | 6379 |
| Neo4j Browser | 7474 |
| Neo4j Bolt | 7687 |
| Milvus | 19530 |

## Next Steps

- Integrate real-time streaming ingestion via Kafka or Pulsar.
- Replace SQLite with a cloud analytics warehouse (e.g., BigQuery or Snowflake).
- Expand monitoring with OpenTelemetry traces and Prometheus metrics.

