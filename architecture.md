# Architecture Overview

## Batch Layer

1. **Ingestion** – Airflow reads `data/conversations.csv`, validates payloads with `MessageRecord`, and persists canonical message documents to MongoDB.
2. **Embedding** – Messages are vectorized with the Sentence Transformer model specified in `EMBEDDER_MODEL`. Embedding health checks guard against empty or zero-norm vectors.
3. **Persistence** – MongoDB stores text and metadata, Milvus stores embeddings with HNSW indexing, and Neo4j maintains user→campaign interaction edges. The DAG only passes file references through XCom to keep payloads lightweight.
4. **Analytics** – Engagement events are appended into SQLite. Aggregations refresh `campaign_stats` for downstream ranking.

## Serving Layer

- **FastAPI** builds a user profile vector from Milvus, performs similarity search, traverses Neo4j for connected campaigns, ranks them with SQLite engagement counts, and caches results in Redis for 60 seconds.
- **Health Endpoint** checks MongoDB, Milvus, Neo4j, Redis, and SQLite readiness.

## Observability

- **Structured logging** via Loguru emits JSON suitable for ingestion into log pipelines.
- **Monitoring utilities** in `src/utils/monitoring.py` provide anomaly detection for missing relationships and empty embeddings.
- **Retry semantics** – Airflow tasks use PythonOperators with retries and timeouts; Milvus connections auto-create indexes on startup.

## Security & Config

- Secrets and endpoints are configured through environment variables (`src/utils/config.py`) with Docker-friendly defaults.
- Docker Compose constrains services to the local network and persists data in named volumes for repeatable runs.

