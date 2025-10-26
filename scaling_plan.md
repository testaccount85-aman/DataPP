# Scaling Plan

## User Growth to 10M+
- **Partitioned analytics**: migrate from SQLite to a columnar warehouse (BigQuery/Snowflake) with partitioning on `campaign_id` and clustering on `user_id`.
- **Streaming ingestion**: introduce Kafka topics for real-time events and leverage Kafka Connect for MongoDB/Milvus sinks.
- **Sharded metadata**: run MongoDB as a sharded cluster with hashed shard keys on `user_id`.

## Vector Search < 100ms
- **HNSW tuning**: adjust `ef` search parameters dynamically based on SLA and traffic type; precompute user profile vectors to avoid per-request aggregation.
- **Index optimization**: consider IVF_PQ with product quantization for memory efficiency at scale.
- **Horizontal scaling**: deploy Milvus in distributed mode with dedicated query nodes and use read replicas close to API pods.

## Cost Efficiency
- **Autoscaling**: run API pods on Kubernetes with HPA tied to P99 latency; downscale overnight.
- **Tiered storage**: archive cold campaigns to object storage while keeping hot sets in Redis Cluster for low-latency retrieval.
- **Observation-driven**: integrate Prometheus + Grafana dashboards and leverage anomaly alerts to reduce overprovisioning.

