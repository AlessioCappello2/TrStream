# TrStream
*A distributed real-time transaction processing pipeline*

## About
TrStream is a distributed data pipeline designed to simulate and process high-throughput financial transaction streams in real time.

The project explores the architectural patterns behind modern fintech and data engineering systems, including:
- Event-driven ingestion
- Scalable stream processing
- Lakehouse-style storage
- Analytical querying

It was developed as a personal project to apply concepts from [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) (Martin Kleppmann) with a strong focus on **system design** and data flow.

## Motivation

Modern financial platforms ingest millions of events per day and must:
- Reliably process asynchronous events
- Retain raw events for auditing and replay
- Progressively optimize data layout for analytics
- Remain horizontally scalable and loosely coupled

TrStream models this workflow locally using open-source technologies, emphasizing clear data stages and realistic ingestion patterns.

## Overview
At a high level, the pipeline:
1. Ingests transaction events from external systems (e.g. Stripe webhooks) and internal producers
2. Buffers and distributes events via Kafka
3. Persists immutable raw events in a data lake (Parquet on S3-compatible storage)
4. Processes and reorganizes data into optimized analytical layouts
5. Exposes a SQL query layer on analytics-ready data

All components are containerized and orchestrated with Docker Compose, allowing horizontal scaling of producers and consumers.

## Architecture
![Architecture](images/Architecture_v2.1.svg)

## Data flow
- **Event sources**
    - External providers (e.g. Stripe) deliver events through a webhook service
    - Internal producers simulate transaction streams with configurable rates and distributions
- **Kafka ingestion**
    - All events are published to Kafka topics
    - Kafka provides buffering, ordering, partitioning and backpressure handling
    - Kafka UI exposes end-to-end observability of topics and consumers
- **Consumers and storage**
    - Kafka consumers persist events to MinIO (S3-compatible storage)
    - Data flows through explicit lifecycle stages:
        - **Raw**: immutable, append-only Parquet files mirroring incoming events
        - **Processed**: cleaned and normalized data derived from raw events
        - **Analytics**: compacted and query-optimized Parquet files
- **Processing and compaction**
    - A processor transforms raw data into cleaned and normalized datasets
    - A compacter merges small files and optimizes layout for analytical queries
    - Both services are schema- and source-aware, ensuring consistent and query-ready outputs
- **Query and visualization**
    - A query service exposes SQL access (DuckDB-based) over analytics data
    - A Streamlit UI provides an interactive SQL editor for data exploration

## Key features
- **Event-driven ingestion** via webhooks and producers
- **Kafka-based buffering** with horizontal scalability through partitioning
- Clear separation of **data lifecycle stages**: raw → processed → analytics
- **Immutable Parquet storage** for auditing and replay
- **File compaction** and **layout optimization** for analytical workloads
- **SQL querying** on object storage (Athena-like experience)
- **Lightweight SQL editor** implemented with Streamlit
- **Fully containerized local environment** with Docker Compose orchestration and explicit health checks

## Tech stack
| Component     | Technology                   |
|---------------|------------------------------|
| Ingestion     | Kafka, Webhooks              |
| Messaging     | Kafka (Bitnami legacy image) |
| Storage       | MinIO (S3-compatible)        |
| Processing    | Python, PyArrow, Boto3       |
| Query engine  | DuckDB                       |
| API layer     | FastAPI                      |
| Visualization | Streamlit                    |
| Monitoring    | Kafka UI (Provectus Labs)    |
| Orchestration | Docker Compose               |

## Running locally
Helper scripts are provided to simplify common workflows.

- Build all images:
```bash 
bash scripts/scripts-cli/build.sh 
```

- Start the core pipeline:
```bash 
bash scripts/scripts-cli/run.sh 
```

- Start all services (including query layer and dashboards):
```bash 
bash scripts/scripts-cli/run_all.sh 
```

- Optional scaling
```bash 
bash scripts/scripts-cli/run.sh producer=3 consumer=4 
```

See [scripts/scripts-cli/README.md](https://github.com/AlessioCappello2/TrStream/tree/main/scripts/scripts-cli) for more details.

## Access Points
- Stripe Webhook: http://localhost:8100
- Kafka UI: http://localhost:8080
- MinIO Console: http://localhost:9001
- SQL Querier API: http://localhost:8000
- Streamlit Editor: http://localhost:8501

## Roadmap
Planned extensions focus on real-world relevance rather than feature completeness:
- Integration with Revolut API
- Metrics and observability improvements
- Job scheduling through Airflow