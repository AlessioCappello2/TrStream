# TrStream
*A distributed real-time transaction processing pipeline*

## About
TrStream is a distributed data pipeline designed to simulate and process high-throughput financial transaction streams in real time.

The project explores the architectural patterns behind modern fintech and data engineering systems, including:
- event-driven ingestion
- scalable stream processing
- lakehouse-style storage
- analytical querying

It was developed as a personal project to apply concepts from [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) (Martin Kleppmann) in a realistic and reproducible environment.

## Motivation

Modern financial platforms ingest millions of events per day and must:
- process data reliably
- retain raw events for auditing
- optimize data layout for analytics
- remain horizontally scalable

TrStream models this workflow locally using open-source technologies, focusing on **system design** and data flow.

## Overview
At a high level, the pipeline:
1. Generates transaction events from multiple producers
2. Streams the events through Kafka
3. Stores raw data in a data lake (Parquet on S3-compatible storage)
4. Reorganizes and compacts data for analytical workloads
5. Exposes a SQL query layer on optimized data

The system is fully containerized and can be scaled horizontally via Docker Compose.

## Architecture
![Architecture](images/Architecture.svg)

## Data flow
- Producers simulate transaction events with configurable values, distribution and event rates
- Kafka ensures decoupling, buffering and fault tolerance
- Consumers persist immutable raw data in Parquet format
- The partitioner organizes data by date and transaction type
- The compacter merges small files to improve query performance
- The query layer enables SQL queries directly on optimized Parquet data
- A Streamlit SQL editor is provided to conveniently query data

## Key features
- Real-time ingestion with Kafka-based buffering and backpressure
- Horizontal scalable producers and consumers via Kafka partitioning
- End-to-end observability of message flow through Kafka UI
- Immutable Parquet storage designed for downstream analytics and auditing
- Explicit data lifecycle stages: ingestion, partitioning and compaction
- SQL querying on lake data via DuckDB (Athena-like experience)
- Lightweight SQL editor implemented with Streamlit
- Fully containerized local environment with Docker Compose orchestration and explicit health checks

## Tech stack
| Component     | Technology                   |
|---------------|------------------------------|
| Messaging     | Kafka (Bitnami legacy image) |
| Storage       | MinIO (S3-compatible)        |
| Processing    | Python, PyArrow, Boto3       |
| Orchestration | Docker Compose               |
| Monitoring    | Kafka UI (Provectus Labs)    |
| Querying      | DuckDB, FastAPI              |
| Visualization | Streamlit                    |

## Running locally
Helper scripts are provided to simplify common workflows.

- Build all images:
```bash 
bash scripts-cli/build.sh 
```

- Start the core pipeline:
```bash 
bash scripts-cli/run.sh 
```

- Start all services (including query layer and dashboards):
```bash 
bash scripts-cli/run_all.sh 
```

- Optional scaling
```bash 
bash scripts-cli/run.sh producer=3 consumer=4 
```

See [scripts-cli/README.md](https://github.com/AlessioCappello2/TrStream/tree/main/scripts-cli) for more details.

## Access Points
- Kafka UI: http://localhost:8080
- MinIO Console: http://localhost:9001
- SQL Querier API: http://localhost:8000
- Streamlit Editor: http://localhost:8501

## Roadmap
Planned extensions focus on real-world relevance rather than feature completeness:
- Integration with real transaction APIs (e.g. Stripe, Revolut)
- Metrics and observability improvements
- Fraud detection and analytics use cases