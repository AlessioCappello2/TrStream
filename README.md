# TrStream
*A distributed real-time transaction processing pipeline*

## About
TrStream is a real-time data pipeline for ingesting, processing and organizing financial transactions at scale. <br>
This project was developed as a personal initiative in my spare time to explore the design principles of modern data engineering pipelines in a financial context. <br>
I have been inspired by reading [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) by Martin Kleppmann.

## Overview
The pipeline ingests synthetic transactions from multiple Kafka producers and processes them via parallel consumers. Each transaction is sent through the broker and stored in Parquet format within a [MinIO](https://www.min.io/)-based data lake, enabling efficient querying and downstream analytics. The goal is to reproduce the architecture of a modern streaming pipeline in a local and reproducible setup.

## Architecture
              ┌────────────────────────────────────────┐
              │              Producers                 │
              │  Simulate live financial transactions  │
              └────────────────────────────────────────┘
                                   │
                                   ▼
                      ┌────────────────────────┐
                      │         Kafka          │
                      │  Message broker layer  │
                      │  (Topic: transactions) │
                      └────────────────────────┘
                                   │
                                   ▼
          ┌───────────────────────────────────────────────┐
          │                   Consumers                   │
          │     Read, validate, and buffer transactions   │
          │       Write raw data to MinIO as Parquet      │
          └───────────────────────────────────────────────┘
                                   │
                                   ▼
         ┌────────────────────────────────────────────────┐
         │                 MinIO (Data Lake)              │
         │  S3-compatible object storage for:             │
         │   • Raw data (raw-data/)                       │
         │   • Partitioned data (tb-transactions/)        │
         └────────────────────────────────────────────────┘
                                   │
               ┌───────────────────┴─────────────────────┐
               ▼                                         ▼
    ┌───────────────────────────┐          ┌────────────────────────────┐
    |        Partitioner        |          |      Compacter (WIP)       |
    | Reorganizes raw data into |          | Merges small Parquet files |
    | partitioned folders by    |          | and optimizes layout for   |
    | date + transaction type.  |          | analytical performance.    |
    └───────────────────────────┘          └────────────────────────────┘              
                │                                        │
                └────────────────────────────────────────┘             
                                   │
                                   ▼
                      ┌────────────────────────────┐
                      │       Query Layer (WIP)    │
                      │  DuckDB / Athena-like SQL  │
                      │ queries on optimized data. │
                      └────────────────────────────┘

Data flow:
- Kafka Producers simulate continuous transaction streams, generating fake transactions using [Faker](https://pypi.org/project/Faker/), where it is possible to force values and ranges.
- Kafka Consumers process and persist them as raw Parquet files in MinIO, under the ```raw-data``` bucket.
- Partitioner organizes data into logical folders, partitioning by date and transaction type (e.g. ```year=2025/month=10/day=27/transaction_type=CREDIT```).
- Compacter merges fragmented Parquet files to improve query efficiency and storage layout (in progress).
- Query Layer allows for SQL-based queries directly on partitioned and optimized Parquet data (in progress).

## Features
- Real-time ingestion and storage of thousands of transactions per minute
- Balanced workloads through Kafka topic partitioning by user and transaction type
- Reliable event delivery with manual offset commits and crash recovery
- Durable and query-ready Parquet storage for downstream analytics
- Orchestration and health checks implemented through Docker
- Full observability of Kafka topics through Kafka UI

## Tech stack
| Component     | Technology                   |
|---------------|------------------------------|
| Messaging     | Kafka (Bitnami legacy image) |
| Data Lake     | MinIO (S3-compatible)        |
| Processing    | Python, PyArrow, Boto3       |
| Orchestration | Docker Compose               |
| Monitoring    | Kafka UI (Provectus Labs)    |

Access:
- Kafka UI: http://localhost:8080
- MinIO UI: http://localhost:9001 (user: admin, password: admin12345)

## How to run locally
Open a new terminal within the root folder and launch the following commands:
- Build all services:
```
docker-compose build 
```
- Start Kafka, MinIO and the entire pipeline:
```
docker-compose up -d 
```
- Scale producers and consumers (optional):
```
docker-compose up -d --scale producer=3 --scale consumer=4
```

## Work in progress
- Implementation of a DuckDB-based or similar querying layer
- Jobs scheduling for partitioning and compaction

## Additional planned features
Once the infrastructure is ready, I want to focus on some real use cases:
- ML Fraud Detection
- Metrics collection and reporting
