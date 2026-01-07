#!/bin/bash
set -e

# Run all the essential services + extra
echo "Launching the services to allow the pipeline to work and interact with it"
docker compose up -d kafka-config kafka kafka-ui minio-config minio producer consumer querier streamlit