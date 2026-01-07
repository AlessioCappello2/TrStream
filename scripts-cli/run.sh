#!/bin/bash
set -e

# Run all the essential services
echo "Launching the essential services to allow the pipeline to work"
docker compose up -d kafka-config kafka minio-config minio producer consumer