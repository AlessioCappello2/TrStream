#!/bin/bash
set -e

# Usage:
# ./run_all.sh [producer=N] [consumer=M]
# Example:
# ./run_all.sh producer=3 consumer=4

PRODUCER_COUNT=1
CONSUMER_COUNT=1
SCALE_ARGS=()

# Parse arguments
for arg in "$@"; do 
    case $arg in 
        producer=*)
            PRODUCER_COUNT="${arg#producer=}"
            SCALE_ARGS+=(--scale "$arg")
            ;;
        consumer=*)
            CONSUMER_COUNT="${arg#consumer=}"
            SCALE_ARGS+=(--scale "$arg")
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: bash run_all.sh [producer=N] [consumer=M]"
            exit 1
            ;;
    esac
done

# Run all the essential services + extra
echo "Launching all the pipeline services (producer=$PRODUCER_COUNT, consumer=$CONSUMER_COUNT)"

docker compose up -d \
    kafka-config \
    kafka \
    kafka-ui \
    minio-config \
    minio \
    producer \
    consumer \
    querier \
    streamlit \
    "${SCALE_ARGS[@]}"