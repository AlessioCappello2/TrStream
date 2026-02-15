#!/bin/bash
set -e

# Usage:
# ./run.sh [generator=N] [worker=M]
# Example:
# ./run.sh generator=3 worker=2

GENERATOR_COUNT=1
WORKER_COUNT=1
SCALE_ARGS=()

# Parse arguments
for arg in "$@"; do 
    case $arg in 
        generator=*)
            GENERATOR_COUNT="${arg#generator=}"
            SCALE_ARGS+=(--scale "revolut-generator=$GENERATOR_COUNT")
            ;;
        worker=*)
            WORKER_COUNT="${arg#worker=}"
            SCALE_ARGS+=(--scale "revolut-worker=$WORKER_COUNT")
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: bash run.sh [generator=N] [worker=M]"
            exit 1
            ;;
    esac
done

# Run all the essential Revolut services
echo "Launching the Revolut pipeline services (generator=$GENERATOR_COUNT, worker=$WORKER_COUNT)"

docker compose up -d \
    kafka \
    revolut-generator \
    revolut-worker \
    "${SCALE_ARGS[@]}"