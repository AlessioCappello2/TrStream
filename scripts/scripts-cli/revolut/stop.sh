#!/bin/bash
set -e

# Stop Revolut services
echo "Stopping Revolut services..."
docker compose stop revolut-worker revolut-generator