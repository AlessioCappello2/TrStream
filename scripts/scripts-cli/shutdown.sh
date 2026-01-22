#!/bin/bash
set -e

# Shutdown and remove the containers whose services are defined within the compose file
echo "Shutting down all the TrStream services..."
docker compose down