#!/bin/bash
set -e

# Build base image
echo "Building base image..."
docker build -f ./docker/Dockerfile.base . -t trstream-base:latest

# Build services (microservices defined in docker-compose.yml)
echo "Building services..."
docker compose build