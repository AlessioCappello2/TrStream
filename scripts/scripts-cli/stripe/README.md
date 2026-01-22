# Stripe Integration

This integration simulates and processes **Stripe transactions in real time**. It provides a fully containerized **webhook service** and **event generator**, along with a host-based **Stripe CLI event forwarder**, allowing you to test ingestion and processing workflows locally.

## Overview
The Stripe stack consists of:
- **Webhook service**
    - Listens for incoming Stripe events and exposes them to the internal pipeline
    - Runs in a Docker container and includes a health check on port `8100`
- **Event generator**
    - Simulates Stripe transaction events for testing purposes
    - Publishes events to Kafka via the webhook
- **Stripe CLI event forwarder**
    - Uses the official Stripe CLI (run outside Docker) to forward real or test Stripe events to the webhook service

## Features
- **Event-driven ingestion**: simulated or real Stripe events are captured and published to Kafka topics.
- **Dockerized services**: webhook and generator run as containers for reproducibility.
- **Webhook health checks**: ensures the service is ready before events are forwarded.
- **Safe CLI integration**: Stripe CLI runs outside Docker to avoid embedding credentials, with automatic start/stop via helper scripts.

## Prerequisites
- Docker and Docker Compose
- [Stripe CLI](https://docs.stripe.com/stripe-cli) installed on the host system 
- Stripe API keys (test mode)

## Usage

### Start the stack
Run the start script:
```
bash start.sh
```

This script will:
1. Stop any previous CLI process
2. Launch the webhook and Kafka services via Docker Compose
3. Wait for the webhook to become ready
4. Start the Stripe CLI forwarding process
5. Start the event generator container
6. Persist the Stripe CLI in `.stripe.pid` for proper shutdown

### Stop the stack
Run the stop script
```
bash stop.sh
```

This script will:
1. Stop the generator container
2. Stop the webhook container
3. Kill the Stripe CLI forwarding process
4. Remove the `.stripe.pid` file

### Usage notes
- **Multiple runs**: the start script automatically kills any previous Stripe CLI process to prevent conflicts
- **Logs**: Stripe CLI output is redirected to `stripe.log`
- **Health monitoring**: the webhook container exposes `/health` for readiness checks
- **Extensibility**: you can increase the number of generators or adapt event types for testing more complex scenarios

## Access
- Webhook API: http://localhost:8100/webhook
- Kafka topics: configured in the main pipeline
- Event logs: `stripe.log`

## Notes
- Designed for **local development and testing only**.
- Credentials are **never stored in Docker**; the Stripe CLI must run on the host.
- Works with both simulated and real Stripe test events.