# Stripe Integration

Real-time Stripe transaction ingestion for the TrStream pipeline. This integration captures Stripe webhook events and forwards them to Kafka for downstream processing.

## Table of Contents
- [Introduction](#introduction)
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Notes](#notes)
- [Access Points](#access-points)
- [Learn more](#learn-more)

## Introduction

This integration simulates and processes **Stripe transactions in real time**. It provides a fully containerized **webhook service** and **event generator**, along with a host-based **Stripe CLI event forwarder**, allowing you to test ingestion and processing workflows locally.

## Overview
The Stripe stack consists of:
- **Webhook service (Containerized)**
    - Receives Stripe webhook events via HTTP
    - Validates event signatures for security
    - Forwards validated events to Kafka
    - Exposes health check endpoint on port `8100`
- **Event generator (Containerized)**
    - Simulates Stripe transaction events for testing purposes
    - Sends events directly to the webhook service
- **Stripe CLI event forwarder (Host-based)**
    - Uses the official Stripe CLI (run outside Docker) to forward real or test Stripe events to the webhook service
    - Enables testing with actual Stripe Dashboard events

## Architecture
![Stripe Stack Architecture](../../../images/stripe/Architecture.svg)

## Prerequisites
* Docker and Docker Compose
* [Stripe CLI](https://docs.stripe.com/stripe-cli) installed on the host system 
* Stripe API keys (test mode):
    - `STRIPE_SECRET_KEY` - for the generator
    - `STRIPE_WEBHOOK_SECRET` - for the webhook signature verification

## Setup
### 1. Login to Stripe CLI
Open a terminal and authenticate:
```bash
stripe login
```
This opens your browser to authorize the CLI with your Stripe account.

### 2. Get your Webhook Secret
Start the Stripe CLI listener to obtain your webhook signing secret:
```bash
stripe listen --forward-to http://localhost:8100/webhook
```

The CLI will output something like:
```
> Ready! Your webhook signing secret is whsec_1234567890abcdef (^C to quit)
```

**Copy** the **'whsec_xxxxxx'** value, it is needed in the next step.

**NOTE**: The Stripe CLI reuses the same webhook secret when you're logged into the same account. This secret persists across CLI restarts, so you only need to retrieve it once during initial setup.

### 3. Configure Environment Variables
Navigate to `config/env/integrations/stripe` and remove the `.example` extension from the files. 

Edit the files and, in particular, fill in your credentials:
- `STRIPE_SECRET_KEY=sk_test_xxxxxx` (from Stripe Dashboard)
- `STRIPE_WEBHOOK_SECRET=whsec_xxxxxx` (from stripe listen command)

## Usage
All commands should be run from the `scripts/scripts-cli/stripe` directory.

### Start the stack
Run the start script:
```bash
start.sh
```

This script will:
1. Stop any previous CLI process
2. Launch the webhook and Kafka services via Docker Compose
3. Wait for the webhook to become ready through health check
4. Start the Stripe CLI forwarding process
5. Start the event generator container
6. Persist the Stripe CLI in `.stripe.pid` for proper shutdown

### Stop the stack
Run the stop script
```bash
stop.sh
```

This script will:
1. Stop the generator container
2. Stop the webhook container
3. Kill the Stripe CLI forwarding process
4. Remove the `.stripe.pid` file

## Monitoring

### Check webhook health
Verify the webhook service is running:
```bash
curl http://localhost:8100/health
```
**Expected response:**
```json
{
    "status":"ok",
    "in_flight":0,
    "service":"stripe-webhook"
}
```

### View Webhook logs 
```bash
docker compose logs -f stripe-webhook
```

**Look for:**
- `Event evt_abcd1234efgh5678 forwarded to Kafka`
- `"POST /webhook HTTP/1.1" 200 OK`

### View Stripe CLI logs
The Stripe CLI output is saved to `stripe.log` within this folder.
```bash
cat stripe.log
```

**Look for:**
```
<--  [200] POST http://localhost:8100/webhook 
--> payment_intent.succeeded
```

### Verify events in Kafka
Using Kafka UI:
```bash
open http://localhost:8080
```

Navigate to Topics -> `transactions-stripe` -> Messages

A message should appear like this:
```json
{
	"source": "stripe",
	"received_at": "2026-02-15T14:12:16.432",
	"payload": {
		"id": "evt_abcd1234efgh5678",
		"type": "payment_intent.succeeded",
		"data": {
            ... 
        }
    "api_version": "2025-12-15.clover",
    "created": 1771164735,
    "livemode": false
    }
}
```

## Notes

### Development vs Production
- This setup works perfectly for local testing: **do not use this approach in production!**

For production:
- Register a public webhook endpoint in [Stripe Dashboard](https://dashboard.stripe.com/webhooks)
- Use the persistent webhook secret you receive
- Deploy webhook service behind HTTPS

### Security
- **Never** commit env files to version control
- **Never** use live mode keys in development
- **Always** validate signatures (handled by the webhook service)
- Stripe CLI credentials stay on your machine, never in Docker

### Multiple runs
- The `start.sh` script automatically cleans up previous runs
- The `.stripe.pid` file ensures proper process management

## Access Points
| Service | URL | Purpose |
|---------|-----|---------|
| Webhook API | http://localhost:8100/webhook | Receive Stripe events |
| Health Check | http://localhost:8100/health | Check webhook status |
| Kafka UI | http://localhost:8080 | View Kafka messages |

## Learn more
- [Stripe Webhooks Documentation](https://stripe.com/docs/webhooks)
- [Stripe CLI Documentation](https://stripe.com/docs/stripe-cli)
- [Stripe Testing Guide](https://stripe.com/docs/testing)
- [Stripe Event Types](https://stripe.com/docs/api/events/types)