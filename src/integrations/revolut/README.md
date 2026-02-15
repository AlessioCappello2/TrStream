# Revolut Integration

Real-time Revolut transaction ingestion for the TrStream pipeline. This integration uses a serverless webhook (Vercel), Redis queue (Upstash) and worker containers to process Revolut events.

## Table of Contents
- [Introduction](#introduction)
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Setup](#setup)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Notes](#notes)
- [Access Points](#access-points)
- [Learn More](#learn-more)

## Introduction
This integration captures Revolut transaction events through an asynchronous pipeline built on local containers and publicly exposed endpoints for webhook ingestion and queueing before forwarding data to Kafka.

## Overview
This integration captures transaction events through an asynchronous pipeline:
- **Setup (Containerized)**: interactive CLI for Revolut authentication and webhook registration
- **Generator (Containerized)**: simulates Revolut events for testing
- **Webhook (Vercel)**: receives Revolut events and queues them in Redis
- **Redis queue (Upstash)**: decouples webhook from processing
- **Worker container (Containerized)**: processes events from queue and sends to Kafka

## Architecture
![Revolut Stack Architecture](../../../images/revolut/Architecture.svg)

## Prerequisites

### Required accounts
1. **Revolut Sandbox Business account**
    - Sign up: https://sandbox-business.revolut.com/signup
    - Sandbox allows to test with real events without involving real money transfers

2. **Vercel account**
    - Sign up: https://vercel.com/signup
    - Used to deploy the webhook endpoint

3. **Upstash account**
    - Sign up: https://upstash.com
    - Create a Redis database (free tier)

### Required software
- Docker and Docker compose
- openssl
- Node.js 18+ (for Vercel CLI)
- Vercel CLI: `npm install -g vercel` (or if you want, locally within `src/intergrations/revolut/webhook` without `-g` flag)

### Required files
- Private key and public certificate for Revolut API

## Quick Start (TL;DR)
```bash
# 1. Generate certificates
cd keys/revolut
openssl genrsa -out private.pem 2048
openssl req -new -x509 -key private.pem -out certificate.pem -days 365

# 2. Upload certificate.pem to Revolut Dashboard

# 3. Create Upstash Redis database

# 4. Deploy webhook
cd src/integrations/revolut/webhook
vercel

# 5. Configure environment
cp config/env/integrations/revolut/revolut.env.example config/env/integrations/revolut/revolut.env
# Edit revolut.env with your credentials

# 6. Run setup
docker compose run --rm revolut-setup
# Choose option 7 (Quick setup)

# 7. Update Vercel with signing secret
vercel env add REVOLUT_WEBHOOK_SECRET production

# 8. Start integration
cd scripts/integrations/revolut
./start.sh
```

For detailed instructions, see [Setup](#setup) section below.

## Setup
This is a one-time setup process. Follow steps in order.

### Step 1: Generate and upload Revolut API Certificate
1. Navigate to `keys/`:
```bash
cd keys
```
2. Launch the following commands and follow the indications to obtain the private key and public certificate:
    - Generate the private key:
        ```bash 
        openssl genrsa -out private.pem 2048
        ```
    - Generate the public certificate (valid for 1 year):
        ```bash
        openssl req -new -x509 -key private.pem -out certificate.pem -days 365
        ```
3. Login to your Sandbox Revolut Business account using your email and password, go to Settings -> API -> API Certificates

4. Add a new certificate:
    - Title: name of your certificate (e.g. `trstream-revolut-integration`)
    - OAuth Redirect URI: url where you will be redirected to retrieve OA codes
    - Public Key X509: paste the entire content of `certificate.pem` (including `-----BEGIN CERTIFICATE-----` and `-----END CERTIFICATE-----`)

**Note**: the redirect URI is where Revolut redirects after authorization. Use a simple placeholder, like `example.com`, since you only need the OA code from the URL. It must match `REVOLUT_REDIRECT_AUTH` configured in Step 4.


5. Copy the Client ID that appears when you click on the API certificate (needed for Step 4).

### Step 2: Create Upstash Redis Database
1. Go to https://console.upstash.com
2. Click on Redis, then `Create Database`
3. Configure:
    - **Name**: your Redis db name (e.g. `trstream-revolut`)
    - **Cloud Provider**: provider hosting your Redis db (e.g. AWS)
    - **Region**: Choose closest to you
    - **Tier**: Free
4. Click `Create`
5. Copy from database page:
    - **REST URL**: `https://xxxxx.upstash.io`
    - **REST TOKEN**: `abcde12345`

### Step 3: Deploy Webhook to Vercel
1. Move to `src/integrations/revolut/webhook`
```bash
cd ./src/integrations/revolut/webhook
```

2. After having installed the Vercel CLI (see Prerequisites), login to Vercel CLI (it will open your browser):
```bash
vercel login
```

3. Create the `.env` file in the webhook directory:
```bash
UPSTASH_REDIS_REST_URL="https://xxxxx.upstash.io"
UPSTASH_REDIS_REST_TOKEN="abcde12345"
REVOLUT_WEBHOOK_SECRET=placeholder
```

**Note**: `REVOLUT_WEBHOOK_SECRET` is a placeholder: you'll get the real value in Step 5.

4. Deploy to Vercel (follow prompt instructions):
```bash
vercel deploy [--prod]
```

5. Copy the deployment/webhook URL displayed as it's needed for `setup.env` and `generator.env`

### Step 4: Configure environment variables
You can remove the `.example` extension from the files under `config/env/integrations/revolut` and provide the required values:
```bash
# Revolut API credentials
- REVOLUT_CLIENT_ID=client-id-from-step-1
- REVOLUT_WEBHOOK_URL=webhook-url-from-step-3
- REVOLUT_REDIRECT_AUTH=example.com
# Account IDs (you'll get these in Step 5)
- REVOLUT_SOURCE_ACCOUNT=
- REVOLUT_TARGET_ACCOUNT= 
# Upstash Redis (from Step 2)
- UPSTASH_REDIS_REST_URL=https://xxxxx.upstash.io
- UPSTASH_REDIS_REST_TOKEN=abcde12345
# Kafka configuration (default values, change if needed)
- KAFKA_BROKER=kafka:9092
- REVOLUT_TOPIC=transactions-revolut
```

### Step 5: Run Setup Container (interactive)
Run the interactive setup to authenticate and register the Vercel Webhook:
```bash
docker compose run --rm revolut-setup
```

The CLI will guide you through:
```
============================================================
REVOLUT INTEGRATION SETUP CLI
============================================================

1. Authenticate (get & save tokens)
2. Register webhook
3. List webhooks
4. Delete webhook
5. Check token status
6. List accounts
7. Quick setup (auth + webhook)
0. Exit

============================================================
Select an option:
```
#### Step 5.1

**Choose Option 7 for a fast initial setup**: this runs the complete authentication and webhook registration flow.

```
Step 1/3: Authentication
Paste Revolut OA code:
```

To get the OA code:
1. Click on the API Certificate you created in Step 1, then on `Enable access`
2. A new tab will open, asking you to authorize the access
3. Click on `Authorize`, insert your password and wait to be redirected to your redirect URI 
4. Copy the oa_code from the url: `?code=oa_sand_xxxxx`
5. Paste it into the CLI

The setup will exchange code for access/refresh tokens, save tokens to Redis and confirm authentication success.

---
```
Step 2/3: Webhook Registration
Default webhook URL: `REVOLUT_WEBHOOK_URL`
Use default URL? (y/n):
```

Type `y` to use the configured webhook url.

The setup will register the webhook with Revolut and return the webhook ID and **signing secret**

**IMPORTANT**: copy the signing secret shown (it's needed for Step 6):

```
SIGNING SECRET: whsec_xxxxx...
```
---
```
Step 3/3: Listing Accounts
Fetching accounts...

============================================================
Found 2 account(s):
============================================================

1. Account ID: xxxyyyzzz
   Name: Main
   Currency: GBP
   Balance: 2800
   State: active
   Created: 2026-01-27T18:56:18.612Z

2. Account ID: zzzxxxyyy
   Name: Business trips
   Currency: USD
   Balance: 1470
   State: active
   Created: 2026-01-27T18:56:18.612Z
```

Copy the account IDs you want to use and update your `generator.env`:
```bash
# In config/env/integrations/revolut/generator.env
REVOLUT_SOURCE_ACCOUNT=xxxyyyzzz
REVOLUT_TARGET_ACCOUNT=zzzxxxyyy
```

#### Other options
- **Option 1:** Authenticate only
- **Option 2:** Register webhook only
- **Option 3:** List all registered webhooks
- **Option 4:** Delete a webhook
- **Option 5:** Check token expiration
- **Option 6:** List accounts (useful if you skipped quick setup)

### Step 6: Update Vercel Environment
#### Option 1
1. Move to the webhook directory:
```bash
cd src/integrations/revolut/webhook
```

2. Add the signing secret to Vercel:
```bash
vercel env add REVOLUT_WEBHOOK_SECRET production
```

When prompted, paste the signing secret from Step 5

3. Redeploy to apply new environment variable:
```bash
vercel deploy --prod
```

#### Option 2
1. Go to https://vercel.com/dashboard
2. Select your project
3. Go to Settings -> Environment Variables
4. Add/Edit `REVOLUT_WEBHOOK_SECRET`
5. Set value to the signing secret
6. Redeploy from Deployments tab

## Usage
Two simple scripts are provided within `scripts/scripts-cli/revolut` directory. Those scripts are intended to be simple since only part of the Revolut Integration is run locally through containers.

### Start Revolut services
```bash
start.sh
```

This starts:
- **Generator container**: simulates Revolut events
- **Worker container**: processes events from Redis -> Kafka

You can optionally scale generators and workers. If no parameters are provided, the default scale value is 1.

```bash
start.sh generator=3 worker=2
```

### Stop Revolut services
```bash
stop.sh
```

It stops: generator, worker

## Monitoring

### Check generator status
```bash
docker compose logs -f revolut-generator
```

**Look for:**
- `HTTP Request: POST https://xxxxx.upstash.io "HTTP/1.1 200 OK"`

### Check worker status
```bash
docker compose logs -f revolut-worker
```

**Look for:**
- `HTTP Request: POST https://xxxxx.upstash.io "HTTP/1.1 200 OK"`
- `Processing event: {...}`
- `Sent to Kafka successfully!`

### Check Redis
- Go to https://console.upstash.com
- Select your database
- Go to `Data Browser`
- Check Redis content

### Check Vercel Webhook
- Go to https://vercel.com
- Select your project
- Access the Logs tab
- Check requests status

### Verify events in Kafka
Using Kafka UI:
```bash
open http://localhost:8080
```

Navigate to Topics -> `transactions-revolut` -> Messages

A message should appear like this:
```json
{
	"source": "revolut",
	"received_at": "2026-02-15T19:24:27.286",
	"payload": {
		"data": {
            ...
		},
		"event": "TransactionCreated",
		"timestamp": "2026-02-15T19:22:31.049041Z"
	}
}
```

## Notes

### Cost considerations
- **Vercel**: free tier (100 GB bandwidth/month, 1M invocations/month)
- **Upstash**: free tier (one database, 500k commands/month)
- **Revolut Sandbox**: free for testing

### Development vs Production
- This setup works perfectly for local testing.

For production:
- Deploy worker to production infrastructure (not localhost)
- Implement automatic token refresh
- Use Vercel Pro and Upstash paid tier for higher limits
- Set up proper monitoring and alerting
- Use secrets management (not env files)

### Why Serverless Webhook?
Revolut requires HTTPS and public URL for webhooks.

Avoided solutions:
- ngrok (requires running process, auth, free tier limits)
- exposing localhost (security risk, no HTTPS)

On the other hand, Vercel includes:
- Free tier sufficient for testing
- Automatic HTTPS
- Reliable, no maintenance
- Easy deployment

### Token storage
Tokens are stored in Redis, alongside with the events queue. Features:
- Tokens persist across container restarts
- Can be accessed by multiple workers
- No local file management
However, tokens expire (typically 40 minutes). If not refreshed by a service before expiring, one needs to perform the manual authentication again.

## Access Points
| Service | URL | Purpose |
| ------- | --- | ------- |
| Webhook | Vercel generated | Receives Revolut events |
| Redis | Upstash console | Persists events and tokens |
| Kafka UI | http://localhost:8080 | View Kafka messages |

## Learn More
- [Revolut Business API Docs](https://developer.revolut.com/docs/business/business-api)
- [Revolut Webhooks](https://developer.revolut.com/docs/business/webhooks)
- [Upstash Redis Docs](https://docs.upstash.com/redis)
- [Vercel Deployment](https://vercel.com/docs)