#!/bin/bash
set -e

# Launch this script from CLI to enable Stripe event generation and capture

# Kill old listening process if present
if [ -f .stripe.pid ]; then
  OLD_PID=$(cat .stripe.pid)
  if kill -0 $OLD_PID 2>/dev/null; then
    echo "Killing previous Stripe CLI process ($OLD_PID)..."
    kill $OLD_PID
    sleep 1
  fi
  rm .stripe.pid
fi

# Start the webhook and kafka serivces
echo "Starting Docker services..."
docker compose up -d stripe-webhook kafka

# Wait for the webhook to become ready
echo "Waiting for webhook to become ready..."
until curl -sf http://localhost:8100/health > /dev/null; do
  sleep 1
done

# Forward stripe events to webhook port
echo "Starting Stripe event forwarding..."
nohup stripe listen --forward-to http://localhost:8100/webhook > stripe.log 2>&1 &
STRIPE_PID=$!

echo "Webhook is ready."

# Start the generator service
echo "Starting generator..."
docker compose up -d stripe-generator

echo "Stripe stack started."
echo "Stripe PID: $STRIPE_PID"

echo $STRIPE_PID > .stripe.pid