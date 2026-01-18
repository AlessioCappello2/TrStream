#!/bin/bash
set -e

# Launch this script from CLI to sistematically stop the Stripe stack

# Stop the generator first
echo "Stopping Stripe generator..."
docker compose stop stripe-generator

echo "Waiting for generator to stop..."
while docker compose ps stripe-generator | grep -q "Up"; do
  sleep 1
done
echo "Generator stopped."

# Stop the webhook 
echo "Stopping Stripe webhook..."
docker compose stop stripe-webhook

# Kill the listening process
echo "Stopping Stripe CLI forwarding..."
if [ -f .stripe.pid ]; then
  STRIPE_PID=$(cat .stripe.pid)
  if kill -0 $STRIPE_PID 2>/dev/null; then
    kill $STRIPE_PID
    echo "Stripe CLI (PID $STRIPE_PID) stopped."
  else
    echo "Stripe CLI (PID $STRIPE_PID) already stopped."
  fi
  rm .stripe.pid
fi

echo "Stripe stack fully stopped."
