#!/bin/bash
set -e

# Usage:
# ./create-topics.sh <broker> <topic> <retention_ms>
# Example:
# ./create-topics.sh localhost:9092 transactions 3600000

BROKER=${1:-localhost:9092}
TOPIC=${2:-transactions-trial0}
RETENTION_MS=${3:-3600000}  # default: 1 hour

echo "[==== Kafka topic setup ====]"
echo "Broker: $BROKER"
echo "Topic: $TOPIC"
echo "Retention: $RETENTION_MS ms"
echo "[===========================]"

# Wait for Kafka to become ready
echo "Waiting for Kafka broker at $BROKER..."
for i in {1..30}; do
    if /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    echo "Kafka not ready yet, retrying in 2s... ($i/30)"
    sleep 2
done

# If Kafka is still not available, exit
if ! /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list > /dev/null 2>&1; then
    echo "ERROR: Kafka broker not reachable after waiting. Exiting."
    exit 1
fi

# Check if topic exists
EXISTS=$(/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list | grep -w "$TOPIC" || true)

if [ -z "$EXISTS" ]; then
    echo "Creating topic '$TOPIC'..."
    /opt/bitnami/kafka/bin/kafka-topics.sh \
      --create \
      --if-not-exists \
      --bootstrap-server "$BROKER" \
      --topic "$TOPIC" \
      --partitions 3 \
      --replication-factor 1 \
      --config retention.ms=$RETENTION_MS \
      --config cleanup.policy=delete
    echo "Topic '$TOPIC' created with retention.ms=$RETENTION_MS"
else
    echo "Topic '$TOPIC' already exists. Checking retention..."
    CURRENT_RETENTION=$(/opt/bitnami/kafka/bin/kafka-configs.sh \
        --bootstrap-server "$BROKER" \
        --entity-type topics \
        --entity-name "$TOPIC" \
        --describe 2>/dev/null | grep -o 'retention.ms=[0-9]*' | cut -d= -f2)

    if [ -z "$CURRENT_RETENTION" ]; then
        echo "No retention.ms set. Applying retention.ms=$RETENTION_MS"
        /opt/bitnami/kafka/bin/kafka-configs.sh \
            --bootstrap-server "$BROKER" \
            --alter \
            --entity-type topics \
            --entity-name "$TOPIC" \
            --add-config retention.ms=$RETENTION_MS
    elif [ "$CURRENT_RETENTION" != "$RETENTION_MS" ]; then
        echo "Updating retention.ms from $CURRENT_RETENTION to $RETENTION_MS"
        /opt/bitnami/kafka/bin/kafka-configs.sh \
            --bootstrap-server "$BROKER" \
            --alter \
            --entity-type topics \
            --entity-name "$TOPIC" \
            --add-config retention.ms=$RETENTION_MS
    else
        echo "Retention already set to $RETENTION_MS: no update needed."
    fi
fi
