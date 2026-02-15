#!/bin/bash
set -e

# Usage:
# ./create-topics.sh <broker> <retention_ms> <topic1> <topic2> ...
# Example:
# ./create-topics.sh kafka:9092 3600000 transactions-trial stripe-events revolut-events

BROKER=${1:-localhost:9092}
RETENTION_MS=${2:-3600000}  # default: 1 hour

# Remaining arguments are topics
shift 2
TOPICS=("$@")

# Default topic if none provided
if [ ${#TOPICS[@]} -eq 0 ]; then
    TOPICS=("transactions-trial")
fi

echo "[==== Kafka Topic Setup ====]"
echo "Broker: $BROKER"
echo "Retention: $RETENTION_MS ms"
echo "Topics: ${TOPICS[*]}"
echo "[============================]"

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

# Check if Kafka is available
if ! /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list > /dev/null 2>&1; then
    echo "ERROR: Kafka broker not reachable after waiting. Exiting."
    exit 1
fi

# Process each topic
for TOPIC in "${TOPICS[@]}"; do
    echo ""
    echo "Processing topic: $TOPIC"
    echo "----------------------------"
    
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
            echo "Retention applied"
        elif [ "$CURRENT_RETENTION" != "$RETENTION_MS" ]; then
            echo "Updating retention.ms from $CURRENT_RETENTION to $RETENTION_MS"
            /opt/bitnami/kafka/bin/kafka-configs.sh \
                --bootstrap-server "$BROKER" \
                --alter \
                --entity-type topics \
                --entity-name "$TOPIC" \
                --add-config retention.ms=$RETENTION_MS
            echo "Retention updated"
        else
            echo "Retention already set to $RETENTION_MS"
        fi
    fi
done

echo ""
echo "[==================================]"
echo " All topics processed successfully!"
echo "[==================================]"