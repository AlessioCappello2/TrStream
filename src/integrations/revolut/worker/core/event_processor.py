from datetime import datetime

def process_event(event: dict) -> dict:
    """Process a Revolut event and prepare it for Kafka."""

    return {
        "source": "revolut",
        "received_at": datetime.now().isoformat(timespec='milliseconds'),
        "payload": event
    }