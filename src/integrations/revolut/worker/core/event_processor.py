from datetime import datetime

def process_event(event: dict) -> dict:
    """
    
    """

    return {
        "source": "revolut",
        "received_at": datetime.now().isoformat(timespec='milliseconds'),
        "payload": event
    }