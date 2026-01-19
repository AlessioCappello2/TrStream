import json
from datetime import datetime
from typing import Dict, Tuple

def validate_and_normalize_event(event: dict) -> Tuple[Dict, str]:
    required = {"source", "received_at", "payload"}
    if not required.issubset(event):
        raise ValueError("Invalid event envelope")
    
    norm_dt = datetime.fromisoformat(event["received_at"]).date().isoformat()
    return {
        "source": event["source"],
        "received_at": event["received_at"],
        "payload": json.dumps(event["payload"])
    }, norm_dt