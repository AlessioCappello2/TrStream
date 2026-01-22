import json
import pyarrow as pa
from datetime import datetime
from typing import List, Dict

FAKER_SCHEMA = pa.schema([
    pa.field("source", pa.string()),
    pa.field("received_at", pa.timestamp("ms")),
    pa.field("transaction_id", pa.string()),
    pa.field("user_id", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("transaction_type", pa.string()),
])

STRIPE_SCHEMA = pa.schema([
    pa.field("source", pa.string()),
    pa.field("received_at", pa.timestamp("ms")),
    pa.field("payment_intent_id", pa.string()),
    pa.field("customer_id", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("payment_method", pa.string()),   
])


def _decode_payload_column(table: pa.Table) -> List[Dict]:
    payload_column = table["payload"].to_pylist()
    return [json.loads(p) for p in payload_column]


def normalize_faker(table: pa.Table) -> pa.Table:
    payloads = _decode_payload_column(table)

    rows = []

    for i, payload in enumerate(payloads):
        if payload.get("status").lower() != "completed":
            continue

        rows.append({
            "source": "faker",
            "received_at": datetime.fromisoformat(table["received_at"][i].as_py()),
            "transaction_id": payload["transaction_id"],
            "user_id": payload["user_id"],
            "amount": payload["amount"],
            "currency": payload["currency"].lower(),
            "transaction_type": payload["transaction_type"].lower()
        })

    if not rows:
        return pa.Table.from_batches([], schema=FAKER_SCHEMA)
    
    return pa.Table.from_pylist(rows, schema=FAKER_SCHEMA)
    

def normalize_stripe(table: pa.Table) -> pa.Table:
    payloads = _decode_payload_column(table)

    rows = []

    for i, event in enumerate(payloads):
        obj = event["data"]["object"]

        if obj["object"] != "payment_intent" or obj["status"] != "succeeded":
            continue

        rows.append({
            "source": "stripe",
            "received_at": datetime.fromisoformat(table["received_at"][i].as_py()),
            "payment_intent_id": obj["id"],
            "customer_id": obj["customer"],
            "amount": float(obj["amount"]) / 100.0,
            "currency": obj["currency"],
            "payment_method": obj["payment_method"],
        })

    if not rows:
        return pa.Table.from_batches([], schema=STRIPE_SCHEMA)

    return pa.Table.from_pylist(rows, schema=STRIPE_SCHEMA)