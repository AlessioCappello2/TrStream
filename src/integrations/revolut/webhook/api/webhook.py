import os 
import json
import hmac 
import hashlib 

from fastapi import FastAPI, Request, HTTPException
from upstash_redis import Redis 

# Redis client
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"]
)

# App and Redis List/Queue name
app = FastAPI()
QUEUE_NAME = "revolut:queue"

# Webhook secret
REVOLUT_SECRET = os.environ["REVOLUT_SECRET"]

# Verify Revolut signature to accept only valid events
def verify_signature(payload: bytes, signature: str, timestamp: str) -> bool:
    if not signature or not timestamp:
        return False
    digest = hmac.new(
        key=REVOLUT_SECRET.encode("utf-8"),
        msg=f"v1.{timestamp}.{payload.decode('utf-8')}".encode("utf-8"),
        digestmod=hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(digest, signature)


# App webhook endpoint
@app.post("/api/webhook")
async def webhook(request: Request):
    payload_bytes = await request.body()
    signature = request.headers.get("Revolut-Signature").replace("v1=", "")
    timestamp = request.headers.get("Revolut-Request-Timestamp")


    if not verify_signature(payload_bytes, signature, timestamp):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(payload_bytes)
    print("Received event:", payload) # just for logging purposes
    redis.lpush(QUEUE_NAME, json.dumps(payload))
    return {"status": "ok", "queued": True}
