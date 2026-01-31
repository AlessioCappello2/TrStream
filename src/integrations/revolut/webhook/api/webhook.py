from fastapi import FastAPI, Request
from upstash_redis import Redis 
import os 

redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)

app = FastAPI()

@app.post("/api/webhook")
async def webhook(request: Request):
    payload = await request.json()
    print("Received event:", payload)

    queue_name = "revolut-queue"
    redis.lpush(queue_name, payload)
    return {"status": "ok", "queued": True}
