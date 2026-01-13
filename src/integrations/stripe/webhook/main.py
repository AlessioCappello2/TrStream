import signal
import asyncio

from fastapi import FastAPI, Request, Header, HTTPException
from contextlib import asynccontextmanager

from .config.logging_config import setup_logging
from .core.kafka_producer import StripeKafkaProducer
from .core.event_processor import process_event

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield 
    while in_flight > 0:
        await asyncio.sleep(0.1)

logger = setup_logging()
app = FastAPI(title="Stripe Webhook", lifespan=lifespan)
producer = StripeKafkaProducer()
running = True 
in_flight = 0

def handle_termination(signum, frame):
    global running
    logger.warning("Shutdown signal received!")
    running = False

signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_termination)

@app.post("/webhook")
async def stripe_webhook(request: Request, stripe_signature: str = Header(None)):
    global in_flight
    if not running:
        raise HTTPException(status_code=503, detail="Webhook shutting down!")
    
    in_flight += 1

    try:
        body = await request.body()
        event = process_event(body, stripe_signature)
        producer.send_event(event.model_dump())
        logger.info(f"Event {event.id} of type {event.type} sent to Kafka")
        return {"status": "success"}
    except ValueError as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        in_flight -= 1
