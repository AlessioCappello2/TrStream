# import signal
# import asyncio
# import logging

# from contextlib import asynccontextmanager
# from fastapi import FastAPI, Request, Header, HTTPException

# from shared.config.logging_config import setup_logging
# from .core.kafka_producer import StripeKafkaProducer
# from .core.event_processor import process_event

# # Lifespan for start and end of the app
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # Start
#     yield 
#     # End
#     global in_flight
#     while in_flight > 0:
#         await asyncio.sleep(0.1)

# ####################################################################
# # Setup logging, app and producer
# ####################################################################
# filter = lambda record: "/health" not in record.getMessage()
# logger = setup_logging(
#     service_name="trstream.stripe.webhook", 
#     suppress_loggers={"kafka": logging.CRITICAL}, 
#     filters={"uvicorn.access": filter}
# )
# app = FastAPI(title="Stripe Webhook", lifespan=lifespan)
# producer = StripeKafkaProducer()
# in_flight = 0

# ####################################################################
# # Handle SIGTERM/SIGINT Exceptions
# ####################################################################
# running = True 

# def handle_termination(signum, frame):
#     global running
#     logger.warning("Shutdown signal received!")
#     running = False

# signal.signal(signal.SIGTERM, handle_termination)
# signal.signal(signal.SIGINT, handle_termination)

# ####################################################################
# # APP ENDPOINTS 
# ####################################################################
# @app.post("/webhook")
# async def stripe_webhook(request: Request, stripe_signature: str = Header(None)):
#     global in_flight, running
#     if not running:
#         raise HTTPException(status_code=503, detail="Webhook shutting down!")
    
#     in_flight += 1

#     try:
#         body = await request.body()
#         event = process_event(body, stripe_signature)
#         producer.send_event(event)
#         logger.info(f"Event {event['payload']['data']['object']['id']} sent to Kafka")
#         return {"status": "success"}
#     except ValueError as e:
#         logger.error(e)
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         logger.error(f"Webhook processing failed: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         in_flight -= 1

# @app.get("/health")
# async def health():
#     return {"status": "ok"}

import signal
import asyncio
import logging
from contextlib import asynccontextmanager

from pathlib import Path
from fastapi import FastAPI

from shared.config.logging_config import setup_logging
from shared.config.load_config import load_config_from_directory
from webhook.api import routes
from webhook.core.kafka_producer import StripeKafkaProducer
from webhook.config.settings import settings

# Shared state
in_flight = {"count": 0}
running_flag = {"running": True}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifespan.
    
    On startup: Initialize services
    On shutdown: Wait for in-flight requests
    """
    # Startup
    logger.info("Starting Stripe webhook service...")
    logger.info(f"Kafka broker: {settings.kafka_broker}")
    logger.info(f"Kafka topic: {settings.stripe_topic}")
    
    yield
    
    # Shutdown
    logger.info("Initiating graceful shutdown...")
    running_flag["running"] = False
    
    # Wait for in-flight requests to complete
    timeout = 30
    elapsed = 0
    while in_flight["count"] > 0 and elapsed < timeout:
        remaining = in_flight["count"]
        logger.info(f"Waiting for {remaining} in-flight request(s)...")
        await asyncio.sleep(0.5)
        elapsed += 0.5
    
    if in_flight["count"] > 0:
        logger.warning(
            f"Shutdown timeout: {in_flight['count']} "
            f"request(s) still in-flight"
        )
    else:
        logger.info("All requests completed")
    
    # Close producer
    producer.flush()
    producer.close()
    logger.info("Stripe webhook service stopped")


# Setup logging with health check filter
filter_health = lambda record: "/health" not in record.getMessage()
logger = setup_logging(
    service_name="trstream.stripe.webhook",
    suppress_loggers={"kafka": logging.CRITICAL},
    filters={"uvicorn.access": filter_health}
)

# Create FastAPI app
app = FastAPI(
    title="Stripe Webhook Service",
    description="Receives Stripe webhooks and forwards events to Kafka",
    lifespan=lifespan
)

# Initialize Kafka producer
logger.info("Initializing Kafka producer...")

cfg = load_config_from_directory(Path("src"), "webhook.yaml")
batch_size_log = cfg.get('batch_size_log', 100)
max_send_retries = cfg.get('max_send_retries', 5)

producer = StripeKafkaProducer(
    batch_size_log=batch_size_log,
    max_send_retries=max_send_retries,
    linger_ms=1
)

# Configure routes with shared state
routes.set_producer(producer)
routes.set_in_flight_counter(in_flight)
routes.set_running_flag(running_flag)

# Register routes
app.include_router(routes.router)

# Setup signal handlers for graceful shutdown
def handle_shutdown_signal(signum, frame):
    """Handle SIGTERM/SIGINT signals."""
    logger.warning(f"Received shutdown signal ({signum})")
    running_flag["running"] = False

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)

logger.info("Stripe webhook service ready")