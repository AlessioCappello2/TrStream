"""API route handlers for Stripe webhook service."""
import logging
from fastapi import APIRouter, Request, Header, HTTPException

from webhook.core.kafka_producer import StripeKafkaProducer
from webhook.core.event_processor import process_event
from shared.config.logging_config import setup_logging
from webhook.api.models import WebhookResponse, HealthResponse

logger = setup_logging(
    service_name="trstream.stripe.webhook", 
    suppress_loggers={"kafka": logging.CRITICAL}, 
    filters={"uvicorn.access": lambda record: "/health" not in record.getMessage()}
)
router = APIRouter()

# Global state
producer: StripeKafkaProducer = None
in_flight = {"count": 0}
running_flag = {"running": True}


def set_producer(kafka_producer: StripeKafkaProducer) -> None:
    """Set the Kafka producer instance."""
    global producer
    producer = kafka_producer


def set_running_flag(flag: dict) -> None:
    """Set the running flag reference."""
    global running_flag
    running_flag = flag


def set_in_flight_counter(counter: dict) -> None:
    """Set the in-flight counter reference."""
    global in_flight
    in_flight = counter


@router.post("/webhook", response_model=WebhookResponse)
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="Stripe-Signature")
):
    """
    Process Stripe webhook events.
    
    Validates webhook signature, processes the event,
    and forwards to Kafka for downstream processing.
    
    Args:
        request: FastAPI request with raw body
        stripe_signature: Stripe-Signature header for verification
        
    Returns:
        Success response with event ID
        
    Raises:
        HTTPException: 503 if shutting down, 400 for invalid events, 500 for errors
    """
    # Check if service is shutting down
    if not running_flag["running"]:
        logger.warning("Rejected request: service shutting down")
        raise HTTPException(
            status_code=503,
            detail="Service is shutting down"
        )
    
    in_flight["count"] += 1

    try:
        # Read raw body (needed for signature verification)
        body = await request.body()
        
        # Validate signature and process event
        event = process_event(body, stripe_signature)
        
        # Extract event ID from processed event
        event_id = event['payload']['id']
        
        # Send to Kafka
        producer.send_event(event)
        
        logger.info(f"Event {event_id} forwarded to Kafka")
        
        return WebhookResponse(
            status="success",
            event_id=event_id
        )
        
    except ValueError as e:
        # Invalid signature or malformed event
        logger.error(f"Invalid webhook event: {e}")
        raise HTTPException(status_code=400, detail=str(e))
        
    except Exception as e:
        # Unexpected error (Kafka connection, etc.)
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )
        
    finally:
        in_flight["count"] -= 1


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns service status and current in-flight request count.
    """
    return HealthResponse(
        status="ok",
        in_flight=in_flight["count"],
        service="stripe-webhook"
    )