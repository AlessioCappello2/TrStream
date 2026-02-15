import stripe
from datetime import datetime

from webhook.api.models import StripeEvent
from webhook.config.settings import settings

def process_event(body: dict, signature: str) -> dict:
    """
    Validate incoming Stripe event and envelop it for Kafka messaging and consumer processing.

    This function:
    1. Verifies the webhook signature to ensure authenticity
    2. Validates the event structure with Pydantic
    3. Wraps the event in an envelope for Kafka with metadata
    
    Args:
        body: Raw request body (bytes)
        signature: Stripe-Signature header value
        
    Returns:
        Enveloped event dict with structure:
        {
            "source": "stripe",
            "received_at": "2026-02-15T10:30:45.123",
            "payload": {validated Stripe event}
        }
        
    Raises:
        ValueError: If signature verification fails or event is malformed
    """
    try:
        validated_event = stripe.Webhook.construct_event(
            payload=body,
            sig_header=signature,
            secret=settings.stripe_webhook_secret
        )
    except ValueError as e:
        raise ValueError("Error parsing payload") from e
    except stripe.error.SignatureVerificationError as e:
        raise ValueError("Error verifying webhook signature") from e
    
    try:
        stripe_event = StripeEvent(**validated_event)
    except Exception as e:
        raise ValueError(f"Invalid event structure: {e}") from e

    return {
        "source": "stripe",
        "received_at": datetime.now().isoformat(timespec='milliseconds'),
        "payload": stripe_event.model_dump()
    }