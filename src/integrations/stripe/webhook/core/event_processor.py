import stripe
from datetime import datetime

from ..api.models import StripeEvent
from ..config.settings import settings

def process_event(body: dict, signature: str) -> dict:
    """
    Validate incoming Stripe event and envelop it for Kafka messaging and consumer processing.
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

    return {
        "source": "stripe",
        "received_at": datetime.now().isoformat(timespec='milliseconds'),
        "payload": StripeEvent(**validated_event).model_dump()
    }