import stripe

from ..api.models import StripeEvent
from ..config.settings import settings

#stripe.api_key = settings.stripe_api_key

def process_event(body: dict, signature: str) -> StripeEvent:
    """
    Validate incoming Stripe event.
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

    return StripeEvent(**validated_event)
