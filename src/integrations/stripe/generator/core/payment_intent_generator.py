import random
import stripe
from pathlib import Path

from generator.config.settings import settings
from shared.config.load_config import load_config_from_directory

stripe.api_key = settings.stripe_secret_api_key

cfg = load_config_from_directory(Path("src"), "generator.yaml")
events_cfg = cfg['events']

def create_payment_intent():
    """
    Call this function to generate a PaymentIntent object with configured parameters.
    """
    intent = stripe.PaymentIntent.create(
        amount=random.randint(events_cfg['amount']['min'], events_cfg['amount']['max']),
        currency=events_cfg['currency'],
        payment_method=random.choice(events_cfg['payment_methods']),
        confirm=True,
        automatic_payment_methods={
            "enabled": True,
            "allow_redirects": "never",
        }
    )

    return intent
