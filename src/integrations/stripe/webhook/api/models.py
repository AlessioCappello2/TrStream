from pydantic import BaseModel, Field
from typing import Any, Dict, Optional

class StripeEvent(BaseModel):
    """Stripe Webhook event model. Validates the basic structure of Stripe events."""
    id: str = Field(..., description="Unique Stripe event ID")
    type: str = Field(..., description="Event type (e.g. 'payment_intent.created')")
    data: Dict[str, Any] = Field(..., description="Event data containing the object")  

    # Optional Stripe fields
    api_version: Optional[str] = Field(None, description="Stripe API version")
    created: Optional[int] = Field(None, description="Event creation timestamp")
    livemode: Optional[bool] = Field(None, description="Live or test mode")


class WebhookResponse(BaseModel):
    """Response model for successful webhook processing."""
    status: str = Field(default="success")
    event_id: Optional[str] = Field(None, description="Processed event ID")


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str = Field(..., description="Service status")
    in_flight: int = Field(..., description="Number of requests being processed")
    service: str = Field(default="stripe-webhook")