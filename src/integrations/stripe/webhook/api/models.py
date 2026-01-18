from pydantic import BaseModel
from typing import Any, Dict

class StripeEvent(BaseModel):
    id: str
    data: dict  