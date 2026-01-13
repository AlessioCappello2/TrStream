from pydantic import BaseModel
from typing import Any, Dict

class StripeEvent(BaseModel):
    id: str
    object: str
    type: str
    created: int
    data: Dict[str, Any]   