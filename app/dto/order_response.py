from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class OrderResponse(BaseModel):
    id: str
    idempotency_key: str
    amount: Decimal
    status: str
    created_at: datetime