from decimal import Decimal

from pydantic import BaseModel, Field


class CreateOrderRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=100)
    amount: Decimal = Field(gt=0)