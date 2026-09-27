from decimal import Decimal
from uuid import UUID


class PaymentService:

    @staticmethod
    async def process_payment(
        order_id: UUID,
        amount: Decimal,
        idempotency_key: str,
    ) -> bool:
        print(
            f"[PAYMENT] Processing payment "
            f"order_id={order_id}, "
            f"amount={amount}, "
            f"idempotency_key={idempotency_key}"
        )

        # Controlled failure for testing retry/DLQ.
        if amount == Decimal("0.01"):
            raise RuntimeError(
                "Simulated payment gateway failure"
            )

        # Mock payment gateway success.
        return True