from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.order import Order
from app.redis.streams import publish_order


class OrderService:

    @staticmethod
    async def create_order(
        session: AsyncSession,
        idempotency_key: str,
        amount: Decimal,
    ) -> tuple[Order, bool]:

        try:
            async with session.begin():

                result = await session.execute(
                    select(Order).where(
                        Order.idempotency_key == idempotency_key
                    )
                )

                existing_order = result.scalar_one_or_none()

                if existing_order:
                    return existing_order, False

                order = Order(
                    idempotency_key=idempotency_key,
                    amount=amount,
                    status="PENDING",
                )

                session.add(order)

            # DB transaction is committed here

            await publish_order(
                order_id=str(order.id),
                idempotency_key=order.idempotency_key,
                amount=str(order.amount),
            )

            return order, True

        except IntegrityError:
            await session.rollback()

            result = await session.execute(
                select(Order).where(
                    Order.idempotency_key == idempotency_key
                )
            )

            existing_order = result.scalar_one()

            return existing_order, False