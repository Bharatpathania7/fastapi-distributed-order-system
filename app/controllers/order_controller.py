from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.dto.order import CreateOrderRequest
from app.dto.order_response import OrderResponse
from app.rate_limit.limiter import rate_limit
from app.services.order_service import OrderService


router = APIRouter(
    prefix="/api/orders",
    tags=["Orders"],
)


@router.post(
    "",
    response_model=OrderResponse,
    dependencies=[Depends(rate_limit)],
)
async def create_order(
    request: CreateOrderRequest,
    session: AsyncSession = Depends(get_db_session),
):
    print("🔥 CREATE ORDER ROUTE HIT")
    order, created = await OrderService.create_order(
        session=session,
        idempotency_key=request.idempotency_key,
        amount=request.amount,
    )

    return OrderResponse(
        id=str(order.id),
        idempotency_key=order.idempotency_key,
        amount=order.amount,
        status=order.status,
        created_at=order.created_at,
    )