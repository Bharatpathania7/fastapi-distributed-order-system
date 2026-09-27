from redis.exceptions import ResponseError

from app.redis.client import redis_client


ORDER_STREAM = "orders.v1"
ORDER_DLQ = "orders.dlq"


async def publish_order(
    order_id: str,
    idempotency_key: str,
    amount: str,
    retry_count: int = 0,
) -> str:
    message_id = await redis_client.xadd(
        ORDER_STREAM,
        {
            "order_id": order_id,
            "idempotency_key": idempotency_key,
            "amount": amount,
            "retry_count": str(retry_count),
        },
    )

    return message_id


async def publish_to_dlq(
    source_message_id: str,
    order_id: str,
    idempotency_key: str,
    amount: str,
    retry_count: int,
    error: str,
) -> str:

    try:
        message_id = await redis_client.xadd(
            ORDER_DLQ,
            {
                "order_id": order_id,
                "idempotency_key": idempotency_key,
                "amount": amount,
                "retry_count": str(retry_count),
                "error": error,
                "source_message_id": source_message_id,
            },
            id=source_message_id,
        )

        return message_id

    except ResponseError as exc:
        if "equal or smaller than the target stream top item" in str(exc):
            print(
                f"[DLQ] Message {source_message_id} "
                f"already exists in DLQ"
            )

            return source_message_id

        raise