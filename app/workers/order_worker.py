from decimal import Decimal
from uuid import UUID

from redis.exceptions import TimeoutError
from sqlalchemy import select

from app.db.session import AsyncSessionLocal
from app.models.order import Order
from app.models.payment_audit import PaymentAuditLog
from app.redis.client import redis_client
from app.redis.streams import (
    ORDER_DLQ,
    ORDER_STREAM,
    publish_order,
    publish_to_dlq,
)
from app.services.payment_service import PaymentService


CONSUMER_GROUP = "order-workers"
CONSUMER_NAME = "worker-1"

MAX_RETRIES = 3
PENDING_MESSAGE_IDLE_TIME = 30000


async def create_consumer_group():
    try:
        await redis_client.xgroup_create(
            name=ORDER_STREAM,
            groupname=CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )

        print(
            f"Consumer group '{CONSUMER_GROUP}' "
            f"created for stream '{ORDER_STREAM}'"
        )

    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            print(
                f"Consumer group '{CONSUMER_GROUP}' "
                f"already exists"
            )
        else:
            raise


async def process_order(
    message_id: str,
    data: dict,
):
    order_id = UUID(data["order_id"])
    idempotency_key = data["idempotency_key"]
    amount = Decimal(data["amount"])

    async with AsyncSessionLocal() as session:
        async with session.begin():

            result = await session.execute(
                select(Order)
                .where(Order.id == order_id)
                .with_for_update()
            )

            order = result.scalar_one_or_none()

            if order is None:
                raise ValueError(
                    f"Order not found: {order_id}"
                )

            if order.status != "PENDING":
                print(
                    f"[WORKER] Order {order_id} "
                    f"already processed with status "
                    f"{order.status}"
                )
                return

            payment_success = (
                await PaymentService.process_payment(
                    order_id=order.id,
                    amount=amount,
                    idempotency_key=idempotency_key,
                )
            )

            if payment_success:
                order.status = "PROCESSED"
                audit_status = "SUCCESS"
            else:
                order.status = "FAILED"
                audit_status = "FAILED"

            audit_log = PaymentAuditLog(
                order_id=order.id,
                idempotency_key=idempotency_key,
                status=audit_status,
            )

            session.add(audit_log)

            print(
                f"[WORKER] Order {order_id} "
                f"updated to {order.status}"
            )


async def handle_failed_message(
    message_id: str,
    data: dict,
    error: Exception,
):
    order_id = data["order_id"]
    idempotency_key = data["idempotency_key"]
    amount = data["amount"]

    retry_count = int(
        data.get("retry_count", "0")
    )

    next_retry = retry_count + 1

    if next_retry <= MAX_RETRIES:

        await publish_order(
            order_id=order_id,
            idempotency_key=idempotency_key,
            amount=amount,
            retry_count=next_retry,
        )

        await redis_client.xack(
            ORDER_STREAM,
            CONSUMER_GROUP,
            message_id,
        )

        print(
            f"[RETRY] Order {order_id} "
            f"retry={next_retry}/{MAX_RETRIES}"
        )

        return

    # Final failure → DLQ
    await publish_to_dlq(
        source_message_id=message_id,
        order_id=order_id,
        idempotency_key=idempotency_key,
        amount=amount,
        retry_count=retry_count,
        error=str(error),
    )

    # Mark order FAILED in PostgreSQL
    async with AsyncSessionLocal() as session:
        async with session.begin():

            result = await session.execute(
                select(Order)
                .where(Order.id == UUID(order_id))
                .with_for_update()
            )

            order = result.scalar_one_or_none()

            if order is None:
                raise ValueError(
                    f"Order not found: {order_id}"
                )

            if order.status == "PENDING":

                order.status = "FAILED"

                audit_log = PaymentAuditLog(
                    order_id=order.id,
                    idempotency_key=idempotency_key,
                    status="FAILED",
                )

                session.add(audit_log)

                print(
                    f"[WORKER] Order {order_id} "
                    f"updated to FAILED"
                )

            elif order.status == "FAILED":

                print(
                    f"[WORKER] Order {order_id} "
                    f"is already FAILED"
                )

    # ACK only after DB transaction succeeds
    await redis_client.xack(
        ORDER_STREAM,
        CONSUMER_GROUP,
        message_id,
    )

    print(
        f"[DLQ] Order {order_id} "
        f"moved to {ORDER_DLQ}"
    )


async def recover_pending_messages():
    print("[RECOVERY] Checking pending messages...")

    try:
        next_start, messages, deleted_ids = (
            await redis_client.xautoclaim(
                name=ORDER_STREAM,
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                min_idle_time=PENDING_MESSAGE_IDLE_TIME,
                start_id="0-0",
                count=10,
            )
        )

        if not messages:
            print(
                "[RECOVERY] No pending messages found"
            )
            return

        print(
            f"[RECOVERY] Claimed "
            f"{len(messages)} pending message(s)"
        )

        for message_id, data in messages:

            try:
                print(
                    f"[RECOVERY] Processing "
                    f"message={message_id}"
                )

                await process_order(
                    message_id,
                    data,
                )

                await redis_client.xack(
                    ORDER_STREAM,
                    CONSUMER_GROUP,
                    message_id,
                )

                print(
                    f"[RECOVERY] ACK "
                    f"message={message_id}"
                )

            except Exception as exc:

                print(
                    f"[RECOVERY] Failed "
                    f"message={message_id}: {exc}"
                )

                try:
                    await handle_failed_message(
                        message_id,
                        data,
                        exc,
                    )

                except Exception as recovery_exc:

                    print(
                        f"[RECOVERY] Retry/DLQ "
                        f"handling failed: "
                        f"{recovery_exc}"
                    )

    except Exception as exc:

        print(
            f"[RECOVERY] Redis recovery error: {exc}"
        )


async def order_worker():
    print(
        f"[WORKER] Started: "
        f"group={CONSUMER_GROUP}, "
        f"consumer={CONSUMER_NAME}"
    )

    while True:

        try:
            messages = await redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={
                    ORDER_STREAM: ">"
                },
                count=10,
                block=5000,
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:

                for message_id, data in stream_messages:

                    try:
                        print(
                            f"[WORKER] Processing "
                            f"message={message_id} "
                            f"retry="
                            f"{data.get('retry_count', '0')}"
                        )

                        await process_order(
                            message_id,
                            data,
                        )

                        await redis_client.xack(
                            ORDER_STREAM,
                            CONSUMER_GROUP,
                            message_id,
                        )

                        print(
                            f"[WORKER] ACK "
                            f"message={message_id}"
                        )

                    except Exception as exc:

                        print(
                            f"[WORKER] Failed "
                            f"message={message_id}: {exc}"
                        )

                        try:
                            await handle_failed_message(
                                message_id,
                                data,
                                exc,
                            )

                        except Exception as retry_exc:

                            print(
                                f"[WORKER] Retry/DLQ "
                                f"handling failed: "
                                f"{retry_exc}"
                            )

        except TimeoutError:

            print(
                "[WORKER] Redis read timeout, retrying..."
            )

        except Exception as exc:

            print(
                f"[WORKER] Redis error: {exc}"
            )


async def start_worker():
    await create_consumer_group()

    await recover_pending_messages()

    await order_worker()