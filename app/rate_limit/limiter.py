from fastapi import HTTPException, Request, status

from app.redis.client import redis_client


RATE_LIMIT = 10
WINDOW_SECONDS = 60


async def rate_limit(request: Request):
    print("[RATE LIMIT] Dependency called")

    client_ip = request.client.host if request.client else "unknown"

    key = f"rate_limit:{client_ip}"

    current_count = await redis_client.incr(key)

    print(
        f"[RATE LIMIT] "
        f"IP={client_ip} "
        f"COUNT={current_count}"
    )

    if current_count == 1:
        await redis_client.expire(
            key,
            WINDOW_SECONDS,
        )

    if current_count > RATE_LIMIT:
        print(
            f"[RATE LIMIT] BLOCKED "
            f"IP={client_ip} "
            f"COUNT={current_count}"
        )

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Try again later.",
        )