from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.controllers.order_controller import router as order_router
from app.core.lifespan import lifespan
from app.db.session import get_db_session
from app.dependencies.request_context import get_request_id


settings = get_settings()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)

app.include_router(order_router)


@app.get("/health")
async def health():
    return {
        "status": "UP"
    }


@app.get("/test-request-context")
async def test_request_context(
    request_id: str = Depends(get_request_id),
):
    return {
        "request_id": request_id
    }


@app.get("/health/db")
async def database_health(
    session: AsyncSession = Depends(get_db_session),
):
    result = await session.execute(text("SELECT 1"))

    return {
        "database": "UP",
        "result": result.scalar_one(),
    }