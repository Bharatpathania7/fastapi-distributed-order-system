from fastapi import Depends, FastAPI

from app.config import get_settings
from app.core.lifespan import lifespan
from app.dependencies.request_context import get_request_id


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)


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