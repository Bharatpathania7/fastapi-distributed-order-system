from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.workers.order_worker import create_consumer_group


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application starting...")

    await create_consumer_group()

    yield

    print("Application shutting down")