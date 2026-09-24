from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Application starting...")

    yield

    # Shutdown
    print("Application shutting down...")