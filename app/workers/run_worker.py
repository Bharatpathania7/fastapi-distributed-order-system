import asyncio

from app.workers.order_worker import start_worker


if __name__ == "__main__":
    print("Starting order worker...")

    asyncio.run(start_worker())

    print("Order worker stopped.")