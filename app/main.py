"""API entry point: python -m uvicorn app.main:app --reload"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from app.api import router
from app.broker import broker
from app.db.session import close_db, init_db


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    try:
        await init_db()
        try:
            await broker.connect()
        except Exception as error:
            logging.getLogger(__name__).warning("Broker unavailable at startup: %s", type(error).__name__)
        yield
    finally:
        try:
            await broker.close()
        finally:
            await close_db()


def create_app() -> FastAPI:
    application = FastAPI(
        title="AI Agent Execution Service",
        version="0.1.0",
        description="Submit persistent runs, dispatch via RabbitMQ, and query execution results.",
        lifespan=lifespan,
    )
    application.include_router(router)
    return application


app = create_app()
