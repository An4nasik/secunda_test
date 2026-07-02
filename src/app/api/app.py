"""FastAPI application factory and lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.payments import router as payments_router
from app.config import get_settings
from app.db import build_engine, build_session_factory
from app.logs import configure_logging
from app.services.payments import IdempotencyConflictError


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build and dispose per-process resources (settings, engine, sessions)."""
    settings = get_settings()
    if not settings.api_key.get_secret_value():
        msg = "API_KEY must be set to a non-empty value"
        raise RuntimeError(msg)
    configure_logging(settings.log_level, json_output=settings.log_json)

    engine = build_engine(settings.database_url)
    app.state.settings = settings
    app.state.session_factory = build_session_factory(engine)
    try:
        yield
    finally:
        await engine.dispose()


def _idempotency_conflict_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})


def create_app() -> FastAPI:
    """Create the payment service ASGI application."""
    app = FastAPI(
        title="Payment Service",
        description="Asynchronous payment processing with outbox-based delivery guarantees.",
        version="1.0.0",
        lifespan=_lifespan,
    )
    app.include_router(payments_router)
    app.add_exception_handler(IdempotencyConflictError, _idempotency_conflict_handler)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
