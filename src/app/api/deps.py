"""FastAPI dependencies: static API-key authentication and database sessions."""

import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_app_settings(request: Request) -> Settings:
    """Return the settings instance attached to the application state."""
    return request.app.state.settings


def require_api_key(
    provided: Annotated[str | None, Security(_api_key_header)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> None:
    """Reject the request unless a valid ``X-API-Key`` header is present.

    Uses a constant-time comparison to avoid leaking the key via timing.
    """
    expected = settings.api_key.get_secret_value()
    if provided is None or not secrets.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield a request-scoped database session."""
    async with request.app.state.session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
