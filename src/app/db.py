"""Async SQLAlchemy engine and session factory construction.

No global engine lives here on purpose: each service (API, consumer, relay)
builds and disposes its own engine within its lifecycle, which keeps the
modules decoupled and the tests trivial.
"""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def build_engine(url: str) -> AsyncEngine:
    """Create an async engine with production-grade pool defaults.

    Args:
        url: SQLAlchemy database URL (``postgresql+asyncpg://...``).
    """
    return create_async_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20)


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create a session factory bound to the given engine."""
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
