"""Outbox relay: publishes pending outbox rows to RabbitMQ.

The relay closes the gap of the transactional outbox pattern: events are
committed to the ``outbox`` table together with the payment and delivered to
the broker asynchronously. Delivery is at-least-once — if the process dies
between a publish and the commit of ``published_at``, the batch is published
again later, and consumers deduplicate.
"""

import asyncio
from datetime import UTC, datetime

import structlog
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import PAYMENTS_EXCHANGE, create_broker, declare_topology
from app.config import get_settings
from app.db import build_engine, build_session_factory
from app.logs import configure_logging
from app.models import OutboxMessage

logger = structlog.get_logger(__name__)


async def publish_pending_batch(
    session: AsyncSession, broker: RabbitBroker, batch_size: int
) -> int:
    """Publish one batch of unpublished outbox rows and mark them published.

    Rows are locked with ``FOR UPDATE SKIP LOCKED`` so multiple relay
    replicas never fight over the same events.

    Returns:
        The number of messages published in this batch.
    """
    result = await session.execute(
        select(OutboxMessage)
        .where(OutboxMessage.published_at.is_(None))
        .order_by(OutboxMessage.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    messages = result.scalars().all()
    for message in messages:
        await broker.publish(
            message.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=message.routing_key,
            persist=True,
            message_id=str(message.id),
        )
        message.published_at = datetime.now(UTC)
    await session.commit()
    return len(messages)


async def _safe_publish_batch(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker, batch_size: int
) -> int:
    """Run one batch, converting any failure into a logged, retryable no-op."""
    try:
        async with session_factory() as session:
            return await publish_pending_batch(session, broker, batch_size)
    except Exception:
        logger.exception("outbox_batch_failed")
        return 0


async def run_relay() -> None:
    """Run the relay until the process is stopped."""
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_json)
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    broker = create_broker(settings)

    async with broker:
        await declare_topology(broker, settings)
        logger.info(
            "outbox_relay_started",
            poll_interval=settings.outbox_poll_interval_seconds,
            batch_size=settings.outbox_batch_size,
        )
        try:
            while True:
                published = await _safe_publish_batch(
                    session_factory, broker, settings.outbox_batch_size
                )
                if published:
                    logger.info("outbox_published", count=published)
                else:
                    # Drain the backlog without pauses; sleep only when idle.
                    await asyncio.sleep(settings.outbox_poll_interval_seconds)
        finally:
            await engine.dispose()


def main() -> None:
    """Console entry point."""
    try:
        asyncio.run(run_relay())
    except KeyboardInterrupt:  # pragma: no cover - manual shutdown
        logger.info("outbox_relay_stopped")
