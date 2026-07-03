"""FastStream application factory for the consumer service."""

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage

from app.broker import PAYMENTS_EXCHANGE, create_broker, declare_topology, payments_new_queue
from app.config import get_settings
from app.consumer.handler import PaymentProcessor
from app.db import build_engine, build_session_factory
from app.logs import configure_logging
from app.schemas import PaymentCreatedEvent


def create_app() -> FastStream:
    """Create the consumer application with all dependencies wired."""
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_json)

    broker = create_broker(settings)
    engine = build_engine(settings.database_url)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)
    processor = PaymentProcessor(
        session_factory=build_session_factory(engine),
        broker=broker,
        http_client=http_client,
        settings=settings,
    )

    async def declare() -> None:
        await declare_topology(broker, settings)

    async def cleanup() -> None:
        await http_client.aclose()
        await engine.dispose()

    app = FastStream(broker, after_startup=[declare], after_shutdown=[cleanup])

    @broker.subscriber(payments_new_queue, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        """Consume ``payments.new`` and delegate to the processor."""
        await processor.process(event, dict(message.headers or {}))

    return app
