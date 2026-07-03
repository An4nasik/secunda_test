"""The single ``payments.new`` consumer: gateway call, DB update, webhook.

Failure handling contract:

* A gateway decline is a business outcome — the payment becomes ``failed``
  and the webhook is still delivered.
* Any *processing* error (database down, webhook endpoint unavailable, ...)
  re-queues the event into the delayed retry queue for the next attempt, or
  into the DLQ once ``max_retries`` is exhausted.
* The handler is idempotent: a redelivered event never charges the gateway
  twice (payment status is checked under a row lock) and never re-sends an
  already acknowledged webhook (``webhook_delivered_at`` marker).
"""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import (
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE,
    RETRY_EXCHANGE,
    retry_delay_seconds,
    retry_queue_name,
)
from app.config import Settings
from app.consumer.gateway import process_payment
from app.consumer.webhook import send_webhook
from app.models import Payment, PaymentStatus
from app.schemas import PaymentCreatedEvent, WebhookPayload

ATTEMPT_HEADER = "x-attempt"

logger = structlog.get_logger(__name__)

Gateway = Callable[[], Awaitable[PaymentStatus]]


class PaymentNotFoundError(Exception):
    """The event references a payment that is missing from the database."""

    def __init__(self, payment_id: uuid.UUID) -> None:
        super().__init__(f"payment {payment_id} not found")


def parse_attempt(headers: dict[str, Any]) -> int:
    """Return the current attempt number, tolerating missing or corrupt headers.

    A malformed ``x-attempt`` value (possible after manual re-publishing from
    the management UI) must not crash the handler before its error handling
    even starts: the ladder restarts from zero instead, which is bounded by
    ``max_retries`` and therefore safe.
    """
    raw = headers.get(ATTEMPT_HEADER, 0)
    try:
        return max(0, int(raw))
    except TypeError, ValueError:
        logger.warning("attempt_header_corrupt", value=repr(raw))
        return 0


class PaymentProcessor:
    """Orchestrates processing of one payment event with retry/DLQ routing."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        http_client: httpx.AsyncClient,
        settings: Settings,
        gateway: Gateway | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._http_client = http_client
        self._settings = settings
        self._gateway = gateway or (lambda: process_payment(settings))

    async def process(self, event: PaymentCreatedEvent, headers: dict[str, Any]) -> None:
        """Process the event; on failure route it to a retry queue or the DLQ.

        Never raises for processing errors — the message is explicitly
        republished and the original gets acked. An exception escapes only if
        the failure routing itself fails; the work queue then dead-letters
        the rejected message to the DLQ, so it is never lost.
        """
        attempt = parse_attempt(headers)
        log = logger.bind(payment_id=str(event.payment_id), attempt=attempt)
        try:
            await self._process_once(event, log)
        except Exception as exc:
            await self._route_failure(event, attempt, exc, log)

    async def _process_once(
        self, event: PaymentCreatedEvent, log: structlog.stdlib.BoundLogger
    ) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, event.payment_id, with_for_update=True)
            if payment is None:
                raise PaymentNotFoundError(event.payment_id)

            if payment.status is PaymentStatus.PENDING:
                payment.status = await self._gateway()
                payment.processed_at = datetime.now(UTC)
                # Commit before the webhook: the payment outcome must survive
                # even if the notification fails and the event is retried.
                await session.commit()
                log.info("payment_processed", status=payment.status)

            if payment.webhook_delivered_at is None:
                payload = WebhookPayload(
                    payment_id=payment.id,
                    status=payment.status,
                    amount=payment.amount,
                    currency=payment.currency,
                    description=payment.description,
                    metadata=payment.metadata_,
                    processed_at=payment.processed_at,
                )
                await send_webhook(self._http_client, payment.webhook_url, payload)
                payment.webhook_delivered_at = datetime.now(UTC)
                await session.commit()
                log.info("webhook_delivered", url=payment.webhook_url)

    async def _route_failure(
        self,
        event: PaymentCreatedEvent,
        attempt: int,
        exc: Exception,
        log: structlog.stdlib.BoundLogger,
    ) -> None:
        next_attempt = attempt + 1
        body = event.model_dump(mode="json")
        if next_attempt <= self._settings.max_retries:
            delay = retry_delay_seconds(next_attempt, self._settings.retry_base_delay_seconds)
            log.warning(
                "payment_processing_failed",
                error=str(exc),
                next_attempt=next_attempt,
                retry_in_seconds=delay,
            )
            await self._broker.publish(
                body,
                exchange=RETRY_EXCHANGE,
                routing_key=retry_queue_name(next_attempt),
                persist=True,
                headers={ATTEMPT_HEADER: next_attempt},
                correlation_id=str(event.payment_id),
            )
        else:
            log.error("payment_dead_lettered", error=str(exc), attempts=attempt + 1)
            await self._broker.publish(
                body,
                exchange=DLX_EXCHANGE,
                routing_key=DLQ_ROUTING_KEY,
                persist=True,
                headers={
                    ATTEMPT_HEADER: attempt,
                    "x-error": str(exc)[:500],
                    "x-error-type": type(exc).__name__,
                },
                correlation_id=str(event.payment_id),
            )
