"""Payment use cases: idempotent creation with an outbox event, retrieval."""

import hashlib
import json
import uuid

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage, Payment
from app.schemas import PAYMENTS_NEW_ROUTING_KEY, PaymentCreatedEvent, PaymentCreateRequest

logger = structlog.get_logger(__name__)


class IdempotencyConflictError(Exception):
    """The same ``Idempotency-Key`` was reused with a different request body."""

    def __init__(self) -> None:
        super().__init__("Idempotency-Key already used with a different request body")


def request_fingerprint(body: PaymentCreateRequest) -> str:
    """Return a canonical SHA-256 fingerprint of the request body.

    Keys are sorted so that two payloads differing only in JSON key order
    produce the same fingerprint.
    """
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, body: PaymentCreateRequest, idempotency_key: str
) -> tuple[Payment, bool]:
    """Create a payment and its outbox event in one transaction.

    Args:
        session: Database session; committed on success.
        body: Validated request body.
        idempotency_key: Client-supplied deduplication key.

    Returns:
        ``(payment, replayed)`` — ``replayed`` is ``True`` when an existing
        payment with the same idempotency key and body was returned.

    Raises:
        IdempotencyConflictError: the key was reused with a different body.
    """
    fingerprint = request_fingerprint(body)

    existing = await _find_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _replay(existing, fingerprint), True

    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata_=body.metadata,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
        webhook_url=str(body.webhook_url),
    )
    session.add(payment)
    try:
        await session.flush()
    except IntegrityError:
        # Lost a race against a concurrent request with the same key: the
        # transaction is aborted, so recover the winner's row and replay.
        await session.rollback()
        existing = await _find_by_idempotency_key(session, idempotency_key)
        if existing is None:  # pragma: no cover - not an idempotency race
            raise
        return _replay(existing, fingerprint), True

    event = PaymentCreatedEvent(payment_id=payment.id, occurred_at=payment.created_at)
    session.add(
        OutboxMessage(
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            payload=event.model_dump(mode="json"),
        )
    )
    await session.commit()
    logger.info("payment_created", payment_id=str(payment.id), currency=payment.currency)
    return payment, False


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    """Return the payment by id, or ``None`` when it does not exist."""
    return await session.get(Payment, payment_id)


async def _find_by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    result = await session.execute(select(Payment).where(Payment.idempotency_key == key))
    return result.scalar_one_or_none()


def _replay(existing: Payment, fingerprint: str) -> Payment:
    if existing.request_hash != fingerprint:
        raise IdempotencyConflictError
    logger.info("payment_replayed", payment_id=str(existing.id))
    return existing
