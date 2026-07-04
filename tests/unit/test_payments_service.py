"""create_payment: the concurrent same-key race is resolved, never leaked."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Currency, Payment, PaymentStatus
from app.schemas import PaymentCreateRequest
from app.services.payments import (
    IdempotencyConflictError,
    create_payment,
    request_fingerprint,
)

BODY = PaymentCreateRequest.model_validate(
    {
        "amount": "10.00",
        "currency": "USD",
        "webhook_url": "https://client.example.com/hook",
    }
)


def make_winner(fingerprint: str) -> Payment:
    """The payment a concurrent request managed to insert first."""
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        status=PaymentStatus.PENDING,
        idempotency_key="race-key",
        request_hash=fingerprint,
        webhook_url="https://client.example.com/hook",
        created_at=datetime.now(UTC),
    )


class FakeScalarResult:
    def __init__(self, row):
        self._row = row

    def scalar_one_or_none(self):
        return self._row


class RacingSession:
    """Simulates losing an idempotency-key race.

    The initial lookup sees nothing, the INSERT hits the unique constraint,
    and the post-rollback lookup returns the concurrent winner.
    """

    def __init__(self, winner: Payment):
        self._winner = winner
        self._lost_race = False
        self.rolled_back = False
        self.commits = 0

    async def execute(self, statement):
        return FakeScalarResult(self._winner if self._lost_race else None)

    def add(self, instance):
        pass

    async def flush(self):
        self._lost_race = True
        raise IntegrityError("INSERT", {}, Exception("uq_payments_idempotency_key"))

    async def rollback(self):
        self.rolled_back = True

    async def commit(self):
        self.commits += 1


async def test_lost_race_returns_winner_as_replay():
    winner = make_winner(request_fingerprint(BODY))
    session = RacingSession(winner)

    payment, replayed = await create_payment(session, BODY, "race-key")

    assert replayed is True
    assert payment is winner
    assert session.rolled_back is True
    assert session.commits == 0, "the loser must not write anything"


async def test_lost_race_with_different_body_is_a_conflict():
    winner = make_winner("another-body-fingerprint")
    session = RacingSession(winner)

    with pytest.raises(IdempotencyConflictError):
        await create_payment(session, BODY, "race-key")
