"""Outbox relay batch semantics: publish everything or mark nothing."""

import pytest

from app.broker import PAYMENTS_EXCHANGE
from app.models import OutboxMessage
from app.outbox.relay import publish_pending_batch


class RecordingBroker:
    def __init__(self):
        self.published = []

    async def publish(self, body, **kwargs):
        self.published.append((body, kwargs))


class ExplodingBroker(RecordingBroker):
    """Fails on the N-th publish (1-based), like a broker dying mid-batch."""

    def __init__(self, fail_on: int):
        super().__init__()
        self._fail_on = fail_on

    async def publish(self, body, **kwargs):
        if len(self.published) + 1 == self._fail_on:
            raise ConnectionError("broker connection lost")
        await super().publish(body, **kwargs)


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class FakeSession:
    """Returns the given outbox rows for the batch select."""

    def __init__(self, rows):
        self._rows = rows
        self.commits = 0

    async def execute(self, statement):
        return FakeResult(self._rows)

    async def commit(self):
        self.commits += 1


def make_rows(count: int) -> list[OutboxMessage]:
    return [
        OutboxMessage(
            id=n, routing_key="payments.new", payload={"payment_id": f"p{n}"}, published_at=None
        )
        for n in range(1, count + 1)
    ]


async def test_batch_publishes_persistent_messages_and_marks_rows():
    rows = make_rows(3)
    session = FakeSession(rows)
    broker = RecordingBroker()

    published = await publish_pending_batch(session, broker, batch_size=100)

    assert published == 3
    assert session.commits == 1
    assert all(row.published_at is not None for row in rows)
    for row, (body, kwargs) in zip(rows, broker.published, strict=True):
        assert body == row.payload
        assert kwargs["exchange"] is PAYMENTS_EXCHANGE
        assert kwargs["routing_key"] == "payments.new"
        assert kwargs["persist"] is True
        assert kwargs["message_id"] == str(row.id)


async def test_empty_outbox_is_a_cheap_no_op():
    session = FakeSession([])
    broker = RecordingBroker()

    assert await publish_pending_batch(session, broker, batch_size=100) == 0
    assert broker.published == []


async def test_mid_batch_failure_commits_nothing():
    # The broker dies after the first of three messages: the transaction must
    # not be committed, so all three rows stay unpublished and the whole batch
    # is retried later. The consumer deduplicates the one duplicate delivery.
    rows = make_rows(3)
    session = FakeSession(rows)
    broker = ExplodingBroker(fail_on=2)

    with pytest.raises(ConnectionError):
        await publish_pending_batch(session, broker, batch_size=100)

    assert session.commits == 0
    assert len(broker.published) == 1
