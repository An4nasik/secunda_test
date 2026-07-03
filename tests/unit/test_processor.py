"""PaymentProcessor: idempotency and retry/DLQ routing (hand-rolled fakes)."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.broker import DLX_EXCHANGE, RETRY_EXCHANGE
from app.config import Settings
from app.consumer.handler import ATTEMPT_HEADER, PaymentProcessor, parse_attempt
from app.models import Currency, Payment, PaymentStatus
from app.schemas import PaymentCreatedEvent


class RecordingBroker:
    """Captures publish calls instead of talking to RabbitMQ."""

    def __init__(self):
        self.published = []

    async def publish(self, body, **kwargs):
        self.published.append((body, kwargs))


class FakeSession:
    """Minimal AsyncSession stand-in returning one payment row."""

    def __init__(self, payment):
        self.payment = payment
        self.commits = 0

    async def get(self, model, payment_id, with_for_update=False):
        return self.payment

    async def commit(self):
        self.commits += 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class ExplodingSessionFactory:
    """Session factory for simulating a database outage."""

    def __call__(self):
        raise RuntimeError("database is down")


def make_payment(**overrides) -> Payment:
    defaults = {
        "id": uuid.uuid4(),
        "amount": Decimal("10.00"),
        "currency": Currency.USD,
        "status": PaymentStatus.PENDING,
        "idempotency_key": "key",
        "request_hash": "h" * 64,
        "webhook_url": "https://client.example.com/hook",
        "metadata_": None,
        "description": None,
        "processed_at": None,
        "webhook_delivered_at": None,
    }
    return Payment(**{**defaults, **overrides})


def make_settings(**overrides) -> Settings:
    return Settings(
        **{"max_retries": 3, "retry_base_delay_seconds": 2.0, "_env_file": None, **overrides}
    )


def make_event(payment: Payment | None = None) -> PaymentCreatedEvent:
    payment_id = payment.id if payment is not None else uuid.uuid4()
    return PaymentCreatedEvent(payment_id=payment_id, occurred_at=datetime.now(UTC))


def make_processor(
    session_factory, broker, http_handler=None, gateway=None, settings=None
) -> PaymentProcessor:
    transport = httpx.MockTransport(http_handler or (lambda request: httpx.Response(200)))
    return PaymentProcessor(
        session_factory=session_factory,
        broker=broker,
        http_client=httpx.AsyncClient(transport=transport),
        settings=settings or make_settings(),
        gateway=gateway,
    )


async def test_pending_payment_is_charged_and_webhook_sent():
    payment = make_payment()
    session = FakeSession(payment)
    broker = RecordingBroker()
    webhooks = []

    def http_handler(request):
        webhooks.append(request)
        return httpx.Response(200)

    async def gateway():
        return PaymentStatus.SUCCEEDED

    processor = make_processor(lambda: session, broker, http_handler, gateway)
    await processor.process(make_event(payment), headers={})

    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert session.commits == 2, "status must be committed before the webhook attempt"
    assert len(webhooks) == 1
    assert broker.published == [], "no retries on the happy path"


async def test_redelivery_after_processing_skips_gateway_and_resends_webhook():
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))
    session = FakeSession(payment)
    broker = RecordingBroker()
    webhooks = []

    def http_handler(request):
        webhooks.append(request)
        return httpx.Response(200)

    async def gateway():
        raise AssertionError("gateway must not be called twice")

    processor = make_processor(lambda: session, broker, http_handler, gateway)
    await processor.process(make_event(payment), headers={ATTEMPT_HEADER: 1})

    assert len(webhooks) == 1
    assert payment.webhook_delivered_at is not None


async def test_fully_processed_payment_is_a_no_op():
    payment = make_payment(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_delivered_at=datetime.now(UTC),
    )
    session = FakeSession(payment)
    broker = RecordingBroker()

    async def gateway():
        raise AssertionError("gateway must not be called")

    def http_handler(request):
        raise AssertionError("webhook must not be re-sent")

    processor = make_processor(lambda: session, broker, http_handler, gateway)
    await processor.process(make_event(payment), headers={ATTEMPT_HEADER: 2})

    assert session.commits == 0
    assert broker.published == []


async def test_failed_webhook_routes_event_to_first_retry_queue():
    payment = make_payment(status=PaymentStatus.FAILED, processed_at=datetime.now(UTC))
    session = FakeSession(payment)
    broker = RecordingBroker()

    processor = make_processor(lambda: session, broker, lambda request: httpx.Response(503))
    await processor.process(make_event(payment), headers={})

    assert len(broker.published) == 1
    body, kwargs = broker.published[0]
    assert kwargs["exchange"] is RETRY_EXCHANGE
    assert kwargs["routing_key"] == "payments.new.retry.1"
    assert kwargs["headers"][ATTEMPT_HEADER] == 1
    assert kwargs["persist"] is True
    assert body["payment_id"] == str(payment.id)


async def test_database_outage_routes_to_next_retry_queue():
    broker = RecordingBroker()
    processor = make_processor(ExplodingSessionFactory(), broker)

    await processor.process(make_event(), headers={ATTEMPT_HEADER: 2})

    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is RETRY_EXCHANGE
    assert kwargs["routing_key"] == "payments.new.retry.3"
    assert kwargs["headers"][ATTEMPT_HEADER] == 3


async def test_exhausted_retries_dead_letter_the_event():
    broker = RecordingBroker()
    processor = make_processor(ExplodingSessionFactory(), broker)

    await processor.process(make_event(), headers={ATTEMPT_HEADER: 3})

    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is DLX_EXCHANGE
    assert kwargs["routing_key"] == "payments.new.dlq"
    assert kwargs["headers"]["x-error-type"] == "RuntimeError"
    assert "database is down" in kwargs["headers"]["x-error"]


async def test_missing_payment_is_retried_not_dropped():
    payment_missing_session = FakeSession(None)
    broker = RecordingBroker()
    processor = make_processor(lambda: payment_missing_session, broker)

    await processor.process(make_event(), headers={})

    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is RETRY_EXCHANGE
    assert kwargs["headers"][ATTEMPT_HEADER] == 1


# --- corner cases -----------------------------------------------------------


async def test_gateway_crash_is_a_processing_error_not_a_decline():
    payment = make_payment()
    session = FakeSession(payment)
    broker = RecordingBroker()

    async def gateway():
        raise RuntimeError("gateway emulator blew up")

    processor = make_processor(lambda: session, broker, gateway=gateway)
    await processor.process(make_event(payment), headers={})

    assert payment.status is PaymentStatus.PENDING, "a crash must not fabricate an outcome"
    assert session.commits == 0
    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is RETRY_EXCHANGE
    assert kwargs["routing_key"] == "payments.new.retry.1"


@pytest.mark.parametrize("corrupt", ["garbage", None, [1], {"n": 1}])
async def test_corrupt_attempt_header_restarts_the_ladder(corrupt):
    broker = RecordingBroker()
    processor = make_processor(ExplodingSessionFactory(), broker)

    await processor.process(make_event(), headers={ATTEMPT_HEADER: corrupt})

    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is RETRY_EXCHANGE, "corrupt header must not skip retries"
    assert kwargs["headers"][ATTEMPT_HEADER] == 1


async def test_negative_attempt_header_is_clamped_to_zero():
    assert parse_attempt({ATTEMPT_HEADER: -5}) == 0
    assert parse_attempt({ATTEMPT_HEADER: "2"}) == 2
    assert parse_attempt({}) == 0


async def test_attempt_far_beyond_max_goes_to_dlq_not_a_ghost_queue():
    broker = RecordingBroker()
    processor = make_processor(ExplodingSessionFactory(), broker)

    await processor.process(make_event(), headers={ATTEMPT_HEADER: 99})

    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is DLX_EXCHANGE
    assert kwargs["routing_key"] == "payments.new.dlq"


async def test_zero_max_retries_dead_letters_on_first_failure():
    broker = RecordingBroker()
    processor = make_processor(
        ExplodingSessionFactory(), broker, settings=make_settings(max_retries=0)
    )

    await processor.process(make_event(), headers={})

    _, kwargs = broker.published[0]
    assert kwargs["exchange"] is DLX_EXCHANGE
    assert kwargs["routing_key"] == "payments.new.dlq"
