"""Broker topology: retry queue parameters implement exponential backoff."""

from app.broker import (
    PAYMENTS_EXCHANGE,
    retry_delay_seconds,
    retry_queue,
    retry_queue_name,
)


def test_retry_delays_grow_exponentially():
    delays = [retry_delay_seconds(attempt, base_delay=2.0) for attempt in (1, 2, 3)]
    assert delays == [2.0, 4.0, 8.0]


def test_retry_queue_parks_message_and_returns_it_to_work_queue():
    queue = retry_queue(3, base_delay=2.0)
    assert queue.name == retry_queue_name(3) == "payments.new.retry.3"
    assert queue.durable is True
    expected = {
        "x-message-ttl": 8000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": "payments.new",
    }
    assert expected.items() <= queue.arguments.items()
