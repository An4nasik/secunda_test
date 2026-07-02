"""RabbitMQ topology: exchanges, work queue, delayed retry queues, DLQ.

Retries are implemented broker-side without plugins: each retry attempt has a
parking queue with a fixed message TTL and a dead-letter route back to the
work queue. Expired messages therefore re-enter ``payments.new`` after an
exponentially growing delay (``retry_base_delay_seconds * 2**(attempt-1)``).
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings
from app.schemas import PAYMENTS_NEW_ROUTING_KEY

DLQ_ROUTING_KEY = "payments.new.dlq"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(PAYMENTS_NEW_ROUTING_KEY, durable=True)
dlq_queue = RabbitQueue(DLQ_ROUTING_KEY, durable=True)


def retry_delay_seconds(attempt: int, base_delay: float) -> float:
    """Return the exponential backoff delay before retry ``attempt`` (1-based)."""
    return base_delay * 2 ** (attempt - 1)


def retry_queue_name(attempt: int) -> str:
    """Return the parking queue name for retry ``attempt`` (1-based)."""
    return f"{PAYMENTS_NEW_ROUTING_KEY}.retry.{attempt}"


def retry_queue(attempt: int, base_delay: float) -> RabbitQueue:
    """Build the parking queue for retry ``attempt``.

    The queue has no consumers: messages sit here until the TTL expires and
    the broker dead-letters them back into the work queue.
    """
    return RabbitQueue(
        retry_queue_name(attempt),
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay_seconds(attempt, base_delay) * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


def create_broker(settings: Settings) -> RabbitBroker:
    """Create a RabbitMQ broker instance (robust connection, no topology yet)."""
    return RabbitBroker(settings.rabbitmq_url)


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Declare all exchanges, queues and bindings idempotently.

    Both the consumer and the outbox relay call this on startup, so the
    services can boot in any order.
    """
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry_exchange = await broker.declare_exchange(RETRY_EXCHANGE)
    dlx_exchange = await broker.declare_exchange(DLX_EXCHANGE)

    work = await broker.declare_queue(payments_new_queue)
    await work.bind(payments_exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY)

    dead_letters = await broker.declare_queue(dlq_queue)
    await dead_letters.bind(dlx_exchange, routing_key=DLQ_ROUTING_KEY)

    for attempt in range(1, settings.max_retries + 1):
        parking = await broker.declare_queue(
            retry_queue(attempt, settings.retry_base_delay_seconds)
        )
        await parking.bind(retry_exchange, routing_key=retry_queue_name(attempt))
