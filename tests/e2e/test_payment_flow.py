"""End-to-end scenarios against the docker compose environment.

Run with the stack up:

    docker compose up -d --build
    uv run pytest -m e2e
"""

import asyncio
import os
import time
import uuid

import aio_pika
import pytest

pytestmark = pytest.mark.e2e

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
DLQ_NAME = "payments.new.dlq"


def make_body(webhook_url: str, **overrides) -> dict:
    return {
        "amount": "149.99",
        "currency": "RUB",
        "description": "e2e payment",
        "metadata": {"order_id": 42},
        "webhook_url": webhook_url,
        **overrides,
    }


async def wait_payment(api, payment_id: str, predicate, timeout: float = 60) -> dict:
    """Poll the payment until ``predicate(payment)`` holds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = await api.get(f"/api/v1/payments/{payment_id}")
        assert response.status_code == 200
        payment = response.json()
        if predicate(payment):
            return payment
        await asyncio.sleep(0.5)
    raise AssertionError(f"payment {payment_id} did not reach the expected state in {timeout}s")


def is_final(payment: dict) -> bool:
    return payment["status"] != "pending"


async def test_health_and_auth(api):
    health = await api.get("/healthz", headers={"X-API-Key": ""})
    assert health.status_code == 200

    unauthorized = await api.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "bad"})
    assert unauthorized.status_code == 401


async def test_payment_is_processed_and_webhook_delivered(api, webhook_catcher):
    catcher, webhook_url = webhook_catcher
    body = make_body(webhook_url)
    key = f"e2e-{uuid.uuid4()}"

    created = await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})
    assert created.status_code == 202
    accepted = created.json()
    assert accepted["status"] == "pending"
    payment_id = accepted["payment_id"]

    payload = await catcher.next_payload()
    assert payload["payment_id"] == payment_id
    assert payload["status"] in {"succeeded", "failed"}
    assert payload["amount"] == "149.99"
    assert payload["metadata"] == {"order_id": 42}

    # The delivery marker is committed just after our receiver responds 200,
    # so poll briefly instead of asserting immediately.
    payment = await wait_payment(
        api, payment_id, lambda p: p["webhook_delivered_at"] is not None, timeout=10
    )
    assert payment["status"] == payload["status"]
    assert payment["processed_at"] is not None


async def test_idempotent_replay_and_conflict(api, webhook_catcher):
    _, webhook_url = webhook_catcher
    body = make_body(webhook_url)
    key = f"e2e-{uuid.uuid4()}"

    first = await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})
    assert first.status_code == 202

    replay = await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})
    assert replay.status_code == 202
    assert replay.headers.get("idempotency-replayed") == "true"
    assert replay.json()["payment_id"] == first.json()["payment_id"]

    conflicting = await api.post(
        "/api/v1/payments",
        json=make_body(webhook_url, amount="1.00"),
        headers={"Idempotency-Key": key},
    )
    assert conflicting.status_code == 409

    missing_key = await api.post("/api/v1/payments", json=body)
    assert missing_key.status_code == 422


async def test_poisoned_message_goes_to_dlq_and_consumer_survives(api, webhook_catcher):
    # A message that cannot even be parsed into PaymentCreatedEvent must not
    # kill or wedge the consumer: the work queue's DLX routes the reject
    # straight to the DLQ, and the next valid payment is processed as usual.
    catcher, webhook_url = webhook_catcher

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        dlq = await channel.get_queue(DLQ_NAME)
        await dlq.purge()

        exchange = await channel.get_exchange("payments")
        poison = b'{"this": "is not a payment event"}'
        await exchange.publish(
            aio_pika.Message(poison, content_type="application/json"),
            routing_key="payments.new",
        )

        deadline = time.monotonic() + 30
        dead_letter = None
        while time.monotonic() < deadline:
            message = await dlq.get(fail=False, no_ack=True)
            if message is not None:
                dead_letter = message
                break
            await asyncio.sleep(0.5)

        assert dead_letter is not None, "poisoned message never reached the DLQ"
        assert dead_letter.body == poison
        death = (dead_letter.headers or {}).get("x-death")
        assert death and death[0]["queue"] == "payments.new"

    # The consumer is still alive: a real payment sails through end to end.
    created = await api.post(
        "/api/v1/payments",
        json=make_body(webhook_url),
        headers={"Idempotency-Key": f"e2e-poison-{uuid.uuid4()}"},
    )
    assert created.status_code == 202
    payload = await catcher.next_payload()
    assert payload["payment_id"] == created.json()["payment_id"]


async def test_undeliverable_webhook_ends_in_dlq(api):
    # Port 9 (discard) on the host gateway: connection is always refused,
    # so every webhook attempt fails and the event must reach the DLQ
    # after MAX_RETRIES delayed redeliveries.
    body = make_body("http://host.docker.internal:9/hook")
    key = f"e2e-dlq-{uuid.uuid4()}"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        dlq = await channel.get_queue(DLQ_NAME)
        await dlq.purge()

        created = await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})
        assert created.status_code == 202
        payment_id = created.json()["payment_id"]

        # The payment itself must still be processed to a final status.
        payment = await wait_payment(api, payment_id, is_final)
        assert payment["status"] in {"succeeded", "failed"}
        assert payment["webhook_delivered_at"] is None

        # Filter by payment_id: earlier tests may leave their own dead
        # letters behind (their webhook receivers are gone by now).
        deadline = time.monotonic() + 120
        dead_letter = None
        while dead_letter is None and time.monotonic() < deadline:
            message = await dlq.get(fail=False, no_ack=True)
            if message is not None and payment_id in message.body.decode():
                dead_letter = message
            elif message is None:
                await asyncio.sleep(1)

        assert dead_letter is not None, "event never reached the DLQ"
        assert dead_letter.headers["x-attempt"] == MAX_RETRIES
        assert dead_letter.headers["x-error-type"] == "WebhookDeliveryError"
