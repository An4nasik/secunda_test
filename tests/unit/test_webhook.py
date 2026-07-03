"""Webhook delivery semantics: only a 2xx response counts as delivered."""

import uuid
from decimal import Decimal

import httpx
import pytest

from app.consumer.webhook import WebhookDeliveryError, send_webhook
from app.models import Currency, PaymentStatus
from app.schemas import WebhookPayload

PAYLOAD = WebhookPayload(
    payment_id=uuid.uuid4(),
    status=PaymentStatus.SUCCEEDED,
    amount=Decimal("10.00"),
    currency=Currency.USD,
    description=None,
    metadata=None,
    processed_at=None,
)


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_2xx_counts_as_delivered():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200)

    async with make_client(handler) as client:
        await send_webhook(client, "https://client.example.com/hook", PAYLOAD)

    assert len(requests) == 1
    assert requests[0].headers["content-type"] == "application/json"
    assert b'"status":"succeeded"' in requests[0].content


@pytest.mark.parametrize("status_code", [400, 404, 500, 503])
async def test_non_2xx_is_a_delivery_error(status_code):
    async with make_client(lambda request: httpx.Response(status_code)) as client:
        with pytest.raises(WebhookDeliveryError):
            await send_webhook(client, "https://client.example.com/hook", PAYLOAD)


async def test_transport_error_is_a_delivery_error():
    def handler(request):
        raise httpx.ConnectError("connection refused")

    async with make_client(handler) as client:
        with pytest.raises(WebhookDeliveryError):
            await send_webhook(client, "https://client.example.com/hook", PAYLOAD)


async def test_timeout_is_a_delivery_error():
    def handler(request):
        raise httpx.ReadTimeout("endpoint accepted the connection but never answered")

    async with make_client(handler) as client:
        with pytest.raises(WebhookDeliveryError):
            await send_webhook(client, "https://client.example.com/hook", PAYLOAD)


async def test_redirect_is_not_followed_and_not_a_delivery():
    # A 3xx answer means nobody actually consumed the notification: following
    # redirects would silently deliver payment data to an unexpected host.
    def handler(request):
        return httpx.Response(302, headers={"Location": "https://elsewhere.example/hook"})

    async with make_client(handler) as client:
        with pytest.raises(WebhookDeliveryError):
            await send_webhook(client, "https://client.example.com/hook", PAYLOAD)
