"""Webhook delivery to the client-provided URL."""

import httpx

from app.schemas import WebhookPayload


class WebhookDeliveryError(Exception):
    """The webhook endpoint did not acknowledge the notification."""


async def send_webhook(client: httpx.AsyncClient, url: str, payload: WebhookPayload) -> None:
    """POST the payload to ``url``; only a 2xx response counts as delivered.

    Raises:
        WebhookDeliveryError: on transport errors, timeouts or non-2xx codes.
    """
    try:
        response = await client.post(
            url,
            content=payload.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise WebhookDeliveryError(str(exc)) from exc
