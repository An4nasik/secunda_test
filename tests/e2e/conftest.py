"""Fixtures for end-to-end tests against the running docker compose stack."""

import asyncio
import json
import os
import socket
from collections.abc import AsyncIterator

import httpx
import pytest
import uvicorn

API_URL = os.environ.get("E2E_API_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "dev-secret-key")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

# Containers reach the test process through the docker host gateway.
WEBHOOK_HOST = os.environ.get("E2E_WEBHOOK_HOST", "host.docker.internal")


class WebhookCatcher:
    """Minimal ASGI app that records webhook POSTs it receives."""

    def __init__(self):
        self.received: asyncio.Queue = asyncio.Queue()

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        await self.received.put(json.loads(body))
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def next_payload(self, timeout: float = 60) -> dict:
        return await asyncio.wait_for(self.received.get(), timeout)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


@pytest.fixture
async def webhook_catcher() -> AsyncIterator[tuple[WebhookCatcher, str]]:
    """Run an in-process webhook receiver; yield it with its container-visible URL."""
    catcher = WebhookCatcher()
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(catcher, host="0.0.0.0", port=port, log_level="warning", lifespan="off")
    )
    serve_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    yield catcher, f"http://{WEBHOOK_HOST}:{port}/hook"
    server.should_exit = True
    await serve_task


@pytest.fixture
async def api() -> AsyncIterator[httpx.AsyncClient]:
    """HTTP client for the API with the key pre-set."""
    async with httpx.AsyncClient(
        base_url=API_URL, headers={"X-API-Key": API_KEY}, timeout=10
    ) as client:
        yield client
