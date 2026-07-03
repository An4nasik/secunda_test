"""Request validation at the HTTP boundary (no database required).

Every scenario here fails validation before any endpoint logic runs, so the
tests exercise the real ASGI app without infrastructure.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

from app.api.app import create_app
from app.config import get_settings

VALID_BODY = {
    "amount": "10.00",
    "currency": "USD",
    "webhook_url": "https://client.example.com/hook",
}
AUTH = {"X-API-Key": "test-key"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-key")
    get_settings.cache_clear()
    with TestClient(create_app()) as test_client:
        yield test_client
    get_settings.cache_clear()


def test_non_uuid_payment_id_is_422(client):
    response = client.get("/api/v1/payments/not-a-uuid", headers=AUTH)
    assert response.status_code == 422


def test_malformed_json_body_is_422(client):
    response = client.post(
        "/api/v1/payments",
        content=b'{"amount": ',
        headers={**AUTH, "Idempotency-Key": "k", "Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_empty_idempotency_key_is_422(client):
    response = client.post(
        "/api/v1/payments", json=VALID_BODY, headers={**AUTH, "Idempotency-Key": ""}
    )
    assert response.status_code == 422


def test_oversized_idempotency_key_is_422(client):
    response = client.post(
        "/api/v1/payments", json=VALID_BODY, headers={**AUTH, "Idempotency-Key": "k" * 256}
    )
    assert response.status_code == 422


def test_validation_errors_still_require_auth(client):
    # Even a garbage request must not reveal anything without a valid key.
    response = client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401
