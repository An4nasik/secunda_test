"""Static API-key authentication."""

import uuid

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.api.app import create_app
from app.api.deps import require_api_key
from app.config import Settings, get_settings


def make_settings(key: str) -> Settings:
    return Settings(api_key=SecretStr(key), _env_file=None)


def test_valid_key_is_accepted():
    require_api_key("expected", make_settings("expected"))


@pytest.mark.parametrize("provided", [None, "", "wrong", "expected "])
def test_invalid_key_is_rejected(provided):
    with pytest.raises(HTTPException) as exc_info:
        require_api_key(provided, make_settings("expected"))
    assert exc_info.value.status_code == 401


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-key")
    get_settings.cache_clear()
    with TestClient(create_app()) as test_client:
        yield test_client
    get_settings.cache_clear()


def test_healthz_needs_no_auth(client):
    assert client.get("/healthz").status_code == 200


def test_endpoints_reject_missing_key(client):
    response = client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 401


def test_endpoints_reject_wrong_key(client):
    response = client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "nope"})
    assert response.status_code == 401


def test_empty_configured_key_fails_startup(monkeypatch):
    # An env var (even an empty one) takes precedence over any local .env file.
    monkeypatch.setenv("API_KEY", "")
    get_settings.cache_clear()
    with pytest.raises(RuntimeError), TestClient(create_app()):
        pass
    get_settings.cache_clear()
