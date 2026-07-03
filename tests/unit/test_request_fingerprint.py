"""Canonical request fingerprinting used for idempotency conflict detection."""

from app.schemas import PaymentCreateRequest
from app.services.payments import request_fingerprint

BASE = {
    "amount": "10.00",
    "currency": "USD",
    "metadata": {"a": 1, "b": 2},
    "webhook_url": "https://client.example.com/hook",
}


def fingerprint(**overrides) -> str:
    return request_fingerprint(PaymentCreateRequest.model_validate({**BASE, **overrides}))


def test_same_body_produces_same_fingerprint():
    assert fingerprint() == fingerprint()


def test_metadata_key_order_does_not_matter():
    assert fingerprint(metadata={"b": 2, "a": 1}) == fingerprint()


def test_different_amount_changes_fingerprint():
    assert fingerprint(amount="10.01") != fingerprint()


def test_different_metadata_changes_fingerprint():
    assert fingerprint(metadata={"a": 1}) != fingerprint()


# --- corner cases -----------------------------------------------------------


def test_explicit_null_equals_omitted_field():
    explicit = request_fingerprint(
        PaymentCreateRequest.model_validate({**BASE, "description": None})
    )
    omitted = request_fingerprint(PaymentCreateRequest.model_validate(BASE))
    assert explicit == omitted


def test_trailing_zeros_change_fingerprint():
    # "10.5" and "10.50" are byte-different bodies: like Stripe, a replay must
    # send the exact original payload, otherwise it is a 409 conflict.
    assert fingerprint(amount="10.5") != fingerprint(amount="10.50")


def test_unicode_is_stable():
    body = {"description": "Заказ №42 🚀", "metadata": {"к": "спасибо"}}
    assert fingerprint(**body) == fingerprint(**body)
    assert fingerprint(**body) != fingerprint()
