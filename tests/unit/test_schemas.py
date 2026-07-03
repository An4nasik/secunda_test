"""Validation rules of the public API contract."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.models import Currency
from app.schemas import PaymentCreateRequest

VALID_BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://client.example.com/webhooks/payments",
}


def make_body(**overrides):
    return {**VALID_BODY, **overrides}


def test_valid_request_parses_to_decimal():
    request = PaymentCreateRequest.model_validate(make_body())
    assert request.amount == Decimal("100.50")
    assert request.currency is Currency.RUB


@pytest.mark.parametrize("amount", ["0", "-1", "10.123", "12345678901.00"])
def test_invalid_amounts_rejected(amount):
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(amount=amount))


def test_unsupported_currency_rejected():
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(currency="GBP"))


@pytest.mark.parametrize("url", ["not-a-url", "ftp://example.com/hook", ""])
def test_invalid_webhook_url_rejected(url):
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(webhook_url=url))


def test_unknown_fields_rejected():
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(unexpected="x"))


def test_metadata_and_description_are_optional():
    body = make_body()
    del body["metadata"], body["description"]
    request = PaymentCreateRequest.model_validate(body)
    assert request.metadata is None
    assert request.description is None


# --- corner cases -----------------------------------------------------------


def test_amount_boundaries_are_inclusive():
    minimum = PaymentCreateRequest.model_validate(make_body(amount="0.01"))
    assert minimum.amount == Decimal("0.01")
    maximum = PaymentCreateRequest.model_validate(make_body(amount="9999999999.99"))
    assert maximum.amount == Decimal("9999999999.99")


def test_amount_as_json_number_is_accepted():
    request = PaymentCreateRequest.model_validate(make_body(amount=100.5))
    assert request.amount == Decimal("100.5")


def test_currency_is_case_sensitive():
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(currency="rub"))


def test_description_length_boundary():
    ok = PaymentCreateRequest.model_validate(make_body(description="я" * 1024))
    assert len(ok.description) == 1024
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(description="я" * 1025))


@pytest.mark.parametrize("bad_metadata", [[1, 2], "text", 42, True])
def test_metadata_must_be_an_object(bad_metadata):
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_body(metadata=bad_metadata))


def test_unicode_survives_validation():
    request = PaymentCreateRequest.model_validate(
        make_body(description="Заказ №42 🚀", metadata={"комментарий": "спасибо"})
    )
    assert request.description == "Заказ №42 🚀"
    assert request.metadata == {"комментарий": "спасибо"}
