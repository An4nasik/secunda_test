"""Shared contracts: API requests/responses, broker events, webhook payload."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl
from pydantic.networks import UrlConstraints

from app.models import Currency, PaymentStatus

PAYMENTS_NEW_ROUTING_KEY = "payments.new"

Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=12, decimal_places=2, examples=["100.50"]),
]

# Pydantic alone allows URLs up to 2083 characters while the database column
# is ``String(2048)``: without this cap a 2049+ character URL would pass
# validation and blow up on INSERT with a 500 instead of a clean 422.
WebhookUrl = Annotated[HttpUrl, UrlConstraints(max_length=2048)]


class PaymentCreateRequest(BaseModel):
    """Body of ``POST /api/v1/payments``."""

    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str | None = Field(default=None, max_length=1024)
    metadata: dict[str, Any] | None = None
    webhook_url: WebhookUrl


class PaymentAcceptedResponse(BaseModel):
    """202 body for payment creation (also returned on an idempotent replay)."""

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    """Detailed payment view for ``GET /api/v1/payments/{payment_id}``."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias=AliasChoices("id", "payment_id"))
    amount: Decimal
    currency: Currency
    description: str | None = None
    metadata: dict[str, Any] | None = Field(
        default=None, validation_alias=AliasChoices("metadata_", "metadata")
    )
    status: PaymentStatus
    webhook_url: HttpUrl
    created_at: datetime
    processed_at: datetime | None = None
    webhook_delivered_at: datetime | None = None


class PaymentCreatedEvent(BaseModel):
    """Message published to ``payments.new``.

    The event intentionally carries only the identifier: the consumer always
    reads the current payment state from the database, so a redelivered or
    delayed message can never apply stale data.
    """

    payment_id: uuid.UUID
    occurred_at: datetime


class WebhookPayload(BaseModel):
    """Notification delivered to the client's ``webhook_url``."""

    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None
    processed_at: datetime | None
