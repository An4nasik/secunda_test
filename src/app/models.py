"""ORM models: payments and the transactional outbox."""

import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Currency(enum.StrEnum):
    """Supported payment currencies."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(enum.StrEnum):
    """Payment lifecycle states."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _enum_values(enum_cls: type[enum.Enum]) -> list[str]:
    """Store enum values (not member names) in the database."""
    return [member.value for member in enum_cls]


class Base(DeclarativeBase):
    """Declarative base with naming conventions and shared type mappings."""

    metadata = sa.MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map: ClassVar = {
        datetime: sa.DateTime(timezone=True),
        Decimal: sa.Numeric(12, 2),
        dict[str, Any]: JSONB(),
        uuid.UUID: sa.Uuid(),
        Currency: sa.Enum(Currency, name="currency", values_callable=_enum_values),
        PaymentStatus: sa.Enum(PaymentStatus, name="payment_status", values_callable=_enum_values),
    }


class Payment(Base):
    """A payment request and the outcome of its processing."""

    __tablename__ = "payments"
    __table_args__ = (sa.CheckConstraint(sa.column("amount") > 0, name="amount_positive"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid7)
    amount: Mapped[Decimal]
    currency: Mapped[Currency]
    description: Mapped[str | None] = mapped_column(sa.Text())
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata")
    status: Mapped[PaymentStatus] = mapped_column(default=PaymentStatus.PENDING)
    idempotency_key: Mapped[str] = mapped_column(sa.String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(sa.String(64))
    """SHA-256 of the request body; detects idempotency-key reuse with a different payload."""

    webhook_url: Mapped[str] = mapped_column(sa.String(2048))
    created_at: Mapped[datetime] = mapped_column(server_default=sa.func.now())
    processed_at: Mapped[datetime | None]
    webhook_delivered_at: Mapped[datetime | None]
    """Set after the client acknowledged the webhook; guards against duplicate notifications."""


class OutboxMessage(Base):
    """An event awaiting publication to the broker (transactional outbox)."""

    __tablename__ = "outbox"
    __table_args__ = (
        sa.Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=sa.column("published_at").is_(None),
        ),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger(), sa.Identity(), primary_key=True)
    routing_key: Mapped[str] = mapped_column(sa.String(255))
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime] = mapped_column(server_default=sa.func.now())
    published_at: Mapped[datetime | None]
