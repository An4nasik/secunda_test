"""Application settings shared by the API, consumer and outbox relay."""

from functools import lru_cache
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration read from environment variables (and ``.env`` locally).

    A single settings class keeps every knob documented and validated in one
    place; each service reads only the fields it needs.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    api_key: SecretStr = SecretStr("")
    """Static key for the ``X-API-Key`` header; the API refuses to start when empty."""

    gateway_delay_min_seconds: float = Field(default=2.0, ge=0)
    gateway_delay_max_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)

    webhook_timeout_seconds: float = Field(default=10.0, gt=0)

    max_retries: int = Field(default=3, ge=0)
    """Redelivery attempts after the first failed one; the message is dead-lettered next."""

    retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    """Delay before retry N is ``base * 2**(N-1)`` seconds (exponential backoff)."""

    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    log_level: str = "INFO"
    log_json: bool = True

    @model_validator(mode="after")
    def _validate_delay_range(self) -> Self:
        if self.gateway_delay_max_seconds < self.gateway_delay_min_seconds:
            msg = "gateway_delay_max_seconds must be >= gateway_delay_min_seconds"
            raise ValueError(msg)
        return self


@lru_cache
def get_settings() -> Settings:
    """Return process-wide settings, loaded once on first use."""
    return Settings()
