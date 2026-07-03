"""Payment gateway emulation."""

import time

from app.config import Settings
from app.consumer.gateway import process_payment
from app.models import PaymentStatus


def make_settings(**overrides) -> Settings:
    defaults = {
        "gateway_delay_min_seconds": 0.01,
        "gateway_delay_max_seconds": 0.02,
        "_env_file": None,
    }
    return Settings(**{**defaults, **overrides})


async def test_always_succeeds_at_rate_one():
    settings = make_settings(gateway_success_rate=1.0)
    assert await process_payment(settings) is PaymentStatus.SUCCEEDED


async def test_always_fails_at_rate_zero():
    settings = make_settings(gateway_success_rate=0.0)
    assert await process_payment(settings) is PaymentStatus.FAILED


async def test_processing_takes_at_least_the_minimum_delay():
    settings = make_settings(gateway_success_rate=1.0)
    started = time.monotonic()
    await process_payment(settings)
    assert time.monotonic() - started >= 0.01
