"""Emulation of the external payment gateway."""

import asyncio
import random

from app.config import Settings
from app.models import PaymentStatus


async def process_payment(settings: Settings) -> PaymentStatus:
    """Emulate a charge through the external gateway.

    The call takes ``gateway_delay_min/max_seconds`` and succeeds with
    ``gateway_success_rate`` probability. A declined payment is a normal
    business outcome (``failed`` status), not a processing error.
    """
    delay = random.uniform(  # noqa: S311 - emulation, not cryptography
        settings.gateway_delay_min_seconds, settings.gateway_delay_max_seconds
    )
    await asyncio.sleep(delay)
    succeeded = random.random() < settings.gateway_success_rate  # noqa: S311
    return PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
