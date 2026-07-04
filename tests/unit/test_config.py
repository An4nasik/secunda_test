"""Settings validation: misconfiguration must fail at startup, not at runtime."""

import pytest
from pydantic import ValidationError

from app.config import Settings


def make_settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def test_inverted_gateway_delay_range_is_rejected():
    with pytest.raises(ValidationError, match="gateway_delay_max_seconds"):
        make_settings(gateway_delay_min_seconds=5.0, gateway_delay_max_seconds=1.0)


def test_equal_delay_bounds_are_allowed():
    settings = make_settings(gateway_delay_min_seconds=3.0, gateway_delay_max_seconds=3.0)
    assert settings.gateway_delay_min_seconds == settings.gateway_delay_max_seconds


@pytest.mark.parametrize("rate", [-0.1, 1.1])
def test_success_rate_outside_unit_interval_is_rejected(rate):
    with pytest.raises(ValidationError):
        make_settings(gateway_success_rate=rate)


def test_negative_max_retries_is_rejected():
    with pytest.raises(ValidationError):
        make_settings(max_retries=-1)


@pytest.mark.parametrize(
    "field", ["retry_base_delay_seconds", "webhook_timeout_seconds", "outbox_poll_interval_seconds"]
)
def test_non_positive_durations_are_rejected(field):
    with pytest.raises(ValidationError):
        make_settings(**{field: 0})


def test_unknown_environment_variables_are_ignored():
    settings = Settings(_env_file=None, totally_unknown_option="x")
    assert not hasattr(settings, "totally_unknown_option")
