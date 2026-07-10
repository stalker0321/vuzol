from pydantic import ValidationError
from pytest import raises

from vuzol.config import Settings


def test_settings_accept_valid_values() -> None:
    settings = Settings(environment="test", port=9000, worker_poll_interval_seconds=0.1)

    assert settings.environment == "test"
    assert settings.port == 9000


def test_settings_reject_invalid_port() -> None:
    with raises(ValidationError, match="less than or equal to 65535"):
        Settings(port=70000)


def test_settings_reject_invalid_log_level() -> None:
    with raises(ValidationError, match="Input should be"):
        Settings(log_level="VERBOSE")  # type: ignore[arg-type]
