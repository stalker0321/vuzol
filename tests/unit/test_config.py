from pydantic import ValidationError
from pytest import MonkeyPatch, raises

from vuzol.config import InterpretationSettings, Settings


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


def test_nested_settings_load_from_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("VUZOL_CONCURRENCY__HEAVY", "2")
    monkeypatch.setenv("VUZOL_LIMITS__PROVIDER_ATTEMPTS", "5")
    monkeypatch.setenv("VUZOL_DATABASE__POOL_SIZE", "7")

    settings = Settings(_env_file=None)  # type: ignore[call-arg]

    assert settings.concurrency.heavy == 2
    assert settings.limits.provider_attempts == 5
    assert settings.database.pool_size == 7


def test_automatic_interpretation_requires_evaluation_report() -> None:
    with raises(ValidationError, match="requires an evaluation report"):
        InterpretationSettings(automatic_execution_enabled=True)

    with raises(ValidationError, match="lease must exceed provider timeouts"):
        InterpretationSettings(lease_seconds=30, provider_timeout_seconds=30)
