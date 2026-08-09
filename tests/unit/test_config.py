from pathlib import Path

from pydantic import ValidationError
from pytest import MonkeyPatch, raises

from vuzol.config import (
    BackupSettings,
    ExecutionSettings,
    InterpretationSettings,
    Settings,
    SubscriptionLimitSettings,
    TelegramDogfoodSettings,
    TelegramSettings,
)


def test_settings_accept_valid_values() -> None:
    settings = Settings(environment="test", port=9000, worker_poll_interval_seconds=0.1)

    assert settings.environment == "test"
    assert settings.port == 9000
    assert settings.project_discussion_enabled is False
    assert settings.subscription_limits.source == "legacy"
    assert settings.subscription_limits.snapshot_file is None
    assert settings.subscription_limits.snapshot_max_age_seconds == 900
    assert settings.telegram.orchestration_trace_enabled is True
    assert settings.telegram.orchestration_trace_sample_percent == 100
    assert settings.telegram.orchestration_trace_always_include_anomalies is True


def test_settings_reject_invalid_port() -> None:
    with raises(ValidationError, match="less than or equal to 65535"):
        Settings(port=70000)


def test_telegram_dogfood_requires_explicit_safe_allowlist() -> None:
    assert not TelegramDogfoodSettings().enabled
    configured = TelegramDogfoodSettings(
        enabled=True,
        fault_injection_enabled=True,
        allowed_project_ids=("vuzol-test",),
    )
    assert configured.allowed_project_ids == ("vuzol-test",)
    with raises(ValidationError):
        TelegramDogfoodSettings(enabled=True)
    with raises(ValidationError):
        TelegramDogfoodSettings(
            fault_injection_enabled=True,
            allowed_project_ids=("vuzol-test",),
        )
    with raises(ValidationError):
        TelegramDogfoodSettings(enabled=True, allowed_project_ids=("Bad Project",))


def test_settings_reject_invalid_log_level() -> None:
    with raises(ValidationError, match="Input should be"):
        Settings(log_level="VERBOSE")  # type: ignore[arg-type]


def test_nested_settings_load_from_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("VUZOL_CONCURRENCY__HEAVY", "2")
    monkeypatch.setenv("VUZOL_LIMITS__PROVIDER_ATTEMPTS", "5")
    monkeypatch.setenv("VUZOL_DATABASE__POOL_SIZE", "7")
    monkeypatch.setenv("VUZOL_SUBSCRIPTION_LIMITS__SOURCE", "legacy")
    monkeypatch.setenv("VUZOL_SUBSCRIPTION_LIMITS__SNAPSHOT_MAX_AGE_SECONDS", "1200")
    monkeypatch.setenv("VUZOL_TELEGRAM__ORCHESTRATION_TRACE_ENABLED", "true")
    monkeypatch.setenv("VUZOL_TELEGRAM__ORCHESTRATION_TRACE_SAMPLE_PERCENT", "25")
    monkeypatch.setenv("VUZOL_TELEGRAM__ORCHESTRATION_TRACE_ALWAYS_INCLUDE_ANOMALIES", "false")

    settings = Settings(_env_file=None)  # type: ignore[call-arg]

    assert settings.concurrency.heavy == 2
    assert settings.limits.provider_attempts == 5
    assert settings.database.pool_size == 7
    assert settings.subscription_limits.source == "legacy"
    assert settings.subscription_limits.snapshot_max_age_seconds == 1200
    assert settings.telegram.orchestration_trace_enabled is True
    assert settings.telegram.orchestration_trace_sample_percent == 25
    assert settings.telegram.orchestration_trace_always_include_anomalies is False


def test_project_discussion_flag_loads_from_environment_but_defaults_off(
    monkeypatch: MonkeyPatch,
) -> None:
    assert Settings(_env_file=None).project_discussion_enabled is False  # type: ignore[call-arg]

    monkeypatch.setenv("VUZOL_PROJECT_DISCUSSION_ENABLED", "true")
    assert Settings(_env_file=None).project_discussion_enabled is True  # type: ignore[call-arg]


def test_trace_sample_percent_is_bounded() -> None:
    with raises(ValidationError):
        TelegramSettings(orchestration_trace_sample_percent=-1)
    with raises(ValidationError):
        TelegramSettings(orchestration_trace_sample_percent=101)


def test_subscription_limits_snapshot_required_needs_absolute_path() -> None:
    with raises(ValidationError, match="snapshot_file is required"):
        SubscriptionLimitSettings(source="snapshot_required")

    with raises(ValidationError, match="must be absolute"):
        SubscriptionLimitSettings(
            source="snapshot_required",
            snapshot_file=Path("relative/snap.json"),
        )

    # Path need not exist at parse time (exporter may publish later).
    configured = SubscriptionLimitSettings(
        source="snapshot_required",
        snapshot_file=Path("/var/lib/vuzol-subscription-limits/grok.json"),
        snapshot_max_age_seconds=600,
    )
    assert configured.snapshot_file is not None
    assert configured.snapshot_file.is_absolute()
    assert configured.snapshot_max_age_seconds == 600


def test_subscription_limits_legacy_allows_unset_snapshot() -> None:
    # Relative snapshot under legacy is unused and not rejected at settings parse.
    legacy = SubscriptionLimitSettings(
        source="legacy",
        snapshot_file=Path("unused-relative.json"),
    )
    assert legacy.source == "legacy"
    assert Settings(_env_file=None).subscription_limits.source == "legacy"  # type: ignore[call-arg]


def test_subscription_limits_env_snapshot_required(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("VUZOL_SUBSCRIPTION_LIMITS__SOURCE", "snapshot_required")
    monkeypatch.setenv(
        "VUZOL_SUBSCRIPTION_LIMITS__SNAPSHOT_FILE",
        "/var/lib/vuzol-subscription-limits/grok.json",
    )
    monkeypatch.setenv("VUZOL_SUBSCRIPTION_LIMITS__SNAPSHOT_MAX_AGE_SECONDS", "450")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.subscription_limits.source == "snapshot_required"
    assert settings.subscription_limits.snapshot_file == Path(
        "/var/lib/vuzol-subscription-limits/grok.json"
    )
    assert settings.subscription_limits.snapshot_max_age_seconds == 450


def test_automatic_interpretation_requires_evaluation_report() -> None:
    with raises(ValidationError, match="requires an evaluation report"):
        InterpretationSettings(automatic_execution_enabled=True)

    with raises(ValidationError, match="lease must exceed provider timeouts"):
        InterpretationSettings(lease_seconds=30, provider_timeout_seconds=30)


def test_enabled_execution_requires_paired_seccomp_path_and_digest() -> None:
    with raises(ValidationError, match="requires a pinned sandbox seccomp profile"):
        ExecutionSettings(enabled=True)

    with raises(ValidationError, match="path and digest must be configured together"):
        ExecutionSettings(sandbox_seccomp_profile=Path("/etc/vuzol/sandbox-seccomp.json"))

    configured = ExecutionSettings(
        enabled=True,
        sandbox_seccomp_profile=Path("/etc/vuzol/sandbox-seccomp.json"),
        sandbox_seccomp_profile_sha256="a" * 64,
    )
    assert configured.sandbox_seccomp_profile_sha256 == "a" * 64


def test_backup_restore_settings_default_fail_closed() -> None:
    backup = BackupSettings()

    assert backup.enabled is False
    assert backup.capture_cli_permitted is False
    assert backup.restore_cli_permitted is False
    assert backup.restore_dsn_reference is None
    assert backup.restore_overall_timeout_seconds is None
    assert backup.restore_require_empty_target is True
    assert backup.restore_probe_capture_lock is True


def test_backup_restore_dsn_reference_pattern() -> None:
    assert BackupSettings(restore_dsn_reference="env:RESTORE_DSN").restore_dsn_reference == (
        "env:RESTORE_DSN"
    )
    assert BackupSettings(restore_dsn_reference="file:restore.dsn").restore_dsn_reference == (
        "file:restore.dsn"
    )
    with raises(ValidationError):
        BackupSettings(restore_dsn_reference="plaintext-not-allowed")
    with raises(ValidationError):
        BackupSettings(restore_dsn_reference="secret:x")


def test_backup_restore_timeout_positive_when_set() -> None:
    assert (
        BackupSettings(restore_overall_timeout_seconds=3600.0).restore_overall_timeout_seconds
        == 3600.0
    )
    with raises(ValidationError):
        BackupSettings(restore_overall_timeout_seconds=0)
    with raises(ValidationError):
        BackupSettings(restore_overall_timeout_seconds=-1.0)


def test_backup_restore_settings_reject_extra_fields() -> None:
    with raises(ValidationError):
        BackupSettings.model_validate(
            {"restore_cli_permitted": False, "unknown_restore_flag": True}
        )


def test_backup_restore_nested_env_load(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("VUZOL_BACKUP__RESTORE_DSN_REFERENCE", "env:LAB_RESTORE_DSN")
    monkeypatch.setenv("VUZOL_BACKUP__RESTORE_OVERALL_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("VUZOL_BACKUP__RESTORE_REQUIRE_EMPTY_TARGET", "false")
    monkeypatch.setenv("VUZOL_BACKUP__RESTORE_PROBE_CAPTURE_LOCK", "false")

    settings = Settings(_env_file=None)  # type: ignore[call-arg]

    assert settings.backup.restore_dsn_reference == "env:LAB_RESTORE_DSN"
    assert settings.backup.restore_overall_timeout_seconds == 120.0
    assert settings.backup.restore_require_empty_target is False
    assert settings.backup.restore_probe_capture_lock is False
    assert settings.backup.restore_cli_permitted is False
