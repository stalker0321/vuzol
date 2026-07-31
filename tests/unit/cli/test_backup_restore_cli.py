"""Fake-only contract tests for the default-off restore CLI."""

from __future__ import annotations

import argparse
import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vuzol.cli import backup as cli
from vuzol.config import BackupSettings, Settings
from vuzol.ops.backup.restore_cli_hooks import LockProbeFailed, LockProbeUnreachable

_RUN_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_PROD_DSN = "postgresql+asyncpg://prod_user:fake@127.0.0.1:5432/vuzol"  # pragma: allowlist secret
_RESTORE_DSN = "postgresql://restore:fake@127.0.0.1/restore"  # pragma: allowlist secret


def _settings(tmp_path: Path, **backup_changes: object) -> Settings:
    backup = {
        "staging_root": tmp_path / "staging",
        "drill_root": tmp_path / "drill",
        "restore_dsn_reference": "env:RESTORE_DSN",
        **backup_changes,
    }
    return Settings.model_validate(
        {
            "environment": "test",
            "repository_root": tmp_path / "repositories",
            "worktree_root": tmp_path / "worktrees",
            "artifact_root": tmp_path / "artifacts",
            "secret_file_root": tmp_path / "secrets",
            "database_dsn_reference": "env:PRODUCTION_DSN",
            "backup": BackupSettings.model_validate(backup),
        }
    )


def _args(*extra: str) -> argparse.Namespace:
    return cli._parse_args(["restore", "--run-id", _RUN_ID, "--json", *extra])


def _report(*, ok: bool = True, code: str = "would_restore") -> object:
    payload = {
        "ok": ok,
        "code": code,
        "message": code,
        "mode": "dry_run",
        "schedule": "disabled",
    }
    return SimpleNamespace(
        ok=ok,
        code=code,
        message=code,
        mode="dry_run",
        to_operational_payload=lambda: payload,
    )


def _await_kwargs(mock: AsyncMock) -> dict[str, Any]:
    call = mock.await_args
    assert call is not None
    return dict(call.kwargs)


async def _run(
    tmp_path: Path,
    *extra: str,
    settings: Settings | None = None,
    report: object | None = None,
    lock_state: str = "free",
    lock_error: Exception | None = None,
    orchestrator_error: BaseException | None = None,
) -> tuple[int, AsyncMock, AsyncMock, MagicMock]:
    configured = settings or _settings(tmp_path)
    orchestrate = AsyncMock(return_value=report or _report())
    if orchestrator_error is not None:
        orchestrate.side_effect = orchestrator_error
    probe = AsyncMock(return_value=lock_state)
    if lock_error is not None:
        probe.side_effect = lock_error
    empty_factory = MagicMock(return_value=AsyncMock())

    def resolve(_settings: Settings, reference: str) -> str:
        return _PROD_DSN if "PRODUCTION" in reference else _RESTORE_DSN

    with (
        patch.object(cli, "_resolve_scoped_secret", side_effect=resolve),
        patch.object(cli, "_resolve_kek", return_value=b"k" * 32),
        patch(
            "vuzol.ops.backup.postgres_dump.parse_dump_identity",
            return_value=SimpleNamespace(
                user="restore_user",
                database="vuzol_restore",
                password="must-not-leak",  # noqa: S106  # pragma: allowlist secret
            ),
        ),
        patch(
            "vuzol.ops.backup.restore_orchestrator.run_restore_orchestration",
            orchestrate,
        ),
        patch("vuzol.ops.backup.restore_cli_hooks.probe_capture_lock_session", probe),
        patch("vuzol.ops.backup.restore_cli_hooks.make_assert_empty_target", empty_factory),
    ):
        result = await cli._run_restore(_args(*extra), configured, MagicMock())
    return result, orchestrate, probe, empty_factory


def test_parser_defaults_to_dry_run_and_keeps_legacy_namespaces_separate() -> None:
    restore = _args()
    capture = cli._parse_args(["capture"])
    gc = cli._parse_args(["gc-staging"])

    assert restore.apply is False
    assert restore.verify_crypto is False
    assert restore.dry_run is False
    assert not hasattr(capture, "run_id")
    assert not hasattr(gc, "run_id")


@pytest.mark.parametrize("flag", ["--kek", "--restore-dsn", "--password"])
def test_parser_rejects_secret_bearing_flags(flag: str) -> None:
    with pytest.raises(SystemExit):
        cli._parse_args(["restore", "--run-id", _RUN_ID, flag, "secret"])


@pytest.mark.anyio
async def test_default_dry_run_uses_no_kek_or_apply_hooks(tmp_path: Path) -> None:
    code, orchestrate, probe, empty = await _run(tmp_path)

    assert code == 0
    kwargs = _await_kwargs(orchestrate)
    assert kwargs["mode"].value == "dry_run"
    assert kwargs["apply_authorized"] is False
    assert kwargs["verify_crypto"] is False
    assert kwargs["kek"] is None
    assert kwargs["probe_capture_lock"] is None
    assert kwargs["assert_empty_target"] is None
    probe.assert_not_awaited()
    empty.assert_not_called()


@pytest.mark.anyio
async def test_verify_crypto_does_not_need_apply_permission(tmp_path: Path) -> None:
    configured = _settings(tmp_path, kek_reference="env:KEK")
    code, orchestrate, _, _ = await _run(
        tmp_path,
        "--verify-crypto",
        settings=configured,
        report=_report(code="crypto_verified"),
    )

    assert code == 0
    kwargs = _await_kwargs(orchestrate)
    assert kwargs["apply_authorized"] is False
    assert kwargs["verify_crypto"] is True
    assert kwargs["kek"] == b"k" * 32


@pytest.mark.anyio
async def test_apply_requires_settings_gate(tmp_path: Path) -> None:
    code, orchestrate, _, _ = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
    )
    assert code == 1
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
async def test_apply_requires_confirmation(tmp_path: Path) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, orchestrate, _, _ = await _run(tmp_path, "--apply", settings=configured)
    assert code == 1
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
async def test_apply_wires_literal_authority_and_safety_hooks(tmp_path: Path) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, orchestrate, probe, empty = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
        report=_report(code="restored"),
    )

    assert code == 0
    kwargs = _await_kwargs(orchestrate)
    assert kwargs["apply_authorized"] is True
    assert type(kwargs["apply_authorized"]) is bool
    assert kwargs["allow_local_hosts_only"] is True
    assert kwargs["restore_user"] == "restore_user"
    assert kwargs["restore_database"] == "vuzol_restore"
    assert kwargs["probe_capture_lock"]() is True
    assert kwargs["assert_empty_target"] is empty.return_value
    probe.assert_awaited_once_with(_PROD_DSN)
    empty.assert_called_once_with(_RESTORE_DSN)


@pytest.mark.anyio
async def test_busy_lock_is_exit_two_without_orchestrator(tmp_path: Path) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, orchestrate, _, _ = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
        lock_state="busy",
    )
    assert code == 2
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
async def test_unknown_lock_state_fails_closed_without_orchestrator(tmp_path: Path) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, orchestrate, _, _ = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
        lock_state="unknown",
    )
    assert code == 1
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (LockProbeUnreachable(), "lock_probe_unreachable"),
        (LockProbeFailed(), "lock_probe_failed"),
    ],
)
async def test_lock_failures_are_fixed_exit_one(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
    expected: str,
) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, orchestrate, _, _ = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
        lock_error=error,
    )
    assert code == 1
    assert expected in capsys.readouterr().out
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
async def test_lock_failure_never_emits_sensitive_cause(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, orchestrate, _, _ = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
        lock_error=LockProbeUnreachable(f"driver leaked {_PROD_DSN}"),
    )
    emitted = capsys.readouterr().out
    assert code == 1
    assert _PROD_DSN not in emitted
    assert "driver leaked" not in emitted
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
async def test_apply_can_skip_optional_hooks_only_when_configured(tmp_path: Path) -> None:
    configured = _settings(
        tmp_path,
        restore_cli_permitted=True,
        kek_reference="env:KEK",
        restore_probe_capture_lock=False,
        restore_require_empty_target=False,
    )
    _, orchestrate, probe, empty = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
    )
    assert _await_kwargs(orchestrate)["probe_capture_lock"] is None
    assert _await_kwargs(orchestrate)["assert_empty_target"] is None
    probe.assert_not_awaited()
    empty.assert_not_called()


@pytest.mark.anyio
async def test_allow_nonempty_flag_skips_empty_hook(tmp_path: Path) -> None:
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    _, orchestrate, _, empty = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        "--allow-non-empty-target",
        settings=configured,
    )
    assert _await_kwargs(orchestrate)["assert_empty_target"] is None
    empty.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "flags",
    [
        ("--apply", "--dry-run"),
        ("--apply", "--verify-crypto"),
    ],
)
async def test_conflicting_modes_refuse_before_work(tmp_path: Path, flags: tuple[str, ...]) -> None:
    code, orchestrate, _, _ = await _run(tmp_path, *flags)
    assert code == 1
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
async def test_invalid_timeout_refuses_before_work(tmp_path: Path, value: str) -> None:
    code, orchestrate, _, _ = await _run(tmp_path, "--timeout-seconds", value)
    assert code == 1
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
async def test_cli_timeout_overrides_settings(tmp_path: Path) -> None:
    configured = _settings(tmp_path, restore_overall_timeout_seconds=3600)
    _, orchestrate, _, _ = await _run(
        tmp_path,
        "--timeout-seconds",
        "10",
        settings=configured,
    )
    assert _await_kwargs(orchestrate)["overall_timeout_seconds"] == 10.0


@pytest.mark.anyio
async def test_invalid_run_id_refuses_before_work(tmp_path: Path) -> None:
    args = cli._parse_args(["restore", "--run-id", "not-a-uuid", "--json"])
    with patch.object(cli, "_resolve_scoped_secret") as resolve:
        code = await cli._run_restore(args, _settings(tmp_path), MagicMock())
    assert code == 1
    resolve.assert_not_called()


@pytest.mark.anyio
async def test_missing_secret_references_refuses(tmp_path: Path) -> None:
    configured = _settings(tmp_path).model_copy(
        update={
            "database_dsn_reference": None,
            "backup": _settings(tmp_path).backup.model_copy(update={"restore_dsn_reference": None}),
        }
    )
    code, orchestrate, _, _ = await _run(tmp_path, settings=configured)
    assert code == 1
    orchestrate.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("report_code", "expected"),
    [
        ("would_restore", 0),
        ("cancelled", 2),
        ("restore_timeout", 2),
        ("preflight_package", 1),
    ],
)
async def test_report_exit_mapping(
    tmp_path: Path,
    report_code: str,
    expected: int,
) -> None:
    ok = report_code == "would_restore"
    code, _, _, _ = await _run(
        tmp_path,
        report=_report(ok=ok, code=report_code),
    )
    assert code == expected


def test_timeout_helper_accepts_none_and_positive_only() -> None:
    assert cli._restore_timeout(None, None) is None
    assert cli._restore_timeout(None, 3.0) == 3.0
    assert cli._restore_timeout(2.0, 3.0) == 2.0
    with pytest.raises(ValueError):
        cli._restore_timeout(0, None)


@pytest.mark.parametrize("escape_kind", ["absolute", "traversal", "symlink"])
def test_file_kek_cannot_escape_secret_root(tmp_path: Path, escape_kind: str) -> None:
    root = tmp_path / "secrets"
    root.mkdir()
    outside = tmp_path / "outside.kek"
    outside.write_bytes(b"x" * 32)
    if escape_kind == "absolute":
        reference = f"file:{outside}"
    elif escape_kind == "traversal":
        reference = "file:../outside.kek"
    else:
        (root / "link.kek").symlink_to(outside)
        reference = "file:link.kek"
    configured = _settings(tmp_path, kek_reference=reference)

    with pytest.raises(ValueError, match="escapes"):
        cli._resolve_kek(configured)


@pytest.mark.anyio
async def test_resolver_failure_never_emits_sensitive_context(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    marker = f"password={_RESTORE_DSN};path={tmp_path / 'staging'}"
    logger = MagicMock()
    with patch.object(cli, "_resolve_scoped_secret", side_effect=RuntimeError(marker)):
        code = await cli._run_restore(_args(), _settings(tmp_path), logger)

    emitted = capsys.readouterr().out + repr(logger.method_calls)
    assert code == 1
    assert marker not in emitted
    assert _RESTORE_DSN not in emitted
    assert str(tmp_path / "staging") not in emitted


@pytest.mark.anyio
async def test_identity_failure_never_emits_dsn_or_password(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    logger = MagicMock()

    def resolve(_settings: Settings, reference: str) -> str:
        return _PROD_DSN if "PRODUCTION" in reference else _RESTORE_DSN

    with (
        patch.object(cli, "_resolve_scoped_secret", side_effect=resolve),
        patch(
            "vuzol.ops.backup.postgres_dump.parse_dump_identity",
            side_effect=ValueError(f"leaked:{_RESTORE_DSN}:password"),
        ),
    ):
        code = await cli._run_restore(_args(), _settings(tmp_path), logger)

    emitted = capsys.readouterr().out + repr(logger.method_calls)
    assert code == 1
    assert _RESTORE_DSN not in emitted
    assert "leaked:" not in emitted


@pytest.mark.anyio
async def test_kek_failure_never_emits_sensitive_context(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configured = _settings(tmp_path, kek_reference="env:KEK")
    logger = MagicMock()

    def resolve(_settings: Settings, reference: str) -> str:
        return _PROD_DSN if "PRODUCTION" in reference else _RESTORE_DSN

    with (
        patch.object(cli, "_resolve_scoped_secret", side_effect=resolve),
        patch(
            "vuzol.ops.backup.postgres_dump.parse_dump_identity",
            return_value=SimpleNamespace(
                user="restore",
                database="vuzol_restore",
                password="hidden",  # noqa: S106  # pragma: allowlist secret
            ),
        ),
        patch.object(cli, "_resolve_kek", side_effect=RuntimeError("KEK=super-secret")),
    ):
        code = await cli._run_restore(
            _args("--verify-crypto"),
            configured,
            logger,
        )

    emitted = capsys.readouterr().out + repr(logger.method_calls)
    assert code == 1
    assert "super-secret" not in emitted
    assert _RESTORE_DSN not in emitted


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("path", "error"),
    [
        ("success", None),
        ("exception", RuntimeError("orchestrator secret")),
        ("cancel", asyncio.CancelledError()),
    ],
)
async def test_kek_buffer_is_zeroized_on_all_orchestrator_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    error: BaseException | None,
) -> None:
    buffers: list[bytearray] = []

    def tracked(value: bytes = b"") -> bytearray:
        buffer = bytearray(value)
        buffers.append(buffer)
        return buffer

    monkeypatch.setattr(cli, "bytearray", tracked, raising=False)
    configured = _settings(tmp_path, kek_reference="env:KEK")
    if error is None:
        await _run(tmp_path, "--verify-crypto", settings=configured)
    else:
        with pytest.raises(type(error)):
            await _run(
                tmp_path,
                "--verify-crypto",
                settings=configured,
                orchestrator_error=error,
            )
    assert path in {"success", "exception", "cancel"}
    assert len(buffers) == 1
    assert buffers[0] == bytearray(b"\0" * 32)


@pytest.mark.anyio
async def test_kek_buffer_is_zeroized_on_lock_early_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffers: list[bytearray] = []

    def tracked(value: bytes = b"") -> bytearray:
        buffer = bytearray(value)
        buffers.append(buffer)
        return buffer

    monkeypatch.setattr(cli, "bytearray", tracked, raising=False)
    configured = _settings(tmp_path, restore_cli_permitted=True, kek_reference="env:KEK")
    code, _, _, _ = await _run(
        tmp_path,
        "--apply",
        "--i-understand-partial-postgres-only",
        settings=configured,
        lock_state="busy",
    )
    assert code == 2
    assert buffers[0] == bytearray(b"\0" * 32)


def test_run_id_is_passed_as_uuid() -> None:
    assert uuid.UUID(_RUN_ID) == uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
