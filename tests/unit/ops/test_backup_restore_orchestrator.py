"""Fake-only unit tests for B3.3 pure restore orchestration.

No Docker, DB, real decrypt, production filesystem package open, or KEK retrieval.
All lower layers are injected fakes except bounded tmp_path binder checks.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from vuzol.ops.backup.crypto import BackupCryptoError
from vuzol.ops.backup.paths import ProductionRoots
from vuzol.ops.backup.postgres_restore import RestoreProcessResult
from vuzol.ops.backup.restore import PackagePreflightReport
from vuzol.ops.backup.restore_orchestrator import (
    CODE_BLOB_AUTH,
    CODE_CRYPTO,
    CODE_CRYPTO_VERIFIED,
    CODE_FAILED,
    CODE_LOCK_BUSY,
    CODE_NOT_PERMITTED,
    CODE_PACKAGE_REBIND,
    CODE_PREFLIGHT_KEK,
    CODE_PREFLIGHT_POSTGRES,
    CODE_RESTORED_ORCH,
    CODE_TARGET_NOT_EMPTY,
    CODE_WOULD_RESTORE,
    CODE_WRAP_AUTH,
    PublishedPackageHandle,
    RestoreMode,
    RestoreOrchestrationReport,
    _default_bind_package_handle,
    run_restore_orchestration,
)
from vuzol.ops.backup.restore_target import TargetPreflightReport

_RUN_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_RUN_ID_S = str(_RUN_ID)
_KEK = b"k" * 32
_DEK = b"d" * 32
_STAGING = Path("/lab/staging/vuzol-backup-orch-test")
_DSN_PROD = "postgresql://prod_user:s3cret@127.0.0.1:5432/vuzol"  # pragma: allowlist secret
_DSN_RESTORE = (
    "postgresql://restore_user:other@127.0.0.1:5432/vuzol_restore"  # pragma: allowlist secret
)
_DRILL = Path("/lab/drill/vuzol-backup-orch-test")

_SECRET_MARKERS = (
    "s3cret",
    "other",
    "postgresql://",
    str(_STAGING),
    str(_DRILL),
    "dek.wrap",
    "postgres.dump.enc",
    "k" * 32,
    "d" * 32,
    "prod_user",
    "restore_user",
    "spawn boom",
    "unwrap boom",
    "stderr-secret",
    "--clean",
)


def _production() -> ProductionRoots:
    return ProductionRoots(
        repository_root=Path("/prod/repos"),
        worktree_root=Path("/prod/wt"),
        artifact_root=Path("/prod/art"),
        secret_file_root=Path("/prod/secrets"),
        config_root=Path("/etc/vuzol"),
        deploy_root=Path("/opt/vuzol"),
    )


def _pkg_ok() -> PackagePreflightReport:
    return PackagePreflightReport(
        ok=True,
        code="package_ok",
        message="package ok",
        run_id=_RUN_ID_S,
        partial=True,
    )


def _pkg_fail() -> PackagePreflightReport:
    return PackagePreflightReport(
        ok=False,
        code="preflight_package",
        message="package missing",
        run_id=None,
    )


def _tgt_ok() -> TargetPreflightReport:
    return TargetPreflightReport(
        ok=True,
        code="target_ok",
        message="target ok",
        host="127.0.0.1",
        port=5432,
        database="vuzol_restore",
    )


def _tgt_fail() -> TargetPreflightReport:
    return TargetPreflightReport(
        ok=False,
        code="preflight_target_host",
        message="host rejected",
    )


def _handle() -> PublishedPackageHandle:
    publish = _STAGING / "runs" / _RUN_ID_S / "publish"
    return PublishedPackageHandle(
        run_id=_RUN_ID,
        resolved_staging_root=_STAGING,
        resolved_run_dir=_STAGING / "runs" / _RUN_ID_S,
        resolved_publish_dir=publish,
        wrap_path=publish / "dek.wrap",
        blob_path=publish / "postgres.dump.enc",
        manifest_path=publish / "manifest.v1.json",
        package_report=_pkg_ok(),
    )


class _TrackingGen:
    """Iterator that records close() and optional mid-stream raise."""

    def __init__(
        self,
        chunks: list[bytes] | None = None,
        *,
        raise_at: int | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.chunks = list(chunks or [b"chunk"])
        self.raise_at = raise_at
        self.error = error or BackupCryptoError("mid-stream auth")
        self.closed = False
        self.consumed = 0

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        if self.raise_at is not None and self.consumed == self.raise_at:
            raise self.error
        if self.consumed >= len(self.chunks):
            raise StopIteration
        chunk = self.chunks[self.consumed]
        self.consumed += 1
        return chunk

    def close(self) -> None:
        self.closed = True


def _bind_ok(**_kwargs: Any) -> PublishedPackageHandle:
    return _handle()


def _ok_process(
    *,
    ok: bool = True,
    code: str = "restored",
    exit_code: int | None = 0,
    bytes_written: int = 4,
    process_started: bool = True,
) -> RestoreProcessResult:
    return RestoreProcessResult(
        ok=ok,
        code=code,
        exit_code=exit_code,
        bytes_written=bytes_written,
        process_started=process_started,
    )


def _default_argv(**_k: Any) -> list[str]:
    return [
        "docker",
        "exec",
        "-i",
        "vuzol-postgres-1",
        "pg_restore",
        "-U",
        "vuzol",
        "-d",
        "vuzol_restore",
        "--no-owner",
        "--no-acl",
    ]


def _base_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "staging_root": _STAGING,
        "run_id": _RUN_ID,
        "production": _production(),
        "production_dsn": _DSN_PROD,
        "restore_dsn": _DSN_RESTORE,
        "drill_root": _DRILL,
        "bind_package_handle": _bind_ok,
        "package_preflight": lambda **_k: _pkg_ok(),
        "target_preflight": lambda **_k: _tgt_ok(),
        "unwrap": lambda **_k: _DEK,
        "decrypt_stream": lambda **_k: _TrackingGen(),
        "run_restore": lambda *_a, **_k: _ok_process(),
        "build_argv": _default_argv,
    }
    kwargs.update(overrides)
    return kwargs


def _run(**kwargs: Any) -> RestoreOrchestrationReport:
    return asyncio.run(run_restore_orchestration(**_base_kwargs(**kwargs)))


def _assert_redacted(report: RestoreOrchestrationReport, *extra: str) -> None:
    blob = str(report.to_operational_payload()) + report.message + str(report)
    for marker in _SECRET_MARKERS + extra:
        assert marker not in blob


# ---------------------------------------------------------------------------
# Group 1: Default/string dry-run purity and invalid-mode zero-call refusal
# ---------------------------------------------------------------------------


def test_default_mode_is_dry_run() -> None:
    report = _run()
    assert report.mode == "dry_run"
    assert report.code == CODE_WOULD_RESTORE
    assert report.ok is True
    assert report.partial is True
    assert report.schedule == "disabled"


def test_string_dry_run_normalize_purity() -> None:
    unwrap_calls: list[Any] = []
    restore_calls: list[Any] = []
    build_calls: list[Any] = []
    bind_calls: list[Any] = []

    def unwrap(**k: Any) -> bytes:
        unwrap_calls.append(k)
        return _DEK

    def run_restore(*a: Any, **k: Any) -> RestoreProcessResult:
        restore_calls.append((a, k))
        return _ok_process(process_started=False, bytes_written=0)

    def build(**k: Any) -> list[str]:
        build_calls.append(k)
        return _default_argv()

    def bind(**k: Any) -> PublishedPackageHandle:
        bind_calls.append(k)
        return _handle()

    report = _run(
        mode="dry_run",
        unwrap=unwrap,
        run_restore=run_restore,
        build_argv=build,
        bind_package_handle=bind,
    )
    assert report.ok is True
    assert report.code == CODE_WOULD_RESTORE
    assert report.mode == "dry_run"
    assert report.partial is True
    assert unwrap_calls == []
    assert restore_calls == []
    assert build_calls == []
    assert bind_calls == []


def test_invalid_mode_fail_closed_zero_calls() -> None:
    pkg_calls: list[Any] = []
    tgt_calls: list[Any] = []
    unwrap_calls: list[Any] = []
    restore_calls: list[Any] = []

    def pkg(**k: Any) -> PackagePreflightReport:
        pkg_calls.append(k)
        return _pkg_ok()

    def tgt(**k: Any) -> TargetPreflightReport:
        tgt_calls.append(k)
        return _tgt_ok()

    def unwrap(**k: Any) -> bytes:
        unwrap_calls.append(k)
        return _DEK

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process(process_started=False, bytes_written=0)

    report = _run(
        mode="not_a_mode",
        apply_authorized=True,
        package_preflight=pkg,
        target_preflight=tgt,
        unwrap=unwrap,
        run_restore=run_restore,
    )
    assert report.ok is False
    assert report.code == CODE_FAILED
    assert report.mode == "invalid"
    assert pkg_calls == []
    assert tgt_calls == []
    assert unwrap_calls == []
    assert restore_calls == []
    _assert_redacted(report)


# ---------------------------------------------------------------------------
# Group 2: APPLY auth matrix — only actual True proceeds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "authorized",
    [False, None, 0, 1, "yes", "True", object()],
)
def test_apply_auth_refuses_non_singleton_true(authorized: object) -> None:
    pkg_calls: list[Any] = []
    unwrap_calls: list[Any] = []
    restore_calls: list[Any] = []

    def pkg(**k: Any) -> PackagePreflightReport:
        pkg_calls.append(k)
        return _pkg_ok()

    def unwrap(**k: Any) -> bytes:
        unwrap_calls.append(k)
        return _DEK

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process(process_started=False, bytes_written=0)

    # Pass via untyped kwargs so full-project mypy accepts intentional non-bool values.
    kwargs: dict[str, Any] = _base_kwargs(
        mode=RestoreMode.APPLY,
        package_preflight=pkg,
        unwrap=unwrap,
        run_restore=run_restore,
    )
    kwargs["apply_authorized"] = authorized
    report = asyncio.run(run_restore_orchestration(**kwargs))
    assert report.ok is False
    assert report.code == CODE_NOT_PERMITTED
    assert report.mode == "apply"
    assert pkg_calls == []
    assert unwrap_calls == []
    assert restore_calls == []


def test_string_apply_unauthorized_no_package() -> None:
    pkg_calls: list[Any] = []

    def pkg(**k: Any) -> PackagePreflightReport:
        pkg_calls.append(k)
        return _pkg_ok()

    report = _run(
        mode="apply",
        apply_authorized=False,
        package_preflight=pkg,
    )
    assert report.code == CODE_NOT_PERMITTED
    assert report.mode == "apply"
    assert pkg_calls == []


def test_apply_authorized_true_proceeds() -> None:
    report = _run(mode=RestoreMode.APPLY, apply_authorized=True, kek=_KEK)
    assert report.ok is True
    assert report.code == CODE_RESTORED_ORCH


# ---------------------------------------------------------------------------
# Group 3: Package/target first- and second-pass short-circuit; double preflight
# ---------------------------------------------------------------------------


def test_package_fail_skips_target_and_crypto() -> None:
    target_calls: list[Any] = []
    unwrap_calls: list[Any] = []

    report = _run(
        package_preflight=lambda **_k: _pkg_fail(),
        target_preflight=lambda **k: target_calls.append(k) or _tgt_ok(),  # type: ignore[func-returns-value]
        unwrap=lambda **k: unwrap_calls.append(k) or _DEK,  # type: ignore[func-returns-value]
    )
    assert report.ok is False
    assert report.code == "preflight_package"
    assert report.package_code == "preflight_package"
    assert target_calls == []
    assert unwrap_calls == []


def test_target_fail_no_crypto() -> None:
    unwrap_calls: list[Any] = []
    restore_calls: list[Any] = []

    report = _run(
        target_preflight=lambda **_k: _tgt_fail(),
        unwrap=lambda **k: unwrap_calls.append(k) or _DEK,  # type: ignore[func-returns-value]
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.ok is False
    assert report.code == "preflight_target_host"
    assert report.target_code == "preflight_target_host"
    assert unwrap_calls == []
    assert restore_calls == []


def test_apply_success_double_preflight() -> None:
    pkg_calls = 0
    tgt_calls = 0

    def pkg(**_k: Any) -> PackagePreflightReport:
        nonlocal pkg_calls
        pkg_calls += 1
        return _pkg_ok()

    def tgt(**_k: Any) -> TargetPreflightReport:
        nonlocal tgt_calls
        tgt_calls += 1
        return _tgt_ok()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        package_preflight=pkg,
        target_preflight=tgt,
    )
    assert report.ok is True
    assert report.code == CODE_RESTORED_ORCH
    assert pkg_calls == 2
    assert tgt_calls == 2


def test_apply_second_package_fail() -> None:
    n = {"i": 0}

    def pkg(**_k: Any) -> PackagePreflightReport:
        n["i"] += 1
        if n["i"] == 1:
            return _pkg_ok()
        return _pkg_fail()

    restore_calls: list[Any] = []
    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        package_preflight=pkg,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.ok is False
    assert report.code == "preflight_package"
    assert n["i"] == 2
    assert restore_calls == []


def test_apply_second_target_fail() -> None:
    n = {"i": 0}

    def tgt(**_k: Any) -> TargetPreflightReport:
        n["i"] += 1
        if n["i"] == 1:
            return _tgt_ok()
        return _tgt_fail()

    restore_calls: list[Any] = []
    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        target_preflight=tgt,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.ok is False
    assert report.code == "preflight_target_host"
    assert n["i"] == 2
    assert restore_calls == []


# ---------------------------------------------------------------------------
# Group 4: Sealed-handle rebind (second-pass identity, lstat, tmp_path)
# ---------------------------------------------------------------------------


def _make_isolated_staging(tmp_path: Path) -> tuple[Path, ProductionRoots, Path]:
    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
        config_root=tmp_path / "etc" / "vuzol",
        deploy_root=tmp_path / "opt" / "vuzol",
    )
    for root in production.all_roots():
        root.mkdir(parents=True, exist_ok=True)
    staging = tmp_path / "staging"
    staging.mkdir()
    drill = tmp_path / "drill"
    drill.mkdir()
    return staging, production, drill


def test_default_bind_ok_regular_files(tmp_path: Path) -> None:
    staging, production, drill = _make_isolated_staging(tmp_path)
    publish = staging / "runs" / _RUN_ID_S / "publish"
    publish.mkdir(parents=True)
    (publish / "dek.wrap").write_bytes(b"w" * 86)
    (publish / "postgres.dump.enc").write_bytes(b"blob")
    (publish / "manifest.v1.json").write_text("{}", encoding="utf-8")

    report = asyncio.run(
        run_restore_orchestration(
            staging_root=staging,
            run_id=_RUN_ID,
            production=production,
            production_dsn=_DSN_PROD,
            restore_dsn=_DSN_RESTORE,
            drill_root=drill,
            package_preflight=lambda **_k: _pkg_ok(),
            target_preflight=lambda **_k: _tgt_ok(),
            verify_crypto=True,
            kek=_KEK,
            unwrap=lambda **_k: _DEK,
            decrypt_stream=lambda **_k: _TrackingGen(),
        )
    )
    assert report.ok is True
    assert report.code == CODE_CRYPTO_VERIFIED
    _assert_redacted(report, str(staging), str(publish))


def test_default_bind_refuses_symlink_file(tmp_path: Path) -> None:
    staging, production, _drill = _make_isolated_staging(tmp_path)
    publish = staging / "runs" / _RUN_ID_S / "publish"
    publish.mkdir(parents=True)
    real = publish / "real.wrap"
    real.write_bytes(b"w" * 86)
    os.symlink(real.name, publish / "dek.wrap")
    (publish / "postgres.dump.enc").write_bytes(b"blob")
    (publish / "manifest.v1.json").write_text("{}", encoding="utf-8")

    handle = _default_bind_package_handle(
        staging_root=staging,
        run_id=_RUN_ID,
        production=production,
        package_report=_pkg_ok(),
        mode_s="dry_run",
    )
    assert isinstance(handle, RestoreOrchestrationReport)
    assert handle.ok is False
    assert handle.code == CODE_PACKAGE_REBIND
    assert str(staging) not in handle.message
    assert str(publish) not in handle.message


def test_default_bind_refuses_directory_as_file(tmp_path: Path) -> None:
    staging, production, _drill = _make_isolated_staging(tmp_path)
    publish = staging / "runs" / _RUN_ID_S / "publish"
    publish.mkdir(parents=True)
    (publish / "dek.wrap").mkdir()  # non-regular
    (publish / "postgres.dump.enc").write_bytes(b"blob")
    (publish / "manifest.v1.json").write_text("{}", encoding="utf-8")

    handle = _default_bind_package_handle(
        staging_root=staging,
        run_id=_RUN_ID,
        production=production,
        package_report=_pkg_ok(),
        mode_s="apply",
    )
    assert isinstance(handle, RestoreOrchestrationReport)
    assert handle.code == CODE_PACKAGE_REBIND


def test_default_bind_refuses_escaping_publish(tmp_path: Path) -> None:
    staging, production, _drill = _make_isolated_staging(tmp_path)
    run_dir = staging / "runs" / _RUN_ID_S
    run_dir.mkdir(parents=True)
    outside = tmp_path / "outside_publish"
    outside.mkdir()
    (outside / "dek.wrap").write_bytes(b"w" * 86)
    (outside / "postgres.dump.enc").write_bytes(b"blob")
    (outside / "manifest.v1.json").write_text("{}", encoding="utf-8")
    os.symlink(outside, run_dir / "publish")

    handle = _default_bind_package_handle(
        staging_root=staging,
        run_id=_RUN_ID,
        production=production,
        package_report=_pkg_ok(),
        mode_s="apply",
    )
    assert isinstance(handle, RestoreOrchestrationReport)
    assert handle.code == CODE_PACKAGE_REBIND


def test_bind_failure_short_circuits() -> None:
    def bad_bind(**kwargs: Any) -> RestoreOrchestrationReport:
        return RestoreOrchestrationReport(
            ok=False,
            code=CODE_PACKAGE_REBIND,
            message="rebind failed",
            mode=kwargs.get("mode_s", "apply"),
        )

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        bind_package_handle=bad_bind,
    )
    assert report.ok is False
    assert report.code == CODE_PACKAGE_REBIND


# ---------------------------------------------------------------------------
# Group 5: Verify crypto full consumption; wrap/truncated/auth/generic failures
# ---------------------------------------------------------------------------


def test_verify_without_kek() -> None:
    unwrap_calls: list[Any] = []
    report = _run(
        verify_crypto=True,
        kek=None,
        unwrap=lambda **k: unwrap_calls.append(k) or _DEK,  # type: ignore[func-returns-value]
    )
    assert report.ok is False
    assert report.code == CODE_PREFLIGHT_KEK
    assert unwrap_calls == []


def test_verify_invalid_kek_length() -> None:
    report = _run(verify_crypto=True, kek=b"short")
    assert report.ok is False
    assert report.code == CODE_PREFLIGHT_KEK


def test_verify_success_fully_consumes_and_closes() -> None:
    gen = _TrackingGen(chunks=[b"a", b"b", b"c"])
    restore_calls: list[Any] = []

    report = _run(
        verify_crypto=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.ok is True
    assert report.code == CODE_CRYPTO_VERIFIED
    assert report.partial is True
    assert gen.closed is True
    assert gen.consumed == 3
    assert restore_calls == []
    assert report.code != CODE_RESTORED_ORCH


def test_verify_unwrap_auth_no_spawn() -> None:
    restore_calls: list[Any] = []

    def bad_unwrap(**_k: Any) -> bytes:
        raise BackupCryptoError("bad wrap")

    report = _run(
        verify_crypto=True,
        kek=_KEK,
        unwrap=bad_unwrap,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.code == CODE_WRAP_AUTH
    assert restore_calls == []
    _assert_redacted(report, "bad wrap")


def test_verify_midstream_auth_fail_closes_gen_no_spawn() -> None:
    gen = _TrackingGen(chunks=[b"a", b"b"], raise_at=1)
    restore_calls: list[Any] = []

    report = _run(
        verify_crypto=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.ok is False
    assert report.code == CODE_BLOB_AUTH
    assert gen.closed is True
    assert restore_calls == []


def test_verify_generic_crypto_failures() -> None:
    def boom_unwrap(**_k: Any) -> bytes:
        raise RuntimeError("unwrap boom")

    r1 = _run(verify_crypto=True, kek=_KEK, unwrap=boom_unwrap)
    assert r1.code == CODE_CRYPTO
    _assert_redacted(r1, "unwrap boom")

    def boom_dec(**_k: Any) -> Iterator[bytes]:
        raise RuntimeError("dec boom")

    r2 = _run(verify_crypto=True, kek=_KEK, decrypt_stream=boom_dec)
    assert r2.code == CODE_CRYPTO
    _assert_redacted(r2, "dec boom")


# ---------------------------------------------------------------------------
# Group 6: Lock / empty hooks order and gating
# ---------------------------------------------------------------------------


def test_lock_false_refuses_before_pass_two() -> None:
    pkg_calls = 0
    restore_calls: list[Any] = []

    def pkg(**_k: Any) -> PackagePreflightReport:
        nonlocal pkg_calls
        pkg_calls += 1
        return _pkg_ok()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        package_preflight=pkg,
        probe_capture_lock=lambda: False,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.code == CODE_LOCK_BUSY
    assert pkg_calls == 1
    assert restore_calls == []


def test_lock_none_refuses_before_pass_two() -> None:
    n = {"i": 0}

    def pkg(**_k: Any) -> PackagePreflightReport:
        n["i"] += 1
        return _pkg_ok()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        package_preflight=pkg,
        probe_capture_lock=lambda: None,
    )
    assert report.code == CODE_LOCK_BUSY
    assert n["i"] == 1


def test_lock_true_and_empty_hooks_order() -> None:
    order: list[str] = []

    def pkg(**_k: Any) -> PackagePreflightReport:
        order.append("package")
        return _pkg_ok()

    def tgt(**_k: Any) -> TargetPreflightReport:
        order.append("target")
        return _tgt_ok()

    def lock() -> bool:
        order.append("lock")
        return True

    def empty() -> None:
        order.append("empty")

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        package_preflight=pkg,
        target_preflight=tgt,
        probe_capture_lock=lock,
        assert_empty_target=empty,
    )
    assert report.ok is True
    assert order == [
        "package",
        "target",
        "lock",
        "empty",
        "package",
        "target",
    ]


def test_async_lock_and_empty_injects() -> None:
    async def free_lock() -> bool:
        return True

    async def empty_ok() -> None:
        return None

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        probe_capture_lock=free_lock,
        assert_empty_target=empty_ok,
    )
    assert report.ok is True
    assert report.code == CODE_RESTORED_ORCH


def test_empty_inject_fail() -> None:
    restore_calls: list[Any] = []

    def not_empty() -> None:
        raise RuntimeError("not empty")

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        assert_empty_target=not_empty,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.code == CODE_TARGET_NOT_EMPTY
    assert restore_calls == []
    # Exception text must not appear; fixed message "restore target is not empty" is OK.
    _assert_redacted(report, "RuntimeError")


# ---------------------------------------------------------------------------
# Group 7: Builder override=None, force_clean_isolated=False; no --clean
# ---------------------------------------------------------------------------


def test_builder_receives_exact_flags_no_clean() -> None:
    argv_kw: list[dict[str, Any]] = []
    seen_argv: list[list[str]] = []

    def build(**kwargs: Any) -> list[str]:
        argv_kw.append(dict(kwargs))
        argv = [
            "docker",
            "exec",
            "-i",
            kwargs.get("container", "c"),
            "pg_restore",
            "-U",
            kwargs.get("user", "u"),
            "-d",
            kwargs.get("database", "db"),
            "--no-owner",
            "--no-acl",
        ]
        seen_argv.append(argv)
        return argv

    def run_restore(argv: list[str], _gen: Any, **_k: Any) -> RestoreProcessResult:
        assert "--clean" not in argv
        assert "--if-exists" not in argv
        return _ok_process(bytes_written=1)

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        build_argv=build,
        run_restore=run_restore,
    )
    assert report.ok is True
    assert argv_kw[0].get("override") is None
    assert argv_kw[0].get("force_clean_isolated") is False
    assert "--clean" not in seen_argv[0]
    assert "--if-exists" not in seen_argv[0]


def test_builder_rejection_maps_preflight_postgres() -> None:
    def bad_build(**_k: Any) -> list[str]:
        raise ValueError("bad argv secret")

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        build_argv=bad_build,
    )
    assert report.ok is False
    assert report.code == CODE_PREFLIGHT_POSTGRES
    _assert_redacted(report, "bad argv secret")


# ---------------------------------------------------------------------------
# Group 8: Parameterized B3.2 code passthrough + dirty iff started nonsuccess
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("proc", "expect_code", "expect_ok", "expect_dirty"),
    [
        (
            {"ok": True, "code": "restored", "process_started": True, "bytes_written": 4},
            CODE_RESTORED_ORCH,
            True,
            False,
        ),
        (
            {
                "ok": False,
                "code": "preflight_postgres",
                "process_started": False,
                "bytes_written": 0,
            },
            "preflight_postgres",
            False,
            False,
        ),
        (
            {
                "ok": False,
                "code": "cancelled",
                "process_started": True,
                "bytes_written": 10,
            },
            "cancelled",
            False,
            True,
        ),
        (
            {
                "ok": False,
                "code": "restore_timeout",
                "process_started": True,
                "bytes_written": 2,
            },
            "restore_timeout",
            False,
            True,
        ),
        (
            {
                "ok": False,
                "code": "restore_plaintext_failed",
                "process_started": True,
                "bytes_written": 3,
            },
            "restore_plaintext_failed",
            False,
            True,
        ),
        (
            {
                "ok": False,
                "code": "restore_broken_pipe",
                "process_started": True,
                "bytes_written": 1,
            },
            "restore_broken_pipe",
            False,
            True,
        ),
        (
            {
                "ok": False,
                "code": "restore_process_failed",
                "process_started": True,
                "bytes_written": 0,
            },
            "restore_process_failed",
            False,
            True,
        ),
    ],
)
def test_b32_result_passthrough_and_dirty(
    proc: dict[str, Any],
    expect_code: str,
    expect_ok: bool,
    expect_dirty: bool,
) -> None:
    gen = _TrackingGen(chunks=[b"x"])

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        return _ok_process(
            ok=bool(proc["ok"]),
            code=str(proc["code"]),
            exit_code=0 if proc["ok"] else None,
            bytes_written=int(proc["bytes_written"]),
            process_started=bool(proc["process_started"]),
        )

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
        run_restore=run_restore,
    )
    assert report.ok is expect_ok
    assert report.code == expect_code
    assert report.process_code == proc["code"]
    assert report.process_started is proc["process_started"]
    assert report.target_may_be_dirty is expect_dirty
    assert report.partial is True
    assert gen.closed is True


# ---------------------------------------------------------------------------
# Group 9: cancel_flag / deadline / close_plaintext passthrough; Exception dirty;
#          BaseException after gen close + DEK wipe
# ---------------------------------------------------------------------------


def test_cancel_deadline_close_plaintext_passed_unchanged() -> None:
    captured: dict[str, Any] = {}

    def flag() -> bool:
        return False

    def run_restore(*_a: Any, **k: Any) -> RestoreProcessResult:
        captured.update(k)
        return _ok_process()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        cancel_flag=flag,
        overall_timeout_seconds=12.5,
        run_restore=run_restore,
    )
    assert report.ok is True
    assert captured.get("cancel_flag") is flag
    assert captured.get("overall_timeout_seconds") == 12.5
    assert captured.get("close_plaintext") is True


def test_runner_ordinary_exception_dirty_fail_closed() -> None:
    gen = _TrackingGen(chunks=[b"x"])

    def raise_run(*_a: Any, **_k: Any) -> RestoreProcessResult:
        raise RuntimeError("spawn boom")

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
        run_restore=raise_run,
    )
    assert report.ok is False
    assert report.code == CODE_FAILED
    assert report.target_may_be_dirty is True
    assert report.process_started is True
    assert gen.closed is True
    _assert_redacted(report, "spawn boom")


def test_runner_base_exception_propagates_after_close_and_wipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gen = _TrackingGen(chunks=[b"x"])
    wipe_log: list[str] = []

    import vuzol.ops.backup.restore_orchestrator as orch

    real_zeroize = orch._zeroize

    def tracking_zeroize(buf: bytearray) -> None:
        wipe_log.append("before")
        real_zeroize(buf)
        wipe_log.append("after")
        assert all(b == 0 for b in buf)

    monkeypatch.setattr(orch, "_zeroize", tracking_zeroize)

    def raise_base(*_a: Any, **_k: Any) -> RestoreProcessResult:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            decrypt_stream=lambda **_k: gen,
            run_restore=raise_base,
        )
    assert gen.closed is True
    assert wipe_log == ["before", "after"]


def test_apply_plaintext_fail_closes_gen() -> None:
    gen = _TrackingGen(chunks=[b"x"])
    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
        run_restore=lambda *_a, **_k: _ok_process(
            ok=False,
            code="restore_plaintext_failed",
            process_started=True,
            bytes_written=3,
        ),
    )
    assert report.code == "restore_plaintext_failed"
    assert report.target_may_be_dirty is True
    assert gen.closed is True


# ---------------------------------------------------------------------------
# Group 10: Redaction scan across report families
# ---------------------------------------------------------------------------


def test_redaction_across_report_families() -> None:
    families: list[RestoreOrchestrationReport] = [
        _run(),  # would_restore
        _run(verify_crypto=True, kek=_KEK),  # crypto_verified
        _run(mode=RestoreMode.APPLY, apply_authorized=True, kek=_KEK),  # restored
        _run(package_preflight=lambda **_k: _pkg_fail()),
        _run(target_preflight=lambda **_k: _tgt_fail()),
        _run(mode=RestoreMode.APPLY, apply_authorized=False),
        _run(mode="not_a_mode"),
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            probe_capture_lock=lambda: False,
        ),
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            assert_empty_target=lambda: (_ for _ in ()).throw(RuntimeError("x")),
        ),
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            run_restore=lambda *_a, **_k: _ok_process(
                ok=False,
                code="restore_process_failed",
                process_started=True,
            ),
        ),
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            run_restore=lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("spawn boom")),
        ),
        _run(
            verify_crypto=True,
            kek=_KEK,
            unwrap=lambda **_k: (_ for _ in ()).throw(BackupCryptoError("wrap")),
        ),
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            build_argv=lambda **_k: (_ for _ in ()).throw(ValueError("argv secret")),
        ),
    ]
    secret_extras = ("argv secret", "spawn boom", "RuntimeError")
    for report in families:
        _assert_redacted(report, *secret_extras)
        assert report.schedule == "disabled"
        payload = report.to_operational_payload()
        assert payload["schedule"] == "disabled"
        # Handles / paths never on payload keys beyond fixed fields.
        for forbidden_key in ("wrap_path", "blob_path", "argv", "stderr", "kek", "dek"):
            assert forbidden_key not in payload


def test_payload_partial_true_on_product_success() -> None:
    dry = _run()
    assert dry.partial is True
    verify = _run(verify_crypto=True, kek=_KEK)
    assert verify.partial is True
    apply = _run(mode=RestoreMode.APPLY, apply_authorized=True, kek=_KEK)
    assert apply.partial is True


def test_apply_missing_kek_no_spawn() -> None:
    restore_calls: list[Any] = []
    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=None,
        run_restore=lambda *_a, **_k: restore_calls.append(1) or _ok_process(),  # type: ignore[func-returns-value]
    )
    assert report.code == CODE_PREFLIGHT_KEK
    assert restore_calls == []


def test_apply_unwrap_and_decrypt_auth_and_generic() -> None:
    r1 = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        unwrap=lambda **_k: (_ for _ in ()).throw(BackupCryptoError("wrap")),
    )
    assert r1.code == CODE_WRAP_AUTH

    r2 = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        unwrap=lambda **_k: (_ for _ in ()).throw(RuntimeError("x")),
    )
    assert r2.code == CODE_CRYPTO

    r3 = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: (_ for _ in ()).throw(BackupCryptoError("blob")),
    )
    assert r3.code == CODE_BLOB_AUTH

    r4 = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: (_ for _ in ()).throw(RuntimeError("dec")),
    )
    assert r4.code == CODE_CRYPTO


# ---------------------------------------------------------------------------
# Correction pass: verify_crypto bool, KEK/DEK bytes, binder identity, cleanup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_verify",
    ["false", "true", 0, 1, None, object(), "False"],
)
def test_verify_crypto_non_bool_fails_closed_zero_calls(bad_verify: object) -> None:
    pkg_calls: list[Any] = []
    tgt_calls: list[Any] = []
    unwrap_calls: list[Any] = []
    restore_calls: list[Any] = []
    bind_calls: list[Any] = []

    def pkg(**k: Any) -> PackagePreflightReport:
        pkg_calls.append(k)
        return _pkg_ok()

    def tgt(**k: Any) -> TargetPreflightReport:
        tgt_calls.append(k)
        return _tgt_ok()

    def unwrap(**k: Any) -> bytes:
        unwrap_calls.append(k)
        return _DEK

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process()

    def bind(**k: Any) -> PublishedPackageHandle:
        bind_calls.append(k)
        return _handle()

    kwargs: dict[str, Any] = _base_kwargs(
        package_preflight=pkg,
        target_preflight=tgt,
        unwrap=unwrap,
        run_restore=run_restore,
        bind_package_handle=bind,
        kek=_KEK,
    )
    kwargs["verify_crypto"] = bad_verify
    report = asyncio.run(run_restore_orchestration(**kwargs))
    assert report.ok is False
    assert report.code == CODE_FAILED
    assert report.message == "invalid verify mode"
    assert pkg_calls == []
    assert tgt_calls == []
    assert unwrap_calls == []
    assert restore_calls == []
    assert bind_calls == []
    _assert_redacted(report)


@pytest.mark.parametrize(
    "bad_kek",
    [None, b"short", b"k" * 31, b"k" * 33, "k" * 32, bytearray(b"k" * 32), 32, object()],
)
def test_verify_invalid_kek_type_or_length(bad_kek: object) -> None:
    decrypt_calls: list[Any] = []
    restore_calls: list[Any] = []

    def decrypt(**k: Any) -> _TrackingGen:
        decrypt_calls.append(k)
        return _TrackingGen()

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process()

    kwargs: dict[str, Any] = _base_kwargs(
        verify_crypto=True,
        decrypt_stream=decrypt,
        run_restore=run_restore,
    )
    kwargs["kek"] = bad_kek
    report = asyncio.run(run_restore_orchestration(**kwargs))
    assert report.ok is False
    assert report.code == CODE_PREFLIGHT_KEK
    assert decrypt_calls == []
    assert restore_calls == []


@pytest.mark.parametrize(
    "bad_kek",
    [None, b"short", b"k" * 31, "k" * 32, bytearray(b"k" * 32), 1],
)
def test_apply_invalid_kek_type_or_length(bad_kek: object) -> None:
    unwrap_calls: list[Any] = []
    restore_calls: list[Any] = []

    def unwrap(**k: Any) -> bytes:
        unwrap_calls.append(k)
        return _DEK

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process()

    kwargs: dict[str, Any] = _base_kwargs(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        unwrap=unwrap,
        run_restore=run_restore,
    )
    kwargs["kek"] = bad_kek
    report = asyncio.run(run_restore_orchestration(**kwargs))
    assert report.ok is False
    assert report.code == CODE_PREFLIGHT_KEK
    assert unwrap_calls == []
    assert restore_calls == []


@pytest.mark.parametrize(
    "bad_dek",
    [b"short", b"d" * 31, "d" * 32, bytearray(b"d" * 32), None, 32],
)
def test_verify_invalid_unwrap_output_no_decrypt(bad_dek: object) -> None:
    decrypt_calls: list[Any] = []
    restore_calls: list[Any] = []

    def unwrap(**_k: Any) -> object:
        return bad_dek

    def decrypt(**k: Any) -> _TrackingGen:
        decrypt_calls.append(k)
        return _TrackingGen()

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process()

    kwargs: dict[str, Any] = _base_kwargs(
        verify_crypto=True,
        kek=_KEK,
        decrypt_stream=decrypt,
        run_restore=run_restore,
    )
    kwargs["unwrap"] = unwrap
    report = asyncio.run(run_restore_orchestration(**kwargs))
    assert report.ok is False
    assert report.code == CODE_CRYPTO
    assert decrypt_calls == []
    assert restore_calls == []


@pytest.mark.parametrize(
    "bad_dek",
    [b"short", bytearray(b"d" * 32), "d" * 32, None],
)
def test_apply_invalid_unwrap_output_no_decrypt_runner(bad_dek: object) -> None:
    decrypt_calls: list[Any] = []
    restore_calls: list[Any] = []

    def unwrap(**_k: Any) -> object:
        return bad_dek

    def decrypt(**k: Any) -> _TrackingGen:
        decrypt_calls.append(k)
        return _TrackingGen()

    def run_restore(*_a: Any, **_k: Any) -> RestoreProcessResult:
        restore_calls.append(1)
        return _ok_process()

    kwargs: dict[str, Any] = _base_kwargs(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=decrypt,
        run_restore=run_restore,
    )
    kwargs["unwrap"] = unwrap
    report = asyncio.run(run_restore_orchestration(**kwargs))
    assert report.ok is False
    assert report.code == CODE_CRYPTO
    assert decrypt_calls == []
    assert restore_calls == []


def test_default_binder_rejects_non_ok_package_report() -> None:
    bad = PackagePreflightReport(
        ok=False,
        code="preflight_package",
        message="nope",
        run_id=_RUN_ID_S,
    )
    handle = _default_bind_package_handle(
        staging_root=_STAGING,
        run_id=_RUN_ID,
        production=_production(),
        package_report=bad,
        mode_s="apply",
    )
    assert isinstance(handle, RestoreOrchestrationReport)
    assert handle.code == CODE_PACKAGE_REBIND
    assert str(_STAGING) not in handle.message


def test_default_binder_rejects_run_id_mismatch() -> None:
    other = uuid.UUID("11111111-2222-3333-4444-555555555555")
    pkg = PackagePreflightReport(
        ok=True,
        code="package_ok",
        message="ok",
        run_id=str(other),
        partial=True,
    )
    handle = _default_bind_package_handle(
        staging_root=_STAGING,
        run_id=_RUN_ID,
        production=_production(),
        package_report=pkg,
        mode_s="apply",
    )
    assert isinstance(handle, RestoreOrchestrationReport)
    assert handle.code == CODE_PACKAGE_REBIND
    assert handle.run_id is None


def test_default_binder_identity_mismatch_never_echoes_secret_run_id() -> None:
    """Malformed injected run_id must not appear on the failure report/payload."""

    # Marker must look secret but avoid bandit S105 password heuristics.
    secret = "exfil-marker-" + "s3cret" + "-not-a-uuid"  # pragma: allowlist secret
    pkg = PackagePreflightReport(
        ok=True,
        code="package_ok",
        message="ok",
        run_id=secret,
        partial=True,
    )
    handle = _default_bind_package_handle(
        staging_root=_STAGING,
        run_id=_RUN_ID,
        production=_production(),
        package_report=pkg,
        mode_s="apply",
    )
    assert isinstance(handle, RestoreOrchestrationReport)
    assert handle.code == CODE_PACKAGE_REBIND
    assert handle.run_id is None
    blob = str(handle.to_operational_payload()) + handle.message + str(handle)
    assert secret not in blob
    assert "s3cret" not in blob
    assert "exfil-marker-" not in blob


def test_apply_two_pass_binder_receives_exact_second_package_report() -> None:
    first = PackagePreflightReport(
        ok=True,
        code="package_ok",
        message="first pass",
        run_id=_RUN_ID_S,
        partial=True,
        size_ciphertext=1,
    )
    second = PackagePreflightReport(
        ok=True,
        code="package_ok",
        message="second pass",
        run_id=_RUN_ID_S,
        partial=True,
        size_ciphertext=2,
    )
    assert first is not second
    n = {"i": 0}
    bound: list[PackagePreflightReport] = []

    def pkg(**_k: Any) -> PackagePreflightReport:
        n["i"] += 1
        return first if n["i"] == 1 else second

    def bind(**kwargs: Any) -> PublishedPackageHandle:
        report = kwargs["package_report"]
        assert isinstance(report, PackagePreflightReport)
        bound.append(report)
        return _handle()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        package_preflight=pkg,
        bind_package_handle=bind,
    )
    assert report.ok is True
    assert n["i"] == 2
    assert len(bound) == 1
    assert bound[0] is second
    assert bound[0] is not first
    assert bound[0].size_ciphertext == 2


class _EvilCloseAttrGen:
    """Iterator whose close attribute lookup raises."""

    def __init__(self) -> None:
        self.consumed = 0

    def __iter__(self) -> _EvilCloseAttrGen:
        return self

    def __next__(self) -> bytes:
        if self.consumed:
            raise StopIteration
        self.consumed += 1
        return b"x"

    @property
    def close(self) -> object:
        raise RuntimeError("close attr boom")


class _EvilCloseInvokeGen:
    """Iterator whose close() invocation raises."""

    def __init__(self) -> None:
        self.consumed = 0
        self.close_called = False

    def __iter__(self) -> _EvilCloseInvokeGen:
        return self

    def __next__(self) -> bytes:
        if self.consumed:
            raise StopIteration
        self.consumed += 1
        return b"x"

    def close(self) -> None:
        self.close_called = True
        raise RuntimeError("close invoke boom")


def test_close_attr_raises_preserves_result_and_wipes_dek(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vuzol.ops.backup.restore_orchestrator as orch

    wipe_log: list[str] = []
    real_zeroize = orch._zeroize

    def tracking_zeroize(buf: bytearray) -> None:
        wipe_log.append("before")
        real_zeroize(buf)
        wipe_log.append("after")
        assert all(b == 0 for b in buf)

    monkeypatch.setattr(orch, "_zeroize", tracking_zeroize)
    gen = _EvilCloseAttrGen()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
    )
    assert report.ok is True
    assert report.code == CODE_RESTORED_ORCH
    assert wipe_log == ["before", "after"]
    _assert_redacted(report, "close attr boom")


def test_close_invoke_raises_preserves_result_and_wipes_dek(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vuzol.ops.backup.restore_orchestrator as orch

    wipe_log: list[str] = []
    real_zeroize = orch._zeroize

    def tracking_zeroize(buf: bytearray) -> None:
        wipe_log.append("before")
        real_zeroize(buf)
        wipe_log.append("after")
        assert all(b == 0 for b in buf)

    monkeypatch.setattr(orch, "_zeroize", tracking_zeroize)
    gen = _EvilCloseInvokeGen()

    report = _run(
        mode=RestoreMode.APPLY,
        apply_authorized=True,
        kek=_KEK,
        decrypt_stream=lambda **_k: gen,
    )
    assert report.ok is True
    assert report.code == CODE_RESTORED_ORCH
    assert gen.close_called is True
    assert wipe_log == ["before", "after"]
    _assert_redacted(report, "close invoke boom")


def test_close_attr_raises_base_exception_still_wipes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vuzol.ops.backup.restore_orchestrator as orch

    wipe_log: list[str] = []
    real_zeroize = orch._zeroize

    def tracking_zeroize(buf: bytearray) -> None:
        wipe_log.append("before")
        real_zeroize(buf)
        wipe_log.append("after")
        assert all(b == 0 for b in buf)

    monkeypatch.setattr(orch, "_zeroize", tracking_zeroize)
    gen = _EvilCloseAttrGen()

    def raise_base(*_a: Any, **_k: Any) -> RestoreProcessResult:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            decrypt_stream=lambda **_k: gen,
            run_restore=raise_base,
        )
    assert wipe_log == ["before", "after"]


def test_close_invoke_raises_base_exception_still_wipes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vuzol.ops.backup.restore_orchestrator as orch

    wipe_log: list[str] = []
    real_zeroize = orch._zeroize

    def tracking_zeroize(buf: bytearray) -> None:
        wipe_log.append("before")
        real_zeroize(buf)
        wipe_log.append("after")
        assert all(b == 0 for b in buf)

    monkeypatch.setattr(orch, "_zeroize", tracking_zeroize)
    gen = _EvilCloseInvokeGen()

    def raise_base(*_a: Any, **_k: Any) -> RestoreProcessResult:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        _run(
            mode=RestoreMode.APPLY,
            apply_authorized=True,
            kek=_KEK,
            decrypt_stream=lambda **_k: gen,
            run_restore=raise_base,
        )
    assert gen.close_called is True
    assert wipe_log == ["before", "after"]
