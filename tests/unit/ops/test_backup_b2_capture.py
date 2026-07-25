"""Unit tests for B2 staging, dump argv, capture orchestration, and CLI."""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from vuzol.config.settings import BackupSettings, Settings
from vuzol.ops.backup.capture import (
    BackupCaptureRunner,
    CaptureMode,
    CaptureReport,
    _load_kek,
    _safe_hostname,
)
from vuzol.ops.backup.crypto import BackupCryptoError, decrypt_blob_stream, unwrap_dek
from vuzol.ops.backup.paths import BackupPathError, ProductionRoots
from vuzol.ops.backup.postgres_dump import (
    PostgresDumpError,
    build_pg_dump_argv,
    iter_dump_stdout,
    parse_dump_identity,
)
from vuzol.ops.backup.staging import (
    STATE_DUMPING,
    STATE_FAILED,
    STATE_PUBLISHED,
    BackupStagingError,
    assert_safe_staging_root,
    cleanup_run_dir,
    ensure_staging_tree,
    free_space_bytes,
    gc_incomplete_runs,
    prune_published_runs,
    publish_run,
    read_state,
    write_state,
)


def test_dump_argv_builder_and_rejects() -> None:
    argv = build_pg_dump_argv(container="vuzol-postgres-1", user="vuzol", database="vuzol")
    assert argv[:3] == ["docker", "exec", "-i"]
    assert "-Fc" in argv
    with pytest.raises(PostgresDumpError):
        build_pg_dump_argv(container="../evil", user="vuzol", database="vuzol")
    with pytest.raises(PostgresDumpError):
        build_pg_dump_argv(
            container="c",
            user="u",
            database="d",
            override=("bash", "-c", "rm -rf /"),
        )
    with pytest.raises(PostgresDumpError):
        build_pg_dump_argv(container="ok", user="bad user", database="d")
    with pytest.raises(PostgresDumpError):
        build_pg_dump_argv(
            container="c",
            user="u",
            database="d",
            override=("pg_dump", "a;b"),
        )
    ok = build_pg_dump_argv(
        container="c1",
        user="u1",
        database="d1",
        override=("pg_dump", "-U", "{user}", "-d", "{database}", "-Fc"),
    )
    assert ok == ["pg_dump", "-U", "u1", "-d", "d1", "-Fc"]


def test_parse_dump_identity() -> None:
    ident = parse_dump_identity(
        "postgresql://vuzol:secret@127.0.0.1:5432/vuzol_test"  # pragma: allowlist secret
    )
    assert ident.user == "vuzol"
    assert ident.database == "vuzol_test"
    assert ident.password == "secret"  # noqa: S105  # pragma: allowlist secret
    with pytest.raises(PostgresDumpError):
        parse_dump_identity("mysql://u@h/db")
    with pytest.raises(PostgresDumpError):
        parse_dump_identity("postgresql://@127.0.0.1/db")
    with pytest.raises(PostgresDumpError):
        parse_dump_identity("postgresql://user@127.0.0.1/")
    with pytest.raises(PostgresDumpError):
        parse_dump_identity("postgresql://bad-user!@127.0.0.1/db")


def test_staging_conflict_and_publish_layout(tmp_path: Path) -> None:
    prod_art = tmp_path / "artifacts"
    prod_art.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()
    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=prod_art,
        secret_file_root=tmp_path / "secrets",
    )
    for root in production.all_roots()[:4]:
        root.mkdir(parents=True, exist_ok=True)
    resolved = assert_safe_staging_root(staging, production)
    assert resolved == staging.resolve()
    with pytest.raises(BackupPathError):
        assert_safe_staging_root(prod_art / "nested", production)
    run_id = uuid.uuid4()
    run_dir, _tmp, _publish = ensure_staging_tree(staging, run_id)
    assert (run_dir / "STATE").read_text().startswith("starting")
    assert free_space_bytes(staging) > 0
    write_state(run_dir, STATE_PUBLISHED)
    assert read_state(run_dir) == STATE_PUBLISHED
    assert read_state(tmp_path / "missing-run") is None
    # second published for prune
    run2 = uuid.uuid4()
    d2, _, _ = ensure_staging_tree(staging, run2)
    write_state(d2, STATE_PUBLISHED)
    run3 = uuid.uuid4()
    d3, _, _ = ensure_staging_tree(staging, run3)
    write_state(d3, STATE_PUBLISHED)
    removed = prune_published_runs(staging, keep=2)
    assert removed == 1
    write_state(ensure_staging_tree(staging, uuid.uuid4())[0], "dumping")
    assert gc_incomplete_runs(staging, production, max_age_seconds=0) >= 1
    assert prune_published_runs(tmp_path / "no-runs", keep=1) == 0
    empty_staging = tmp_path / "no-runs"
    empty_staging.mkdir()
    assert gc_incomplete_runs(empty_staging, production) == 0

    # publish_run missing source
    rid = uuid.uuid4()
    rd, t, p = ensure_staging_tree(staging, rid)
    with pytest.raises(Exception, match="missing publish"):
        publish_run(run_dir=rd, tmp=t, publish=p, files={"x": t / "missing"})

    # cleanup outside staging refused
    outside = tmp_path / "outside-run"
    outside.mkdir()
    with pytest.raises(Exception, match="refuse cleanup"):
        cleanup_run_dir(outside, staging)


def test_gc_incomplete_refuses_production_root_conflict(tmp_path: Path) -> None:
    """gc must not traverse/delete when staging nests under a production root."""

    prod_art = tmp_path / "artifacts"
    prod_art.mkdir()
    # Unsafe: staging lives inside production artifact root.
    staging = prod_art / "nested-staging"
    staging.mkdir()
    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=prod_art,
        secret_file_root=tmp_path / "secrets",
    )
    for root in (production.repository_root, production.worktree_root, production.secret_file_root):
        root.mkdir(parents=True, exist_ok=True)

    rid = uuid.uuid4()
    run_dir, _, _ = ensure_staging_tree(staging, rid)
    write_state(run_dir, STATE_FAILED)
    marker = run_dir / "keep-me"
    marker.write_text("x", encoding="utf-8")

    with pytest.raises(BackupPathError):
        gc_incomplete_runs(staging, production, max_age_seconds=0)

    # No deletion on refusal.
    assert marker.is_file()
    assert run_dir.is_dir()


def test_gc_incomplete_refuses_symlink_to_production(tmp_path: Path) -> None:
    """Symlink staging that resolves into production must fail closed before GC."""

    prod_art = tmp_path / "artifacts"
    prod_art.mkdir()
    # Real directory under production that incomplete runs would live in if GC ran.
    real_under_prod = prod_art / "real-staging"
    real_under_prod.mkdir()
    link_staging = tmp_path / "staging-link"
    link_staging.symlink_to(real_under_prod)

    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=prod_art,
        secret_file_root=tmp_path / "secrets",
    )
    for root in (production.repository_root, production.worktree_root, production.secret_file_root):
        root.mkdir(parents=True, exist_ok=True)

    rid = uuid.uuid4()
    run_dir, _, _ = ensure_staging_tree(real_under_prod, rid)
    write_state(run_dir, "dumping")
    marker = run_dir / "payload"
    marker.write_text("stay", encoding="utf-8")

    with pytest.raises(BackupPathError):
        gc_incomplete_runs(link_staging, production, max_age_seconds=0)

    assert marker.is_file()
    assert run_dir.is_dir()


def test_gc_incomplete_safe_root_removes_incomplete_only(tmp_path: Path) -> None:
    """Safe isolated staging: incomplete runs removed; published preserved."""

    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
    )
    for root in production.all_roots()[:4]:
        root.mkdir(parents=True, exist_ok=True)
    staging = tmp_path / "staging"
    staging.mkdir()
    assert_safe_staging_root(staging, production)

    published_id = uuid.uuid4()
    pub_dir, _, _ = ensure_staging_tree(staging, published_id)
    write_state(pub_dir, STATE_PUBLISHED)
    (pub_dir / "keep").write_text("published", encoding="utf-8")

    failed_id = uuid.uuid4()
    fail_dir, _, _ = ensure_staging_tree(staging, failed_id)
    write_state(fail_dir, STATE_FAILED)
    (fail_dir / "drop").write_text("incomplete", encoding="utf-8")

    removed = gc_incomplete_runs(staging, production, max_age_seconds=3600.0)
    assert removed == 1
    assert pub_dir.is_dir()
    assert (pub_dir / "keep").is_file()
    assert not fail_dir.exists()


def test_gc_incomplete_preserves_young_dumping_under_default_age(tmp_path: Path) -> None:
    """C3: young dumping runs are not removed under default max_age_seconds=3600."""

    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
    )
    for root in production.all_roots()[:4]:
        root.mkdir(parents=True, exist_ok=True)
    staging = tmp_path / "staging"
    staging.mkdir()

    dump_dir, _, _ = ensure_staging_tree(staging, uuid.uuid4())
    write_state(dump_dir, STATE_DUMPING)
    marker = dump_dir / "in-progress"
    marker.write_text("live", encoding="utf-8")

    removed = gc_incomplete_runs(staging, production, max_age_seconds=3600.0)
    assert removed == 0
    assert dump_dir.is_dir()
    assert marker.is_file()
    assert read_state(dump_dir) == STATE_DUMPING


def test_gc_incomplete_refuses_escaping_runs_symlink(tmp_path: Path) -> None:
    """C2: runs/ symlink that resolves outside staging fails before scan/delete."""

    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
    )
    for root in production.all_roots()[:4]:
        root.mkdir(parents=True, exist_ok=True)
    staging = tmp_path / "staging"
    staging.mkdir()
    foreign = tmp_path / "foreign-runs"
    foreign.mkdir()
    victim = foreign / "escape-run"
    victim.mkdir()
    (victim / "STATE").write_text("failed\n", encoding="utf-8")
    marker = victim / "payload"
    marker.write_text("stay", encoding="utf-8")
    (staging / "runs").symlink_to(foreign)

    with pytest.raises(BackupStagingError, match="refuse path outside staging_root"):
        gc_incomplete_runs(staging, production, max_age_seconds=0)

    assert marker.is_file()
    assert victim.is_dir()


def test_gc_incomplete_refuses_escaping_child_symlink(tmp_path: Path) -> None:
    """C2: individual run dir symlink escaping staging fails before read_state."""

    production = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
    )
    for root in production.all_roots()[:4]:
        root.mkdir(parents=True, exist_ok=True)
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "runs").mkdir()
    foreign = tmp_path / "foreign-child"
    foreign.mkdir()
    (foreign / "STATE").write_text("failed\n", encoding="utf-8")
    marker = foreign / "payload"
    marker.write_text("stay", encoding="utf-8")
    (staging / "runs" / "linked-run").symlink_to(foreign)

    with pytest.raises(BackupStagingError, match="refuse path outside staging_root"):
        gc_incomplete_runs(staging, production, max_age_seconds=0)

    assert marker.is_file()


def test_cli_gc_io_error_distinct_from_path_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """C1: OSError during GC maps to gc_io_error, not preflight_path_conflict."""

    from vuzol.cli import backup as backup_cli

    settings = _base_settings(tmp_path)
    runtime = MagicMock()
    runtime.settings = settings
    monkeypatch.setattr(backup_cli, "get_runtime_configuration", lambda **_k: runtime)
    monkeypatch.setattr(backup_cli, "configure_logging", lambda **_k: None)
    monkeypatch.setattr(backup_cli, "get_logger", lambda _n: MagicMock())

    def _raise_oserror(*_a: object, **_k: object) -> int:
        raise OSError(13, "permission denied")

    monkeypatch.setattr(backup_cli, "gc_incomplete_runs", _raise_oserror)
    code = asyncio.run(backup_cli._run(backup_cli._parse_args(["gc-staging", "--json"])))
    assert code == 1
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["ok"] is False
    assert payload["code"] == "gc_io_error"
    assert payload["schedule"] == "disabled"
    assert "permission denied" not in out
    assert str(tmp_path) not in out


def test_backup_settings_capture_gate() -> None:
    assert BackupSettings().capture_cli_permitted is False
    with pytest.raises(ValidationError):
        BackupSettings(enabled=True)
    BackupSettings(capture_cli_permitted=True, staging_root=Path("/var/tmp/staging"))  # noqa: S108


def test_capture_report_payload() -> None:
    report = CaptureReport(
        ok=True,
        mode=CaptureMode.DRY_RUN,
        code="would_capture",
        message="ok",
        run_id=None,
        detail={"schedule": "disabled"},
    )
    payload = report.to_operational_payload()
    assert payload["ok"] is True
    assert payload["mode"] == "dry_run"
    detail = payload["detail"]
    assert isinstance(detail, dict)
    assert detail["schedule"] == "disabled"


def _base_settings(tmp_path: Path, **backup_kw: object) -> Settings:
    staging = tmp_path / "staging"
    secrets = tmp_path / "sec"
    staging.mkdir(exist_ok=True)
    secrets.mkdir(exist_ok=True)
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=secrets,
        backup=BackupSettings(
            staging_root=staging,
            capture_cli_permitted=True,
            allow_non_loopback_dump=True,
            min_free_bytes=0,
            **backup_kw,  # type: ignore[arg-type]
        ),
    )
    for path in (
        settings.repository_root,
        settings.worktree_root,
        settings.artifact_root,
        settings.secret_file_root,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return settings


class _Result:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar(self) -> object:
        return self._value


class _Conn:
    def __init__(
        self,
        *,
        retention_busy: bool = False,
        backup_busy: bool = False,
        db_size: object = 1024,
        alembic: object = "b2c9e4f81a03",  # pragma: allowlist secret
        raise_on: str | None = None,
    ) -> None:
        self.locks: set[int] = set()
        self.retention_busy = retention_busy
        self.backup_busy = backup_busy
        self.db_size = db_size
        self.alembic = alembic
        self.raise_on = raise_on
        self._retention_attempts = 0
        self._backup_attempts = 0

    async def execute(self, statement: object, params: object = None) -> _Result:
        sql = str(statement)
        if self.raise_on and self.raise_on in sql:
            raise RuntimeError("forced")
        if "pg_try_advisory_lock" in sql:
            key = params["key"] if isinstance(params, dict) else None
            assert isinstance(key, int)
            if key == 8_946_527_105:  # retention
                self._retention_attempts += 1
                if self.retention_busy:
                    return _Result(False)
            if key == 8_946_527_110:  # backup
                self._backup_attempts += 1
                if self.backup_busy:
                    return _Result(False)
            if key in self.locks:
                return _Result(False)
            self.locks.add(key)
            return _Result(True)
        if "pg_advisory_unlock" in sql and isinstance(params, dict):
            self.locks.discard(params["key"])
            return _Result(True)
        if "pg_database_size" in sql:
            return _Result(self.db_size)
        if "alembic_version" in sql:
            return _Result(self.alembic)
        return _Result(None)

    async def scalar(self, statement: object, params: object = None) -> object:
        result = await self.execute(statement, params)
        return result.scalar()

    async def close(self) -> None:
        return None


class _Engine:
    def __init__(self, conn: _Conn | None = None) -> None:
        self.conn = conn or _Conn()

    async def connect(self) -> _Conn:
        return self.conn

    async def dispose(self) -> None:
        return None


class _FailingConnectEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def connect(self) -> _Conn:
        raise RuntimeError("database details must not escape")

    async def dispose(self) -> None:
        self.disposed = True


@pytest.mark.anyio
async def test_capture_dry_run_no_files(tmp_path: Path) -> None:
    settings = _base_settings(tmp_path)
    runner = BackupCaptureRunner(
        settings,
        dsn="postgresql://vuzol:x@127.0.0.1:5432/vuzol_test",
    )
    report = await runner.run(mode=CaptureMode.DRY_RUN)
    assert report.ok
    assert report.code == "would_capture"
    staging = settings.backup.staging_root
    assert staging is not None
    assert not (staging / "runs").exists()


@pytest.mark.anyio
async def test_capture_apply_with_fake_dump(tmp_path: Path) -> None:
    kek = bytes(range(32))
    secrets = tmp_path / "sec"
    secrets.mkdir(exist_ok=True)
    (secrets / "backup_kek").write_bytes(kek)
    settings = _base_settings(tmp_path, kek_reference="file:backup_kek")
    payload = b"FAKE-PG-DUMP-BYTES" * 100

    def fake_stream(_argv: list[str]) -> list[bytes]:
        return [payload]

    with patch("vuzol.ops.backup.capture.create_async_engine", return_value=_Engine()):
        runner = BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1:5432/vuzol_test",
            kek_bytes=kek,
            dump_stream_factory=fake_stream,
        )
        report = await runner.run(mode=CaptureMode.APPLY)
    assert report.ok, report
    assert report.code == "captured"
    assert report.detail and report.detail.get("schedule") == "disabled"
    staging = settings.backup.staging_root
    assert staging is not None
    runs = list((staging / "runs").iterdir())
    assert len(runs) == 1
    publish = runs[0] / "publish"
    assert (publish / "postgres.dump.enc").is_file()
    assert (publish / "dek.wrap").is_file()
    assert (publish / "manifest.v1.json").is_file()
    assert read_state(runs[0]) == STATE_PUBLISHED
    run_id = uuid.UUID(runs[0].name)
    dek = unwrap_dek(kek=kek, wrap_path=publish / "dek.wrap", expected_run_id=run_id)
    plain = b"".join(
        decrypt_blob_stream(
            dek=dek,
            blob_path=publish / "postgres.dump.enc",
            run_id=run_id,
            component="postgres",
            fmt="pg_custom",
        )
    )
    assert plain == payload


def test_capture_not_permitted_without_flag(tmp_path: Path) -> None:
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "r",
        worktree_root=tmp_path / "w",
        artifact_root=tmp_path / "a",
        secret_file_root=tmp_path / "s",
        backup=BackupSettings(staging_root=tmp_path / "st", capture_cli_permitted=False),
    )
    for path in (
        settings.repository_root,
        settings.worktree_root,
        settings.artifact_root,
        settings.secret_file_root,
        tmp_path / "st",
    ):
        path.mkdir(parents=True, exist_ok=True)

    async def _run() -> None:
        report = await BackupCaptureRunner(
            settings, dsn="postgresql://vuzol:x@127.0.0.1/vuzol"
        ).run(mode=CaptureMode.APPLY)
        assert report.ok is False
        assert report.code == "capture_not_permitted"

    asyncio.run(_run())


@pytest.mark.anyio
async def test_capture_preflight_failures(tmp_path: Path) -> None:
    # missing staging
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "r",
        worktree_root=tmp_path / "w",
        artifact_root=tmp_path / "a",
        secret_file_root=tmp_path / "s",
        backup=BackupSettings(staging_root=None, capture_cli_permitted=True),
    )
    for path in (
        settings.repository_root,
        settings.worktree_root,
        settings.artifact_root,
        settings.secret_file_root,
    ):
        path.mkdir(parents=True, exist_ok=True)
    report = await BackupCaptureRunner(settings, dsn="postgresql://u:p@127.0.0.1/db").run(
        CaptureMode.DRY_RUN
    )
    assert report.code == "preflight_staging"

    # path conflict with production artifact root
    settings2 = _base_settings(tmp_path)
    bad = Settings(
        environment="test",
        repository_root=settings2.repository_root,
        worktree_root=settings2.worktree_root,
        artifact_root=settings2.artifact_root,
        secret_file_root=settings2.secret_file_root,
        backup=BackupSettings(
            staging_root=settings2.artifact_root / "nested",
            capture_cli_permitted=True,
            allow_non_loopback_dump=True,
        ),
    )
    (settings2.artifact_root / "nested").mkdir(parents=True, exist_ok=True)
    report = await BackupCaptureRunner(bad, dsn="postgresql://u:p@127.0.0.1/db").run(
        CaptureMode.DRY_RUN
    )
    assert report.code == "preflight_path_conflict"

    # missing dsn
    ok_settings = _base_settings(tmp_path)
    report = await BackupCaptureRunner(ok_settings, dsn=None).run(CaptureMode.DRY_RUN)
    assert report.code == "preflight_dsn"

    # bad dsn
    report = await BackupCaptureRunner(ok_settings, dsn="not-a-dsn").run(CaptureMode.DRY_RUN)
    assert report.code == "preflight_dsn"

    # non-loopback refused
    loop_settings = Settings(
        environment="test",
        repository_root=ok_settings.repository_root,
        worktree_root=ok_settings.worktree_root,
        artifact_root=ok_settings.artifact_root,
        secret_file_root=ok_settings.secret_file_root,
        backup=BackupSettings(
            staging_root=ok_settings.backup.staging_root,
            capture_cli_permitted=True,
            allow_non_loopback_dump=False,
        ),
    )
    report = await BackupCaptureRunner(loop_settings, dsn="postgresql://u:p@8.8.8.8/db").run(
        CaptureMode.DRY_RUN
    )
    assert report.code == "preflight_dsn"
    assert "non-loopback" in report.message


@pytest.mark.anyio
async def test_capture_bad_container_argv(tmp_path: Path) -> None:
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "r",
        worktree_root=tmp_path / "w",
        artifact_root=tmp_path / "a",
        secret_file_root=tmp_path / "s",
        backup=BackupSettings(
            staging_root=tmp_path / "st",
            capture_cli_permitted=True,
            allow_non_loopback_dump=True,
            postgres_container="vuzol-postgres-1",
            postgres_dump_argv=("bash", "-c", "id"),
        ),
    )
    for path in (
        settings.repository_root,
        settings.worktree_root,
        settings.artifact_root,
        settings.secret_file_root,
        tmp_path / "st",
    ):
        path.mkdir(parents=True, exist_ok=True)
    report = await BackupCaptureRunner(settings, dsn="postgresql://vuzol:x@127.0.0.1/vuzol").run(
        CaptureMode.DRY_RUN
    )
    assert report.ok is False
    assert "postgres" in report.code or "shell" in report.message.lower() or report.code


@pytest.mark.anyio
async def test_capture_lock_busy_paths(tmp_path: Path) -> None:
    kek = bytes(range(32))
    settings = _base_settings(tmp_path)

    with patch(
        "vuzol.ops.backup.capture.create_async_engine",
        return_value=_Engine(_Conn(retention_busy=True)),
    ):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=kek,
            dump_stream_factory=lambda _a: [b"x"],
        ).run(CaptureMode.APPLY)
    assert report.code == "lock_busy_retention"

    with patch(
        "vuzol.ops.backup.capture.create_async_engine",
        return_value=_Engine(_Conn(backup_busy=True)),
    ):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=kek,
            dump_stream_factory=lambda _a: [b"x"],
        ).run(CaptureMode.APPLY)
    assert report.code == "lock_busy"


@pytest.mark.anyio
async def test_capture_disk_and_size_failures(tmp_path: Path) -> None:
    kek = bytes(range(32))
    settings = _base_settings(tmp_path)

    with patch(
        "vuzol.ops.backup.capture.create_async_engine",
        return_value=_Engine(_Conn(db_size=None)),
    ):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=kek,
            dump_stream_factory=lambda _a: [b"x"],
        ).run(CaptureMode.APPLY)
    assert report.code == "preflight_db_size"

    with (
        patch(
            "vuzol.ops.backup.capture.create_async_engine",
            return_value=_Engine(_Conn(db_size=10_000_000)),
        ),
        patch("vuzol.ops.backup.capture.free_space_bytes", return_value=1),
    ):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=kek,
            dump_stream_factory=lambda _a: [b"x"],
        ).run(CaptureMode.APPLY)
    assert report.code == "preflight_disk"


@pytest.mark.anyio
async def test_capture_encrypt_failure_cleans_run(tmp_path: Path) -> None:
    kek = bytes(range(32))
    settings = _base_settings(tmp_path)

    def boom(_argv: list[str]) -> list[bytes]:
        raise PostgresDumpError("pg_dump_failed", "boom")

    with patch("vuzol.ops.backup.capture.create_async_engine", return_value=_Engine()):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=kek,
            dump_stream_factory=boom,
        ).run(CaptureMode.APPLY)
    assert report.ok is False
    assert report.code == "pg_dump_failed"
    staging = settings.backup.staging_root
    assert staging is not None
    # failed run cleaned by default keep_failed_runs=False
    runs = list((staging / "runs").iterdir()) if (staging / "runs").exists() else []
    assert runs == []


@pytest.mark.anyio
async def test_capture_generic_exception_path(tmp_path: Path) -> None:
    kek = bytes(range(32))
    settings = _base_settings(tmp_path)

    with patch(
        "vuzol.ops.backup.capture.create_async_engine",
        return_value=_Engine(_Conn(raise_on="pg_database_size")),
    ):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=kek,
            dump_stream_factory=lambda _a: [b"x"],
        ).run(CaptureMode.APPLY)
    assert report.code == "capture_failed"


@pytest.mark.anyio
async def test_capture_connection_failure_is_reported_and_disposed(tmp_path: Path) -> None:
    settings = _base_settings(tmp_path)
    engine = _FailingConnectEngine()

    with patch("vuzol.ops.backup.capture.create_async_engine", return_value=engine):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:secret@127.0.0.1/vuzol",
            kek_bytes=bytes(range(32)),
        ).run(CaptureMode.APPLY)

    assert report.ok is False
    assert report.code == "capture_failed"
    assert report.message == "RuntimeError"
    assert "secret" not in str(report.to_operational_payload())
    assert engine.disposed is True


@pytest.mark.anyio
async def test_capture_engine_factory_failure_is_reported(tmp_path: Path) -> None:
    settings = _base_settings(tmp_path)

    with patch(
        "vuzol.ops.backup.capture.create_async_engine",
        side_effect=RuntimeError("database details must not escape"),
    ):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:secret@127.0.0.1/vuzol",
            kek_bytes=bytes(range(32)),
        ).run(CaptureMode.APPLY)

    assert report.ok is False
    assert report.code == "capture_failed"
    assert report.message == "RuntimeError"
    assert "secret" not in str(report.to_operational_payload())


@pytest.mark.anyio
async def test_capture_loads_kek_from_file_and_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kek = bytes(range(32))
    secrets = tmp_path / "sec"
    secrets.mkdir(exist_ok=True)
    (secrets / "backup_kek").write_bytes(kek)
    settings = _base_settings(tmp_path, kek_reference="file:backup_kek")
    assert _load_kek(settings, settings.backup) == kek

    monkeypatch.setenv("VUZOL_TEST_BACKUP_KEK", kek.hex())
    env_settings = _base_settings(tmp_path, kek_reference="env:VUZOL_TEST_BACKUP_KEK")
    assert _load_kek(env_settings, env_settings.backup) == kek

    with pytest.raises(BackupCryptoError):
        _load_kek(_base_settings(tmp_path), BackupSettings())

    bare = _base_settings(tmp_path)
    with pytest.raises(BackupCryptoError):
        monkeypatch.delenv("MISSING_KEK", raising=False)
        env_missing = BackupSettings(
            staging_root=bare.backup.staging_root,
            capture_cli_permitted=True,
            kek_reference="env:MISSING_KEK",
        )
        _load_kek(bare, env_missing)

    # apply path loads kek when not injected
    with patch("vuzol.ops.backup.capture.create_async_engine", return_value=_Engine()):
        report = await BackupCaptureRunner(
            settings,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=None,
            dump_stream_factory=lambda _a: [b"payload"],
        ).run(CaptureMode.APPLY)
    assert report.ok, report

    # missing kek preflight
    no_kek = _base_settings(tmp_path)  # no kek_reference
    with patch("vuzol.ops.backup.capture.create_async_engine", return_value=_Engine()):
        report = await BackupCaptureRunner(
            no_kek,
            dsn="postgresql://vuzol:x@127.0.0.1/vuzol",
            kek_bytes=None,
            dump_stream_factory=lambda _a: [b"x"],
        ).run(CaptureMode.APPLY)
    assert report.code == "preflight_kek"


def test_safe_hostname() -> None:
    name = _safe_hostname()
    assert name
    assert len(name) <= 100


def test_dump_process_group_cancel_no_hang() -> None:
    """Cancel mid-stream must terminate process group quickly."""
    import time

    argv = [
        "python3",
        "-c",
        "import sys,time\n"
        "while True:\n"
        " sys.stdout.buffer.write(b'x'*4096); sys.stdout.buffer.flush(); time.sleep(0.01)\n",
    ]
    cancelled = {"v": False}

    def flag() -> bool:
        return cancelled["v"]

    start = time.monotonic()
    try:
        gen = iter_dump_stdout(argv, cancel_flag=flag, read_size=1024, wait_timeout_seconds=2)
        next(gen)
        cancelled["v"] = True
        for _ in gen:
            if time.monotonic() - start > 8:
                break
    except PostgresDumpError as error:
        assert error.code == "cancelled"
    elapsed = time.monotonic() - start
    assert elapsed < 10.0


def test_iter_dump_stdout_success_and_failure() -> None:
    # successful short process
    chunks = list(
        iter_dump_stdout(
            ["python3", "-c", "import sys; sys.stdout.buffer.write(b'hello')"],
            wait_timeout_seconds=5,
        )
    )
    assert b"".join(chunks) == b"hello"

    # non-zero exit
    with pytest.raises(PostgresDumpError) as exc:
        list(
            iter_dump_stdout(
                ["python3", "-c", "import sys; sys.exit(7)"],
                wait_timeout_seconds=5,
            )
        )
    assert exc.value.code == "pg_dump_failed"

    # spawn failure
    with pytest.raises(PostgresDumpError) as exc2:
        list(iter_dump_stdout(["/nonexistent/binary/vuzol-dump-xyz"], wait_timeout_seconds=2))
    assert exc2.value.code == "preflight_postgres"

    # env merge path (PGPASSWORD) with successful echo
    script = "import os,sys; sys.stdout.buffer.write(os.environ.get('PGPASSWORD','x').encode())"
    chunks = list(
        iter_dump_stdout(
            ["python3", "-c", script],
            env={"PGPASSWORD": "unit"},  # pragma: allowlist secret
            wait_timeout_seconds=5,
        )
    )
    assert b"".join(chunks) == b"unit"

    with pytest.raises(PostgresDumpError):
        list(iter_dump_stdout([123]))  # type: ignore[list-item]


def test_iter_dump_open_stream_without_factory(tmp_path: Path) -> None:
    """Exercise _open_dump_stream real path (no factory) via runner helper."""
    settings = _base_settings(tmp_path)
    runner = BackupCaptureRunner(
        settings,
        dsn="postgresql://vuzol:pw@127.0.0.1/vuzol",  # pragma: allowlist secret
    )
    from vuzol.ops.backup.postgres_dump import DumpIdentity

    identity = DumpIdentity(
        user="vuzol",
        database="vuzol",
        password="pw",  # noqa: S106  # pragma: allowlist secret
        host="127.0.0.1",
    )
    with patch("vuzol.ops.backup.capture.iter_dump_stdout") as mock_iter:
        mock_iter.return_value = iter([b"chunk"])
        stream = runner._open_dump_stream(["python3", "-c", "pass"], identity)
        assert list(stream) == [b"chunk"]
        mock_iter.assert_called_once()
        kwargs = mock_iter.call_args.kwargs
        assert kwargs.get("env") == {"PGPASSWORD": "pw"}  # pragma: allowlist secret


def test_cli_capture_dry_run_and_gc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from vuzol.cli import backup as backup_cli

    settings = _base_settings(tmp_path)
    runtime = MagicMock()
    runtime.settings = settings

    monkeypatch.setattr(backup_cli, "get_runtime_configuration", lambda **_k: runtime)
    monkeypatch.setattr(backup_cli, "configure_logging", lambda **_k: None)
    monkeypatch.setattr(backup_cli, "get_logger", lambda _n: MagicMock())

    # dry-run without DSN resolver success still works if runner gets None dsn → preflight
    with patch.object(backup_cli, "_resolve_backup_dsn", side_effect=RuntimeError("no dsn")):
        code = asyncio.run(backup_cli._run(backup_cli._parse_args(["capture", "--json"])))
    assert code == 1
    out = capsys.readouterr().out
    assert "preflight_dsn" in out or "preflight" in out

    # gc-staging (safe isolated staging from _base_settings)
    staging = settings.backup.staging_root
    assert staging is not None
    rid = uuid.uuid4()
    rd, _, _ = ensure_staging_tree(staging, rid)
    write_state(rd, STATE_FAILED)
    code = asyncio.run(backup_cli._run(backup_cli._parse_args(["gc-staging", "--json"])))
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["schedule"] == "disabled"
    assert payload["removed"] >= 1
    assert not rd.exists()

    # gc without staging
    runtime.settings = Settings(
        environment="test",
        repository_root=settings.repository_root,
        worktree_root=settings.worktree_root,
        artifact_root=settings.artifact_root,
        secret_file_root=settings.secret_file_root,
        backup=BackupSettings(staging_root=None),
    )
    code = asyncio.run(backup_cli._run(backup_cli._parse_args(["gc-staging", "--json"])))
    assert code == 1
    assert "preflight_staging" in capsys.readouterr().out

    # gc refuses production-nested staging with stable code and no path leak / no delete
    bad_staging = settings.artifact_root / "evil-staging"
    bad_staging.mkdir(parents=True, exist_ok=True)
    bad_run, _, _ = ensure_staging_tree(bad_staging, uuid.uuid4())
    write_state(bad_run, STATE_FAILED)
    marker = bad_run / "untouched"
    marker.write_text("alive", encoding="utf-8")
    runtime.settings = Settings(
        environment="test",
        repository_root=settings.repository_root,
        worktree_root=settings.worktree_root,
        artifact_root=settings.artifact_root,
        secret_file_root=settings.secret_file_root,
        backup=BackupSettings(staging_root=bad_staging),
    )
    code = asyncio.run(backup_cli._run(backup_cli._parse_args(["gc-staging", "--json"])))
    assert code == 1
    refuse_out = capsys.readouterr().out
    refuse_payload = json.loads(refuse_out)
    assert refuse_payload["ok"] is False
    assert refuse_payload["code"] == "preflight_path_conflict"
    assert refuse_payload["schedule"] == "disabled"
    # No absolute production/staging paths leaked in operational JSON.
    assert str(settings.artifact_root) not in refuse_out
    assert str(bad_staging) not in refuse_out
    assert marker.is_file()


def test_cli_resolve_helpers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from vuzol.cli import backup as backup_cli

    kek = bytes(range(32))
    secrets = tmp_path / "sec"
    secrets.mkdir()
    (secrets / "backup_kek").write_bytes(kek)
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "r",
        worktree_root=tmp_path / "w",
        artifact_root=tmp_path / "a",
        secret_file_root=secrets,
        database_dsn_reference="env:VUZOL_DATABASE_DSN",
        backup=BackupSettings(
            staging_root=tmp_path / "st",
            kek_reference="file:backup_kek",
        ),
    )
    for path in (
        settings.repository_root,
        settings.worktree_root,
        settings.artifact_root,
        tmp_path / "st",
    ):
        path.mkdir(parents=True, exist_ok=True)

    class _Secret:
        def get_secret_value(self) -> str:
            return "postgresql://vuzol:x@127.0.0.1/vuzol"

    with patch("vuzol.cli.backup.ScopedSecretResolver") as resolver_cls:
        resolver_cls.return_value.get.return_value = _Secret()
        dsn = backup_cli._resolve_backup_dsn(settings)
        assert dsn.startswith("postgresql://")

    assert backup_cli._resolve_kek(settings) == kek
    monkeypatch.setenv("VUZOL_TEST_KEK_HEX", kek.hex())
    env_settings = Settings(
        environment="test",
        repository_root=settings.repository_root,
        worktree_root=settings.worktree_root,
        artifact_root=settings.artifact_root,
        secret_file_root=secrets,
        backup=BackupSettings(
            staging_root=tmp_path / "st",
            kek_reference="env:VUZOL_TEST_KEK_HEX",
        ),
    )
    assert backup_cli._resolve_kek(env_settings) == kek
