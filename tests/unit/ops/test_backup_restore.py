"""Unit tests for B3.0 published-package preflight (no KEK/decrypt/DSN/CLI)."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import vuzol.ops.backup.restore as restore_module
from vuzol.ops.backup.manifest import (
    SCHEMA_VERSION,
    ArtifactReconciliation,
    BackupAppIdentity,
    BackupComponent,
    BackupConfigSnapshot,
    BackupManifest,
    BackupRetentionMeta,
    BackupRpoRto,
    BackupSchemaIdentity,
    store_manifest,
)
from vuzol.ops.backup.paths import ProductionRoots
from vuzol.ops.backup.restore import (
    CODE_BLOB,
    CODE_COMPONENT,
    CODE_MANIFEST_HASH,
    CODE_MANIFEST_INVALID,
    CODE_OK,
    CODE_PACKAGE,
    CODE_PARTIAL,
    CODE_PATH_CONFLICT,
    CODE_RUN_ID,
    CODE_SCHEMA_MISMATCH,
    CODE_UNSUPPORTED,
    DEFAULT_HASH_READ_SIZE,
    MANIFEST_MAX_BYTES,
    PackagePreflightReport,
    preflight_published_package,
)
from vuzol.ops.backup.staging import STATE_PUBLISHED, ensure_staging_tree, write_state


def _production(tmp_path: Path) -> ProductionRoots:
    roots = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
    )
    for root in roots.all_roots()[:4]:
        root.mkdir(parents=True, exist_ok=True)
    return roots


def _safe_staging(tmp_path: Path) -> Path:
    staging = tmp_path / "staging"
    staging.mkdir()
    return staging


def _partial_manifest(
    run_id: uuid.UUID,
    *,
    ciphertext: bytes,
    partial: bool = True,
    extra_components: bool = False,
    filename: str = "postgres.dump.enc",
    cipher: str = "aes-256-gcm",
    fmt: str = "pg_custom",
    expected_head: str = "a" * 64,
    observed_head: str = "a" * 64,
) -> BackupManifest:
    start = datetime(2026, 7, 19, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    digest = hashlib.sha256(ciphertext).hexdigest()
    components: dict[str, BackupComponent] = {
        "postgres": BackupComponent(
            filename=filename,
            sha256_ciphertext=digest,
            size_ciphertext=len(ciphertext),
            cipher=cipher,
            format=fmt,
        )
    }
    if extra_components:
        components["artifacts"] = BackupComponent(
            filename="artifacts.tar.enc",
            sha256_ciphertext="b" * 64,
            size_ciphertext=1,
            object_count=0,
            inventory_sha256="c" * 64,
        )
    return BackupManifest.model_validate(
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "created_at": end,
            "t_start": start,
            "t_end": end,
            "hostname": "lab-host",
            "app": BackupAppIdentity(
                git_commit="0" * 40,
                deploy_path="/opt/vuzol",
                service_name="vuzol",
            ),
            "schema_identity": BackupSchemaIdentity(
                alembic_head_expected=expected_head,
                alembic_head_observed=observed_head,
            ),
            "config": BackupConfigSnapshot(registry_revision="0" * 64, files=()),
            "components": components,
            "artifact_reconciliation": ArtifactReconciliation(
                db_rows=0,
                fs_objects=0,
                missing_blobs=(),
                orphan_files=(),
                skipped_symlinks=0,
            ),
            "retention": BackupRetentionMeta(keep_local_runs=3, keep_offhost_days=28),
            "rpo_rto": BackupRpoRto(rpo_seconds_target=86_400, rto_seconds_target=7_200),
            "partial": partial,
        }
    )


def _write_published_package(
    staging: Path,
    run_id: uuid.UUID,
    *,
    ciphertext: bytes = b"fake-ciphertext-bytes",
    partial: bool = True,
    extra_components: bool = False,
    corrupt_sidecar: bool = False,
    wrong_blob_bytes: bytes | None = None,
    symlink_blob: bool = False,
    state: str = STATE_PUBLISHED,
    directory_run_id: uuid.UUID | None = None,
    cipher: str = "aes-256-gcm",
    fmt: str = "pg_custom",
    expected_head: str = "a" * 64,
    observed_head: str = "a" * 64,
) -> Path:
    dir_id = directory_run_id or run_id
    run_dir, tmp, publish = ensure_staging_tree(staging, dir_id)
    manifest = _partial_manifest(
        run_id,
        ciphertext=ciphertext,
        partial=partial,
        extra_components=extra_components,
        cipher=cipher,
        fmt=fmt,
        expected_head=expected_head,
        observed_head=observed_head,
    )
    manifest_path = tmp / "manifest.v1.json"
    digest = store_manifest(manifest_path, manifest)
    sha_path = tmp / "manifest.sha256"
    sha_path.write_text(("0" * 64 if corrupt_sidecar else digest) + "\n", encoding="utf-8")
    blob_path = tmp / "postgres.dump.enc"
    blob_path.write_bytes(wrong_blob_bytes if wrong_blob_bytes is not None else ciphertext)
    wrap_path = tmp / "dek.wrap"
    wrap_path.write_bytes(b"x" * 86)
    # Move into publish like B2 publish_run (simple rename for tests).
    for name in ("manifest.v1.json", "manifest.sha256", "postgres.dump.enc", "dek.wrap"):
        (tmp / name).replace(publish / name)
    if symlink_blob:
        target = publish / "postgres.dump.enc"
        real = publish / "postgres.dump.enc.real"
        target.replace(real)
        target.symlink_to(real)
    write_state(run_dir, state)
    return run_dir


def test_preflight_ok_safe_report_has_no_paths(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    ciphertext = b"stream-me-" + b"\x00" * 100
    _write_published_package(staging, run_id, ciphertext=ciphertext)

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
        hash_read_size=16,
    )
    assert report.ok is True
    assert report.code == CODE_OK
    assert report.run_id == str(run_id)
    assert report.partial is True
    assert report.size_ciphertext == len(ciphertext)
    assert report.sha256_ciphertext == hashlib.sha256(ciphertext).hexdigest()
    payload = report.to_operational_payload()
    assert payload["ok"] is True
    assert payload["schedule"] == "disabled"
    text = str(payload)
    assert str(staging) not in text
    assert str(tmp_path) not in text


def test_preflight_refuses_migration_head_mismatch(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(
        staging,
        run_id,
        expected_head="a" * 64,
        observed_head="b" * 64,
    )

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
    )

    assert report.ok is False
    assert report.code == CODE_SCHEMA_MISMATCH
    assert report.run_id is None


def test_preflight_refuses_production_nested_staging(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = production.artifact_root / "nested-staging"
    staging.mkdir()
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PATH_CONFLICT
    assert str(staging) not in report.message
    assert str(production.artifact_root) not in report.message


def test_preflight_refuses_unpublished_state(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id, state="dumping")

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE


def test_preflight_refuses_symlink_blob(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id, symlink_blob=True)

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE


def test_preflight_manifest_hash_mismatch(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id, corrupt_sidecar=True)

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_MANIFEST_HASH


def test_preflight_run_id_directory_bind(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    manifest_id = uuid.uuid4()
    dir_id = uuid.uuid4()
    _write_published_package(staging, manifest_id, directory_run_id=dir_id)

    report = preflight_published_package(staging_root=staging, run_id=dir_id, production=production)
    assert report.ok is False
    assert report.code == CODE_RUN_ID


def test_preflight_requires_partial_postgres_only(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id, partial=False)

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PARTIAL

    run2 = uuid.uuid4()
    _write_published_package(staging, run2, extra_components=True)
    report2 = preflight_published_package(staging_root=staging, run_id=run2, production=production)
    assert report2.ok is False
    assert report2.code == CODE_UNSUPPORTED


def test_preflight_component_labels(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id, fmt="plain")

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_COMPONENT


def test_preflight_blob_size_and_hash(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    good = b"good-bytes"
    _write_published_package(staging, run_id, ciphertext=good, wrong_blob_bytes=b"tampered!")

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_BLOB


def test_preflight_missing_run(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    report = preflight_published_package(
        staging_root=staging, run_id=uuid.uuid4(), production=production
    )
    assert report.ok is False
    assert report.code == CODE_PACKAGE


def test_report_payload_types() -> None:
    report = PackagePreflightReport(
        ok=False,
        code=CODE_PACKAGE,
        message="run is not published",
    )
    payload = report.to_operational_payload()
    assert payload["ok"] is False
    assert payload["run_id"] is None
    assert "message" in payload


def test_preflight_refuses_publish_dir_symlink(tmp_path: Path) -> None:
    """C1/C3: publish/ as symlink is refused before child reads."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    run_dir = _write_published_package(staging, run_id)
    publish = run_dir / "publish"
    real = run_dir / "publish-real"
    publish.rename(real)
    publish.symlink_to(real)

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "symlink" in report.message
    assert str(staging) not in report.message
    assert str(publish) not in report.message


def test_preflight_missing_required_publish_file(tmp_path: Path) -> None:
    """C3: missing required regular publish file → preflight_package."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    run_dir = _write_published_package(staging, run_id)
    (run_dir / "publish" / "dek.wrap").unlink()

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "required publish file" in report.message
    assert str(run_dir) not in report.message


def test_preflight_invalid_run_id_string(tmp_path: Path) -> None:
    """C3: non-UUID run_id fails closed as package error without path leak."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    report = preflight_published_package(
        staging_root=staging,
        run_id="not-a-uuid",
        production=production,
    )
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "UUID" in report.message
    assert str(staging) not in report.message


def test_preflight_nonpositive_hash_read_size(tmp_path: Path) -> None:
    """C3: hash_read_size < 1 rejected before package walk."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)

    for bad in (0, -1):
        report = preflight_published_package(
            staging_root=staging,
            run_id=run_id,
            production=production,
            hash_read_size=bad,
        )
        assert report.ok is False
        assert report.code == CODE_PACKAGE
        assert "hash read size" in report.message


@pytest.mark.parametrize("bad", [1.5, "64", True])
def test_preflight_rejects_noninteger_hash_read_size(
    tmp_path: Path,
    bad: object,
) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
        hash_read_size=bad,  # type: ignore[arg-type]
    )
    assert report.code == CODE_PACKAGE
    assert report.message == "invalid hash read size"


def test_preflight_refuses_escaping_runs_symlink(tmp_path: Path) -> None:
    """C3: runs/{id} symlink resolving outside staging → path conflict, no leak."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    # Build a complete package outside staging, then point runs/{id} at it.
    outside = tmp_path / "outside-escape"
    outside.mkdir()
    _write_published_package(outside, run_id)
    outside_run = outside / "runs" / str(run_id)

    runs_root = staging / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    link = runs_root / str(run_id)
    link.symlink_to(outside_run)

    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PATH_CONFLICT
    assert str(outside) not in report.message
    assert str(outside_run) not in report.message
    assert str(staging) not in report.message
    # Outside package must remain untouched (no delete/mutation).
    assert (outside_run / "publish" / "postgres.dump.enc").is_file()


def test_preflight_rejects_oversized_hash_read_size(tmp_path: Path) -> None:
    """hash_read_size above DEFAULT/MAX is rejected without path leak."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
        hash_read_size=DEFAULT_HASH_READ_SIZE + 1,
    )
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "hash read size" in report.message
    assert str(staging) not in report.message


def test_preflight_state_symlink_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    run_dir = staging / "runs" / str(run_id)
    state_path = run_dir / "STATE"
    target = run_dir / "STATE.target"
    target.write_text("published\n", encoding="utf-8")
    state_path.unlink()
    state_path.symlink_to(target)
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "symlink" in report.message
    assert str(staging) not in report.message
    assert str(state_path) not in report.message


def test_preflight_state_nonregular_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    run_dir = staging / "runs" / str(run_id)
    state_path = run_dir / "STATE"
    state_path.unlink()
    state_path.mkdir()
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "regular" in report.message
    assert str(state_path) not in report.message


def test_preflight_state_oversize_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    run_dir = staging / "runs" / str(run_id)
    (run_dir / "STATE").write_text("x" * 200, encoding="utf-8")
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "size" in report.message
    assert str(staging) not in report.message


def test_preflight_state_invalid_utf8_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    run_dir = staging / "runs" / str(run_id)
    (run_dir / "STATE").write_bytes(b"published\xff\xfe\n")
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "encoding" in report.message
    assert "\xff" not in report.message
    assert str(staging) not in report.message


def test_preflight_state_missing_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    (staging / "runs" / str(run_id) / "STATE").unlink()
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_PACKAGE
    assert "STATE" in report.message
    assert str(staging) not in report.message


def test_preflight_sidecar_invalid_utf8_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    sha = staging / "runs" / str(run_id) / "publish" / "manifest.sha256"
    sha.write_bytes(b"\xff" * 64)
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_MANIFEST_HASH
    assert "sidecar" in report.message
    assert "\xff" not in report.message
    assert str(sha) not in report.message


def test_preflight_sidecar_oversize_refused(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    sha = staging / "runs" / str(run_id) / "publish" / "manifest.sha256"
    sha.write_text(("a" * 64) + ("b" * 200), encoding="utf-8")
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_MANIFEST_HASH
    assert str(staging) not in report.message


def test_preflight_manifest_oversize_refused_without_full_load(tmp_path: Path) -> None:
    """st_size > 2 MiB fails as manifest_invalid before unbounded allocation."""

    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    manifest_path = staging / "runs" / str(run_id) / "publish" / "manifest.v1.json"
    # Oversized regular file: sparse seek write avoids multi-MiB RAM in the test process.
    with manifest_path.open("wb") as handle:
        handle.seek(MANIFEST_MAX_BYTES)
        handle.write(b"x")
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_MANIFEST_INVALID
    assert "size bound" in report.message
    assert str(manifest_path) not in report.message
    assert str(staging) not in report.message


def test_preflight_manifest_invalid_utf8_taxonomy(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)
    manifest_path = staging / "runs" / str(run_id) / "publish" / "manifest.v1.json"
    manifest_path.write_bytes(b'{"schema_version": "\xff"}')
    # Sidecar still present but content hash will not matter if UTF-8 fails first;
    # keep sidecar so order is load-then-hash; invalid UTF-8 fails at load.
    report = preflight_published_package(staging_root=staging, run_id=run_id, production=production)
    assert report.ok is False
    assert report.code == CODE_MANIFEST_INVALID
    assert "UTF-8" in report.message or "validation" in report.message
    assert "\xff" not in report.message
    assert str(manifest_path) not in report.message


def test_preflight_refuses_missing_and_non_directory_publish(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)

    missing_id = uuid.uuid4()
    missing_run = _write_published_package(staging, missing_id)
    publish = missing_run / "publish"
    for child in publish.iterdir():
        child.unlink()
    publish.rmdir()
    missing = preflight_published_package(
        staging_root=staging,
        run_id=missing_id,
        production=production,
    )
    assert missing.code == CODE_PACKAGE
    assert "publish directory" in missing.message

    file_id = uuid.uuid4()
    file_run = _write_published_package(staging, file_id)
    publish = file_run / "publish"
    for child in publish.iterdir():
        child.unlink()
    publish.rmdir()
    publish.write_bytes(b"not-a-directory")
    non_directory = preflight_published_package(
        staging_root=staging,
        run_id=file_id,
        production=production,
    )
    assert non_directory.code == CODE_PACKAGE
    assert "publish directory" in non_directory.message


def test_preflight_refuses_nonregular_required_file(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    run_dir = _write_published_package(staging, run_id)
    wrap = run_dir / "publish" / "dek.wrap"
    wrap.unlink()
    wrap.mkdir()

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
    )
    assert report.code == CODE_PACKAGE
    assert "regular file" in report.message


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"{", "UTF-8 JSON"),
        (b"[]", "validation"),
        (b"{}", "validation"),
    ],
)
def test_preflight_rejects_invalid_manifest_shapes(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    run_dir = _write_published_package(staging, run_id)
    (run_dir / "publish" / "manifest.v1.json").write_bytes(payload)

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
    )
    assert report.code == CODE_MANIFEST_INVALID
    assert message in report.message


@pytest.mark.parametrize(
    ("filename", "cipher"),
    [
        ("wrong.dump.enc", "aes-256-gcm"),
        ("postgres.dump.enc", "unsupported"),
    ],
)
def test_preflight_rejects_component_filename_and_cipher(
    tmp_path: Path,
    filename: str,
    cipher: str,
) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    run_dir, tmp, publish = ensure_staging_tree(staging, run_id)
    ciphertext = b"ciphertext"
    manifest = _partial_manifest(
        run_id,
        ciphertext=ciphertext,
        filename=filename,
        cipher=cipher,
    )
    digest = store_manifest(tmp / "manifest.v1.json", manifest)
    (tmp / "manifest.sha256").write_text(digest + "\n", encoding="utf-8")
    (tmp / "postgres.dump.enc").write_bytes(ciphertext)
    (tmp / "dek.wrap").write_bytes(b"x" * 86)
    for name in ("manifest.v1.json", "manifest.sha256", "postgres.dump.enc", "dek.wrap"):
        (tmp / name).replace(publish / name)
    write_state(run_dir, STATE_PUBLISHED)

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
    )
    assert report.code == CODE_COMPONENT


def test_preflight_detects_same_size_blob_hash_mismatch(tmp_path: Path) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(
        staging,
        run_id,
        ciphertext=b"expected",
        wrong_blob_bytes=b"tampered",
    )

    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
    )
    assert report.code == CODE_BLOB
    assert "hash mismatch" in report.message


def test_preflight_maps_ciphertext_read_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _production(tmp_path)
    staging = _safe_staging(tmp_path)
    run_id = uuid.uuid4()
    _write_published_package(staging, run_id)

    def fail_hash(path: Path, *, read_size: int) -> tuple[str, int]:
        del path, read_size
        raise OSError("sensitive filesystem detail")

    monkeypatch.setattr(restore_module, "_stream_sha256", fail_hash)
    report = preflight_published_package(
        staging_root=staging,
        run_id=run_id,
        production=production,
    )
    assert report.code == CODE_BLOB
    assert report.message == "ciphertext unreadable"


def test_sidecar_helper_refuses_missing_symlink_and_nonregular(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest"
    manifest.write_bytes(b"{}")
    missing = tmp_path / "missing"
    with pytest.raises(restore_module.BackupRestorePreflightError) as missing_error:
        restore_module._check_manifest_sidecar_hash(manifest, missing)
    assert missing_error.value.code == CODE_PACKAGE

    target = tmp_path / "target"
    target.write_text("0" * 64, encoding="utf-8")
    symlink = tmp_path / "symlink"
    symlink.symlink_to(target)
    with pytest.raises(restore_module.BackupRestorePreflightError) as symlink_error:
        restore_module._check_manifest_sidecar_hash(manifest, symlink)
    assert symlink_error.value.code == CODE_PACKAGE

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(restore_module.BackupRestorePreflightError) as directory_error:
        restore_module._check_manifest_sidecar_hash(manifest, directory)
    assert directory_error.value.code == CODE_PACKAGE


def test_manifest_helper_refuses_missing_symlink_and_nonregular(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(restore_module.BackupRestorePreflightError) as missing_error:
        restore_module._load_manifest_bounded(missing)
    assert missing_error.value.code == CODE_PACKAGE

    target = tmp_path / "target"
    target.write_text("{}", encoding="utf-8")
    symlink = tmp_path / "symlink"
    symlink.symlink_to(target)
    with pytest.raises(restore_module.BackupRestorePreflightError) as symlink_error:
        restore_module._load_manifest_bounded(symlink)
    assert symlink_error.value.code == CODE_PACKAGE

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(restore_module.BackupRestorePreflightError) as directory_error:
        restore_module._load_manifest_bounded(directory)
    assert directory_error.value.code == CODE_PACKAGE


def test_sidecar_helper_rejects_nonhex_digest(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest"
    manifest.write_bytes(b"{}")
    sidecar = tmp_path / "sidecar"
    sidecar.write_text("z" * 64, encoding="utf-8")

    with pytest.raises(restore_module.BackupRestorePreflightError) as error:
        restore_module._check_manifest_sidecar_hash(manifest, sidecar)
    assert error.value.code == CODE_MANIFEST_HASH


def test_bounded_reader_rejects_invalid_and_exceeded_bounds(tmp_path: Path) -> None:
    path = tmp_path / "input"
    path.write_bytes(b"ab")

    with pytest.raises(restore_module.BackupRestorePreflightError) as invalid:
        restore_module._read_file_bounded(path, max_bytes=-1, read_size=1)
    assert invalid.value.code == CODE_PACKAGE

    with pytest.raises(restore_module.BackupRestorePreflightError) as exceeded:
        restore_module._read_file_bounded(path, max_bytes=1, read_size=1)
    assert exceeded.value.code == CODE_PACKAGE
    assert "exceeds" in str(exceeded.value)


def test_internal_io_errors_are_mapped_to_bounded_taxonomy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "run"
    state_dir.mkdir()
    (state_dir / "STATE").write_text(STATE_PUBLISHED, encoding="utf-8")

    def fail_read(path: Path, *, max_bytes: int, read_size: int) -> bytes:
        del path, max_bytes, read_size
        raise OSError("sensitive filesystem detail")

    monkeypatch.setattr(restore_module, "_read_file_bounded", fail_read)
    with pytest.raises(restore_module.BackupRestorePreflightError) as state_error:
        restore_module._read_state_bound(state_dir)
    assert str(state_error.value) == "STATE unreadable"

    manifest = tmp_path / "manifest"
    manifest.write_bytes(b"{}")
    with pytest.raises(restore_module.BackupRestorePreflightError) as manifest_error:
        restore_module._load_manifest_bounded(manifest)
    assert str(manifest_error.value) == "manifest unreadable"

    sidecar = tmp_path / "sidecar"
    sidecar.write_text("0" * 64, encoding="utf-8")
    with pytest.raises(restore_module.BackupRestorePreflightError) as sidecar_error:
        restore_module._check_manifest_sidecar_hash(manifest, sidecar)
    assert str(sidecar_error.value) == "manifest hash sidecar unreadable"
