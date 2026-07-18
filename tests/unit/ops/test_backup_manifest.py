"""Boundary tests for backup-manifest.v1 pure validation and store/load."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.ops.backup.manifest import (
    SCHEMA_VERSION,
    ArtifactReconciliation,
    BackupAppIdentity,
    BackupComponent,
    BackupConfigSnapshot,
    BackupManifest,
    BackupManifestError,
    BackupRetentionMeta,
    BackupRpoRto,
    BackupSchemaIdentity,
    ConfigFileHash,
    MissingBlobRecord,
    OrphanFileRecord,
    canonical_manifest_json,
    load_manifest,
    manifest_sha256,
    store_manifest,
    validate_manifest,
)


def _sha(label: str = "a") -> str:
    return (label * 64)[:64]


def _manifest(**changes: object) -> BackupManifest:
    start = datetime(2026, 7, 18, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(minutes=5)
    values: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "created_at": end,
        "t_start": start,
        "t_end": end,
        "hostname": "vps-example",
        "app": BackupAppIdentity(
            git_commit="c8fd90295c7c63268e65755079ebaf97e74b6a04",  # pragma: allowlist secret
            deploy_path="/opt/vuzol",
            service_name="vuzol",
        ),
        "schema_identity": BackupSchemaIdentity(
            alembic_head_expected="a8b1c2d3e4f5",  # pragma: allowlist secret
            alembic_head_observed="a8b1c2d3e4f5",  # pragma: allowlist secret
            postgres_version="16.13",
        ),
        "config": BackupConfigSnapshot(
            registry_revision=_sha("b"),
            files=(ConfigFileHash(name="executor-registries.toml", sha256=_sha("c"), size=100),),
            seccomp_sha256=_sha("d"),
        ),
        "components": {
            "postgres": BackupComponent(
                filename="postgres.dump.enc",
                sha256_ciphertext=_sha("e"),
                size_ciphertext=1_024,
                cipher="aes-256-gcm",
                format="pg_custom",
            ),
            "artifacts": BackupComponent(
                filename="artifacts.tar.enc",
                sha256_ciphertext=_sha("f"),
                size_ciphertext=2_048,
                object_count=3,
                bytes_logical=4_096,
                inventory_sha256=_sha("1"),
            ),
            "config": BackupComponent(
                filename="config.tar.enc",
                sha256_ciphertext=_sha("2"),
                size_ciphertext=512,
            ),
        },
        "artifact_reconciliation": ArtifactReconciliation(
            db_rows=3,
            fs_objects=3,
            missing_blobs=(),
            orphan_files=(),
            skipped_symlinks=0,
        ),
        "retention": BackupRetentionMeta(keep_local_runs=3, keep_offhost_days=28),
        "rpo_rto": BackupRpoRto(
            targets_proposed=True,
            rpo_seconds_target=86_400,
            rto_seconds_target=7_200,
        ),
    }
    values.update(changes)
    return BackupManifest.model_validate(values)


def test_round_trip_canonical_json_is_deterministic(tmp_path: Path) -> None:
    manifest = _manifest()
    first = canonical_manifest_json(manifest)
    second = canonical_manifest_json(manifest)
    assert first == second
    assert first.startswith(b"{")
    assert b" " not in first  # compact separators
    digest = manifest_sha256(manifest)
    assert len(digest) == 64
    path = tmp_path / "manifest.json"
    stored = store_manifest(path, manifest)
    assert stored == digest
    loaded = load_manifest(path)
    assert loaded.run_id == manifest.run_id
    observed = loaded.schema_identity.alembic_head_observed
    assert observed == "a8b1c2d3e4f5"  # pragma: allowlist secret
    dumped = json.loads(canonical_manifest_json(loaded))
    assert dumped["schema"]["alembic_head_observed"] == observed
    assert canonical_manifest_json(loaded) == first


def test_rejects_unknown_schema_version() -> None:
    raw = _manifest().model_dump(mode="json")
    raw["schema_version"] = "backup-manifest.v0"
    with pytest.raises(BackupManifestError, match="unsupported schema_version"):
        validate_manifest(raw)


def test_rejects_malformed_hashes() -> None:
    with pytest.raises(ValidationError):
        ConfigFileHash(name="a.toml", sha256="not-a-hash", size=1)
    with pytest.raises(ValidationError):
        BackupComponent(
            filename="x.enc",
            sha256_ciphertext="Z" * 64,
            size_ciphertext=1,
        )


def test_rejects_secret_like_fields_and_values() -> None:
    raw = _manifest().model_dump(mode="json")
    raw["credential_blob"] = "nope"
    with pytest.raises(BackupManifestError, match="secret-like field"):
        validate_manifest(raw)

    raw = _manifest().model_dump(mode="json")
    raw["artifact_reconciliation"]["orphan_files"] = [
        {"relpath": "ab/cd", "action": "token=supersecret-value"}  # pragma: allowlist secret
    ]
    with pytest.raises(BackupManifestError, match="secret-like value"):
        validate_manifest(raw)

    raw = _manifest().model_dump(mode="json")
    raw["artifact_reconciliation"]["orphan_files"] = [
        {
            "relpath": "ab/cd",
            "action": "included",
        }
    ]
    # DSN-shaped string in action text.
    raw["artifact_reconciliation"]["orphan_files"][0]["action"] = (
        "postgresql://user:x@127.0.0.1/db"  # pragma: allowlist secret
    )
    with pytest.raises(BackupManifestError, match="secret-like value"):
        validate_manifest(raw)


def test_f3_secret_value_scanner_covers_postgres_and_bearer_jwt() -> None:
    """postgres:// and Bearer/JWT material must be rejected in free-text fields (F3)."""

    for action in (
        "postgres://user:pass@127.0.0.1/db",  # pragma: allowlist secret
        "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature",  # pragma: allowlist secret
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abc123def456ghi789jkl",  # pragma: allowlist secret
    ):
        raw = _manifest().model_dump(mode="json")
        raw["artifact_reconciliation"]["orphan_files"] = [{"relpath": "ab/cd", "action": action}]
        with pytest.raises(BackupManifestError, match="secret-like value"):
            validate_manifest(raw)


def test_f5_secret_field_name_token_boundaries() -> None:
    """Token-boundary names: no-secret.toml allowed; password/api_key rejected (F5)."""

    # Allowed basenames (negation / scanner context — not credential fields).
    assert ConfigFileHash(name="no-secret.toml", sha256=_sha("c"), size=1).name == "no-secret.toml"
    assert (
        ConfigFileHash(name="secret-scan-report.toml", sha256=_sha("c"), size=1).name
        == "secret-scan-report.toml"
    )
    # Credential-like basenames still rejected.
    with pytest.raises((ValidationError, BackupManifestError)):
        ConfigFileHash(name="password.toml", sha256=_sha("c"), size=1)
    with pytest.raises((ValidationError, BackupManifestError)):
        ConfigFileHash(name="api_key.toml", sha256=_sha("c"), size=1)
    with pytest.raises((ValidationError, BackupManifestError)):
        ConfigFileHash(name="client-secret.env", sha256=_sha("c"), size=1)
    # Dict keys with dotted separators still fail closed.
    raw = _manifest().model_dump(mode="json")
    raw["api.key"] = "nope"
    with pytest.raises(BackupManifestError, match="secret-like field"):
        validate_manifest(raw)


def test_rejects_forbidden_secret_path_material() -> None:
    raw = _manifest().model_dump(mode="json")
    raw["app"]["deploy_path"] = "/run/secrets/foo"
    with pytest.raises(BackupManifestError, match="forbidden path"):
        validate_manifest(raw)


def test_rejects_impossible_time_order() -> None:
    start = datetime(2026, 7, 18, 12, 0, 0, tzinfo=UTC)
    with pytest.raises(ValidationError):
        _manifest(t_start=start, t_end=start - timedelta(seconds=1), created_at=start)


def test_rejects_inconsistent_component_metadata() -> None:
    with pytest.raises(ValidationError):
        _manifest(
            components={
                "postgres": BackupComponent(
                    filename="postgres.dump.enc",
                    sha256_ciphertext=_sha("e"),
                    size_ciphertext=1,
                    object_count=1,
                    inventory_sha256=_sha("1"),
                )
            }
        )
    with pytest.raises(ValidationError):
        _manifest(
            components={
                "artifacts": BackupComponent(
                    filename="artifacts.tar.enc",
                    sha256_ciphertext=_sha("f"),
                    size_ciphertext=1,
                    # missing object_count / inventory
                )
            }
        )


def test_rejects_unknown_component_keys() -> None:
    raw = _manifest().model_dump(mode="json")
    raw["components"]["secrets"] = {
        "filename": "secrets.enc",
        "sha256_ciphertext": _sha("9"),
        "size_ciphertext": 1,
    }
    with pytest.raises(BackupManifestError):
        validate_manifest(raw)


def test_rejects_absolute_orphan_relpath() -> None:
    with pytest.raises(ValidationError):
        ArtifactReconciliation(
            db_rows=0,
            fs_objects=1,
            orphan_files=(OrphanFileRecord(relpath="/etc/passwd", action="include"),),
        )


def test_store_requires_absolute_path(tmp_path: Path) -> None:
    with pytest.raises(BackupManifestError, match="absolute"):
        store_manifest(Path("relative.json"), _manifest())


def test_load_rejects_non_object_json(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("[1,2,3]\n")
    with pytest.raises(BackupManifestError, match="object"):
        load_manifest(path)


def test_canonical_json_sorts_keys() -> None:
    data = json.loads(canonical_manifest_json(_manifest()))
    assert list(data.keys()) == sorted(data.keys())


def test_rejects_unsafe_config_and_component_names() -> None:
    with pytest.raises(ValidationError):
        ConfigFileHash(name="../secrets.toml", sha256=_sha("c"), size=1)
    with pytest.raises(ValidationError):
        ConfigFileHash(name="bad name.toml", sha256=_sha("c"), size=1)
    with pytest.raises(ValidationError):
        BackupComponent(
            filename="dir/x.enc",
            sha256_ciphertext=_sha("e"),
            size_ciphertext=1,
        )
    with pytest.raises(ValidationError):
        BackupAppIdentity(
            git_commit="not-hex!!!",
            deploy_path="/opt/vuzol",
            service_name="vuzol",
        )
    with pytest.raises(ValidationError):
        BackupAppIdentity(
            git_commit="abcdef0",
            deploy_path="relative/opt",
            service_name="vuzol",
        )


def test_rejects_bad_storage_state_and_orphan_parent_escape() -> None:
    with pytest.raises(ValidationError):
        ArtifactReconciliation(
            db_rows=1,
            fs_objects=1,
            missing_blobs=(MissingBlobRecord(content_hash=_sha("a"), storage_state="weird"),),
        )
    with pytest.raises(ValidationError):
        OrphanFileRecord(relpath="a/../b", action="include")


def test_validate_manifest_rejects_non_mapping() -> None:
    with pytest.raises(BackupManifestError, match="mapping"):
        validate_manifest([])  # type: ignore[arg-type]


def test_load_rejects_huge_and_invalid_utf8(tmp_path: Path) -> None:
    huge = tmp_path / "huge.json"
    huge.write_bytes(b"{" + b"a" * 2_000_001 + b"}")
    with pytest.raises(BackupManifestError, match="size bound"):
        load_manifest(huge)
    bad = tmp_path / "bad.json"
    bad.write_bytes(b"\xff\xfe not json")
    with pytest.raises(BackupManifestError, match="UTF-8 JSON"):
        load_manifest(bad)
    missing = tmp_path / "missing.json"
    with pytest.raises(BackupManifestError, match="unreadable"):
        load_manifest(missing)


def test_postgres_requires_format() -> None:
    with pytest.raises(ValidationError):
        _manifest(
            components={
                "postgres": BackupComponent(
                    filename="postgres.dump.enc",
                    sha256_ciphertext=_sha("e"),
                    size_ciphertext=1,
                    format=None,
                )
            }
        )


def test_complete_manifest_requires_postgres_unless_partial() -> None:
    with pytest.raises(ValidationError, match="partial"):
        _manifest(
            components={
                "config": BackupComponent(
                    filename="config.tar.enc",
                    sha256_ciphertext=_sha("2"),
                    size_ciphertext=512,
                )
            }
        )
    partial = _manifest(
        partial=True,
        components={
            "config": BackupComponent(
                filename="config.tar.enc",
                sha256_ciphertext=_sha("2"),
                size_ciphertext=512,
            )
        },
    )
    assert partial.partial is True
    assert "postgres" not in partial.components


def test_rejects_naive_timestamps() -> None:
    naive = datetime(2026, 7, 18, 12, 0, 0)  # intentional naive for rejection
    with pytest.raises(ValidationError, match="timezone-aware"):
        _manifest(t_start=naive)


def test_forbidden_path_uses_boundary_not_substring() -> None:
    # Substring false positive must not fire for mirrored deploy paths.
    manifest = _manifest(
        app=BackupAppIdentity(
            git_commit="abcdef0",
            deploy_path="/opt/mirror/etc/vuzol/notreally",
            service_name="vuzol",
        )
    )
    assert manifest.app.deploy_path.endswith("notreally")
    raw = _manifest().model_dump(mode="json")
    raw["app"]["deploy_path"] = "/etc/vuzol/executor.env"
    with pytest.raises(BackupManifestError, match="forbidden path"):
        validate_manifest(raw)


def test_golden_canonical_json_vector() -> None:
    """Lock ISO-8601 / UUID encoding for stable content hashes."""

    manifest = _manifest()
    raw = canonical_manifest_json(manifest)
    text = raw.decode("utf-8")
    # Pydantic mode=json emits Z or +00:00; accept either but require UTC and fixed run_id.
    assert '"run_id":"12345678-1234-5678-1234-567812345678"' in text
    assert '"t_start":"2026-07-18T12:00:00' in text
    assert '"partial":false' in text
    assert '"schema_version":"backup-manifest.v1"' in text
    assert list(json.loads(raw).keys()) == sorted(json.loads(raw).keys())
    # Byte-stable across repeated dumps.
    assert canonical_manifest_json(manifest) == raw
    digest = manifest_sha256(manifest)
    assert len(digest) == 64
    assert digest == manifest_sha256(validate_manifest(json.loads(raw)))
