"""B3.0 published-package preflight (read-only; no KEK/decrypt/DSN/restore process).

Validates a local B2 published run under staging against ProductionRoots and the
strict package order: containment, STATE=published, regular publish files,
manifest + sidecar hash, partial postgres-only shape, run_id bind, ciphertext
size/hash via bounded streaming reads.
"""

from __future__ import annotations

import hashlib
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

from vuzol.ops.backup.manifest import (
    BackupManifest,
    BackupManifestError,
    load_manifest,
)
from vuzol.ops.backup.paths import BackupPathError, ProductionRoots, resolve_isolation_path
from vuzol.ops.backup.staging import STATE_PUBLISHED, assert_safe_staging_root, read_state

# Stable operational codes — never embed absolute paths in messages.
CODE_OK = "package_ok"
CODE_PATH_CONFLICT = "preflight_path_conflict"
CODE_PACKAGE = "preflight_package"
CODE_MANIFEST_INVALID = "manifest_invalid"
CODE_MANIFEST_HASH = "manifest_hash_mismatch"
CODE_COMPONENT = "manifest_component"
CODE_UNSUPPORTED = "restore_unsupported_components"
CODE_PARTIAL = "partial_not_accepted"
CODE_RUN_ID = "run_id_mismatch"
CODE_BLOB = "blob_integrity"

PUBLISH_MANIFEST = "manifest.v1.json"
PUBLISH_MANIFEST_SHA = "manifest.sha256"
PUBLISH_BLOB = "postgres.dump.enc"
PUBLISH_WRAP = "dek.wrap"
REQUIRED_PUBLISH_NAMES = (
    PUBLISH_MANIFEST,
    PUBLISH_MANIFEST_SHA,
    PUBLISH_BLOB,
    PUBLISH_WRAP,
)

_POSTGRES_FILENAME = "postgres.dump.enc"
_POSTGRES_CIPHER = "aes-256-gcm"
_POSTGRES_FORMAT = "pg_custom"

# Bounded stream read size for integrity hashing (never load whole blob).
DEFAULT_HASH_READ_SIZE = 65_536


class BackupRestorePreflightError(ValueError):
    """Package preflight failed fail-closed."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True, slots=True)
class PackagePreflightReport:
    """Safe operational report: no absolute paths, DSN, or key material."""

    ok: bool
    code: str
    message: str
    run_id: str | None = None
    partial: bool | None = None
    size_ciphertext: int | None = None
    sha256_ciphertext: str | None = None

    def to_operational_payload(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "run_id": self.run_id,
            "partial": self.partial,
            "size_ciphertext": self.size_ciphertext,
            "sha256_ciphertext": self.sha256_ciphertext,
            "schedule": "disabled",
        }


def preflight_published_package(
    *,
    staging_root: Path,
    run_id: uuid.UUID | str,
    production: ProductionRoots,
    hash_read_size: int = DEFAULT_HASH_READ_SIZE,
) -> PackagePreflightReport:
    """Read-only preflight for a published B2 run under ``staging_root``.

    Does not load KEK, unwrap DEK, decrypt, open DSN, spawn restore, write
    evidence, or delete files. Failures never include absolute paths.
    """

    if hash_read_size < 1:
        return _fail(CODE_PACKAGE, "invalid hash read size")

    try:
        run_uuid = run_id if isinstance(run_id, uuid.UUID) else uuid.UUID(str(run_id))
    except (ValueError, TypeError, AttributeError):
        return _fail(CODE_PACKAGE, "run_id is not a valid UUID")

    try:
        staging = assert_safe_staging_root(staging_root, production)
    except BackupPathError:
        return _fail(CODE_PATH_CONFLICT, "staging root conflicts with production roots")
    except OSError:
        return _fail(CODE_PATH_CONFLICT, "staging root is not resolvable")

    run_dir = staging / "runs" / str(run_uuid)
    try:
        resolved_run = resolve_isolation_path(run_dir)
        resolved_run.relative_to(staging)
    except (BackupPathError, ValueError, OSError):
        return _fail(CODE_PATH_CONFLICT, "run directory escapes staging root")

    if not resolved_run.is_dir():
        return _fail(CODE_PACKAGE, "run directory missing")

    state = read_state(resolved_run)
    if state != STATE_PUBLISHED:
        return _fail(CODE_PACKAGE, "run is not published")

    # C1: bind publish/ under the resolved run before any child read; refuse
    # symlink or escape (resolve + relative_to after non-symlink lstat).
    try:
        publish = _bind_publish_dir(resolved_run)
    except BackupRestorePreflightError as error:
        return _fail(error.code, str(error) or error.code)

    try:
        paths = _require_regular_publish_files(publish)
    except BackupRestorePreflightError as error:
        return _fail(error.code, str(error) or error.code)

    # C2: I/O failures → preflight_package; schema/JSON → manifest_invalid.
    # load_manifest wraps OSError as BackupManifestError; inspect __cause__.
    try:
        manifest = load_manifest(paths[PUBLISH_MANIFEST])
    except BackupManifestError as error:
        if isinstance(error.__cause__, OSError):
            return _fail(CODE_PACKAGE, "manifest unreadable")
        return _fail(CODE_MANIFEST_INVALID, "manifest validation failed")
    except OSError:
        return _fail(CODE_PACKAGE, "manifest unreadable")

    try:
        _check_manifest_sidecar_hash(paths[PUBLISH_MANIFEST], paths[PUBLISH_MANIFEST_SHA])
    except BackupRestorePreflightError as error:
        return _fail(error.code, str(error) or error.code)

    if manifest.run_id != run_uuid:
        return _fail(CODE_RUN_ID, "manifest run_id does not match directory id")

    shape = _check_b3_postgres_shape(manifest)
    if shape is not None:
        return shape

    component = manifest.components["postgres"]
    try:
        digest, size = _stream_sha256(
            paths[PUBLISH_BLOB],
            read_size=hash_read_size,
        )
    except OSError:
        return _fail(CODE_BLOB, "ciphertext unreadable")

    if size != component.size_ciphertext:
        return _fail(CODE_BLOB, "ciphertext size mismatch")
    if digest != component.sha256_ciphertext:
        return _fail(CODE_BLOB, "ciphertext hash mismatch")

    return PackagePreflightReport(
        ok=True,
        code=CODE_OK,
        message="published postgres package preflight ok",
        run_id=str(run_uuid),
        partial=True,
        size_ciphertext=size,
        sha256_ciphertext=digest,
    )


def _fail(code: str, message: str) -> PackagePreflightReport:
    return PackagePreflightReport(ok=False, code=code, message=message)


def _bind_publish_dir(resolved_run: Path) -> Path:
    """Resolve and contain ``publish/`` under ``resolved_run`` before child reads.

    Refuses symlink publish directories and any resolve result that escapes the run.
    """

    publish = resolved_run / "publish"
    try:
        st = publish.lstat()
    except OSError as error:
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "publish directory missing or unsafe"
        ) from error
    if stat.S_ISLNK(st.st_mode):
        raise BackupRestorePreflightError(CODE_PACKAGE, "publish directory must not be a symlink")
    if not stat.S_ISDIR(st.st_mode):
        raise BackupRestorePreflightError(CODE_PACKAGE, "publish directory missing or unsafe")
    try:
        resolved_publish = resolve_isolation_path(publish)
        resolved_publish.relative_to(resolved_run)
    except (BackupPathError, ValueError, OSError) as error:
        raise BackupRestorePreflightError(
            CODE_PATH_CONFLICT, "publish directory escapes run root"
        ) from error
    if not resolved_publish.is_dir() or resolved_publish.is_symlink():
        raise BackupRestorePreflightError(CODE_PACKAGE, "publish directory missing or unsafe")
    return resolved_publish


def _require_regular_publish_files(publish: Path) -> dict[str, Path]:
    """Return map of required basenames to paths; refuse symlinks/non-regular."""

    found: dict[str, Path] = {}
    for name in REQUIRED_PUBLISH_NAMES:
        path = publish / name
        try:
            st = path.lstat()
        except OSError as error:
            raise BackupRestorePreflightError(
                CODE_PACKAGE, "required publish file missing"
            ) from error
        if stat.S_ISLNK(st.st_mode):
            raise BackupRestorePreflightError(CODE_PACKAGE, "publish file must not be a symlink")
        if not stat.S_ISREG(st.st_mode):
            raise BackupRestorePreflightError(CODE_PACKAGE, "publish file must be a regular file")
        found[name] = path
    return found


def _check_manifest_sidecar_hash(manifest_path: Path, sha_path: Path) -> None:
    """Compare on-disk manifest bytes to sidecar digest (do not re-canonicalize)."""

    try:
        expected = sha_path.read_text(encoding="utf-8").strip().lower()
    except OSError as error:
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "manifest hash sidecar unreadable"
        ) from error
    if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise BackupRestorePreflightError(CODE_MANIFEST_HASH, "manifest hash sidecar invalid")

    digest = hashlib.sha256()
    try:
        with manifest_path.open("rb") as handle:
            while True:
                block = handle.read(DEFAULT_HASH_READ_SIZE)
                if not block:
                    break
                digest.update(block)
    except OSError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest unreadable for hash") from error

    if digest.hexdigest() != expected:
        raise BackupRestorePreflightError(CODE_MANIFEST_HASH, "manifest content hash mismatch")


def _check_b3_postgres_shape(manifest: BackupManifest) -> PackagePreflightReport | None:
    if not manifest.partial:
        return _fail(CODE_PARTIAL, "B3 requires partial=true postgres-only package")
    keys = set(manifest.components)
    if keys != {"postgres"}:
        return _fail(CODE_UNSUPPORTED, "B3 supports only postgres component")
    component = manifest.components["postgres"]
    if component.filename != _POSTGRES_FILENAME:
        return _fail(CODE_COMPONENT, "postgres filename must be postgres.dump.enc")
    if component.cipher != _POSTGRES_CIPHER:
        return _fail(CODE_COMPONENT, "postgres cipher must be aes-256-gcm")
    if component.format != _POSTGRES_FORMAT:
        return _fail(CODE_COMPONENT, "postgres format must be pg_custom")
    return None


def _stream_sha256(path: Path, *, read_size: int) -> tuple[str, int]:
    """SHA-256 and byte length via bounded reads."""

    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            block = handle.read(read_size)
            if not block:
                break
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size
