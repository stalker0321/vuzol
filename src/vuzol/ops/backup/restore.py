"""B3.0 published-package preflight (read-only; no KEK/decrypt/DSN/restore process).

Validates a local B2 published run under staging against ProductionRoots and the
strict package order: containment, STATE=published, regular publish files,
manifest + sidecar hash, partial postgres-only shape, run_id bind, ciphertext
size/hash via bounded streaming reads.
"""

from __future__ import annotations

import hashlib
import json
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

from vuzol.ops.backup.manifest import (
    BackupManifest,
    BackupManifestError,
    validate_manifest,
)
from vuzol.ops.backup.paths import BackupPathError, ProductionRoots, resolve_isolation_path
from vuzol.ops.backup.staging import STATE_PUBLISHED, assert_safe_staging_root

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
CODE_SCHEMA_MISMATCH = "migration_head_mismatch"
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
# Callers may not request larger chunks than this default/max.
DEFAULT_HASH_READ_SIZE = 65_536
MAX_HASH_READ_SIZE = DEFAULT_HASH_READ_SIZE

# Align with manifest.load_manifest size bound without calling unbounded read_bytes.
MANIFEST_MAX_BYTES = 2_000_000
# STATE is a short token line (e.g. "published"); refuse oversized/non-UTF-8.
STATE_MAX_BYTES = 64
# Sidecar is 64 hex chars + optional newline/whitespace.
SIDECAR_MAX_BYTES = 128


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

    if (
        isinstance(hash_read_size, bool)
        or not isinstance(hash_read_size, int)
        or hash_read_size < 1
        or hash_read_size > MAX_HASH_READ_SIZE
    ):
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

    try:
        state = _read_state_bound(resolved_run)
    except BackupRestorePreflightError as error:
        return _fail(error.code, str(error) or error.code)
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

    # Bounded manifest load (no unbounded read_bytes); I/O vs schema taxonomy.
    try:
        manifest = _load_manifest_bounded(paths[PUBLISH_MANIFEST])
    except BackupRestorePreflightError as error:
        return _fail(error.code, str(error) or error.code)

    try:
        _check_manifest_sidecar_hash(paths[PUBLISH_MANIFEST], paths[PUBLISH_MANIFEST_SHA])
    except BackupRestorePreflightError as error:
        return _fail(error.code, str(error) or error.code)

    if manifest.run_id != run_uuid:
        return _fail(CODE_RUN_ID, "manifest run_id does not match directory id")

    if (
        manifest.schema_identity.alembic_head_expected
        != manifest.schema_identity.alembic_head_observed
    ):
        return _fail(CODE_SCHEMA_MISMATCH, "manifest migration heads differ")

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


def _read_state_bound(resolved_run: Path) -> str:
    """Read STATE as a regular non-symlink file with bounded positive reads.

    Does not use staging.read_state (which follows links / unbounded text read).
    """

    path = resolved_run / "STATE"
    try:
        st = path.lstat()
    except OSError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "STATE missing or unreadable") from error
    if stat.S_ISLNK(st.st_mode):
        raise BackupRestorePreflightError(CODE_PACKAGE, "STATE must not be a symlink")
    if not stat.S_ISREG(st.st_mode):
        raise BackupRestorePreflightError(CODE_PACKAGE, "STATE must be a regular file")
    if st.st_size > STATE_MAX_BYTES:
        raise BackupRestorePreflightError(CODE_PACKAGE, "STATE exceeds size bound")
    try:
        raw = _read_file_bounded(path, max_bytes=STATE_MAX_BYTES, read_size=STATE_MAX_BYTES)
    except BackupRestorePreflightError:
        raise
    except OSError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "STATE unreadable") from error
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "STATE encoding invalid") from error
    return text.strip()


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


def _load_manifest_bounded(manifest_path: Path) -> BackupManifest:
    """Load/validate manifest with pre-size check and bounded reads (no full unbounded load)."""

    try:
        st = manifest_path.lstat()
    except OSError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest unreadable") from error
    if stat.S_ISLNK(st.st_mode):
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest must not be a symlink")
    if not stat.S_ISREG(st.st_mode):
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest must be a regular file")
    if st.st_size > MANIFEST_MAX_BYTES:
        # Fail before allocating the whole file into memory.
        raise BackupRestorePreflightError(CODE_MANIFEST_INVALID, "manifest exceeds size bound")
    try:
        raw = _read_file_bounded(
            manifest_path,
            max_bytes=MANIFEST_MAX_BYTES,
            read_size=DEFAULT_HASH_READ_SIZE,
        )
    except BackupRestorePreflightError as error:
        if error.code == CODE_PACKAGE and "exceeds" in str(error):
            raise BackupRestorePreflightError(
                CODE_MANIFEST_INVALID, "manifest exceeds size bound"
            ) from error
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest unreadable") from error
    except OSError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest unreadable") from error
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BackupRestorePreflightError(
            CODE_MANIFEST_INVALID, "manifest is not valid UTF-8 JSON"
        ) from error
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as error:
        raise BackupRestorePreflightError(
            CODE_MANIFEST_INVALID, "manifest is not valid UTF-8 JSON"
        ) from error
    if not isinstance(payload, dict):
        raise BackupRestorePreflightError(CODE_MANIFEST_INVALID, "manifest validation failed")
    try:
        return validate_manifest(payload)
    except BackupManifestError as error:
        raise BackupRestorePreflightError(
            CODE_MANIFEST_INVALID, "manifest validation failed"
        ) from error


def _check_manifest_sidecar_hash(manifest_path: Path, sha_path: Path) -> None:
    """Compare on-disk manifest bytes to sidecar digest (do not re-canonicalize)."""

    try:
        st = sha_path.lstat()
    except OSError as error:
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "manifest hash sidecar unreadable"
        ) from error
    if stat.S_ISLNK(st.st_mode):
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "manifest hash sidecar must not be a symlink"
        )
    if not stat.S_ISREG(st.st_mode):
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "manifest hash sidecar must be a regular file"
        )
    if st.st_size > SIDECAR_MAX_BYTES:
        raise BackupRestorePreflightError(CODE_MANIFEST_HASH, "manifest hash sidecar invalid")
    try:
        raw = _read_file_bounded(
            sha_path,
            max_bytes=SIDECAR_MAX_BYTES,
            read_size=SIDECAR_MAX_BYTES,
        )
    except BackupRestorePreflightError as error:
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "manifest hash sidecar unreadable"
        ) from error
    except OSError as error:
        raise BackupRestorePreflightError(
            CODE_PACKAGE, "manifest hash sidecar unreadable"
        ) from error
    try:
        expected = raw.decode("utf-8").strip().lower()
    except UnicodeDecodeError as error:
        raise BackupRestorePreflightError(
            CODE_MANIFEST_HASH, "manifest hash sidecar invalid"
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
                if len(block) > DEFAULT_HASH_READ_SIZE:
                    raise BackupRestorePreflightError(CODE_PACKAGE, "manifest unreadable for hash")
                digest.update(block)
    except OSError as error:
        raise BackupRestorePreflightError(CODE_PACKAGE, "manifest unreadable for hash") from error

    if digest.hexdigest() != expected:
        raise BackupRestorePreflightError(CODE_MANIFEST_HASH, "manifest content hash mismatch")


def _read_file_bounded(path: Path, *, max_bytes: int, read_size: int) -> bytes:
    """Read at most ``max_bytes`` with positive bounded ``read`` calls only."""

    if max_bytes < 0 or read_size < 1:
        raise BackupRestorePreflightError(CODE_PACKAGE, "invalid read bound")
    parts = bytearray()
    with path.open("rb") as handle:
        while True:
            # Read one extra byte past max so oversize is detected without trusting st_size alone.
            room = max_bytes - len(parts) + 1
            if room <= 0:
                raise BackupRestorePreflightError(CODE_PACKAGE, "file exceeds size bound")
            to_read = min(read_size, room)
            chunk = handle.read(to_read)
            if not chunk:
                break
            if len(chunk) > to_read:
                raise BackupRestorePreflightError(CODE_PACKAGE, "oversized read")
            parts.extend(chunk)
            if len(parts) > max_bytes:
                raise BackupRestorePreflightError(CODE_PACKAGE, "file exceeds size bound")
    return bytes(parts)


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
            if len(block) > read_size:
                raise OSError("oversized read from ciphertext")
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size
