"""Typed ``backup-manifest.v1`` models with pure validation, load, and store.

Manifests are non-secret operational metadata. Secret-like field names/values,
absolute secret paths, unknown schema versions, and inconsistent component
metadata are rejected fail-closed.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

SCHEMA_VERSION = "backup-manifest.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HEX_COMMIT_RE = re.compile(r"^[0-9a-f]{7,64}$")
_SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

# Exact identifier tokens (after non-alnum split) that mark secret-bearing fields.
_SECRET_FIELD_TOKENS = frozenset(
    {
        "password",
        "secret",
        "token",
        "apikey",
        "credential",
        "credentials",
        "passwd",
        "pwd",
        "dsn",
        "authorization",
        "bearer",
    }
)
# Adjacent token pairs after separator normalization (api_key, private_key, …).
_SECRET_FIELD_PAIRS = frozenset(
    {
        ("api", "key"),
        ("private", "key"),
        ("access", "token"),
        ("refresh", "token"),
        ("client", "secret"),
        ("auth", "token"),
        ("signing", "key"),
    }
)
# Leading tokens that negate a following secret token (no_secret, not_token, …).
_SECRET_NEGATION_TOKENS = frozenset({"no", "non", "not", "without"})
# Allow "secret" when it clearly names a scanner/report artifact, not a credential.
_SECRET_CONTEXT_ALLOW = frozenset({"scan", "scanning", "scanner", "report", "policy", "audit"})
_FILE_EXT_TOKENS = frozenset(
    {"toml", "json", "yaml", "yml", "md", "txt", "env", "cfg", "ini", "pem", "conf"}
)

# Path prefixes that must not appear as string values (secret/config credential roots).
# Matched on path-segment boundaries, not raw substring (avoids /mirror/etc/vuzol/… FP).
_FORBIDDEN_PATH_PREFIXES = (
    "/run/secrets",
    "/etc/vuzol",
)

# Values that look like embedded secrets (fail-closed; not a complete secret detector).
_SECRET_VALUE_RE = re.compile(
    r"(?i)(?:^|[^\w])(?:password|secret|api[_-]?key|token)\s*[:=]\s*\S+"
    r"|sk-[A-Za-z0-9]{16,}"
    # postgres:// and postgresql:// and postgresql+driver://
    r"|postgres(?:ql)?(?:\+[a-z0-9_]+)?://[^\s]+"
    # Authorization Bearer tokens (including JWT-shaped)
    r"|Bearer\s+[A-Za-z0-9\-._~+/]+=*"
    # Compact JWT (three base64url segments)
    r"|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
)


class BackupManifestError(ValueError):
    """Manifest validation or I/O failed fail-closed."""


class FrozenBackupModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        serialize_by_alias=True,
    )


def _require_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware (UTC preferred)")
    return value.astimezone(UTC)


class BackupAppIdentity(FrozenBackupModel):
    git_commit: str = Field(min_length=7, max_length=64)
    deploy_path: str = Field(min_length=1, max_length=500)
    service_name: str = Field(min_length=1, max_length=100)

    @field_validator("git_commit")
    @classmethod
    def validate_commit(cls, value: str) -> str:
        lowered = value.strip().lower()
        if not _HEX_COMMIT_RE.fullmatch(lowered):
            raise ValueError("git_commit must be a hex revision")
        return lowered

    @field_validator("deploy_path")
    @classmethod
    def validate_deploy_path(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned.startswith("/"):
            raise ValueError("deploy_path must be absolute")
        _reject_secret_string(cleaned, field="deploy_path")
        return cleaned


class BackupSchemaIdentity(FrozenBackupModel):
    alembic_head_expected: str = Field(min_length=1, max_length=64)
    alembic_head_observed: str = Field(min_length=1, max_length=64)
    postgres_version: str | None = Field(default=None, max_length=100)

    @field_validator("alembic_head_expected", "alembic_head_observed")
    @classmethod
    def validate_alembic_id(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if not re.fullmatch(r"[0-9a-f]+", cleaned):
            raise ValueError("alembic head must be a hex revision id")
        return cleaned


class ConfigFileHash(FrozenBackupModel):
    name: str = Field(min_length=1, max_length=200)
    sha256: str = Field(min_length=64, max_length=64)
    size: int = Field(ge=0, le=100_000_000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if "/" in cleaned or "\\" in cleaned or cleaned.startswith("."):
            raise ValueError("config file name must be a basename without path")
        if not _SAFE_FILENAME_RE.fullmatch(cleaned):
            raise ValueError("config file name contains unsafe characters")
        _reject_secret_field_name(cleaned)
        return cleaned

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _require_sha256(value)


class BackupConfigSnapshot(FrozenBackupModel):
    registry_revision: str = Field(min_length=64, max_length=64)
    files: tuple[ConfigFileHash, ...] = Field(default=(), max_length=50)
    seccomp_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("registry_revision", "seccomp_sha256")
    @classmethod
    def validate_optional_hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_sha256(value)


class BackupComponent(FrozenBackupModel):
    filename: str = Field(min_length=1, max_length=200)
    sha256_ciphertext: str = Field(min_length=64, max_length=64)
    size_ciphertext: int = Field(ge=0, le=50_000_000_000)
    cipher: str = Field(default="aes-256-gcm", min_length=1, max_length=100)
    format: str | None = Field(default=None, max_length=100)
    object_count: int | None = Field(default=None, ge=0, le=10_000_000)
    bytes_logical: int | None = Field(default=None, ge=0, le=50_000_000_000)
    inventory_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("filename")
    @classmethod
    def validate_filename(cls, value: str) -> str:
        cleaned = value.strip()
        if "/" in cleaned or "\\" in cleaned:
            raise ValueError("component filename must not contain path separators")
        if not _SAFE_FILENAME_RE.fullmatch(cleaned):
            raise ValueError("component filename contains unsafe characters")
        return cleaned

    @field_validator("sha256_ciphertext", "inventory_sha256")
    @classmethod
    def validate_hashes(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_sha256(value)

    @field_validator("cipher", "format")
    @classmethod
    def validate_labels(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", cleaned):
            raise ValueError("component label is unsafe")
        _reject_secret_field_name(cleaned)
        return cleaned


class MissingBlobRecord(FrozenBackupModel):
    content_hash: str = Field(min_length=64, max_length=64)
    storage_state: str = Field(min_length=1, max_length=40)

    @field_validator("content_hash")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _require_sha256(value)

    @field_validator("storage_state")
    @classmethod
    def validate_state(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"available", "staging", "quarantined", "missing", "deleted"}:
            raise ValueError("unknown storage_state")
        return cleaned


class OrphanFileRecord(FrozenBackupModel):
    relpath: str = Field(min_length=1, max_length=500)
    action: str = Field(min_length=1, max_length=100)

    @field_validator("relpath")
    @classmethod
    def validate_relpath(cls, value: str) -> str:
        cleaned = value.strip().replace("\\", "/")
        if cleaned.startswith("/") or ".." in cleaned.split("/"):
            raise ValueError("orphan relpath must be relative without ..")
        _reject_secret_string(cleaned, field="relpath")
        return cleaned


class ArtifactReconciliation(FrozenBackupModel):
    db_rows: int = Field(ge=0, le=10_000_000)
    fs_objects: int = Field(ge=0, le=10_000_000)
    missing_blobs: tuple[MissingBlobRecord, ...] = Field(default=(), max_length=10_000)
    orphan_files: tuple[OrphanFileRecord, ...] = Field(default=(), max_length=10_000)
    skipped_symlinks: int = Field(default=0, ge=0, le=10_000_000)


class BackupRetentionMeta(FrozenBackupModel):
    keep_local_runs: int = Field(ge=1, le=100)
    keep_offhost_days: int = Field(ge=1, le=3_650)


class BackupQuiesceInfo(FrozenBackupModel):
    mode: Literal["fence", "manual", "none"] = "none"
    duration_seconds: int = Field(default=0, ge=0, le=3_600)


class BackupRpoRto(FrozenBackupModel):
    targets_proposed: bool = True
    rpo_seconds_target: int = Field(ge=60, le=604_800)
    rto_seconds_target: int = Field(ge=60, le=86_400)
    rpo_seconds_measured: int | None = Field(default=None, ge=0, le=604_800)
    rto_seconds_measured: int | None = Field(default=None, ge=0, le=86_400)


class BackupManifest(FrozenBackupModel):
    """Versioned backup run identity and non-secret operational metadata."""

    schema_version: Literal["backup-manifest.v1"] = "backup-manifest.v1"
    run_id: uuid.UUID
    created_at: datetime
    t_start: datetime
    t_end: datetime
    hostname: str = Field(min_length=1, max_length=253)
    app: BackupAppIdentity
    # JSON key remains "schema"; attribute avoids shadowing BaseModel.schema.
    schema_identity: BackupSchemaIdentity = Field(alias="schema")
    config: BackupConfigSnapshot
    components: dict[str, BackupComponent] = Field(min_length=1, max_length=20)
    artifact_reconciliation: ArtifactReconciliation
    retention: BackupRetentionMeta
    quiesce: BackupQuiesceInfo = Field(default_factory=BackupQuiesceInfo)
    rpo_rto: BackupRpoRto
    # When true, incomplete component sets (e.g. config-only) are allowed.
    partial: bool = False

    @field_validator("hostname")
    @classmethod
    def validate_hostname(cls, value: str) -> str:
        cleaned = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,252}", cleaned):
            raise ValueError("hostname is unsafe")
        return cleaned

    @field_validator("created_at", "t_start", "t_end")
    @classmethod
    def validate_aware_timestamps(cls, value: datetime) -> datetime:
        return _require_aware_utc(value)

    @field_validator("components")
    @classmethod
    def validate_component_keys(
        cls, value: dict[str, BackupComponent]
    ) -> dict[str, BackupComponent]:
        allowed = {"postgres", "artifacts", "config"}
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown component keys: {sorted(unknown)}")
        for key in value:
            _reject_secret_field_name(key)
        return value

    @model_validator(mode="after")
    def validate_time_order_and_consistency(self) -> BackupManifest:
        if self.t_end < self.t_start:
            raise ValueError("t_end must not precede t_start")
        if self.created_at < self.t_start:
            raise ValueError("created_at must not precede t_start")
        # Complete manifests must include postgres ciphertext metadata unless partial.
        if not self.partial and "postgres" not in self.components:
            raise ValueError(
                "complete manifest requires postgres component (set partial=true for subsets)"
            )
        # Component metadata consistency: artifact inventory fields only on artifacts.
        for name, component in self.components.items():
            if name == "artifacts":
                if component.object_count is None:
                    raise ValueError("artifacts component requires object_count")
                if component.inventory_sha256 is None:
                    raise ValueError("artifacts component requires inventory_sha256")
            else:
                if component.object_count is not None or component.inventory_sha256 is not None:
                    raise ValueError(f"{name} component must not carry artifact inventory fields")
            if name == "postgres" and component.format is None:
                raise ValueError("postgres component requires format")
        # Soft alignment: object_count must match db_rows or fs_objects unless partial.
        artifacts = self.components.get("artifacts")
        if (
            artifacts is not None
            and artifacts.object_count is not None
            and not self.partial
            and self.artifact_reconciliation.db_rows != artifacts.object_count
            and self.artifact_reconciliation.fs_objects != artifacts.object_count
        ):
            raise ValueError(
                "artifact object_count must align with artifact_reconciliation "
                "db_rows or fs_objects (or set partial=true)"
            )
        return self


def _require_sha256(value: str) -> str:
    cleaned = value.strip().lower()
    if not _SHA256_RE.fullmatch(cleaned):
        raise ValueError("hash must be a 64-character lowercase hex sha256")
    return cleaned


def _reject_secret_field_name(name: str) -> None:
    """Reject secret-like identifiers using token boundaries (not raw substrings).

    Allows basenames such as ``no-secret.toml`` (negation) and
    ``secret-scan-report.toml`` (scanner context). Still rejects ``password``,
    ``api_key``, ``client_secret``, and similar credential field names.
    """

    segments = [part for part in re.split(r"[^a-z0-9]+", name.lower()) if part]
    if segments and segments[-1] in _FILE_EXT_TOKENS:
        segments = segments[:-1]
    if not segments:
        return

    for index, segment in enumerate(segments):
        if index + 1 < len(segments) and (segment, segments[index + 1]) in _SECRET_FIELD_PAIRS:
            raise BackupManifestError(f"secret-like field name is forbidden: {name}")
        if segment not in _SECRET_FIELD_TOKENS:
            continue
        if index > 0 and segments[index - 1] in _SECRET_NEGATION_TOKENS:
            continue
        if (
            segment == "secret"
            and index + 1 < len(segments)
            and segments[index + 1] in _SECRET_CONTEXT_ALLOW
        ):
            continue
        raise BackupManifestError(f"secret-like field name is forbidden: {name}")


def _forbidden_path_material(value: str) -> bool:
    """True when value contains a forbidden absolute path prefix at a boundary."""

    for prefix in _FORBIDDEN_PATH_PREFIXES:
        idx = 0
        while True:
            found = value.find(prefix, idx)
            if found < 0:
                break
            # Require start or non-path char before prefix so
            # "/opt/mirror/etc/vuzol" is not a false positive for "/etc/vuzol".
            prev_ok = found == 0 or value[found - 1] not in (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
            )
            if prev_ok:
                end = found + len(prefix)
                if end == len(value) or value[end] in "/":
                    return True
            idx = found + 1
    return False


def _reject_secret_string(value: str, *, field: str) -> None:
    if _SECRET_VALUE_RE.search(value):
        raise BackupManifestError(f"secret-like value is forbidden in {field}")
    if _forbidden_path_material(value):
        raise BackupManifestError(f"forbidden path material in {field}")


def _walk_reject_secrets(obj: object, *, path: str = "$") -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if not isinstance(key, str):
                raise BackupManifestError("manifest keys must be strings")
            _reject_secret_field_name(key)
            _walk_reject_secrets(value, path=f"{path}.{key}")
    elif isinstance(obj, list | tuple):
        for index, item in enumerate(obj):
            _walk_reject_secrets(item, path=f"{path}[{index}]")
    elif isinstance(obj, str):
        _reject_secret_string(obj, field=path)


def validate_manifest(manifest: BackupManifest | dict[str, object]) -> BackupManifest:
    """Validate a model or raw mapping; reject unknown schema and secret material."""

    if isinstance(manifest, BackupManifest):
        payload = manifest.model_dump(mode="json")
    elif isinstance(manifest, dict):
        payload = manifest
    else:
        raise BackupManifestError("manifest must be a mapping or BackupManifest")

    version = payload.get("schema_version")
    if version != SCHEMA_VERSION:
        raise BackupManifestError(f"unsupported schema_version: {version!r}")

    _walk_reject_secrets(payload)
    try:
        return BackupManifest.model_validate(payload)
    except ValidationError as error:
        raise BackupManifestError(str(error)) from error


def canonical_manifest_json(manifest: BackupManifest) -> bytes:
    """Deterministic UTF-8 JSON (sorted keys, no insignificant whitespace variance)."""

    validated = validate_manifest(manifest)
    payload = validated.model_dump(mode="json")
    # Strict: no default=str — unexpected types must fail rather than silently
    # change the hash (F9).
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def manifest_sha256(manifest: BackupManifest) -> str:
    return hashlib.sha256(canonical_manifest_json(manifest)).hexdigest()


def store_manifest(path: Path, manifest: BackupManifest) -> str:
    """Write canonical JSON to ``path``; return the content sha256."""

    validated = validate_manifest(manifest)
    data = canonical_manifest_json(validated)
    digest = hashlib.sha256(data).hexdigest()
    path = Path(path)
    if not path.is_absolute():
        raise BackupManifestError("manifest store path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(data)
        temporary.chmod(0o600)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)
    return digest


def load_manifest(path: Path) -> BackupManifest:
    """Load and validate a manifest file."""

    path = Path(path)
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise BackupManifestError(f"manifest is unreadable: {error}") from error
    if len(raw) > 2_000_000:
        raise BackupManifestError("manifest exceeds size bound")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BackupManifestError("manifest is not valid UTF-8 JSON") from error
    if not isinstance(payload, dict):
        raise BackupManifestError("manifest root must be an object")
    return validate_manifest(payload)
