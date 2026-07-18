"""Backup foundation: typed manifests, pure validation, and restore path/DSN guards.

Slice B1 only — no crypto, capture, sink, dump, restore runtime, CLI, or systemd.
"""

from vuzol.ops.backup.manifest import (
    SCHEMA_VERSION,
    ArtifactReconciliation,
    BackupAppIdentity,
    BackupComponent,
    BackupConfigSnapshot,
    BackupManifest,
    BackupManifestError,
    BackupQuiesceInfo,
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
from vuzol.ops.backup.paths import (
    BackupPathError,
    ProductionRoots,
    assert_isolated_restore_dsn,
    assert_safe_restore_paths,
    database_name_is_isolated,
    normalize_dsn_identity,
    resolve_isolation_path,
)
from vuzol.ops.backup.settings import BackupSettings

__all__ = [
    "SCHEMA_VERSION",
    "ArtifactReconciliation",
    "BackupAppIdentity",
    "BackupComponent",
    "BackupConfigSnapshot",
    "BackupManifest",
    "BackupManifestError",
    "BackupPathError",
    "BackupQuiesceInfo",
    "BackupRetentionMeta",
    "BackupRpoRto",
    "BackupSchemaIdentity",
    "BackupSettings",
    "ConfigFileHash",
    "MissingBlobRecord",
    "OrphanFileRecord",
    "ProductionRoots",
    "assert_isolated_restore_dsn",
    "assert_safe_restore_paths",
    "canonical_manifest_json",
    "database_name_is_isolated",
    "load_manifest",
    "manifest_sha256",
    "normalize_dsn_identity",
    "resolve_isolation_path",
    "store_manifest",
    "validate_manifest",
]
