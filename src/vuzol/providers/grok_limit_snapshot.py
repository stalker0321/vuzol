"""Bounded Grok limit snapshot library (S1a code-dark).

Bindings map ``profile_id`` → account leaf under a profiles root. Export reads
only allowlisted log paths, derives a one-way principal digest, and publishes a
strict snapshot document. Load is by ``profile_id`` only.

This module is intentionally unwired from executor / subscription_limits.
No auth.json reads, no JWT subject binding, no network I/O.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final

# --- schema versions ---
BINDINGS_SCHEMA_VERSION: Final = "vuzol-grok-limit-bindings.v1"
SNAPSHOT_SCHEMA_VERSION: Final = "vuzol-grok-limit-snapshot.v1"

# --- bounds ---
MAX_BINDINGS_BYTES: Final = 256_000
MAX_SNAPSHOT_BYTES: Final = 1_000_000
MAX_LOG_SEGMENT_BYTES: Final = 4_000_000
MAX_LOG_LINE_BYTES: Final = 100_000
MAX_BINDINGS: Final = 64
MAX_ENTRIES: Final = 64
DEFAULT_MAX_AGE: Final = timedelta(minutes=15)

# --- fixed error codes (oracle) ---
CODE_EXPORT_BINDINGS_INVALID: Final = "limits_export_bindings_invalid"
CODE_EXPORT_PATH_REJECTED: Final = "limits_export_path_rejected"
CODE_EXPORT_OWNERSHIP_FAILED: Final = "limits_export_ownership_failed"
CODE_SNAPSHOT_UNREADABLE: Final = "limits_snapshot_unreadable"
CODE_SNAPSHOT_INVALID: Final = "limits_snapshot_invalid"
CODE_SNAPSHOT_STALE: Final = "limits_snapshot_stale"
CODE_SNAPSHOT_UNBOUND: Final = "limits_snapshot_unbound"
CODE_BINDING_MISMATCH: Final = "limits_binding_mismatch"

_ALLOWED_LOG_RELS: Final = (
    Path("logs") / "unified.jsonl",
    Path(".grok") / "logs" / "unified.jsonl",
)
_BINDINGS_TOP_KEYS: Final = frozenset({"schema_version", "profiles_root", "bindings"})
_BINDING_KEYS: Final = frozenset({"profile_id", "account_leaf", "expected_principal_digest"})
_SNAPSHOT_TOP_KEYS: Final = frozenset({"schema_version", "generated_at", "entries"})
_ENTRY_KEYS: Final = frozenset(
    {
        "profile_id",
        "principal_digest",
        "remaining_percent",
        "reset_at",
        "plan_label",
        "observed_at",
    }
)
_HEX64: Final = frozenset("0123456789abcdef")
_SAFE_ID: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}\Z")
_FORBIDDEN_SNAPSHOT_KEYS: Final = frozenset(
    {
        "access_token",
        "token",
        "authorization",
        "principal",
        "principal_id",
        "sub",
        "auth",
        "jwt",
    }
)


class GrokLimitSnapshotError(Exception):
    """Fail-closed export/load error with a fixed oracle code."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class Binding:
    profile_id: str
    account_leaf: str
    expected_principal_digest: str | None = None


@dataclass(frozen=True, slots=True)
class BindingsDocument:
    profiles_root: Path
    bindings: tuple[Binding, ...]


@dataclass(frozen=True, slots=True)
class GrokLimitEntry:
    profile_id: str
    principal_digest: str
    remaining_percent: int
    reset_at: datetime | None
    plan_label: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Structured loader outcome — never raises for ordinary unbound/stale cases."""

    entry: GrokLimitEntry | None
    code: str | None

    @property
    def ok(self) -> bool:
        return self.entry is not None and self.code is None


def principal_digest(principal: str) -> str:
    """One-way digest; callers must not log ``principal``."""

    return hashlib.sha256(principal.encode("utf-8")).hexdigest()


def verify_entry_against_digest(
    entry: GrokLimitEntry, expected_principal_digest: str | None
) -> str | None:
    """Return mismatch code or None when ok / no expected digest."""

    if expected_principal_digest is None:
        return None
    if not _is_hex64(expected_principal_digest):
        return CODE_BINDING_MISMATCH
    if entry.principal_digest != expected_principal_digest.lower():
        return CODE_BINDING_MISMATCH
    return None


def load_bindings(bindings_file: Path) -> BindingsDocument:
    """Load and strictly validate a bindings document."""

    raw = _read_regular_file(
        bindings_file, maximum=MAX_BINDINGS_BYTES, code=CODE_EXPORT_BINDINGS_INVALID
    )
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as error:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "bindings json") from error
    if not isinstance(document, dict):
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "bindings not object")
    if set(document.keys()) != _BINDINGS_TOP_KEYS:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "bindings keys")
    if document.get("schema_version") != BINDINGS_SCHEMA_VERSION:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "bindings schema")
    root_raw = document.get("profiles_root")
    if not isinstance(root_raw, str) or not root_raw:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "profiles_root")
    root = Path(root_raw)
    if not root.is_absolute():
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "profiles_root not absolute")
    _reject_symlink_path(root, code=CODE_EXPORT_PATH_REJECTED)
    if not root.is_dir():
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "profiles_root not directory")

    raw_bindings = document.get("bindings")
    if not isinstance(raw_bindings, list):
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "bindings list")
    if len(raw_bindings) > MAX_BINDINGS:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "too many bindings")

    bindings: list[Binding] = []
    seen_ids: set[str] = set()
    seen_leaves: set[str] = set()
    for item in raw_bindings:
        if not isinstance(item, dict):
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "binding not object")
        if not set(item.keys()) <= _BINDING_KEYS:
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "binding keys")
        if "profile_id" not in item or "account_leaf" not in item:
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "binding required fields")
        profile_id = item["profile_id"]
        account_leaf = item["account_leaf"]
        if not isinstance(profile_id, str) or _SAFE_ID.fullmatch(profile_id) is None:
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "profile_id")
        if not isinstance(account_leaf, str):
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "account_leaf type")
        _validate_account_leaf(account_leaf)
        expected = item.get("expected_principal_digest")
        if expected is not None:
            if not isinstance(expected, str) or not _is_hex64(expected):
                raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "digest")
            expected = expected.lower()
        if profile_id in seen_ids:
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "duplicate profile_id")
        if account_leaf in seen_leaves:
            raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "duplicate account_leaf")
        seen_ids.add(profile_id)
        seen_leaves.add(account_leaf)
        bindings.append(
            Binding(
                profile_id=profile_id,
                account_leaf=account_leaf,
                expected_principal_digest=expected,
            )
        )
    return BindingsDocument(profiles_root=root, bindings=tuple(bindings))


def export_grok_limit_snapshot(
    bindings_file: Path,
    output_file: Path,
    *,
    now: datetime | None = None,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> int:
    """Export snapshot entries from bindings; return entry count.

    Fail-closed: binding-level principal/billing failures raise and publish nothing.
    Unmapped FS accounts are never scanned (bindings only).
    """

    observed = _utc(now or datetime.now(UTC))
    document = load_bindings(bindings_file)
    entries: list[GrokLimitEntry] = []
    for binding in document.bindings:
        entry = _entry_from_binding(document.profiles_root, binding, observed=observed)
        entries.append(entry)
    if len(entries) > MAX_ENTRIES:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "too many entries")
    entries.sort(key=lambda item: item.profile_id)
    payload_obj: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": observed.isoformat(),
        "entries": [
            {
                "profile_id": entry.profile_id,
                "principal_digest": entry.principal_digest,
                "remaining_percent": entry.remaining_percent,
                "reset_at": entry.reset_at.isoformat() if entry.reset_at else None,
                "plan_label": entry.plan_label,
                "observed_at": entry.observed_at.isoformat(),
            }
            for entry in entries
        ],
    }
    payload = (json.dumps(payload_obj, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(payload) > MAX_SNAPSHOT_BYTES:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "snapshot oversize")
    _atomic_publish(
        output_file,
        payload,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )
    return len(entries)


def load_grok_limit_entry(
    snapshot_file: Path,
    profile_id: str,
    *,
    now: datetime | None = None,
    max_age: timedelta = DEFAULT_MAX_AGE,
    expected_principal_digest: str | None = None,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> LoadResult:
    """Load one entry by ``profile_id`` with fixed error codes."""

    observed = _utc(now or datetime.now(UTC))
    if not isinstance(profile_id, str) or not profile_id:
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if max_age <= timedelta(0):
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    try:
        fd = _open_regular_nofollow(snapshot_file)
        try:
            st = os.fstat(fd)
            mode = stat.S_IMODE(st.st_mode)
            if (
                mode & 0o006
                or st.st_size > MAX_SNAPSHOT_BYTES
                or (expected_uid is not None and st.st_uid != expected_uid)
                or (expected_gid is not None and st.st_gid != expected_gid)
            ):
                return LoadResult(None, CODE_SNAPSHOT_INVALID)
            raw = _read_bounded_fd(fd, MAX_SNAPSHOT_BYTES)
        finally:
            os.close(fd)
    except OSError:
        return LoadResult(None, CODE_SNAPSHOT_UNREADABLE)
    except ValueError:
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if len(raw) > MAX_SNAPSHOT_BYTES:
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if not isinstance(document, dict) or set(document.keys()) != _SNAPSHOT_TOP_KEYS:
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if document.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if any(key in document for key in _FORBIDDEN_SNAPSHOT_KEYS):
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    generated = _parse_datetime(document.get("generated_at"))
    if generated is None or generated > observed + timedelta(minutes=1):
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if observed - generated > max_age:
        return LoadResult(None, CODE_SNAPSHOT_STALE)
    raw_entries = document.get("entries")
    if not isinstance(raw_entries, list) or len(raw_entries) > MAX_ENTRIES:
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    found: GrokLimitEntry | None = None
    for raw_entry in raw_entries:
        entry = _validated_entry(raw_entry)
        if entry is None:
            return LoadResult(None, CODE_SNAPSHOT_INVALID)
        if entry.profile_id == profile_id:
            if found is not None:
                return LoadResult(None, CODE_SNAPSHOT_INVALID)
            found = entry
    if found is None:
        return LoadResult(None, CODE_SNAPSHOT_UNBOUND)
    if found.observed_at > observed + timedelta(minutes=1):
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if found.observed_at > generated + timedelta(minutes=1):
        return LoadResult(None, CODE_SNAPSHOT_INVALID)
    if observed - found.observed_at > max_age:
        return LoadResult(None, CODE_SNAPSHOT_STALE)
    mismatch = verify_entry_against_digest(found, expected_principal_digest)
    if mismatch is not None:
        return LoadResult(None, mismatch)
    return LoadResult(found, None)


# --- internal: bindings / paths ---


def _validate_account_leaf(leaf: str) -> None:
    if not leaf or leaf in {".", ".."}:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "account_leaf")
    if "/" in leaf or "\\" in leaf or "\x00" in leaf:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "account_leaf traversal")
    if leaf.startswith(".") and leaf not in {".", ".."}:
        # Allow normal names; reject only explicit traversal. Single-segment is enough.
        pass
    if Path(leaf).name != leaf:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "account_leaf multi")
    if _SAFE_ID.fullmatch(leaf) is None:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "account_leaf unsafe")


def _reject_symlink_path(path: Path, *, code: str) -> None:
    try:
        st = path.lstat()
    except OSError as error:
        raise GrokLimitSnapshotError(code, "path missing") from error
    if stat.S_ISLNK(st.st_mode):
        raise GrokLimitSnapshotError(code, "symlink")


def _require_beneath_without_symlinks(root: Path, path: Path) -> None:
    """Ensure ``path`` is under ``root`` with no symlink components."""

    try:
        root_resolved = root.resolve(strict=True)
    except OSError as error:
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "root resolve") from error
    # Walk from root to path; reject any symlink component.
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        # Compare using resolved strings carefully
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "not beneath root") from error
    cursor = root
    _reject_symlink_path(cursor, code=CODE_EXPORT_PATH_REJECTED)
    for part in relative.parts:
        cursor = cursor / part
        _reject_symlink_path(cursor, code=CODE_EXPORT_PATH_REJECTED)
    try:
        if not path.resolve(strict=True).is_relative_to(root_resolved):
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "escape")
    except (OSError, ValueError) as error:
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "resolve") from error


# --- internal: export from one binding ---


def _entry_from_binding(
    profiles_root: Path, binding: Binding, *, observed: datetime
) -> GrokLimitEntry:
    account = profiles_root / binding.account_leaf
    _require_beneath_without_symlinks(profiles_root, account)
    try:
        st = account.lstat()
    except OSError as error:
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "account missing") from error
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "account not dir")

    principals: set[str] = set()
    latest_billing: tuple[dict[str, Any], datetime] | None = None
    for relative in _ALLOWED_LOG_RELS:
        log_path = account / relative
        try:
            st_log = log_path.lstat()
        except OSError:
            continue
        # Present path must be a regular file under root without symlink hops.
        if stat.S_ISLNK(st_log.st_mode):
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log symlink")
        if not stat.S_ISREG(st_log.st_mode):
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log nonregular")
        if stat.S_IMODE(st_log.st_mode) & 0o002:
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log world writable")
        if st_log.st_size > MAX_LOG_SEGMENT_BYTES:
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log oversize")
        _require_beneath_without_symlinks(profiles_root, log_path)
        try:
            head, tail, modified = _read_log_segments(log_path)
        except GrokLimitSnapshotError:
            raise
        except OSError as error:
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log read") from error
        principals |= _principals_from_log_bytes(head + b"\n" + tail)
        billing = _latest_billing_from_bytes(tail)
        if billing is not None and (latest_billing is None or modified > latest_billing[1]):
            latest_billing = billing, modified

    if len(principals) == 0:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "zero principal")
    if len(principals) > 1:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "multi principal")
    principal = next(iter(principals))
    digest = principal_digest(principal)
    if (
        binding.expected_principal_digest is not None
        and digest != binding.expected_principal_digest
    ):
        # Do not include principal in the message.
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "digest mismatch")
    if latest_billing is None:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "no billing")
    billing_ctx, modified = latest_billing
    remaining, reset_at, plan_label = _normalize_billing(billing_ctx)
    return GrokLimitEntry(
        profile_id=binding.profile_id,
        principal_digest=digest,
        remaining_percent=remaining,
        reset_at=reset_at,
        plan_label=plan_label,
        observed_at=_utc(modified) if modified.tzinfo else modified.replace(tzinfo=UTC),
    )


def _principals_from_log_bytes(raw: bytes) -> set[str]:
    """Collect principal ids from structured log JSON only (no JWT/auth)."""

    found: set[str] = set()
    text = raw.decode("utf-8", errors="ignore")
    for line in text.splitlines():
        if len(line.encode("utf-8", errors="ignore")) > MAX_LOG_LINE_BYTES:
            continue
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        for key in ("principal_id", "principal", "sub"):
            value = payload.get(key)
            if isinstance(value, str) and _looks_like_principal(value):
                found.add(value)
        ctx = payload.get("ctx")
        if isinstance(ctx, dict):
            for key in ("principal_id", "principal", "sub"):
                value = ctx.get(key)
                if isinstance(value, str) and _looks_like_principal(value):
                    found.add(value)
    return found


def _looks_like_principal(value: str) -> bool:
    if not value or len(value) > 200 or len(value) < 3:
        return False
    if any(ch.isspace() for ch in value):
        return False
    return not (value in {".", ".."} or "/" in value)


def _latest_billing_from_bytes(raw: bytes) -> dict[str, Any] | None:
    text = raw.decode("utf-8", errors="ignore")
    for line in reversed(text.splitlines()):
        if len(line) > MAX_LOG_LINE_BYTES:
            continue
        if "billing: fetched credits config" not in line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        ctx = payload.get("ctx")
        if isinstance(ctx, dict):
            return ctx
    return None


def _normalize_billing(ctx: dict[str, Any]) -> tuple[int, datetime | None, str]:
    config = ctx.get("config") if isinstance(ctx.get("config"), dict) else ctx
    if not isinstance(config, dict):
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "billing shape")
    used = config.get("creditUsagePercent")
    if (
        not isinstance(used, (int, float))
        or isinstance(used, bool)
        or not math.isfinite(float(used))
    ):
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "usage")
    used_f = float(used)
    if used_f < 0 or used_f > 100:
        raise GrokLimitSnapshotError(CODE_EXPORT_BINDINGS_INVALID, "usage range")
    remaining = max(0, min(100, round(100.0 - used_f)))
    reset_at = _parse_datetime(config.get("billingPeriodEnd") or config.get("end"))
    plan_raw = ctx.get("subscriptionTier") or config.get("subscriptionTier") or "Super"
    plan_label = _normalized_plan(plan_raw)
    return remaining, reset_at, plan_label


def _normalized_plan(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return "Super"
    text = value.strip()
    lowered = text.lower().replace(" ", "").replace("_", "")
    mapping = {
        "supergrok": "Super",
        "super": "Super",
        "supergrokheavy": "Super Heavy",
        "free": "Free",
    }
    return mapping.get(lowered, "Unknown")


def _read_log_segments(path: Path) -> tuple[bytes, bytes, datetime]:
    fd = _open_regular_nofollow(path)
    try:
        opened = os.fstat(fd)
        if stat.S_IMODE(opened.st_mode) & 0o002:
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log world writable")
        if opened.st_size > MAX_LOG_SEGMENT_BYTES:
            raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "log oversize")
        head_size = min(opened.st_size, MAX_LOG_SEGMENT_BYTES // 2)
        head = os.pread(fd, head_size, 0)
        tail_offset = max(0, opened.st_size - MAX_LOG_SEGMENT_BYTES // 2)
        tail = os.pread(fd, opened.st_size - tail_offset, tail_offset)
    finally:
        os.close(fd)
    modified = datetime.fromtimestamp(opened.st_mtime, tz=UTC)
    return head, tail, modified


# --- internal: publish / load helpers ---


def _atomic_publish(
    output_file: Path,
    payload: bytes,
    *,
    expected_uid: int | None,
    expected_gid: int | None,
) -> None:
    if not output_file.is_absolute():
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "output not absolute")
    parent = output_file.parent
    _reject_symlink_path(parent, code=CODE_EXPORT_PATH_REJECTED)
    if not parent.is_dir():
        raise GrokLimitSnapshotError(CODE_EXPORT_PATH_REJECTED, "output parent")
    parent_st = parent.stat()
    if expected_uid is not None or expected_gid is not None:
        if stat.S_IMODE(parent_st.st_mode) != 0o2750:
            raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "directory mode")
        if expected_uid is not None and parent_st.st_uid != expected_uid:
            raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "directory uid")
        if expected_gid is not None and parent_st.st_gid != expected_gid:
            raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "directory gid")

    fd: int | None = None
    tmp_name: str | None = None
    published_identity: tuple[int, int] | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=".grok-limit-", suffix=".tmp", dir=str(parent))
        with os.fdopen(fd, "wb") as handle:
            fd = None  # owned by handle
            handle.write(payload)
            handle.flush()
            os.fchmod(handle.fileno(), 0o640)
            os.fsync(handle.fileno())
            # Explicitly do not fchown — setgid dir must supply group.
            written = os.fstat(handle.fileno())
            published_identity = (written.st_dev, written.st_ino)
        os.replace(tmp_name, output_file)
        tmp_name = None
        dir_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError as error:
        if tmp_name is not None:
            with suppress(OSError):
                os.unlink(tmp_name)
        if fd is not None:
            with suppress(OSError):
                os.close(fd)
        raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "publish") from error

    try:
        st = output_file.lstat()
    except OSError as error:
        raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "verify lstat") from error
    identity_matches = published_identity == (st.st_dev, st.st_ino)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode) or not identity_matches:
        raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "verify type")
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o640:
        with suppress(OSError):
            output_file.unlink()
        raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "verify mode")
    if expected_uid is not None and st.st_uid != expected_uid:
        with suppress(OSError):
            output_file.unlink()
        raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "verify uid")
    if expected_gid is not None and st.st_gid != expected_gid:
        with suppress(OSError):
            output_file.unlink()
        raise GrokLimitSnapshotError(CODE_EXPORT_OWNERSHIP_FAILED, "verify gid")


def _read_regular_file(path: Path, *, maximum: int, code: str) -> str:
    try:
        fd = _open_regular_nofollow(path)
        try:
            st = os.fstat(fd)
            if st.st_size > maximum:
                raise GrokLimitSnapshotError(code, "oversize")
            raw = _read_bounded_fd(fd, maximum)
        finally:
            os.close(fd)
    except (OSError, ValueError) as error:
        raise GrokLimitSnapshotError(code, "read") from error
    if len(raw) > maximum:
        raise GrokLimitSnapshotError(code, "oversize")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise GrokLimitSnapshotError(code, "utf-8") from error


def _open_regular_nofollow(path: Path) -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("not regular")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _read_bounded_fd(fd: int, maximum: int) -> bytes:
    chunks: list[bytes] = []
    remaining = maximum + 1
    while remaining:
        chunk = os.read(fd, min(remaining, 64 * 1024))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    raw = b"".join(chunks)
    if len(raw) > maximum:
        raise ValueError("oversize")
    return raw


def _validated_entry(raw: object) -> GrokLimitEntry | None:
    if not isinstance(raw, dict):
        return None
    if set(raw.keys()) != _ENTRY_KEYS:
        return None
    if any(key in raw for key in _FORBIDDEN_SNAPSHOT_KEYS):
        return None
    profile_id = raw.get("profile_id")
    digest = raw.get("principal_digest")
    remaining = raw.get("remaining_percent")
    plan = raw.get("plan_label")
    observed_raw = raw.get("observed_at")
    reset_raw = raw.get("reset_at")
    if not isinstance(profile_id, str) or _SAFE_ID.fullmatch(profile_id) is None:
        return None
    if not isinstance(digest, str) or not _is_hex64(digest):
        return None
    if (
        not isinstance(remaining, int)
        or isinstance(remaining, bool)
        or remaining < 0
        or remaining > 100
    ):
        return None
    if not isinstance(plan, str) or not plan or len(plan) > 50:
        return None
    observed = _parse_datetime(observed_raw)
    if observed is None:
        return None
    reset_at = None
    if reset_raw is not None:
        reset_at = _parse_datetime(reset_raw)
        if reset_at is None:
            return None
    return GrokLimitEntry(
        profile_id=profile_id,
        principal_digest=digest.lower(),
        remaining_percent=remaining,
        reset_at=reset_at,
        plan_label=plan,
        observed_at=observed,
    )


def _is_hex64(value: str) -> bool:
    if len(value) != 64:
        return False
    return all(ch in _HEX64 for ch in value.lower())


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
