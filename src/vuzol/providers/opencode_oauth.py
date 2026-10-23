"""Explicit, bounded, read-only OpenCode credential-store access.

Protocol reference: get-bb/bb@2884950f17e76f78b28c64ccf4a2e9a7c74188a8,
opencode-usage.ts; store verified against anomalyco/opencode@096c2ba5.
Independent Python implementation: no active-account discovery, fallback,
refresh-token use, migration, login or SQL writes. OpenCode owns rotation.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import stat
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from pydantic import SecretStr

from vuzol.config.models import OpenCodeUsageOAuthConfig
from vuzol.providers.account_auth import AccountAuthError

IMPORT_MIGRATION = "20260805200742_import_legacy_credentials"


@dataclass(frozen=True)
class GoOAuthCredential:
    access: SecretStr
    expires_at: datetime
    org_id: str


def _official_issuer(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parts = urlsplit(value)
    return (
        parts.scheme == "https"
        and parts.netloc == "opencode.ai"
        and parts.path in {"", "/", "/console", "/console/"}
        and not parts.query
        and not parts.fragment
    )


def read_go_oauth(
    binding: OpenCodeUsageOAuthConfig, *, now: datetime, allow_expired: bool = False
) -> GoOAuthCredential:
    """Read one selected record in a consistent transaction, including WAL.

    mode=ro/query_only prohibit credential writes. Do not use immutable=1:
    ignoring the CLI's WAL would resurrect pre-rotation/revoked credentials.
    Never expose sqlite errors, JSON records, emails or tokens in diagnostics.
    """
    path = binding.store_path
    try:
        if path.resolve() != path or path.parent.stat().st_mode & 0o022:
            raise AccountAuthError("auth_permissions")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o027:
                raise AccountAuthError("auth_permissions")
            # One exact row, not a desktop credential scan. SQL limits prevent
            # oversized JSON from reaching Python; progress/busy limits bound IO.
            deadline = time.monotonic() + 1
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", timeout=0.2)) as db:
                db.execute("PRAGMA query_only=ON")
                db.execute("PRAGMA trusted_schema=OFF")
                db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                after = path.stat()
                if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                    raise AccountAuthError("auth_store_changed")
                db.execute("BEGIN")
                tables = {
                    r[0]
                    for r in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name IN ('migration','credential','account','account_state')"
                    )
                }
                migrated = (
                    "migration" in tables
                    and db.execute(
                        "SELECT 1 FROM migration WHERE id=?", (IMPORT_MIGRATION,)
                    ).fetchone()
                    is not None
                )
                if binding.store_format == "credential_v2":
                    if not migrated or "credential" not in tables:
                        raise AccountAuthError("auth_store_unsupported")
                    row = db.execute(
                        "SELECT CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value END, "
                        "integration_id FROM credential WHERE id=?",
                        (binding.credential_id,),
                    ).fetchone()
                    if row is None:
                        raise AccountAuthError("reauth_required")
                    if row[1] != "opencode":
                        raise AccountAuthError("auth_identity_mismatch")
                    raw = json.loads(row[0]) if isinstance(row[0], str) else None
                    if (
                        not isinstance(raw, dict)
                        or raw.get("type") != "oauth"
                        or raw.get("methodID") != "device"
                    ):
                        raise AccountAuthError("auth_invalid")
                    meta = raw.get("metadata")
                    if not isinstance(meta, dict):
                        raise AccountAuthError("auth_invalid")
                    # Default issuer is defined by the official device method;
                    # an explicit foreign issuer must never inherit that default.
                    issuer = meta.get("server", "https://opencode.ai/console")
                    if (
                        meta.get("accountID") != binding.account_id
                        or meta.get("orgID") != binding.org_id
                    ):
                        raise AccountAuthError("auth_identity_mismatch")
                    access, expiry = raw.get("access"), raw.get("expires")
                else:
                    if migrated:
                        raise AccountAuthError("auth_store_migrated")
                    if not {"account", "account_state"} <= tables:
                        raise AccountAuthError("auth_store_unsupported")
                    # The configured identity, not the active desktop account,
                    # selects the row. State is only corroborating org evidence.
                    row = db.execute(
                        "SELECT a.url, CASE WHEN length(a.access_token)<=8192 "
                        "THEN a.access_token END, a.token_expiry, s.active_org_id "
                        "FROM account a JOIN account_state s ON s.active_account_id=a.id "
                        "WHERE a.id=? AND s.id=1",
                        (binding.account_id,),
                    ).fetchone()
                    if row is None or row[3] != binding.org_id:
                        raise AccountAuthError("auth_identity_mismatch")
                    issuer, access, expiry = row[:3]
                if not _official_issuer(issuer):
                    raise AccountAuthError("auth_issuer_mismatch")
                if (
                    not isinstance(access, str)
                    or not access
                    or len(access) > 8192
                    or any(ord(c) < 33 or ord(c) > 126 for c in access)
                    or isinstance(expiry, bool)
                    or not isinstance(expiry, (int, float))
                    or not math.isfinite(expiry)
                    or expiry < 0
                ):
                    raise AccountAuthError("auth_invalid")
                expires_at = datetime.fromtimestamp(expiry / 1000, UTC)
                # Leave enough lifetime for our bounded HTTP request.
                if not allow_expired and (expires_at - now).total_seconds() <= 5:
                    raise AccountAuthError("auth_expired")
                return GoOAuthCredential(SecretStr(access), expires_at, binding.org_id)
    except AccountAuthError:
        raise
    except OSError:
        raise AccountAuthError("auth_unreadable") from None
    except sqlite3.OperationalError as error:
        code = getattr(error, "sqlite_errorcode", None)
        if code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_INTERRUPT}:
            raise AccountAuthError("auth_store_busy") from None
        raise AccountAuthError("auth_invalid") from None
    except (sqlite3.Error, ValueError, TypeError, OverflowError, RecursionError):
        raise AccountAuthError("auth_invalid") from None
