"""Bounded, read-only access to the explicitly configured account's auth.

No login, scanning, shell key commands, token refresh or sandbox write-back.
"""

import json
import os
import stat

from pydantic import SecretStr

from vuzol.config.models import AccountConfig


class AccountAuthError(ValueError):
    """Fixed safe code, never credential contents or provider exception text."""


def read_go_key(account: AccountConfig) -> SecretStr:
    if account.auth_method != "static_api_key" or account.service != "opencode-go":
        raise AccountAuthError("auth_method_unsupported")
    if account.state_directory is None:
        raise AccountAuthError("auth_unreadable")
    try:
        descriptor = os.open(
            account.state_directory / "auth.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        )
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o007:
                raise AccountAuthError("auth_permissions")
            data = stream.read(65537)
        if len(data) > 65536:
            raise AccountAuthError("auth_invalid")
        raw = json.loads(data)
        entry = raw.get("opencode-go") if isinstance(raw, dict) else None
        if not isinstance(entry, dict) or entry.get("type") != "api_key":
            raise AccountAuthError("auth_method_unsupported")
        key = entry.get("key")
        if isinstance(key, str) and key.startswith("!"):
            raise AccountAuthError("auth_method_unsupported")
        if not isinstance(key, str) or not key or len(key) > 8192 or any(c.isspace() for c in key):
            raise AccountAuthError("auth_invalid")
        # Pi also supports command-backed keys. A quota check must not execute
        # them or accidentally transmit their source text as a credential.
        return SecretStr(key)
    except AccountAuthError:
        raise
    except OSError:
        raise AccountAuthError("auth_unreadable") from None
    except (ValueError, UnicodeError):
        raise AccountAuthError("auth_invalid") from None
