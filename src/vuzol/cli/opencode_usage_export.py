"""One usage export tick. The official OpenCode CLI alone rotates OAuth tokens."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import subprocess
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from vuzol.config.models import AccountConfig
from vuzol.config.settings import OpenCodeSnapshotBinding
from vuzol.providers.account_auth import AccountAuthError
from vuzol.providers.opencode_oauth import read_go_oauth
from vuzol.providers.opencode_snapshot import UsageSnapshot, publish_usage_snapshot
from vuzol.providers.opencode_usage import GoUsage, OpenCodeUsageCollector


class ExportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    account: AccountConfig
    output: OpenCodeSnapshotBinding
    binding_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    cli_binary: Path
    cli_version: str
    xdg_root: Path
    lock_file: Path
    owner_launcher: Path | None = None
    owner_launcher_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def binding_matches(self) -> ExportConfig:
        oauth = self.account.quota_oauth
        if (
            oauth is None
            or oauth.store_format != "account_v1"
            or self.account.id != self.output.account_id
            or oauth.scope_id != self.output.scope_id
            or oauth.store_path != self.xdg_root / "data/opencode/opencode.db"
            or not all(p.is_absolute() for p in (self.cli_binary, self.xdg_root, self.lock_file))
        ):
            raise ValueError("export requires one exact supported account binding")
        if (self.owner_launcher is None) != (self.owner_launcher_sha256 is None):
            raise ValueError("owner launcher requires its verified digest")
        if self.owner_launcher is not None and not self.owner_launcher.is_absolute():
            raise ValueError("owner launcher must be absolute")
        return self


@contextmanager
def owner_lock(path: Path, *, wait: bool = False) -> Iterator[int]:
    if path.parent.resolve() != path.parent or path.parent.stat().st_mode & 0o022:
        raise AccountAuthError("auth_permissions")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_mode & 0o077
        ):
            raise AccountAuthError("auth_permissions")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError:
            raise AccountAuthError("auth_refresh_busy") from None
        yield fd
    finally:
        os.close(fd)


def cli_environment(config: ExportConfig) -> dict[str, str]:
    # Do not forward API-key overrides, plugins, proxy settings or debug logging.
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(Path.home()),
        "LANG": "C.UTF-8",
        "XDG_DATA_HOME": str(config.xdg_root / "data"),
        "XDG_CONFIG_HOME": str(config.xdg_root / "config"),
        "XDG_STATE_HOME": str(config.xdg_root / "state"),
        "XDG_CACHE_HOME": str(config.xdg_root / "cache"),
        "OPENCODE_DISABLE_AUTOUPDATE": "true",
        "OPENCODE_DISABLE_MODELS_FETCH": "true",
    }


def refresh_with_cli(
    config: ExportConfig, *, now: datetime, verify_cli: bool = False, lock_fd: int | None = None
) -> None:
    """Must run under owner_lock, shared with the operator's CLI launcher.

    console orgs resolves OAuth through OpenCode's refresh implementation, without
    inference. It visits all accounts, so require a single selected account here.
    Exit code alone is insufficient: OpenCode can swallow a failed orgs request.
    """
    oauth = config.account.quota_oauth
    assert oauth is not None
    credential = read_go_oauth(oauth, now=now, allow_expired=True)
    if not verify_cli and (credential.expires_at - now).total_seconds() > 300:
        return
    if config.owner_launcher is not None:
        metadata = config.owner_launcher.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_mode & 0o022
            or hashlib.sha256(config.owner_launcher.read_bytes()).hexdigest()
            != config.owner_launcher_sha256
        ):
            raise AccountAuthError("auth_refresh_owner_changed")
    with closing(sqlite3.connect(oauth.store_path.as_uri() + "?mode=ro", timeout=0.2)) as db:
        rows = db.execute("SELECT id FROM account LIMIT 2").fetchall()
        if rows != [(oauth.account_id,)]:
            raise AccountAuthError("auth_refresh_scope_invalid")
    environment = cli_environment(config)
    try:
        version = subprocess.run(  # noqa: S603
            [str(config.cli_binary), "--version"],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
        if version.returncode or version.stdout.decode().strip() != config.cli_version:
            raise AccountAuthError("auth_cli_version_changed")
        completed = subprocess.run(  # noqa: S603
            [str(config.cli_binary), "--pure", "console", "orgs"],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
            check=False,
            pass_fds=(lock_fd,) if lock_fd is not None else (),
        )
        after = read_go_oauth(oauth, now=datetime.now(UTC), allow_expired=True)
        if completed.returncode or (after.expires_at - datetime.now(UTC)).total_seconds() <= 300:
            raise AccountAuthError("auth_refresh_failed")
    except (OSError, subprocess.TimeoutExpired):
        raise AccountAuthError("auth_refresh_failed") from None


def export_once(config: ExportConfig, *, verify_cli: bool = False) -> UsageSnapshot:
    now = datetime.now(UTC)
    try:
        with owner_lock(config.lock_file) as lock_fd:
            refresh_with_cli(config, now=now, verify_cli=verify_cli, lock_fd=lock_fd)
            usage = OpenCodeUsageCollector().collect(config.account)
    except AccountAuthError as error:
        if str(error) == "auth_refresh_busy":
            # An interactive CLI owns rotation; reading its current token is safe.
            usage = OpenCodeUsageCollector().collect(config.account)
            if usage.detail == "auth_expired":
                usage = GoUsage(now, detail="auth_refresh_busy")
        else:
            usage = GoUsage(now, detail=str(error))
    except (OSError, sqlite3.Error, ValueError):
        usage = GoUsage(now, detail="auth_refresh_failed")
    snap = UsageSnapshot(
        account_id=config.account.id,
        binding_digest=config.binding_digest,
        scope_id=config.output.scope_id,
        generated_at=datetime.now(UTC),
        attempted_at=usage.attempted_at,
        observed_at=usage.observed_at,
        detail=usage.detail,
        windows=usage.windows,
        entitlement_observed_at=usage.entitlement_observed_at,
    )
    publish_usage_snapshot(config.output.path, snap)
    return snap


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--verify-cli", action="store_true")
    args = parser.parse_args()
    try:
        config = ExportConfig.model_validate_json(args.config.read_bytes())
        snapshot = export_once(config, verify_cli=args.verify_cli)
        print(
            json.dumps(
                {
                    "account": snapshot.account_id,
                    "ok": not snapshot.detail,
                    "detail": snapshot.detail,
                    "at": snapshot.generated_at.isoformat(),
                }
            )
        )
        raise SystemExit(0 if not snapshot.detail else 2)
    except (OSError, ValueError):
        print('{"error":"usage_export_configuration_or_output_invalid"}')
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
