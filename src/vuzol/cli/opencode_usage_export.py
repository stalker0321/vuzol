"""One usage export tick. The official OpenCode CLI alone rotates OAuth tokens.

Owner 2026-11-25: limits of every OpenCode Go account are read without per-account
setup. ``--setup`` finds each Pi account of the registry whose console login lives in
``accounts_root/<account_id>`` and writes ``usage_root/<account_id>/usage.json``; a new
account needs only its login (``vuzol.cli.opencode_owner --account <id> -- console login``).
"""

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

from vuzol.config.loader import load_document
from vuzol.config.models import AccountConfig, LaunchMode, OpenCodeUsageOAuthConfig
from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import OpenCodeSnapshotBinding
from vuzol.providers.account_auth import AccountAuthError
from vuzol.providers.opencode_oauth import read_go_oauth
from vuzol.providers.opencode_snapshot import (
    UsageSnapshot,
    execution_binding,
    publish_usage_snapshot,
)
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


class Setup(BaseModel):
    """Where account logins and usage snapshots live; nothing per account."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    registry: Path
    accounts_root: Path
    usage_root: Path
    cli_binary: Path
    cli_version: str
    owner_launcher: Path | None = None
    owner_launcher_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


def login_root(setup: Setup, account_id: str) -> Path:
    return setup.accounts_root / account_id


def store_identity(store: Path) -> tuple[str, str] | None:
    """The one console account of a login store and its active org, or None."""
    try:
        with closing(sqlite3.connect(store.as_uri() + "?mode=ro", uri=True, timeout=0.2)) as db:
            accounts = db.execute("SELECT id FROM account LIMIT 2").fetchall()
            state = db.execute(
                "SELECT active_account_id, active_org_id FROM account_state LIMIT 2"
            ).fetchall()
    except sqlite3.Error:
        return None
    if len(accounts) != 1 or len(state) != 1 or state[0][0] != accounts[0][0] or not state[0][1]:
        return None
    return str(accounts[0][0]), str(state[0][1])


def discover(setup: Setup) -> list[ExportConfig]:
    """An export for every enabled Pi account of the registry that has a console login."""
    configs: list[ExportConfig] = []
    seen: set[str] = set()
    for profile in ProfileRegistry(load_document(setup.registry).profiles).items():
        account_id = profile.account_id
        if (
            account_id is None
            or account_id in seen
            or not profile.enabled
            or profile.provider != "pi"
            or profile.launch_mode is not LaunchMode.CLI
            or profile.state_directory is None
            or profile.runtime_identity is None
        ):
            continue
        root = login_root(setup, account_id)
        store = root / "data/opencode/opencode.db"
        identity = store_identity(store) if store.is_file() else None
        if identity is None:
            continue
        seen.add(account_id)
        oauth = OpenCodeUsageOAuthConfig(
            store_path=store, store_format="account_v1", account_id=identity[0], org_id=identity[1]
        )
        configs.append(
            ExportConfig(
                account=AccountConfig(
                    id=account_id,
                    service="opencode-go",
                    provider="pi",
                    launch_mode=LaunchMode.CLI,
                    auth_method="static_api_key",
                    runtime_identity=profile.runtime_identity,
                    state_directory=profile.state_directory,
                    concurrency_limit=profile.concurrency_limit,
                    control_reserved_slots=profile.control_reserved_slots,
                    quota_collector="opencode_go",
                    quota_scope_id=oauth.scope_id,
                    quota_oauth=oauth,
                ),
                output=OpenCodeSnapshotBinding(
                    account_id=account_id,
                    scope_id=oauth.scope_id,
                    path=setup.usage_root / account_id / "usage.json",
                ),
                binding_digest=execution_binding(profile),
                cli_binary=setup.cli_binary,
                cli_version=setup.cli_version,
                xdg_root=root,
                lock_file=root / "data/opencode/vuzol-owner.lock",
                owner_launcher=setup.owner_launcher,
                owner_launcher_sha256=setup.owner_launcher_sha256,
            )
        )
    return configs


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


def cli_environment(xdg_root: Path) -> dict[str, str]:
    # Do not forward API-key overrides, plugins, proxy settings or debug logging.
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(Path.home()),
        "LANG": "C.UTF-8",
        "XDG_DATA_HOME": str(xdg_root / "data"),
        "XDG_CONFIG_HOME": str(xdg_root / "config"),
        "XDG_STATE_HOME": str(xdg_root / "state"),
        "XDG_CACHE_HOME": str(xdg_root / "cache"),
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
    with closing(
        sqlite3.connect(oauth.store_path.as_uri() + "?mode=ro", uri=True, timeout=0.2)
    ) as db:
        rows = db.execute("SELECT id FROM account LIMIT 2").fetchall()
        if rows != [(oauth.account_id,)]:
            raise AccountAuthError("auth_refresh_scope_invalid")
    environment = cli_environment(config.xdg_root)
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


def _report(snapshot: UsageSnapshot) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", type=Path, help="one account's export config")
    source.add_argument("--setup", type=Path, help="every account with a console login")
    parser.add_argument("--verify-cli", action="store_true")
    args = parser.parse_args()
    try:
        if args.config is not None:
            configs = [ExportConfig.model_validate_json(args.config.read_bytes())]
        else:
            configs = discover(Setup.model_validate_json(args.setup.read_bytes()))
        failed = False
        for config in configs:
            directory = config.output.path.parent
            if not directory.exists():
                # The group comes from usage_root's setgid; the unit's umask would hide it.
                directory.mkdir()
                directory.chmod(0o2750)
            snapshot = export_once(config, verify_cli=args.verify_cli)
            _report(snapshot)
            failed = failed or bool(snapshot.detail)
        raise SystemExit(2 if failed else 0)
    except (OSError, ValueError):
        print('{"error":"usage_export_configuration_or_output_invalid"}')
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
