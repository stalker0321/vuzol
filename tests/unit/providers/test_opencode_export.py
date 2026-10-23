"""Expired-token recovery goes through the official CLI and one shared owner lock."""

import hashlib
import os
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tests.unit.config.test_account_descriptors import descriptor
from tests.unit.providers.test_opencode_oauth import oauth_payload
from vuzol.cli.opencode_usage_export import ExportConfig, export_once, owner_lock, refresh_with_cli
from vuzol.providers.account_auth import AccountAuthError
from vuzol.providers.opencode_oauth import read_go_oauth
from vuzol.providers.opencode_usage import GoUsage, parse_oauth_go_usage


def config_fixture(tmp_path):
    now = datetime.now(UTC)
    root = tmp_path / "xdg"
    store = root / "data/opencode/opencode.db"
    store.parent.mkdir(parents=True, mode=0o700)
    tmp_path.chmod(0o700)
    with sqlite3.connect(store) as db:
        db.executescript(
            "CREATE TABLE account(id TEXT,url TEXT,access_token TEXT,token_expiry INTEGER);"
            "CREATE TABLE account_state(id INTEGER,active_account_id TEXT,active_org_id TEXT);"
        )
        db.execute(
            "INSERT INTO account VALUES ('account-c','https://opencode.ai/console',?,?)",
            ("old-access", (now.timestamp() - 10) * 1000),
        )
        db.execute("INSERT INTO account_state VALUES (1,'account-c','org-c')")
    store.chmod(0o600)
    return ExportConfig.model_validate(
        {
            "account": descriptor(
                tmp_path,
                id="go-c",
                quota_scope_id="opencode:organization:org-c:account:account-c",
                quota_oauth={
                    "store_path": str(store),
                    "store_format": "account_v1",
                    "account_id": "account-c",
                    "org_id": "org-c",
                },
            ),
            "output": {
                "account_id": "go-c",
                "scope_id": "opencode:organization:org-c:account:account-c",
                "path": str(tmp_path / "usage.json"),
            },
            "binding_digest": "a" * 64,
            "cli_binary": "/usr/local/bin/opencode",
            "cli_version": "1.18.32",
            "xdg_root": str(root),
            "lock_file": str(tmp_path / "owner.lock"),
        }
    )


def test_expired_refresh_persists_rotation_then_new_reader_and_no_repeat(tmp_path, monkeypatch):
    config = config_fixture(tmp_path)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["stdin"] == subprocess.DEVNULL and kwargs["timeout"] <= 30
        assert "OPENCODE_API_KEY" not in kwargs["env"]
        if argv[-1] == "--version":
            return SimpleNamespace(returncode=0, stdout=b"1.18.32\n")
        assert argv[-3:] == ["--pure", "console", "orgs"]
        with sqlite3.connect(config.account.quota_oauth.store_path) as db:
            db.execute(
                "UPDATE account SET access_token=?,token_expiry=?",
                ("new-access", (datetime.now(UTC).timestamp() + 3600) * 1000),
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setenv("OPENCODE_API_KEY", "never-forward")
    monkeypatch.setattr(subprocess, "run", run)
    with owner_lock(config.lock_file):
        refresh_with_cli(config, now=datetime.now(UTC))
    assert read_go_oauth(
        config.account.quota_oauth, now=datetime.now(UTC)
    ).access.get_secret_value() == ("new-access")
    with owner_lock(config.lock_file):
        refresh_with_cli(config, now=datetime.now(UTC))
    assert len(calls) == 2


@pytest.mark.parametrize("failure", ["timeout", "swallowed", "version", "identity"])
def test_refresh_failure_is_published_without_old_success(tmp_path, monkeypatch, failure):
    config = config_fixture(tmp_path)

    def run(argv, **kwargs):
        if argv[-1] == "--version":
            return SimpleNamespace(
                returncode=0, stdout=b"changed" if failure == "version" else b"1.18.32"
            )
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 30)
        if failure == "identity":
            with sqlite3.connect(config.account.quota_oauth.store_path) as db:
                db.execute("UPDATE account_state SET active_org_id='different'")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(
        "vuzol.providers.opencode_usage.OpenCodeUsageCollector.collect",
        lambda *a: pytest.fail("failed refresh cannot send an expired token"),
    )
    snap = export_once(config)
    assert snap.detail and not snap.windows and snap.observed_at is None
    assert "old-access" not in config.output.path.read_text()


def test_owner_lock_contended_collects_fresh_token_but_does_not_refresh(tmp_path, monkeypatch):
    config = config_fixture(tmp_path)
    now = datetime.now(UTC)
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("second refresh owner"))
    monkeypatch.setattr(
        "vuzol.providers.opencode_usage.OpenCodeUsageCollector.collect",
        lambda *a: GoUsage(
            now,
            now,
            parse_oauth_go_usage(
                oauth_payload(now), observed_at=now, scope_id=config.output.scope_id
            ),
        ),
    )
    with owner_lock(config.lock_file):
        with (
            pytest.raises(AccountAuthError, match="auth_refresh_busy"),
            owner_lock(config.lock_file),
        ):
            pass
        assert export_once(config).windows


def test_multiple_accounts_never_refreshed_together(tmp_path, monkeypatch):
    config = config_fixture(tmp_path)
    with sqlite3.connect(config.account.quota_oauth.store_path) as db:
        db.execute("INSERT INTO account VALUES ('other','https://opencode.ai','private',0)")
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("scope must be checked first")
    )
    assert export_once(config).detail == "auth_refresh_scope_invalid"


def test_changed_owner_launcher_disables_refresh(tmp_path, monkeypatch):
    config = config_fixture(tmp_path)
    launcher = tmp_path / "opencode"
    launcher.write_text("original dispatcher")
    launcher.chmod(0o755)
    config = config.model_copy(
        update={
            "owner_launcher": launcher,
            "owner_launcher_sha256": hashlib.sha256(b"original dispatcher").hexdigest(),
        }
    )
    launcher.write_text("replaced by package update")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("lost serialization"))
    assert export_once(config).detail == "auth_refresh_owner_changed"


def test_operator_exec_inherits_lock_and_preserves_terminal_environment(tmp_path, monkeypatch):
    from vuzol.cli import opencode_owner

    config = config_fixture(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(config.model_dump_json())
    monkeypatch.setattr(sys, "argv", ["owner", "--config", str(config_path), "--", "--version"])
    monkeypatch.setenv("TERM", "xterm-256color")
    called = []

    def execve(binary, arguments, environment):
        assert str(binary) == str(config.cli_binary)
        assert arguments[-1] == "--version" and environment["TERM"] == "xterm-256color"
        assert any(
            os.get_inheritable(int(fd))
            and os.readlink(f"/proc/self/fd/{fd}") == str(config.lock_file)
            for fd in os.listdir("/proc/self/fd")
            if fd.isdigit() and os.path.exists(f"/proc/self/fd/{fd}")
        )
        with (
            pytest.raises(AccountAuthError, match="auth_refresh_busy"),
            owner_lock(config.lock_file),
        ):
            pass
        called.append(True)
        raise SystemExit(0)

    monkeypatch.setattr(os, "execve", execve)
    with pytest.raises(SystemExit) as done:
        opencode_owner.main()
    assert done.value.code == 0 and called == [True]
