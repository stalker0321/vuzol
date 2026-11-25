"""Owner 2026-11-25: a new OpenCode account needs only its login for its limits to show."""

import json
import os
import sqlite3
import sys
from datetime import UTC, datetime

import pytest

from vuzol.cli.opencode_usage_export import Setup, discover
from vuzol.providers.opencode_snapshot import discovered_bindings, execution_binding

PROFILE = """
[[profiles]]
id = "pi-opencode-go-{name}"
account_id = "opencode-go-{name}"
provider = "pi"
model = "deepseek"
launch_mode = "cli"
credential_required = false
runtime_identity = "vuzol-pi-{name}"
state_directory = "/var/lib/vuzol-provider-state/pi-opencode-go-{name}"
capabilities = ["code_edit"]
concurrency_limit = 4
cost_class = "balanced"
supported_task_types = ["coding"]
enabled = {enabled}

[[profiles]]
id = "pi-opencode-go-{name}-muse"
base_profile_id = "pi-opencode-go-{name}"
model = "muse"
"""


def setup_for(tmp_path, accounts):
    registry = tmp_path / "account-profiles.toml"
    registry.write_text(
        "".join(
            PROFILE.format(name=name, enabled=str(enabled).lower())
            for name, enabled in accounts.items()
        )
    )
    return Setup(
        registry=registry,
        accounts_root=tmp_path / "logins",
        usage_root=tmp_path / "usage",
        cli_binary="/usr/local/bin/opencode-native",
        cli_version="1.18.32",
    )


def log_in(setup, account_id, *, users=("user-1",), org="org-1"):
    store = setup.accounts_root / account_id / "data/opencode/opencode.db"
    store.parent.mkdir(parents=True)
    with sqlite3.connect(store) as db:
        db.executescript(
            "CREATE TABLE account(id TEXT,url TEXT,access_token TEXT,token_expiry INTEGER);"
            "CREATE TABLE account_state(id INTEGER,active_account_id TEXT,active_org_id TEXT);"
        )
        for user in users:
            db.execute(
                "INSERT INTO account VALUES (?,'https://opencode.ai/console','t',0)", (user,)
            )
        db.execute("INSERT INTO account_state VALUES (1,?,?)", (users[0], org))


def test_each_enabled_account_with_one_login_gets_an_export(tmp_path):
    setup = setup_for(tmp_path, {"a": True, "b": True, "c": False, "d": True})
    log_in(setup, "opencode-go-a")
    log_in(setup, "opencode-go-c")  # disabled
    log_in(setup, "opencode-go-d", users=("user-1", "user-2"))  # ambiguous login
    # b has no login yet.

    (config,) = discover(setup)

    assert config.account.id == config.output.account_id == "opencode-go-a"
    assert config.output.scope_id == "opencode:organization:org-1:account:user-1"
    assert config.output.path == tmp_path / "usage/opencode-go-a/usage.json"
    assert config.xdg_root == tmp_path / "logins/opencode-go-a"
    assert config.lock_file == config.xdg_root / "data/opencode/vuzol-owner.lock"
    assert config.account.quota_oauth.store_path == config.xdg_root / "data/opencode/opencode.db"
    assert config.account.runtime_identity == "vuzol-pi-a"


def test_the_card_finds_each_snapshot_and_keeps_explicit_bindings(tmp_path):
    from vuzol.config.loader import load_document
    from vuzol.config.registries import ProfileRegistry
    from vuzol.config.settings import OpenCodeSnapshotBinding

    setup = setup_for(tmp_path, {"a": True})
    log_in(setup, "opencode-go-a")
    (config,) = discover(setup)
    profile = ProfileRegistry(load_document(setup.registry).profiles).get("pi-opencode-go-a")
    assert config.binding_digest == execution_binding(profile)
    usage = tmp_path / "usage"
    (usage / "opencode-go-a").mkdir(parents=True)
    (usage / "opencode-go-a/usage.json").write_text(json.dumps({"scope_id": "scope-a"}))
    (usage / "broken").mkdir()
    (usage / "broken/usage.json").write_text("{")
    explicit = OpenCodeSnapshotBinding(account_id="opencode-go-c", scope_id="c", path=usage / "c")

    found = discovered_bindings(usage, (explicit,))

    assert found == (
        explicit,
        OpenCodeSnapshotBinding(
            account_id="opencode-go-a", scope_id="scope-a", path=usage / "opencode-go-a/usage.json"
        ),
    )
    assert discovered_bindings(None, (explicit,)) == (explicit,)
    assert discovered_bindings(tmp_path / "missing") == ()


def test_logging_an_account_in_runs_the_cli_in_its_own_login_under_its_lock(tmp_path, monkeypatch):
    from vuzol.cli import opencode_owner

    setup = setup_for(tmp_path, {"a": True})
    setup_file = tmp_path / "setup.json"
    setup_file.write_text(setup.model_dump_json())
    tmp_path.chmod(0o700)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "owner",
            "--setup",
            str(setup_file),
            "--account",
            "opencode-go-a",
            "--",
            "console",
            "login",
        ],
    )
    seen = []

    def execve(binary, arguments, environment):
        seen.append((str(binary), arguments[1:], environment["XDG_DATA_HOME"]))
        lock = setup.accounts_root / "opencode-go-a/data/opencode/vuzol-owner.lock"
        assert any(
            os.get_inheritable(int(fd)) and os.readlink(f"/proc/self/fd/{fd}") == str(lock)
            for fd in os.listdir("/proc/self/fd")
            if fd.isdigit() and os.path.exists(f"/proc/self/fd/{fd}")
        )
        raise SystemExit(0)

    monkeypatch.setattr(os, "execve", execve)
    with pytest.raises(SystemExit):
        opencode_owner.main()
    assert seen == [
        (
            "/usr/local/bin/opencode-native",
            ["console", "login"],
            str(setup.accounts_root / "opencode-go-a/data"),
        )
    ]

    monkeypatch.setattr(sys, "argv", ["owner", "--setup", str(setup_file), "--account", "../x"])
    with pytest.raises(SystemExit) as refused:
        opencode_owner.main()
    assert refused.value.code == 1


def test_discovered_exports_publish_where_the_card_looks(tmp_path, monkeypatch):
    from vuzol.cli import opencode_usage_export
    from vuzol.providers.opencode_usage import GoUsage

    setup = setup_for(tmp_path, {"a": True})
    log_in(setup, "opencode-go-a")
    setup.usage_root.mkdir(mode=0o750)
    tmp_path.chmod(0o700)
    setup_file = tmp_path / "setup.json"
    setup_file.write_text(setup.model_dump_json())
    monkeypatch.setattr(sys, "argv", ["export", "--setup", str(setup_file)])
    monkeypatch.setattr(
        opencode_usage_export,
        "refresh_with_cli",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        opencode_usage_export.OpenCodeUsageCollector,
        "collect",
        lambda self, account: GoUsage(datetime.now(UTC), detail="usage_unavailable"),
    )
    mask = os.umask(0o077)  # the unit's
    try:
        with pytest.raises(SystemExit) as done:
            opencode_usage_export.main()
    finally:
        os.umask(mask)
    assert done.value.code == 2  # the measurement failed, the export did not
    directory = setup.usage_root / "opencode-go-a"
    assert directory.stat().st_mode & 0o777 == 0o750
    (binding,) = discovered_bindings(setup.usage_root)
    assert binding.scope_id == "opencode:organization:org-1:account:user-1"
