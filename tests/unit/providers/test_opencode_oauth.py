"""Synthetic SQLite + fake HTTP, never owner credentials or browser login."""

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from tests.unit.config.test_account_descriptors import descriptor, document
from tests.unit.providers.test_opencode_usage import NOW, collector, payload
from vuzol.cli.accounts import inventory
from vuzol.config.loader import load_document
from vuzol.config.models import AccountConfig
from vuzol.config.registries import (
    ConfigurationBundle,
    ProfileRegistry,
    ProjectRegistry,
    SandboxRegistry,
    TopicRegistry,
)
from vuzol.config.revision import content_revision
from vuzol.providers.account_auth import AccountAuthError
from vuzol.providers.opencode_oauth import IMPORT_MIGRATION, read_go_oauth
from vuzol.providers.opencode_usage import GO_OAUTH_USAGE_URL, parse_oauth_go_usage
from vuzol.providers.subscription_limits import collect_profile_limits, subscription_inventory


def oauth_payload(now: datetime = NOW) -> dict:
    return {
        "usage": {
            name: {
                "status": status,
                "percent": percent,
                "resetsAt": (now + timedelta(seconds=seconds)).isoformat(),
            }
            for name, status, percent, seconds in (
                ("rolling", "ok", 13.7, 3600),
                ("weekly", "rate-limited", 100, 86400),
                ("monthly", "ok", 61.2, 864000),
            )
        }
    }


def oauth_account(tmp_path: Path, *, now: datetime = NOW, identity: str = "c") -> AccountConfig:
    state = tmp_path / identity
    state.mkdir(mode=0o700)
    store = state / "opencode.db"
    with sqlite3.connect(store) as db:
        db.executescript(
            "CREATE TABLE credential(id TEXT PRIMARY KEY, integration_id TEXT, "
            "value TEXT, active INTEGER);"
            "CREATE TABLE migration(id TEXT PRIMARY KEY);"
        )
        db.execute("INSERT INTO migration VALUES (?)", (IMPORT_MIGRATION,))
        raw = {
            "type": "oauth",
            "methodID": "device",
            "access": f"synthetic-access-{identity}",
            "refresh": "never-send-this-refresh-token",
            "expires": (now.timestamp() + 3600) * 1000,
            "metadata": {
                "accountID": f"account-{identity}",
                "orgID": f"org-{identity}",
                "server": "https://opencode.ai/console",
            },
        }
        # Deliberately inactive: the desktop's current selection is NOT ours.
        db.execute("INSERT INTO credential VALUES ('selected','opencode',?,0)", (json.dumps(raw),))
        db.execute(
            "INSERT INTO credential VALUES ('desktop','opencode',?,1)",
            (json.dumps({**raw, "access": "wrong-active-account"}),),
        )
    store.chmod(0o600)
    return AccountConfig.model_validate(
        descriptor(
            tmp_path,
            id=f"go-{identity}",
            state_directory=str(state),
            quota_scope_id=f"opencode:organization:org-{identity}:account:account-{identity}",
            quota_oauth={
                "store_path": str(store),
                "credential_id": "selected",
                "account_id": f"account-{identity}",
                "org_id": f"org-{identity}",
            },
        )
    )


def mutate(binding: AccountConfig, **changes) -> None:
    with sqlite3.connect(binding.quota_oauth.store_path) as db:
        row = db.execute("SELECT value FROM credential WHERE id='selected'").fetchone()
        raw = json.loads(row[0])
        raw.update(changes)
        db.execute("UPDATE credential SET value=? WHERE id='selected'", (json.dumps(raw),))


def oauth_document(tmp_path: Path, binding: AccountConfig) -> Path:
    path = document(tmp_path)
    raw = json.loads(path.read_text())
    raw["accounts"] = [binding.model_dump(mode="json")]
    raw["profiles"][0]["account_ref"] = binding.id
    path.write_text(json.dumps(raw))
    return path


def test_new_endpoint_exact_record_org_readonly_and_no_secret_output(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)
    before = binding.quota_oauth.store_path.read_bytes()
    calls = []

    def transport(request):
        calls.append(request)
        assert str(request.url) == GO_OAUTH_USAGE_URL
        assert request.method == "GET"
        assert request.headers["authorization"] == "Bearer synthetic-access-c"
        assert request.headers["x-opencode-org-id"] == "org-c"
        return httpx.Response(200, json=oauth_payload())

    value = collector(transport).collect(binding)
    assert len(calls) == 1 and not value.detail
    assert [w.used_percent for w in value.windows] == [13.7, 100, 61.2]
    assert [w.status for w in value.windows] == ["ok", "rate-limited", "ok"]
    assert all(w.scope_id == binding.quota_scope_id for w in value.windows)
    assert value.observed_at == NOW
    assert binding.quota_oauth.store_path.read_bytes() == before
    assert "synthetic-access" not in repr(value) and "refresh-token" not in repr(value)


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"expires": 0}, "auth_expired"),
        ({"expires": None}, "auth_invalid"),
        ({"expires": True}, "auth_invalid"),
        ({"expires": float("inf")}, "auth_invalid"),
        ({"type": "key"}, "auth_invalid"),
        ({"methodID": "foreign"}, "auth_invalid"),
        ({"access": "token\r\nInjected: yes"}, "auth_invalid"),
        ({"metadata": {"accountID": "someone-else", "orgID": "org-c"}}, "auth_identity_mismatch"),
        ({"metadata": {"accountID": "account-c"}}, "auth_identity_mismatch"),
    ],
)
def test_bad_identity_expiry_and_format_never_send(tmp_path: Path, changes, expected) -> None:
    binding = oauth_account(tmp_path)
    mutate(binding, **changes)
    service = collector(lambda r: pytest.fail("invalid auth must not send HTTP"))
    result = service.collect(binding)
    assert result.detail == expected and not result.windows and result.observed_at is None


@pytest.mark.parametrize(
    "issuer",
    [
        "https://opencode.ai.attacker.example/console",
        "http://opencode.ai/console",
        "https://enterprise.example/console",
        "https://opencode.ai/zen",
        "https://user:pass@opencode.ai/console",
        "https://opencode.ai/console?redirect=evil",
        "https://opencode.ai/console#foreign",
    ],
)
def test_foreign_issuer_never_reaches_transport(tmp_path: Path, issuer: str) -> None:
    binding = oauth_account(tmp_path)
    mutate(binding, metadata={"server": issuer, "accountID": "account-c", "orgID": "org-c"})
    result = collector(lambda r: pytest.fail("foreign issuer")).collect(binding)
    assert result.detail == "auth_issuer_mismatch"


@pytest.mark.parametrize(
    "damage,expected",
    [
        ("DELETE FROM credential WHERE id='selected'", "reauth_required"),
        (
            "UPDATE credential SET value='malformed private-secret' WHERE id='selected'",
            "auth_invalid",
        ),
        ("UPDATE credential SET value=zeroblob(65537) WHERE id='selected'", "auth_invalid"),
        (
            "UPDATE credential SET integration_id='opencode-go' WHERE id='selected'",
            "auth_identity_mismatch",
        ),
        ("DELETE FROM migration", "auth_store_unsupported"),
    ],
)
def test_no_fallback_to_active_key_env_or_stale_auth(
    tmp_path: Path, monkeypatch, damage, expected
) -> None:
    binding = oauth_account(tmp_path)
    (binding.state_directory / "auth.json").write_text(
        json.dumps(
            {
                "opencode-go": {"type": "api_key", "key": "stale-legacy-key"},
            }
        )
    )
    monkeypatch.setenv("OPENCODE_API_KEY", "must-not-use")
    with sqlite3.connect(binding.quota_oauth.store_path) as db:
        db.execute(damage)
    result = collector(lambda r: pytest.fail("no fallback")).collect(binding)
    assert result.detail == expected and not result.windows
    assert "private-secret" not in repr(result)


def test_rotation_recovers_cached_401_and_logout_fences_cached_success(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)
    calls = []

    def transport(request):
        calls.append(request)
        status = 200 if request.headers["authorization"] == "Bearer rotated-token" else 401
        return httpx.Response(status, json=oauth_payload())

    service = collector(transport)
    assert service.collect(binding).detail == "reauth_required"
    assert service.collect(binding).detail == "reauth_required" and len(calls) == 1
    mutate(binding, access="rotated-token")
    assert service.collect(binding).windows and len(calls) == 2
    with sqlite3.connect(binding.quota_oauth.store_path) as db:
        db.execute("DELETE FROM credential WHERE id='selected'")
    assert service.collect(binding).detail == "reauth_required" and len(calls) == 2


def test_rotation_and_expiry_while_request_in_flight_fence_result(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)

    def transport(request):
        mutate(binding, access="rotated-in-flight")
        return httpx.Response(200, json=oauth_payload())

    service = collector(transport)
    assert service.collect(binding).detail == "auth_store_changed"
    mutate(binding, expires=NOW.timestamp() * 1000)
    assert service.collect(binding).detail == "auth_expired"


def test_wal_rotation_is_read_without_checkpoint_or_restart(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)
    writer = sqlite3.connect(binding.quota_oauth.store_path)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        mutate(binding, access="wal-token")
        assert read_go_oauth(binding.quota_oauth, now=NOW).access.get_secret_value() == "wal-token"
    finally:
        writer.close()


def test_busy_store_recovers_next_tick_without_manual_reset(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)
    service = collector(lambda r: httpx.Response(200, json=oauth_payload()))
    writer = sqlite3.connect(binding.quota_oauth.store_path)
    try:
        writer.execute("BEGIN EXCLUSIVE")
        assert service.collect(binding).detail == "auth_store_busy"
    finally:
        writer.rollback()
        writer.close()
    assert service.collect(binding).windows


def test_deep_malformed_json_is_failure_not_collector_crash(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)
    service = collector(
        lambda r: httpx.Response(
            200,
            content=b"[" * 2000 + b"0" + b"]" * 2000,
            headers={"content-type": "application/json"},
        )
    )
    result = service.collect(binding)
    assert result.detail == "usage_schema_invalid" and not result.windows


def test_two_accounts_singleflight_and_selected_refresh_only(tmp_path: Path) -> None:
    a, c = oauth_account(tmp_path, identity="a"), oauth_account(tmp_path)
    calls = []

    def transport(request):
        calls.append((request.headers["x-opencode-org-id"], request.headers["authorization"]))
        return httpx.Response(200, json=oauth_payload())

    service = collector(transport)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(service.collect, [c] * 12))
    assert all(r.windows for r in results) and calls == [("org-c", "Bearer synthetic-access-c")]
    assert service.collect(a).windows
    assert calls[-1] == ("org-a", "Bearer synthetic-access-a")


@pytest.mark.parametrize(
    "field,value",
    [
        ("percent", True),
        ("percent", "50"),
        ("percent", -1),
        ("percent", 101),
        ("percent", float("nan")),
        ("status", "unknown"),
        ("resetsAt", 123),
        ("resetsAt", "2026-10-24T01:00:00"),
        ("resetsAt", "2027-10-24T01:00:00Z"),
        ("resetsAt", "2025-10-24T01:00:00Z"),
    ],
)
def test_new_schema_failures_are_not_legacy_fallback(field, value) -> None:
    data = oauth_payload()
    data["usage"]["monthly"][field] = value
    data.update(payload())
    with pytest.raises(ValueError):
        parse_oauth_go_usage(data, observed_at=NOW, scope_id="test")


def test_missing_new_window_does_not_use_legacy_payload(tmp_path: Path) -> None:
    binding = oauth_account(tmp_path)
    data = oauth_payload()
    del data["usage"]["weekly"]
    data.update(payload())
    assert (
        collector(lambda r: httpx.Response(200, json=data)).collect(binding).detail
        == "usage_schema_invalid"
    )


def test_config_usage_binding_does_not_change_execution_pins_or_account_slots(
    tmp_path: Path,
) -> None:
    binding = oauth_account(tmp_path)
    profiles = load_document(oauth_document(tmp_path, binding)).profiles
    profile = profiles[0]
    previous = profile.model_copy(
        update={
            "account": binding.model_copy(
                update={
                    "quota_oauth": None,
                    "quota_scope_id": "legacy-meter",
                }
            )
        }
    )
    assert profile.account_binding == previous.account_binding
    assert content_revision(profile) == content_revision(previous)
    assert profile.account.auth_method == "static_api_key"
    assert profile.state_directory == binding.state_directory
    assert profile.concurrency_limit == 2 and profile.control_reserved_slots == 1
    projects = ProjectRegistry((), repository_root=tmp_path)
    bundle = ConfigurationBundle(
        profiles=ProfileRegistry((profile,)),
        projects=projects,
        topics=TopicRegistry((), projects=projects),
        sandboxes=SandboxRegistry(()),
        revision="test",
    )
    snapshot = bundle.model_copy(update={"profiles": ProfileRegistry((previous,))}).snapshot(
        profile_id=profile.id
    )
    assert bundle.evaluate(snapshot).allowed
    changed_runtime = profile.model_copy(update={"state_directory": tmp_path / "other"})
    changed_bundle = bundle.model_copy(update={"profiles": ProfileRegistry((changed_runtime,))})
    assert not changed_bundle.evaluate(snapshot).allowed
    rows = inventory(profiles, check_auth=False)
    assert len(rows) == 1 and rows[0]["quota_health"] == "not-probed"


def test_new_source_and_scope_roundtrip_inventory(tmp_path: Path, monkeypatch) -> None:
    binding = oauth_account(tmp_path)
    profiles = load_document(oauth_document(tmp_path, binding)).profiles
    monkeypatch.setattr(
        "vuzol.providers.opencode_usage.go_usage_collector",
        collector(lambda r: httpx.Response(200, json=oauth_payload())),
    )
    snap = collect_profile_limits(profiles[0], now=NOW)
    assert snap.source == "opencode_go_oauth_v1" and snap.plan.tier_id == "go"
    assert subscription_inventory(profiles, [snap], now=NOW)[0].ok
    from dataclasses import replace

    assert not subscription_inventory(
        profiles, [replace(snap, source="opencode_go_usage_v1")], now=NOW
    )[0].ok


def test_v1_selected_account_and_migration_logout_do_not_resurrect_stale_auth(
    tmp_path: Path,
) -> None:
    binding = oauth_account(tmp_path)
    oauth = binding.quota_oauth.model_copy(
        update={"store_format": "account_v1", "credential_id": None}
    )
    with sqlite3.connect(oauth.store_path) as db:
        db.execute("DELETE FROM migration")
        db.executescript(
            "CREATE TABLE account(id TEXT PRIMARY KEY,url TEXT,"
            "access_token TEXT,token_expiry INTEGER);"
            "CREATE TABLE account_state(id INTEGER PRIMARY KEY,"
            "active_account_id TEXT,active_org_id TEXT);"
        )
        db.execute(
            "INSERT INTO account VALUES ('account-c','https://opencode.ai/console','v1-token',?)",
            ((NOW.timestamp() + 3600) * 1000,),
        )
        db.execute("INSERT INTO account_state VALUES (1,'account-c','org-c')")
    assert read_go_oauth(oauth, now=NOW).access.get_secret_value() == "v1-token"
    with sqlite3.connect(oauth.store_path) as db:
        db.execute("UPDATE account_state SET active_account_id='different-desktop-account'")
    with pytest.raises(AccountAuthError, match="auth_identity_mismatch"):
        read_go_oauth(oauth, now=NOW)
    with sqlite3.connect(oauth.store_path) as db:
        db.execute("INSERT INTO migration VALUES (?)", (IMPORT_MIGRATION,))
    with pytest.raises(AccountAuthError, match="auth_store_migrated"):
        read_go_oauth(oauth, now=NOW)


@pytest.mark.parametrize(
    "change",
    [
        {"store_path": "/home/owner/.local/share/opencode/auth.json"},
        {"store_path": "relative/opencode.db"},
        {"credential_id": None},
        {"store_format": "account_v1"},
        {"org_id": "org-different"},
        {"org_id": "bad\r\nheader"},
        {"refresh_owner": "collector"},
    ],
)
def test_bad_usage_bindings_rejected_by_config(tmp_path: Path, change) -> None:
    binding = oauth_account(tmp_path)
    raw = binding.model_dump(mode="json")
    raw["quota_oauth"].update(change)
    with pytest.raises(ValueError):
        AccountConfig.model_validate(raw)


def test_world_readable_store_and_fifo_are_refused_without_http(tmp_path: Path) -> None:
    import os

    binding = oauth_account(tmp_path)
    path = binding.quota_oauth.store_path
    path.chmod(0o644)
    with pytest.raises(AccountAuthError, match="auth_permissions"):
        read_go_oauth(binding.quota_oauth, now=NOW)
    path.unlink()  # Synthetic fixture only.
    os.mkfifo(path, mode=0o600)
    with pytest.raises(AccountAuthError, match="auth_permissions"):
        read_go_oauth(binding.quota_oauth, now=NOW)


def test_explicit_personal_usage_source_is_not_runtime_home_migration(tmp_path: Path) -> None:
    raw = oauth_account(tmp_path).model_dump(mode="json")
    raw["quota_oauth"]["store_path"] = "/home/operator/explicit-c/data/opencode/opencode.db"
    selected = AccountConfig.model_validate(raw)
    assert selected.state_directory == tmp_path / "c"
    # Execution retains the canonical-state guard; only read-only usage can
    # follow an operator-selected personal login, with strict file ACLs.
    raw["state_directory"] = "/home/operator/explicit-c"
    with pytest.raises(ValueError, match="outside personal homes"):
        AccountConfig.model_validate(raw)


def test_check_is_local_and_reports_usage_identity_not_the_inference_secret(
    tmp_path: Path, monkeypatch
) -> None:
    from datetime import UTC

    binding = oauth_account(tmp_path, now=datetime.now(UTC))
    profiles = load_document(oauth_document(tmp_path, binding)).profiles
    monkeypatch.setattr(
        httpx.Client, "send", lambda *a, **kw: pytest.fail("check must not send HTTP")
    )
    rows = inventory(profiles, check_auth=True)
    assert len(rows) == 1
    assert rows[0]["usage_auth"]["account_id"] == "account-c"
    assert rows[0]["usage_auth"]["org_id"] == "org-c"
    assert rows[0]["auth_method"] == "static_api_key"
    assert rows[0]["quota_health"] == "not-probed"
    assert "synthetic-access" not in json.dumps(rows) and "refresh-token" not in json.dumps(rows)


@pytest.mark.parametrize(
    "status,expected",
    [
        (401, "reauth_required"),
        (403, "usage_permission_denied"),
        (429, "usage_rate_limited"),
        (500, "usage_unavailable"),
        (302, "usage_redirect_denied"),
    ],
)
def test_oauth_errors_have_no_fake_windows_or_endpoint_fallback(
    tmp_path: Path, status, expected
) -> None:
    binding = oauth_account(tmp_path)
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(
            status,
            text="private upstream details",
            headers={
                "location": "https://elsewhere.invalid",
                "retry-after": "150",
            },
        )

    service = collector(transport)
    result = service.collect(binding)
    assert result.detail == expected and not result.windows and result.observed_at is None
    assert len(calls) == 1 and str(calls[0].url) == GO_OAUTH_USAGE_URL
    assert "private upstream" not in repr(result)
    if status == 429:
        assert result.retry_after_seconds == 150
