"""S1a unit tests for bounded Grok limit snapshot library (fake tmp only)."""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import vuzol.providers.grok_limit_snapshot as snapshot
from vuzol.providers.grok_limit_snapshot import (
    BINDINGS_SCHEMA_VERSION,
    CODE_BINDING_MISMATCH,
    CODE_EXPORT_BINDINGS_INVALID,
    CODE_EXPORT_OWNERSHIP_FAILED,
    CODE_EXPORT_PATH_REJECTED,
    CODE_SNAPSHOT_INVALID,
    CODE_SNAPSHOT_STALE,
    CODE_SNAPSHOT_UNBOUND,
    CODE_SNAPSHOT_UNREADABLE,
    SNAPSHOT_SCHEMA_VERSION,
    GrokLimitSnapshotError,
    export_grok_limit_snapshot,
    load_bindings,
    load_grok_limit_entry,
    principal_digest,
)


def _write_bindings(
    path: Path,
    *,
    root: Path,
    bindings: list[dict[str, Any]],
    schema: str = BINDINGS_SCHEMA_VERSION,
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": schema,
                "profiles_root": str(root),
                "bindings": bindings,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _account_logs(
    root: Path,
    leaf: str,
    *,
    principal: str,
    remaining_used: float = 40.0,
    plan: str = "SuperGrok",
    extra_principals: tuple[str, ...] = (),
) -> Path:
    account = root / leaf
    logs = account / "logs"
    logs.mkdir(parents=True)
    lines = [
        json.dumps({"principal_id": principal, "msg": "AuthManager::new"}),
    ]
    for other in extra_principals:
        lines.append(json.dumps({"principal_id": other}))
    remaining = 100.0 - remaining_used
    del remaining  # remaining derived at export from used
    lines.append(
        json.dumps(
            {
                "msg": "billing: fetched credits config",
                "ctx": {
                    "subscriptionTier": plan,
                    "config": {
                        "creditUsagePercent": remaining_used,
                        "billingPeriodEnd": "2026-08-01T00:00:00+00:00",
                    },
                },
            }
        )
    )
    # auth.json present as trap — must never be opened by library
    (account / "auth.json").write_text(
        json.dumps({"token": "REAL_TOKEN_MUST_NOT_BE_READ"}),
        encoding="utf-8",
    )
    (logs / "unified.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return account


def _write_snapshot(path: Path, document: object) -> None:
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
    try:
        os.write(fd, json.dumps(document).encode())
        os.fchmod(fd, 0o640)
    finally:
        os.close(fd)


def test_b_schema_ok_export_and_load_by_profile_id(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a", remaining_used=25.0)
    bindings = tmp_path / "bindings.json"
    out = tmp_path / "snap" / "grok.json"
    out.parent.mkdir()
    digest = principal_digest("prin-a")
    _write_bindings(
        bindings,
        root=root,
        bindings=[
            {
                "profile_id": "grok-sub-a",
                "account_leaf": "account-1",
                "expected_principal_digest": digest,
            }
        ],
    )
    count = export_grok_limit_snapshot(bindings, out)
    assert count == 1
    st = out.lstat()
    assert stat.S_IMODE(st.st_mode) == 0o640
    raw = json.loads(out.read_text(encoding="utf-8"))
    assert raw["schema_version"] == SNAPSHOT_SCHEMA_VERSION
    assert set(raw.keys()) == {"schema_version", "generated_at", "entries"}
    assert "token" not in json.dumps(raw)
    assert "prin-a" not in json.dumps(raw)
    entry = raw["entries"][0]
    assert entry["profile_id"] == "grok-sub-a"
    assert entry["principal_digest"] == digest
    assert entry["remaining_percent"] == 75
    result = load_grok_limit_entry(out, "grok-sub-a")
    assert result.ok and result.entry is not None
    assert result.entry.principal_digest == digest
    assert result.entry.remaining_percent == 75


def test_b_schema_bad_ver(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(bindings, root=root, bindings=[], schema="wrong.v0")
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (lambda doc: [], CODE_EXPORT_BINDINGS_INVALID),
        (lambda doc: {**doc, "extra": True}, CODE_EXPORT_BINDINGS_INVALID),
        (
            lambda doc: {**doc, "profiles_root": "relative"},
            CODE_EXPORT_BINDINGS_INVALID,
        ),
        (lambda doc: {**doc, "profiles_root": 1}, CODE_EXPORT_BINDINGS_INVALID),
        (lambda doc: {**doc, "bindings": {}}, CODE_EXPORT_BINDINGS_INVALID),
        (lambda doc: {**doc, "bindings": [None]}, CODE_EXPORT_BINDINGS_INVALID),
        (
            lambda doc: {**doc, "bindings": [{"profile_id": "p1", "account_leaf": "a", "x": 1}]},
            CODE_EXPORT_BINDINGS_INVALID,
        ),
        (
            lambda doc: {**doc, "bindings": [{"profile_id": "p1"}]},
            CODE_EXPORT_BINDINGS_INVALID,
        ),
        (
            lambda doc: {
                **doc,
                "bindings": [{"profile_id": "bad id", "account_leaf": "a"}],
            },
            CODE_EXPORT_BINDINGS_INVALID,
        ),
        (
            lambda doc: {
                **doc,
                "bindings": [{"profile_id": "p1", "account_leaf": 1}],
            },
            CODE_EXPORT_BINDINGS_INVALID,
        ),
        (
            lambda doc: {
                **doc,
                "bindings": [
                    {
                        "profile_id": "p1",
                        "account_leaf": "a",
                        "expected_principal_digest": "bad",
                    }
                ],
            },
            CODE_EXPORT_BINDINGS_INVALID,
        ),
    ],
)
def test_b_strict_schema_rejections(tmp_path: Path, mutate: Any, expected_code: str) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    document: Any = {
        "schema_version": BINDINGS_SCHEMA_VERSION,
        "profiles_root": str(root),
        "bindings": [],
    }
    bindings = tmp_path / "bindings.json"
    bindings.write_text(json.dumps(mutate(document)), encoding="utf-8")
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == expected_code


def test_b_malformed_oversize_and_too_many(tmp_path: Path) -> None:
    bindings = tmp_path / "bindings.json"
    bindings.write_text("{", encoding="utf-8")
    with pytest.raises(GrokLimitSnapshotError):
        load_bindings(bindings)

    bindings.write_bytes(b"x" * 256_001)
    with pytest.raises(GrokLimitSnapshotError):
        load_bindings(bindings)

    root = tmp_path / "profiles"
    root.mkdir()
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": f"p{i}", "account_leaf": f"a{i}"} for i in range(65)],
    )
    with pytest.raises(GrokLimitSnapshotError):
        load_bindings(bindings)


def test_b_bindings_symlink(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    real = tmp_path / "real.json"
    _write_bindings(real, root=root, bindings=[])
    link = tmp_path / "link.json"
    link.symlink_to(real)
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(link)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


@pytest.mark.parametrize(
    "leaf",
    ["", ".", "..", "a/b", "../x", "a\\b"],
)
def test_b_leaf_rejected(tmp_path: Path, leaf: str) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": leaf}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


def test_b_dup_profile_and_leaf(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[
            {"profile_id": "p1", "account_leaf": "a1"},
            {"profile_id": "p1", "account_leaf": "a2"},
        ],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID
    _write_bindings(
        bindings,
        root=root,
        bindings=[
            {"profile_id": "p1", "account_leaf": "same"},
            {"profile_id": "p2", "account_leaf": "same"},
        ],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


def test_b_multi_and_zero_principal(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "multi", principal="prin-a", extra_principals=("prin-b",))
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "multi"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID
    assert "prin-a" not in str(err.value)
    assert "prin-b" not in str(err.value)

    empty = root / "empty"
    (empty / "logs").mkdir(parents=True)
    (empty / "logs" / "unified.jsonl").write_text("{}\n", encoding="utf-8")
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p2", "account_leaf": "empty"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


def test_b_digest_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[
            {
                "profile_id": "p1",
                "account_leaf": "account-1",
                "expected_principal_digest": "0" * 64,
            }
        ],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID
    assert "prin-a" not in str(err.value)


def test_b_no_auth_read(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    account = _account_logs(root, "account-1", principal="prin-a")
    auth = account / "auth.json"
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    real_open = Path.open

    def guarded_open(
        self: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        if self.name == "auth.json" or self.resolve() == auth.resolve():
            raise AssertionError("auth.json must not be opened")
        return real_open(
            self, mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline
        )

    with patch.object(Path, "open", guarded_open):
        export_grok_limit_snapshot(bindings, out)


def test_b_rejected_heuristic_unmapped_leaf(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    _account_logs(root, "grok-sub-orphan", principal="prin-orphan")
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "grok-sub-a", "account_leaf": "account-1"}],
    )
    export_grok_limit_snapshot(bindings, out)
    raw = json.loads(out.read_text(encoding="utf-8"))
    ids = {e["profile_id"] for e in raw["entries"]}
    assert ids == {"grok-sub-a"}
    digests = {e["principal_digest"] for e in raw["entries"]}
    assert principal_digest("prin-orphan") not in digests


def test_account_and_log_symlink_rejected(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    real = root / "real"
    _account_logs(real, "nested", principal="prin-a")
    # account is symlink
    (root / "link-acc").symlink_to(real / "nested")
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "link-acc"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_PATH_REJECTED

    # log is symlink
    acc = root / "acc"
    logs = acc / "logs"
    logs.mkdir(parents=True)
    target = tmp_path / "outside.jsonl"
    target.write_text(
        json.dumps({"principal_id": "prin-a"})
        + "\n"
        + json.dumps(
            {
                "msg": "billing: fetched credits config",
                "ctx": {"config": {"creditUsagePercent": 10}},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (logs / "unified.jsonl").symlink_to(target)
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p2", "account_leaf": "acc"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_PATH_REJECTED


@pytest.mark.parametrize("kind", ["world-writable", "oversize"])
def test_export_rejects_unsafe_or_oversized_log(tmp_path: Path, kind: str) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    account = _account_logs(root, "account-1", principal="prin-a")
    log = account / "logs" / "unified.jsonl"
    if kind == "world-writable":
        log.chmod(0o666)
    else:
        with log.open("ab") as handle:
            handle.truncate(4_000_001)
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_PATH_REJECTED
    assert not out.exists()


def test_a_publish_mode_and_no_fchown(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "dir" / "grok.json"
    out.parent.mkdir()
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    with (
        patch("os.fchown", side_effect=AssertionError("fchown forbidden")),
        patch("os.chown", side_effect=AssertionError("chown forbidden")),
    ):
        export_grok_limit_snapshot(bindings, out)
    assert stat.S_IMODE(out.lstat().st_mode) == 0o640
    # temp sibling pattern: only final remains
    leftovers = list(out.parent.glob(".grok-limit-*.tmp"))
    assert leftovers == []


def test_a_directory_contract_checked_before_write(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "snapshot" / "grok.json"
    out.parent.mkdir(mode=0o750)
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(
            bindings,
            out,
            expected_uid=out.parent.stat().st_uid,
            expected_gid=out.parent.stat().st_gid,
        )
    assert err.value.code == CODE_EXPORT_OWNERSHIP_FAILED
    assert not out.exists()

    out.parent.chmod(0o2750)
    export_grok_limit_snapshot(
        bindings,
        out,
        expected_uid=out.parent.stat().st_uid,
        expected_gid=out.parent.stat().st_gid,
    )
    assert stat.S_IMODE(out.stat().st_mode) == 0o640


def test_a_verify_uid_fail_unlinks(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out, expected_uid=-1)
    assert err.value.code == CODE_EXPORT_OWNERSHIP_FAILED
    assert not out.exists()


def test_load_rejects_symlink_and_world_modes(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    bindings = tmp_path / "b.json"
    real = tmp_path / "real.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    export_grok_limit_snapshot(bindings, real)
    link = tmp_path / "link.json"
    link.symlink_to(real)
    result = load_grok_limit_entry(link, "p1")
    assert result.code == CODE_SNAPSHOT_UNREADABLE

    os.chmod(real, 0o644)
    result = load_grok_limit_entry(real, "p1")
    assert result.code == CODE_SNAPSHOT_INVALID
    os.chmod(real, 0o640)


def test_load_stale_unbound_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "account-1", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    export_grok_limit_snapshot(bindings, out)
    past = datetime.now(UTC) + timedelta(hours=2)
    result = load_grok_limit_entry(out, "p1", now=past, max_age=timedelta(minutes=15))
    assert result.code == CODE_SNAPSHOT_STALE
    result = load_grok_limit_entry(out, "missing-id")
    assert result.code == CODE_SNAPSHOT_UNBOUND
    result = load_grok_limit_entry(out, "p1", expected_principal_digest="f" * 64)
    assert result.code == CODE_BINDING_MISMATCH


def test_load_rejects_stale_observation_and_bad_expected_owner(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    account = _account_logs(root, "account-1", principal="prin-a")
    old = datetime.now(UTC) - timedelta(hours=1)
    os.utime(account / "logs" / "unified.jsonl", (old.timestamp(), old.timestamp()))
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    export_grok_limit_snapshot(bindings, out)
    result = load_grok_limit_entry(out, "p1", max_age=timedelta(minutes=15))
    assert result.code == CODE_SNAPSHOT_STALE
    result = load_grok_limit_entry(out, "p1", expected_uid=-1)
    assert result.code == CODE_SNAPSHOT_INVALID


def test_load_oversize_and_bad_schema(tmp_path: Path) -> None:
    big = tmp_path / "big.json"
    # Create a world-safe 0640 file that is oversized for loader
    payload = b"{" + b"a" * (1_000_001)
    fd = os.open(big, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
    try:
        os.write(fd, payload)
        os.fchmod(fd, 0o640)
    finally:
        os.close(fd)
    result = load_grok_limit_entry(big, "p1")
    assert result.code in {CODE_SNAPSHOT_INVALID, CODE_SNAPSHOT_UNREADABLE}

    bad = tmp_path / "bad.json"
    fd = os.open(bad, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
    try:
        os.write(fd, b'{"schema_version":"nope","generated_at":"x","entries":[]}\n')
        os.fchmod(fd, 0o640)
    finally:
        os.close(fd)
    result = load_grok_limit_entry(bad, "p1")
    assert result.code == CODE_SNAPSHOT_INVALID


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda doc, now: [], CODE_SNAPSHOT_INVALID),
        (lambda doc, now: {**doc, "extra": True}, CODE_SNAPSHOT_INVALID),
        (
            lambda doc, now: {**doc, "schema_version": "wrong"},
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {**doc, "generated_at": "bad"},
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "generated_at": (now + timedelta(minutes=2)).isoformat(),
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "generated_at": (now - timedelta(hours=1)).isoformat(),
            },
            CODE_SNAPSHOT_STALE,
        ),
        (lambda doc, now: {**doc, "entries": {}}, CODE_SNAPSHOT_INVALID),
        (
            lambda doc, now: {**doc, "entries": doc["entries"] * 65},
            CODE_SNAPSHOT_INVALID,
        ),
        (lambda doc, now: {**doc, "entries": [None]}, CODE_SNAPSHOT_INVALID),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "extra": True}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "profile_id": "bad id"}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "principal_digest": "bad"}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "remaining_percent": True}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "remaining_percent": -1}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "plan_label": ""}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "observed_at": "bad"}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [{**doc["entries"][0], "reset_at": "bad"}],
            },
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {**doc, "entries": doc["entries"] * 2},
            CODE_SNAPSHOT_INVALID,
        ),
        (
            lambda doc, now: {
                **doc,
                "entries": [
                    {
                        **doc["entries"][0],
                        "observed_at": (now + timedelta(minutes=2)).isoformat(),
                    }
                ],
            },
            CODE_SNAPSHOT_INVALID,
        ),
    ],
)
def test_load_strict_snapshot_rejections(tmp_path: Path, mutation: Any, expected: str) -> None:
    now = datetime.now(UTC)
    document: Any = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "entries": [
            {
                "profile_id": "p1",
                "principal_digest": "a" * 64,
                "remaining_percent": 50,
                "reset_at": None,
                "plan_label": "Super",
                "observed_at": now.isoformat(),
            }
        ],
    }
    path = tmp_path / "snapshot.json"
    _write_snapshot(path, mutation(document, now))
    assert load_grok_limit_entry(path, "p1", now=now).code == expected


def test_load_rejects_bad_arguments_and_malformed_bytes(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
    try:
        os.write(fd, b"\xff")
        os.fchmod(fd, 0o640)
    finally:
        os.close(fd)
    assert load_grok_limit_entry(path, "p1").code == CODE_SNAPSHOT_INVALID
    assert load_grok_limit_entry(path, "").code == CODE_SNAPSHOT_INVALID
    assert load_grok_limit_entry(path, "p1", max_age=timedelta(0)).code == CODE_SNAPSHOT_INVALID


def test_low_level_validation_oracles(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    entry = snapshot.GrokLimitEntry(
        profile_id="p1",
        principal_digest="a" * 64,
        remaining_percent=50,
        reset_at=None,
        plan_label="Super",
        observed_at=now,
    )
    assert snapshot.verify_entry_against_digest(entry, None) is None
    assert snapshot.verify_entry_against_digest(entry, "a" * 64) is None
    assert snapshot.verify_entry_against_digest(entry, "bad") == CODE_BINDING_MISMATCH

    noisy = (
        b"x\n"
        + b"{"
        + b"x" * 100_001
        + b"}\n"
        + b"{bad\n"
        + b"[]\n"
        + json.dumps({"ctx": {"principal": "prin-a", "sub": "bad value"}}).encode()
    )
    assert snapshot._principals_from_log_bytes(noisy) == {"prin-a"}
    assert not snapshot._looks_like_principal("x")
    assert not snapshot._looks_like_principal("bad value")

    billing_noise = (
        b"billing: fetched credits config not-json\n"
        + json.dumps(["billing: fetched credits config"]).encode()
        + b"\n"
        + json.dumps({"msg": "billing: fetched credits config", "ctx": "bad"}).encode()
    )
    assert snapshot._latest_billing_from_bytes(billing_noise) is None

    for ctx in (
        {"config": "bad", "creditUsagePercent": None},
        {"creditUsagePercent": True},
        {"creditUsagePercent": float("inf")},
        {"creditUsagePercent": -1},
        {"creditUsagePercent": 101},
    ):
        with pytest.raises(GrokLimitSnapshotError):
            snapshot._normalize_billing(ctx)
    assert snapshot._normalized_plan(None) == "Super"
    assert snapshot._normalized_plan("Super Grok Heavy") == "Super Heavy"
    assert snapshot._normalized_plan("unexpected") == "Unknown"
    assert snapshot._parse_datetime("2026-07-25T00:00:00Z") is not None
    assert snapshot._parse_datetime("2026-07-25T00:00:00") is None

    oversized = tmp_path / "oversized"
    oversized.write_bytes(b"xx")
    fd = os.open(oversized, os.O_RDONLY)
    try:
        with pytest.raises(ValueError):
            snapshot._read_bounded_fd(fd, 1)
    finally:
        os.close(fd)


def test_export_path_and_missing_data_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "bindings.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "missing"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, tmp_path / "out.json")
    assert err.value.code == CODE_EXPORT_PATH_REJECTED

    account = root / "account"
    account.write_text("not a directory", encoding="utf-8")
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, tmp_path / "out.json")
    assert err.value.code == CODE_EXPORT_PATH_REJECTED

    account.unlink()
    (account / "logs").mkdir(parents=True)
    (account / "logs" / "unified.jsonl").write_text(
        json.dumps({"principal_id": "prin-a"}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, tmp_path / "out.json")
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID

    empty_bindings = tmp_path / "empty-bindings.json"
    _write_bindings(empty_bindings, root=root, bindings=[])
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(empty_bindings, Path("relative.json"))
    assert err.value.code == CODE_EXPORT_PATH_REJECTED
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(empty_bindings, tmp_path / "missing-parent" / "out.json")
    assert err.value.code == CODE_EXPORT_PATH_REJECTED


def test_profiles_root_symlink_rejected(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    real_root.mkdir()
    link_root = tmp_path / "link"
    link_root.symlink_to(real_root)
    bindings = tmp_path / "b.json"
    _write_bindings(bindings, root=link_root, bindings=[])
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_PATH_REJECTED


def test_bindings_rejects_profiles_root_that_is_file(tmp_path: Path) -> None:
    obstacle = tmp_path / "profiles"
    obstacle.write_text("not a directory", encoding="utf-8")
    bindings = tmp_path / "b.json"
    _write_bindings(bindings, root=obstacle, bindings=[])
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_PATH_REJECTED


@pytest.mark.parametrize("leaf", [".hidden", "..hidden", "a b", ".x y"])
def test_bindings_rejects_unsafe_leaf_shapes(tmp_path: Path, leaf: str) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": leaf}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


def test_export_rejects_nonregular_log(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    logs = root / "acc" / "logs"
    logs.mkdir(parents=True)
    (logs / "unified.jsonl").mkdir()
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "acc"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, out)
    assert err.value.code == CODE_EXPORT_PATH_REJECTED


def test_export_rejects_unreadable_log(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    account = _account_logs(root, "acc", principal="prin-a")
    log = account / "logs" / "unified.jsonl"
    log.chmod(0o200)
    try:
        bindings = tmp_path / "b.json"
        out = tmp_path / "out.json"
        _write_bindings(
            bindings,
            root=root,
            bindings=[{"profile_id": "p1", "account_leaf": "acc"}],
        )
        with pytest.raises(GrokLimitSnapshotError) as err:
            export_grok_limit_snapshot(bindings, out)
        assert err.value.code == CODE_EXPORT_PATH_REJECTED
    finally:
        log.chmod(0o600)


def test_scan_skips_oversized_lines_but_finds_signals(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    logs = root / "acc" / "logs"
    logs.mkdir(parents=True)
    giant = json.dumps({"pad": "y" * 150_000})
    lines = [
        json.dumps({"principal_id": "prin-a", "msg": "session"}),
        giant,
        json.dumps(
            {
                "msg": "billing: fetched credits config",
                "ctx": {"config": {"creditUsagePercent": 40}},
            }
        ),
        giant,
    ]
    log = logs / "unified.jsonl"
    log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(log, 0o600)
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[
            {
                "profile_id": "p1",
                "account_leaf": "acc",
                "expected_principal_digest": principal_digest("prin-a"),
            }
        ],
    )
    assert export_grok_limit_snapshot(bindings, out) == 1
    result = load_grok_limit_entry(out, "p1")
    assert result.ok and result.entry is not None
    assert result.entry.remaining_percent == 60


def test_export_rejects_output_parent_not_a_directory(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "acc", principal="prin-a")
    bindings = tmp_path / "b.json"
    blocker = tmp_path / "blocker"
    blocker.write_text("occupied\n", encoding="utf-8")
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "acc"}],
    )
    with pytest.raises(GrokLimitSnapshotError) as err:
        export_grok_limit_snapshot(bindings, blocker / "out.json")
    assert err.value.code == CODE_EXPORT_PATH_REJECTED


def test_export_requires_expected_parent_uid_and_gid(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "acc", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "snap" / "grok.json"
    out.parent.mkdir()
    out.parent.chmod(0o2750)
    try:
        _write_bindings(
            bindings,
            root=root,
            bindings=[{"profile_id": "p1", "account_leaf": "acc"}],
        )
        with pytest.raises(GrokLimitSnapshotError) as err:
            export_grok_limit_snapshot(bindings, out, expected_uid=os.getuid() + 1)
        assert err.value.code == CODE_EXPORT_OWNERSHIP_FAILED
        assert "uid" in str(err.value)
        with pytest.raises(GrokLimitSnapshotError) as err:
            export_grok_limit_snapshot(bindings, out, expected_gid=os.getgid() + 1)
        assert err.value.code == CODE_EXPORT_OWNERSHIP_FAILED
        assert "gid" in str(err.value)
    finally:
        out.parent.chmod(0o750)


def test_publish_failure_cleans_up_and_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "acc", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "frozen" / "grok.json"
    out.parent.mkdir()
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "acc"}],
    )
    out.parent.chmod(0o555)
    try:
        with pytest.raises(GrokLimitSnapshotError) as err:
            export_grok_limit_snapshot(bindings, out)
        assert err.value.code == CODE_EXPORT_OWNERSHIP_FAILED
        assert not out.exists()
        assert list(out.parent.glob(".grok-limit-*")) == []
    finally:
        out.parent.chmod(0o750)


def test_bindings_rejects_invalid_utf8(tmp_path: Path) -> None:
    bindings = tmp_path / "b.json"
    bindings.write_bytes(b'{"schema_version":"\xff\xfe"}')
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


def test_bindings_rejects_directory_input(tmp_path: Path) -> None:
    bindings = tmp_path / "b-dir"
    bindings.mkdir()
    with pytest.raises(GrokLimitSnapshotError) as err:
        load_bindings(bindings)
    assert err.value.code == CODE_EXPORT_BINDINGS_INVALID


def test_load_rejects_observation_ahead_of_generated_window(tmp_path: Path) -> None:
    generated = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
    document = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "entries": [
            {
                "profile_id": "p1",
                "principal_digest": "a" * 64,
                "remaining_percent": 50,
                "reset_at": None,
                "plan_label": "Super",
                "observed_at": (generated + timedelta(seconds=75)).isoformat(),
            }
        ],
    }
    path = tmp_path / "snapshot.json"
    _write_snapshot(path, document)
    result = load_grok_limit_entry(path, "p1", now=generated + timedelta(seconds=30))
    assert result.code == CODE_SNAPSHOT_INVALID


@pytest.mark.parametrize(
    "mutation",
    [
        lambda doc: {**doc, "generated_at": ""},
        lambda doc: {**doc, "entries": [{**doc["entries"][0], "observed_at": ""}]},
        lambda doc: {**doc, "entries": [{**doc["entries"][0], "reset_at": ""}]},
    ],
)
def test_load_rejects_empty_datetime_strings(tmp_path: Path, mutation: Any) -> None:
    now = datetime.now(UTC)
    document: Any = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "entries": [
            {
                "profile_id": "p1",
                "principal_digest": "a" * 64,
                "remaining_percent": 50,
                "reset_at": None,
                "plan_label": "Super",
                "observed_at": now.isoformat(),
            }
        ],
    }
    path = tmp_path / "snapshot.json"
    _write_snapshot(path, mutation(document))
    assert load_grok_limit_entry(path, "p1").code == CODE_SNAPSHOT_INVALID


def test_export_normalizes_naive_now_to_utc(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _account_logs(root, "acc", principal="prin-a")
    bindings = tmp_path / "b.json"
    out = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "acc"}],
    )
    expected = datetime.now(UTC).replace(microsecond=0)
    assert export_grok_limit_snapshot(bindings, out, now=expected.replace(tzinfo=None)) == 1
    raw = json.loads(out.read_text(encoding="utf-8"))
    generated = datetime.fromisoformat(raw["generated_at"])
    assert generated.tzinfo is not None
    assert abs(generated - expected) <= timedelta(seconds=2)
    result = load_grok_limit_entry(out, "p1")
    assert result.ok and result.entry is not None
