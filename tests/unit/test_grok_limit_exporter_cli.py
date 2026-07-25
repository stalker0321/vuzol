"""Unit tests for S1b Grok limit exporter CLI (fake tmp only)."""

from __future__ import annotations

import json
import os
import stat
from grp import getgrgid
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from vuzol.cli import grok_limit_exporter as cli
from vuzol.providers.grok_limit_snapshot import (
    BINDINGS_SCHEMA_VERSION,
    CODE_EXPORT_BINDINGS_INVALID,
    CODE_EXPORT_PATH_REJECTED,
    GrokLimitSnapshotError,
    principal_digest,
)


def _group_name() -> str:
    return getgrgid(os.getgid()).gr_name


def _prepare_snapshot_dir(path: Path) -> Path:
    """Create output parent as setgid 02750 owned by current uid/gid (S1a contract)."""

    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o2750)  # noqa: S103 — intentional setgid fixture for S1a dir contract
    return path


def _write_bindings(path: Path, *, root: Path, bindings: list[dict[str, Any]]) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": BINDINGS_SCHEMA_VERSION,
                "profiles_root": str(root),
                "bindings": bindings,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _seed_account(root: Path, leaf: str, *, principal: str = "prin-a") -> None:
    account = root / leaf
    logs = account / "logs"
    logs.mkdir(parents=True)
    lines = [
        json.dumps({"principal_id": principal, "msg": "AuthManager::new"}),
        json.dumps(
            {
                "msg": "billing: fetched credits config",
                "ctx": {
                    "subscriptionTier": "SuperGrok",
                    "config": {
                        "creditUsagePercent": 20.0,
                        "billingPeriodEnd": "2026-08-01T00:00:00+00:00",
                    },
                },
            }
        ),
    ]
    (account / "auth.json").write_text('{"token":"MUST_NOT_LEAK"}', encoding="utf-8")
    (logs / "unified.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _argv(
    *,
    bindings: Path,
    root: Path,
    output: Path,
    group: str | None = None,
) -> list[str]:
    args = [
        "--bindings-file",
        str(bindings),
        "--profiles-root",
        str(root),
        "--output-file",
        str(output),
        "--executor-group",
        group if group is not None else _group_name(),
    ]
    return args


def test_success_stdout_stable_json_no_leakage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _seed_account(root, "account-1")
    bindings = tmp_path / "bindings.json"
    output = _prepare_snapshot_dir(tmp_path / "out") / "grok.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "grok-sub-a", "account_leaf": "account-1"}],
    )
    code = cli.run(_argv(bindings=bindings, root=root, output=output))
    assert code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out.strip())
    assert payload == {"entry_count": 1, "status": "ok"}
    # No paths, digests, principals, tokens in process output.
    blob = captured.out + captured.err
    assert str(tmp_path) not in blob
    assert "prin-a" not in blob
    assert principal_digest("prin-a") not in blob
    assert "MUST_NOT_LEAK" not in blob
    assert "auth.json" not in blob
    assert stat.S_IMODE(output.lstat().st_mode) == 0o640


def test_env_fallbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _seed_account(root, "account-1")
    bindings = tmp_path / "bindings.json"
    snap_dir = _prepare_snapshot_dir(tmp_path / "snapdir")
    output = snap_dir / "snap.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    monkeypatch.setenv(cli.ENV_BINDINGS, str(bindings))
    monkeypatch.setenv(cli.ENV_PROFILES_ROOT, str(root))
    monkeypatch.setenv(cli.ENV_OUTPUT, str(output))
    code = cli.run(["--executor-group", _group_name()])
    assert code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["status"] == "ok"
    assert payload["entry_count"] == 1


def test_relative_path_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(bindings, root=root, bindings=[])
    code = cli.run(
        [
            "--bindings-file",
            "b.json",  # relative
            "--profiles-root",
            str(root),
            "--output-file",
            str(tmp_path / "out.json"),
            "--executor-group",
            _group_name(),
        ]
    )
    assert code == 1
    err = capsys.readouterr().err.strip()
    assert err == cli.CODE_CLI_CONFIG
    assert "b.json" not in err
    assert str(tmp_path) not in err


def test_missing_config_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.run([])
    assert code == 1
    assert capsys.readouterr().err.strip() == cli.CODE_CLI_CONFIG


def test_parser_help_and_usage_never_echo_values(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.run(["--help"]) == 0
    assert "vuzol-grok-limit-exporter" in capsys.readouterr().out

    hostile = "/private/host/path/SHOULD_NOT_ECHO"
    assert cli.run(["--unknown", hostile]) == 1
    captured = capsys.readouterr()
    assert captured.err.strip() == cli.CODE_CLI_CONFIG
    assert hostile not in captured.out + captured.err

    with pytest.raises(SystemExit) as exited:
        cli.main([])
    assert exited.value.code == 1


def test_profiles_root_mismatch(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "profiles"
    other = tmp_path / "other"
    root.mkdir()
    other.mkdir()
    _seed_account(root, "account-1")
    bindings = tmp_path / "b.json"
    output = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    code = cli.run(_argv(bindings=bindings, root=other, output=output))
    assert code == 1
    assert capsys.readouterr().err.strip() == cli.CODE_CLI_ROOT_MISMATCH
    assert not output.exists()


def test_library_error_mapping_no_exception_text(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    # Account missing → library path rejected
    bindings = tmp_path / "b.json"
    output = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "missing-leaf"}],
    )
    code = cli.run(_argv(bindings=bindings, root=root, output=output))
    assert code == 1
    err = capsys.readouterr().err.strip()
    assert err == CODE_EXPORT_PATH_REJECTED
    assert "Traceback" not in err
    assert "missing-leaf" not in err
    assert str(tmp_path) not in err


def test_bad_bindings_schema_maps_library_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    bindings.write_text('{"schema_version":"nope"}', encoding="utf-8")
    output = tmp_path / "out.json"
    code = cli.run(_argv(bindings=bindings, root=root, output=output))
    assert code == 1
    assert capsys.readouterr().err.strip() == CODE_EXPORT_BINDINGS_INVALID


def test_ownership_parameters_passed(tmp_path: Path) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _seed_account(root, "account-1")
    bindings = tmp_path / "b.json"
    output = tmp_path / "out.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    seen: dict[str, object] = {}

    def fake_export(
        bindings_file: Path,
        output_file: Path,
        *,
        now: object = None,
        expected_uid: int | None = None,
        expected_gid: int | None = None,
    ) -> int:
        del bindings_file, output_file, now
        seen["uid"] = expected_uid
        seen["gid"] = expected_gid
        return 0

    with patch.object(cli, "export_grok_limit_snapshot", side_effect=fake_export):
        # load_bindings still real — need valid bindings + matching root
        code = cli.run(_argv(bindings=bindings, root=root, output=output))
    assert code == 0
    assert seen["uid"] == os.getuid()
    assert seen["gid"] == os.getgid()


def test_unknown_group_fixed_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(bindings, root=root, bindings=[])
    output = tmp_path / "out.json"
    code = cli.run(
        _argv(
            bindings=bindings,
            root=root,
            output=output,
            group="no-such-group-vuzol-s1b-xyz",
        )
    )
    assert code == 1
    assert capsys.readouterr().err.strip() == cli.CODE_CLI_GROUP


def test_group_and_host_lookup_failures_use_fixed_codes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(GrokLimitSnapshotError) as error:
        cli._resolve_group_gid("")
    assert error.value.code == cli.CODE_CLI_GROUP

    with (
        patch.object(cli, "getgrnam", side_effect=OSError("private host detail")),
        pytest.raises(GrokLimitSnapshotError) as error,
    ):
        cli._resolve_group_gid("executor")
    assert error.value.code == cli.CODE_CLI_GROUP

    root = tmp_path / "profiles"
    root.mkdir()
    bindings = tmp_path / "b.json"
    _write_bindings(bindings, root=root, bindings=[])
    with patch.object(cli, "load_bindings", side_effect=OSError("private host detail")):
        assert cli.run(_argv(bindings=bindings, root=root, output=tmp_path / "out.json")) == 1
    captured = capsys.readouterr()
    assert captured.err.strip() == cli.CODE_CLI_CONFIG
    assert "private host detail" not in captured.out + captured.err


def test_arg_overrides_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "profiles"
    root.mkdir()
    _seed_account(root, "account-1")
    bindings = tmp_path / "b.json"
    output = _prepare_snapshot_dir(tmp_path / "gooddir") / "good.json"
    _write_bindings(
        bindings,
        root=root,
        bindings=[{"profile_id": "p1", "account_leaf": "account-1"}],
    )
    monkeypatch.setenv(cli.ENV_BINDINGS, str(tmp_path / "missing.json"))
    monkeypatch.setenv(cli.ENV_PROFILES_ROOT, str(tmp_path / "missing-root"))
    monkeypatch.setenv(cli.ENV_OUTPUT, str(tmp_path / "bad.json"))
    code = cli.run(_argv(bindings=bindings, root=root, output=output))
    assert code == 0
    assert json.loads(capsys.readouterr().out)["entry_count"] == 1
    assert output.exists()
