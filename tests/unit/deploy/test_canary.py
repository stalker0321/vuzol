"""Canary tests (split for cohesion)."""

from __future__ import annotations

from ._test_mvp_helpers import *


def test_canary_uses_running_service_without_stop_or_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module("mvp_canary", ROOT / "deploy/mvp/canary.py")
    interpreter = tmp_path / "bin" / "python"
    companion = interpreter.with_name("vuzol-experiment")
    companion.parent.mkdir()
    companion.touch(mode=0o700)
    monkeypatch.setattr(module.sys, "executable", str(interpreter))
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"experiment_id": "mvp-canary-test"}))
    calls: list[tuple[str, ...]] = []

    def fake_run(argv: tuple[str, ...]) -> str:
        calls.append(argv)
        if argv[0] == "/usr/bin/systemctl":
            return "ActiveState=active\nSubState=running\nMainPID=123\nNRestarts=0\n"
        if argv[1] == "seed":
            return '{"run_uuid":"run"}'
        return '{"runs":[{"status":"completed"}]}'

    monkeypatch.setattr(module, "_run", fake_run)
    result = module.run(request, timeout_seconds=1)
    flattened = " ".join(part for call in calls for part in call)
    assert result["excluded_from_worker_quality"] is True
    assert result["experiment_executable"] == str(companion)
    assert " stop " not in f" {flattened} "
    assert " restart " not in f" {flattened} "
    assert (
        calls.count(
            (
                "/usr/bin/systemctl",
                "show",
                "vuzol-executor.service",
                "--property=ActiveState,SubState,MainPID,NRestarts",
            )
        )
        == 2
    )
    assert all(Path(call[0]).is_absolute() for call in calls)


def test_canary_resolves_companion_beside_interpreter_and_ignores_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module("mvp_canary_resolution", ROOT / "deploy/mvp/canary.py")
    sibling = tmp_path / "venv" / "bin" / "vuzol-experiment"
    sibling.parent.mkdir(parents=True)
    sibling.touch(mode=0o700)
    unrelated = tmp_path / "unrelated" / "vuzol-experiment"
    unrelated.parent.mkdir()
    unrelated.touch(mode=0o700)
    monkeypatch.setattr(module.sys, "executable", str(sibling.with_name("python")))
    monkeypatch.setenv("PATH", "")
    assert module._experiment_cli() == sibling
    monkeypatch.setenv("PATH", str(unrelated.parent))
    assert module._experiment_cli() == sibling


def test_canary_missing_companion_fails_before_service_or_seeding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module("mvp_canary_missing", ROOT / "deploy/mvp/canary.py")
    monkeypatch.setattr(module.sys, "executable", str(tmp_path / "bin" / "python"))
    service = MagicMock()
    run = MagicMock()
    monkeypatch.setattr(module, "_service", service)
    monkeypatch.setattr(module, "_run", run)
    request = tmp_path / "request.json"
    request.write_text('{"experiment_id":"mvp-canary-test"}')
    with pytest.raises(RuntimeError, match="companion executable is absent or not executable"):
        module.run(request, timeout_seconds=1)
    service.assert_not_called()
    run.assert_not_called()


def test_canary_subprocess_uses_argv_without_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module("mvp_canary_subprocess", ROOT / "deploy/mvp/canary.py")
    invoked: dict[str, object] = {}

    def run(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        invoked.update({"argv": argv, **kwargs})
        return subprocess.CompletedProcess(argv, 0, "ok", "")

    monkeypatch.setattr(module.subprocess, "run", run)
    assert module._run(("/exact/vuzol-experiment", "seed", "/request.json")) == "ok"
    assert invoked["argv"] == ("/exact/vuzol-experiment", "seed", "/request.json")
    assert "shell" not in invoked
