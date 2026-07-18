"""Mvp check tests (split for cohesion)."""

from __future__ import annotations

from ._test_mvp_helpers import (
    ROOT,
    Path,
    _module,
    os,
    pytest,
)


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled", "blocked"])
def test_mvp_check_rejects_active_worktree_for_terminal_run(
    status: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module(f"mvp_check_terminal_worktree_{status}", ROOT / "deploy/mvp/check.py")
    monkeypatch.setattr(
        module,
        "_run",
        lambda *_args, **_kwargs: f"terminal_active_worktree|worktree-id|run-id|{status}",
    )
    with pytest.raises(module.MvpCheckError, match=f"worktree-id\\|run-id\\|{status}"):
        module._durable_state()


def test_mvp_check_allows_active_worktree_for_running_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module("mvp_check_running_worktree", ROOT / "deploy/mvp/check.py")
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...], **_kwargs: object) -> str:
        calls.append(argv)
        return ""

    monkeypatch.setattr(module, "_run", run)
    module._durable_state()
    sql = calls[0][-1]
    assert "x.status IN ('completed', 'failed', 'cancelled', 'blocked')" in sql
    assert "w.delivery_state='active'" in sql


def test_mvp_check_rejects_terminal_reserved_budget_independent_of_experiment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module("mvp_check_terminal_budget", ROOT / "deploy/mvp/check.py")
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...], **_kwargs: object) -> str:
        calls.append(argv)
        return "terminal_reserved_budget|reservation-id|run-id|completed"

    monkeypatch.setattr(module, "_run", run)
    with pytest.raises(module.MvpCheckError, match="terminal_reserved_budget"):
        module._durable_state()
    sql = calls[0][-1]
    terminal_query = sql.split("SELECT 'terminal_reserved_budget'", 1)[1].split("UNION ALL", 1)[0]
    assert "experiment_id" not in terminal_query


def test_mvp_check_clean_durable_state_passes_and_preserves_qualification_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module("mvp_check_clean_durable", ROOT / "deploy/mvp/check.py")
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...], **_kwargs: object) -> str:
        calls.append(argv)
        return ""

    monkeypatch.setattr(module, "_run", run)
    module._durable_state()
    sql = calls[0][-1]
    assert "qualification_reserved_budget" in sql
    assert "LIKE '%qual%'" in sql
    assert "LIMIT 10" in sql


def test_mvp_check_inspects_protected_runtime_without_direct_traversal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module("mvp_check", ROOT / "deploy/mvp/check.py")
    calls: list[tuple[str, ...]] = []

    def fake_run(argv: tuple[str, ...], *, cwd: Path | None = None) -> str:
        del cwd
        calls.append(argv)
        return ""

    monkeypatch.setattr(module, "_run", fake_run)
    assert module._proxy_runtime_is_empty() is True
    assert calls == [
        (
            "sudo",
            "-n",
            "find",
            "/run/vuzol/proxy",
            "-mindepth",
            "1",
            "-maxdepth",
            "1",
            "-print",
            "-quit",
        )
    ]


def test_mvp_check_accepts_current_interpreter_prompt_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module("mvp_check_interpreter_current", ROOT / "deploy/mvp/check.py")
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...], **_kwargs: object) -> str:
        calls.append(argv)
        return "running" if argv[1] == "inspect" else module.INTERPRETER_PROMPT_VERSION

    monkeypatch.setattr(module, "_run", run)
    assert module._interpreter_prompt_version() == module.INTERPRETER_PROMPT_VERSION
    assert calls[0][-1] == module.INTERPRETER_CONTAINER
    assert calls[1][:3] == ("docker", "exec", module.INTERPRETER_CONTAINER)


def test_mvp_check_rejects_stale_interpreter_prompt_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module("mvp_check_interpreter_stale", ROOT / "deploy/mvp/check.py")
    monkeypatch.setattr(
        module,
        "_run",
        lambda argv, **_kwargs: "running" if argv[1] == "inspect" else "architecture-routing-v7",
    )
    with pytest.raises(module.MvpCheckError, match="runtime='architecture-routing-v7'"):
        module._interpreter_prompt_version()


def test_mvp_check_rejects_stopped_interpreter(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module("mvp_check_interpreter_stopped", ROOT / "deploy/mvp/check.py")
    monkeypatch.setattr(module, "_run", lambda *_args, **_kwargs: "exited")
    with pytest.raises(module.MvpCheckError, match="interpreter container is not running"):
        module._interpreter_prompt_version()


def test_mvp_check_ignores_processes_that_exit_during_proc_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module("mvp_check_proc_race", ROOT / "deploy/mvp/check.py")
    process = tmp_path / "123"
    process.mkdir()
    original_stat = Path.stat

    def disappearing_stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if path == process:
            raise FileNotFoundError(path)
        return original_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", disappearing_stat)
    assert module._is_executor_dockerd(process, os.getuid()) is False
