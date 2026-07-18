"""Scaffold gate: green only for empty/docs-only managed projects."""

from __future__ import annotations

from pathlib import Path

import pytest

from vuzol.execution.scaffold import (
    PROJECT_SCAFFOLD_MAKEFILE,
    SCAFFOLD_GATE_MARKER,
    executable_product_paths,
    makefile_has_scaffold_gate,
    path_is_docs_only,
    path_is_executable_product,
    scaffold_gate_violation,
)


def test_scaffold_makefile_contains_machine_marker() -> None:
    assert makefile_has_scaffold_gate(PROJECT_SCAFFOLD_MAKEFILE)
    assert SCAFFOLD_GATE_MARKER in PROJECT_SCAFFOLD_MAKEFILE
    assert "scaffold: no project tests yet" in PROJECT_SCAFFOLD_MAKEFILE


def test_docs_only_paths_do_not_require_real_gate() -> None:
    assert path_is_docs_only("README.md")
    assert path_is_docs_only("docs/guide.md")
    assert path_is_docs_only("CHANGELOG.md")
    assert not path_is_docs_only("src/app.py")
    assert not path_is_docs_only("pyproject.toml")


def test_executable_product_paths_detect_code_and_config() -> None:
    changed = (
        "README.md",
        "docs/note.md",
        "src/app.py",
        "package.json",
        "Makefile",
    )
    assert executable_product_paths(changed) == ("src/app.py", "package.json")
    assert path_is_executable_product("src/app.py")
    assert path_is_executable_product("pyproject.toml")
    assert not path_is_executable_product("README.md")
    assert not path_is_executable_product("Makefile")


def test_scaffold_gate_allows_docs_only_changes(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("README.md", "docs/intro.md"),
        trusted_gate_command_ids=("make test",),
    )
    assert reason is None


def test_scaffold_gate_blocks_code_without_real_gate(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("README.md", "src/app.py"),
        trusted_gate_command_ids=("make test",),
    )
    assert reason is not None
    assert "scaffold" in reason
    assert "src/app.py" in reason
    assert SCAFFOLD_GATE_MARKER in reason


def test_real_make_test_without_marker_is_allowed(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(".PHONY: test\ntest:\n\t/usr/bin/pytest -q\n")
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("src/app.py",),
        trusted_gate_command_ids=("make test",),
    )
    assert reason is None


def test_additional_trusted_gate_allows_code_with_scaffold_make_test(tmp_path: Path) -> None:
    # A second trusted gate counts as a configured real gate.
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("src/app.py",),
        trusted_gate_command_ids=("make test", "make lint"),
    )
    assert reason is None


def test_scaffold_marker_without_gates_blocks_code(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("main.go",),
        trusted_gate_command_ids=(),
    )
    assert reason is not None


def test_empty_gates_without_scaffold_marker_allow_historical_projects(tmp_path: Path) -> None:
    # No marker and no validation_commands: keep pre-scaffold behavior.
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("main.go",),
        trusted_gate_command_ids=(),
    )
    assert reason is None


@pytest.mark.anyio
async def test_result_validation_fails_scaffold_on_code_transition(
    tmp_path: Path,
) -> None:
    """Integration-style unit: handler rejects code under scaffold gate."""
    from unittest.mock import AsyncMock, MagicMock
    from uuid import uuid4

    from vuzol.config.models import CommandDefinition
    from vuzol.execution.domain import GitInspection
    from vuzol.execution.result_validation import ResultValidationError, ResultValidationHandler
    from vuzol.execution.scaffold import PROJECT_SCAFFOLD_MAKEFILE
    from vuzol.workflows.ports import CancellationContext

    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    (worktree_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    (worktree_path / "src").mkdir()
    (worktree_path / "src" / "app.py").write_text("print('hi')\n")

    project = MagicMock()
    project.repository_path = tmp_path / "repo"
    project.repository_path.mkdir()
    project.sandbox_profile = "default"
    project.validation_commands = (CommandDefinition(name="tests", argv=("make", "test")),)

    worktree = MagicMock()
    worktree.id = uuid4()
    worktree.branch = "vuzol/task-x"
    worktree.base_commit = "a" * 40
    worktree.result_commit = None
    worktree.diff_hash = None
    worktree.path = str(worktree_path)

    inspection = GitInspection(
        head=worktree.base_commit,
        branch=worktree.branch,
        changed_files=("src/app.py",),
        diff=b"diff --git a/src/app.py\n",
    )
    git = AsyncMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=inspection)

    handler = ResultValidationHandler(
        session_factory=MagicMock(),
        registries=MagicMock(),
        git=git,
        worktree_root=tmp_path,
        gate_runner=None,
        worktree_access=MagicMock(),
        artifacts=None,
    )
    # Bypass grant/load and call _validate directly with prepared objects.
    request = MagicMock()
    request.task_id = uuid4()
    request.run_id = uuid4()
    request.step_id = uuid4()
    request.lease = MagicMock(generation=1)

    from vuzol.experiments.domain import RequiredGate

    with pytest.raises(ResultValidationError, match="scaffold") as raised:
        await handler._validate(
            request,
            worktree=worktree,
            project=project,
            path=worktree_path,
            trusted_gates=(RequiredGate(name="tests", command_id="make test"),),
            cancellation=CancellationContext(),
        )
    assert raised.value.category == "validation_scaffold_gate"
