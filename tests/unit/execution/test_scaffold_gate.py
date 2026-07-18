"""Scaffold gate: green only for empty/docs-only managed projects."""

from __future__ import annotations

from pathlib import Path

import pytest

from vuzol.execution.scaffold import (
    PROJECT_SCAFFOLD_MAKEFILE,
    SCAFFOLD_GATE_MARKER,
    executable_product_paths,
    makefile_has_exact_scaffold_marker,
    makefile_has_real_test_recipe,
    makefile_has_scaffold_gate,
    makefile_has_scaffold_test_recipe,
    path_is_docs_only,
    path_is_executable_product,
    project_lacks_real_validation_gate,
    scaffold_gate_violation,
    worktree_uses_scaffold_gate,
)


def test_scaffold_makefile_contains_machine_marker() -> None:
    assert makefile_has_scaffold_gate(PROJECT_SCAFFOLD_MAKEFILE)
    assert makefile_has_exact_scaffold_marker(PROJECT_SCAFFOLD_MAKEFILE)
    assert makefile_has_scaffold_test_recipe(PROJECT_SCAFFOLD_MAKEFILE)
    assert not makefile_has_real_test_recipe(PROJECT_SCAFFOLD_MAKEFILE)
    assert SCAFFOLD_GATE_MARKER in PROJECT_SCAFFOLD_MAKEFILE
    assert "scaffold: no project tests yet" in PROJECT_SCAFFOLD_MAKEFILE


def test_docs_only_paths_do_not_require_real_gate() -> None:
    assert path_is_docs_only("README.md")
    assert path_is_docs_only("docs/guide.md")
    assert path_is_docs_only("CHANGELOG.md")
    assert path_is_docs_only("docs/openapi.json")
    assert path_is_docs_only("docs/schema.yaml")
    assert not path_is_docs_only("src/app.py")
    assert not path_is_docs_only("pyproject.toml")
    assert not path_is_docs_only("requirements.txt")


def test_executable_product_paths_detect_code_and_config() -> None:
    changed = (
        "README.md",
        "docs/note.md",
        "src/app.py",
        "package.json",
        "Makefile",
        "requirements.txt",
        "data/seed.csv",
        "docs/openapi.json",
    )
    assert executable_product_paths(changed) == ("src/app.py", "package.json", "requirements.txt")
    assert path_is_executable_product("src/app.py")
    assert path_is_executable_product("pyproject.toml")
    assert path_is_executable_product("requirements.txt")
    assert path_is_executable_product("constraints.txt")
    assert not path_is_executable_product("README.md")
    assert not path_is_executable_product("Makefile")
    assert not path_is_executable_product("data/seed.csv")
    assert not path_is_executable_product("docs/openapi.json")
    assert path_is_executable_product("src/fixtures/data.csv")


def test_scaffold_gate_allows_docs_only_changes(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("README.md", "docs/intro.md", "docs/api.yaml"),
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


def test_removing_marker_alone_does_not_clear_scaffold_recipe(tmp_path: Path) -> None:
    """Finding 1: noop make test without marker must not count as a real gate."""

    (tmp_path / "Makefile").write_text(
        '.PHONY: test\ntest:\n\t@echo "scaffold: no project tests yet (ok)"\n'
    )
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("src/app.py",),
        trusted_gate_command_ids=("make test",),
    )
    assert reason is not None


def test_second_lint_gate_does_not_bypass_scaffold_make_test(tmp_path: Path) -> None:
    """Finding 2: make lint must not unlock product code under scaffold make test."""

    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("src/app.py",),
        trusted_gate_command_ids=("make test", "make lint"),
    )
    assert reason is not None


def test_real_make_test_allows_code_even_with_stale_marker_comment(tmp_path: Path) -> None:
    """Finding 3: real recipe + prose mention of the marker must not block."""

    (tmp_path / "Makefile").write_text(
        ".PHONY: test\n"
        "test:\n"
        "\t/usr/bin/pytest -q\n"
        "# note: never leave vuzol-scaffold-gate: true in production docs\n"
    )
    assert not makefile_has_exact_scaffold_marker((tmp_path / "Makefile").read_text())
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("src/app.py",),
        trusted_gate_command_ids=("make test",),
    )
    assert reason is None


def test_real_make_test_with_exact_marker_line_still_allowed(tmp_path: Path) -> None:
    """Real test recipe wins over an accidentally left dedicated marker line."""

    (tmp_path / "Makefile").write_text(
        f"# {SCAFFOLD_GATE_MARKER}\n.PHONY: test\ntest:\n\t/usr/bin/pytest -q\n"
    )
    assert makefile_has_exact_scaffold_marker((tmp_path / "Makefile").read_text())
    assert makefile_has_real_test_recipe((tmp_path / "Makefile").read_text())
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("src/app.py",),
        trusted_gate_command_ids=("make test",),
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


def test_empty_gates_without_scaffold_allow_historical_projects(tmp_path: Path) -> None:
    # No Makefile and no validation_commands: keep pre-scaffold behavior.
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("main.go",),
        trusted_gate_command_ids=(),
    )
    assert reason is None


def test_requirements_txt_is_product_under_scaffold(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=("requirements.txt",),
        trusted_gate_command_ids=("make test",),
    )
    assert reason is not None
    assert "requirements.txt" in reason


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


def test_worktree_uses_scaffold_from_recipe_or_exact_marker(tmp_path: Path) -> None:
    """worktree_uses_scaffold_gate: recipe OR exact marker (no real recipe)."""

    assert not worktree_uses_scaffold_gate(tmp_path)
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    assert worktree_uses_scaffold_gate(tmp_path)

    # Recipe alone (marker removed) still counts as scaffold.
    (tmp_path / "Makefile").write_text(
        '.PHONY: test\ntest:\n\t@echo "scaffold: no project tests yet (ok)"\n'
    )
    assert worktree_uses_scaffold_gate(tmp_path)

    # Exact marker without real recipe keeps scaffold mode.
    (tmp_path / "Makefile").write_text(
        f"# {SCAFFOLD_GATE_MARKER}\n.PHONY: test\n# no test target body\n"
    )
    assert worktree_uses_scaffold_gate(tmp_path)

    # Real recipe clears scaffold even with exact marker line present.
    (tmp_path / "Makefile").write_text(
        f"# {SCAFFOLD_GATE_MARKER}\n.PHONY: test\ntest:\n\t/usr/bin/pytest -q\n"
    )
    assert not worktree_uses_scaffold_gate(tmp_path)


def test_recipe_edge_cases_inline_blank_comments_and_echoes() -> None:
    """Exercise makefile recipe parser edge cases for real vs scaffold detection."""

    # No test target at all.
    assert not makefile_has_scaffold_test_recipe(".PHONY: lint\nlint:\n\techo ok\n")
    assert not makefile_has_real_test_recipe(".PHONY: lint\nlint:\n\techo ok\n")

    # Inline recipe on the same line as the target.
    inline = "test: /usr/bin/pytest -q\n"
    assert makefile_has_real_test_recipe(inline)
    assert not makefile_has_scaffold_test_recipe(inline)

    # Blank lines inside recipe + trailing blanks + comment-only recipe lines.
    with_blanks = "test:\n\t# prepare\n\n\t/usr/bin/pytest -q\n\n\n"
    assert makefile_has_real_test_recipe(with_blanks)

    # Non-scaffold echo alone is still not a real gate.
    other_echo = 'test:\n\t@echo "hello world"\n'
    assert not makefile_has_real_test_recipe(other_echo)
    assert not makefile_has_scaffold_test_recipe(other_echo)

    # echo without space after keyword (echo"...") is treated as echo, not real.
    packed_echo = 'test:\n\t@echo"scaffold: no project tests yet (ok)"\n'
    assert not makefile_has_real_test_recipe(packed_echo)


def test_absolute_and_parent_paths_are_not_docs_only() -> None:
    assert not path_is_docs_only("/etc/passwd")
    assert not path_is_docs_only("../escape.md")
    assert path_is_executable_product("/abs/src/app.py")
    assert path_is_executable_product("../escape.py")
    assert path_is_docs_only(".gitignore")
    assert not path_is_executable_product("Makefile")
    assert not path_is_executable_product("makefile")


def test_empty_make_test_recipe_with_configured_gate_lacks_real_gate(tmp_path: Path) -> None:
    """make test configured but empty/missing recipe is not a real product gate."""

    (tmp_path / "Makefile").write_text(".PHONY: test\ntest:\n")
    assert project_lacks_real_validation_gate(
        worktree=tmp_path,
        trusted_gate_command_ids=("make test",),
    )
    # Exact marker without recipe also keeps scaffold mode.
    (tmp_path / "Makefile").write_text(f"# {SCAFFOLD_GATE_MARKER}\n.PHONY: all\nall:\n\ttrue\n")
    assert project_lacks_real_validation_gate(
        worktree=tmp_path,
        trusted_gate_command_ids=("make test",),
    )
    # make test without any Makefile is incomplete for product work.
    bare = tmp_path / "bare"
    bare.mkdir()
    assert project_lacks_real_validation_gate(
        worktree=bare,
        trusted_gate_command_ids=("make test",),
    )


def test_scaffold_violation_lists_ellipsis_for_many_product_files(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(PROJECT_SCAFFOLD_MAKEFILE)
    many = tuple(f"src/mod{i}.py" for i in range(7))
    reason = scaffold_gate_violation(
        worktree=tmp_path,
        changed_files=many,
        trusted_gate_command_ids=("make test",),
    )
    assert reason is not None
    assert "..." in reason
    assert "src/mod0.py" in reason
