"""Managed-project scaffold gate: green only while empty/docs-only."""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath

# Exact dedicated marker line embedded in provisioned Makefiles.
SCAFFOLD_GATE_MARKER = "vuzol-scaffold-gate: true"
_EXACT_MARKER_LINE = f"# {SCAFFOLD_GATE_MARKER}"
# Scaffold make test recipe signature (independent of the marker line).
_SCAFFOLD_TEST_ECHO = "scaffold: no project tests yet (ok)"

# Docs / non-product paths that may change while the scaffold gate remains.
_DOC_NAMES = frozenset(
    {
        "readme",
        "readme.md",
        "license",
        "license.md",
        "licence",
        "copying",
        "changelog",
        "changelog.md",
        "authors",
        "contributors",
        "code_of_conduct.md",
        "security.md",
        "makefile",
        ".gitignore",
        ".gitattributes",
        ".editorconfig",
    }
)
_DOC_SUFFIXES = frozenset({".md", ".rst", ".txt", ".adoc"})
# Structured documentation allowed under docs/ without forcing a real gate.
_DOC_TREE_SUFFIXES = _DOC_SUFFIXES | frozenset({".json", ".yaml", ".yml", ".toml"})
_DOC_ROOT_DIRS = frozenset({"docs", "doc", "documentation", "notes"})

# Dependency / tooling manifests written as .txt that are product signals.
_PRODUCT_TXT_PREFIXES = (
    "requirements",
    "constraints",
    "requirements-dev",
    "requirements-test",
)

# Pure data suffixes that do not by themselves imply executable product behavior.
_DATA_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".parquet",
        ".feather",
        ".arrow",
        ".npy",
        ".npz",
        ".pkl",
        ".pickle",
        ".xlsx",
        ".xls",
        ".ods",
    }
)
_CODE_ROOT_DIRS = frozenset(
    {"src", "app", "lib", "pkg", "cmd", "internal", "packages", "services", "bin"}
)

# Paths that imply product/executable work and require a real validation gate.
_CODE_SUFFIXES = frozenset(
    {
        ".py",
        ".pyi",
        ".js",
        ".jsx",
        ".ts",
        ".tsx",
        ".mjs",
        ".cjs",
        ".go",
        ".rs",
        ".java",
        ".kt",
        ".c",
        ".cc",
        ".cpp",
        ".h",
        ".hpp",
        ".rb",
        ".php",
        ".swift",
        ".m",
        ".cs",
        ".scala",
        ".sh",
        ".bash",
        ".zsh",
        ".ps1",
        ".sql",
        ".toml",
        ".yaml",
        ".yml",
        ".json",
        ".lock",
        ".gradle",
        ".cmake",
        ".proto",
    }
)
_CODE_NAMES = frozenset(
    {
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "package.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "cargo.toml",
        "cargo.lock",
        "go.mod",
        "go.sum",
        "gemfile",
        "gemfile.lock",
        "composer.json",
        "dockerfile",
        "compose.yaml",
        "compose.yml",
        "docker-compose.yml",
        "docker-compose.yaml",
    }
)


PROJECT_SCAFFOLD_MAKEFILE = f"""\
{_EXACT_MARKER_LINE}
# Scaffolded by Vuzol for managed projects.
# `make test` may stay green only while the project is empty or docs-only.
# When you add executable product code, replace this target with a real gate
# (for example pytest) and remove the scaffold marker line above.
# Platform testing policy: docs/TESTING.md (in the Vuzol repository).

.PHONY: test
test:
\t@echo "{_SCAFFOLD_TEST_ECHO}"
"""


def makefile_has_exact_scaffold_marker(makefile_text: str) -> bool:
    """True when a dedicated exact marker line is present (not a prose substring)."""

    return any(raw.strip() == _EXACT_MARKER_LINE for raw in makefile_text.splitlines())


def makefile_has_scaffold_gate(makefile_text: str) -> bool:
    """Backward-compatible alias: exact dedicated marker line only."""

    return makefile_has_exact_scaffold_marker(makefile_text)


def makefile_has_scaffold_test_recipe(makefile_text: str) -> bool:
    """True when the ``test`` recipe is still the scaffold no-op implementation."""

    recipe = _extract_make_target_recipe(makefile_text, "test")
    if recipe is None:
        return False
    normalized = " ".join(recipe.split())
    # Match the provisioned scaffold echo (with or without make's @ prefix).
    return _SCAFFOLD_TEST_ECHO in normalized and not _recipe_has_real_commands(recipe)


def makefile_has_real_test_recipe(makefile_text: str) -> bool:
    """True when ``make test`` runs something other than the scaffold no-op."""

    recipe = _extract_make_target_recipe(makefile_text, "test")
    if recipe is None:
        return False
    if makefile_has_scaffold_test_recipe(makefile_text):
        return False
    return _recipe_has_real_commands(recipe)


def _recipe_has_real_commands(recipe: str) -> bool:
    for line in recipe.splitlines():
        body = line.lstrip("\t ").lstrip("@-+")
        body = body.strip()
        if not body or body.startswith("#"):
            continue
        if body.startswith("echo ") or body.startswith('echo"') or body.startswith("echo'"):
            # Pure echo of the scaffold message is not a real gate.
            if _SCAFFOLD_TEST_ECHO in body:
                continue
            # Other echos alone still do not count as a real project test gate.
            continue
        return True
    return False


def _extract_make_target_recipe(makefile_text: str, target: str) -> str | None:
    """Return the recipe body for a simple Make target, or None if absent."""

    # Match "test:" or "test : ..." at line start (not .PHONY).
    pattern = re.compile(rf"^(?P<head>{re.escape(target)}\s*:)(?P<rest>[^\n]*)\n?", re.M)
    match = pattern.search(makefile_text)
    if match is None:
        return None
    lines: list[str] = []
    inline = match.group("rest").strip()
    if inline and not inline.startswith("#"):
        lines.append("\t" + inline)
    # Subsequent tab-indented lines belong to the recipe until a non-recipe line.
    end = match.end()
    remainder = makefile_text[end:]
    for line in remainder.splitlines(keepends=True):
        # Make recipes are tab-indented; allow spaces only after a tab started.
        if line.startswith("\t") or (lines and line.startswith(" ")):
            lines.append(line.rstrip("\n"))
            continue
        if line.strip() == "":
            # Blank lines can appear inside recipes; keep scanning if next is recipe.
            lines.append("")
            continue
        break
    # Drop trailing blank lines.
    while lines and lines[-1].strip() == "":
        lines.pop()
    return "\n".join(lines) if lines else ""


def path_is_docs_only(path: str) -> bool:
    """True for documentation/meta paths that do not require a real project gate."""

    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return False
    name = candidate.name.lower()
    if _is_product_txt_name(name):
        return False
    if name in _DOC_NAMES:
        return True
    if name.startswith(".") and name in {".gitignore", ".gitattributes", ".editorconfig"}:
        return True
    under_docs = any(part.lower() in _DOC_ROOT_DIRS for part in candidate.parts[:-1])
    suffix = candidate.suffix.lower()
    if under_docs:
        # Structured docs (OpenAPI JSON/YAML, TOML samples) under docs/ stay docs-only.
        return suffix in _DOC_TREE_SUFFIXES or not suffix
    return suffix in _DOC_SUFFIXES


def path_is_executable_product(path: str) -> bool:
    """True for product/source/config paths that require a real validation gate."""

    if path_is_docs_only(path):
        return False
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return True
    name = candidate.name.lower()
    # Makefile content is evaluated via scaffold recipe/marker, not as product code itself.
    if name == "makefile":
        return False
    if _is_product_txt_name(name):
        return True
    suffix = candidate.suffix.lower()
    if suffix in _DATA_SUFFIXES:
        # Pure data alone is not executable product unless under a code root.
        return any(part.lower() in _CODE_ROOT_DIRS for part in candidate.parts[:-1])
    if name in _CODE_NAMES or suffix in _CODE_SUFFIXES:
        return True
    # Unknown non-doc paths are treated conservatively as product changes.
    return True


def _is_product_txt_name(name: str) -> bool:
    if not name.endswith(".txt"):
        return False
    stem = name[: -len(".txt")]
    return any(stem == prefix or stem.startswith(f"{prefix}-") for prefix in _PRODUCT_TXT_PREFIXES)


def executable_product_paths(changed_files: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(path for path in changed_files if path_is_executable_product(path))


def worktree_uses_scaffold_gate(worktree: Path) -> bool:
    """True when the worktree still uses the scaffold make test implementation.

    Scaffold status is detected from the exact marker line **or** the scaffold
    test recipe template. Removing only the marker while leaving the no-op
    recipe does not clear scaffold status.
    """

    makefile = worktree / "Makefile"
    if not makefile.is_file():
        return False
    text = makefile.read_text(encoding="utf-8", errors="replace")
    if makefile_has_scaffold_test_recipe(text):
        return True
    # Exact marker without a real test recipe still means scaffold mode.
    return makefile_has_exact_scaffold_marker(text) and not makefile_has_real_test_recipe(text)


def project_lacks_real_validation_gate(
    *,
    worktree: Path,
    trusted_gate_command_ids: tuple[str, ...],
) -> bool:
    """True when product code must not land yet (scaffold make test still in force).

    Historical projects with no Makefile and no validation_commands are unchanged.
    A second trusted gate such as ``make lint`` never bypasses scaffold status:
    product code requires a non-scaffold ``make test`` implementation (or no
    make-test gate at all with a non-scaffold Makefile).
    """

    makefile = worktree / "Makefile"
    if not makefile.is_file():
        # Configured make test without a Makefile is incomplete for product work.
        return any(command_id == "make test" for command_id in trusted_gate_command_ids)

    text = makefile.read_text(encoding="utf-8", errors="replace")

    # Primary signal: scaffold no-op recipe still present.
    if makefile_has_scaffold_test_recipe(text):
        return True

    # Real make test recipe → product code is allowed (stale marker comment is fine;
    # only the exact dedicated marker line without a real recipe keeps scaffold mode).
    if makefile_has_real_test_recipe(text):
        return False

    # No parseable real recipe: exact marker keeps scaffold mode; otherwise historical.
    if makefile_has_exact_scaffold_marker(text):
        return True
    # make test configured but recipe missing/empty → not a real gate yet.
    return any(command_id == "make test" for command_id in trusted_gate_command_ids)


def scaffold_gate_violation(
    *,
    worktree: Path,
    changed_files: tuple[str, ...],
    trusted_gate_command_ids: tuple[str, ...],
) -> str | None:
    """Return a fail-closed reason when executable code lands without a real gate."""

    product = executable_product_paths(changed_files)
    if not product:
        return None
    if not project_lacks_real_validation_gate(
        worktree=worktree,
        trusted_gate_command_ids=trusted_gate_command_ids,
    ):
        return None
    sample = ", ".join(product[:5])
    if len(product) > 5:
        sample += ", ..."
    return (
        "executable product files changed while only the scaffold validation gate is configured "
        f"({sample}); replace the scaffold Makefile test recipe with a real test/build/smoke gate "
        f"and remove the dedicated marker line `{_EXACT_MARKER_LINE}` "
        f"(see {SCAFFOLD_GATE_MARKER})"
    )
