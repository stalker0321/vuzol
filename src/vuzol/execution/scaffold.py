"""Managed-project scaffold gate: green only while empty/docs-only."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

# Machine-readable marker embedded in provisioned Makefiles.
SCAFFOLD_GATE_MARKER = "vuzol-scaffold-gate: true"

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
_DOC_ROOT_DIRS = frozenset({"docs", "doc", "documentation", "notes"})

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
# {SCAFFOLD_GATE_MARKER}
# Scaffolded by Vuzol for managed projects.
# `make test` may stay green only while the project is empty or docs-only.
# When you add executable product code, replace this target with a real gate
# (for example pytest) and remove the scaffold marker line above.
# Platform testing policy: docs/TESTING.md (in the Vuzol repository).

.PHONY: test
test:
\t@echo "scaffold: no project tests yet (ok)"
"""


def makefile_has_scaffold_gate(makefile_text: str) -> bool:
    """Return True when the Makefile still declares the scaffold gate marker."""

    return SCAFFOLD_GATE_MARKER in makefile_text


def path_is_docs_only(path: str) -> bool:
    """True for documentation/meta paths that do not require a real project gate."""

    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return False
    name = candidate.name.lower()
    if name in _DOC_NAMES:
        return True
    if name.startswith(".") and name in {".gitignore", ".gitattributes", ".editorconfig"}:
        return True
    if any(part.lower() in _DOC_ROOT_DIRS for part in candidate.parts[:-1]):
        return candidate.suffix.lower() in _DOC_SUFFIXES or not candidate.suffix
    return candidate.suffix.lower() in _DOC_SUFFIXES


def path_is_executable_product(path: str) -> bool:
    """True for product/source/config paths that require a real validation gate."""

    if path_is_docs_only(path):
        return False
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return True
    name = candidate.name.lower()
    # Makefile content is evaluated via the scaffold marker, not as product code itself.
    if name == "makefile":
        return False
    if name in _CODE_NAMES or candidate.suffix.lower() in _CODE_SUFFIXES:
        return True
    # Unknown non-doc paths are treated conservatively as product changes.
    return True


def executable_product_paths(changed_files: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(path for path in changed_files if path_is_executable_product(path))


def worktree_uses_scaffold_gate(worktree: Path) -> bool:
    """True when the worktree Makefile still carries the scaffold marker."""

    makefile = worktree / "Makefile"
    if not makefile.is_file():
        return False
    return makefile_has_scaffold_gate(makefile.read_text(encoding="utf-8", errors="replace"))


def project_lacks_real_validation_gate(
    *,
    worktree: Path,
    trusted_gate_command_ids: tuple[str, ...],
) -> bool:
    """True when the project is still limited to the scaffold gate.

    Historical projects with no validation_commands are unchanged: empty gates are
    not treated as scaffold. Scaffold enforcement applies when the worktree still
    carries the machine marker and every configured gate is the scaffold ``make test``.
    """

    scaffold = worktree_uses_scaffold_gate(worktree)
    if not scaffold:
        return False
    if not trusted_gate_command_ids:
        # Marker present but no gates configured still blocks product code.
        return True
    for command_id in trusted_gate_command_ids:
        if command_id == "make test":
            continue
        # Any other trusted gate counts as a configured real gate.
        return False
    return True


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
        f"({sample}); replace the scaffold Makefile marker with a real test/build/smoke gate "
        f"(see {SCAFFOLD_GATE_MARKER})"
    )
