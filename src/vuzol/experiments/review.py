"""Mechanical review signals and independent Git result verification."""

import re
import subprocess
from pathlib import Path, PurePosixPath

from pydantic import Field

from vuzol.experiments.domain import FrozenModel, WorkerResultManifest, WorkerTaskCapsule


class SuspiciousSignal(FrozenModel):
    path: str
    line: int = Field(ge=1)
    classification: str
    excerpt: str = Field(max_length=300)


class VerificationResult(FrozenModel):
    exact_base: bool
    exact_branch: bool
    commit_exists: bool
    changed_files_match: bool
    allowed_scope: bool
    gates_match: bool
    findings: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return all(
            (
                self.exact_base,
                self.exact_branch,
                self.commit_exists,
                self.changed_files_match,
                self.allowed_scope,
                self.gates_match,
            )
        )


_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("forced_success", re.compile(r"\bor\s+True\b|\bexit\s+0\b")),
    (
        "exception_swallowing",
        re.compile(r"except\s+(?:Exception|BaseException)[^:]*:\s*(?:#.*\n\s*)?pass\b"),
    ),
    ("arbitrary_skip", re.compile(r"pytest\.(?:skip|mark\.skip)|@pytest\.mark\.xfail")),
    # Coverage floors are informational (docs/TESTING.md); do not treat threshold
    # edits as sabotage. Keep signals for real quality bypasses only.
    ("ignore_added", re.compile(r"noqa|nosec|type:\s*ignore|pragma:\s*no cover")),
    (
        "broad_cleanup",
        re.compile(r"docker\s+(?:system|container|network)\s+prune|rm\s+-rf\s+[^\n]*\*"),
    ),
    ("shell_execution", re.compile(r"subprocess\.(?:run|Popen|call)\([^\n]*shell\s*=\s*True")),
    (
        "cleanup_error_assertion",
        re.compile(r"assert[^\n]+(?:not found|no such|deleted)[^\n]+\bor\b", re.I),
    ),
)


def scan_suspicious_patterns(changed_text: dict[str, str]) -> tuple[SuspiciousSignal, ...]:
    signals: list[SuspiciousSignal] = []
    for path, content in sorted(changed_text.items()):
        for classification, pattern in _PATTERNS:
            for match in pattern.finditer(content):
                line = content.count("\n", 0, match.start()) + 1
                excerpt = content.splitlines()[line - 1].strip()[:300]
                signals.append(
                    SuspiciousSignal(
                        path=path,
                        line=line,
                        classification=classification,
                        excerpt=excerpt,
                    )
                )
    return tuple(signals)


def path_is_allowed(path: str, allowed: tuple[str, ...]) -> bool:
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return False
    for raw in allowed:
        scope = PurePosixPath(raw.rstrip("/"))
        if candidate == scope or scope in candidate.parents:
            return True
    return False


class GitWorkerResultVerifier:
    def verify(
        self,
        worktree: Path,
        capsule: WorkerTaskCapsule,
        manifest: WorkerResultManifest,
    ) -> VerificationResult:
        head = self._git(worktree, "rev-parse", "HEAD")
        branch = self._git(worktree, "branch", "--show-current")
        commit_exists = self._git_ok(
            worktree, "cat-file", "-e", f"{manifest.result_commit}^{{commit}}"
        )
        actual_base = self._git(worktree, "merge-base", capsule.base_commit, manifest.result_commit)
        changed = tuple(
            sorted(
                line
                for line in self._git(
                    worktree,
                    "diff",
                    "--name-only",
                    f"{capsule.base_commit}..{manifest.result_commit}",
                ).splitlines()
                if line
            )
        )
        manifest_files = tuple(sorted(manifest.changed_files))
        expected_gates = {(gate.name, gate.command_id) for gate in capsule.required_gates}
        actual_gates = {
            (gate.name, gate.command_id) for gate in manifest.gates if gate.exit_code == 0
        }
        findings: list[str] = []
        if head != manifest.result_commit:
            findings.append("worktree HEAD differs from result commit")
        if changed != manifest_files:
            findings.append("manifest changed-file claim differs from Git")
        if not all(path_is_allowed(path, capsule.allowed_paths) for path in changed):
            findings.append("Git change exceeds allowed scope")
        if not expected_gates.issubset(actual_gates):
            findings.append("required successful gate evidence is missing")
        return VerificationResult(
            exact_base=actual_base == capsule.base_commit,
            exact_branch=branch == capsule.target_branch,
            commit_exists=commit_exists and head == manifest.result_commit,
            changed_files_match=changed == manifest_files,
            allowed_scope=all(path_is_allowed(path, capsule.allowed_paths) for path in changed),
            gates_match=expected_gates.issubset(actual_gates),
            findings=tuple(findings),
        )

    @staticmethod
    def _git(worktree: Path, *args: str) -> str:
        result = subprocess.run(  # noqa: S603
            ("/usr/bin/git", "-C", str(worktree), *args),
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    @staticmethod
    def _git_ok(worktree: Path, *args: str) -> bool:
        result = subprocess.run(  # noqa: S603
            ("/usr/bin/git", "-C", str(worktree), *args),
            check=False,
            capture_output=True,
            text=True,
        )
        return result.returncode == 0


def shadow_auto_accept(
    verification: VerificationResult,
    suspicious: tuple[SuspiciousSignal, ...],
    *,
    diff_lines: int,
    changed_file_limit: int = 8,
    changed_file_count: int,
) -> bool:
    return (
        verification.passed
        and not suspicious
        and changed_file_count <= changed_file_limit
        and diff_lines <= 800
    )
