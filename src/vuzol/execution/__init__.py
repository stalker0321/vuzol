"""Isolated worktree and process execution boundary."""

from vuzol.execution.domain import ProcessEnvelope, SandboxSpec, WorktreeReference
from vuzol.execution.egress import AllowedConnectTarget, compile_proxy_allowlist
from vuzol.execution.git import GitError, LocalGit

__all__ = [
    "AllowedConnectTarget",
    "GitError",
    "LocalGit",
    "ProcessEnvelope",
    "SandboxSpec",
    "WorktreeReference",
    "compile_proxy_allowlist",
]
