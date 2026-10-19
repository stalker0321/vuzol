"""Branch and delegation checks that do not import workflow or identity code."""

from __future__ import annotations

from datetime import datetime

from vuzol.leadership.commands import PROTECTED_BRANCHES


def integration_branch_rejection(
    branch: str, *, prefix: str, default_branch: str | None
) -> str | None:
    """Return a reason when the branch is not an isolated managed ref."""

    if not branch.startswith(prefix) or len(branch) > 255:
        return "integration_branch_not_delegated"
    remainder = branch[len(prefix) :]
    if not remainder or remainder.startswith("/") or ".." in remainder.split("/"):
        return "integration_branch_not_delegated"
    leaf = remainder.rsplit("/", 1)[-1]
    if leaf in PROTECTED_BRANCHES or branch in PROTECTED_BRANCHES:
        return "protected_branch"
    if default_branch and (branch == default_branch or leaf == default_branch):
        return "protected_branch"
    if any(part in PROTECTED_BRANCHES for part in remainder.split("/")):
        return "protected_branch"
    return None


def delegation_is_expired(expires_at: datetime | None, *, now: datetime) -> bool:
    if expires_at is None:
        return False
    return expires_at <= now
