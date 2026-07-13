"""Typed no-shell Git operations for worktree lifecycle."""

import asyncio
import hashlib
import os
import shutil
from pathlib import Path

from vuzol.execution.domain import GitInspection


class GitError(RuntimeError):
    """A bounded system-controlled Git operation failed."""


class LocalGit:
    def __init__(self, *, timeout_seconds: float = 30) -> None:
        self._timeout = timeout_seconds

    async def repository_identity(self, repository: Path) -> tuple[str, str | None]:
        common = await self._run(
            repository, "rev-parse", "--path-format=absolute", "--git-common-dir"
        )
        remote = await self._optional(repository, "remote", "get-url", "origin")
        identity = hashlib.sha256(common.decode().strip().encode()).hexdigest()
        return identity, remote.decode().strip() if remote else None

    async def resolve_commit(self, repository: Path, ref: str) -> str:
        value = await self._run(repository, "rev-parse", "--verify", f"{ref}^{{commit}}")
        commit = value.decode().strip()
        if len(commit) not in {40, 64} or any(char not in "0123456789abcdef" for char in commit):
            raise GitError("Git returned an invalid commit identity")
        return commit

    async def require_clean_source(self, repository: Path) -> None:
        status = await self._run(repository, "status", "--porcelain=v2", "--untracked-files=all")
        if status:
            raise GitError("source repository is dirty")
        git_dir = Path(
            (await self._run(repository, "rev-parse", "--absolute-git-dir")).decode().strip()
        )
        markers = (
            "MERGE_HEAD",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
            "rebase-apply",
            "rebase-merge",
            "sequencer",
        )
        if any((git_dir / marker).exists() for marker in markers):
            raise GitError("source repository has an in-progress Git operation")

    async def add_worktree(
        self, repository: Path, path: Path, branch: str, base_commit: str
    ) -> None:
        # A linked Git worktree stores an absolute pointer into the source repository's
        # common .git directory. Only mounting the task directory into the sandbox then
        # makes Git unusable; mounting the common directory would expose every branch,
        # other worktree metadata, remote configuration, and repository history.
        #
        # Fetch exactly the recorded base at depth one into a standalone repository.
        # The provider sees only its own branch, current tree, and one shallow base
        # commit. No source path or remote remains in its Git configuration.
        path.mkdir()  # noqa: ASYNC240 - bounded local Git preparation
        try:
            await self._run(path, "init", "--initial-branch", branch)
            await self._run(
                path,
                "fetch",
                "--depth=1",
                "--no-tags",
                str(repository),
                base_commit,
            )
            await self._run(path, "checkout", "-B", branch, "FETCH_HEAD")
            remote = await self._optional(path, "remote")
            if remote:
                raise GitError("isolated worktree unexpectedly retained a remote")
        except BaseException:
            shutil.rmtree(path, ignore_errors=True)
            raise

    async def inspect(self, worktree: Path, base_commit: str | None = None) -> GitInspection:
        head = (await self._run(worktree, "rev-parse", "HEAD")).decode().strip()
        branch = (await self._run(worktree, "branch", "--show-current")).decode().strip()
        comparison = base_commit or "HEAD"
        # Use intent-to-add so untracked/new files, deletes, renames
        # are included in evidence and patch.
        await self._run(worktree, "add", "-N", ".")
        try:
            names = await self._run(
                worktree, "diff", "--name-only", "-z", "--no-ext-diff", comparison
            )
            changed = tuple(
                item.decode("utf-8", "surrogateescape") for item in names.split(b"\0") if item
            )
            diff = await self._run(
                worktree,
                "diff",
                "--binary",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                comparison,
            )
        finally:
            # Clean intent-to-add without touching working tree contents
            await self._run(worktree, "reset")
        return GitInspection(head=head, branch=branch, changed_files=changed, diff=diff)

    async def remove_worktree(self, repository: Path, path: Path) -> None:
        if (path / ".git").is_dir():
            # Standalone shallow worktrees have no source-repository registration.
            # The containing WorktreeService performs the separately verified,
            # path-contained filesystem removal.
            return
        await self._run(repository, "worktree", "remove", "--force", str(path))

    async def _optional(self, cwd: Path, *args: str) -> bytes | None:
        try:
            return await self._run(cwd, *args)
        except GitError:
            return None

    async def _run(self, cwd: Path, *args: str) -> bytes:
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": "/nonexistent",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_PAGER": "cat",
            "GIT_OPTIONAL_LOCKS": "0",
        }
        command = (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.pager=cat",
            "-c",
            "diff.external=",
            "-c",
            "credential.helper=",
            *args,
        )
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(process.communicate(), self._timeout)
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise GitError("Git operation timed out") from error
        if process.returncode != 0:
            raise GitError(f"Git operation failed: {args[0]}")
        return stdout
