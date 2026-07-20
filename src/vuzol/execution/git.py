"""Typed no-shell Git operations for worktree lifecycle."""

import asyncio
import hashlib
import os
import shutil
from pathlib import Path, PurePosixPath

from vuzol.execution.domain import GitInspection
from vuzol.execution.scaffold import PROJECT_SCAFFOLD_MAKEFILE


class GitError(RuntimeError):
    """A bounded system-controlled Git operation failed."""


SYSTEM_GIT_CONFIG = (
    "core.hooksPath=/dev/null",
    "core.pager=cat",
    "diff.external=",
    "credential.helper=",
    "commit.gpgSign=false",
)


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

    async def initialize_repository(self, repository: Path, *, readme: str) -> str:
        """Create or finish one contained repository with a deterministic initial commit."""

        if repository.exists() and not repository.is_dir():  # noqa: ASYNC240
            raise GitError("project repository path is not a directory")
        repository.mkdir(parents=False, exist_ok=True)  # noqa: ASYNC240
        if not (repository / ".git").is_dir():
            if any(repository.iterdir()):  # noqa: ASYNC240
                raise GitError("project repository path is not empty")
            await self._run(repository, "init", "--initial-branch", "main")
        existing = await self._optional(repository, "rev-parse", "HEAD")
        if existing is not None:
            await self.require_clean_source(repository)
            return existing.decode().strip()
        readme_path = repository / "README.md"
        if readme_path.exists() and readme_path.read_text() != readme:
            raise GitError("unfinished project README differs from the requested project")
        makefile_path = repository / "Makefile"
        if makefile_path.exists() and makefile_path.read_text() != PROJECT_SCAFFOLD_MAKEFILE:
            raise GitError("unfinished project Makefile differs from the scaffold")
        readme_path.write_text(readme)
        makefile_path.write_text(PROJECT_SCAFFOLD_MAKEFILE)
        await self.stage_paths(repository, ("README.md", "Makefile"))
        commit = await self.create_commit(repository, "chore: initialize project")
        # Leave the managed primary tree detached so apply can CAS-update main
        # without fighting a checked-out branch on newly provisioned projects.
        await self._run(repository, "switch", "--detach", "HEAD")
        return commit

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
        names = await self._run(
            worktree,
            "diff",
            "--name-only",
            "-z",
            "--no-ext-diff",
            "--no-renames",
            comparison,
        )
        untracked = await self._run(worktree, "ls-files", "--others", "--exclude-standard", "-z")
        changed = tuple(
            sorted(
                {
                    item.decode("utf-8", "surrogateescape")
                    for item in (*names.split(b"\0"), *untracked.split(b"\0"))
                    if item
                }
            )
        )
        diff = await self._run(
            worktree,
            "diff",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--no-color",
            "--no-renames",
            comparison,
        )
        for path in (
            item.decode("utf-8", "surrogateescape") for item in untracked.split(b"\0") if item
        ):
            _require_safe_path(path)
            addition = await self._run_allowed(
                worktree,
                {0, 1},
                "diff",
                "--no-index",
                "--binary",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                "--",
                "/dev/null",
                path,
            )
            diff += addition
        return GitInspection(head=head, branch=branch, changed_files=changed, diff=diff)

    async def stage_paths(self, worktree: Path, paths: tuple[str, ...]) -> None:
        if not paths:
            raise GitError("cannot stage an empty path set")
        for path in paths:
            _require_safe_path(path)
        await self._run(worktree, "add", "--all", "--", *paths)

    async def create_commit(self, worktree: Path, message: str) -> str:
        if not message or len(message) > 200 or "\n" in message or "\r" in message:
            raise GitError("commit message is invalid")
        await self._run(
            worktree,
            "commit",
            "--no-verify",
            "--no-gpg-sign",
            "-m",
            message,
            extra_environment={
                "GIT_AUTHOR_NAME": "Vuzol Worker Finalizer",
                "GIT_AUTHOR_EMAIL": "vuzol-worker@localhost.invalid",
                "GIT_COMMITTER_NAME": "Vuzol Worker Finalizer",
                "GIT_COMMITTER_EMAIL": "vuzol-worker@localhost.invalid",
            },
        )
        return await self.resolve_commit(worktree, "HEAD")

    async def commit_parent(self, worktree: Path, commit: str) -> str:
        return await self.resolve_commit(worktree, f"{commit}^")

    async def require_no_remotes(self, worktree: Path) -> None:
        if await self._run(worktree, "remote"):
            raise GitError("isolated worktree unexpectedly has a remote")

    async def require_clean_worktree(self, worktree: Path) -> None:
        status = await self._run(worktree, "status", "--porcelain=v2", "--untracked-files=all")
        if status:
            raise GitError("finalized worktree is dirty")

    async def require_diff_check(self, worktree: Path) -> None:
        """Fail closed on conflict markers or whitespace errors in the staged index."""

        await self._run(worktree, "diff", "--cached", "--check", "--no-ext-diff", "--no-color")

    async def apply_result(
        self,
        repository: Path,
        worktree: Path,
        *,
        target_branch: str,
        expected_head: str,
        result_commit: str,
    ) -> bool:
        """Atomically advance the target ref to one verified result.

        Prefer an un-checked-out ref (``update-ref`` CAS). When the managed
        repository still has the target branch checked out — typical for a
        freshly provisioned project — require a clean tree at ``expected_head``
        and hard-reset after the CAS so the worktree stays coherent.
        """

        await self.require_clean_worktree(worktree)
        await self.require_no_remotes(worktree)
        if await self.commit_parent(worktree, result_commit) != expected_head:
            raise GitError("result commit is not a direct child of the approved base")
        target_ref = f"refs/heads/{target_branch}"
        checked_out = await self._optional(repository, "symbolic-ref", "--quiet", "HEAD")
        target_is_checked_out = (
            checked_out is not None and checked_out.decode().strip() == target_ref
        )
        current = await self.resolve_commit(repository, target_ref)
        if current == result_commit:
            if target_is_checked_out:
                if await self._tree_matches_commit(repository, result_commit):
                    return False
                if not await self._tree_matches_commit(repository, expected_head):
                    raise GitError("managed worktree diverged during apply recovery")
                await self._run(repository, "reset", "--hard", result_commit)
                if not await self._tree_matches_commit(repository, result_commit):
                    raise GitError("managed worktree did not recover to the approved result")
            return False
        if current != expected_head:
            raise GitError("target branch changed after the result was produced")
        if target_is_checked_out:
            await self.require_clean_source(repository)
        await self._run(repository, "fetch", "--no-tags", str(worktree), result_commit)
        if await self.resolve_commit(repository, "FETCH_HEAD") != result_commit:
            raise GitError("fetched result identity does not match the approval")
        await self._run(repository, "update-ref", target_ref, result_commit, expected_head)
        if await self.resolve_commit(repository, target_ref) != result_commit:
            raise GitError("target branch did not advance to the approved result")
        if target_is_checked_out:
            # Keep the primary worktree aligned with the advanced branch tip.
            await self._run(repository, "reset", "--hard", result_commit)
            if await self.resolve_commit(repository, "HEAD") != result_commit:
                raise GitError("managed worktree did not reset to the approved result")
        return True

    async def _tree_matches_commit(self, repository: Path, commit: str) -> bool:
        tracked = await self._run(
            repository,
            "diff",
            "--name-only",
            "-z",
            "--no-ext-diff",
            commit,
            "--",
        )
        untracked = await self._run(repository, "ls-files", "--others", "--exclude-standard", "-z")
        return not tracked and not untracked

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

    async def _run(
        self, cwd: Path, *args: str, extra_environment: dict[str, str] | None = None
    ) -> bytes:
        return await self._run_allowed(cwd, {0}, *args, extra_environment=extra_environment)

    async def _run_allowed(
        self,
        cwd: Path,
        allowed_returncodes: set[int],
        *args: str,
        extra_environment: dict[str, str] | None = None,
    ) -> bytes:
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": "/nonexistent",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "/bin/false",
            "SSH_ASKPASS": "/bin/false",
            "GIT_PAGER": "cat",
            "GIT_OPTIONAL_LOCKS": "0",
        }
        environment.update(extra_environment or {})
        trusted_configuration = (*SYSTEM_GIT_CONFIG, f"safe.directory={cwd}")
        configuration = tuple(value for item in trusted_configuration for value in ("-c", item))
        command = ("/usr/bin/git", *configuration, *args)
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
        if process.returncode not in allowed_returncodes:
            raise GitError(f"Git operation failed: {args[0]}")
        return stdout


def _require_safe_path(path: str) -> None:
    candidate = PurePosixPath(path)
    if not path or candidate.is_absolute() or ".." in candidate.parts:
        raise GitError("Git path is not repository-relative")
