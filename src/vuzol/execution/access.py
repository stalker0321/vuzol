"""Bounded POSIX ACL handoff for rootless sandbox worktrees."""

import asyncio
import errno
import os
import shutil
import stat
import struct
import tempfile
from dataclasses import dataclass
from pathlib import Path

from vuzol.execution.paths import contained, trusted_root

_ACL_USER = 0x0002
_ACL_USER_OBJ = 0x0001
_ACL_GROUP_OBJ = 0x0004
_ACL_OTHER = 0x0020
_ACL_ENTRY_SIZE = 8
_BATCH_SIZE = 100
_ACCESS_XATTR = "system.posix_acl_access"
_DEFAULT_XATTR = "system.posix_acl_default"


class WorktreeAccessError(RuntimeError):
    """A sandbox worktree grant could not be proven or safely revoked."""


@dataclass(frozen=True, slots=True)
class RootlessIdentity:
    namespace_pid: int
    namespace_inode: int
    sandbox_uid: int
    sandbox_gid: int
    host_uid: int
    host_gid: int


@dataclass(frozen=True, slots=True)
class _EntrySnapshot:
    mode: int
    uid: int
    gid: int
    access_acl: bytes | None
    default_acl: bytes | None


class RootlessIdentityResolver:
    """Resolve configured container IDs from the active dockerd user namespace."""

    def __init__(
        self,
        socket: Path,
        *,
        proc_root: Path = Path("/proc"),
        pid_file: Path | None = None,
    ) -> None:
        self._socket = socket
        self._proc_root = proc_root
        self._pid_file = pid_file or socket.parent / "docker.pid"

    def resolve(self, sandbox_uid: int, sandbox_gid: int) -> RootlessIdentity:
        if sandbox_uid <= 0 or sandbox_gid <= 0:
            raise WorktreeAccessError("sandbox UID and GID must be non-root identities")
        pid = self._read_daemon_pid()
        process = self._proc_root / str(pid)
        try:
            if process.stat().st_uid != os.geteuid():
                raise WorktreeAccessError("rootless daemon is not owned by the executor")
            command = (process / "cmdline").read_bytes().split(b"\0")
            namespace_inode = (process / "ns" / "user").stat().st_ino
            uid_map = _read_id_map(process / "uid_map")
            gid_map = _read_id_map(process / "gid_map")
        except OSError as error:
            raise WorktreeAccessError("active rootless namespace is unavailable") from error
        if not command or Path(os.fsdecode(command[0])).name != "dockerd":
            raise WorktreeAccessError("rootless daemon PID does not identify dockerd")
        expected_host = os.fsencode(f"--host=unix://{self._socket}")
        if expected_host not in command:
            raise WorktreeAccessError("rootless daemon does not own the configured socket")
        if _map_id(uid_map, 0) != os.geteuid() or _map_id(gid_map, 0) != os.getegid():
            raise WorktreeAccessError("rootless namespace root does not map to the executor")
        host_uid = _map_id(uid_map, sandbox_uid)
        host_gid = _map_id(gid_map, sandbox_gid)
        if host_uid == os.geteuid() or host_gid == os.getegid():
            raise WorktreeAccessError("sandbox identity unexpectedly maps to the executor")
        return RootlessIdentity(
            namespace_pid=pid,
            namespace_inode=namespace_inode,
            sandbox_uid=sandbox_uid,
            sandbox_gid=sandbox_gid,
            host_uid=host_uid,
            host_gid=host_gid,
        )

    def _read_daemon_pid(self) -> int:
        try:
            metadata = self._pid_file.lstat()
            resolved = self._pid_file.resolve(strict=True)
            value = self._pid_file.read_text(encoding="ascii").strip()
        except OSError as error:
            raise WorktreeAccessError("rootless daemon PID file is unavailable") from error
        if (
            self._pid_file.is_symlink()
            or resolved != self._pid_file
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_mode & 0o022
            or not value.isascii()
            or not value.isdecimal()
        ):
            raise WorktreeAccessError("rootless daemon PID file is unsafe")
        pid = int(value)
        if pid <= 1:
            raise WorktreeAccessError("rootless daemon PID is invalid")
        return pid


class WorktreeAccessLease:
    """Idempotently revocable grant for one exact worktree."""

    def __init__(
        self,
        manager: "WorktreeAccessManager",
        root: Path,
        identity: RootlessIdentity,
        snapshots: dict[Path, _EntrySnapshot],
        git_metadata: _EntrySnapshot,
    ) -> None:
        self._manager = manager
        self.root = root
        self.identity = identity
        self._snapshots = snapshots
        self._git_metadata = git_metadata
        self._revoked = False
        self._lock = asyncio.Lock()

    @property
    def revoked(self) -> bool:
        return self._revoked

    async def revoke(self) -> None:
        async with self._lock:
            if self._revoked:
                return
            await self._manager._revoke(self)
            self._revoked = True


class WorktreeAccessManager:
    """Grant and revoke one mapped UID's access without exposing Git metadata."""

    def __init__(self, root: Path, resolver: RootlessIdentityResolver) -> None:
        self._root = trusted_root(root, create=True)
        self._resolver = resolver
        self._executor_uid = os.geteuid()
        self._executor_gid = os.getegid()
        self._setfacl = Path("/usr/bin/setfacl")
        self._getfacl = Path("/usr/bin/getfacl")
        self._nsenter = Path("/usr/bin/nsenter")
        self._chown = Path("/usr/bin/chown")

    async def preflight(self, identities: tuple[tuple[int, int], ...]) -> None:
        for command in (self._setfacl, self._getfacl, self._nsenter, self._chown):
            _require_trusted_command(command)
        if not identities:
            raise WorktreeAccessError("no sandbox identities are configured")
        resolved = tuple(self._resolver.resolve(uid, gid) for uid, gid in identities)
        probe = Path(tempfile.mkdtemp(prefix=".acl-preflight-", dir=self._root))
        try:
            target = probe / "probe"
            target.touch(mode=0o600)
            await self._run(
                self._setfacl,
                "--physical",
                "-m",
                f"u:{resolved[0].host_uid}:rw-",
                str(target),
            )
            output = await self._run(self._getfacl, "-ncp", str(target), capture=True)
            if f"user:{resolved[0].host_uid}:rw-" not in output.splitlines():
                raise WorktreeAccessError("worktree filesystem did not retain the POSIX ACL")
            await self._run(
                self._setfacl,
                "--physical",
                "-x",
                f"u:{resolved[0].host_uid}",
                str(target),
            )
        finally:
            shutil.rmtree(probe, ignore_errors=True)

    async def grant(
        self, worktree: Path, *, sandbox_uid: int, sandbox_gid: int
    ) -> WorktreeAccessLease:
        root = contained(self._root, worktree)
        identity = self._resolver.resolve(sandbox_uid, sandbox_gid)
        paths, symlinks = _collect_entries(root)
        if symlinks:
            raise WorktreeAccessError("worktree contains a symbolic link")
        git_path = root / ".git"
        if git_path not in paths:
            raise WorktreeAccessError("worktree Git metadata is missing")
        git_metadata = _snapshot(git_path)
        ordinary = tuple(
            path for path in paths if path != git_path and git_path not in path.parents
        )
        snapshots = {path: _snapshot(path) for path in ordinary}
        if any(
            item.uid != self._executor_uid
            or item.gid != self._executor_gid
            or _acl_has_named_user(item.access_acl, identity.host_uid)
            or _acl_has_named_user(item.default_acl, identity.host_uid)
            for item in snapshots.values()
        ):
            raise WorktreeAccessError("worktree ownership or pre-existing ACL is unsafe")
        if _acl_has_named_user(git_metadata.access_acl, identity.host_uid) or _acl_has_named_user(
            git_metadata.default_acl, identity.host_uid
        ):
            raise WorktreeAccessError("Git metadata already grants sandbox access")
        lease = WorktreeAccessLease(self, root, identity, snapshots, git_metadata)
        try:
            await self._grant_entries(lease)
            self._verify_git_metadata(lease)
            return lease
        except BaseException:
            await lease.revoke()
            raise

    async def _grant_entries(self, lease: WorktreeAccessLease) -> None:
        directories = tuple(
            path for path, item in lease._snapshots.items() if stat.S_ISDIR(item.mode)
        )
        regular = tuple(path for path, item in lease._snapshots.items() if stat.S_ISREG(item.mode))
        executable = tuple(
            path
            for path in regular
            if lease._snapshots[path].mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        )
        plain = tuple(path for path in regular if path not in executable)
        for batch in _batches(directories):
            await self._run(
                self._setfacl,
                "--physical",
                "-m",
                f"u:{lease.identity.host_uid}:rwx",
                "-m",
                f"d:u:{lease.identity.host_uid}:rwx",
                "-m",
                f"d:u:{self._executor_uid}:rwx",
                "--",
                *(str(path) for path in batch),
            )
        for paths, permissions in ((plain, "rw-"), (executable, "rwx")):
            for batch in _batches(paths):
                await self._run(
                    self._setfacl,
                    "--physical",
                    "-m",
                    f"u:{lease.identity.host_uid}:{permissions}",
                    "--",
                    *(str(path) for path in batch),
                )

    async def _revoke(self, lease: WorktreeAccessLease) -> None:
        current = self._resolver.resolve(lease.identity.sandbox_uid, lease.identity.sandbox_gid)
        if (
            current.host_uid != lease.identity.host_uid
            or current.host_gid != lease.identity.host_gid
        ):
            raise WorktreeAccessError("rootless identity mapping changed before ACL revocation")
        paths, symlinks = _collect_entries(lease.root)
        git_path = lease.root / ".git"
        ordinary = tuple(
            path for path in paths if path != git_path and git_path not in path.parents
        )
        reclaim = (*ordinary, *symlinks)
        for batch in _batches(reclaim):
            await self._run(
                self._nsenter,
                f"--user=/proc/{current.namespace_pid}/ns/user",
                "--preserve-credentials",
                str(self._chown),
                "--no-dereference",
                "0:0",
                "--",
                *(str(path) for path in batch),
            )
        for path in ordinary:
            snapshot = lease._snapshots.get(path)
            if snapshot is None:
                _clear_acl(path)
            else:
                _restore_snapshot(path, snapshot)
        self._verify_git_metadata(lease)
        for path in ordinary:
            metadata = path.lstat()
            if metadata.st_uid != self._executor_uid or metadata.st_gid != self._executor_gid:
                raise WorktreeAccessError("worktree ownership was not reclaimed")
            if _acl_has_named_user(_get_xattr(path, _ACCESS_XATTR), current.host_uid) or (
                _acl_has_named_user(_get_xattr(path, _DEFAULT_XATTR), current.host_uid)
            ):
                raise WorktreeAccessError("sandbox ACL remained after revocation")
        if symlinks:
            raise WorktreeAccessError("provider introduced a symbolic link")

    def _verify_git_metadata(self, lease: WorktreeAccessLease) -> None:
        git_path = lease.root / ".git"
        current = _snapshot(git_path)
        if current != lease._git_metadata:
            raise WorktreeAccessError("worktree Git metadata changed during sandbox access")
        if _acl_has_named_user(current.access_acl, lease.identity.host_uid) or _acl_has_named_user(
            current.default_acl, lease.identity.host_uid
        ):
            raise WorktreeAccessError("worktree Git metadata is writable by the sandbox")

    async def _run(self, *argv: object, capture: bool = False) -> str:
        process = await asyncio.create_subprocess_exec(
            *(str(item) for item in argv),
            stdout=asyncio.subprocess.PIPE if capture else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent"},
        )
        stdout, _stderr = await process.communicate()
        if process.returncode != 0:
            raise WorktreeAccessError(f"worktree access command failed: {Path(str(argv[0])).name}")
        return stdout.decode("utf-8", "strict") if capture else ""


def _read_id_map(path: Path) -> tuple[tuple[int, int, int], ...]:
    rows: list[tuple[int, int, int]] = []
    for line in path.read_text(encoding="ascii").splitlines():
        fields = line.split()
        if len(fields) != 3 or not all(field.isdecimal() for field in fields):
            raise WorktreeAccessError("rootless namespace ID map is malformed")
        inside, outside, length = (int(field) for field in fields)
        if length <= 0:
            raise WorktreeAccessError("rootless namespace ID map has an empty range")
        rows.append((inside, outside, length))
    if not rows:
        raise WorktreeAccessError("rootless namespace ID map is empty")
    return tuple(rows)


def _map_id(rows: tuple[tuple[int, int, int], ...], value: int) -> int:
    matches = [
        outside + value - inside
        for inside, outside, length in rows
        if inside <= value < inside + length
    ]
    if len(matches) != 1:
        raise WorktreeAccessError("configured sandbox ID has no unique rootless mapping")
    return matches[0]


def _require_trusted_command(path: Path) -> None:
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise WorktreeAccessError(f"required ACL command is unavailable: {path.name}") from error
    if (
        path.is_symlink()
        or resolved != path
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_mode & 0o022
        or not os.access(path, os.X_OK)
    ):
        raise WorktreeAccessError(f"required ACL command is unsafe: {path.name}")


def _collect_entries(root: Path) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    try:
        metadata = root.lstat()
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise WorktreeAccessError("worktree is unavailable") from error
    if root.is_symlink() or resolved != root or not stat.S_ISDIR(metadata.st_mode):
        raise WorktreeAccessError("worktree root is not a contained regular directory")
    entries: list[Path] = [root]
    symlinks: list[Path] = []
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            children = sorted(directory.iterdir(), key=lambda item: os.fsencode(item.name))
        except OSError as error:
            raise WorktreeAccessError("worktree traversal failed") from error
        for child in children:
            try:
                child_metadata = child.lstat()
            except OSError as error:
                raise WorktreeAccessError("worktree entry disappeared during traversal") from error
            if stat.S_ISLNK(child_metadata.st_mode):
                symlinks.append(child)
            elif stat.S_ISDIR(child_metadata.st_mode):
                entries.append(child)
                if child != root / ".git":
                    pending.append(child)
            elif stat.S_ISREG(child_metadata.st_mode):
                entries.append(child)
            else:
                raise WorktreeAccessError("worktree contains a non-regular entry")
    return tuple(entries), tuple(symlinks)


def _snapshot(path: Path) -> _EntrySnapshot:
    metadata = path.lstat()
    return _EntrySnapshot(
        mode=metadata.st_mode,
        uid=metadata.st_uid,
        gid=metadata.st_gid,
        access_acl=_get_xattr(path, _ACCESS_XATTR),
        default_acl=_get_xattr(path, _DEFAULT_XATTR),
    )


def _get_xattr(path: Path, name: str) -> bytes | None:
    try:
        return os.getxattr(path, name, follow_symlinks=False)
    except OSError as error:
        if error.errno in {errno.ENODATA, getattr(errno, "ENOATTR", errno.ENODATA)}:
            return None
        raise WorktreeAccessError("worktree ACL inspection failed") from error


def _restore_snapshot(path: Path, snapshot: _EntrySnapshot) -> None:
    _set_xattr(path, _ACCESS_XATTR, snapshot.access_acl)
    _set_xattr(path, _DEFAULT_XATTR, snapshot.default_acl)
    os.chmod(path, stat.S_IMODE(snapshot.mode), follow_symlinks=False)


def _clear_acl(path: Path) -> None:
    mode = _base_acl_mode(path)
    _set_xattr(path, _ACCESS_XATTR, None)
    _set_xattr(path, _DEFAULT_XATTR, None)
    os.chmod(path, mode, follow_symlinks=False)


def _base_acl_mode(path: Path) -> int:
    metadata = path.lstat()
    value = _get_xattr(path, _ACCESS_XATTR)
    if value is None:
        return stat.S_IMODE(metadata.st_mode)
    entries = _acl_entries(value)
    base = {tag: permissions for tag, permissions, _identifier in entries}
    if not {_ACL_USER_OBJ, _ACL_GROUP_OBJ, _ACL_OTHER}.issubset(base):
        raise WorktreeAccessError("worktree POSIX ACL lacks base entries")
    return (
        metadata.st_mode & 0o7000
        | base[_ACL_USER_OBJ] << 6
        | base[_ACL_GROUP_OBJ] << 3
        | base[_ACL_OTHER]
    )


def _set_xattr(path: Path, name: str, value: bytes | None) -> None:
    try:
        if value is None:
            os.removexattr(path, name, follow_symlinks=False)
        else:
            os.setxattr(path, name, value, follow_symlinks=False)
    except OSError as error:
        if value is None and error.errno in {
            errno.ENODATA,
            getattr(errno, "ENOATTR", errno.ENODATA),
        }:
            return
        raise WorktreeAccessError("worktree ACL restoration failed") from error


def _acl_has_named_user(value: bytes | None, uid: int) -> bool:
    if value is None:
        return False
    return any(
        tag == _ACL_USER and identifier == uid
        for tag, _permissions, identifier in _acl_entries(value)
    )


def _acl_entries(value: bytes) -> tuple[tuple[int, int, int], ...]:
    if len(value) < 4 or (len(value) - 4) % _ACL_ENTRY_SIZE:
        raise WorktreeAccessError("worktree POSIX ACL xattr is malformed")
    return tuple(
        struct.unpack_from("<HHI", value, offset)
        for offset in range(4, len(value), _ACL_ENTRY_SIZE)
    )


def _batches(paths: tuple[Path, ...]) -> tuple[tuple[Path, ...], ...]:
    return tuple(paths[index : index + _BATCH_SIZE] for index in range(0, len(paths), _BATCH_SIZE))
