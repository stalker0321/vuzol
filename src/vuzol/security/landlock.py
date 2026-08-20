"""Landlock confinement for untrusted preview runtime processes.

This module is intentionally standard-library-only so it can run as a
standalone pre-exec confinement wrapper in front of project code:

    python .../vuzol/security/landlock.py '<json-spec>' -- <argv...>

The wrapper applies a Landlock domain where the listed paths are readable
and exactly the declared per-run runtime directories are writable, then
execs the target command.  When confinement cannot be applied the wrapper
exits with ``EXIT_CONFINEMENT_FAILED`` before executing anything: preview
confinement fails closed, it never degrades to an unconstrained process.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import sys
import sysconfig
from collections.abc import Sequence

# x86_64 and arm64 share the Landlock syscall numbers.
_LANDLOCK_CREATE_RULESET = 444
_LANDLOCK_ADD_RULE = 445
_LANDLOCK_RESTRICT_SELF = 446
_LANDLOCK_GET_ABI_VERSION = 1
_LANDLOCK_RULE_PATH_BENEATH = 1
_PR_SET_NO_NEW_PRIVS = 38

ACCESS_FS_EXECUTE = 1 << 0
ACCESS_FS_WRITE_FILE = 1 << 1
ACCESS_FS_READ_FILE = 1 << 2
ACCESS_FS_READ_DIR = 1 << 3
ACCESS_FS_REMOVE_DIR = 1 << 4
ACCESS_FS_REMOVE_FILE = 1 << 5
ACCESS_FS_MAKE_CHAR = 1 << 6
ACCESS_FS_MAKE_DIR = 1 << 7
ACCESS_FS_MAKE_REG = 1 << 8
ACCESS_FS_MAKE_SOCK = 1 << 9
ACCESS_FS_MAKE_FIFO = 1 << 10
ACCESS_FS_MAKE_BLOCK = 1 << 11
ACCESS_FS_MAKE_SYM = 1 << 12
ACCESS_FS_REFER = 1 << 13
ACCESS_FS_TRUNCATE = 1 << 14
ACCESS_FS_IOCTL_DEV = 1 << 15

_ALL_FS_ACCESS = (1 << 16) - 1
_READ_ONLY_ACCESS = (
    ACCESS_FS_EXECUTE | ACCESS_FS_READ_FILE | ACCESS_FS_READ_DIR | ACCESS_FS_IOCTL_DEV
)

EXIT_OK = 0
EXIT_BAD_SPEC = 97
EXIT_CONFINEMENT_FAILED = 98


class LandlockUnavailable(RuntimeError):
    """The kernel refused to create or apply the requested Landlock domain."""


class _RulesetAttr(ctypes.Structure):
    _fields_ = (
        ("handled_access_fs", ctypes.c_uint64),
        # Network access stays unhandled so the domain does not restrict sockets.
        ("handled_access_net", ctypes.c_uint64),
    )


class _PathBeneathAttr(ctypes.Structure):
    _pack_ = 1
    _fields_ = (
        ("allowed_access", ctypes.c_uint64),
        ("parent_fd", ctypes.c_int32),
    )


def _load_libc() -> ctypes.CDLL:
    name = ctypes.util.find_library("c")
    library = ctypes.CDLL(name if name else None, use_errno=True)
    library.syscall.restype = ctypes.c_long
    return library


def _syscall(libc: ctypes.CDLL, number: int, *args: object) -> int:
    return int(libc.syscall(number, *args))


def landlock_abi_version() -> int:
    """Return the kernel Landlock ABI version, or 0 when unavailable."""
    try:
        libc = _load_libc()
    except OSError:
        return 0
    version = _syscall(libc, _LANDLOCK_CREATE_RULESET, None, 0, _LANDLOCK_GET_ABI_VERSION)
    return version if version > 0 else 0


def _add_rule(libc: ctypes.CDLL, ruleset_fd: int, path: str, access: int) -> None:
    try:
        flags = os.O_PATH | os.O_DIRECTORY
        if not os.path.isdir(path):
            flags = os.O_PATH
        parent_fd = os.open(path, flags)
    except OSError as error:
        raise LandlockUnavailable(f"confinement path {path!r} is not openable: {error}") from error
    try:
        rule = _PathBeneathAttr(allowed_access=access, parent_fd=parent_fd)
        result = _syscall(
            libc,
            _LANDLOCK_ADD_RULE,
            ruleset_fd,
            _LANDLOCK_RULE_PATH_BENEATH,
            ctypes.byref(rule),
            0,
        )
        if result != 0:
            errno = ctypes.get_errno()
            raise LandlockUnavailable(f"landlock_add_rule failed for {path!r}: errno {errno}")
    finally:
        os.close(parent_fd)


def apply_confinement(
    read_only: Sequence[str],
    read_write: Sequence[str],
    extra_rules: Sequence[tuple[str, int]] = (),
) -> None:
    """Restrict the calling thread to the declared paths; inherited across exec."""
    libc = _load_libc()
    attr = _RulesetAttr(handled_access_fs=_ALL_FS_ACCESS, handled_access_net=0)
    ruleset_fd = _syscall(
        libc, _LANDLOCK_CREATE_RULESET, ctypes.byref(attr), ctypes.sizeof(attr), 0
    )
    if ruleset_fd < 0:
        raise LandlockUnavailable(f"landlock_create_ruleset failed: errno {ctypes.get_errno()}")
    try:
        for path in read_only:
            _add_rule(libc, ruleset_fd, path, _READ_ONLY_ACCESS)
        for path, access in extra_rules:
            _add_rule(libc, ruleset_fd, path, access)
        for path in read_write:
            _add_rule(libc, ruleset_fd, path, _ALL_FS_ACCESS)
        if int(libc.prctl(_PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) != 0:
            raise LandlockUnavailable("prctl(PR_SET_NO_NEW_PRIVS) failed")
        if _syscall(libc, _LANDLOCK_RESTRICT_SELF, ruleset_fd, 0) != 0:
            raise LandlockUnavailable(f"landlock_restrict_self failed: errno {ctypes.get_errno()}")
    finally:
        os.close(ruleset_fd)


def interpreter_read_only() -> list[tuple[str, int]]:
    """Interpreter and system paths every confined child needs to read."""
    candidates = [
        "/usr",
        "/etc",
        "/sys",
        sys.prefix,
        os.path.dirname(os.path.realpath(sys.executable)),
        sysconfig.get_path("stdlib") or "",
        sysconfig.get_path("platstdlib") or "",
        os.path.dirname(os.path.realpath(__file__)),
    ]
    rules: list[tuple[str, int]] = [
        (path, _READ_ONLY_ACCESS) for path in dict.fromkeys(candidates) if path
    ]
    device_access = ACCESS_FS_READ_FILE | ACCESS_FS_IOCTL_DEV
    rules.append(("/dev/urandom", device_access))
    rules.append(("/dev/null", device_access | ACCESS_FS_WRITE_FILE))
    return [(path, access) for path, access in rules if os.path.exists(path)]


def _read_only_from_spec(spec: object) -> list[str]:
    if not isinstance(spec, dict):
        raise ValueError("spec must be an object")
    extra = spec.get("read_only", [])
    if not isinstance(extra, list) or not all(isinstance(item, str) and item for item in extra):
        raise ValueError("read_only must be a list of non-empty strings")
    return list(extra)


def _read_write_from_spec(spec: object) -> list[str]:
    if not isinstance(spec, dict):
        raise ValueError("spec must be an object")
    declared = spec.get("read_write", [])
    if (
        not isinstance(declared, list)
        or not declared
        or not all(isinstance(item, str) and item for item in declared)
    ):
        raise ValueError("read_write must be a non-empty list of non-empty strings")
    return list(declared)


def main(argv: Sequence[str]) -> int:
    """CLI entrypoint: ``landlock.py '<json-spec>' -- <command...>``."""
    if len(argv) < 3 or argv[1] != "--":
        print(
            "usage: landlock.py '<json-spec>' -- <command...>",
            file=sys.stderr,
        )
        return EXIT_BAD_SPEC
    try:
        spec = json.loads(argv[0])
        read_only = _read_only_from_spec(spec)
        read_write = _read_write_from_spec(spec)
    except ValueError as error:
        print(f"vuzol-landlock: invalid spec: {error}", file=sys.stderr)
        return EXIT_BAD_SPEC
    command = list(argv[2:])
    if not command or not os.path.isabs(command[0]):
        print("vuzol-landlock: command must start with an absolute path", file=sys.stderr)
        return EXIT_BAD_SPEC
    defaults = interpreter_read_only()
    default_dirs = [path for path, _access in defaults if os.path.isdir(path)]
    device_rules = tuple((path, access) for path, access in defaults if not os.path.isdir(path))
    try:
        apply_confinement(
            read_only=tuple(dict.fromkeys(default_dirs + read_only)),
            read_write=tuple(read_write),
            extra_rules=device_rules,
        )
    except LandlockUnavailable as error:
        print(f"vuzol-landlock: confinement unavailable: {error}", file=sys.stderr)
        return EXIT_CONFINEMENT_FAILED
    os.execv(command[0], command)  # noqa: S606 - the wrapper exists to exec the confined binary
    return EXIT_OK  # unreachable; keeps the return type explicit


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
