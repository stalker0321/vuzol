"""Validated receipts and sandbox exposure for managed capability toolchains."""

from __future__ import annotations

import json
import re
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from vuzol.execution.paths import contained

TOOLCHAIN_RECEIPT_SCHEMA = "capability-toolchain-receipt.v1"
TOOLCHAIN_RECEIPT = ".vuzol-toolchain.json"

_CAPABILITY_KEY = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_COMMAND = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,63}")
_ENVIRONMENT_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


class ToolchainReceiptError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ToolchainSpec:
    capability_key: str
    version: str
    archive_sha256: str
    executables: tuple[tuple[str, str], ...]
    environment: tuple[tuple[str, str], ...] = ()

    @property
    def required_paths(self) -> tuple[str, ...]:
        return tuple(path for _command, path in self.executables)

    def receipt(self) -> dict[str, object]:
        return {
            "schema_version": TOOLCHAIN_RECEIPT_SCHEMA,
            "capability_key": self.capability_key,
            "version": self.version,
            "archive_sha256": self.archive_sha256,
            "executables": {command: path for command, path in self.executables},
            "environment": {name: path for name, path in self.environment},
        }


@dataclass(frozen=True, slots=True)
class ToolchainRuntime:
    executables: tuple[tuple[str, str], ...] = ()
    environment: tuple[tuple[str, str], ...] = ()
    path_entries: tuple[str, ...] = ()


def parse_toolchain_spec(raw: object, *, expected_key: str) -> ToolchainSpec:
    if not isinstance(raw, dict) or raw.get("schema_version") != TOOLCHAIN_RECEIPT_SCHEMA:
        raise ToolchainReceiptError("toolchain receipt schema is unsupported")
    capability_key = raw.get("capability_key")
    version = raw.get("version")
    archive_sha256 = raw.get("archive_sha256")
    executables = raw.get("executables")
    environment = raw.get("environment", {})
    if capability_key != expected_key or not isinstance(capability_key, str):
        raise ToolchainReceiptError("toolchain receipt targets another capability")
    if _CAPABILITY_KEY.fullmatch(capability_key) is None:
        raise ToolchainReceiptError("toolchain capability key is unsafe")
    if not isinstance(version, str) or not version or len(version) > 100 or "\x00" in version:
        raise ToolchainReceiptError("toolchain version is invalid")
    if (
        not isinstance(archive_sha256, str)
        or len(archive_sha256) != 64
        or any(character not in "0123456789abcdef" for character in archive_sha256)
    ):
        raise ToolchainReceiptError("toolchain archive hash is invalid")
    if not isinstance(executables, dict) or not executables or len(executables) > 64:
        raise ToolchainReceiptError("toolchain executables are invalid")
    normalized_executables: list[tuple[str, str]] = []
    for command in sorted(executables):
        relative = executables[command]
        if (
            not isinstance(command, str)
            or _COMMAND.fullmatch(command) is None
            or not isinstance(relative, str)
            or not _safe_relative(relative)
        ):
            raise ToolchainReceiptError("toolchain executable mapping is unsafe")
        normalized_executables.append((command, relative))
    if not isinstance(environment, dict) or len(environment) > 32:
        raise ToolchainReceiptError("toolchain environment is invalid")
    normalized_environment: list[tuple[str, str]] = []
    for name in sorted(environment):
        relative = environment[name]
        if (
            not isinstance(name, str)
            or _ENVIRONMENT_NAME.fullmatch(name) is None
            or name in {"HOME", "PATH"}
            or not isinstance(relative, str)
            or not _safe_relative(relative)
        ):
            raise ToolchainReceiptError("toolchain environment mapping is unsafe")
        normalized_environment.append((name, relative))
    return ToolchainSpec(
        capability_key=capability_key,
        version=version,
        archive_sha256=archive_sha256,
        executables=tuple(normalized_executables),
        environment=tuple(normalized_environment),
    )


def load_installed_toolchain(root: Path, capability_key: str) -> ToolchainSpec | None:
    if _CAPABILITY_KEY.fullmatch(capability_key) is None:
        return None
    if not root.is_dir() or root.is_symlink():
        return None
    target = contained(root, root / capability_key, must_exist=False)
    if not target.is_dir() or target.is_symlink():
        return None
    receipt_path = contained(target, target / TOOLCHAIN_RECEIPT, must_exist=False)
    try:
        metadata = receipt_path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or receipt_path.is_symlink()
            or metadata.st_mode & 0o022
        ):
            return None
        raw = json.loads(receipt_path.read_text(encoding="utf-8"))
        spec = parse_toolchain_spec(raw, expected_key=capability_key)
        for _command, relative in spec.executables:
            executable = contained(target, target.joinpath(*PurePosixPath(relative).parts))
            executable_metadata = executable.lstat()
            if (
                not stat.S_ISREG(executable_metadata.st_mode)
                or executable.is_symlink()
                or not executable_metadata.st_mode & 0o111
            ):
                return None
        for _name, relative in spec.environment:
            value = contained(target, target.joinpath(*PurePosixPath(relative).parts))
            value_metadata = value.lstat()
            if value.is_symlink() or not (
                stat.S_ISREG(value_metadata.st_mode) or stat.S_ISDIR(value_metadata.st_mode)
            ):
                return None
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return spec


def toolchain_runtime(root: Path, capability_keys: tuple[str, ...]) -> ToolchainRuntime:
    executables: dict[str, str] = {}
    environment: dict[str, str] = {}
    path_entries: set[str] = set()
    for capability_key in sorted(set(capability_keys)):
        spec = load_installed_toolchain(root, capability_key)
        if spec is None:
            continue
        prefix = f"/toolchains/{capability_key}"
        for command, relative in spec.executables:
            target = f"{prefix}/{relative}"
            existing = executables.get(command)
            if existing is not None and existing != target:
                raise ToolchainReceiptError(f"duplicate managed executable: {command}")
            executables[command] = target
            path_entries.add(str(PurePosixPath(target).parent))
        for name, relative in spec.environment:
            value = f"{prefix}/{relative}"
            existing = environment.get(name)
            if existing is not None and existing != value:
                raise ToolchainReceiptError(f"duplicate managed environment variable: {name}")
            environment[name] = value
    return ToolchainRuntime(
        executables=tuple(sorted(executables.items())),
        environment=tuple(sorted(environment.items())),
        path_entries=tuple(sorted(path_entries)),
    )


def _safe_relative(value: str) -> bool:
    path = PurePosixPath(value)
    return bool(value) and not path.is_absolute() and ".." not in path.parts and "\x00" not in value
