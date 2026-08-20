"""Trusted dependency-manifest inspection and immutable environment receipts."""

from __future__ import annotations

import hashlib
import json
import re
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path

from vuzol.execution.paths import contained, trusted_root
from vuzol.projects.source_catalog import PackageRegistry, SourceCatalog

DEPENDENCY_APPROVAL_SCHEMA = "dependency-provisioning-approval.v1"
DEPENDENCY_RECEIPT_SCHEMA = "dependency-environment-receipt.v1"
DEPENDENCY_RECEIPT = ".vuzol-dependencies.json"
_PROJECT_ID = re.compile(r"[a-z0-9]+(?:[-_][a-z0-9]+)*")
_PYTHON_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*(?:\[[A-Za-z0-9,._-]+\])?")
_NODE_NAME = re.compile(r"(?:@[a-z0-9._-]+/)?[a-z0-9._-]+", re.IGNORECASE)
_NODE_VERSION = re.compile(r"[0-9A-Za-z.*+~^<>=| -]+")


class DependencyError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DependencyRequest:
    ecosystem: str
    manifest_name: str
    manifest_sha256: str
    direct_dependencies: tuple[str, ...]
    registry_provider: str
    registry_hosts: tuple[str, ...]
    input_lockfile_name: str | None = None
    input_lockfile_sha256: str | None = None

    @property
    def environment_key(self) -> str:
        encoded = json.dumps(self.approval_record(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def approval_record(self) -> dict[str, object]:
        return {
            "ecosystem": self.ecosystem,
            "manifest_name": self.manifest_name,
            "manifest_sha256": self.manifest_sha256,
            "direct_dependencies": list(self.direct_dependencies),
            "registry_provider": self.registry_provider,
            "registry_hosts": list(self.registry_hosts),
            "input_lockfile_name": self.input_lockfile_name,
            "input_lockfile_sha256": self.input_lockfile_sha256,
        }


@dataclass(frozen=True, slots=True)
class DependencyEnvironment:
    request: DependencyRequest
    root: Path
    lockfile_name: str
    lockfile_sha256: str

    def receipt(self) -> dict[str, object]:
        return {
            "schema_version": DEPENDENCY_RECEIPT_SCHEMA,
            "environment_key": self.request.environment_key,
            **self.request.approval_record(),
            "lockfile_name": self.lockfile_name,
            "lockfile_sha256": self.lockfile_sha256,
        }


def inspect_dependency_requests(
    worktree: Path,
    catalog: SourceCatalog,
    *,
    maximum_direct_dependencies: int,
) -> tuple[DependencyRequest, ...]:
    root = trusted_root(worktree, create=False)
    requests: list[DependencyRequest] = []
    for ecosystem, manifest_name, parser in (
        ("python", "pyproject.toml", _python_dependencies),
        ("node", "package.json", _node_dependencies),
    ):
        manifest = contained(root, root / manifest_name, must_exist=False)
        if not manifest.exists():
            continue
        content = _trusted_manifest(manifest)
        dependencies = parser(content)
        if not dependencies:
            continue
        if len(dependencies) > maximum_direct_dependencies:
            raise DependencyError("dependency manifest exceeds direct dependency policy")
        registry = catalog.registry(ecosystem)
        if registry is None:
            raise DependencyError(f"no trusted package registry for {ecosystem}")
        lockfile_name, lockfile_sha256 = _existing_lockfile(root, registry)
        requests.append(
            _request(
                ecosystem,
                manifest_name,
                content,
                dependencies,
                registry,
                input_lockfile_name=lockfile_name,
                input_lockfile_sha256=lockfile_sha256,
            )
        )
    return tuple(requests)


def dependency_environment_path(root: Path, project_id: str, request: DependencyRequest) -> Path:
    if _PROJECT_ID.fullmatch(project_id) is None:
        raise DependencyError("project ID is unsafe for dependency storage")
    storage = trusted_root(root, create=True)
    return contained(
        storage,
        storage / project_id / request.ecosystem / request.environment_key,
        must_exist=False,
    )


def load_dependency_environment(
    root: Path, project_id: str, request: DependencyRequest
) -> DependencyEnvironment | None:
    if not root.is_dir() or root.is_symlink():
        return None
    target = dependency_environment_path(root, project_id, request)
    if not target.is_dir() or target.is_symlink():
        return None
    receipt = contained(target, target / DEPENDENCY_RECEIPT, must_exist=False)
    try:
        metadata = receipt.lstat()
        if not stat.S_ISREG(metadata.st_mode) or receipt.is_symlink() or metadata.st_mode & 0o022:
            return None
        raw = json.loads(receipt.read_text(encoding="utf-8"))
        if (
            not isinstance(raw, dict)
            or raw.get("schema_version") != DEPENDENCY_RECEIPT_SCHEMA
            or raw.get("environment_key") != request.environment_key
            or any(raw.get(key) != value for key, value in request.approval_record().items())
        ):
            return None
        lockfile_name = raw.get("lockfile_name")
        lockfile_sha256 = raw.get("lockfile_sha256")
        if (
            not isinstance(lockfile_name, str)
            or not lockfile_name
            or Path(lockfile_name).name != lockfile_name
            or not isinstance(lockfile_sha256, str)
            or len(lockfile_sha256) != 64
        ):
            return None
        lockfile = contained(target, target / lockfile_name, must_exist=False)
        if _sha256_regular(lockfile) != lockfile_sha256:
            return None
        environment_directory = target / (
            "venv" if request.ecosystem == "python" else "node_modules"
        )
        if not environment_directory.is_dir() or environment_directory.is_symlink():
            return None
        return DependencyEnvironment(request, target, lockfile_name, lockfile_sha256)
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _request(
    ecosystem: str,
    manifest_name: str,
    content: bytes,
    dependencies: tuple[str, ...],
    registry: PackageRegistry,
    *,
    input_lockfile_name: str | None,
    input_lockfile_sha256: str | None,
) -> DependencyRequest:
    return DependencyRequest(
        ecosystem=ecosystem,
        manifest_name=manifest_name,
        manifest_sha256=hashlib.sha256(content).hexdigest(),
        direct_dependencies=dependencies,
        registry_provider=registry.provider,
        registry_hosts=registry.hosts,
        input_lockfile_name=input_lockfile_name,
        input_lockfile_sha256=input_lockfile_sha256,
    )


def _existing_lockfile(root: Path, registry: PackageRegistry) -> tuple[str | None, str | None]:
    for name in registry.lockfiles:
        if "*" in name:
            continue
        candidate = contained(root, root / name, must_exist=False)
        if candidate.exists():
            content = _trusted_manifest(candidate)
            return name, hashlib.sha256(content).hexdigest()
    return None, None


def _trusted_manifest(path: Path) -> bytes:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink() or metadata.st_size > 1_000_000:
        raise DependencyError("dependency manifest is not a bounded regular file")
    return path.read_bytes()


def _python_dependencies(content: bytes) -> tuple[str, ...]:
    try:
        raw = tomllib.loads(content.decode("utf-8"))
        project = raw.get("project", {})
        values = project.get("dependencies", []) if isinstance(project, dict) else []
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise DependencyError("Python dependency manifest is invalid") from error
    if not isinstance(values, list):
        raise DependencyError("Python dependencies must be a list")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value or len(value) > 500 or "@" in value:
            raise DependencyError("Python dependency requires a custom source")
        name = re.split(r"[<>=!~; ]", value, maxsplit=1)[0]
        if _PYTHON_NAME.fullmatch(name) is None:
            raise DependencyError("Python dependency name is unsafe")
        normalized.append(value.strip())
    return tuple(sorted(set(normalized), key=str.casefold))


def _node_dependencies(content: bytes) -> tuple[str, ...]:
    try:
        raw = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DependencyError("Node dependency manifest is invalid") from error
    if not isinstance(raw, dict):
        raise DependencyError("Node dependency manifest must be an object")
    normalized: list[str] = []
    for section_name in ("dependencies", "devDependencies"):
        section = raw.get(section_name, {})
        if not isinstance(section, dict):
            raise DependencyError("Node dependency section must be an object")
        for name, version in section.items():
            if (
                not isinstance(name, str)
                or _NODE_NAME.fullmatch(name) is None
                or not isinstance(version, str)
                or _NODE_VERSION.fullmatch(version) is None
                or len(version) > 200
            ):
                raise DependencyError("Node dependency requires a custom source")
            normalized.append(f"{name}@{version}")
    return tuple(sorted(set(normalized), key=str.casefold))


def _sha256_regular(path: Path) -> str | None:
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
            return None
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None
