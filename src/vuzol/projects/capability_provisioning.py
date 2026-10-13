"""Hash-bound approval and offline installation for project capabilities."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import stat
import tarfile
import uuid
import zipfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import CapabilityProvisioningSettings
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.paths import contained, trusted_root
from vuzol.project_environment import (
    current_environment,
    environment_hash,
    record_detected_environment,
    record_detected_tool_requirements,
)
from vuzol.projects.descriptors import is_tool_requirement
from vuzol.projects.installations import (
    InstallationState,
    installation_states,
    probe_toolchain,
    record_installation,
    verify_installed_capability,
)
from vuzol.projects.source_catalog import SourceCatalog, ToolchainSource
from vuzol.projects.source_download import SourceDownloadError, TrustedSourceDownloader
from vuzol.projects.toolchains import (
    TOOLCHAIN_RECEIPT,
    TOOLCHAIN_RECEIPT_SCHEMA,
    ToolchainReceiptError,
    ToolchainSpec,
    load_installed_toolchain,
    parse_toolchain_spec,
)
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import (
    Approval,
    Event,
    ProjectEnvironmentRevision,
    Run,
    Step,
    Task,
    Worktree,
)
from vuzol.storage.types import (
    ApprovalStatus,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash, verified_envelope
from vuzol.workflows.transitions import transition_step, transition_task

CAPABILITY_APPROVAL_SCHEMA = "capability-provisioning-approval.v1"
CAPABILITY_BUNDLE_SCHEMA = "capability-bundle.v2"
CAPABILITY_APPROVAL_TTL = timedelta(days=7)

_HOST_EXECUTABLES = {
    "git": ("git",),
    "python-runtime": ("python3",),
}


class CapabilityProvisioningError(RuntimeError):
    pass


def _skippable_catalogue_symlink(bundle: CapabilityBundle, member: tarfile.TarInfo) -> bool:
    """Skip only Node's three known convenience links from the hash-bound archive."""

    if bundle.source is None or bundle.capability_key != "node-runtime" or not member.issym():
        return False
    return (member.name, member.linkname) in {
        (
            "node-v24.19.0-linux-x64/bin/corepack",
            "../lib/node_modules/corepack/dist/corepack.js",
        ),
        (
            "node-v24.19.0-linux-x64/bin/npm",
            "../lib/node_modules/npm/bin/npm-cli.js",
        ),
        (
            "node-v24.19.0-linux-x64/bin/npx",
            "../lib/node_modules/npm/bin/npx-cli.js",
        ),
    }


@dataclass(frozen=True, slots=True)
class CapabilityBundle:
    capability_key: str
    archive: Path
    archive_sha256: str
    archive_bytes: int
    version: str
    executables: tuple[tuple[str, str], ...]
    environment: tuple[tuple[str, str], ...] = ()
    archive_format: str = "tar"
    source: ToolchainSource | None = None

    @property
    def required_paths(self) -> tuple[str, ...]:
        return tuple(path for _command, path in self.executables)

    def toolchain_spec(self) -> ToolchainSpec:
        return ToolchainSpec(
            capability_key=self.capability_key,
            version=self.version,
            archive_sha256=self.archive_sha256,
            executables=self.executables,
            environment=self.environment,
        )

    def approval_record(self) -> dict[str, object]:
        return {
            "capability_key": self.capability_key,
            "archive_sha256": self.archive_sha256,
            "archive_bytes": self.archive_bytes,
            "version": self.version,
            "required_paths": list(self.required_paths),
            "executables": {command: path for command, path in self.executables},
            "environment": {name: path for name, path in self.environment},
            "source_kind": "catalogue" if self.source is not None else "operator_staged",
            "source_provider": None if self.source is None else self.source.provider,
            "source_url": None if self.source is None else self.source.url,
        }


class OfflineCapabilityInstaller:
    def __init__(
        self,
        settings: CapabilityProvisioningSettings,
        *,
        catalog: SourceCatalog | None = None,
        downloader: TrustedSourceDownloader | None = None,
    ) -> None:
        self._settings = settings
        self._catalog = catalog or SourceCatalog.builtin()
        self._downloader = downloader or TrustedSourceDownloader(
            settings.download_cache_root,
            maximum_bytes=settings.maximum_bundle_bytes,
        )

    @property
    def installation_root(self) -> Path:
        return self._settings.toolchain_root

    @property
    def node_id(self) -> str:
        return self._settings.node_id

    @property
    def health_ttl_seconds(self) -> int:
        return self._settings.health_ttl_seconds

    def ready(self, capability_key: str) -> bool:
        host = _HOST_EXECUTABLES.get(capability_key)
        if host is not None and all(shutil.which(executable) is not None for executable in host):
            return True
        return load_installed_toolchain(self._settings.toolchain_root, capability_key) is not None

    def inspect_bundle(self, capability_key: str) -> CapabilityBundle:
        if not self._settings.enabled:
            raise CapabilityProvisioningError("offline capability provisioning is disabled")
        if (
            self._settings.allowed_capabilities
            and capability_key not in self._settings.allowed_capabilities
        ):
            raise CapabilityProvisioningError(
                "capability is absent from the provisioning allowlist"
            )
        bundle_root = trusted_root(self._settings.bundle_root, create=False)
        manifest_path = contained(
            bundle_root, bundle_root / f"{capability_key}.json", must_exist=False
        )
        if not manifest_path.exists():
            source = self._catalog.toolchain(capability_key)
            if source is None:
                raise CapabilityProvisioningError(
                    "capability bundle is unavailable and no trusted source is registered"
                )
            suffix = ".zip" if source.archive_format == "zip" else ".tar"
            archive = self._settings.download_cache_root / (source.spec.archive_sha256 + suffix)
            return CapabilityBundle(
                capability_key=capability_key,
                archive=archive,
                archive_sha256=source.spec.archive_sha256,
                archive_bytes=source.archive_bytes,
                version=source.spec.version,
                executables=source.spec.executables,
                environment=source.spec.environment,
                archive_format=source.archive_format,
                source=source,
            )
        _require_trusted_regular_file(manifest_path)
        try:
            raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise CapabilityProvisioningError("capability bundle manifest is invalid") from error
        if not isinstance(raw, dict) or raw.get("schema_version") != CAPABILITY_BUNDLE_SCHEMA:
            raise CapabilityProvisioningError("capability bundle schema is unsupported")
        if raw.get("capability_key") != capability_key:
            raise CapabilityProvisioningError("capability bundle targets another capability")
        archive_name = raw.get("archive")
        expected_hash = raw.get("sha256")
        if (
            not isinstance(archive_name, str)
            or PurePosixPath(archive_name).name != archive_name
            or not archive_name.endswith(".tar")
            or not isinstance(expected_hash, str)
            or len(expected_hash) != 64
            or any(character not in "0123456789abcdef" for character in expected_hash)
        ):
            raise CapabilityProvisioningError("capability bundle manifest fields are unsafe")
        archive = contained(bundle_root, bundle_root / archive_name, must_exist=False)
        _require_trusted_regular_file(archive)
        size = archive.stat().st_size
        if size <= 0 or size > self._settings.maximum_bundle_bytes:
            raise CapabilityProvisioningError("capability bundle size is outside policy")
        measured = _sha256_file(archive)
        if measured != expected_hash:
            raise CapabilityProvisioningError("capability bundle hash does not match its manifest")
        try:
            spec = parse_toolchain_spec(
                {
                    "schema_version": TOOLCHAIN_RECEIPT_SCHEMA,
                    "capability_key": capability_key,
                    "version": raw.get("version"),
                    "archive_sha256": measured,
                    "executables": raw.get("executables"),
                    "environment": raw.get("environment", {}),
                },
                expected_key=capability_key,
            )
        except ToolchainReceiptError as error:
            raise CapabilityProvisioningError(str(error)) from error
        return CapabilityBundle(
            capability_key,
            archive,
            measured,
            size,
            spec.version,
            spec.executables,
            spec.environment,
        )

    def install(self, bundle: CapabilityBundle) -> None:
        if self.inspect_bundle(bundle.capability_key) != bundle:
            raise CapabilityProvisioningError("capability bundle changed after approval")
        root = trusted_root(self._settings.toolchain_root, create=True)
        lock_path = contained(root, root / ".installation.lock", must_exist=False)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "rb") as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise CapabilityProvisioningError("installation lock is not a regular file")
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            self._install_locked(bundle, root)

    def _install_locked(self, bundle: CapabilityBundle, root: Path) -> None:
        current = self.inspect_bundle(bundle.capability_key)
        if current != bundle:
            raise CapabilityProvisioningError("capability bundle changed after approval")
        target = contained(root, root / bundle.capability_key, must_exist=False)
        if target.exists():
            installed = load_installed_toolchain(root, bundle.capability_key)
            if installed == bundle.toolchain_spec():
                return
            raise CapabilityProvisioningError(
                "incomplete or different capability installation needs operator attention"
            )
        if bundle.source is not None:
            try:
                bundle = replace(bundle, archive=self._downloader.materialize(bundle.source))
            except SourceDownloadError as error:
                raise CapabilityProvisioningError(str(error)) from error
        temporary = contained(
            root, root / f".{bundle.capability_key}-{uuid.uuid4().hex}.tmp", must_exist=False
        )
        temporary.mkdir(mode=0o700)
        try:
            self._extract(bundle, temporary)
            for relative in bundle.required_paths:
                if not _safe_executable(temporary, relative):
                    raise CapabilityProvisioningError(
                        f"capability bundle is missing required executable: {relative}"
                    )
            for _name, relative in bundle.environment:
                if not _safe_runtime_path(temporary, relative):
                    raise CapabilityProvisioningError(
                        f"capability bundle is missing environment path: {relative}"
                    )
            temporary.chmod(0o755)
            receipt = contained(temporary, temporary / TOOLCHAIN_RECEIPT, must_exist=False)
            receipt.write_text(
                json.dumps(
                    bundle.toolchain_spec().receipt(), sort_keys=True, separators=(",", ":")
                ),
                encoding="utf-8",
            )
            receipt.chmod(0o444)
            temporary.chmod(0o555)
            os.replace(temporary, target)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    def _extract(self, bundle: CapabilityBundle, destination: Path) -> None:
        if bundle.archive_format == "zip":
            self._extract_zip(bundle, destination)
            return
        total = 0
        directories: set[Path] = {destination}
        try:
            with tarfile.open(bundle.archive, mode="r:*") as archive:
                members = archive.getmembers()
                if len(members) > self._settings.maximum_files:
                    raise CapabilityProvisioningError("capability bundle contains too many files")
                for member in members:
                    relative = PurePosixPath(member.name)
                    if _skippable_catalogue_symlink(bundle, member):
                        continue
                    if (
                        not member.name
                        or relative.is_absolute()
                        or ".." in relative.parts
                        or member.issym()
                        or member.islnk()
                        or not (member.isdir() or member.isreg())
                    ):
                        raise CapabilityProvisioningError(
                            "capability bundle contains unsafe entries"
                        )
                    total += member.size
                    if total > self._settings.maximum_bundle_bytes:
                        raise CapabilityProvisioningError(
                            "expanded capability bundle exceeds policy"
                        )
                    target = contained(
                        destination, destination.joinpath(*relative.parts), must_exist=False
                    )
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True, mode=0o755)
                        directories.add(target)
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    directories.update((target.parent, *target.parent.parents))
                    source = archive.extractfile(member)
                    if source is None:
                        raise CapabilityProvisioningError("capability bundle entry is unreadable")
                    with target.open("xb") as output:
                        shutil.copyfileobj(source, output, length=1024 * 1024)
                    target.chmod(0o555 if member.mode & 0o111 else 0o444)
                for directory in sorted(
                    (
                        path
                        for path in directories
                        if destination == path or destination in path.parents
                    ),
                    key=lambda path: len(path.parts),
                    reverse=True,
                ):
                    directory.chmod(0o555)
        except (OSError, tarfile.TarError) as error:
            raise CapabilityProvisioningError("capability bundle extraction failed") from error

    def _extract_zip(self, bundle: CapabilityBundle, destination: Path) -> None:
        total = 0
        directories: set[Path] = {destination}
        try:
            with zipfile.ZipFile(bundle.archive) as archive:
                members = archive.infolist()
                if len(members) > self._settings.maximum_files:
                    raise CapabilityProvisioningError("capability bundle contains too many files")
                for member in members:
                    relative = PurePosixPath(member.filename)
                    mode = member.external_attr >> 16
                    if (
                        not member.filename
                        or relative.is_absolute()
                        or ".." in relative.parts
                        or stat.S_ISLNK(mode)
                        or not (member.is_dir() or stat.S_ISREG(mode) or mode == 0)
                    ):
                        raise CapabilityProvisioningError(
                            "capability bundle contains unsafe entries"
                        )
                    total += member.file_size
                    if total > self._settings.maximum_bundle_bytes:
                        raise CapabilityProvisioningError(
                            "expanded capability bundle exceeds policy"
                        )
                    target = contained(
                        destination,
                        destination.joinpath(*relative.parts),
                        must_exist=False,
                    )
                    if member.is_dir():
                        target.mkdir(parents=True, exist_ok=True, mode=0o755)
                        directories.add(target)
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    directories.update((target.parent, *target.parent.parents))
                    with archive.open(member) as source, target.open("xb") as output:
                        shutil.copyfileobj(source, output, length=1024 * 1024)
                    target.chmod(0o555 if mode & 0o111 else 0o444)
                for directory in sorted(
                    (
                        path
                        for path in directories
                        if destination == path or destination in path.parents
                    ),
                    key=lambda path: len(path.parts),
                    reverse=True,
                ):
                    directory.chmod(0o555)
        except (OSError, zipfile.BadZipFile) as error:
            raise CapabilityProvisioningError("capability bundle extraction failed") from error


class CapabilityProvisioningHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        installer: OfflineCapabilityInstaller,
        *,
        registries: ConfigurationBundle | None = None,
        worktree_root: Path | None = None,
    ) -> None:
        self._factory = factory
        self._installer = installer
        self._registries = registries
        self._worktree_root = worktree_root

    async def ensure_before_dependencies(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        """Hand newly declared needs to the existing privileged applier step.

        The executor never installs a shared toolchain or gains host-write access.
        This is an ordinary additional instance of ensure_capabilities, not a
        planner call, a source repair, or a new workflow type.
        """

        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        try:
            _task, _step, environment, _approval, inventory = await self._load(request)
            missing = [
                key
                for key, _label, _mode in _requirements(environment.contract)
                if key not in inventory
            ]
            if not missing:
                return StepOutcome.succeeded({"status": "ready"})
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={
                    "environment_revision_id": str(environment.id),
                    "environment_hash": environment.content_hash,
                    "capabilities": missing,
                },
                category="capability_preflight_required",
                summary="Общую установку инструментов проверит привилегированный applier.",
            )
        except LeaseLost:
            cancellation.request()
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="lease_lost")
        except (CapabilityProvisioningError, LookupError, ValueError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="capability_provisioning_failed",
                summary=str(error)[:500],
            )

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        side_effect_started = False
        try:
            task, step, environment, approval, inventory = await self._load(request)
            requirements = _requirements(environment.contract)
            missing = tuple(key for key, _label, _mode in requirements if key not in inventory)
            if not missing:
                if (
                    approval is not None
                    and approval.status is ApprovalStatus.APPROVED
                    and step.payload.get("capability_wait_approval_id") != str(approval.id)
                ):
                    await self._consume(request, approval.id)
                return StepOutcome.succeeded({"status": "ready", "capabilities": []})
            if approval is None:
                bundles: list[CapabilityBundle] = []
                for key, label, mode in requirements:
                    if key not in missing:
                        continue
                    if mode == "external_setup" and not is_tool_requirement(key):
                        return _needs_setup(key, label, "external configuration is required")
                    try:
                        bundles.append(self._installer.inspect_bundle(key))
                    except CapabilityProvisioningError as error:
                        return _needs_setup(key, label, str(error))
                approval = await self._request_approval(
                    request,
                    task=task,
                    step=step,
                    environment=environment,
                    bundles=tuple(bundles),
                )
                return StepOutcome(
                    kind=OutcomeKind.NEEDS_APPROVAL,
                    result={"approval_id": str(approval.id), "capabilities": list(missing)},
                    category="capability_installation_approval_required",
                    summary=(
                        "Ожидается общая установка инструмента на ноду; новый апрув не нужен."
                        if step.payload.get("capability_wait_approval_id")
                        else "Нужен апрув первой установки инструмента на общую ноду."
                    ),
                )
            shared_waiter = step.payload.get("capability_wait_approval_id") == str(approval.id)
            if approval.status is ApprovalStatus.PENDING or (
                shared_waiter and approval.status is ApprovalStatus.APPROVED
            ):
                return StepOutcome(
                    kind=OutcomeKind.NEEDS_APPROVAL,
                    result={"approval_id": str(approval.id), "capabilities": list(missing)},
                    category="capability_installation_approval_required",
                    summary=(
                        "Ожидается общая установка инструмента на ноду."
                        if shared_waiter
                        else "Ожидается подтверждение установки инструмента."
                    ),
                )
            if approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
                raise CapabilityProvisioningError("capability installation was not approved")
            if shared_waiter:
                raise CapabilityProvisioningError(
                    "shared installation is not healthy after install"
                )
            envelope = verified_envelope(step, approval)
            _verify_environment(envelope, environment, self._installer)
            raw_bundles = envelope.get("bundles")
            if not isinstance(raw_bundles, list) or not raw_bundles:
                raise CapabilityProvisioningError("approved capability bundle list is missing")
            for raw in raw_bundles:
                bundle = _bundle_from_envelope(raw, self._installer)
                await self._assert_current_lease(request)
                if cancellation.requested:
                    return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
                side_effect_started = True
                self._installer.install(bundle)
            if any(not self._installer.ready(key) for key in missing):
                await self._record_installations(missing, environment, failed=True)
                raise CapabilityProvisioningError("installed capability did not pass its probe")
            # WP03: persist the verified installation (receipt/env hash/health
            # TTL) from the existing post-install probe. Approval path unchanged.
            await self._record_installations(missing, environment, failed=False)
            await self._consume(request, approval.id)
            return StepOutcome.succeeded(
                {
                    "status": "installed",
                    "capabilities": list(missing),
                    "approval_id": str(approval.id),
                }
            )
        except LeaseLost:
            cancellation.request()
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="lease_lost")
        except (CapabilityProvisioningError, LookupError, ValueError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="capability_provisioning_failed",
                summary=str(error)[:500],
                unknown_effects=side_effect_started,
            )

    async def _record_installations(
        self,
        missing: tuple[str, ...],
        environment: ProjectEnvironmentRevision,
        *,
        failed: bool,
    ) -> None:
        """Persist verified installations from the existing post-install probe."""

        root = self._installer.installation_root
        env_hash = environment_hash(environment.contract)
        for key in missing:
            if load_installed_toolchain(root, key) is None:
                continue
            state, spec, detail = probe_toolchain(root, key, confined_roots=(root,))
            if failed and state is InstallationState.INSTALLED:
                state = InstallationState.FAILED
                detail = "installed capability did not pass its probe"
            async with self._factory.begin() as session:
                await record_installation(
                    session,
                    probe=(state, spec, detail),
                    capability_key=key,
                    installation_root=root,
                    node_id=self._installer.node_id,
                    environment_hash=env_hash,
                    health_ttl_seconds=self._installer.health_ttl_seconds,
                )

    async def _load(
        self, request: StepExecutionRequest
    ) -> tuple[Task, Step, ProjectEnvironmentRevision, Approval | None, frozenset[str]]:
        async with self._factory.begin() as session:
            task = await session.get(Task, request.task_id)
            step = await session.scalar(
                select(Step).where(Step.id == request.step_id).with_for_update()
            )
            if task is None or step is None:
                raise LookupError("capability provisioning state is incomplete")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_owner != request.lease.owner
                or step.lease_generation != request.lease.generation
                or step.run_id != request.run_id
                or task.id != request.task_id
            ):
                raise LeaseLost("capability provisioning step lease is stale")
            environment = (
                None
                if task.project_id is None
                else await current_environment(session, task.project_id)
            )
            # Registry-created projects may predate environment versioning and
            # have no ProjectProvisioning row. Detect only the existing managed
            # repository, once, using the same conservative import baseline.
            # This neither chooses a stack nor authorizes installs/commands.
            if environment is None and task.project_id is not None and self._registries:
                project = self._registries.projects.get(task.project_id)
                if not project.enabled or not project.repository_path.is_dir():
                    raise LookupError("managed project repository is unavailable")
                environment = await record_detected_environment(
                    session, project_id=task.project_id, repository=project.repository_path
                )
            if task.project_id is not None and self._worktree_root is not None:
                worktree = await session.scalar(
                    select(Worktree).where(
                        Worktree.task_id == task.id,
                        Worktree.run_id == request.run_id,
                        Worktree.project_id == task.project_id,
                    )
                )
                if worktree is not None:
                    path = contained(self._worktree_root, Path(worktree.path))
                    environment = await record_detected_tool_requirements(
                        session, project_id=task.project_id, repository=path
                    )
            approval = await session.scalar(
                select(Approval)
                .where(
                    Approval.step_id == request.step_id,
                    Approval.requested_action == "install_capabilities",
                )
                .order_by(Approval.requested_at.desc())
                .limit(1)
            )
            shared_id = step.payload.get("capability_wait_approval_id")
            if approval is None and isinstance(shared_id, str):
                approval = await session.get(Approval, uuid.UUID(shared_id))
                if approval is None:
                    raise CapabilityProvisioningError("shared installation approval is missing")
                leader = await session.get(Step, approval.step_id)
                if leader is None:
                    raise CapabilityProvisioningError("shared installation step is missing")
                shared_envelope = verified_envelope(leader, approval)
                if shared_envelope.get("node_id") != self._installer.node_id or shared_envelope.get(
                    "installation_root"
                ) != str(self._installer.installation_root):
                    raise CapabilityProvisioningError("shared installation targets another node")
                if approval.status is ApprovalStatus.CONSUMED:
                    step.payload = {
                        key: value
                        for key, value in step.payload.items()
                        if key != "capability_wait_approval_id"
                    }
                    session.add(
                        Event(
                            entity_type="step",
                            entity_id=step.id,
                            event_type="capability.shared_installation_settled",
                            actor_type="capability_inventory",
                            payload={"approval_id": str(approval.id)},
                        )
                    )
                    approval = None
            inventory = (
                frozenset() if environment is None else await self._inventory(session, environment)
            )
        if environment is None:
            raise LookupError("capability provisioning state is incomplete")
        return task, step, environment, approval, inventory

    async def _inventory(
        self, session: AsyncSession, environment: ProjectEnvironmentRevision
    ) -> frozenset[str]:
        """Resolve verified node installations independently of the project flags."""

        ready: set[str] = set()
        requirements = _requirements(environment.contract)
        if not requirements:
            return frozenset()
        states = await installation_states(session, node_id=self._installer.node_id)
        for key, _label, mode in requirements:
            if not is_tool_requirement(key) and mode in {"external_setup", "approval_required"}:
                continue
            if key in _HOST_EXECUTABLES and self._installer.ready(key):
                if key in states and states[key] != InstallationState.INSTALLED.value:
                    raise CapabilityProvisioningError(f"host capability is not healthy: {key}")
                ready.add(key)
                continue
            spec = await verify_installed_capability(
                session,
                root=self._installer.installation_root,
                capability_key=key,
                node_id=self._installer.node_id,
                health_ttl_seconds=self._installer.health_ttl_seconds,
                environment_hash=environment.content_hash,
            )
            if spec is not None:
                ready.add(key)
            elif load_installed_toolchain(self._installer.installation_root, key) is not None:
                raise CapabilityProvisioningError(f"installed capability is not healthy: {key}")
        return frozenset(ready)

    async def _request_approval(
        self,
        request: StepExecutionRequest,
        *,
        task: Task,
        step: Step,
        environment: ProjectEnvironmentRevision,
        bundles: tuple[CapabilityBundle, ...],
    ) -> Approval:
        if not bundles:
            raise CapabilityProvisioningError("no installable capabilities were selected")
        envelope: dict[str, Any] = {
            "schema_version": CAPABILITY_APPROVAL_SCHEMA,
            "requested_action": "install_capabilities",
            "task_id": str(task.id),
            "run_id": str(request.run_id),
            "step_id": str(step.id),
            "project_id": task.project_id,
            "environment_revision_id": str(environment.id),
            "environment_hash": environment.content_hash,
            "installation_root": str(self._installer.installation_root),
            "node_id": self._installer.node_id,
            "bundles": [bundle.approval_record() for bundle in bundles],
        }
        digest = envelope_hash(envelope)
        binding = envelope_hash(
            {
                "node_id": self._installer.node_id,
                "installation_root": str(self._installer.installation_root),
                "bundles": envelope["bundles"],
            }
        )
        target = f"{self._installer.node_id}:managed-toolchains:{binding}"
        approval_id = uuid.uuid4()
        approval = Approval(
            id=approval_id,
            step_id=step.id,
            action_envelope_hash=digest,
            requested_action="install_capabilities",
            normalized_target=target,
            human_summary=(
                f"Установить один раз на ноду {self._installer.node_id} для всех проектов: "
                + ", ".join(bundle.capability_key for bundle in bundles)
            ),
            token_hash=hashlib.sha256(f"{approval_id}:{digest}".encode()).hexdigest(),
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(UTC) + CAPABILITY_APPROVAL_TTL,
        )
        async with self._factory.begin() as session:
            lock_scope = (
                f"capability-install:{self._installer.node_id}:{self._installer.installation_root}"
            )
            lock_key = int.from_bytes(hashlib.sha256(lock_scope.encode()).digest()[:8], signed=True)
            await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
            locked = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
                .with_for_update()
            )
            if locked is None:
                raise LeaseLost("capability approval lost its step lease")
            existing = await session.scalar(
                select(Approval).where(
                    Approval.step_id == step.id, Approval.requested_action == "install_capabilities"
                )
            )
            if existing is not None:
                return existing
            shared_candidates = tuple(
                (
                    await session.execute(
                        select(Approval, Step)
                        .join(Step, Step.id == Approval.step_id)
                        .join(Run, Run.id == Step.run_id)
                        .where(
                            Approval.normalized_target.startswith(
                                f"{self._installer.node_id}:managed-toolchains:"
                            ),
                            Approval.requested_action == "install_capabilities",
                            Approval.status.in_((ApprovalStatus.PENDING, ApprovalStatus.APPROVED)),
                            Approval.expires_at > func.now(),
                            Run.status == RunStatus.RUNNING,
                            Step.status.in_(
                                (
                                    StepStatus.RUNNING,
                                    StepStatus.WAITING_APPROVAL,
                                    StepStatus.QUEUED,
                                    StepStatus.LEASED,
                                )
                            ),
                        )
                        .order_by(Approval.requested_at)
                    )
                ).all()
            )
            expected_records = {
                bundle.capability_key: bundle.approval_record() for bundle in bundles
            }
            shared: Approval | None = None
            for candidate, candidate_step in shared_candidates:
                candidate_envelope = verified_envelope(candidate_step, candidate)
                records = candidate_envelope.get("bundles")
                if (
                    candidate_envelope.get("node_id") == self._installer.node_id
                    and candidate_envelope.get("installation_root")
                    == str(self._installer.installation_root)
                    and isinstance(records, list)
                    and any(
                        isinstance(record, dict)
                        and expected_records.get(str(record.get("capability_key"))) == record
                        for record in records
                    )
                ):
                    shared = candidate
                    break
            if shared is not None:
                locked.payload = {
                    **locked.payload,
                    "capability_wait_approval_id": str(shared.id),
                }
                return shared
            session.add(approval)
            locked.payload = {
                **locked.payload,
                "approval_id": str(approval.id),
                "action_envelope": envelope,
            }
            locked.external_idempotency_key = f"install-capabilities:{digest}"
            await session.flush()
        return approval

    async def _assert_current_lease(self, request: StepExecutionRequest) -> None:
        async with self._factory() as session:
            step = await session.scalar(
                select(Step).where(
                    Step.id == request.step_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
            )
            if step is None:
                raise LeaseLost("capability provisioning lease was lost before installation")

    async def _consume(self, request: StepExecutionRequest, approval_id: uuid.UUID) -> None:
        async with self._factory.begin() as session:
            step = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
                .with_for_update()
            )
            approval = await session.scalar(
                select(Approval).where(Approval.id == approval_id).with_for_update()
            )
            if step is None or approval is None:
                raise LeaseLost("capability provisioning records disappeared")
            if approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
                raise CapabilityProvisioningError("capability approval changed before completion")
            approval.status = ApprovalStatus.CONSUMED
            approval.consumed_at = approval.consumed_at or datetime.now(UTC)
            await wake_capability_waiters(session, approval)


async def wake_capability_waiters(session: AsyncSession, approval: Approval) -> None:
    """Wake existing steps after a shared install settles; never invent an approval."""

    waiters = tuple(
        (
            await session.scalars(
                select(Step)
                .join(Run, Run.id == Step.run_id)
                .where(
                    Step.status == StepStatus.WAITING_APPROVAL,
                    Step.payload["capability_wait_approval_id"].astext == str(approval.id),
                    Run.status == RunStatus.RUNNING,
                )
                .with_for_update(of=Step, skip_locked=True)
            )
        ).all()
    )
    for waiter in waiters:
        await transition_step(session, waiter, StepStatus.QUEUED, actor_type="capability_inventory")
        if waiter.attempt_count > 0:
            waiter.attempt_count -= 1
        run = await session.get(Run, waiter.run_id)
        task = None if run is None else await session.get(Task, run.task_id)
        if task is not None and task.status is TaskStatus.WAITING_APPROVAL:
            await transition_task(
                session, task, TaskStatus.EXECUTING, actor_type="capability_inventory"
            )


async def abandon_capability_installation(
    session: AsyncSession, step_id: uuid.UUID, *, reason: str
) -> None:
    """A cancelled/failed leader must not strand other projects' waiters."""

    approvals = tuple(
        (
            await session.scalars(
                select(Approval)
                .where(
                    Approval.step_id == step_id,
                    Approval.requested_action == "install_capabilities",
                    Approval.status.in_((ApprovalStatus.PENDING, ApprovalStatus.APPROVED)),
                )
                .with_for_update()
            )
        ).all()
    )
    for approval in approvals:
        approval.status = ApprovalStatus.EXPIRED
        session.add(
            Event(
                entity_type="approval",
                entity_id=approval.id,
                event_type="capability.installation_unavailable",
                actor_type="workflow_manager",
                payload={"step_id": str(step_id), "reason": reason},
            )
        )
        await wake_capability_waiters(session, approval)


def _requirements(contract: dict[str, object]) -> tuple[tuple[str, str, str], ...]:
    raw = contract.get("capabilities")
    if not isinstance(raw, dict):
        return ()
    requirements: list[tuple[str, str, str]] = []
    for key in sorted(raw):
        item = raw[key]
        if isinstance(key, str) and isinstance(item, dict):
            requirements.append(
                (key, str(item.get("label") or key), str(item.get("provisioning") or "automatic"))
            )
    return tuple(requirements)


def _needs_setup(key: str, label: str, detail: str) -> StepOutcome:
    return StepOutcome(
        kind=OutcomeKind.BLOCKED,
        result={"status": "needs_setup", "capability": key},
        category="capability_setup_required",
        summary=f"{label}: {detail}"[:500],
    )


def _verify_environment(
    envelope: dict[str, Any],
    environment: ProjectEnvironmentRevision,
    installer: OfflineCapabilityInstaller,
) -> None:
    if (
        envelope.get("schema_version") != CAPABILITY_APPROVAL_SCHEMA
        or envelope.get("requested_action") != "install_capabilities"
        or envelope.get("environment_revision_id") != str(environment.id)
        or envelope.get("environment_hash") != environment.content_hash
        or envelope.get("project_id") != environment.project_id
        or envelope.get("installation_root") != str(installer.installation_root)
        or envelope.get("node_id", installer.node_id) != installer.node_id
    ):
        raise CapabilityProvisioningError(
            "project environment changed after approval was requested"
        )


def _bundle_from_envelope(raw: object, installer: OfflineCapabilityInstaller) -> CapabilityBundle:
    if not isinstance(raw, dict) or not isinstance(raw.get("capability_key"), str):
        raise CapabilityProvisioningError("approved capability bundle record is malformed")
    current = installer.inspect_bundle(raw["capability_key"])
    if current.approval_record() != raw:
        raise CapabilityProvisioningError("capability bundle no longer matches the approval")
    return current


def _safe_executable(root: Path, relative: str) -> bool:
    try:
        path = contained(root, root.joinpath(*PurePosixPath(relative).parts))
        metadata = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISREG(metadata.st_mode) and not path.is_symlink() and bool(metadata.st_mode & 0o111)
    )


def _safe_runtime_path(root: Path, relative: str) -> bool:
    try:
        path = contained(root, root.joinpath(*PurePosixPath(relative).parts))
        metadata = path.lstat()
    except OSError:
        return False
    return not path.is_symlink() and (
        stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)
    )


def _require_trusted_regular_file(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise CapabilityProvisioningError(
            f"capability bundle file is unavailable: {path.name}"
        ) from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or metadata.st_uid not in {0, os.geteuid()}
        or metadata.st_mode & 0o022
    ):
        raise CapabilityProvisioningError(f"capability bundle file is not trusted: {path.name}")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
