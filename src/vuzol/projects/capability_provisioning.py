"""Hash-bound approval and offline installation for project capabilities."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tarfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import CapabilityProvisioningSettings
from vuzol.execution.paths import contained, trusted_root
from vuzol.project_environment import current_environment
from vuzol.projects.toolchains import (
    TOOLCHAIN_RECEIPT,
    TOOLCHAIN_RECEIPT_SCHEMA,
    ToolchainReceiptError,
    ToolchainSpec,
    load_installed_toolchain,
    parse_toolchain_spec,
)
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Approval, ProjectEnvironmentRevision, Step, Task
from vuzol.storage.types import ApprovalStatus, StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash, verified_envelope

CAPABILITY_APPROVAL_SCHEMA = "capability-provisioning-approval.v1"
CAPABILITY_BUNDLE_SCHEMA = "capability-bundle.v2"
CAPABILITY_APPROVAL_TTL = timedelta(days=7)

_HOST_EXECUTABLES = {
    "git": ("git",),
    "node-runtime": ("node",),
    "python-runtime": ("python3",),
}


class CapabilityProvisioningError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CapabilityBundle:
    capability_key: str
    archive: Path
    archive_sha256: str
    archive_bytes: int
    version: str
    executables: tuple[tuple[str, str], ...]
    environment: tuple[tuple[str, str], ...] = ()

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
        }


class OfflineCapabilityInstaller:
    def __init__(self, settings: CapabilityProvisioningSettings) -> None:
        self._settings = settings

    @property
    def installation_root(self) -> Path:
        return self._settings.toolchain_root

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
        current = self.inspect_bundle(bundle.capability_key)
        if current != bundle:
            raise CapabilityProvisioningError("capability bundle changed after approval")
        root = trusted_root(self._settings.toolchain_root, create=True)
        target = contained(root, root / bundle.capability_key, must_exist=False)
        if target.exists():
            if self.ready(bundle.capability_key):
                return
            raise CapabilityProvisioningError("incomplete capability installation needs cleanup")
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
        total = 0
        directories: set[Path] = {destination}
        try:
            with tarfile.open(bundle.archive, mode="r:") as archive:
                members = archive.getmembers()
                if len(members) > self._settings.maximum_files:
                    raise CapabilityProvisioningError("capability bundle contains too many files")
                for member in members:
                    relative = PurePosixPath(member.name)
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


class CapabilityProvisioningHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        installer: OfflineCapabilityInstaller,
    ) -> None:
        self._factory = factory
        self._installer = installer

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        side_effect_started = False
        try:
            task, step, environment, approval = await self._load(request)
            requirements = _requirements(environment.contract)
            missing = tuple(
                key for key, _label, _mode in requirements if not self._installer.ready(key)
            )
            if not missing:
                if approval is not None and approval.status is ApprovalStatus.APPROVED:
                    await self._consume(request, approval.id)
                return StepOutcome.succeeded({"status": "ready", "capabilities": []})
            if approval is None:
                bundles: list[CapabilityBundle] = []
                for key, label, mode in requirements:
                    if key not in missing:
                        continue
                    if mode == "external_setup":
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
                    summary="Установка инструментов проекта требует отдельного разрешения.",
                )
            if approval.status is ApprovalStatus.PENDING:
                return StepOutcome(
                    kind=OutcomeKind.NEEDS_APPROVAL,
                    result={"approval_id": str(approval.id), "capabilities": list(missing)},
                    category="capability_installation_approval_required",
                )
            if approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
                raise CapabilityProvisioningError("capability installation was not approved")
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
                raise CapabilityProvisioningError("installed capability did not pass its probe")
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

    async def _load(
        self, request: StepExecutionRequest
    ) -> tuple[Task, Step, ProjectEnvironmentRevision, Approval | None]:
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
            step = await session.get(Step, request.step_id)
            environment = (
                None
                if task is None or task.project_id is None
                else await current_environment(session, task.project_id)
            )
            approval = await session.scalar(
                select(Approval)
                .where(Approval.step_id == request.step_id)
                .order_by(Approval.requested_at.desc())
                .limit(1)
            )
        if task is None or step is None or environment is None:
            raise LookupError("capability provisioning state is incomplete")
        if (
            step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
            or step.lease_owner != request.lease.owner
            or step.lease_generation != request.lease.generation
            or step.run_id != request.run_id
        ):
            raise LeaseLost("capability provisioning step lease is stale")
        return task, step, environment, approval

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
            "bundles": [bundle.approval_record() for bundle in bundles],
        }
        digest = envelope_hash(envelope)
        approval_id = uuid.uuid4()
        approval = Approval(
            id=approval_id,
            step_id=step.id,
            action_envelope_hash=digest,
            requested_action="install_capabilities",
            normalized_target=f"{task.project_id}:managed-toolchains",
            human_summary=(
                "Установить в изолированное хранилище Vuzol: "
                + ", ".join(bundle.capability_key for bundle in bundles)
            ),
            token_hash=hashlib.sha256(f"{approval_id}:{digest}".encode()).hexdigest(),
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(UTC) + CAPABILITY_APPROVAL_TTL,
        )
        async with self._factory.begin() as session:
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
            existing = await session.scalar(select(Approval).where(Approval.step_id == step.id))
            if existing is not None:
                return existing
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
