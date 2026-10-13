"""Verified capability installations, probe freshness and run version pins (WP03).

Installation is distinct from a descriptor (what the capability means) and from a
permission grant (who may use it). The installer remains the backend; this module
only records and re-verifies what it installed, with an environment hash and a
health TTL, and pins the version a run resolved.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.execution.paths import contained
from vuzol.projects.toolchains import ToolchainSpec, load_installed_toolchain
from vuzol.security.confined_paths import executable_within_roots
from vuzol.storage.models import CapabilityInstallation, CapabilityRunPin


class InstallationState(StrEnum):
    INSTALLED = "installed"
    STALE = "stale"
    FAILED = "failed"
    UNKNOWN = "unknown"


class CapabilityPinMismatch(RuntimeError):
    """An installed toolchain changed after the run pinned its version.

    Raised from the resolution path so a downgrade/upgrade fails closed instead
    of silently changing how an in-flight run resolves its executable.
    """


def receipt_digest(spec: ToolchainSpec) -> str:
    encoded = json.dumps(spec.receipt(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def probe_toolchain(
    root: Path,
    capability_key: str,
    *,
    confined_roots: tuple[Path, ...] = (),
) -> tuple[InstallationState, ToolchainSpec | None, str]:
    """Re-verify a managed toolchain receipt and its confined readability.

    Side-effect free: it never installs and never executes the binary.
    """

    spec = load_installed_toolchain(root, capability_key)
    if spec is None:
        return InstallationState.FAILED, None, "managed toolchain is not installed"
    for _command, relative in spec.executables:
        from pathlib import PurePosixPath

        executable = root / capability_key / Path(*PurePosixPath(relative).parts)
        if not executable_within_roots(executable, confined_roots):
            return (
                InstallationState.FAILED,
                spec,
                "managed executable is not readable in the confined environment",
            )
    return InstallationState.INSTALLED, spec, "verified"


async def record_installation(
    session: AsyncSession,
    *,
    probe: tuple[InstallationState, ToolchainSpec | None, str],
    capability_key: str,
    installation_root: Path,
    node_id: str,
    environment_hash: str | None,
    health_ttl_seconds: int,
    now: datetime | None = None,
) -> CapabilityInstallation:
    state, spec, detail = probe
    moment = now or datetime.now(UTC)
    lock_key = int.from_bytes(
        hashlib.sha256(f"installation:{node_id}:{capability_key}".encode()).digest()[:8],
        signed=True,
    )
    await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
    row = await session.scalar(
        select(CapabilityInstallation)
        .where(
            CapabilityInstallation.capability_key == capability_key,
            CapabilityInstallation.node_id == node_id,
        )
        .with_for_update()
    )
    if row is None:
        row = CapabilityInstallation(
            capability_key=capability_key,
            node_id=node_id,
            installation_root=str(installation_root),
            version=spec.version if spec is not None else "unknown",
            status=state.value,
            probe_status=("healthy" if state is InstallationState.INSTALLED else "failed"),
            detail=detail[:500],
        )
        session.add(row)
    row.version = spec.version if spec is not None else row.version
    row.status = state.value
    row.receipt_hash = receipt_digest(spec) if spec is not None else None
    row.environment_hash = environment_hash
    row.installation_root = str(installation_root)
    row.probe_status = "healthy" if state is InstallationState.INSTALLED else "failed"
    row.probed_at = moment
    row.health_until = (
        moment + timedelta(seconds=health_ttl_seconds)
        if state is InstallationState.INSTALLED
        else None
    )
    row.detail = detail[:500]
    await session.flush()
    return row


async def installation_states(
    session: AsyncSession,
    *,
    node_id: str = "local",
    now: datetime | None = None,
) -> dict[str, str]:
    """Effective status per capability: installed (healthy), stale, failed, unknown."""

    moment = now or datetime.now(UTC)
    rows = (
        await session.scalars(
            select(CapabilityInstallation).where(CapabilityInstallation.node_id == node_id)
        )
    ).all()
    states: dict[str, str] = {}
    for row in rows:
        if row.status == InstallationState.INSTALLED.value:
            if row.probe_status != "healthy":
                states[row.capability_key] = InstallationState.FAILED.value
            elif row.health_until is None or row.health_until <= moment:
                states[row.capability_key] = InstallationState.STALE.value
            else:
                states[row.capability_key] = InstallationState.INSTALLED.value
        else:
            states[row.capability_key] = row.status or InstallationState.UNKNOWN.value
    return states


async def verify_installed_capability(
    session: AsyncSession,
    *,
    root: Path,
    capability_key: str,
    node_id: str,
    health_ttl_seconds: int,
    environment_hash: str | None = None,
) -> ToolchainSpec | None:
    """Resolve the shared inventory, rechecking bytes before sandbox exposure.

    A stale or legacy receipt can be re-verified without installation or human
    approval. A recorded failed installation is attention, never silently healed.
    This does not grant permission to execute an action or access a secret.
    """

    lock_key = int.from_bytes(
        hashlib.sha256(f"installation:{node_id}:{capability_key}".encode()).digest()[:8],
        signed=True,
    )
    await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
    row = await session.scalar(
        select(CapabilityInstallation)
        .where(
            CapabilityInstallation.capability_key == capability_key,
            CapabilityInstallation.node_id == node_id,
        )
        .with_for_update()
    )
    if row is not None and (
        row.status == InstallationState.FAILED.value or row.probe_status == "failed"
    ):
        raise ValueError(f"registered capability is not healthy: {capability_key}")
    if row is not None and row.installation_root != str(root):
        raise ValueError(f"registered capability belongs to another root: {capability_key}")
    state, spec, detail = probe_toolchain(root, capability_key, confined_roots=(root,))
    if spec is None and row is None:
        if _installation_target_exists(root, capability_key):
            raise ValueError(f"capability installation is incomplete: {capability_key}")
        return None
    if spec is None:
        raise ValueError(f"registered capability receipt is missing or invalid: {capability_key}")
    if (
        row is not None
        and spec is not None
        and row.receipt_hash
        not in {
            None,
            receipt_digest(spec),
        }
    ):
        state = InstallationState.FAILED
        detail = "managed toolchain receipt differs from the registered installation"
    await record_installation(
        session,
        probe=(state, spec, detail),
        capability_key=capability_key,
        installation_root=root,
        node_id=node_id,
        environment_hash=environment_hash,
        health_ttl_seconds=health_ttl_seconds,
    )
    return spec if state is InstallationState.INSTALLED else None


def _installation_target_exists(root: Path, capability_key: str) -> bool:
    return root.is_dir() and contained(root, root / capability_key, must_exist=False).exists()


async def pin_capability(
    session: AsyncSession, *, run_id: uuid.UUID, spec: ToolchainSpec
) -> CapabilityRunPin:
    """Pin the exact verified toolchain version a run resolved (idempotent)."""

    digest = receipt_digest(spec)
    existing = await session.scalar(
        select(CapabilityRunPin).where(
            CapabilityRunPin.run_id == run_id,
            CapabilityRunPin.capability_key == spec.capability_key,
        )
    )
    if existing is not None:
        return existing
    pin = CapabilityRunPin(
        run_id=run_id,
        capability_key=spec.capability_key,
        version=spec.version,
        receipt_hash=digest,
        pinned_at=datetime.now(UTC),
    )
    session.add(pin)
    await session.flush()
    return pin


async def pin_matches(session: AsyncSession, *, run_id: uuid.UUID, spec: ToolchainSpec) -> bool:
    """A downgrade/upgrade must not silently change an already-pinned run."""

    pin = await session.scalar(
        select(CapabilityRunPin).where(
            CapabilityRunPin.run_id == run_id,
            CapabilityRunPin.capability_key == spec.capability_key,
        )
    )
    if pin is None:
        return True
    return pin.receipt_hash == receipt_digest(spec)


async def enforce_run_pins(
    session: AsyncSession,
    *,
    run_id: uuid.UUID,
    root: Path,
    capability_keys: tuple[str, ...],
) -> None:
    """Pin each resolved toolchain on first use; fail closed on later drift.

    Called by the resolution paths (`execution/codex.py`,
    `execution/dependency_build.py`) before a managed runtime is mounted. A
    missing toolchain is skipped (the caller handles "not installed"); an
    already-pinned toolchain whose receipt hash changed raises.
    """

    for capability_key in sorted(set(capability_keys)):
        spec = load_installed_toolchain(root, capability_key)
        if spec is None:
            continue
        existing = await session.scalar(
            select(CapabilityRunPin).where(
                CapabilityRunPin.run_id == run_id,
                CapabilityRunPin.capability_key == capability_key,
            )
        )
        digest = receipt_digest(spec)
        if existing is None:
            session.add(
                CapabilityRunPin(
                    run_id=run_id,
                    capability_key=capability_key,
                    version=spec.version,
                    receipt_hash=digest,
                    pinned_at=datetime.now(UTC),
                )
            )
            continue
        if existing.receipt_hash != digest:
            raise CapabilityPinMismatch(
                f"capability {capability_key} changed after this run pinned "
                f"version {existing.version}"
            )
    await session.flush()
