"""Versioned project environment contracts bound to approved plan revisions."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import PlanRevision, ProjectEnvironmentRevision


def environment_hash(contract: dict[str, object]) -> str:
    encoded = json.dumps(contract, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


async def current_environment(
    session: AsyncSession, project_id: str, *, for_update: bool = False
) -> ProjectEnvironmentRevision | None:
    statement = (
        select(ProjectEnvironmentRevision)
        .where(ProjectEnvironmentRevision.project_id == project_id)
        .order_by(ProjectEnvironmentRevision.revision_number.desc())
        .limit(1)
    )
    if for_update:
        statement = statement.with_for_update()
    revision = await session.scalar(statement)
    return revision if isinstance(revision, ProjectEnvironmentRevision) else None


async def apply_approved_environment_delta(
    session: AsyncSession,
    *,
    project_id: str,
    plan_revision: PlanRevision,
    approved_by_user_id: int,
) -> ProjectEnvironmentRevision | None:
    """Atomically append the environment declared by one approved plan revision."""

    raw_delta = plan_revision.immutable_body.get("environment_delta")
    if not isinstance(raw_delta, dict):
        return None
    existing_for_plan = await session.scalar(
        select(ProjectEnvironmentRevision).where(
            ProjectEnvironmentRevision.source_plan_revision_id == plan_revision.id
        )
    )
    if existing_for_plan is not None:
        return existing_for_plan
    lock_key = int.from_bytes(
        hashlib.sha256(f"environment:{project_id}".encode()).digest()[:8], signed=True
    )
    await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
    parent = await current_environment(session, project_id, for_update=True)
    contract = _merge_environment(
        {} if parent is None else parent.contract,
        raw_delta,
    )
    revision = ProjectEnvironmentRevision(
        project_id=project_id,
        revision_number=1 if parent is None else parent.revision_number + 1,
        parent_revision_id=None if parent is None else parent.id,
        source="plan_approval",
        source_plan_revision_id=plan_revision.id,
        contract=contract,
        content_hash=environment_hash(contract),
        approved_by_user_id=approved_by_user_id,
    )
    session.add(revision)
    await session.flush()
    return revision


async def record_detected_environment(
    session: AsyncSession,
    *,
    project_id: str,
    repository: Path,
) -> ProjectEnvironmentRevision:
    """Persist an idempotent conservative baseline observed during import."""

    lock_key = int.from_bytes(
        hashlib.sha256(f"environment:{project_id}".encode()).digest()[:8], signed=True
    )
    await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
    current = await current_environment(session, project_id, for_update=True)
    if current is not None:
        return current
    contract = detect_environment_contract(repository)
    revision = ProjectEnvironmentRevision(
        project_id=project_id,
        revision_number=1,
        source="detected",
        contract=contract,
        content_hash=environment_hash(contract),
    )
    session.add(revision)
    await session.flush()
    return revision


def detect_environment_contract(repository: Path) -> dict[str, object]:
    """Detect high-confidence components without installing or executing project code."""

    components: dict[str, dict[str, object]] = {}
    capabilities: dict[str, dict[str, object]] = {}
    if (repository / "server.js").is_file():
        components["web"] = {
            "key": "web",
            "label": "Web service",
            "kind": "web_service",
            "technology": "Node.js",
            "version": None,
            "run_command": ["node", "server.js"],
            "port": 8080,
            "healthcheck_path": "/",
            "artifact_patterns": [],
            "confidence": "detected",
        }
        capabilities["node-runtime"] = _detected_capability("node-runtime", "Node.js runtime")
    elif (repository / "index.html").is_file():
        components["web"] = {
            "key": "web",
            "label": "Static site",
            "kind": "static_site",
            "technology": "HTML",
            "version": None,
            "run_command": [],
            "port": None,
            "healthcheck_path": None,
            "artifact_patterns": ["**/*"],
            "confidence": "detected",
        }
    if (repository / "gradlew").is_file() and any(repository.glob("**/build.gradle*")):
        components["android"] = {
            "key": "android",
            "label": "Android application",
            "kind": "android_app",
            "technology": "Gradle",
            "version": None,
            "run_command": [],
            "port": None,
            "healthcheck_path": None,
            "artifact_patterns": ["**/build/outputs/apk/**/*.apk"],
            "confidence": "detected",
        }
        capabilities["android-sdk"] = _detected_capability("android-sdk", "Android SDK")
    if (repository / "pyproject.toml").is_file():
        capabilities["python-runtime"] = _detected_capability("python-runtime", "Python runtime")
    return {
        "schema_version": "project-environment.v1",
        "components": components,
        "capabilities": capabilities,
    }


def _detected_capability(key: str, label: str) -> dict[str, object]:
    return {
        "key": key,
        "label": label,
        "provisioning": "automatic",
        "reason": "Detected from the imported repository",
    }


def _merge_environment(previous: dict[str, object], delta: dict[str, object]) -> dict[str, object]:
    previous_components = previous.get("components", {})
    previous_capabilities = previous.get("capabilities", {})
    components = dict(previous_components) if isinstance(previous_components, dict) else {}
    capabilities = dict(previous_capabilities) if isinstance(previous_capabilities, dict) else {}
    removed = delta.get("remove_components", [])
    if isinstance(removed, list):
        for key in removed:
            if isinstance(key, str):
                components.pop(key, None)
    upserts = delta.get("upsert_components", [])
    if isinstance(upserts, list):
        for component in upserts:
            if isinstance(component, dict) and isinstance(component.get("key"), str):
                components[str(component["key"])] = dict(component)
    requirements = delta.get("required_capabilities", [])
    if isinstance(requirements, list):
        for capability in requirements:
            if isinstance(capability, dict) and isinstance(capability.get("key"), str):
                capabilities[str(capability["key"])] = dict(capability)
    return {
        "schema_version": "project-environment.v1",
        "components": {key: components[key] for key in sorted(components)},
        "capabilities": {key: capabilities[key] for key in sorted(capabilities)},
    }
