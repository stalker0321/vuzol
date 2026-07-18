"""Project-scoped executor worker preference (auto routing or pinned worker)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.config.registries import ConfigurationBundle, RegistryError
from vuzol.storage.models import ProjectExecutorPreference

PREFERENCE_SCHEMA_VERSION = "project-executor-preference.v1"
DEFAULT_CODEX_MODEL_FAMILY = "gpt-5.6"
DEFAULT_REASONING_EFFORT = "medium"
REASONING_EFFORTS: tuple[str, ...] = ("low", "medium", "high", "xhigh")
CODEX_WORKERS: tuple[str, ...] = ("sol", "terra", "luna")
ALL_WORKERS: tuple[str, ...] = (*CODEX_WORKERS, "grok")

PAYLOAD_MODEL_OVERRIDE = "executor_model_override"
PAYLOAD_EFFORT_OVERRIDE = "executor_reasoning_effort"
PAYLOAD_WORKER_KEY = "executor_worker_key"
PAYLOAD_PREFERENCE_MODE = "executor_preference_mode"


class ExecutorPreferenceMode(StrEnum):
    AUTO = "auto"
    PIN = "pin"


class ExecutorWorkerKey(StrEnum):
    SOL = "sol"
    TERRA = "terra"
    LUNA = "luna"
    GROK = "grok"


class ExecutorPreferenceError(RuntimeError):
    """Safe user-facing rejection of a stale or invalid model preference action."""


@dataclass(frozen=True, slots=True)
class ExecutorPreferenceView:
    project_id: str
    mode: ExecutorPreferenceMode
    worker_key: ExecutorWorkerKey | None
    reasoning_effort: str | None
    revision: int

    @property
    def is_auto(self) -> bool:
        return self.mode is ExecutorPreferenceMode.AUTO


@dataclass(frozen=True, slots=True)
class ExecutorRoutePin:
    """Resolved routing pin for one provider claim."""

    trusted_profile_id: str
    model_override: str | None
    reasoning_effort: str | None
    worker_key: ExecutorWorkerKey
    allow_same_family_fallbacks: bool = True


@dataclass(frozen=True, slots=True)
class WorkerOption:
    key: ExecutorWorkerKey
    label: str
    supports_reasoning_effort: bool


def default_preference(project_id: str) -> ExecutorPreferenceView:
    return ExecutorPreferenceView(
        project_id=project_id,
        mode=ExecutorPreferenceMode.AUTO,
        worker_key=None,
        reasoning_effort=None,
        revision=1,
    )


def preference_from_row(row: ProjectExecutorPreference) -> ExecutorPreferenceView:
    mode = ExecutorPreferenceMode(row.mode)
    worker = ExecutorWorkerKey(row.worker_key) if row.worker_key else None
    return ExecutorPreferenceView(
        project_id=row.project_id,
        mode=mode,
        worker_key=worker,
        reasoning_effort=row.reasoning_effort,
        revision=row.revision,
    )


async def load_preference(session: AsyncSession, project_id: str) -> ExecutorPreferenceView:
    row = await session.get(ProjectExecutorPreference, project_id)
    if row is None:
        return default_preference(project_id)
    return preference_from_row(row)


async def ensure_preference_row(
    session: AsyncSession, project_id: str
) -> ProjectExecutorPreference:
    row = await session.get(ProjectExecutorPreference, project_id, with_for_update=True)
    if row is not None:
        return row
    row = ProjectExecutorPreference(
        project_id=project_id,
        mode=ExecutorPreferenceMode.AUTO.value,
        worker_key=None,
        reasoning_effort=None,
        revision=1,
    )
    session.add(row)
    await session.flush()
    return row


async def set_auto_preference(
    session: AsyncSession,
    *,
    project_id: str,
    user_id: int,
    expected_revision: int,
) -> ExecutorPreferenceView:
    row = await ensure_preference_row(session, project_id)
    if row.revision != expected_revision:
        raise ExecutorPreferenceError("model options are stale; send /model again")
    row.mode = ExecutorPreferenceMode.AUTO.value
    row.worker_key = None
    row.reasoning_effort = None
    row.revision += 1
    row.updated_by_user_id = user_id
    await session.flush()
    return preference_from_row(row)


async def set_worker_preference(
    session: AsyncSession,
    *,
    project_id: str,
    user_id: int,
    expected_revision: int,
    worker_key: ExecutorWorkerKey,
    reasoning_effort: str | None,
    registries: ConfigurationBundle,
) -> ExecutorPreferenceView:
    available = available_workers(registries)
    if worker_key not in {option.key for option in available}:
        raise ExecutorPreferenceError("that worker is not available")
    option = next(item for item in available if item.key is worker_key)
    if option.supports_reasoning_effort:
        if reasoning_effort is None or reasoning_effort not in REASONING_EFFORTS:
            raise ExecutorPreferenceError("reasoning effort is required for this worker")
    elif reasoning_effort is not None:
        raise ExecutorPreferenceError("this worker does not support reasoning effort")
    row = await ensure_preference_row(session, project_id)
    if row.revision != expected_revision:
        raise ExecutorPreferenceError("model options are stale; send /model again")
    row.mode = ExecutorPreferenceMode.PIN.value
    row.worker_key = worker_key.value
    row.reasoning_effort = reasoning_effort
    row.revision += 1
    row.updated_by_user_id = user_id
    await session.flush()
    return preference_from_row(row)


def available_workers(registries: ConfigurationBundle) -> tuple[WorkerOption, ...]:
    """Product worker identities currently backed by an enabled executor profile."""

    profiles = tuple(
        profile
        for profile in registries.profiles.items()
        if profile.enabled and ProviderRole.EXECUTOR in profile.roles
    )
    options: list[WorkerOption] = []
    has_codex = any(
        profile.provider == "codex" and profile.launch_mode is LaunchMode.CLI
        for profile in profiles
    )
    has_grok = any(
        profile.provider == "grok" and profile.launch_mode is LaunchMode.CLI for profile in profiles
    )
    if has_codex:
        options.extend(
            WorkerOption(
                key=ExecutorWorkerKey(key),
                label=_worker_label(key),
                supports_reasoning_effort=True,
            )
            for key in CODEX_WORKERS
        )
    if has_grok:
        options.append(
            WorkerOption(
                key=ExecutorWorkerKey.GROK,
                label="Grok",
                supports_reasoning_effort=False,
            )
        )
    return tuple(options)


def resolve_route_pin(
    preference: ExecutorPreferenceView,
    registries: ConfigurationBundle,
) -> ExecutorRoutePin | None:
    """Map a project preference to a trusted executor profile + optional model overrides."""

    if preference.is_auto or preference.worker_key is None:
        return None
    worker = preference.worker_key
    if worker is ExecutorWorkerKey.GROK:
        profile = _first_enabled_executor(registries, provider="grok")
        if profile is None:
            return None
        return ExecutorRoutePin(
            trusted_profile_id=profile.id,
            model_override=None,
            reasoning_effort=None,
            worker_key=worker,
        )
    profile = _first_enabled_executor(registries, provider="codex")
    if profile is None:
        return None
    return ExecutorRoutePin(
        trusted_profile_id=profile.id,
        model_override=codex_model_for_worker(profile.model, worker),
        reasoning_effort=preference.reasoning_effort or DEFAULT_REASONING_EFFORT,
        worker_key=worker,
    )


def same_family_fallback_ids(
    registries: ConfigurationBundle,
    *,
    worker_key: ExecutorWorkerKey,
    primary_profile_id: str,
) -> tuple[str, ...]:
    """When pinned, only allow fallbacks inside the same worker family."""

    primary = registries.profiles.get(primary_profile_id)
    provider = "grok" if worker_key is ExecutorWorkerKey.GROK else "codex"
    allowed: list[str] = []
    for profile_id in primary.fallback_profile_ids:
        try:
            candidate = registries.profiles.get(profile_id)
        except RegistryError:
            continue
        if (
            candidate.enabled
            and ProviderRole.EXECUTOR in candidate.roles
            and candidate.provider == provider
            and candidate.launch_mode is LaunchMode.CLI
        ):
            allowed.append(profile_id)
    return tuple(allowed)


_ALLOWED_EFFORT_OVERRIDES: frozenset[str] = frozenset((*REASONING_EFFORTS, "max", "ultra"))


def apply_profile_overrides(
    profile: ProviderProfileConfig,
    payload: dict[str, object],
) -> ProviderProfileConfig:
    """Apply claim-time model/effort overrides without mutating the registry profile.

    Invalid override values fail closed rather than silently widening the command surface.
    Non-mapping payloads are ignored so handler unit fixtures stay override-neutral.
    """

    if not isinstance(payload, dict):
        return profile
    updates: dict[str, object] = {}
    if PAYLOAD_MODEL_OVERRIDE in payload:
        model = payload.get(PAYLOAD_MODEL_OVERRIDE)
        cleaned = model.strip() if isinstance(model, str) else ""
        if not cleaned or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", cleaned):
            raise ValueError("executor model override is invalid")
        updates["model"] = cleaned
    if PAYLOAD_EFFORT_OVERRIDE in payload:
        effort = payload.get(PAYLOAD_EFFORT_OVERRIDE)
        if effort is None:
            updates["model_reasoning_effort"] = None
        elif isinstance(effort, str) and effort.strip() in _ALLOWED_EFFORT_OVERRIDES:
            updates["model_reasoning_effort"] = effort.strip()
        else:
            raise ValueError("executor reasoning effort override is invalid")
    if not updates:
        return profile
    return profile.model_copy(update=updates)


def preference_payload(pin: ExecutorRoutePin) -> dict[str, object]:
    payload: dict[str, object] = {
        PAYLOAD_PREFERENCE_MODE: ExecutorPreferenceMode.PIN.value,
        PAYLOAD_WORKER_KEY: pin.worker_key.value,
    }
    if pin.model_override is not None:
        payload[PAYLOAD_MODEL_OVERRIDE] = pin.model_override
    if pin.reasoning_effort is not None:
        payload[PAYLOAD_EFFORT_OVERRIDE] = pin.reasoning_effort
    return payload


def codex_model_for_worker(base_model: str, worker: ExecutorWorkerKey) -> str:
    """Derive Sol/Terra/Luna model slug from the configured Codex profile model."""

    slug = (base_model or "").strip()
    variant = worker.value
    if not slug or slug.lower() in {"codex", "auto"}:
        return f"{DEFAULT_CODEX_MODEL_FAMILY}-{variant}"
    lowered = slug.lower()
    for known in CODEX_WORKERS:
        if lowered.endswith(f"-{known}") or lowered == known:
            if lowered == known:
                return f"{DEFAULT_CODEX_MODEL_FAMILY}-{variant}"
            return re.sub(rf"-{known}$", f"-{variant}", slug, count=1, flags=re.IGNORECASE)
    if re.fullmatch(r"gpt-\d+(?:\.\d+)*", lowered):
        return f"{slug}-{variant}"
    return f"{DEFAULT_CODEX_MODEL_FAMILY}-{variant}"


def format_preference_label(view: ExecutorPreferenceView) -> str:
    if view.is_auto or view.worker_key is None:
        return "Routing (automatic)"
    label = _worker_label(view.worker_key.value)
    if view.worker_key is ExecutorWorkerKey.GROK:
        return label
    effort = view.reasoning_effort or DEFAULT_REASONING_EFFORT
    return f"{label} · {effort}"


def worker_callback_data(revision: int, worker: ExecutorWorkerKey) -> str:
    return f"v1:pm:w:{revision}:{worker.value}"


def effort_callback_data(revision: int, worker: ExecutorWorkerKey, effort: str) -> str:
    return f"v1:pm:e:{revision}:{worker.value}:{effort}"


def auto_callback_data(revision: int) -> str:
    return f"v1:pm:a:{revision}"


def _worker_label(key: str) -> str:
    return {
        "sol": "Sol",
        "terra": "Terra",
        "luna": "Luna",
        "grok": "Grok",
    }.get(key, key)


def _first_enabled_executor(
    registries: ConfigurationBundle, *, provider: str
) -> ProviderProfileConfig | None:
    candidates = [
        profile
        for profile in registries.profiles.items()
        if profile.enabled
        and profile.provider == provider
        and profile.launch_mode is LaunchMode.CLI
        and ProviderRole.EXECUTOR in profile.roles
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: (item.routing_priority, item.id))[0]
