"""Audited, default-off Telegram dogfood sessions and one-shot faults."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from vuzol.config import TelegramDogfoodSettings
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    PlanRevision,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import DeliveryStatus


class DogfoodError(ValueError):
    """The requested dogfood operation is outside its explicit safety boundary."""


class DogfoodFault(StrEnum):
    PROVIDER_TIMEOUT_BEFORE_EFFECTS = "provider_timeout_before_effects"
    PROVIDER_QUOTA_BEFORE_EFFECTS = "provider_quota_before_effects"
    TELEGRAM_TRANSIENT_BEFORE_REQUEST = "telegram_transient_before_request"


class DogfoodCase(StrEnum):
    T01 = "T01"
    T02 = "T02"
    T03 = "T03"
    T04 = "T04"
    T05 = "T05"
    T06 = "T06"
    T07 = "T07"
    T08 = "T08"
    T09 = "T09"
    T10 = "T10"
    T11 = "T11"
    T12 = "T12"


class DogfoodCaseResult(StrEnum):
    SUCCESS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(frozen=True, slots=True)
class DogfoodReport:
    session_id: uuid.UUID
    project_id: str
    started_at: datetime
    package_counts: dict[str, int]
    task_counts: dict[str, int]
    armed_faults: int
    consumed_faults: int
    case_results: dict[str, str]
    release_ready: bool

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["session_id"] = str(self.session_id)
        value["started_at"] = self.started_at.isoformat()
        return value


@dataclass(frozen=True, slots=True)
class PackageDiagnostic:
    package_id: uuid.UUID
    project_id: str
    status: str
    pause_reason: str | None
    revision_number: int | None
    cursor_ordinal: int | None
    task_id: uuid.UUID | None
    task_status: str | None
    run_id: uuid.UUID | None
    run_status: str | None
    step_id: uuid.UUID | None
    step_type: str | None
    step_status: str | None
    failure_category: str | None
    failure_summary: str | None
    safe_retry: bool
    outbox_counts: dict[str, int]
    outbox_errors: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            key: str(value) if isinstance(value, uuid.UUID) else value
            for key, value in asdict(self).items()
        }


def _authorize(settings: TelegramDogfoodSettings, project_id: str, *, faults: bool = False) -> None:
    if not settings.enabled or project_id not in settings.allowed_project_ids:
        raise DogfoodError("project is not allowlisted for Telegram dogfood")
    if faults and not settings.fault_injection_enabled:
        raise DogfoodError("Telegram dogfood fault injection is disabled")


async def start_session(
    session: AsyncSession,
    settings: TelegramDogfoodSettings,
    *,
    project_id: str,
    configuration_revision: str,
    git_sha: str,
    actor_id: str,
) -> uuid.UUID:
    _authorize(settings, project_id)
    session_id = uuid.uuid4()
    session.add(
        Event(
            entity_type="telegram_dogfood_session",
            entity_id=session_id,
            event_type="telegram_dogfood.started",
            actor_type="operator",
            actor_id=actor_id,
            correlation_id=str(session_id),
            payload={
                "project_id": project_id,
                "configuration_revision": configuration_revision,
                "git_sha": git_sha,
            },
        )
    )
    await session.flush()
    return session_id


async def arm_fault(
    session: AsyncSession,
    settings: TelegramDogfoodSettings,
    *,
    session_id: uuid.UUID,
    project_id: str,
    fault: DogfoodFault,
    actor_id: str,
) -> uuid.UUID:
    _authorize(settings, project_id, faults=True)
    started = await _session_event(session, session_id)
    if started.payload.get("project_id") != project_id:
        raise DogfoodError("dogfood session belongs to another project")
    fault_id = uuid.uuid4()
    session.add(
        Event(
            entity_type="telegram_dogfood_fault",
            entity_id=fault_id,
            event_type="telegram_dogfood.fault_armed",
            actor_type="operator",
            actor_id=actor_id,
            correlation_id=str(session_id),
            payload={
                "session_id": str(session_id),
                "project_id": project_id,
                "fault": fault.value,
            },
        )
    )
    await session.flush()
    return fault_id


async def record_case_result(
    session: AsyncSession,
    settings: TelegramDogfoodSettings,
    *,
    session_id: uuid.UUID,
    case: DogfoodCase,
    result: DogfoodCaseResult,
    actor_id: str,
    package_id: uuid.UUID | None = None,
) -> None:
    """Record bounded human UI evidence without accepting free-form or secret-bearing text."""

    started = await _session_event(session, session_id)
    project_id = str(started.payload["project_id"])
    _authorize(settings, project_id)
    if package_id is not None:
        package = await session.get(WorkPackage, package_id)
        if package is None or package.project_id != project_id:
            raise DogfoodError("checkpoint package belongs to another project")
    session.add(
        Event(
            entity_type="telegram_dogfood_case",
            entity_id=uuid.uuid4(),
            event_type="telegram_dogfood.case_recorded",
            actor_type="operator",
            actor_id=actor_id,
            correlation_id=str(session_id),
            payload={
                "case": case.value,
                "result": result.value,
                "project_id": project_id,
                "package_id": None if package_id is None else str(package_id),
            },
        )
    )
    await session.flush()


async def consume_fault(
    session: AsyncSession,
    settings: TelegramDogfoodSettings,
    *,
    project_id: str,
    fault: DogfoodFault,
    consumer: str,
) -> uuid.UUID | None:
    """Consume at most one armed fault under a row lock; concurrent consumers fail closed."""

    if not settings.fault_injection_enabled or project_id not in settings.allowed_project_ids:
        return None
    consumed = aliased(Event)
    armed = await session.scalar(
        select(Event)
        .where(
            Event.entity_type == "telegram_dogfood_fault",
            Event.event_type == "telegram_dogfood.fault_armed",
            Event.payload["project_id"].as_string() == project_id,
            Event.payload["fault"].as_string() == fault.value,
            ~exists(
                select(consumed.id).where(
                    consumed.entity_type == "telegram_dogfood_fault",
                    consumed.entity_id == Event.entity_id,
                    consumed.event_type == "telegram_dogfood.fault_consumed",
                )
            ),
        )
        .order_by(Event.created_at, Event.id)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    if armed is None:
        return None
    session.add(
        Event(
            entity_type="telegram_dogfood_fault",
            entity_id=armed.entity_id,
            event_type="telegram_dogfood.fault_consumed",
            actor_type="system",
            actor_id=consumer,
            correlation_id=armed.correlation_id,
            previous_state="armed",
            new_state="consumed",
            payload={"fault": fault.value, "project_id": project_id},
        )
    )
    await session.flush()
    return armed.entity_id


async def build_report(
    session: AsyncSession,
    settings: TelegramDogfoodSettings,
    session_id: uuid.UUID,
) -> DogfoodReport:
    started = await _session_event(session, session_id)
    project_id = str(started.payload["project_id"])
    _authorize(settings, project_id)
    package_rows = await session.execute(
        select(WorkPackage.status, func.count())
        .where(WorkPackage.project_id == project_id, WorkPackage.created_at >= started.created_at)
        .group_by(WorkPackage.status)
    )
    task_rows = await session.execute(
        select(Task.status, func.count())
        .where(Task.project_id == project_id, Task.created_at >= started.created_at)
        .group_by(Task.status)
    )
    fault_rows = await session.execute(
        select(Event.event_type, func.count())
        .where(
            Event.entity_type == "telegram_dogfood_fault",
            Event.correlation_id == str(session_id),
        )
        .group_by(Event.event_type)
    )
    faults = {str(kind): int(count) for kind, count in fault_rows}
    case_rows = await session.execute(
        select(Event)
        .where(
            Event.entity_type == "telegram_dogfood_case",
            Event.event_type == "telegram_dogfood.case_recorded",
            Event.correlation_id == str(session_id),
        )
        .order_by(Event.created_at, Event.id)
    )
    case_results: dict[str, str] = {}
    for event in case_rows.scalars():
        case_results[str(event.payload["case"])] = str(event.payload["result"])
    expected_cases = {case.value for case in DogfoodCase}
    release_ready = (
        set(case_results) == expected_cases
        and set(case_results.values()) == {DogfoodCaseResult.SUCCESS.value}
        and faults.get("telegram_dogfood.fault_armed", 0)
        == faults.get("telegram_dogfood.fault_consumed", 0)
    )
    return DogfoodReport(
        session_id=session_id,
        project_id=project_id,
        started_at=started.created_at.astimezone(UTC),
        package_counts={status.value: int(count) for status, count in package_rows},
        task_counts={status.value: int(count) for status, count in task_rows},
        armed_faults=faults.get("telegram_dogfood.fault_armed", 0),
        consumed_faults=faults.get("telegram_dogfood.fault_consumed", 0),
        case_results=case_results,
        release_ready=release_ready,
    )


async def diagnose_package(
    session: AsyncSession,
    settings: TelegramDogfoodSettings,
    package_id: uuid.UUID,
) -> PackageDiagnostic:
    package = await session.get(WorkPackage, package_id)
    if package is None:
        raise DogfoodError("work package not found")
    _authorize(settings, package.project_id)
    revision_number = None
    if package.head_revision_id is not None:
        revision_number = await session.scalar(
            select(PlanRevision.revision_number).where(PlanRevision.id == package.head_revision_id)
        )
    link = None
    if package.cursor_ordinal is not None:
        link = await session.scalar(
            select(MaterializationLink)
            .join(PlanRevision, PlanRevision.id == MaterializationLink.plan_revision_id)
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.ordinal == package.cursor_ordinal,
            )
            .order_by(PlanRevision.revision_number.desc())
            .limit(1)
        )
    task = None if link is None else await session.get(Task, link.task_id)
    run = None
    step = None
    if task is not None:
        run = await session.scalar(
            select(Run).where(Run.task_id == task.id).order_by(Run.created_at.desc()).limit(1)
        )
    if run is not None:
        step = await session.scalar(
            select(Step).where(Step.run_id == run.id).order_by(Step.ordinal.desc()).limit(1)
        )
    from vuzol.workflows.retry_policy import blocked_step_is_retryable

    linked_ids = [package.id]
    if task is not None:
        linked_ids.append(task.id)
    outbox_rows = await session.execute(
        select(
            TransactionalOutbox.destination,
            TransactionalOutbox.status,
            TransactionalOutbox.last_error_category,
            func.count(),
        )
        .where(
            TransactionalOutbox.linked_entity_id.in_(linked_ids),
            TransactionalOutbox.status != DeliveryStatus.DELIVERED,
        )
        .group_by(
            TransactionalOutbox.destination,
            TransactionalOutbox.status,
            TransactionalOutbox.last_error_category,
        )
        .order_by(TransactionalOutbox.destination, TransactionalOutbox.status)
    )
    outbox_counts: dict[str, int] = {}
    outbox_errors: dict[str, int] = {}
    for destination, status, category, count in outbox_rows:
        count = int(count)
        status_key = f"{destination}:{status.value}"
        outbox_counts[status_key] = outbox_counts.get(status_key, 0) + count
        if category is not None:
            error_key = f"{destination}:{category}"
            outbox_errors[error_key] = outbox_errors.get(error_key, 0) + count

    return PackageDiagnostic(
        package_id=package.id,
        project_id=package.project_id,
        status=package.status.value,
        pause_reason=None if package.pause_reason is None else package.pause_reason.value,
        revision_number=revision_number,
        cursor_ordinal=package.cursor_ordinal,
        task_id=None if task is None else task.id,
        task_status=None if task is None else task.status.value,
        run_id=None if run is None else run.id,
        run_status=None if run is None else run.status.value,
        step_id=None if step is None else step.id,
        step_type=None if step is None else step.step_type,
        step_status=None if step is None else step.status.value,
        failure_category=None if step is None else step.failure_category,
        failure_summary=None if step is None else step.failure_summary,
        safe_retry=step is not None and blocked_step_is_retryable(step),
        outbox_counts=outbox_counts,
        outbox_errors=outbox_errors,
    )


async def _session_event(session: AsyncSession, session_id: uuid.UUID) -> Event:
    event = await session.scalar(
        select(Event).where(
            Event.entity_type == "telegram_dogfood_session",
            Event.entity_id == session_id,
            Event.event_type == "telegram_dogfood.started",
        )
    )
    if event is None:
        raise DogfoodError("Telegram dogfood session not found")
    return event
