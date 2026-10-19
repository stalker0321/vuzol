"""Replace misrouted, never-executed chat work without rewriting its history.

The caller obtains a fresh, accounted semantic result against the immutable
source/spec. This operation is deliberately manual-start only, idempotent and
unavailable once a provider invocation or uncertain effect exists.
"""

import hashlib
import uuid
from typing import Any

from sqlalchemy import select

from vuzol.config import RuntimeConfiguration, TopicKind
from vuzol.discussion.work_context import load_work_context
from vuzol.interpretation.domain import InterpretationInput, InterpretationResult, TaskAction
from vuzol.interpretation.planning import PLANNING_TIER_EVENT, resolve_planning_tier
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.work_contract import WORK_CONTRACT_VERSION
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import (
    ConversationTurn,
    Effect,
    Event,
    Interpretation,
    MaterializationLink,
    ProjectDiscussionSession,
    ProviderBudgetReservation,
    Run,
    Step,
    SupervisedProcess,
    Task,
    TaskSpecRevision,
    UsageRecord,
    Worktree,
)
from vuzol.storage.types import (
    ConversationTurnRole,
    DiscussionSessionStatus,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import enqueue_task_status_projection
from vuzol.workflows.application import Principal, validate_principal
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

RECLASSIFIED_EVENT = "task.unstarted_work_reclassified"


async def replace_unstarted_chat_work(
    uow: UnitOfWork,
    runtime: RuntimeConfiguration,
    *,
    task_id: uuid.UUID,
    expected_version: int,
    expected_spec_revision: str,
    principal: Principal,
    result: InterpretationResult,
    receipt: dict[str, Any],
) -> uuid.UUID:
    """Create a new, manual-start task from the same verified discussion source."""
    validate_principal(principal)
    assert uow.session is not None
    session = uow.session
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None or task.user_id != principal.user_id:
        raise ValueError("reclassification_owner_mismatch")
    previous = await session.scalar(
        select(Event).where(
            Event.entity_type == "task",
            Event.entity_id == task_id,
            Event.event_type == RECLASSIFIED_EVENT,
        )
    )
    if previous is not None:
        return uuid.UUID(str(previous.payload["new_task_id"]))
    if (
        task.version != expected_version
        or task.spec_revision != expected_spec_revision
        or task.status is not TaskStatus.BLOCKED
        or task.ingress_source != "chat_work"
        or task.source_turn_id is None
        or task.project_id is None
    ):
        raise ValueError("reclassification_stale_or_unavailable")
    topic = runtime.registries.topics.resolve(task.source_chat_id or 0, task.source_thread_id or 0)
    if (
        not topic.enabled
        or topic.kind is not TopicKind.PROJECT
        or topic.project_id != task.project_id
    ):
        raise ValueError("reclassification_topic_mismatch")
    source = await session.get(ConversationTurn, task.source_turn_id)
    if source is None:
        raise ValueError("reclassification_source_missing")
    discussion = await session.scalar(
        select(ProjectDiscussionSession)
        .where(
            ProjectDiscussionSession.id == source.session_id,
        )
        .with_for_update()
    )
    if (
        discussion is None
        or discussion.status is not DiscussionSessionStatus.ACTIVE
        or (discussion.chat_id, discussion.message_thread_id, discussion.project_id)
        != (task.source_chat_id, task.source_thread_id, task.project_id)
    ):
        raise ValueError("reclassification_source_mismatch")
    latest_source = await session.scalar(
        select(ConversationTurn.id)
        .where(
            ConversationTurn.session_id == source.session_id,
            ConversationTurn.role == ConversationTurnRole.USER,
        )
        .order_by(ConversationTurn.ordinal.desc())
        .limit(1)
    )
    latest_task = await session.scalar(
        select(Task.id)
        .where(
            Task.source_turn_id == source.id,
        )
        .order_by(Task.created_at.desc())
        .limit(1)
    )
    if latest_source != source.id or latest_task != task.id:
        raise ValueError("reclassification_stale_source")
    runs = (
        await session.scalars(select(Run).where(Run.task_id == task.id).with_for_update())
    ).all()
    if (
        len(runs) != 1
        or runs[0].status is not RunStatus.BLOCKED
        or runs[0].failure_category
        not in {
            "no_compatible_profile",
            "executor_route_unavailable",
        }
    ):
        raise ValueError("reclassification_not_a_route_failure")
    run = runs[0]
    steps = (
        await session.scalars(select(Step).where(Step.run_id == run.id).with_for_update())
    ).all()
    if any(
        step.unknown_effects
        or step.status in {StepStatus.RUNNING, StepStatus.LEASED}
        or (
            step.attempt_count
            and step.step_type not in {"interpret", "prepare_context", "prepare_worktree"}
        )
        for step in steps
    ):
        raise ValueError("reclassification_work_already_started")
    for model in (SupervisedProcess, UsageRecord, ProviderBudgetReservation, MaterializationLink):
        if (
            await session.scalar(select(model.id).where(model.task_id == task.id).limit(1))
            is not None
        ):
            raise ValueError("reclassification_has_effect_or_invocation")
    if (
        await session.scalar(
            select(Effect.id)
            .where((Effect.task_id == task.id) | Effect.step_id.in_([step.id for step in steps]))
            .limit(1)
        )
        is not None
    ):
        raise ValueError("reclassification_has_effect_or_invocation")
    # Check hashes and source membership before copying the frozen evidence.
    if not await load_work_context(session, task, expected_revision=expected_spec_revision):
        raise ValueError("reclassification_context_missing")
    spec = await session.scalar(
        select(TaskSpecRevision).where(
            TaskSpecRevision.task_id == task.id,
            TaskSpecRevision.spec_revision == expected_spec_revision,
        )
    )
    assert spec is not None
    selected = receipt.get("selected_choices", {})
    if (
        receipt.get("effect") != "execute_request"
        or selected.get("effect") != "execute_request"
        or receipt.get("abstain")
        or receipt.get("profile_id") != result.profile_id
        or receipt.get("model") != result.model
        or receipt.get("work_contract", {}).get("version") != WORK_CONTRACT_VERSION
        or receipt.get("work_contract", {}).get("error") is not None
        or selected.get("task_type") != result.draft.task_type.value
        or selected.get("operation") != result.draft.operation.value
        or result.draft.action is not TaskAction.CREATE_TASK
        or result.draft.project_id != task.project_id
        or result.draft.goal != task.original_text.strip()
    ):
        raise ValueError("reclassification_result_mismatch")
    policy = enforce_interpretation_policy(
        InterpretationInput(
            original_input=task.original_text,
            topic_kind=TopicKind.PROJECT,
            mapped_project_id=task.project_id,
            conversational_context=True,
            capability_vocabulary=frozenset(),
        ),
        result.draft,
        known_project_ids=frozenset(p.id for p in runtime.registries.projects.items()),
        semantic_effect="execute_request",
    )
    if not policy.automatic_execution_eligible or policy.draft.needs_clarification:
        raise ValueError("reclassification_needs_clarification")
    # Keep every old interpretation, spec, step and worktree. No budget reset.
    for step in steps:
        if step.status in {StepStatus.PENDING, StepStatus.QUEUED, StepStatus.BLOCKED}:
            await transition_step(
                session,
                step,
                StepStatus.CANCELLED,
                actor_type="user",
                actor_id=str(principal.user_id),
            )
    await transition_run(
        session, run, RunStatus.CANCELLED, actor_type="user", actor_id=str(principal.user_id)
    )
    await transition_task(
        session, task, TaskStatus.CANCELLED, actor_type="user", actor_id=str(principal.user_id)
    )
    await enqueue_task_status_projection(session, task, run)
    worktrees = (await session.scalars(select(Worktree).where(Worktree.task_id == task.id))).all()
    for worktree in worktrees:
        if worktree.delivery_state is WorktreeDeliveryState.ACTIVE:
            worktree.delivery_state = WorktreeDeliveryState.WORKTREE_RETAINED
    created = await uow.tasks.create(
        user_id=task.user_id,
        chat_id=task.source_chat_id,
        thread_id=task.source_thread_id,
        project_id=task.project_id,
        original_text=task.original_text,
        task_type=policy.draft.task_type.value,
        task_draft=policy.draft.model_dump(mode="json"),
        ingress_source="chat_work",
        source_turn_id=source.id,
    )
    fresh = await session.get(Task, created.id)
    assert fresh is not None
    fresh.parent_task_id = task.id
    fresh.status = TaskStatus.INTERPRETED
    fresh.risk = policy.draft.suggested_risk
    fresh.interpreter_profile = result.profile_id
    fresh.prompt_version = result.prompt_version
    fresh.draft_schema_version = result.schema_version
    await snapshot_task_spec(session, fresh, chat_work_context=spec.spec["chat_work_context"])
    interpretation = Interpretation(
        task_id=fresh.id,
        original_input_hash=hashlib.sha256(task.original_text.encode()).hexdigest(),
        task_draft=dict(fresh.task_draft),
        profile_id=result.profile_id,
        model=result.model,
        prompt_version=result.prompt_version,
        schema_version=result.schema_version,
    )
    session.add(interpretation)
    await session.flush()
    tier = resolve_planning_tier(policy.draft)
    session.add(
        Event(
            entity_type="task",
            entity_id=fresh.id,
            event_type=PLANNING_TIER_EVENT,
            actor_type="interpretation_policy",
            payload=tier.event_payload(spec_revision=fresh.spec_revision),
        )
    )
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type=RECLASSIFIED_EVENT,
            actor_type="user",
            actor_id=str(principal.user_id),
            payload={
                "new_task_id": str(fresh.id),
                "old_spec_revision": expected_spec_revision,
                "new_spec_revision": fresh.spec_revision,
                "receipt": receipt,
                "manual_start_required": True,
            },
        )
    )
    await uow.outbox.enqueue(
        destination="workflow_dispatch",
        operation_type="dispatch_interpretation",
        entity_type="interpretation",
        entity_id=interpretation.id,
        idempotency_key=f"workflow:dispatch:{interpretation.id}",
        payload={"task_id": str(fresh.id)},
    )
    # Dispatcher emits the replacement's Start card after materialization;
    # the original Telegram message binding never moves to another task.
    return fresh.id
