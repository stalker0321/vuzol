"""Durable project-worker replies for free project discussion."""

from __future__ import annotations

import hashlib
import uuid
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, RuntimeConfiguration
from vuzol.discussion.memory import MemoryPack
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.storage.models import Interpretation, Step, Task
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    IdempotencyClass,
    InteractionMode,
    QueueClass,
    RetryClass,
    RiskLevel,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.domain import MaterializedStep, MaterializedWorkflow, OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.service import materialize_run

DISCUSSION_INTERNAL_TASK_TYPE = "discussion_agent_internal"
DISCUSSION_AGENT_SCHEMA_VERSION = "discussion-agent-reply.v1"
DISCUSSION_AGENT_PROMPT_VERSION = "project-discussion-agent-v1"
DISCUSSION_REPLY_MAX_CHARS = 4_000


class DiscussionAgentReply(BaseModel):
    """Small provider-neutral result returned by the project worker."""

    model_config = ConfigDict(extra="forbid")

    reply: str = Field(min_length=1, max_length=DISCUSSION_REPLY_MAX_CHARS)


async def schedule_discussion_agent(
    session: AsyncSession,
    *,
    runtime: RuntimeConfiguration,
    session_id: uuid.UUID,
    source_turn_id: uuid.UUID,
    project_id: str,
    chat_id: int,
    thread_id: int,
    user_id: int,
    interaction_mode: InteractionMode,
    classifier_confidence: float,
    memory_pack: MemoryPack,
) -> uuid.UUID:
    """Create a hidden internal workflow that uses the project's pinned executor."""

    prompt = _discussion_prompt(project_id=project_id, memory_pack=memory_pack)
    task = Task(
        user_id=user_id,
        source_chat_id=chat_id,
        source_thread_id=thread_id,
        topic_task_number=None,
        public_task_number=None,
        project_id=project_id,
        original_text=prompt,
        task_draft={
            "discussion_agent_contract": DISCUSSION_AGENT_SCHEMA_VERSION,
            "discussion_session_id": str(session_id),
            "source_turn_id": str(source_turn_id),
            "interaction_mode": interaction_mode.value,
            "classifier_confidence": classifier_confidence,
            "goal": "Reply substantively to the latest project discussion message.",
            "task_summary": "Internal project discussion reply",
            "required_capabilities": [Capability.REPOSITORY_READ.value],
        },
        draft_schema_version=DISCUSSION_AGENT_SCHEMA_VERSION,
        interpreter_profile="semantic-classifier",
        prompt_version=DISCUSSION_AGENT_PROMPT_VERSION,
        status=TaskStatus.INTERPRETED,
        risk=RiskLevel.LOW,
        task_type=DISCUSSION_INTERNAL_TASK_TYPE,
    )
    session.add(task)
    await session.flush()
    interpretation = Interpretation(
        task_id=task.id,
        original_input_hash=hashlib.sha256(prompt.encode()).hexdigest(),
        transcript=None,
        task_draft=dict(task.task_draft),
        profile_id="semantic-classifier",
        model="internal",
        prompt_version=DISCUSSION_AGENT_PROMPT_VERSION,
        schema_version=DISCUSSION_AGENT_SCHEMA_VERSION,
    )
    session.add(interpretation)
    await session.flush()
    workflow = MaterializedWorkflow(
        workflow_type="discussion_agent",
        version="1",
        interpretation_id=interpretation.id,
        steps=(
            MaterializedStep(
                ordinal=0,
                key="prepare_worktree",
                step_type="prepare_worktree",
                predecessor_ordinals=(),
                queue_class=QueueClass.HEAVY,
                capabilities=frozenset({Capability.GIT, Capability.FILESYSTEM_WRITE}),
                retry_class=RetryClass.TRANSIENT,
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                timeout_seconds=600,
                max_attempts=3,
                priority=100,
            ),
            MaterializedStep(
                ordinal=1,
                key="execute_agent",
                step_type="execute_agent",
                predecessor_ordinals=(0,),
                queue_class=QueueClass.HEAVY,
                capabilities=frozenset({Capability.REPOSITORY_READ}),
                retry_class=RetryClass.TRANSIENT,
                idempotency_class=IdempotencyClass.READ_ONLY,
                timeout_seconds=900,
                max_attempts=3,
                priority=100,
            ),
            MaterializedStep(
                ordinal=2,
                key="deliver_discussion_reply",
                step_type="deliver_discussion_reply",
                predecessor_ordinals=(1,),
                queue_class=QueueClass.LIGHT,
                capabilities=frozenset(),
                retry_class=RetryClass.TRANSIENT,
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                timeout_seconds=60,
                max_attempts=3,
                priority=100,
            ),
            MaterializedStep(
                ordinal=3,
                key="finalize",
                step_type="finalize",
                predecessor_ordinals=(2,),
                queue_class=QueueClass.CONTROL,
                capabilities=frozenset(),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                timeout_seconds=60,
                max_attempts=1,
                priority=100,
            ),
        ),
    )
    await materialize_run(
        session,
        task_id=task.id,
        workflow=workflow,
        configuration_revision=runtime.registries.revision,
        policy_revision=runtime.registries.revision,
        prompt_revision=DISCUSSION_AGENT_PROMPT_VERSION,
        automatic_start=True,
    )
    return task.id


class DeliverDiscussionReplyHandler:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self._factory = factory

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            session = uow.session
            task = await session.get(Task, request.task_id)
            if task is None or task.task_type != DISCUSSION_INTERNAL_TASK_TYPE:
                return StepOutcome(
                    kind=OutcomeKind.PERMANENT_FAILURE,
                    result={},
                    category="discussion_task_invalid",
                )
            execute_step = await session.scalar(
                select(Step).where(
                    Step.run_id == request.run_id,
                    Step.step_type == "execute_agent",
                    Step.status == StepStatus.COMPLETED,
                )
            )
            structured = (
                execute_step.result.get("structured_output")
                if execute_step is not None and execute_step.result is not None
                else None
            )
            if not isinstance(structured, dict):
                return StepOutcome(
                    kind=OutcomeKind.PERMANENT_FAILURE,
                    result={},
                    category="discussion_reply_invalid",
                )
            try:
                reply = DiscussionAgentReply.model_validate(structured).reply
                session_id = uuid.UUID(str(task.task_draft["discussion_session_id"]))
                source_turn_id = uuid.UUID(str(task.task_draft["source_turn_id"]))
                mode = InteractionMode(str(task.task_draft["interaction_mode"]))
                confidence = Decimal(str(task.task_draft["classifier_confidence"]))
            except (KeyError, TypeError, ValueError):
                return StepOutcome(
                    kind=OutcomeKind.PERMANENT_FAILURE,
                    result={},
                    category="discussion_reply_invalid",
                )
            memory = DiscussionMemoryService(uow)
            assistant_turn_id, _ = await memory.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.ASSISTANT,
                source=ConversationTurnSource.MODEL,
                content=reply,
                classifier_mode=mode,
                classifier_confidence=confidence,
                classifier_prompt_version=DISCUSSION_AGENT_PROMPT_VERSION,
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="conversation_turn",
                entity_id=assistant_turn_id,
                idempotency_key=f"discussion_reply:{assistant_turn_id}",
                payload={
                    "role": "discussion_reply",
                    "session_id": str(session_id),
                    "source_turn_id": str(source_turn_id),
                    "mode": mode.value,
                    "worker_task_id": str(task.id),
                },
            )
        return StepOutcome.succeeded({"assistant_turn_id": str(assistant_turn_id)})


def _discussion_prompt(*, project_id: str, memory_pack: MemoryPack) -> str:
    turns = "\n".join(
        f"{turn.role.value}: {turn.content}" for turn in memory_pack.turns
    )
    decisions = "\n".join(f"- {item.statement}" for item in memory_pack.decisions) or "(none)"
    summary = memory_pack.summary.body if memory_pack.summary is not None else "(none)"
    return f"""You are the project discussion agent for project {project_id}.
Continue the conversation as a capable product and technical partner. The final user turn is the
message you must answer. Contribute concrete ideas, compare tradeoffs, and recommend a practical
default when appropriate. Do not merely paraphrase the user, do not hand their question back to
them, and do not claim to have changed files or created a task. You may inspect the repository for
context, but file edits are forbidden. Reply in the user's language and keep it concise but useful.
Use lightweight Markdown suitable for Telegram: short bold headings, bullets, numbered lists,
quotes, and code blocks when useful. Do not use Markdown tables; express comparisons as compact
lists instead. Keep the complete final reply within {DISCUSSION_REPLY_MAX_CHARS} characters.

Accepted decisions:
{decisions}

Rolling summary:
{summary}

Recent conversation:
{turns}
"""
