"""Explicit bounded Telegram entry point for the first production coding dogfood."""

import shlex
from dataclasses import dataclass
from pathlib import PurePosixPath

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryError, RuntimeConfiguration, TopicKind
from vuzol.execution.git import LocalGit
from vuzol.experiments.domain import (
    BoundedLevel,
    ContextManifest,
    ExecutionMode,
    RequiredGate,
    RiskLevel,
    TaskClass,
    TaskClassification,
)
from vuzol.experiments.service import TrialSeedRequest, seed_trial
from vuzol.storage.models import TelegramIntakeMessage, TelegramMessageLink, TopicMapping
from vuzol.storage.types import IntakeStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import IngressResult, IngressStatus, MessageUpdate
from vuzol.telegram.ingress import update_hash
from vuzol.telegram.policy import TelegramPolicyError, authorize, validate_message

DOGFOOD_WORKFLOW = "adaptive_worker_trial"
TRUSTED_GATES = (
    RequiredGate(name="format-check", command_id="make format-check"),
    RequiredGate(name="lint", command_id="make lint"),
    RequiredGate(name="type-check", command_id="make type-check"),
    RequiredGate(name="security", command_id="make security"),
    RequiredGate(name="tests", command_id="make test"),
)


@dataclass(frozen=True, slots=True)
class SolCommand:
    allowed_paths: tuple[str, ...]
    goal: str


def parse_sol_command(text: str | None) -> SolCommand:
    if not text:
        raise TelegramPolicyError("project dogfood requires a /sol command")
    first, separator, goal = text.strip().partition("\n")
    try:
        arguments = shlex.split(first)
    except ValueError as error:
        raise TelegramPolicyError("invalid /sol command quoting") from error
    if not arguments or arguments[0].split("@", 1)[0] != "/sol":
        raise TelegramPolicyError("project dogfood requires /sol <paths> followed by a goal")
    paths = tuple(arguments[1:])
    if not paths or len(paths) > 10:
        raise TelegramPolicyError("/sol requires between one and ten allowed paths")
    if any(
        not path
        or len(path) > 500
        or PurePosixPath(path).is_absolute()
        or ".." in PurePosixPath(path).parts
        for path in paths
    ):
        raise TelegramPolicyError("/sol paths must be contained repository-relative paths")
    if not separator or not goal.strip():
        raise TelegramPolicyError("/sol requires the task goal on following lines")
    return SolCommand(allowed_paths=paths, goal=goal.strip())


class TelegramDogfoodIngressService:
    """Seed only explicit low-risk Sol tasks from the configured project topic."""

    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._runtime = runtime
        self._factory = session_factory

    async def accept_message(self, update: MessageUpdate) -> IngressResult | None:
        try:
            topic = self._runtime.registries.topics.resolve(
                update.chat_id, update.message_thread_id
            )
        except RegistryError:
            return None
        if topic.default_workflow != DOGFOOD_WORKFLOW:
            return None
        try:
            authorize(
                self._runtime.settings,
                chat_id=update.chat_id,
                user_id=update.user_id,
            )
            validate_message(self._runtime.settings, update)
            if (
                topic.kind is not TopicKind.PROJECT
                or topic.project_id is None
                or not topic.enabled
                or not topic.accepts_new_tasks
            ):
                raise TelegramPolicyError("project dogfood topic is not enabled")
            command = parse_sol_command(update.text)
        except TelegramPolicyError as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        project = self._runtime.registries.projects.get(topic.project_id)
        base = await LocalGit().resolve_commit(project.repository_path, project.default_branch)
        identity = f"telegram-{update.chat_id}-{update.message_thread_id}-{update.message_id}"
        request = TrialSeedRequest(
            experiment_id=identity,
            task_id=identity,
            worker_profile="codex-subscription-prod",
            project_id=project.id,
            base_commit=base,
            goal=command.goal,
            classification=TaskClassification(
                task_class=TaskClass.BOUNDED_FEATURE,
                complexity=BoundedLevel.LOW,
                risk=RiskLevel.LOW,
                testability=BoundedLevel.HIGH,
                blast_radius=BoundedLevel.LOW,
                coupling=BoundedLevel.LOW,
                novelty=BoundedLevel.LOW,
                expected_file_count=len(command.allowed_paths),
            ),
            actual_mode=ExecutionMode.SOL_SOLO,
            override_reason="Explicit bounded Telegram MVP dogfood command",
            allowed_paths=command.allowed_paths,
            acceptance_criteria=(
                "Implement the requested goal within exactly the allowed paths.",
                "All configured trusted repository gates pass.",
            ),
            forbidden_changes=(
                "Do not modify files outside the explicit Telegram command scope.",
                "Do not merge, deploy, use Git, run tests, or access the network.",
            ),
            required_gates=TRUSTED_GATES,
            maximum_repair_count=0,
            context_manifest=ContextManifest(role="worker"),
            source_user_id=update.user_id,
            source_chat_id=update.chat_id,
            source_thread_id=update.message_thread_id,
        )
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            inbox_id, created = await uow.inbox.receive_once(
                source="telegram",
                consumer=f"bot:{update.bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=update_hash(update),
            )
            if not created:
                return IngressResult(status=IngressStatus.DUPLICATE)
            await uow.topics.upsert(
                TopicMapping(
                    chat_id=update.chat_id,
                    message_thread_id=update.message_thread_id,
                    topic_kind=topic.kind.value,
                    project_id=topic.project_id,
                    accepts_new_tasks=topic.accepts_new_tasks,
                    default_workflow=topic.default_workflow,
                    enabled=topic.enabled,
                )
            )
            trial = await seed_trial(uow.session, self._runtime.registries, request)
            intake = TelegramIntakeMessage(
                inbox_id=inbox_id,
                chat_id=update.chat_id,
                message_thread_id=update.message_thread_id,
                message_id=update.message_id,
                user_id=update.user_id,
                task_id=trial.task_uuid,
                original_text=update.text,
                attachments=[],
                affinity_kind="new_task",
                ambiguous_task_ids=[],
                status=IntakeStatus.RECEIVED,
            )
            intake_id = await uow.telegram_intake.add(intake)
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=update.chat_id,
                    message_thread_id=update.message_thread_id,
                    message_id=update.message_id,
                    task_id=trial.task_uuid,
                    message_role="source_request",
                )
            )
            await uow.inbox.mark_processed(
                inbox_id, entity_type="telegram_intake", entity_id=intake_id
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="telegram_intake",
                entity_id=intake_id,
                idempotency_key=f"telegram:dogfood:{trial.run_uuid}:initial",
                payload={
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "role": "intake_ack",
                    "task_id": str(trial.task_uuid),
                },
            )
        return IngressResult(
            status=IngressStatus.CREATED,
            task_id=trial.task_uuid,
            intake_id=intake_id,
        )
