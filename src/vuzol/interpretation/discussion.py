"""Provider-neutral contracts and fail-closed policy for project discussions."""

from __future__ import annotations

import uuid
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import Field, model_validator

from vuzol.discussion.domain import (
    CapabilityProvisioning,
    CapabilityRequirementDraft,
    ComponentKind,
    EnvironmentComponentDraft,
    EnvironmentDeltaDraft,
    PlanDraft,
    PlanItemDraft,
)
from vuzol.discussion.memory import MemoryPack
from vuzol.interpretation.domain import FrozenModel, SuggestedComplexity
from vuzol.storage.types import EstimatedComplexity, InteractionMode, RiskLevel

DISCUSSION_SCHEMA_VERSION = "discussion-interpret-v1"
DISCUSSION_PROMPT_VERSION = "project-discussion-v1"
DISCUSSION_CLASSIFY_DESTINATION = "discussion_classify"
DISCUSSION_REPLY_DESTINATION = "discussion_reply"
DISCUSSION_THINKING_ROLE = "discussion_thinking"
DISCUSSION_CONFIDENCE_FLOOR = 0.55


class AmbiguityFlag(StrEnum):
    LOW_CONFIDENCE = "low_confidence"
    MISSING_PROJECT_BINDING = "missing_project_binding"
    MISSING_SCOPE = "missing_scope"
    CONFLICTING_DECISIONS = "conflicting_decisions"
    UNDERSPECIFIED_SUCCESS_CRITERIA = "underspecified_success_criteria"
    MULTIPLE_POSSIBLE_PLANS = "multiple_possible_plans"
    VOICE_UNCERTAIN = "voice_uncertain"
    UNSAFE_CAPABILITY_REQUEST = "unsafe_capability_request"
    STALE_PLAN_CONTEXT = "stale_plan_context"
    EDIT_SESSION_REQUIRED = "edit_session_required"


class RefusalCode(StrEnum):
    DISCUSS_PREFER = "discuss_prefer"
    CLARIFY_REQUIRED = "clarify_required"
    PLAN_REVISION_STALE = "plan_revision_stale"
    EXECUTION_NOT_APPROVED = "execution_not_approved"
    AUTO_EXEC_DISABLED = "auto_exec_disabled"
    CONTROL_REQUIRES_BUTTON = "control_requires_button"
    OUT_OF_SCOPE = "out_of_scope"
    SAFETY_REFUSE = "safety_refuse"
    LIMIT_EXCEEDED = "limit_exceeded"
    INTERPRETER_UNAVAILABLE = "interpreter_unavailable"
    INVALID_INTERPRETER_OUTPUT = "invalid_interpreter_output"


class PlanRequestIntent(StrEnum):
    CREATE_DRAFT = "create_draft"
    REVISE_DRAFT = "revise_draft"
    REPLAN_REMAINING = "replan_remaining"
    EDIT_ITEM = "edit_item"


class PlanControlAction(StrEnum):
    APPROVE = "approve"
    START = "start"
    DISCARD = "discard"
    PAUSE = "pause"
    RESUME_QUEUE = "resume_queue"
    RETRY_ITEM = "retry_item"
    SKIP_ITEM = "skip_item"
    STOP_PACKAGE = "stop_package"
    REQUEST_REPLAN = "request_replan"


class ControlOverrideKind(StrEnum):
    CONTINUE_DISCUSSION = "continue_discussion"
    CREATE_OR_UPDATE_PLAN = "create_or_update_plan"
    REPLAN = "replan"
    EXPLICIT_TASK = "explicit_task"


class DecisionCandidate(FrozenModel):
    key: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=64)
    statement: str = Field(min_length=1, max_length=500)


class DiscussionPlanItem(FrozenModel):
    local_id: str = Field(min_length=1, max_length=64)
    summary: str = Field(min_length=1, max_length=240)
    goal: str = Field(min_length=1, max_length=4_000)
    expected_outcome: str = Field(min_length=1, max_length=2_000)
    completion_criteria: tuple[str, ...] = Field(min_length=1, max_length=20)
    allowed_scope: str = Field(min_length=1, max_length=2_000)
    out_of_scope: tuple[str, ...] = Field(default=(), max_length=20)
    dependencies: tuple[str, ...] = Field(default=(), max_length=20)
    trusted_checks: tuple[str, ...] = Field(default=(), max_length=20)
    suggested_risk: RiskLevel
    needs_approval: bool
    estimated_complexity: SuggestedComplexity


class EnvironmentComponentProposal(FrozenModel):
    key: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=64)
    label: str = Field(min_length=1, max_length=100)
    kind: ComponentKind
    technology: str = Field(min_length=1, max_length=100)
    version: str | None = Field(default=None, max_length=50)
    run_command: tuple[str, ...] = Field(default=(), max_length=20)
    port: int | None = Field(default=None, ge=1, le=65535)
    healthcheck_path: str | None = Field(default=None, pattern=r"^/", max_length=200)
    artifact_patterns: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def validate_runtime_shape(self) -> EnvironmentComponentProposal:
        if any(not value or len(value) > 500 or "\x00" in value for value in self.run_command):
            raise ValueError("run command arguments must be bounded non-empty strings")
        if self.kind is ComponentKind.WEB_SERVICE and (not self.run_command or self.port is None):
            raise ValueError("web service requires run_command and port")
        return self


class CapabilityRequirementProposal(FrozenModel):
    key: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=64)
    label: str = Field(min_length=1, max_length=100)
    provisioning: CapabilityProvisioning = CapabilityProvisioning.AUTOMATIC
    reason: str = Field(default="", max_length=500)


class EnvironmentDeltaProposal(FrozenModel):
    upsert_components: tuple[EnvironmentComponentProposal, ...] = Field(default=(), max_length=20)
    remove_components: tuple[str, ...] = Field(default=(), max_length=20)
    required_capabilities: tuple[CapabilityRequirementProposal, ...] = Field(
        default=(), max_length=30
    )


class PlanRequestPayload(FrozenModel):
    intent: PlanRequestIntent
    base_revision_id: uuid.UUID | None = None
    base_revision_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    item_local_id: str | None = Field(default=None, max_length=64)
    title: str = Field(min_length=1, max_length=200)
    items: tuple[DiscussionPlanItem, ...] = Field(min_length=1, max_length=20)
    environment_delta: EnvironmentDeltaProposal = EnvironmentDeltaProposal()
    rationale: str | None = Field(default=None, max_length=2_000)


class PlanControlPayload(FrozenModel):
    action: PlanControlAction
    revision_id: uuid.UUID | None = None
    revision_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    item_local_id: str | None = Field(default=None, max_length=64)
    note: str | None = Field(default=None, max_length=1_000)
    authoritative: bool = False


class EditSessionContext(FrozenModel):
    edit_session_id: uuid.UUID
    package_id: uuid.UUID
    revision_id: uuid.UUID | None = None
    revision_number: int = Field(ge=1)
    revision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    session_generation: int = Field(ge=1)
    item_id: uuid.UUID
    item_local_id: str | None = Field(default=None, max_length=64)
    opened_by_user_id: int


class PlanSnapshotItem(FrozenModel):
    item_id: uuid.UUID
    local_id: str | None = Field(default=None, max_length=64)
    ordinal: int = Field(ge=1, le=20)
    summary: str = Field(min_length=1, max_length=240)


class PlanSnapshot(FrozenModel):
    package_id: uuid.UUID
    revision_id: uuid.UUID
    revision_number: int = Field(ge=1)
    revision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status_generation: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=240)
    items: tuple[PlanSnapshotItem, ...] = Field(default=(), max_length=20)


class ControlOverride(FrozenModel):
    kind: ControlOverrideKind


class ItemEditPayload(FrozenModel):
    edit_session_id: uuid.UUID
    package_id: uuid.UUID
    revision_id: uuid.UUID | None = None
    revision_number: int = Field(ge=1)
    revision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    item_id: uuid.UUID
    refinement_text: str | None = Field(default=None, max_length=4_000)


class TaskRequestPayload(FrozenModel):
    summary: str = Field(min_length=1, max_length=240)
    goal: str = Field(min_length=1, max_length=4_000)


class DiscussionInterpretRequest(FrozenModel):
    original_input: str = Field(min_length=1, max_length=20_000)
    project_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    user_id: int
    source_is_voice: bool = False
    transcription_uncertain: bool = False
    memory_pack: MemoryPack | None = None
    plan_snapshot: PlanSnapshot | None = None
    edit_session: EditSessionContext | None = None
    control_override: ControlOverride | None = None


class DiscussionInterpretation(FrozenModel):
    schema_version: Literal["discussion-interpret-v1"] = "discussion-interpret-v1"
    prompt_version: Literal["project-discussion-v1"] = "project-discussion-v1"
    interaction_mode: InteractionMode
    confidence: float = Field(ge=0, le=1)
    should_create_task: bool = False
    should_mutate_plan: bool = False
    should_emit_user_message: bool = True
    ambiguity_flags: frozenset[AmbiguityFlag] = frozenset()
    refusal_code: RefusalCode | None = None
    user_visible_summary: str = Field(
        min_length=1,
        max_length=1_000,
        description=(
            "Concise internal classification/fallback summary. It is not the project worker's "
            "discussion reply and must not be presented as one."
        ),
    )
    clarification_question: str | None = Field(default=None, max_length=1_000)
    decision_candidates: tuple[DecisionCandidate, ...] = Field(default=(), max_length=20)
    plan_request: PlanRequestPayload | None = None
    plan_control: PlanControlPayload | None = None
    item_edit: ItemEditPayload | None = None
    task_request: TaskRequestPayload | None = None

    @model_validator(mode="after")
    def validate_mode_payload(self) -> DiscussionInterpretation:
        payloads = {
            InteractionMode.PLAN_REQUEST: self.plan_request,
            InteractionMode.PLAN_CONTROL: self.plan_control,
            InteractionMode.ITEM_EDIT: self.item_edit,
            InteractionMode.TASK_REQUEST: self.task_request,
        }
        if self.interaction_mode in payloads and payloads[self.interaction_mode] is None:
            raise ValueError(f"{self.interaction_mode.value} requires its matching payload")
        unexpected = [
            mode.value
            for mode, payload in payloads.items()
            if mode is not self.interaction_mode and payload is not None
        ]
        if unexpected:
            raise ValueError("payload does not match interaction_mode: " + ", ".join(unexpected))
        return self


class SemanticDiscussionInterpreter(Protocol):
    async def interpret_discussion(
        self, request: DiscussionInterpretRequest
    ) -> DiscussionInterpretation: ...


_NON_TASK_MODES = frozenset(
    {
        InteractionMode.DISCUSSION,
        InteractionMode.PLAN_REQUEST,
        InteractionMode.PLAN_CONTROL,
        InteractionMode.ITEM_EDIT,
        InteractionMode.QUERY_ONLY,
        InteractionMode.QUERY_REFUSE,
    }
)
_MUTATING_CONTROLS = frozenset(
    {
        PlanControlAction.APPROVE,
        PlanControlAction.START,
        PlanControlAction.DISCARD,
        PlanControlAction.RETRY_ITEM,
        PlanControlAction.SKIP_ITEM,
        PlanControlAction.STOP_PACKAGE,
    }
)


def enforce_discussion_policy(
    request: DiscussionInterpretRequest,
    candidate: DiscussionInterpretation,
) -> DiscussionInterpretation:
    """Tighten model output; this function can never grant execution authority."""

    result = candidate
    has_authorized_edit_session = request.edit_session is not None and (
        request.edit_session.opened_by_user_id == request.user_id
    )
    if has_authorized_edit_session:
        assert request.edit_session is not None
        edit = request.edit_session
        payload = ItemEditPayload(
            edit_session_id=edit.edit_session_id,
            package_id=edit.package_id,
            revision_id=edit.revision_id,
            revision_number=edit.revision_number,
            revision_hash=edit.revision_hash,
            item_id=edit.item_id,
            refinement_text=(
                result.item_edit.refinement_text
                if result.item_edit is not None and result.item_edit.refinement_text
                else request.original_input
            ),
        )
        result = result.model_copy(
            update={
                "interaction_mode": InteractionMode.ITEM_EDIT,
                "item_edit": payload,
                "should_create_task": False,
                "plan_request": None,
                "plan_control": None,
                "task_request": None,
            }
        )
    elif result.interaction_mode is InteractionMode.ITEM_EDIT:
        flags = set(result.ambiguity_flags)
        flags.add(AmbiguityFlag.EDIT_SESSION_REQUIRED)
        result = result.model_copy(
            update={
                "interaction_mode": InteractionMode.QUERY_REFUSE,
                "should_create_task": False,
                "should_mutate_plan": False,
                "ambiguity_flags": frozenset(flags),
                "refusal_code": RefusalCode.CLARIFY_REQUIRED,
                "item_edit": None,
            }
        )

    if result.confidence < DISCUSSION_CONFIDENCE_FLOOR or (
        AmbiguityFlag.LOW_CONFIDENCE in result.ambiguity_flags
    ):
        result = result.model_copy(
            update={
                "interaction_mode": InteractionMode.DISCUSSION,
                "should_create_task": False,
                "should_mutate_plan": False,
                "refusal_code": result.refusal_code or RefusalCode.DISCUSS_PREFER,
                "plan_request": None,
                "plan_control": None,
                "item_edit": None,
                "task_request": None,
            }
        )

    # v1 is confirm-first; P8 owns canonical Task materialization. A validated plan
    # envelope may always materialize a non-executing draft. Do not let the model's
    # redundant boolean suppress the explicit structured request.
    result = result.model_copy(
        update={
            "should_create_task": False,
            "should_mutate_plan": (
                (
                    result.interaction_mode is InteractionMode.PLAN_REQUEST
                    and result.plan_request is not None
                )
                or (
                    result.should_mutate_plan
                    and result.interaction_mode is InteractionMode.ITEM_EDIT
                    and has_authorized_edit_session
                )
            ),
        }
    )

    if result.plan_control is not None:
        control = result.plan_control
        control = control.model_copy(update={"authoritative": False})
        updates: dict[str, object] = {
            "plan_control": control,
            "should_create_task": False,
            "should_mutate_plan": False,
        }
        if control.action in _MUTATING_CONTROLS:
            updates.update(
                refusal_code=RefusalCode.CONTROL_REQUIRES_BUTTON,
            )
        result = result.model_copy(update=updates)

    if result.interaction_mode is InteractionMode.PLAN_REQUEST and result.plan_request is not None:
        plan_request = result.plan_request
        if plan_request.intent is PlanRequestIntent.CREATE_DRAFT:
            result = result.model_copy(
                update={
                    "plan_request": plan_request.model_copy(
                        update={"base_revision_id": None, "base_revision_hash": None}
                    )
                }
            )
        elif request.plan_snapshot is None:
            result = result.model_copy(
                update={
                    "interaction_mode": InteractionMode.QUERY_REFUSE,
                    "should_mutate_plan": False,
                    "refusal_code": RefusalCode.PLAN_REVISION_STALE,
                    "plan_request": None,
                }
            )
        else:
            snapshot = request.plan_snapshot
            result = result.model_copy(
                update={
                    "plan_request": plan_request.model_copy(
                        update={
                            "base_revision_id": snapshot.revision_id,
                            "base_revision_hash": snapshot.revision_hash,
                        }
                    )
                }
            )

    if request.control_override is not None:
        override_mode = {
            ControlOverrideKind.CONTINUE_DISCUSSION: InteractionMode.DISCUSSION,
            ControlOverrideKind.CREATE_OR_UPDATE_PLAN: InteractionMode.PLAN_REQUEST,
            ControlOverrideKind.REPLAN: InteractionMode.PLAN_REQUEST,
            ControlOverrideKind.EXPLICIT_TASK: InteractionMode.TASK_REQUEST,
        }[request.control_override.kind]
        if override_mode is InteractionMode.DISCUSSION:
            result = result.model_copy(
                update={
                    "interaction_mode": override_mode,
                    "should_create_task": False,
                    "should_mutate_plan": False,
                    "plan_request": None,
                    "plan_control": None,
                    "item_edit": None,
                    "task_request": None,
                }
            )
        elif result.interaction_mode is not override_mode:
            raise ValueError("control override requires matching structured payload")
    if request.transcription_uncertain:
        flags = set(result.ambiguity_flags)
        flags.add(AmbiguityFlag.VOICE_UNCERTAIN)
        result = result.model_copy(
            update={
                "ambiguity_flags": frozenset(flags),
                "should_create_task": False,
                "should_mutate_plan": False,
                "refusal_code": RefusalCode.CLARIFY_REQUIRED,
            }
        )
    payload_fields = {
        InteractionMode.PLAN_REQUEST: "plan_request",
        InteractionMode.PLAN_CONTROL: "plan_control",
        InteractionMode.ITEM_EDIT: "item_edit",
        InteractionMode.TASK_REQUEST: "task_request",
    }
    result = result.model_copy(
        update={
            field: None
            for mode, field in payload_fields.items()
            if mode is not result.interaction_mode
        }
    )
    return DiscussionInterpretation.model_validate(result.model_dump(mode="python"))


def plan_draft_from_interpretation(result: DiscussionInterpretation) -> PlanDraft:
    """Map a validated plan envelope to the draft-only work-package contract."""

    if result.interaction_mode is not InteractionMode.PLAN_REQUEST or result.plan_request is None:
        raise ValueError("discussion interpretation does not contain a plan request")
    request = result.plan_request
    return PlanDraft(
        title=request.title,
        items=tuple(
            PlanItemDraft(
                local_id=item.local_id,
                summary=item.summary,
                goal=item.goal,
                expected_outcome=item.expected_outcome,
                completion_criteria=item.completion_criteria,
                allowed_scope=item.allowed_scope,
                out_of_scope=item.out_of_scope,
                dependencies=item.dependencies,
                trusted_checks=item.trusted_checks,
                suggested_risk=item.suggested_risk,
                needs_approval=item.needs_approval,
                estimated_complexity=EstimatedComplexity(item.estimated_complexity.value),
            )
            for item in request.items
        ),
        environment_delta=EnvironmentDeltaDraft(
            upsert_components=tuple(
                EnvironmentComponentDraft(
                    key=component.key,
                    label=component.label,
                    kind=component.kind,
                    technology=component.technology,
                    version=component.version,
                    run_command=component.run_command,
                    port=component.port,
                    healthcheck_path=component.healthcheck_path,
                    artifact_patterns=component.artifact_patterns,
                )
                for component in request.environment_delta.upsert_components
            ),
            remove_components=request.environment_delta.remove_components,
            required_capabilities=tuple(
                CapabilityRequirementDraft(
                    key=capability.key,
                    label=capability.label,
                    provisioning=capability.provisioning,
                    reason=capability.reason,
                )
                for capability in request.environment_delta.required_capabilities
            ),
        ),
    )


class DiscussionInterpretationService:
    """Model boundary that returns advisory/draft data and never applies controls."""

    def __init__(self, interpreter: SemanticDiscussionInterpreter) -> None:
        self._interpreter = interpreter

    async def interpret(self, request: DiscussionInterpretRequest) -> DiscussionInterpretation:
        candidate = await self._interpreter.interpret_discussion(request)
        return enforce_discussion_policy(request, candidate)
