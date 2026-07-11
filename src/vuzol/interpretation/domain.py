"""Strict provider-neutral semantic and transcription contracts."""

import uuid
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from vuzol.config import Capability, TopicKind
from vuzol.storage.types import RiskLevel

TASK_DRAFT_SCHEMA_VERSION = "1.0"
INTERPRETER_PROMPT_VERSION = "step-05-v1"


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TaskAction(StrEnum):
    CREATE_TASK = "create_task"
    CONTINUE_TASK = "continue_task"
    ANSWER_QUESTION = "answer_question"
    APPROVE_STEP = "approve_step"
    REJECT_STEP = "reject_step"
    PAUSE_TASK = "pause_task"
    RESUME_TASK = "resume_task"
    CANCEL_TASK = "cancel_task"
    GENERAL_CONVERSATION = "general_conversation"


class TaskType(StrEnum):
    CODING = "coding"
    RESEARCH = "research"
    INFRASTRUCTURE = "infrastructure"
    FILE_PROCESSING = "file_processing"
    GENERAL = "general"


class TaskOperation(StrEnum):
    INSPECT = "inspect"
    EXPLAIN = "explain"
    CREATE = "create"
    MODIFY = "modify"
    FIX = "fix"
    DEPLOY = "deploy"
    MONITOR = "monitor"


class SuggestedComplexity(StrEnum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class TaskDraft(FrozenModel):
    action: TaskAction
    task_type: TaskType
    operation: TaskOperation
    project_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    goal: str = Field(min_length=1, max_length=4_000)
    requested_outcomes: tuple[str, ...] = Field(default=(), max_length=20)
    constraints: tuple[str, ...] = Field(default=(), max_length=20)
    missing_information: tuple[str, ...] = Field(default=(), max_length=20)
    clarification_question: str | None = Field(default=None, max_length=1_000)
    required_capabilities: frozenset[Capability] = frozenset()
    suggested_complexity: SuggestedComplexity
    suggested_risk: RiskLevel
    needs_planning: bool
    needs_clarification: bool
    referenced_task_id: uuid.UUID | None = None
    normalized_title: str = Field(min_length=1, max_length=120)
    embedded_instructions: tuple[str, ...] = Field(default=(), max_length=20)
    contradiction_detected: bool = False

    @model_validator(mode="after")
    def validate_clarification_and_continuation(self) -> "TaskDraft":
        if self.needs_clarification and not self.clarification_question:
            raise ValueError("clarification question is required")
        if not self.needs_clarification and self.clarification_question is not None:
            raise ValueError("clarification question requires needs_clarification")
        if self.action is TaskAction.CONTINUE_TASK and self.referenced_task_id is None:
            raise ValueError("continuation requires a referenced task")
        return self


class TaskContext(FrozenModel):
    task_id: uuid.UUID
    title: str = Field(max_length=120)


class ProjectSummary(FrozenModel):
    project_id: str
    summary: str = Field(max_length=2_000)


class InterpretationInput(FrozenModel):
    original_input: str = Field(min_length=1, max_length=20_000)
    transcript: str | None = Field(default=None, max_length=20_000)
    topic_kind: TopicKind
    mapped_project_id: str | None = None
    reply_linked_task: TaskContext | None = None
    active_tasks: tuple[TaskContext, ...] = Field(default=(), max_length=20)
    project_summaries: tuple[ProjectSummary, ...] = Field(default=(), max_length=50)
    capability_vocabulary: frozenset[Capability]
    source_is_voice: bool = False
    transcription_uncertain: bool = False


class InterpretationResult(FrozenModel):
    draft: TaskDraft
    profile_id: str
    model: str
    prompt_version: str = INTERPRETER_PROMPT_VERSION
    schema_version: str = TASK_DRAFT_SCHEMA_VERSION
    provider_request_id: str | None = None
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    duration_ms: int = Field(ge=0)
    repaired: bool = False


class TranscriptionInput(FrozenModel):
    content: bytes = Field(min_length=1)
    media_type: str = Field(min_length=1, max_length=200)
    filename: str | None = Field(default=None, max_length=255)
    language_hint: str | None = Field(default=None, max_length=20)


class TranscriptionResult(FrozenModel):
    transcript: str = Field(min_length=1, max_length=20_000)
    profile_id: str
    model: str
    provider_request_id: str | None = None
    duration_ms: int = Field(ge=0)
    uncertain: bool = False
