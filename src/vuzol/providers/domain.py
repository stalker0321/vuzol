"""Provider-neutral immutable request, result, health, and usage contracts."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from vuzol.config.models import Capability, ProviderRole
from vuzol.discussion.context_budget import CHAT_DOCUMENT_CHARS, CHAT_INPUT_CHARS, CHAT_TEXT_CHARS

WHOLE_TURN_POLICY = (
    "Preserve the entire original_input, not just task_draft.goal or its summary. "
    "Respect all user conditions, negations, scope limits and instructions not to start. "
    "Never silently execute only part of a request. If the whole request cannot be handled "
    "within this role and its permissions, report the limitation instead of partial execution. "
    "For conditional changes, check the condition first. If it does not hold, successful "
    "completion without changes is valid: explain the condition and observed evidence; "
    "use no_change_reason when the requested edit-report schema supports it. "
    "Do not manufacture changes or commits merely to produce a diff."
)


class FrozenProviderModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class QuotaState(StrEnum):
    AVAILABLE = "available"
    LIMITED = "limited"
    EXHAUSTED = "exhausted"
    UNKNOWN = "unknown"


class ProviderResultStatus(StrEnum):
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ProviderErrorCategory(StrEnum):
    AUTHENTICATION = "authentication"
    QUOTA_EXHAUSTED = "quota_exhausted"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    INVALID_STRUCTURED_OUTPUT = "invalid_structured_output"
    CANCELLED = "cancelled"
    CONTEXT_TOO_LARGE = "context_too_large"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    PERMANENT_REQUEST = "permanent_request"
    UNKNOWN = "unknown"


class NormalizedUsage(FrozenProviderModel):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cached_tokens: int | None = Field(default=None, ge=0)
    cost_units: Decimal | None = Field(default=None, ge=0)
    quota_units: Decimal | None = Field(default=None, ge=0)
    duration_ms: int = Field(ge=0)


class ContextItem(FrozenProviderModel):
    source: str = Field(min_length=1, max_length=100)
    reference: str = Field(min_length=1, max_length=500)
    content: str = Field(max_length=20_000)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ProviderRequest(FrozenProviderModel):
    schema_version: str = "provider-request.v1"
    task_id: uuid.UUID
    run_id: uuid.UUID
    step_id: uuid.UUID
    provider_attempt: int = Field(ge=1)
    lease_generation: int | None = Field(default=None, ge=1)
    role: ProviderRole
    required_capabilities: frozenset[Capability] = frozenset()
    original_input_reference: str | None = Field(default=None, max_length=500)
    # Application payload guard, not a model context-window limit. Discussion
    # requests currently include their prompt pack here; token budgets still
    # apply independently to the complete provider request.
    original_input: str | None = Field(default=None, max_length=CHAT_INPUT_CHARS)
    # Runtime-measured file text; requests without files retain the 40k guard.
    document_text_chars: int = Field(default=0, ge=0, le=CHAT_DOCUMENT_CHARS)
    task_draft: dict[str, Any] = Field(default_factory=dict)
    context: tuple[ContextItem, ...] = Field(default=(), max_length=50)
    output_schema_name: str | None = Field(default=None, max_length=100)
    output_schema_version: str | None = Field(default=None, max_length=100)
    output_json_schema: dict[str, Any] | None = None
    system_policy_revision: str = Field(min_length=1, max_length=100)
    prompt_revision: str = Field(min_length=1, max_length=100)
    timeout_seconds: float = Field(gt=0, le=3_600)
    deadline: datetime | None = None
    max_input_tokens: int = Field(ge=1)
    max_output_tokens: int = Field(ge=1)
    reasoning_max_tokens: int | None = Field(default=None, ge=1)
    reserved_cost_units: Decimal = Field(ge=0)
    reserved_quota_units: Decimal = Field(ge=0)
    sandbox_reference: str | None = Field(default=None, max_length=500)

    @model_validator(mode="after")
    def validate_reasoning_budget(self) -> "ProviderRequest":
        if self.document_text_chars > len(self.original_input or ""):
            raise ValueError("document text count exceeds original_input")
        if (
            self.original_input is not None
            and len(self.original_input) - self.document_text_chars > CHAT_TEXT_CHARS
        ):
            raise ValueError("non-chat original_input exceeds 40000 characters")
        if (
            self.reasoning_max_tokens is not None
            and self.reasoning_max_tokens > self.max_output_tokens
        ):
            raise ValueError("reasoning token limit must not exceed output token limit")
        return self


class OutputDiagnostic(FrozenProviderModel):
    """Bounded structural evidence, never response text or exception messages."""

    stage: Literal[
        "event_stream",
        "provider_stop",
        "json_parse",
        "non_object",
        "schema_validation",
        "empty_response",
    ]
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_bytes: int = Field(ge=0)
    finish_reason: Literal["stop", "length", "toolUse", "aborted", "error", "unknown"] = "unknown"
    json_position: int | None = Field(default=None, ge=0)
    # This path comes from the trusted schema, NOT the invalid instance/message.
    schema_path: str | None = Field(default=None, max_length=500)
    json_fenced: bool = False


class ProviderFailureDetails(FrozenProviderModel):
    category: ProviderErrorCategory
    retryable: bool
    request_sent: bool
    execution_completed: bool
    diagnostic: OutputDiagnostic | None = None
    # When the provider said its subscription limit resets (quota failures only).
    retry_at: datetime | None = None


class ProviderResult(FrozenProviderModel):
    schema_version: str = "provider-result.v1"
    status: ProviderResultStatus
    text: str | None = Field(default=None, max_length=100_000)
    structured_output: dict[str, Any] | None = None
    provider_request_id: str | None = Field(default=None, max_length=255)
    provider_session_id: str | None = Field(default=None, max_length=255)
    usage: NormalizedUsage
    finish_reason: str | None = Field(default=None, max_length=100)
    raw_result_reference: str | None = Field(default=None, max_length=500)
    adapter_version: str = Field(min_length=1, max_length=100)
    failure: ProviderFailureDetails | None = None


class EffectiveProfileState(FrozenProviderModel):
    healthy: bool = True
    quota_state: QuotaState = QuotaState.UNKNOWN
    unhealthy_until: datetime | None = None
    rate_limit_until: datetime | None = None
    active_leases: int = Field(default=0, ge=0)
    queue_depth: int = Field(default=0, ge=0)
