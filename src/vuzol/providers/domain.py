"""Provider-neutral immutable request, result, health, and usage contracts."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from vuzol.config.models import Capability, ProviderRole


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
    original_input: str | None = Field(default=None, max_length=20_000)
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
    reserved_cost_units: Decimal = Field(ge=0)
    reserved_quota_units: Decimal = Field(ge=0)
    sandbox_reference: str | None = Field(default=None, max_length=500)


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


class EffectiveProfileState(FrozenProviderModel):
    healthy: bool = True
    quota_state: QuotaState = QuotaState.UNKNOWN
    unhealthy_until: datetime | None = None
    rate_limit_until: datetime | None = None
    active_leases: int = Field(default=0, ge=0)
    queue_depth: int = Field(default=0, ge=0)
