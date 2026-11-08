"""The one provider request a leased model call may send; the sandbox rebuilds it to check."""

from __future__ import annotations

import hashlib
import uuid
from decimal import Decimal

from vuzol.config.models import ProviderRole
from vuzol.model_calls.queue import ModelCallClaim
from vuzol.providers.domain import ContextItem, ProviderRequest
from vuzol.storage.models import ModelCall

REVISION = "model-call.v1"
_CHUNK = 19_000
_MAX_CHUNKS = 50
MAX_INPUT_CHARS = _CHUNK * _MAX_CHUNKS
_OUTPUT_TOKENS = 16_000


def attempt_id(claim: ModelCallClaim) -> uuid.UUID:
    return uuid.uuid5(claim.call_id, f"attempt:{claim.generation}")


def provider_request(call: ModelCall, claim: ModelCallClaim) -> ProviderRequest:
    system, user = call.request["system"], call.request["user"]
    if len(user) > MAX_INPUT_CHARS:
        raise ValueError("model call input is too large")
    parts = [user[offset : offset + _CHUNK] for offset in range(0, len(user), _CHUNK)] or [""]
    invocation = attempt_id(claim)
    return ProviderRequest(
        task_id=invocation,
        run_id=invocation,
        step_id=invocation,
        provider_attempt=1,
        lease_generation=claim.generation,
        role=ProviderRole.LEAD,
        original_input=system,
        task_draft={"purpose": call.purpose, "input": "the context items, in order"},
        context=tuple(
            ContextItem(
                source="model_call_input",
                reference=f"{call.id}:{index}",
                content=part,
                content_hash=hashlib.sha256(part.encode()).hexdigest(),
            )
            for index, part in enumerate(parts)
        ),
        output_schema_name=call.purpose[:100],
        output_schema_version=REVISION,
        output_json_schema=call.request["output_schema"],
        system_policy_revision=REVISION,
        prompt_revision=REVISION,
        timeout_seconds=call.timeout_seconds,
        max_input_tokens=(len(system) + len(user)) // 3 + 2_000,
        max_output_tokens=_OUTPUT_TOKENS,
        reserved_cost_units=Decimal(0),
        reserved_quota_units=Decimal(0),
        sandbox_reference=claim.sandbox_reference,
    )
