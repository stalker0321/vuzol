"""Declared binding pairs: who may bind to whom, with what schema (D3 W5).

Replaces hard-coded ``step_type == "synthesize"`` consumer gates with
declared pairs. Three pairs exist:

- Research→Synthesis (auto-wired): ``research_execute`` → ``synthesize``,
  slot ``predecessor_result`` (legacy provider text and source reports).
- Scout→Planner (explicit): step-less scout completion → ``plan`` step,
  slot ``scout_packet`` (see ``vuzol/scout.py``).
- Task→Task (explicit): accepted upstream output → ``plan`` step,
  slot ``upstream_result`` (see ``bind_task_output``).

Consumer-boundary validation has two layers: the slot must carry a
pair-declared schema (tampered schema strings block), and pair schemas with
a content validator (source reports, scout packets) are validated byte-wise
before any provider spend. Unknown slots pass through (legacy compat);
resolver hash/scope/freshness checks always apply first.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.context.resolver import BindingError
from vuzol.storage.models import InputBinding

RESEARCH_SLOT = "predecessor_result"
SCOUT_SLOT = "scout_packet"
TASK_SLOT = "upstream_result"

SLOT_SCHEMAS: dict[str, frozenset[tuple[str, str]]] = {
    RESEARCH_SLOT: frozenset(
        {
            ("research-provider-result", "research-provider-result.v1"),
            ("research-result", "research-result.v1"),
        }
    ),
    SCOUT_SLOT: frozenset({("scout-packet", "scout-packet.v1")}),
    TASK_SLOT: frozenset({("task-result", "task-result.v1")}),
}


@dataclass(frozen=True, slots=True)
class BindingPair:
    producers: frozenset[str]
    consumers: frozenset[str]
    slot: str


BINDING_PAIRS: tuple[BindingPair, ...] = (
    BindingPair(
        producers=frozenset({"research_execute"}),
        consumers=frozenset({"synthesize"}),
        slot=RESEARCH_SLOT,
    ),
    BindingPair(
        producers=frozenset(),
        consumers=frozenset({"plan"}),
        slot=SCOUT_SLOT,
    ),
    BindingPair(
        producers=frozenset({"acceptance", "finalize", "execute_code"}),
        consumers=frozenset({"plan"}),
        slot=TASK_SLOT,
    ),
)


def find_pairs_for_producer(step_type: str) -> tuple[BindingPair, ...]:
    """Declared pairs a producer step type may feed (auto-discovery)."""

    return tuple(pair for pair in BINDING_PAIRS if step_type in pair.producers)


def check_pair_schema(*, slot: str, schema_name: str, schema_version: str) -> None:
    """Fail closed when a pair slot carries an undeclared schema version."""

    allowed = SLOT_SCHEMAS.get(slot)
    if allowed is None:
        return
    if (schema_name, schema_version) not in allowed:
        raise BindingError(
            "pair_schema_mismatch",
            f"slot {slot} carries undeclared schema {schema_name}:{schema_version}",
        )


def validate_binding_content(*, schema_name: str, schema_version: str, content: bytes) -> None:
    """Byte-wise content validation for pair schemas that define it."""

    if (schema_name, schema_version) == ("research-result", "research-result.v1"):
        from vuzol.research.report import validate_source_report_bytes

        errors = validate_source_report_bytes(content)
        if errors:
            raise BindingError(
                "source_report_schema_mismatch",
                f"source report bytes failed validation: {errors[0]}",
            )
    elif (schema_name, schema_version) == ("scout-packet", "scout-packet.v1"):
        from vuzol.scout import validate_scout_packet_bytes

        errors = validate_scout_packet_bytes(content)
        if errors:
            raise BindingError(
                "scout_packet_schema_mismatch",
                f"scout packet bytes failed validation: {errors[0]}",
            )


def validate_resolved_bindings(resolved: object) -> None:
    """Slot + content validation for resolved bindings (consumer boundary).

    Raises ``BindingError`` before any provider spend. Legacy provider-text
    bindings pass (read without a verified label); unknown slots pass
    (legacy compat).
    """

    bindings = getattr(resolved, "bindings", ())
    for binding in bindings:
        schema_name = getattr(binding, "schema_name", "")
        schema_version = getattr(binding, "schema_version", "")
        slot = getattr(binding, "slot", "")
        check_pair_schema(
            slot=slot, schema_name=schema_name, schema_version=schema_version
        )
        validate_binding_content(
            schema_name=schema_name,
            schema_version=schema_version,
            content=getattr(binding, "content", b""),
        )


async def bind_task_output(
    session: AsyncSession,
    *,
    artifact_id: uuid.UUID,
    content_hash: str,
    consumer_step_id: uuid.UUID,
    producer_step_id: uuid.UUID | None,
    schema_name: str = "task-result",
    schema_version: str = "task-result.v1",
    slot: str = TASK_SLOT,
    scope_project_id: str | None,
    access_scope: str = "private",
    required: bool = True,
    freshness_max_age_seconds: int | None = None,
    source_retrieved_at: datetime | None = None,
) -> InputBinding:
    """Bind an accepted upstream output to a consumer step (Task→Task, D3 W5).

    The consumer side is checked by the caller via the slot; the schema must
    belong to the slot's declared pair set — programming errors raise
    ``ValueError``, never silent misbinding.
    """

    check_pair_schema(slot=slot, schema_name=schema_name, schema_version=schema_version)
    if slot != TASK_SLOT:
        raise ValueError(f"task output binds only to {TASK_SLOT!r}")
    binding = InputBinding(
        consumer_step_id=consumer_step_id,
        producer_step_id=producer_step_id,
        artifact_id=artifact_id,
        slot=slot,
        schema_name=schema_name,
        schema_version=schema_version,
        content_hash=content_hash,
        scope_project_id=scope_project_id,
        access_scope=access_scope,
        required=required,
        status="resolved",
        freshness_max_age_seconds=freshness_max_age_seconds,
        source_retrieved_at=source_retrieved_at,
    )
    session.add(binding)
    await session.flush()
    return binding
