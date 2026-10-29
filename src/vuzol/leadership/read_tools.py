"""Read-only lead tools. Provider output is untrusted until validated here."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.settings import HardLimits
from vuzol.leadership.commands import LeadCommandName
from vuzol.leadership.invocation_context import current_turn, invocation_current
from vuzol.leadership.model import LeadModelUnavailable, LeadSnapshot
from vuzol.leadership.persistence import GoalDelegation, LeadershipInference, ManagedGoal
from vuzol.providers.domain import ContextItem
from vuzol.storage.repositories.memory_units import MemoryUnitRepository


class _Object(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)


class RecallArgs(_Object):
    query: str = Field(min_length=1, max_length=200)
    project: Literal["this", "all"] = "this"
    limit: int = Field(default=8, ge=1, le=8)


class ReadCall(_Object):
    name: Literal["read_status", "read_result"]
    args: _Object


class RecallCall(_Object):
    name: Literal["recall"]
    args: RecallArgs


class ReadJobArgs(_Object):
    job_id: str = Field(pattern=r"^[0-9a-f-]{36}$")


class ReadJobCall(_Object):
    name: Literal["read_job"]
    args: ReadJobArgs


class ToolBatch(_Object):
    # Validate the envelope at the provider boundary, individual calls here.
    # Otherwise adapters reject malformed calls before we can return tool errors.
    tool_calls: list[Any] = Field(
        min_length=1,
        description="Read calls: {name: read_status|read_result, args: {}} or "
        "{name: recall, args: {query: text (1-200 chars), project: this|all, limit: 1-8}}. "
        "project defaults to this; limit defaults to 8. "
        "{name: read_job, args: {job_id: UUID}} reads this goal's untrusted job report.",
    )


def parse_tool_call(raw: object) -> ReadCall | RecallCall | ReadJobCall:
    return TypeAdapter(ReadCall | RecallCall | ReadJobCall).validate_python(raw)


async def execute_read(
    factory: async_sessionmaker[AsyncSession],
    snapshot: LeadSnapshot,
    call: ReadCall | RecallCall | ReadJobCall,
) -> dict[str, Any]:
    context = current_turn.get()
    if (
        context is None
        or context.claim.goal_id != snapshot.goal_id
        or context.project_id != snapshot.project_id
    ):
        raise LeadModelUnavailable("lead_model_unavailable")
    async with factory.begin() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        if not await invocation_current(session, context, lock=False):
            raise LeadModelUnavailable("lead_model_unavailable")
        if isinstance(call, ReadJobCall):
            from vuzol.discussion.domain import DomainError
            from vuzol.leadership.jobs import read_job

            try:
                return await read_job(session, goal_id=snapshot.goal_id, job_id=call.args.job_id)
            except DomainError as error:
                return {"error": error.code}
        if isinstance(call, RecallCall):
            units = await MemoryUnitRepository(session).recall_lead(
                goal_id=snapshot.goal_id, **call.args.model_dump()
            )
            hits = [
                {
                    "kind": unit.unit_type,
                    "project_id": unit.project_id,
                    "text": unit.text.encode("utf-8")[:4000].decode("utf-8", errors="ignore"),
                    "created_at": unit.created_at.isoformat(),
                    "goal_id": str(unit.goal_id)
                    if unit.goal_id
                    else (unit.details or {}).get("goal_id"),
                    "links": (unit.details or {}).get("links"),
                }
                for unit in units
            ]
            return {"hits": hits, "message": "matches" if hits else "no matches"}
        from vuzol.leadership.service import read_projection

        goal = await session.get(ManagedGoal, snapshot.goal_id)
        if goal is None:
            raise LeadModelUnavailable("lead_model_unavailable")
        delegation = await session.get(GoalDelegation, goal.delegation_id)
        if delegation is None:
            raise LeadModelUnavailable("lead_model_unavailable")
        name = (
            LeadCommandName.GET_PROJECT_STATE
            if call.name == "read_status"
            else LeadCommandName.GET_RESULT
        )
        return await read_projection(session, name, goal, delegation)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def bounded_result(value: dict[str, Any], budget: int) -> tuple[dict[str, Any], int, bool]:
    """Count serialized UTF-8 bytes, including JSON escaping and truncation markers."""
    rendered = _json(value)
    size = len(rendered.encode("utf-8"))
    if size <= budget:
        return value, size, False
    # Preserve a readable prefix of oversized projections without claiming it is complete.
    low, high = 0, len(rendered)
    while low < high:
        middle = (low + high + 1) // 2
        candidate = {"truncated": True, "text": rendered[:middle]}
        if len(_json(candidate).encode("utf-8")) <= budget:
            low = middle
        else:
            high = middle - 1
    truncated = {"truncated": True, "text": rendered[:low]}
    return truncated, len(_json(truncated).encode("utf-8")), True


async def run_tool_batch(
    factory: async_sessionmaker[AsyncSession],
    snapshot: LeadSnapshot,
    batch: ToolBatch,
    *,
    round: int,
    used_bytes: int,
    limits: HardLimits,
) -> tuple[tuple[ContextItem, ...], int, bool]:
    audits = []
    exhausted = len(batch.tool_calls) >= limits.lead_tool_calls_per_round
    byte_exhausted = False
    for ordinal, raw in enumerate(batch.tool_calls):
        name = raw.get("name") if isinstance(raw, dict) else None
        args = raw.get("args") if isinstance(raw, dict) else raw
        # The full attempted call is already in this round's provider receipt.
        # Echo only a bounded preview of malformed/overlong arguments to the model.
        if len(_json(args).encode()) > 2048:
            args = {"truncated": True, "text": _json(args)[:1000]}
        if not isinstance(name, str | type(None)) or (name is not None and len(name) > 100):
            name = _json(name)[:100]
        audit: dict[str, Any] = {
            "round": round,
            "call": ordinal,
            "name": name,
            "args": args,
            "result_bytes": 0,
            "hit_count": 0,
        }
        remaining = limits.lead_tool_total_bytes - used_bytes
        if ordinal >= limits.lead_tool_calls_per_round:
            audit["error"] = "tool_call_limit"
        elif byte_exhausted or remaining < 128:
            audit["error"] = "tool_byte_limit"
            exhausted = True
        else:
            try:
                call = parse_tool_call(raw)
            except ValidationError:
                audit["error"] = "invalid_tool_call"
                value = {"error": "invalid_tool_call"}
            else:
                try:
                    value = await execute_read(factory, snapshot, call)
                except LeadModelUnavailable:
                    audit["error"] = "read_authority_unavailable"
                    await record_tool_audit(factory, round, audit)
                    raise
            audit["hit_count"] = len(value.get("hits", []))
            bounded, size, truncated = bounded_result(
                value, min(remaining, limits.lead_tool_result_bytes, 16000)
            )
            audit.update(result=bounded, result_bytes=size, truncated=truncated)
            used_bytes += size
            byte_exhausted = truncated or used_bytes >= limits.lead_tool_total_bytes
            exhausted |= byte_exhausted
        await record_tool_audit(
            factory,
            round,
            {
                **audit,
                # Preserve exact attempted arguments in the receipt, even when the
                # model context needs a bounded preview of an invalid call.
                "name": raw.get("name") if isinstance(raw, dict) else None,
                "args": raw.get("args") if isinstance(raw, dict) else raw,
            },
        )
        audits.append(audit)
    return _pack_audits(audits, round), used_bytes, exhausted


async def record_tool_audit(
    factory: async_sessionmaker[AsyncSession], round: int, audit: dict[str, Any]
) -> None:
    """Receipt bookkeeping survives revocation; it grants no command authority."""
    context = current_turn.get()
    if context is None:
        return
    receipt = {key: value for key, value in audit.items() if key != "result"}
    async with factory.begin() as session:
        row = await session.scalar(
            select(LeadershipInference)
            .where(
                LeadershipInference.turn_id == context.turn_id,
                LeadershipInference.phase == "lead",
                LeadershipInference.round == round,
            )
            .with_for_update()
        )
        if row is None or row.state != "received":
            raise LeadModelUnavailable("lead_inference_reconciliation")
        if receipt not in row.tool_audit:
            row.tool_audit = [*row.tool_audit, receipt]


def _pack_audits(audits: list[dict[str, Any]], round: int) -> tuple[ContextItem, ...]:
    # Group small/error results so an overlong call list cannot exceed the
    # provider's 50 context-item limit and make a saved request unreplayable.
    chunks: list[list[dict[str, Any]]] = [[]]
    for audit in audits:
        if len(_json({"tool_results": [*chunks[-1], audit]})) > 20000 and chunks[-1]:
            chunks.append([])
        chunks[-1].append(audit)
    items = []
    for index, chunk in enumerate(chunks):
        content = _json({"tool_results": chunk})
        items.append(
            ContextItem(
                source="untrusted_lead_tool_result",
                reference=f"lead-round:{round}:results:{index}",
                content=content,
                content_hash=hashlib.sha256(content.encode()).hexdigest(),
            )
        )
    return tuple(items)
