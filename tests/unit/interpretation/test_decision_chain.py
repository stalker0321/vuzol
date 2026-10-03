"""J1 decision-chain limiter and kill switch (no provider call on refusal)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import ProviderProfileConfig
from vuzol.config.settings import HardLimits
from vuzol.context.decision_binding import (
    Coverage,
    DecisionBinding,
    DecisionBindingError,
    DecisionSnapshot,
)
from vuzol.interpretation.decision_chain import (
    DecisionChainExhausted,
    DecisionChainFrozen,
    DecisionChainLimits,
    DecisionChainState,
    apply_decision,
    execute_decision_step,
)
from vuzol.providers.budgets import ReservationEstimate

_FACTORY = cast("async_sessionmaker[AsyncSession]", None)


def _profile() -> ProviderProfileConfig:
    return ProviderProfileConfig.model_validate(
        {
            "id": "api",
            "provider": "openai-compatible",
            "model": "model",
            "api_base_url": "https://provider.example/v1",
            "launch_mode": "api",
            "credential_required": False,
            "capabilities": frozenset(),
            "concurrency_limit": 1,
            "cost_class": "balanced",
            "roles": frozenset({"planner"}),
            "supported_task_types": frozenset({"general"}),
            "sandbox_required": False,
            "minimum_unknown_usage_cost": 0.01,
        }
    )


def _estimate(cost: str) -> ReservationEstimate:
    return ReservationEstimate(
        input_tokens=1,
        output_tokens=1,
        cost_units=Decimal(cost),
        quota_units=Decimal("0"),
    )


def test_kind_change_never_resets_chain_counter() -> None:
    chain = DecisionChainState(DecisionChainLimits(max_calls=2, max_cost_units=Decimal("1")))
    chain.admit(decision_kind="intake", estimate=_estimate("0.1"))
    chain.record(decision_kind="intake", estimate=_estimate("0.1"))
    chain.admit(decision_kind="target_resolution", estimate=_estimate("0.1"))
    chain.record(decision_kind="target_resolution", estimate=_estimate("0.1"))

    with pytest.raises(DecisionChainExhausted):
        chain.admit(decision_kind="work_shape", estimate=_estimate("0.1"))
    assert chain.calls == 2
    assert chain.kinds == frozenset({"intake", "target_resolution"})


def test_cost_limit_is_shared_across_kinds() -> None:
    chain = DecisionChainState(DecisionChainLimits(max_calls=5, max_cost_units=Decimal("0.02")))
    chain.admit(decision_kind="intake", estimate=_estimate("0.01"))
    chain.record(decision_kind="intake", estimate=_estimate("0.01"))
    chain.admit(decision_kind="target_resolution", estimate=_estimate("0.01"))
    chain.record(decision_kind="target_resolution", estimate=_estimate("0.01"))
    with pytest.raises(DecisionChainExhausted):
        chain.admit(decision_kind="repair_triage", estimate=_estimate("0.01"))


def test_kill_switch_refuses_execute_before_provider_call() -> None:
    calls: list[str] = []

    async def provider() -> str:
        calls.append("called")
        return "result"

    async def scenario() -> None:
        with pytest.raises(DecisionChainFrozen):
            await execute_decision_step(
                _FACTORY,
                profile=_profile(),
                limits=HardLimits(),
                decision_kind="intake",
                estimate=_estimate("0.01"),
                call=provider,
                kill_switch=True,
            )

    asyncio.run(scenario())
    assert calls == []


def test_chain_exhaustion_refuses_before_provider_call() -> None:
    calls: list[str] = []

    async def provider() -> str:
        calls.append("called")
        return "result"

    chain = DecisionChainState(DecisionChainLimits(max_calls=0, max_cost_units=Decimal("1")))

    async def scenario() -> None:
        with pytest.raises(DecisionChainExhausted):
            await execute_decision_step(
                _FACTORY,
                profile=_profile(),
                limits=HardLimits(),
                decision_kind="intake",
                estimate=_estimate("0.01"),
                call=provider,
                chain=chain,
            )

    asyncio.run(scenario())
    assert calls == []


def test_apply_decision_refuses_on_frozen_and_drift() -> None:
    binding = DecisionBinding(
        decision_kind="intake",
        request_hash="a" * 64,
        schema_hash="b" * 64,
        prompt_hash="c" * 64,
        packet_hash="d" * 64,
        coverage=Coverage.COMPLETE,
    )
    applied: list[str] = []

    async def apply() -> str:
        applied.append("applied")
        return "ok"

    async def scenario() -> None:
        with pytest.raises(DecisionChainFrozen):
            await apply_decision(
                binding=binding,
                snapshot=DecisionSnapshot(refs={}, candidate_ids=frozenset()),
                apply=apply,
                kill_switch=True,
            )
        frozen = DecisionSnapshot(refs={}, candidate_ids=frozenset(), kill_switch=True)
        with pytest.raises(DecisionBindingError):
            await apply_decision(binding=binding, snapshot=frozen, apply=apply)
        drifted = DecisionSnapshot(refs={}, candidate_ids=frozenset({"new:candidate"}))
        with pytest.raises(DecisionBindingError):
            await apply_decision(binding=binding, snapshot=drifted, apply=apply)
        current = DecisionSnapshot(refs={}, candidate_ids=frozenset())
        assert await apply_decision(binding=binding, snapshot=current, apply=apply) == "ok"

    asyncio.run(scenario())
    assert applied == ["applied"]
