from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from vuzol.config.models import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.providers.domain import ProviderRequest
from vuzol.providers.domain import ProviderRole as RequestRole
from vuzol.providers.kimi import KIMI_MODEL, KimiCliAdapter, canonical_kimi_argv
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext


class Transport:
    def __init__(self) -> None:
        self.invocation = None

    async def run(self, invocation, cancellation):
        del cancellation
        self.invocation = invocation
        return CodexProcessResult(
            exit_code=0,
            stdout='{"role":"assistant","content":"done"}\n',
            stderr="",
            duration_ms=12,
        )


def profile() -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id="tokenrouter-kimi-a",
        provider="kimi",
        model=KIMI_MODEL,
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.CHEAP,
        roles=frozenset({ProviderRole.EXECUTOR}),
        supported_task_types=frozenset({"coding"}),
        runtime_identity="vuzol-kimi-a",
        state_directory=Path("/var/lib/vuzol-provider-state/kimi-test"),
    )


def request() -> ProviderRequest:
    identity = {name: uuid.uuid4() for name in ("task_id", "run_id", "step_id")}
    return ProviderRequest(
        **identity,
        role=RequestRole.EXECUTOR,
        original_input="change it",
        system_policy_revision="test-policy",
        prompt_revision="test-prompt",
        required_capabilities=frozenset({Capability.CODE_EDIT}),
        max_input_tokens=10_000,
        max_output_tokens=1_000,
        reserved_cost_units=1,
        reserved_quota_units=1,
        timeout_seconds=60,
        sandbox_reference="worktree:" + str(uuid.uuid4()),
        provider_attempt=1,
        lease_generation=1,
    )


def test_canonical_kimi_command_is_model_allowlisted() -> None:
    assert canonical_kimi_argv(KIMI_MODEL)[0:2] == ("sh", "-c")
    with pytest.raises(ValueError, match="allowlisted"):
        canonical_kimi_argv("another-model")


@pytest.mark.anyio
async def test_adapter_keeps_task_prompt_on_stdin() -> None:
    transport = Transport()
    result = await KimiCliAdapter(transport).execute(request(), profile(), CancellationContext())

    assert result.text == "done"
    assert transport.invocation is not None
    assert "change it" not in " ".join(transport.invocation.argv)
    assert "change it" in transport.invocation.stdin
