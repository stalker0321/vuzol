"""Raw user turns, including trailing restrictions, survive CLI/API envelopes."""

import json
from pathlib import Path

import pytest

from tests.unit.providers._test_providers_helpers import (
    codex_jsonl,
    codex_profile,
    codex_request,
)
from tests.unit.providers.test_kimi_adapter import profile as kimi_profile
from tests.unit.providers.test_pi_adapter import FIXTURES
from tests.unit.providers.test_pi_adapter import profile as pi_profile
from vuzol.config import Capability, ProviderRole
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.domain import WHOLE_TURN_POLICY
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.kimi import KimiCliAdapter
from vuzol.providers.openai import _payload
from vuzol.providers.pi import PiCliAdapter
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.workflows.ports import CancellationContext

RAW = (
    "Проверь поиск.\n"
    + "Контекст задачи. " * 350
    + "\n"
    + "Если тесты упали, исправь только поиск; не меняй авторизацию. Пока не запускай деплой."
)


@pytest.mark.anyio
@pytest.mark.parametrize("provider", ["codex", "pi", "grok", "kimi"])
@pytest.mark.parametrize("read_only", [False, True])
async def test_cli_keeps_full_raw_turn_not_truncated_goal(
    tmp_path: Path,
    provider: str,
    read_only: bool,
) -> None:
    class Transport:
        invocation: CodexInvocation | None = None

        async def run(
            self,
            invocation: CodexInvocation,
            cancellation: CancellationContext,
        ) -> CodexProcessResult:
            self.invocation = invocation
            if provider == "pi":
                stdout = (FIXTURES / "print-json-text.jsonl").read_text()
            elif provider == "grok":
                stdout = (
                    json.dumps({"type": "text", "data": "done"})
                    + "\n"
                    + json.dumps(
                        {
                            "type": "end",
                            "stopReason": "EndTurn",
                            "usage": {},
                        }
                    )
                )
            elif provider == "kimi":
                stdout = json.dumps({"role": "assistant", "content": "done"})
            else:
                stdout = codex_jsonl("done")
            return CodexProcessResult(0, stdout, "", 2)

    transport = Transport()
    request = codex_request().model_copy(
        update={
            "original_input": RAW,
            "role": ProviderRole.PLANNER if read_only else ProviderRole.EXECUTOR,
            "required_capabilities": frozenset({Capability.REPOSITORY_READ})
            if read_only
            else frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
            "task_draft": {"goal": RAW[:4000], "task_summary": "Проверить поиск"},
        }
    )
    if provider == "pi":
        adapter, profile = PiCliAdapter(transport), pi_profile()
    elif provider == "kimi":
        adapter, profile = KimiCliAdapter(transport), kimi_profile()
    elif provider == "grok":
        adapter = GrokCliAdapter(transport)
        profile = codex_profile(tmp_path).model_copy(
            update={"provider": "grok", "model": "grok-build"}
        )
    else:
        adapter, profile = CodexCliAdapter(transport), codex_profile(tmp_path)
    await adapter.execute(request, profile, CancellationContext())
    assert transport.invocation is not None
    envelope = json.loads(transport.invocation.stdin)
    assert envelope["original_input"] == RAW
    assert len(envelope["original_input"]) > 4000
    assert envelope["task_draft"]["goal"] != RAW
    assert WHOLE_TURN_POLICY in json.dumps(envelope["execution_policy"], ensure_ascii=False)
    assert RAW not in " ".join(transport.invocation.argv)


def test_api_also_keeps_full_raw_turn_and_restrictions(tmp_path: Path) -> None:
    request = codex_request().model_copy(
        update={"original_input": RAW, "task_draft": {"goal": RAW[:4000]}}
    )
    payload = _payload(request, codex_profile(tmp_path))
    assert json.loads(payload["messages"][1]["content"])["original_input"] == RAW
    assert WHOLE_TURN_POLICY in payload["messages"][0]["content"]
