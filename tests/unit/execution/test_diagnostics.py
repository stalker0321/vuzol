from __future__ import annotations

import json

from vuzol.execution.diagnostics import (
    build_cli_forensic_diagnostics,
    should_capture_cli_forensics,
    summarize_cli_process,
)


def test_cli_diagnostics_preserve_safe_codex_failure_identity() -> None:
    stdout = "\n".join(
        (
            json.dumps(
                {
                    "type": "item.started",
                    "item": {
                        "type": "function_call",
                        "name": "shell",
                        "id": "call-123",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "function_call_output", "id": "call-123"},
                }
            ),
            json.dumps({"type": "turn.failed", "error": "SECRET_PROVIDER_DETAIL"}),
        )
    )
    summary = summarize_cli_process(
        provider="codex",
        argv=("codex", "exec", "--json"),
        stdout=stdout,
        stderr="SECRET_STDERR",
        exit_code=1,
    )

    assert should_capture_cli_forensics(summary)
    assert summary["last_provider_tool_name"] == "shell"
    assert summary["last_tool_call_id"] == "call-123"
    failure_signals = summary["failure_signals"]
    assert isinstance(failure_signals, list)
    assert "process_exit:1" in failure_signals
    serialized = json.dumps(summary, sort_keys=True)
    assert "SECRET_PROVIDER_DETAIL" not in serialized
    assert "SECRET_STDERR" not in serialized


def test_cli_diagnostics_support_kimi_stream_and_redact_content() -> None:
    stdout = json.dumps({"role": "assistant", "content": "SECRET_ASSISTANT_CONTENT"})
    summary = summarize_cli_process(
        provider="kimi",
        argv=("sh", "-c", "exec kimi --model tokenrouter/kimi-k3-free"),
        stdout=stdout,
        stderr="rate limit",
        exit_code=0,
    )
    artifact = build_cli_forensic_diagnostics(
        provider="kimi",
        argv=("sh", "-c", "exec kimi --model tokenrouter/kimi-k3-free"),
        stdout=stdout,
        stderr="rate limit",
        exit_code=0,
    ).decode()

    assert summary["last_event_type"] == "role:assistant"
    failure_signals = summary["failure_signals"]
    assert isinstance(failure_signals, list)
    assert "stderr_nonempty" in failure_signals
    assert "SECRET_ASSISTANT_CONTENT" not in artifact
    assert "rate limit" not in artifact


def test_successful_turn_ended_event_is_not_a_failure_signal() -> None:
    summary = summarize_cli_process(
        provider="grok",
        argv=("grok",),
        stdout=json.dumps({"type": "turn_ended", "outcome": "succeeded"}),
        stderr="",
        exit_code=0,
    )

    assert summary["failure_signals"] == []
    assert not should_capture_cli_forensics(summary)
