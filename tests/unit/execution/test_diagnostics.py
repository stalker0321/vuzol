from __future__ import annotations

import hashlib
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


def test_cli_diagnostics_count_oversized_and_unparseable_lines_as_malformed() -> None:
    oversized = json.dumps({"type": "item.started", "pad": "y" * 262_144})
    summary = summarize_cli_process(
        provider="codex",
        argv=("codex",),
        stdout="\n".join(
            (
                oversized,
                "[1, 2]",
                "not json at all",
                json.dumps({"type": "turn.completed"}),
            )
        ),
        stderr="",
        exit_code=0,
    )

    assert summary["malformed_event_count"] == 3
    assert summary["event_count"] == 1
    assert summary["last_event_type"] == "turn.completed"
    assert summary["failure_signals"] == []


def test_cli_diagnostics_ring_buffer_keeps_only_last_128_events() -> None:
    stdout = "\n".join(json.dumps({"type": f"step.{index}"}) for index in range(1, 131))
    summary = summarize_cli_process(
        provider="codex",
        argv=("codex",),
        stdout=stdout,
        stderr="",
        exit_code=0,
    )
    events = summary["events"]

    assert isinstance(events, list)
    assert summary["event_count"] == 128
    assert [event["sequence"] for event in (events[0], events[-1])] == [3, 130]


def test_cli_diagnostics_extract_tool_identity_from_tool_calls_and_meta() -> None:
    stdout = "\n".join(
        (
            json.dumps(
                {
                    "type": "response.output",
                    "tool_calls": [
                        "junk",
                        {"name": "shell", "function": {"name": "ignored-inner"}},
                        {"tool_name": "grep"},
                    ],
                }
            ),
            json.dumps({"type": "mcp.call", "_meta": {"x.ai/tool": {"name": "browser"}}}),
            json.dumps({"type": "mcp.call", "_meta": {"x.ai/tool": {"name": "unsafe tool name"}}}),
            json.dumps({"type": "mcp.call", "_meta": {"unrelated": True}}),
        )
    )
    summary = summarize_cli_process(
        provider="grok",
        argv=("grok",),
        stdout=stdout,
        stderr="",
        exit_code=0,
    )
    events = summary["events"]

    assert isinstance(events, list)
    assert events[0]["provider_tool_name"] == "shell"
    assert events[1]["provider_tool_name"] == "browser"
    assert "provider_tool_name" not in events[2]
    assert summary["last_provider_tool_name"] == "browser"


def test_cli_diagnostics_prefer_direct_call_ids_and_reject_unsafe_item_ids() -> None:
    stdout = "\n".join(
        (
            json.dumps({"type": "tool.begin", "call_id": "call-direct"}),
            json.dumps({"type": "tool.end", "item": {"id": "not a safe id"}}),
        )
    )
    summary = summarize_cli_process(
        provider="codex",
        argv=("codex",),
        stdout=stdout,
        stderr="",
        exit_code=0,
    )
    events = summary["events"]

    assert isinstance(events, list)
    assert events[0]["tool_call_id"] == "call-direct"
    assert "tool_call_id" not in events[1]
    assert summary["last_tool_call_id"] is None


def test_cli_diagnostics_mark_status_completed_events_as_result_received() -> None:
    summary = summarize_cli_process(
        provider="grok",
        argv=("grok",),
        stdout=json.dumps({"type": "tool.execution", "status": "succeeded"}),
        stderr="",
        exit_code=0,
    )
    events = summary["events"]

    assert isinstance(events, list)
    assert events[0]["result_received"] is True


def test_cli_diagnostics_signal_failed_turn_ended_and_outcome_once() -> None:
    summary = summarize_cli_process(
        provider="grok",
        argv=("grok",),
        stdout="\n".join(
            (
                json.dumps({"type": "turn_ended", "outcome": "cancelled"}),
                json.dumps({"type": "session.end", "outcome": "failed"}),
                json.dumps({"type": "turn_ended", "outcome": "cancelled"}),
            )
        ),
        stderr="",
        exit_code=0,
    )

    assert summary["failure_signals"] == ["event:turn_ended", "outcome:failed"]
    assert should_capture_cli_forensics(summary)


def test_safe_argv_hashes_entries_exceeding_inline_budget() -> None:
    oversized = "x" * 256
    summary = summarize_cli_process(
        provider="codex",
        argv=("codex", oversized, "broken\x00argv"),
        stdout="",
        stderr="",
        exit_code=0,
    )
    argv = summary["argv"]

    assert isinstance(argv, list)
    assert argv == [
        "codex",
        f"sha256:{hashlib.sha256(oversized.encode()).hexdigest()}",
        f"sha256:{hashlib.sha256(b'broken\x00argv').hexdigest()}",
    ]
