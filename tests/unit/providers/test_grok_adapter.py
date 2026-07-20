"""Grok adapter tests (split for cohesion)."""

from __future__ import annotations

from ._test_providers_helpers import (
    CancellationContext,
    CodexInvocation,
    CodexProcessResult,
    FakeCodexTransport,
    GrokCliAdapter,
    LaunchMode,
    Path,
    ProviderErrorCategory,
    ProviderFailure,
    _grok_diagnostic_events,
    _grok_tool_updates,
    hashlib,
    json,
    profile,
    provider_request,
    pytest,
)


@pytest.mark.anyio
async def test_grok_adapter_uses_strict_headless_contract(tmp_path: Path) -> None:
    class GrokTransport(FakeCodexTransport):
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            self.invocations.append(invocation)
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        json.dumps([]),
                        json.dumps({"type": "thought", "data": "private reasoning"}),
                        json.dumps({"type": "text", "data": "safe "}),
                        json.dumps({"type": "text", "data": "result"}),
                        json.dumps(
                            {
                                "type": "end",
                                "stopReason": "EndTurn",
                                "sessionId": "session",
                                "requestId": "request",
                                "usage": {
                                    "input_tokens": 7,
                                    "output_tokens": 2,
                                    "cache_read_input_tokens": 3,
                                },
                            }
                        ),
                    )
                ),
                "",
                12,
            )

    transport = GrokTransport()
    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    result = await GrokCliAdapter(transport).execute(
        provider_request().model_copy(
            update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
        ),
        configured,
        CancellationContext(),
    )
    invocation = transport.invocations[0]
    assert result.text == "safe result"
    assert result.provider_request_id == "request"
    assert result.usage.cached_tokens == 3
    assert invocation.argv[:2] == ("grok", "--no-auto-update")
    assert invocation.argv[2:4] == ("--prompt-file", "/dev/stdin")
    assert "dontAsk" in invocation.argv and "strict" in invocation.argv
    assert "Read(/grok-home/**)" in invocation.argv
    assert "Edit(/grok-home/**)" in invocation.argv
    assert "Bash(git *)" in invocation.argv
    assert "Bash(tail -n 1 README.md)" in invocation.argv
    assert "Bash(date +%s%3N)" in invocation.argv
    assert "Bash(*)" not in invocation.argv
    assert "auth.json" not in invocation.stdin
    assert "do not use cd" in invocation.stdin
    assert (await GrokCliAdapter(transport).health(configured)).healthy
    from vuzol.providers.grok import _usage_int

    assert _usage_int([], "input_tokens") is None


@pytest.mark.anyio
async def test_grok_adapter_fails_closed_for_invalid_execution(tmp_path: Path) -> None:
    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    request = provider_request()
    adapter = GrokCliAdapter(FakeCodexTransport())
    with pytest.raises(ProviderFailure, match="isolation"):
        await adapter.execute(
            request.model_copy(
                update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
            ),
            configured.model_copy(update={"runtime_identity": None}),
            CancellationContext(),
        )
    with pytest.raises(ProviderFailure, match="requires an isolated"):
        await adapter.execute(request, configured, CancellationContext())

    class FailedTransport:
        def __init__(self, result: CodexProcessResult | None = None) -> None:
            self.result = result

        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            if self.result is None:
                raise RuntimeError("transport unavailable")
            return self.result

    fenced = request.model_copy(
        update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
    )
    for transport in (
        FailedTransport(),
        FailedTransport(CodexProcessResult(1, "", "failed", 1)),
        FailedTransport(CodexProcessResult(0, '{"type":"text","data":"partial"}', "", 1)),
    ):
        with pytest.raises(ProviderFailure):
            await GrokCliAdapter(transport).execute(fenced, configured, CancellationContext())

    class InvalidInvocationTransport(FailedTransport):
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            raise ValueError("invalid invocation")

    with pytest.raises(ValueError, match="invalid invocation"):
        await GrokCliAdapter(InvalidInvocationTransport()).execute(
            fenced, configured, CancellationContext()
        )


@pytest.mark.anyio
async def test_grok_adapter_classifies_structured_provider_cancellation(tmp_path: Path) -> None:
    class CancelledTransport:
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        '{"type":"thought","data":"sensitive model output"}',
                        '{"type":"end","stopReason":"Cancelled","requestId":"request"}',
                    )
                ),
                "",
                75_700,
            )

    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    fenced = provider_request().model_copy(
        update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
    )
    with pytest.raises(ProviderFailure) as captured:
        await GrokCliAdapter(CancelledTransport()).execute(
            fenced, configured, CancellationContext()
        )
    assert captured.value.category is ProviderErrorCategory.CANCELLED
    assert captured.value.retryable is False


@pytest.mark.anyio
async def test_grok_adapter_validates_step09a_edit_report(tmp_path: Path) -> None:
    manifest = {
        "schema_version": "step09a-worker-edit-report.v1",
        "experiment_id": "experiment",
        "task_id": "task",
        "claimed_complete": True,
        "implementation_summary": "Implemented the requested change.",
        "usage": {
            "input_tokens": None,
            "cached_input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
            "unavailable_reason": "The worker does not receive provider accounting.",
        },
        "failure_classification": None,
        "limitations": [],
        "attempt": 1,
    }

    invocations: list[CodexInvocation] = []

    class ManifestTransport:
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del cancellation
            invocations.append(invocation)
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        json.dumps({"type": "text", "data": json.dumps(manifest)}),
                        json.dumps({"type": "end", "stopReason": "EndTurn"}),
                    )
                ),
                "",
                10,
            )

    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    request = provider_request().model_copy(
        update={
            "sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001",
            "output_schema_version": "step09a-worker-edit-report.v1",
        }
    )
    result = await GrokCliAdapter(ManifestTransport()).execute(
        request, configured, CancellationContext()
    )
    assert result.structured_output == manifest
    prompt = json.loads(invocations[-1].stdin)
    instruction = prompt["execution_policy"]["result_manifest"]
    assert "Do not invoke shell commands, Git, or project gates" in instruction
    assert "Vuzol owns inspection, gates, staging, commit creation" in instruction
    shell_instruction = prompt["execution_policy"]["shell_invocation"]
    assert "Do not invoke native shell tools" in shell_instruction
    assert "git, make, or ./verify.sh" not in shell_instruction

    usage = manifest["usage"]
    assert isinstance(usage, dict)
    invalid = {**manifest, "usage": {**usage, "unavailable_reason": None}}
    manifest.clear()
    manifest.update(invalid)
    with pytest.raises(ProviderFailure) as captured:
        await GrokCliAdapter(ManifestTransport()).execute(
            request, configured, CancellationContext()
        )
    assert captured.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
    assert captured.value.retryable is False


def test_grok_event_summary_is_content_free_for_completed_response() -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        "\n".join(
            (
                '{"type":"thought","data":"private prompt and output"}',
                '{"type":"end","stopReason":"Cancelled","requestId":"secret-id"}',
            )
        )
    )
    serialized = json.dumps(summary)
    assert summary["event_count"] == 2
    assert summary["last_event_type"] == "end"
    assert summary["last_stop_reason"] == "Cancelled"
    assert summary["schema_version"] == "grok-event-summary.v2"
    assert summary["cancellation_evidence_category"] == "PROVIDER_CANCELLED_UNATTRIBUTED"
    assert summary["evidence_completeness"] == "unavailable"
    assert "private prompt" not in serialized
    assert "secret-id" not in serialized

    completed = summarize_grok_events(
        "\n".join(
            (
                '{"type":"text","data":"private final response"}',
                '{"type":"end","stopReason":"EndTurn"}',
            )
        )
    )
    assert completed["cancellation_evidence_category"] is None
    assert completed["final_text_generation_began"] is True
    assert "private final response" not in json.dumps(completed)


@pytest.mark.parametrize(
    "session_id",
    ("../escape", "nested/session", "session\\name", "", "space value"),
)
def test_grok_session_id_rejects_traversal_and_malformed_values(session_id: str) -> None:
    from vuzol.providers.grok import grok_session_id_from_stdout, staged_grok_diagnostic_paths

    stdout = json.dumps({"type": "end", "stopReason": "EndTurn", "sessionId": session_id})
    assert grok_session_id_from_stdout(stdout) is None
    assert staged_grok_diagnostic_paths(Path("/safe/staging"), session_id) is None


def test_grok_session_id_selects_only_the_final_exact_protocol_session() -> None:
    from vuzol.providers.grok import grok_session_id_from_stdout

    older = "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    exact = "019f6149-44c0-7520-932c-5e0f41c99351"
    stdout = "\n".join(
        (
            f'{{"type":"end","sessionId":"{older}"}}',
            '{"type":"thought","sessionId":"019f0000-0000-7000-8000-000000000000"}',
            f'{{"type":"end","sessionId":"{exact}"}}',
        )
    )
    assert grok_session_id_from_stdout(stdout) == exact


def test_grok_event_summary_proves_permission_cancellation_and_correlates_sequences() -> None:
    from vuzol.providers.grok import summarize_grok_events

    stdout = "\n".join(
        (
            '{"type":"thought","data":"SECRET_THOUGHT_PAYLOAD"}',
            (
                '{"type":"end","stopReason":"Cancelled",'
                '"sessionId":"019f5e8d-d90b-7e40-a698-8a71fa87eff8",'
                '"requestId":"019f5e8d-d90b-7e40-a698-8a71fa87eff9"}'
            ),
        )
    )
    summary = summarize_grok_events(
        stdout,
        diagnostic_events=_grok_diagnostic_events(
            decision="cancelled", completed=False, category="permission_cancelled"
        ).splitlines(),
        session_updates=_grok_tool_updates("make test", completed=False).splitlines(),
    )
    serialized = json.dumps(summary, sort_keys=True)
    assert summary["cancellation_evidence_category"] == "PROVIDER_PERMISSION_CANCELLED"
    assert summary["last_permission_decision"] == "cancelled"
    assert summary["last_safe_command_identity"] == "make test"
    assert summary["last_tool_kind"] == "Bash"
    assert summary["last_native_tool_request_sequence"] == 2
    assert summary["last_permission_event_sequence"] == 4
    assert summary["last_native_tool_result_sequence"] is None
    assert summary["last_tool_result_received"] is False
    assert summary["cancellation_stage"] == "after_permission_cancellation_before_execution"
    assert summary["evidence_completeness"] == "complete"
    assert summary["provider_session_id"] == "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    for unsafe in (
        "SECRET_THOUGHT_PAYLOAD",
        "private task detail",
        "private tool output",
        "unsafe display title",
    ):
        assert unsafe not in serialized


@pytest.mark.parametrize(
    ("category", "classification"),
    (
        ("invalid_tool", "INVALID_TOOL_INVOCATION"),
        ("cancelled", "PROVIDER_INTERNAL_CANCELLED"),
    ),
)
def test_grok_event_summary_uses_first_party_cancellation_category(
    category: str, classification: str
) -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        '{"type":"end","stopReason":"Cancelled"}',
        diagnostic_events=_grok_diagnostic_events(
            decision="allow", completed=False, category=category
        ).splitlines(),
        session_updates=_grok_tool_updates("make type-check", completed=False).splitlines(),
    )
    assert summary["cancellation_evidence_category"] == classification
    assert summary["last_permission_decision"] == "allowed"
    assert summary["cancellation_stage"] == "during_tool_execution"


@pytest.mark.parametrize(
    ("decision", "completed", "expected_decision", "expected_result"),
    (("allow", True, "allowed", True), ("deny", False, "denied", False)),
)
def test_grok_event_summary_tracks_permission_and_tool_result(
    decision: str, completed: bool, expected_decision: str, expected_result: bool
) -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        '{"type":"end","stopReason":"EndTurn"}',
        diagnostic_events=_grok_diagnostic_events(
            decision=decision, completed=completed
        ).splitlines(),
        session_updates=_grok_tool_updates("make lint", completed=completed).splitlines(),
    )
    assert summary["last_permission_decision"] == expected_decision
    assert summary["last_tool_result_received"] is expected_result
    assert summary["last_native_tool_result_sequence"] == (5 if completed else None)
    if completed:
        assert summary["last_completed_tool_action"] is not None
    else:
        assert summary["last_completed_tool_action"] is None


@pytest.mark.parametrize(
    ("command", "family", "flag"),
    (
        ("custom-tool --password SECRET_COMMAND_VALUE", "unknown", "unknown_command_family"),
        ("cd /private && make test", "cd", "shell_chain_operators"),
        ("/bin/sh -c 'make test'", "shell", "shell_wrapper"),
        ("TOKEN=SECRET make test", "unknown", "environment_prefix"),
        ("uv run pytest", "uv", "direct_tool_invocation"),
    ),
)
def test_grok_event_summary_hashes_unsafe_commands_without_retaining_them(
    command: str, family: str, flag: str
) -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        '{"type":"end","stopReason":"Cancelled"}',
        diagnostic_events=_grok_diagnostic_events(
            decision="cancelled", completed=False, category="permission_cancelled"
        ).splitlines(),
        session_updates=_grok_tool_updates(command, completed=False).splitlines(),
    )
    serialized = json.dumps(summary)
    assert summary["last_safe_command_identity"] is None
    assert summary["last_command_family"] == family
    assert summary["last_command_byte_length"] == len(command.encode())
    assert summary["last_command_sha256"] == hashlib.sha256(command.encode()).hexdigest()
    flags = summary["last_command_structural_flags"]
    assert isinstance(flags, dict)
    assert flags[flag] is True
    assert command not in serialized
    assert "SECRET" not in serialized


def test_grok_event_summary_drops_edit_read_grep_paths_and_payloads() -> None:
    from vuzol.providers.grok import summarize_grok_events

    diagnostic = "\n".join(
        json.dumps(event)
        for event in (
            {"type": "turn_started", "schema_version": "1.0"},
            {"type": "tool_started", "tool_name": "read_file"},
            {"type": "tool_completed", "tool_name": "read_file", "outcome": "success"},
            {"type": "tool_started", "tool_name": "grep"},
            {"type": "tool_completed", "tool_name": "grep", "outcome": "success"},
            {"type": "tool_started", "tool_name": "search_replace"},
            {
                "type": "tool_completed",
                "tool_name": "search_replace",
                "outcome": "success",
            },
        )
    )
    updates = "\n".join(
        json.dumps(
            {
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": f"call-1aa3af3d-e549-4c73-ac4e-fc0c08302ed{i}-3{i}",
                        "rawInput": {
                            "path": f"/SECRET/PATH/{name}",
                            "pattern": "SECRET_GREP_PATTERN",
                            "new_string": "SECRET_EDIT_CONTENT",
                        },
                        "_meta": {"x.ai/tool": {"name": name}},
                    }
                },
            }
        )
        for i, name in enumerate(("read_file", "grep", "search_replace"))
    )
    summary = summarize_grok_events(
        '{"type":"end","stopReason":"EndTurn"}',
        diagnostic_events=diagnostic.splitlines(),
        session_updates=updates.splitlines(),
    )
    serialized = json.dumps(summary)
    assert summary["last_tool_kind"] == "Edit"
    assert summary["diagnostic_tool_count"] == 3
    assert "SECRET" not in serialized
    assert "/PATH/" not in serialized


def test_grok_event_summary_is_malformed_safe_and_bounded() -> None:
    from vuzol.providers.grok import summarize_grok_events

    stdout = "\n".join(["not-json", *('{"type":"thought","data":"hidden"}' for _ in range(140))])
    diagnostic = [
        '{"type":"turn_started","schema_version":"1.0"}',
        "malformed",
        *(
            json.dumps({"type": "phase_changed", "phase": "streaming_reasoning"})
            for _ in range(140)
        ),
    ]
    summary = summarize_grok_events(stdout, diagnostic_events=diagnostic, session_updates=[])
    assert summary["event_count"] == 140
    assert summary["retained_event_count"] == 128
    assert summary["events_truncated"] is True
    assert summary["malformed_event_count"] == 1
    assert summary["diagnostic_event_count"] == 142
    assert summary["retained_diagnostic_event_count"] == 128
    assert summary["diagnostic_events_truncated"] is True
    assert summary["malformed_diagnostic_event_count"] == 1
    assert summary["evidence_completeness"] == "partial"
