"""Grok Build CLI adapter for subscription-backed sandbox execution."""

import hashlib
import json
import re
import uuid
from collections import deque
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from vuzol.config.models import ProviderProfileConfig
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import CodexInvocation, CodexProcessTransport
from vuzol.workflows.ports import CancellationContext


def canonical_grok_argv(model: str) -> tuple[str, ...]:
    """Return the only Grok command accepted by the production transport."""
    return (
        "grok",
        "--no-auto-update",
        "--prompt-file",
        "/dev/stdin",
        "--output-format",
        "streaming-json",
        "--cwd",
        "/workspace",
        "--model",
        model,
        "--permission-mode",
        "dontAsk",
        "--sandbox",
        "strict",
        "--allow",
        "Bash(git *)",
        "--allow",
        "Bash(make *)",
        "--allow",
        "Bash(./verify.sh*)",
        "--allow",
        "Bash(tail -n 1 README.md)",
        "--allow",
        "Bash(date +%s%3N)",
        "--allow",
        "Edit(**)",
        "--allow",
        "Read(**)",
        "--allow",
        "Grep(**)",
        "--deny",
        "Read(/grok-home/**)",
        "--deny",
        "Edit(/grok-home/**)",
        "--deny",
        "Bash(git push*)",
        "--deny",
        "Bash(git reset*)",
        "--deny",
        "Bash(git clean*)",
        "--no-memory",
        "--no-subagents",
        "--disable-web-search",
        "--verbatim",
    )


class GrokCliAdapter:
    adapter_version = "grok-cli.v1"

    def __init__(self, transport: CodexProcessTransport) -> None:
        self._transport = transport

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        if profile.runtime_identity is None or profile.state_directory is None:
            raise ProviderFailure(
                ProviderErrorCategory.PERMANENT_REQUEST,
                retryable=False,
                request_sent=False,
                safe_summary="Grok profile isolation is incomplete",
            )
        if request.sandbox_reference is None:
            raise ProviderFailure(
                ProviderErrorCategory.UNSUPPORTED_CAPABILITY,
                retryable=False,
                request_sent=False,
                safe_summary="Grok execution requires an isolated worktree sandbox",
            )
        prompt = json.dumps(
            {
                "schema_version": request.schema_version,
                "role": request.role.value,
                "original_input": request.original_input,
                "task_draft": request.task_draft,
                "context": [item.model_dump(mode="json") for item in request.context],
                "output_schema": request.output_json_schema,
                "execution_policy": {
                    "shell_invocation": _shell_contract_instruction(request),
                    "file_edits": "Use the Edit tool for workspace file changes.",
                    "result_manifest": _result_contract_instruction(request),
                },
            },
            ensure_ascii=False,
        )
        invocation = CodexInvocation(
            argv=canonical_grok_argv(profile.model),
            stdin=prompt,
            runtime_identity=profile.runtime_identity,
            state_directory=str(profile.state_directory),
            timeout_seconds=request.timeout_seconds,
            sandbox_reference=request.sandbox_reference,
            task_id=request.task_id,
            run_id=request.run_id,
            step_id=request.step_id,
            profile_id=profile.id,
            provider_attempt=request.provider_attempt,
            lease_generation=request.lease_generation,
        )
        try:
            result = await self._transport.run(invocation, cancellation)
        except ValueError:
            raise
        except RuntimeError as error:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="supervised Grok transport failed after launch was possible",
            ) from error
        if result.exit_code != 0:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="Grok CLI invocation failed",
            )
        try:
            body = _decode_output(result.stdout)
            usage = body.get("usage", {})
        except GrokProviderCancelled as error:
            raise ProviderFailure(
                ProviderErrorCategory.CANCELLED,
                retryable=False,
                request_sent=True,
                safe_summary="Grok CLI reported provider-originated cancellation",
            ) from error
        except (json.JSONDecodeError, ValueError) as error:
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Grok CLI returned invalid output",
            ) from error
        structured_output = _step09a_structured_output(request, str(body["text"]))
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=str(body["text"]),
            structured_output=structured_output,
            provider_request_id=_optional_string(body.get("request_id")),
            provider_session_id=_optional_string(body.get("session_id")),
            usage=NormalizedUsage(
                input_tokens=_usage_int(usage, "input_tokens"),
                output_tokens=_usage_int(usage, "output_tokens"),
                cached_tokens=_usage_int(usage, "cache_read_input_tokens"),
                duration_ms=result.duration_ms,
            ),
            finish_reason=_optional_string(body.get("finish_reason")),
            adapter_version=self.adapter_version,
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _decode_output(stdout: str) -> dict[str, object]:
    chunks: list[str] = []
    final: dict[str, object] | None = None
    for line in stdout.splitlines():
        event = json.loads(line)
        if not isinstance(event, dict):
            continue
        if event.get("type") == "text" and isinstance(event.get("data"), str):
            chunks.append(event["data"])
        elif event.get("type") == "end":
            final = event
    if final is not None and final.get("stopReason") == "Cancelled":
        raise GrokProviderCancelled("Grok JSONL ended with provider cancellation")
    if final is None or not chunks:
        raise ValueError("Grok JSONL contains no completed response")
    return {
        "text": "".join(chunks),
        "request_id": final.get("requestId"),
        "session_id": final.get("sessionId"),
        "finish_reason": final.get("stopReason"),
        "usage": final.get("usage", {}),
    }


class GrokProviderCancelled(ValueError):
    """The CLI completed normally but its structured protocol reported cancellation."""


def _step09a_structured_output(request: ProviderRequest, value: str) -> dict[str, object] | None:
    if request.output_schema_version not in {
        "step09a-worker-edit-report.v1",
        "step09a-worker-result.v1",
    }:
        return None
    from pydantic import ValidationError

    from vuzol.experiments.domain import WorkerEditReport, WorkerResultManifest

    try:
        contract = (
            WorkerEditReport
            if request.output_schema_version == "step09a-worker-edit-report.v1"
            else WorkerResultManifest
        )
        manifest = contract.model_validate_json(value)
    except ValidationError as error:
        raise ProviderFailure(
            ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
            retryable=False,
            request_sent=True,
            safe_summary="Grok CLI returned an invalid worker result manifest",
        ) from error
    return manifest.model_dump(mode="json")


def _result_contract_instruction(request: ProviderRequest) -> str:
    if request.output_schema_version == "step09a-worker-edit-report.v1":
        return (
            "Do not invoke shell commands, Git, or project gates. Vuzol owns inspection, gates, "
            "staging, commit creation, and the authoritative result manifest. Return only the "
            "small requested edit report; when all provider usage is unavailable, include a "
            "concise non-empty unavailable_reason."
        )
    return (
        "When every provider usage value is unavailable, set unavailable_reason to a concise "
        "non-empty explanation. Return an object that validates against the requested schema."
    )


def _shell_contract_instruction(request: ProviderRequest) -> str:
    if request.output_schema_version == "step09a-worker-edit-report.v1":
        return (
            "Do not invoke native shell tools. Use repository read, search, and edit tools only; "
            "the deterministic Vuzol finalizer owns all Git and gate commands."
        )
    return (
        "Invoke each allowed shell command separately; do not use cd, chains, wrappers, or "
        "command substitution. Commands must begin exactly with git, make, or ./verify.sh. The "
        "only additional read command allowed for the smoke repository is exactly: tail -n 1 "
        "README.md. To measure manifest durations, use only: date +%s%3N."
    )


_SUMMARY_EVENT_LIMIT = 128
_SUMMARY_TOOL_LIMIT = 64
_SUMMARY_LINE_BYTE_LIMIT = 262_144
GROK_DIAGNOSTIC_FILE_MAX_BYTES = 2_000_000
GROK_STAGED_DIAGNOSTIC_DIRECTORY = "grok-session-diagnostics"
_SAFE_STOP_REASONS = {"Cancelled", "EndTurn", "Error", "MaxTurns", "StopSequence"}
_SAFE_DIAGNOSTIC_SCHEMA_VERSIONS = {"1.0"}
_SAFE_PROTOCOL_TYPES = {
    "auto_compact_completed",
    "auto_compact_started",
    "end",
    "error",
    "max_turns_reached",
    "text",
    "thought",
}
_SAFE_DIAGNOSTIC_TYPES = {
    "turn_started",
    "loop_started",
    "first_token",
    "phase_changed",
    "tool_started",
    "tool_completed",
    "permission_requested",
    "permission_resolved",
    "turn_ended",
}
_SAFE_PHASES = {
    "waiting_for_model",
    "streaming_text",
    "streaming_reasoning",
    "tool_execution",
    "permission_prompt",
}
_SAFE_CANCELLATION_CATEGORIES = {
    "cancelled",
    "hook_denied",
    "invalid_tool",
    "mid_turn_abort",
    "permission_cancelled",
    "permission_rejected",
    "spawn_failed",
    "timeout",
    "transport",
}
_SAFE_TOOL_KINDS = {
    "grep": "Grep",
    "list_dir": "Read",
    "read_file": "Read",
    "run_terminal_command": "Bash",
    "search_replace": "Edit",
}
_SAFE_PROVIDER_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_SAFE_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def grok_session_id_from_stdout(stdout: str) -> str | None:
    """Extract only a strictly safe session ID from the final Grok protocol events."""
    session_id: str | None = None
    for line in stdout.splitlines():
        encoded = line.encode()
        if len(encoded) > _SUMMARY_LINE_BYTE_LIMIT:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("type") == "end":
            session_id = _safe_provider_id(event.get("sessionId"))
    return session_id


def staged_grok_diagnostic_paths(staging: Path, session_id: str) -> tuple[Path, Path] | None:
    """Resolve the two exact staged files for a validated Grok session ID."""
    if _safe_provider_id(session_id) != session_id:
        return None
    session = staging / GROK_STAGED_DIAGNOSTIC_DIRECTORY / session_id
    return session / "events.jsonl", session / "updates.jsonl"


def summarize_grok_events(
    stdout: str,
    *,
    diagnostic_events: Iterable[str] | None = None,
    session_updates: Iterable[str] | None = None,
) -> dict[str, object]:
    """Return a bounded, content-free summary of Grok protocol and session evidence."""
    events: deque[dict[str, object]] = deque(maxlen=_SUMMARY_EVENT_LIMIT)
    event_count = 0
    malformed_event_count = 0
    final: dict[str, object] | None = None
    provider_request_id: str | None = None
    provider_session_id: str | None = None
    final_text_generation_began = False
    for sequence, line in enumerate(stdout.splitlines(), start=1):
        encoded = line.encode()
        if len(encoded) > _SUMMARY_LINE_BYTE_LIMIT:
            malformed_event_count += 1
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            malformed_event_count += 1
            continue
        if not isinstance(event, dict):
            continue
        raw_event_type = event.get("type")
        if not isinstance(raw_event_type, str):
            continue
        event_type = raw_event_type if raw_event_type in _SAFE_PROTOCOL_TYPES else "unknown"
        event_count += 1
        stop_reason = _safe_value(event.get("stopReason"), _SAFE_STOP_REASONS)
        if event_type == "text":
            final_text_generation_began = True
        if event_type == "end":
            provider_request_id = _safe_provider_id(event.get("requestId"))
            provider_session_id = _safe_provider_id(event.get("sessionId"))
        events.append(
            {
                "sequence": sequence,
                "type": event_type,
                "byte_count": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
                "stop_reason": stop_reason if event_type == "end" else None,
            }
        )
        final = events[-1]

    diagnostics = _summarize_diagnostic_events(diagnostic_events)
    updates = _summarize_session_updates(session_updates)
    diagnostic_tools = diagnostics.pop("tools")
    diagnostics.pop("last_completed_tool")
    update_tools = updates.pop("tools")
    update_by_ordinal = {tool["ordinal"]: tool for tool in update_tools}
    correlation_mismatches = 0
    for tool in diagnostic_tools:
        update = update_by_ordinal.get(tool["ordinal"])
        if update is not None and update["tool_kind"] == tool["tool_kind"]:
            tool.update(update)
        elif update is not None:
            correlation_mismatches += 1
    diagnostics["tool_correlation_mismatch_count"] = correlation_mismatches

    last_tool = diagnostic_tools[-1] if diagnostic_tools else None
    last_completed = next(
        (tool for tool in reversed(diagnostic_tools) if tool.get("result_sequence") is not None),
        None,
    )
    cancellation_category = _cancellation_classification(
        final["stop_reason"] if final else None,
        diagnostics.get("turn_cancellation_category"),
    )
    evidence_completeness, missing_evidence_reason = _evidence_completeness(
        diagnostic_events is not None,
        session_updates is not None,
        diagnostics,
        updates,
    )
    permission_decision = last_tool.get("permission_decision") if last_tool else None
    result_received = bool(last_tool and last_tool.get("result_sequence") is not None)
    cancellation_stage = _cancellation_stage(
        cancellation_category,
        permission_decision,
        result_received,
        final_text_generation_began,
        last_tool is not None,
    )

    return {
        "schema_version": "grok-event-summary.v2",
        "event_count": event_count,
        "retained_event_count": len(events),
        "events_truncated": event_count > len(events),
        "malformed_event_count": malformed_event_count,
        "events": list(events),
        "last_event_type": final["type"] if final else None,
        "last_stop_reason": final["stop_reason"] if final else None,
        "provider_request_id": provider_request_id,
        "provider_session_id": provider_session_id,
        **diagnostics,
        **updates,
        "last_native_tool_request_sequence": (
            last_tool.get("request_sequence") if last_tool else None
        ),
        "last_native_tool_result_sequence": (
            last_completed.get("result_sequence") if last_completed else None
        ),
        "last_permission_event_sequence": (
            last_tool.get("permission_sequence") if last_tool else None
        ),
        "last_tool_kind": last_tool.get("tool_kind") if last_tool else None,
        "last_safe_command_identity": (
            last_tool.get("safe_command_identity") if last_tool else None
        ),
        "last_safe_command_identity_reason": _safe_command_identity_reason(last_tool),
        "last_command_family": last_tool.get("command_family") if last_tool else None,
        "last_command_sha256": last_tool.get("command_sha256") if last_tool else None,
        "last_command_byte_length": (last_tool.get("command_byte_length") if last_tool else None),
        "last_command_structural_flags": (
            last_tool.get("command_structural_flags") if last_tool else None
        ),
        "last_permission_decision": permission_decision,
        "last_tool_result_received": result_received,
        "last_completed_tool_action": _public_tool_summary(last_completed),
        "cancellation_evidence_category": cancellation_category,
        "cancellation_stage": cancellation_stage,
        "evidence_completeness": evidence_completeness,
        "missing_evidence_reason": missing_evidence_reason,
        "final_text_generation_began": final_text_generation_began,
    }


def _summarize_diagnostic_events(
    lines: Iterable[str] | None,
) -> dict[str, Any]:
    retained: deque[dict[str, object]] = deque(maxlen=_SUMMARY_EVENT_LIMIT)
    tools: deque[dict[str, object]] = deque(maxlen=_SUMMARY_TOOL_LIMIT)
    stream_hash = hashlib.sha256()
    total = relevant = malformed = oversized = tool_count = 0
    schema_version: str | None = None
    turn_cancellation_category: str | None = None
    current_tool: dict[str, object] | None = None
    last_completed: dict[str, object] | None = None
    if lines is None:
        return {
            "diagnostic_source": None,
            "diagnostic_schema_version": None,
            "diagnostic_event_count": 0,
            "diagnostic_relevant_event_count": 0,
            "retained_diagnostic_event_count": 0,
            "diagnostic_events_truncated": False,
            "malformed_diagnostic_event_count": 0,
            "oversized_diagnostic_event_count": 0,
            "diagnostic_stream_sha256": None,
            "diagnostic_events": [],
            "diagnostic_tool_count": 0,
            "turn_cancellation_category": None,
            "last_completed_tool": None,
            "tools": tools,
        }

    for sequence, raw_line in enumerate(lines, start=1):
        total += 1
        encoded = raw_line.encode()
        stream_hash.update(encoded)
        if len(encoded) > _SUMMARY_LINE_BYTE_LIMIT:
            oversized += 1
            continue
        try:
            event = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeError):
            malformed += 1
            continue
        if not isinstance(event, dict):
            malformed += 1
            continue
        event_type = _safe_value(event.get("type"), _SAFE_DIAGNOSTIC_TYPES)
        if event_type is None:
            continue
        if schema_version is None:
            schema_version = _safe_value(
                event.get("schema_version"), _SAFE_DIAGNOSTIC_SCHEMA_VERSIONS
            )
        safe_event: dict[str, object] = {
            "sequence": sequence,
            "type": event_type,
            "byte_count": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        }
        timestamp = event.get("ts")
        if isinstance(timestamp, str) and _SAFE_TIMESTAMP.fullmatch(timestamp):
            safe_event["timestamp"] = timestamp
        tool_name = event.get("tool_name")
        if isinstance(tool_name, str):
            safe_event["tool_kind"] = _SAFE_TOOL_KINDS.get(tool_name, "Unknown")
        phase = _safe_value(event.get("phase"), _SAFE_PHASES)
        if phase is not None:
            safe_event["phase"] = phase
        decision = _permission_decision(event.get("decision"))
        if decision is not None:
            safe_event["permission_decision"] = decision
        outcome = _safe_value(event.get("outcome"), {"cancelled", "failed", "success"})
        if outcome is not None:
            safe_event["outcome"] = outcome
        category = _safe_value(event.get("cancellation_category"), _SAFE_CANCELLATION_CATEGORIES)
        if category is not None:
            safe_event["cancellation_category"] = category
            turn_cancellation_category = category
        relevant += 1
        retained.append(safe_event)

        if event_type == "tool_started":
            tool_count += 1
            current_tool = {
                "ordinal": tool_count,
                "request_sequence": sequence,
                "tool_kind": safe_event.get("tool_kind", "Unknown"),
                "permission_sequence": None,
                "permission_decision": "unresolved",
                "result_sequence": None,
            }
            tools.append(current_tool)
        elif event_type == "permission_requested" and current_tool is not None:
            current_tool["permission_sequence"] = sequence
        elif event_type == "permission_resolved" and current_tool is not None:
            current_tool["permission_sequence"] = sequence
            current_tool["permission_decision"] = decision or "unresolved"
        elif event_type == "tool_completed" and current_tool is not None:
            current_tool["result_sequence"] = sequence
            current_tool["result_outcome"] = outcome
            last_completed = dict(current_tool)

    return {
        "diagnostic_source": "grok-session-events.v1",
        "diagnostic_schema_version": schema_version,
        "diagnostic_event_count": total,
        "diagnostic_relevant_event_count": relevant,
        "retained_diagnostic_event_count": len(retained),
        "diagnostic_events_truncated": relevant > len(retained),
        "malformed_diagnostic_event_count": malformed,
        "oversized_diagnostic_event_count": oversized,
        "diagnostic_stream_sha256": stream_hash.hexdigest(),
        "diagnostic_events": list(retained),
        "diagnostic_tool_count": tool_count,
        "turn_cancellation_category": turn_cancellation_category,
        "last_completed_tool": _public_tool_summary(last_completed),
        "tools": tools,
    }


def _summarize_session_updates(lines: Iterable[str] | None) -> dict[str, Any]:
    tools: deque[dict[str, object]] = deque(maxlen=_SUMMARY_TOOL_LIMIT)
    tool_by_id: dict[str, dict[str, object]] = {}
    stream_hash = hashlib.sha256()
    total = malformed = oversized = tool_count = 0
    if lines is None:
        return {
            "session_update_count": 0,
            "malformed_session_update_count": 0,
            "oversized_session_update_count": 0,
            "session_update_stream_sha256": None,
            "session_tool_request_count": 0,
            "session_tool_requests_truncated": False,
            "tools": tools,
        }

    for sequence, raw_line in enumerate(lines, start=1):
        total += 1
        encoded = raw_line.encode()
        stream_hash.update(encoded)
        if len(encoded) > _SUMMARY_LINE_BYTE_LIMIT:
            oversized += 1
            continue
        try:
            event = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeError):
            malformed += 1
            continue
        if not isinstance(event, dict) or event.get("method") != "session/update":
            continue
        params = event.get("params")
        update = params.get("update") if isinstance(params, dict) else None
        if not isinstance(update, dict):
            continue
        update_type = update.get("sessionUpdate")
        call_id = _safe_tool_call_id(update.get("toolCallId"))
        if update_type == "tool_call" and call_id is not None:
            raw_input = update.get("rawInput")
            command = raw_input.get("command") if isinstance(raw_input, dict) else None
            tool_count += 1
            tool = {
                "ordinal": tool_count,
                "session_update_sequence": sequence,
                "tool_call_id": call_id,
                "tool_kind": _update_tool_kind(update),
                "result_received_in_session": False,
                **_safe_command_summary(command),
            }
            if len(tools) == tools.maxlen:
                evicted_id = tools[0].get("tool_call_id")
                if isinstance(evicted_id, str):
                    tool_by_id.pop(evicted_id, None)
            tools.append(tool)
            tool_by_id[call_id] = tool
        elif update_type == "tool_call_update" and call_id in tool_by_id:
            status = update.get("status")
            if status in {"completed", "failed"}:
                tool_by_id[call_id]["session_terminal_status"] = status
                tool_by_id[call_id]["result_received_in_session"] = status == "completed"

    return {
        "session_update_count": total,
        "malformed_session_update_count": malformed,
        "oversized_session_update_count": oversized,
        "session_update_stream_sha256": stream_hash.hexdigest(),
        "session_tool_request_count": tool_count,
        "session_tool_requests_truncated": tool_count > len(tools),
        "tools": tools,
    }


def _safe_command_summary(command: object) -> dict[str, object]:
    empty: dict[str, object] = {
        "safe_command_identity": None,
        "command_family": None,
        "command_sha256": None,
        "command_byte_length": None,
        "command_structural_flags": None,
    }
    if not isinstance(command, str):
        return empty
    encoded = command.encode()
    stripped = command.strip()
    flags = {
        "shell_chain_operators": bool(re.search(r"&&|\|\||;|\n|(?<!\|)\|(?!\|)", command)),
        "command_substitution": "$(" in command or "`" in command,
        "shell_wrapper": bool(re.match(r"^(?:/bin/)?(?:ba|da|k|z)?sh\s+-c(?:\s|$)", stripped)),
        "cd_prefix": bool(re.match(r"^cd(?:\s|$)", stripped)),
        "environment_prefix": bool(re.match(r"^(?:env\s+)?[A-Za-z_][A-Za-z0-9_]*=", stripped)),
        "direct_tool_invocation": bool(
            re.match(r"^(?:uv|pytest|ruff|mypy|python|python3)(?:\s|$)", stripped)
        ),
    }
    family = _command_family(stripped)
    flags["unknown_command_family"] = family == "unknown"
    return {
        "safe_command_identity": _safe_command_identity(stripped, flags),
        "command_family": family,
        "command_sha256": hashlib.sha256(encoded).hexdigest(),
        "command_byte_length": len(encoded),
        "command_structural_flags": flags,
    }


def _safe_command_identity(command: str, flags: dict[str, bool]) -> str | None:
    if any(
        flags[key]
        for key in (
            "shell_chain_operators",
            "command_substitution",
            "shell_wrapper",
            "cd_prefix",
            "environment_prefix",
        )
    ):
        return None
    exact = {
        "./verify.sh",
        "date +%s%3N",
        "make format-check",
        "make lint",
        "make test",
        "make type-check",
    }
    if command in exact:
        return command
    git = re.match(r"^git\s+(add|commit|diff|status)(?:\s|$)", command)
    return f"git {git.group(1)}" if git else None


def _command_family(command: str) -> str:
    match = re.match(r"^(?:/bin/)?([^\s]+)", command)
    if match is None:
        return "unknown"
    first = match.group(1)
    safe = {
        "./verify.sh": "verify",
        "bash": "shell",
        "cd": "cd",
        "dash": "shell",
        "date": "date",
        "env": "environment",
        "git": "git",
        "ksh": "shell",
        "make": "make",
        "mypy": "mypy",
        "pytest": "pytest",
        "python": "python",
        "python3": "python",
        "ruff": "ruff",
        "sh": "shell",
        "uv": "uv",
        "zsh": "shell",
    }
    return safe.get(first, "unknown")


def _update_tool_kind(update: dict[str, object]) -> str:
    metadata = update.get("_meta")
    tool = metadata.get("x.ai/tool") if isinstance(metadata, dict) else None
    name = tool.get("name") if isinstance(tool, dict) else None
    return _SAFE_TOOL_KINDS.get(name, "Unknown") if isinstance(name, str) else "Unknown"


def _permission_decision(value: object) -> str | None:
    if value in {"allow", "allowed"}:
        return "allowed"
    if value in {"deny", "denied", "reject", "rejected"}:
        return "denied"
    if value in {"cancel", "cancelled"}:
        return "cancelled"
    return None


def _cancellation_classification(stop_reason: object, diagnostic_category: object) -> str | None:
    if diagnostic_category == "permission_cancelled":
        return "PROVIDER_PERMISSION_CANCELLED"
    if diagnostic_category == "invalid_tool":
        return "INVALID_TOOL_INVOCATION"
    if diagnostic_category in {"cancelled", "mid_turn_abort", "spawn_failed", "timeout"}:
        return "PROVIDER_INTERNAL_CANCELLED"
    if stop_reason == "Cancelled":
        return "PROVIDER_CANCELLED_UNATTRIBUTED"
    return None


def _cancellation_stage(
    classification: str | None,
    permission_decision: object,
    result_received: bool,
    final_text_generation_began: bool,
    tool_requested: bool,
) -> str | None:
    if classification is None:
        return None
    if result_received:
        return "after_tool_result"
    if permission_decision == "cancelled":
        return "after_permission_cancellation_before_execution"
    if permission_decision == "denied":
        return "after_permission_denial_before_execution"
    if permission_decision == "allowed":
        return "during_tool_execution"
    if final_text_generation_began:
        return "during_final_response_generation"
    if tool_requested:
        return "before_permission_resolution"
    return "before_native_tool_request"


def _evidence_completeness(
    diagnostics_available: bool,
    updates_available: bool,
    diagnostics: dict[str, Any],
    updates: dict[str, Any],
) -> tuple[str, str | None]:
    if not diagnostics_available:
        return "unavailable", "Grok session diagnostic events were not retained"
    reasons: list[str] = []
    if diagnostics["malformed_diagnostic_event_count"]:
        reasons.append("malformed diagnostic events")
    if diagnostics["oversized_diagnostic_event_count"]:
        reasons.append("oversized diagnostic events")
    if diagnostics["diagnostic_schema_version"] is None:
        reasons.append("diagnostic schema version missing")
    if diagnostics["diagnostic_tool_count"] and not updates_available:
        reasons.append("ACP session updates were not retained")
    if updates_available and updates["malformed_session_update_count"]:
        reasons.append("malformed ACP session updates")
    if updates_available and updates["oversized_session_update_count"]:
        reasons.append("oversized ACP session updates")
    if (
        updates_available
        and diagnostics["diagnostic_tool_count"] != updates["session_tool_request_count"]
    ):
        reasons.append("tool lifecycle and ACP request counts differ")
    if diagnostics["tool_correlation_mismatch_count"]:
        reasons.append("tool lifecycle and ACP request kinds differ")
    return ("partial", "; ".join(reasons)) if reasons else ("complete", None)


def _public_tool_summary(tool: dict[str, object] | None) -> dict[str, object] | None:
    if tool is None:
        return None
    allowed = {
        "ordinal",
        "request_sequence",
        "result_sequence",
        "tool_kind",
        "safe_command_identity",
        "command_family",
        "command_sha256",
        "command_byte_length",
        "command_structural_flags",
    }
    return {key: value for key, value in tool.items() if key in allowed}


def _safe_command_identity_reason(tool: dict[str, object] | None) -> str:
    if tool is None:
        return "no native tool request was retained"
    if tool.get("tool_kind") != "Bash":
        return "the last native tool request was not a shell command"
    if tool.get("command_sha256") is None:
        return "ACP session updates did not retain the command input"
    if tool.get("safe_command_identity") is None:
        return "the command is outside the finite recognized safe vocabulary"
    return "recognized safe command identity retained"


def _safe_value(value: object, allowed: set[str]) -> str | None:
    return value if isinstance(value, str) and value in allowed else None


def _safe_provider_id(value: object) -> str | None:
    if not isinstance(value, str) or not _SAFE_PROVIDER_ID.fullmatch(value):
        return None
    try:
        return value if str(uuid.UUID(value)) == value else None
    except ValueError:
        return None


def _safe_tool_call_id(value: object) -> str | None:
    if not isinstance(value, str) or not _SAFE_PROVIDER_ID.fullmatch(value):
        return None
    return value if re.fullmatch(r"call-[0-9a-f-]{36}-\d+", value) else None


def _usage_int(usage: object, key: str) -> int | None:
    if not isinstance(usage, dict):
        return None
    value = usage.get(key)
    return int(value) if isinstance(value, int | float) and value >= 0 else None


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
