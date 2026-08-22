"""Provider-agnostic, bounded forensic diagnostics for supervised CLI runs."""

import hashlib
import json
import re
from collections.abc import Sequence

_MAX_EVENT_BYTES = 262_144
_MAX_RETAINED_EVENTS = 128
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,255}$")
_FAILURE_EVENT_TYPES = frozenset({"error", "failed", "turn.failed", "turn.cancelled"})
_FAILURE_STOP_REASONS = frozenset({"Cancelled", "Error", "MaxTurns"})


def summarize_cli_process(
    *, provider: str, argv: Sequence[str], stdout: str, stderr: str, exit_code: int
) -> dict[str, object]:
    events: list[dict[str, object]] = []
    malformed = 0
    failure_signals: list[str] = []
    for sequence, raw_line in enumerate(stdout.splitlines(), start=1):
        encoded = raw_line.encode()
        if len(encoded) > _MAX_EVENT_BYTES:
            malformed += 1
            continue
        try:
            event = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeError):
            malformed += 1
            continue
        if not isinstance(event, dict):
            malformed += 1
            continue
        record = _safe_event_record(sequence, event)
        if len(events) == _MAX_RETAINED_EVENTS:
            events.pop(0)
        events.append(record)
        signal = _failure_signal(event)
        if signal is not None and signal not in failure_signals:
            failure_signals.append(signal)
    if exit_code != 0:
        failure_signals.insert(0, f"process_exit:{exit_code}")
    if stderr.strip() and "stderr_nonempty" not in failure_signals:
        failure_signals.append("stderr_nonempty")
    if stdout.strip() and malformed and not events:
        failure_signals.append("stdout_non_json")
    last_event = events[-1] if events else None
    last_tool = next(
        (event for event in reversed(events) if event.get("provider_tool_name") is not None),
        None,
    )
    return {
        "schema_version": "cli-forensic-diagnostics.v1",
        "provider": provider,
        "argv": _safe_argv(argv),
        "exit_code": exit_code,
        "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
        "stdout_byte_length": len(stdout.encode()),
        "stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
        "stderr_byte_length": len(stderr.encode()),
        "event_count": len(events),
        "malformed_event_count": malformed,
        "failure_signals": failure_signals,
        "events": events,
        "last_event_type": last_event.get("event_type") if last_event else None,
        "last_provider_tool_name": (last_tool.get("provider_tool_name") if last_tool else None),
        "last_tool_call_id": last_tool.get("tool_call_id") if last_tool else None,
        "last_tool_result_received": (last_tool.get("result_received") if last_tool else None),
        "last_stop_reason": (last_event.get("stopReason") if last_event else None),
    }


def build_cli_forensic_diagnostics(
    *,
    provider: str,
    argv: Sequence[str],
    stdout: str,
    stderr: str,
    exit_code: int,
    provider_details: dict[str, object] | None = None,
) -> bytes:
    summary = summarize_cli_process(
        provider=provider,
        argv=argv,
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
    )
    if provider_details is not None:
        summary["provider_details"] = provider_details
    return (
        json.dumps(summary, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def should_capture_cli_forensics(summary: dict[str, object]) -> bool:
    signals = summary.get("failure_signals")
    return isinstance(signals, list) and bool(signals)


def _safe_event_record(sequence: int, event: dict[str, object]) -> dict[str, object]:
    event_type = event.get("type")
    if not isinstance(event_type, str):
        role = event.get("role")
        event_type = f"role:{role}" if isinstance(role, str) else "Unknown"
    record: dict[str, object] = {
        "sequence": sequence,
        "event_type": _safe_text(event_type, fallback="Unknown"),
    }
    for key in ("phase", "status", "outcome", "stopReason", "cancellation_category"):
        value = event.get(key)
        if isinstance(value, str) and len(value) <= 128:
            record[key] = value
    tool_name = _extract_tool_name(event)
    if tool_name is not None:
        record["provider_tool_name"] = tool_name
    call_id = _extract_tool_call_id(event)
    if call_id is not None:
        record["tool_call_id"] = call_id
    result_received = _result_received(event)
    if result_received is not None:
        record["result_received"] = result_received
    return record


def _extract_tool_name(event: dict[str, object]) -> str | None:
    candidates: list[object] = [event.get("tool_name"), event.get("tool")]
    item = event.get("item")
    if isinstance(item, dict):
        candidates.extend((item.get("name"), item.get("tool_name")))
    tool_calls = event.get("tool_calls")
    if isinstance(tool_calls, list):
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            candidates.extend((tool_call.get("name"), tool_call.get("tool_name")))
            function = tool_call.get("function")
            if isinstance(function, dict):
                candidates.append(function.get("name"))
    for candidate in candidates:
        if isinstance(candidate, str) and _SAFE_NAME.fullmatch(candidate):
            return candidate
    metadata = event.get("_meta")
    if isinstance(metadata, dict):
        tool = metadata.get("x.ai/tool")
        if isinstance(tool, dict):
            candidate = tool.get("name")
            if isinstance(candidate, str) and _SAFE_NAME.fullmatch(candidate):
                return candidate
    return None


def _extract_tool_call_id(event: dict[str, object]) -> str | None:
    for key in ("toolCallId", "tool_call_id", "call_id"):
        value = event.get(key)
        if isinstance(value, str) and _SAFE_ID.fullmatch(value):
            return value
    item = event.get("item")
    if isinstance(item, dict):
        value = item.get("id")
        if isinstance(value, str) and _SAFE_ID.fullmatch(value):
            return value
    return None


def _result_received(event: dict[str, object]) -> bool | None:
    status = event.get("status")
    if status in {"completed", "succeeded", "failed"}:
        return True
    event_type = event.get("type")
    if isinstance(event_type, str) and (
        event_type.endswith(".completed") or event_type.endswith(".failed")
    ):
        return True
    return None


def _failure_signal(event: dict[str, object]) -> str | None:
    event_type = event.get("type")
    if isinstance(event_type, str) and event_type in _FAILURE_EVENT_TYPES:
        return f"event:{event_type}"
    if event_type == "turn_ended":
        outcome = event.get("outcome")
        if outcome not in {"failed", "cancelled"}:
            return None
        return f"event:{event_type}"
    stop_reason = event.get("stopReason")
    if stop_reason in _FAILURE_STOP_REASONS:
        return f"stop_reason:{stop_reason}"
    outcome = event.get("outcome")
    if outcome in {"failed", "cancelled"}:
        return f"outcome:{outcome}"
    return None


def _safe_argv(argv: Sequence[str]) -> list[str]:
    result: list[str] = []
    for value in argv[:32]:
        if len(value) <= 255 and "\x00" not in value:
            result.append(value)
        else:
            result.append(f"sha256:{hashlib.sha256(value.encode()).hexdigest()}")
    return result


def _safe_text(value: object, *, fallback: str) -> str:
    return value if isinstance(value, str) and len(value) <= 128 else fallback
