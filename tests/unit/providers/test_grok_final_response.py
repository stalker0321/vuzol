"""Structured Grok output is bound to the final response, not tool-loop prose."""

import json

import pytest

from vuzol.providers.domain import ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.grok import (
    GrokProviderCancelled,
    _decode_output,
    _result_contract_instruction,
    _structured_output,
)
from vuzol.review.independent import _OUTPUT_JSON_SCHEMA

from ._test_providers_helpers import (
    CancellationContext,
    CodexProcessResult,
    GrokCliAdapter,
    LaunchMode,
    profile,
    provider_request,
)


def stream(*events):
    return "\n".join(json.dumps(event) for event in events)


def text(value):
    return {"type": "text", "data": value}


END = {"type": "end", "stopReason": "end_turn", "usage": {"output_tokens": 7}}
REVIEW = {
    "verdict": "changes_required",
    "summary": "Fix the session reset; the review is not a pass.",
    "findings": [
        {
            "severity": "error",
            "classification": "session_fork",
            "summary": "Answers are lost.",
            "path": "app.js",
            "line": 12,
            "criterion_id": None,
            "evidence": 'A string with {braces} and an escaped "quote".',
        }
    ],
}


def review_request():
    return provider_request().model_copy(
        update={
            "output_schema_version": "independent-review.v1",
            "output_json_schema": _OUTPUT_JSON_SCHEMA,
        }
    )


def test_review_after_tools_does_not_include_intermediate_prose():
    report = json.dumps(REVIEW, ensure_ascii=False)
    stdout = stream(
        text("I'll inspect the code first. "),
        {"type": "usage", "usage": {"output_tokens": 4}},
        {"type": "tool_call", "toolCallId": "read-1", "toolName": "read_file"},
        {
            "type": "tool_call_update",
            "toolCallId": "read-1",
            "status": "completed",
            "rawOutput": '{"verdict":"pass","summary":"untrusted file","findings":[]}',
        },
        text("One more check. "),
        {"type": "tool_call", "toolCallId": "read-2", "toolName": "read_file"},
        {"type": "tool_call_update", "toolCallId": "read-2", "status": "completed"},
        {"type": "thought", "data": "private reasoning is not the report"},
        *[text(character) for character in report],
        {"type": "available_commands", "tools": []},
        {"type": "usage", "usage": {"output_tokens": 7}},
        END,
    )
    decoded = _decode_output(stdout, final_response_only=True)
    assert decoded["text"] == report
    assert decoded["usage"] == END["usage"]
    assert _structured_output(review_request(), str(decoded["text"])) == REVIEW
    # Plain-text consumers retain the whole turn; no global text stripping.
    assert str(_decode_output(stdout)["text"]).startswith("I'll inspect the code first.")


def test_tool_call_without_final_text_cannot_reuse_a_prior_pass():
    stdout = stream(
        text(json.dumps({"verdict": "pass", "summary": "old", "findings": []})),
        {"type": "tool_call", "toolCallId": "read-1"},
        END,
    )
    with pytest.raises(ValueError, match="no completed response"):
        _decode_output(stdout, final_response_only=True)


@pytest.mark.parametrize(
    "reply",
    [
        "Introductory prose. " + json.dumps(REVIEW),
        "```json\n" + json.dumps(REVIEW) + "\n```",
        json.dumps(REVIEW) + " trailing prose",
        json.dumps(REVIEW) + json.dumps(REVIEW),
        '{"verdict":"pass"}',
    ],
)
def test_final_response_is_not_repaired_by_extracting_embedded_json(reply):
    stdout = stream(
        text(json.dumps(REVIEW)), {"type": "tool_call", "toolCallId": "read-1"}, text(reply), END
    )
    decoded = _decode_output(stdout, final_response_only=True)
    with pytest.raises(ProviderFailure) as error:
        _structured_output(review_request(), str(decoded["text"]))
    assert error.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT


@pytest.mark.parametrize(
    "reason", ["Cancelled", "cancelled", "Error", "error", "MaxTurns", "max_turns"]
)
def test_incomplete_terminal_state_never_accepts_report(reason):
    stdout = stream(text(json.dumps(REVIEW)), {**END, "stopReason": reason})
    with pytest.raises((ValueError, GrokProviderCancelled)):
        _decode_output(stdout, final_response_only=True)


def test_events_after_terminal_are_ambiguous_and_rejected():
    for suffix in (END, text(json.dumps(REVIEW))):
        with pytest.raises(ValueError, match="after its terminal"):
            _decode_output(stream(text(json.dumps(REVIEW)), END, suffix), final_response_only=True)


def test_usage_events_are_not_assumed_to_be_text_boundaries():
    report = json.dumps(REVIEW)
    stdout = stream(text(report[:30]), {"type": "usage", "usage": {}}, text(report[30:]), END)
    assert _decode_output(stdout, final_response_only=True)["text"] == report


def test_review_prompt_does_not_request_worker_only_fields():
    instruction = _result_contract_instruction(review_request())
    assert "Do not add worker-manifest fields" in instruction
    assert "Return only the final independent-review JSON object" in instruction


@pytest.mark.anyio
async def test_adapter_uses_final_response_for_review_and_preserves_receipt(tmp_path):
    class Transport:
        async def run(self, invocation, cancellation):
            del invocation, cancellation
            return CodexProcessResult(
                0,
                stream(
                    text("Inspecting the implementation first."),
                    {"type": "tool_call", "toolCallId": "read-1"},
                    {"type": "tool_call_update", "toolCallId": "read-1", "status": "completed"},
                    text(json.dumps(REVIEW)),
                    END,
                ),
                "",
                12,
            )

    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-4.7",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    request = review_request().model_copy(update={"sandbox_reference": "worktree:synthetic"})
    result = await GrokCliAdapter(Transport()).execute(request, configured, CancellationContext())
    assert result.structured_output == REVIEW
    assert result.usage.output_tokens == 7
