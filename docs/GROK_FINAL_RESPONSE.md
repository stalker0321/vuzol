# Grok structured response boundaries

The pinned CLI's streaming-json includes text deltas from intermediate model
responses as well as the final answer. The observed review stream was:

    commentary -> usage -> tool calls/results -> ... -> final JSON -> usage -> end

Concatenating every text delta produced prose followed by a correct JSON report.
The strict independent-review validator appropriately rejected that combined
string. Authentication and the provider request had succeeded.

Structured consumers now use only the response after the last native tool_call.
Tool output, private thoughts, availability updates and usage events are never
parsed as a candidate report. Usage events alone are not assumed to mark a text
boundary. Plain-text consumers retain the whole turn. No tool call means the
entire text remains subject to strict validation; introductory/fenced/trailing
prose in that final response is not stripped or heuristically repaired.

An old PASS before a tool call cannot be reused when the final answer is absent
or invalid. Cancellation, error/max-turn termination and events after the end
marker fail closed. The requested output schema and independent verdict rules
remain unchanged: changes_required and every finding stay changes_required.
Whole-session accounting is read independently from the terminal usage receipt.

The review prompt no longer inherits the worker-only unavailable_reason
instruction, which contradicted the review schema's additionalProperties=false.

Regression coverage is in tests/unit/providers/test_grok_final_response.py,
including end-to-end adapter dispatch, token-by-token JSON, braces in strings,
tool-result injection, stale PASS, malformed final reports, terminal failures,
unchanged plain-text behavior and measured receipt preservation. A retained
production review stream was also decoded locally: the complete final report
validated with its original changes_required verdict and all four findings.
That diagnostic neither reapplied a verdict nor made another provider call.

Retry/recovery must use normal owner/version-bound controls. Project code must
be repaired through the bounded workflow so the result commit is revalidated
and independently reviewed; never edit a sealed worktree and reuse stale gates.
