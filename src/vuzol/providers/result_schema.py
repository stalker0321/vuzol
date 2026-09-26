"""Deterministic output schema selection shared by routing and handlers."""

from __future__ import annotations

from typing import Any


def result_schema_for_step(
    step_type: str, task_draft: dict[str, object]
) -> tuple[str | None, str | None, dict[str, object] | None]:
    if step_type == "execute_agent" and task_draft.get("discussion_agent_contract"):
        from vuzol.discussion.agent import DiscussionAgentReply

        return (
            "DiscussionAgentReply",
            "discussion-agent-reply.v1",
            DiscussionAgentReply.model_json_schema(),
        )
    if step_type != "execute_code" or "step09a_capsule" not in task_draft:
        return None, None, None
    from vuzol.experiments.domain import WorkerEditReport

    return (
        "WorkerEditReport",
        "step09a-worker-edit-report.v1",
        WorkerEditReport.model_json_schema(),
    )


def schema_char_count(schema: dict[str, Any] | None) -> int:
    if not schema:
        return 0
    import json

    return len(json.dumps(schema, ensure_ascii=False, sort_keys=True))
