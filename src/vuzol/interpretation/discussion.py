"""Discussion queue names and the owner's explicit control overrides."""

from __future__ import annotations

from enum import StrEnum

DISCUSSION_CLASSIFY_DESTINATION = "discussion_classify"
DISCUSSION_REPLY_DESTINATION = "discussion_reply"
DISCUSSION_THINKING_ROLE = "discussion_thinking"


class ControlOverrideKind(StrEnum):
    CONTINUE_DISCUSSION = "continue_discussion"
    CREATE_OR_UPDATE_PLAN = "create_or_update_plan"
    REPLAN = "replan"
    EXPLICIT_TASK = "explicit_task"
