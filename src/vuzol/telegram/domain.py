"""Provider-neutral Telegram ingress and control envelopes."""

import uuid
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TelegramModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AttachmentKind(StrEnum):
    VOICE = "voice"
    AUDIO = "audio"
    DOCUMENT = "document"
    PHOTO = "photo"


class TelegramAttachment(TelegramModel):
    file_id: str = Field(min_length=1, max_length=500)
    file_unique_id: str = Field(min_length=1, max_length=500)
    kind: AttachmentKind
    file_size: int = Field(ge=0)
    media_type: str = Field(min_length=1, max_length=200)
    filename: str | None = Field(default=None, max_length=255)


class MessageUpdate(TelegramModel):
    bot_id: str = Field(min_length=1, max_length=100)
    update_id: int
    chat_id: int
    message_thread_id: int = Field(ge=1)
    message_id: int = Field(ge=1)
    user_id: int
    text: str | None = None
    reply_to_message_id: int | None = None
    attachments: tuple[TelegramAttachment, ...] = ()
    media_group_id: str | None = Field(default=None, min_length=1, max_length=100)


class ControlUpdate(TelegramModel):
    bot_id: str = Field(min_length=1, max_length=100)
    update_id: int
    callback_query_id: str = Field(min_length=1, max_length=255)
    chat_id: int
    user_id: int
    message_thread_id: int | None = Field(default=None, ge=1)
    message_id: int | None = Field(default=None, ge=1)
    action_kind: str = Field(
        pattern=(
            r"^(approve|redo|reject|start|pause|resume|cancel|retry|"
            r"admit|uncertain|lead_answer|lead_resume|lead_cancel|lead_accept|lead_rework|lead_fallback|memory_undo|think_handoff|"
            r"project_name_select|project_name_regenerate|"
            r"project_model_select_auto|project_model_select_connection|"
            r"project_model_matrix_menu|project_model_matrix_slot|project_model_matrix_pick|"
            r"project_model_select_worker|project_model_select_effort|secret_cancel|chat_retry)$"
        )
    )
    task_id: uuid.UUID | None = None
    assistant_turn_id: uuid.UUID | None = None
    note_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    expected_task_version: int | None = Field(default=None, ge=1)
    retry_step_ordinal: int | None = Field(default=None, ge=0)
    approval_id: uuid.UUID | None = None
    naming_request_id: uuid.UUID | None = None
    naming_revision: int | None = Field(default=None, ge=1)
    naming_option_index: int | None = Field(default=None, ge=0, le=8)
    preference_revision: int | None = Field(default=None, ge=1)
    preference_worker: str | None = Field(default=None, pattern=r"^(sol|terra|luna|grok|kimi)$")
    preference_profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,99}$")
    preference_slot: str | None = Field(
        default=None,
        pattern=r"^(think|lead|goal_reviewer|planner_light|planner_strong|reviewer_light|reviewer_strong|executor)$",
    )
    preference_profile_token: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    preference_effort: str | None = Field(
        default=None, pattern=r"^(low|medium|high|xhigh|max|ultra)$"
    )
    secret_request_id: uuid.UUID | None = None
    goal_version: int | None = Field(default=None, ge=1)
    question_version: int | None = Field(default=None, ge=1)
    answer_option: int | None = Field(default=None, ge=0, le=5)


class WorkPackageControlUpdate(TelegramModel):
    bot_id: str = Field(min_length=1, max_length=100)
    update_id: int
    callback_query_id: str = Field(min_length=1, max_length=255)
    callback_data: str = Field(min_length=1, max_length=64)
    chat_id: int
    message_id: int = Field(ge=1)
    user_id: int
    message_thread_id: int | None = Field(default=None, ge=1)


class IngressStatus(StrEnum):
    CREATED = "created"
    CONTINUATION = "continuation"
    NEEDS_CLARIFICATION = "needs_clarification"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    HANDLED = "handled"


class IngressResult(TelegramModel):
    status: IngressStatus
    task_id: uuid.UUID | None = None
    intake_id: uuid.UUID | None = None
    action_id: uuid.UUID | None = None
    reason: str | None = None
    # D1 L5: prior command outcome, returned on duplicate delivery so the
    # caller gets the same receipt instead of "unknown".
    outcome: dict[str, Any] | None = None
