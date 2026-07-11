"""Provider-neutral Telegram ingress and control envelopes."""

import uuid
from enum import StrEnum

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


class ControlUpdate(TelegramModel):
    bot_id: str = Field(min_length=1, max_length=100)
    update_id: int
    callback_query_id: str = Field(min_length=1, max_length=255)
    chat_id: int
    user_id: int
    action_kind: str = Field(pattern=r"^(approve|reject|pause|resume|cancel|retry)$")
    task_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    approval_id: uuid.UUID | None = None


class IngressStatus(StrEnum):
    CREATED = "created"
    CONTINUATION = "continuation"
    NEEDS_CLARIFICATION = "needs_clarification"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class IngressResult(TelegramModel):
    status: IngressStatus
    task_id: uuid.UUID | None = None
    intake_id: uuid.UUID | None = None
    action_id: uuid.UUID | None = None
    reason: str | None = None
