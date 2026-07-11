"""Deterministic authorization and untrusted attachment validation."""

from pathlib import PurePath

from vuzol.config import Settings
from vuzol.telegram.domain import MessageUpdate, TelegramAttachment

ARCHIVE_SUFFIXES = (".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar")


class TelegramPolicyError(ValueError):
    pass


def authorize(settings: Settings, *, chat_id: int, user_id: int) -> None:
    if chat_id not in settings.allowed_chat_ids:
        raise TelegramPolicyError("chat is not allowlisted")
    if user_id not in settings.allowed_user_ids:
        raise TelegramPolicyError("user is not allowlisted")


def validate_message(settings: Settings, update: MessageUpdate) -> None:
    telegram = settings.telegram
    if update.text is not None and len(update.text) > telegram.max_text_chars:
        raise TelegramPolicyError("message text exceeds configured limit")
    if len(update.attachments) > telegram.max_attachments:
        raise TelegramPolicyError("attachment count exceeds configured limit")
    if update.text is None and not update.attachments:
        raise TelegramPolicyError("message has no supported content")
    for attachment in update.attachments:
        validate_attachment(settings, attachment)


def validate_attachment(settings: Settings, attachment: TelegramAttachment) -> None:
    if attachment.file_size > settings.telegram.max_attachment_bytes:
        raise TelegramPolicyError("attachment size exceeds configured limit")
    if attachment.media_type not in settings.telegram.allowed_media_types:
        raise TelegramPolicyError("attachment media type is not allowed")
    if attachment.filename is not None:
        filename = attachment.filename
        if PurePath(filename).name != filename or "\\" in filename:
            raise TelegramPolicyError("attachment filename is unsafe")
        if filename.lower().endswith(ARCHIVE_SUFFIXES):
            raise TelegramPolicyError("archive attachments are not accepted during intake")
