import pytest

from vuzol.config import Settings
from vuzol.telegram.domain import AttachmentKind, MessageUpdate, TelegramAttachment
from vuzol.telegram.policy import TelegramPolicyError, validate_message


def attachment(**changes: object) -> TelegramAttachment:
    values: dict[str, object] = {
        "file_id": "file",
        "file_unique_id": "unique",
        "kind": AttachmentKind.DOCUMENT,
        "file_size": 10,
        "media_type": "text/plain",
        "filename": "notes.txt",
    }
    values.update(changes)
    return TelegramAttachment.model_validate(values)


def update(*attachments: TelegramAttachment, text: str | None = None) -> MessageUpdate:
    return MessageUpdate(
        bot_id="main",
        update_id=1,
        chat_id=-100,
        message_thread_id=10,
        message_id=1,
        user_id=42,
        text=text,
        attachments=attachments,
    )


@pytest.mark.parametrize(
    "unsafe",
    [
        attachment(file_size=30_000_000),
        attachment(media_type="application/zip", filename="payload.zip"),
        attachment(filename="../escape.txt"),
        attachment(filename="folder\\escape.txt"),
    ],
)
def test_untrusted_attachments_are_rejected(unsafe: TelegramAttachment) -> None:
    with pytest.raises(TelegramPolicyError):
        validate_message(Settings(environment="test"), update(unsafe))


def test_text_and_attachment_count_limits_are_enforced() -> None:
    settings = Settings.model_validate(
        {"environment": "test", "telegram": {"max_text_chars": 3, "max_attachments": 1}}
    )
    with pytest.raises(TelegramPolicyError, match="text exceeds"):
        validate_message(settings, update(text="long"))
    with pytest.raises(TelegramPolicyError, match="count exceeds"):
        validate_message(settings, update(attachment(), attachment()))


@pytest.mark.parametrize(
    "name,mime",
    [
        ("a.md", "text/markdown"),
        ("a.md", "application/octet-stream"),
        ("a.docx", "application/zip"),
        ("a.csv", "text/csv"),
        ("a.json", "application/json"),
    ],
)
def test_document_formats_only_extend_scoped_chat_policy(name: str, mime: str) -> None:
    settings = Settings(environment="test")
    source = update(attachment(filename=name, media_type=mime))
    validate_message(settings, source, chat_documents=True)
    with pytest.raises(TelegramPolicyError):
        validate_message(settings, source)


def test_chat_documents_do_not_relax_path_archive_or_size_guards() -> None:
    settings = Settings(environment="test")
    for file in (
        attachment(filename="../a.md", media_type="text/markdown"),
        attachment(filename="a.zip", media_type="application/octet-stream"),
        attachment(filename="a.docx", media_type="application/zip", file_size=30_000_000),
    ):
        with pytest.raises(TelegramPolicyError):
            validate_message(settings, update(file), chat_documents=True)
