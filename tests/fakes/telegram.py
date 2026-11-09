"""In-memory test adapters."""

import uuid
from dataclasses import dataclass, field


@dataclass(slots=True)
class FakeTelegramClient:
    fail: Exception | None = None
    next_message_id: int = 1
    documents: list[tuple[int, int | None, bytes, str, str]] = field(
        default_factory=list, init=False, repr=False
    )
    sent: list[tuple[int, int | None, str]] = field(default_factory=list, init=False)
    edited: list[tuple[int, int, str]] = field(default_factory=list, init=False)
    deleted: list[tuple[int, int]] = field(default_factory=list, init=False)
    pinned: list[tuple[int, int]] = field(default_factory=list, init=False)
    sent_silent: list[bool] = field(default_factory=list, init=False)
    rich_sent_silent: list[bool] = field(default_factory=list, init=False)
    rich_sent: list[tuple[int, int | None, str]] = field(default_factory=list, init=False)
    rich_edited: list[tuple[int, int, str]] = field(default_factory=list, init=False)
    sent_keyboards: list[tuple[tuple[tuple[str, str], ...], ...]] = field(
        default_factory=list, init=False
    )
    edited_keyboards: list[tuple[tuple[tuple[str, str], ...], ...]] = field(
        default_factory=list, init=False
    )

    async def send_document(
        self, *, chat_id: int, thread_id: int | None, content: bytes, filename: str, caption: str
    ) -> int:
        if self.fail:
            raise self.fail
        self.documents.append((chat_id, thread_id, content, filename, caption))
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    async def send_rich_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        disable_notification: bool = False,
    ) -> int:
        if self.fail:
            raise self.fail
        self.rich_sent.append((chat_id, thread_id, html))
        self.rich_sent_silent.append(disable_notification)
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    async def edit_rich_message(self, *, chat_id: int, message_id: int, html: str) -> None:
        if self.fail:
            raise self.fail
        self.rich_edited.append((chat_id, message_id, html))

    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
        disable_notification: bool = False,
    ) -> int:
        del buttons, task_id, approval_id
        if self.fail:
            raise self.fail
        self.sent.append((chat_id, thread_id, html))
        self.sent_silent.append(disable_notification)
        self.sent_keyboards.append(callback_buttons)
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None:
        del buttons, task_id, approval_id
        if self.fail:
            raise self.fail
        self.edited.append((chat_id, message_id, html))
        self.edited_keyboards.append(callback_buttons)

    async def edit_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...],
    ) -> None:
        if self.fail:
            raise self.fail
        self.edited_keyboards.append(callback_buttons)

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        if self.fail:
            raise self.fail
        self.deleted.append((chat_id, message_id))

    async def pin_message(self, *, chat_id: int, message_id: int) -> bool:
        self.pinned.append((chat_id, message_id))
        return True
