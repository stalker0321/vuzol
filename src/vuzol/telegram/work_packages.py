"""Strict Telegram ``wp-cb-v1`` callback values.

The wire intentionally contains only public revision fences.  The package status
generation is recovered from the durable message link for the card that emitted
the callback; it must never be inferred from current package state.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

MAX_CALLBACK_BYTES = 64
_PKG32 = re.compile(r"^[0-9a-f]{32}$")
_H8 = re.compile(r"^[0-9a-f]{8}$")
_DECIMAL = re.compile(r"^[1-9][0-9]{0,9}$")
_SMALL_DECIMAL = re.compile(r"^[1-9][0-9]{0,2}$")


class WorkPackageCallbackError(ValueError):
    """A stable fail-closed callback parse/encoding rejection."""


class ContinueDiscussionOverrides:
    """Process-local, one-turn F1 overrides; restart safely clears every entry."""

    def __init__(self, *, ttl: timedelta = timedelta(minutes=20)) -> None:
        self._ttl = ttl
        self._entries: dict[tuple[int, int, int], datetime] = {}
        self._lock = asyncio.Lock()

    async def arm(self, *, chat_id: int, thread_id: int, user_id: int) -> None:
        async with self._lock:
            self._entries[(chat_id, thread_id, user_id)] = datetime.now(UTC) + self._ttl

    async def consume(self, *, chat_id: int, thread_id: int, user_id: int) -> bool:
        key = (chat_id, thread_id, user_id)
        async with self._lock:
            expires_at = self._entries.pop(key, None)
        return expires_at is not None and expires_at > datetime.now(UTC)


class WorkPackageCallbackKind(StrEnum):
    APPROVE = "A"
    START = "S"
    DISCARD = "D"
    RETRY_ITEM = "R"
    SKIP_ITEM = "N"
    STOP_PACKAGE = "X"
    REQUEST_REPLAN = "L"
    CLOSE_DETAIL = "C"
    CONTINUE_DISCUSSION = "U"
    OPEN_ITEM = "I"
    OPEN_EDIT = "E"
    SET_PAGE = "G"


_VALUE_KINDS = frozenset(
    {
        WorkPackageCallbackKind.OPEN_ITEM,
        WorkPackageCallbackKind.OPEN_EDIT,
        WorkPackageCallbackKind.SET_PAGE,
    }
)


@dataclass(frozen=True, slots=True)
class WorkPackageCallback:
    kind: WorkPackageCallbackKind
    package_id: uuid.UUID
    revision_number: int
    h8: str
    value: int | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.revision_number <= 9_999_999_999:
            raise WorkPackageCallbackError("revision_out_of_range")
        if _H8.fullmatch(self.h8) is None:
            raise WorkPackageCallbackError("invalid_h8")
        if self.kind in _VALUE_KINDS:
            if self.value is None or not 1 <= self.value <= 999:
                raise WorkPackageCallbackError("value_out_of_range")
        elif self.value is not None:
            raise WorkPackageCallbackError("unexpected_value")

    @property
    def ordinal(self) -> int | None:
        if self.kind in {
            WorkPackageCallbackKind.OPEN_ITEM,
            WorkPackageCallbackKind.OPEN_EDIT,
        }:
            return self.value
        return None

    @property
    def page(self) -> int | None:
        return self.value if self.kind is WorkPackageCallbackKind.SET_PAGE else None


def encode_work_package_callback(callback: WorkPackageCallback) -> str:
    parts = [
        "v1",
        "wp",
        callback.kind.value,
        callback.package_id.hex,
        str(callback.revision_number),
    ]
    if callback.value is not None:
        parts.append(str(callback.value))
    parts.append(callback.h8)
    encoded = ":".join(parts)
    if len(encoded.encode("utf-8")) > MAX_CALLBACK_BYTES:
        raise WorkPackageCallbackError("callback_too_long")
    return encoded


def parse_work_package_callback(value: str) -> WorkPackageCallback:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > MAX_CALLBACK_BYTES:
        raise WorkPackageCallbackError("invalid_length")
    parts = value.split(":")
    if len(parts) not in {6, 7} or parts[:2] != ["v1", "wp"]:
        raise WorkPackageCallbackError("invalid_shape")
    try:
        kind = WorkPackageCallbackKind(parts[2])
    except ValueError as error:
        raise WorkPackageCallbackError("unknown_kind") from error
    expects_value = kind in _VALUE_KINDS
    if len(parts) != (7 if expects_value else 6):
        raise WorkPackageCallbackError("invalid_arity")
    pkg32, revision = parts[3], parts[4]
    if _PKG32.fullmatch(pkg32) is None:
        raise WorkPackageCallbackError("invalid_package")
    if _DECIMAL.fullmatch(revision) is None:
        raise WorkPackageCallbackError("invalid_revision")
    value_part = parts[5] if expects_value else None
    h8 = parts[6] if expects_value else parts[5]
    if value_part is not None and _SMALL_DECIMAL.fullmatch(value_part) is None:
        raise WorkPackageCallbackError("invalid_value")
    if _H8.fullmatch(h8) is None:
        raise WorkPackageCallbackError("invalid_h8")
    return WorkPackageCallback(
        kind=kind,
        package_id=uuid.UUID(hex=pkg32),
        revision_number=int(revision),
        value=None if value_part is None else int(value_part),
        h8=h8,
    )
