"""The assistant worker: answer errands, propose dispatches, send them on the owner's button."""

from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from html import escape
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.assistant import ASSISTANT_DESTINATION
from vuzol.assistant.ingress import schedule
from vuzol.assistant.schemas import TURN_PROMPT, Turn
from vuzol.config.settings import AssistantSettings
from vuzol.inbox.fetch import describe_link
from vuzol.inbox.model import InboxModel, ModelChain
from vuzol.inbox.render import answer_html
from vuzol.inbox.search import WebSearch
from vuzol.interpretation.service import voice_heard_text
from vuzol.leadership.persistence import LIVE_GOAL_LIFECYCLES, ManagedGoal
from vuzol.observability import get_logger
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import (
    AssistantEntry,
    ProjectDiscussionSession,
    TransactionalOutbox,
)
from vuzol.storage.types import DiscussionSessionStatus

_LOG = get_logger(__name__)
_ATTEMPTS = 3
_PAGES = 3
_PAGE_CHARS = 6_000
_GOAL_CHARS = 400
SEND_LABEL = "🚀 Отправить"
EDIT_LABEL = "✏️ Изменить"

Buttons = list[list[list[str]]]


class Sender(Protocol):
    async def __call__(
        self, *, chat_id: int, thread_id: int, html: str, reply_to: int | None, buttons: Buttons
    ) -> int: ...


class ButtonClearer(Protocol):
    async def __call__(self, *, chat_id: int, message_id: int) -> None: ...


class Submitter(Protocol):
    async def __call__(
        self,
        *,
        user_id: int,
        project_id: str,
        session_id: uuid.UUID,
        request_id: uuid.UUID,
        text: str,
    ) -> dict[str, Any]: ...


Downloader = Callable[[str], Awaitable[bytes]]
Transcriber = Callable[[bytes, str], Awaitable[str]]
LinkReader = Callable[[str], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class ProjectRef:
    project_id: str
    name: str


def plan_buttons(entry_id: uuid.UUID) -> Buttons:
    return [[[SEND_LABEL, f"as:s:{entry_id.hex}"], [EDIT_LABEL, f"as:e:{entry_id.hex}"]]]


def plan_html(reply: str, dispatches: Sequence[dict[str, Any]], names: dict[str, str]) -> str:
    lines = [answer_html(reply)]
    if dispatches:
        lines.append("\n📋 <b>План</b>")
        for dispatch in dispatches:
            name = names.get(dispatch["project_id"], dispatch["project_id"])
            request = str(dispatch["request"])
            if len(request) > 600:
                request = request[:600] + "…"
            lines.append(f"\n<b>→ {escape(name)}</b>\n{escape(request)}")
    return "\n".join(lines)


class AssistantWorker:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: AssistantSettings,
        *,
        model: InboxModel | ModelChain,
        projects: Callable[[], Sequence[ProjectRef]],
        submit: Submitter,
        send: Sender,
        clear_buttons: ButtonClearer,
        download: Downloader | None = None,
        transcribe: Transcriber | None = None,
        search: WebSearch | None = None,
        read_link: LinkReader = describe_link,
        owner: str = "assistant",
    ) -> None:
        self._factory = factory
        self._settings = settings
        self._model = model
        self._projects = projects
        self._submit = submit
        self._send = send
        self._clear_buttons = clear_buttons
        self._download = download
        self._transcribe = transcribe
        self._web = search
        self._read_link = read_link
        self._owner = f"assistant:{owner}"

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=3600,  # several search rounds, each a model call that may queue
                allowed_destinations=frozenset({ASSISTANT_DESTINATION}),
            )
        if token is None:
            return False
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            operation, payload, attempts = (
                item.operation_type,
                dict(item.payload),
                item.attempt_count,
            )
        error: str | None = None
        try:
            if operation == "turn":
                await self._turn(payload, final=attempts >= _ATTEMPTS)
            elif operation == "button":
                await self._button(payload)
            elif operation == "reply":
                await self._reply(payload)
            else:
                raise ValueError(f"unknown assistant operation {operation}")
        except Exception as failure:  # every failure is retried, then parked
            error = f"assistant_{type(failure).__name__}"[:100]
            _LOG.warning(
                "assistant operation failed",
                extra={"event": "assistant.failed", "operation": operation, "error": error},
                exc_info=True,
            )
        try:
            async with self._factory.begin() as session:
                if error is None:
                    await complete_outbox_item(session, token)
                elif attempts >= _ATTEMPTS:
                    await dead_letter_outbox_item(session, token, error_category=error)
                else:
                    await retry_outbox_item(
                        session, token, delay_seconds=20 * attempts, error_category=error
                    )
        except LeaseLost:
            pass
        return True

    async def _turn(self, payload: dict[str, Any], *, final: bool) -> None:
        entry_id = uuid.UUID(payload["entry_id"])
        async with self._factory() as session:
            entry = await session.get(AssistantEntry, entry_id)
        if entry is None:
            return
        voice = payload.get("voice")
        if voice and not entry.body.strip():
            # Every voice in a burst is heard, even when a later message answers the burst.
            entry = await self._hear(entry, voice)
        async with self._factory() as session:
            newer = await session.scalar(
                select(AssistantEntry.id).where(
                    AssistantEntry.chat_id == entry.chat_id,
                    AssistantEntry.message_thread_id == entry.message_thread_id,
                    AssistantEntry.created_at > entry.created_at,
                )
            )
        if newer is not None:
            return  # A later message answers the whole burst, or it was answered already.
        refs = {ref.project_id: ref for ref in self._projects()}
        context = await self._context(entry, refs)
        try:
            turn = await self._think(context)
        except Exception:
            if not final:
                raise
            turn = Turn(reply="Модель сейчас недоступна, попробуй ещё раз чуть позже.")
        dispatches = [d.model_dump() for d in turn.dispatches if d.project_id in refs]
        unknown = [d.project_id for d in turn.dispatches if d.project_id not in refs]
        reply = turn.reply
        if unknown:
            reply += "\n\nНе нашёл проекты: " + ", ".join(unknown)
        await self._answer(entry, reply, dispatches, {k: v.name for k, v in refs.items()})

    async def _think(self, context: dict[str, Any]) -> Turn:
        searches: list[str] = []
        rounds = self._settings.search_rounds
        for round_ in range(rounds + 1):
            if self._web is not None:
                context["searches_left"] = rounds - round_
            turn = await self._model.ask(
                Turn, system=self._prompt(), user=json.dumps(context, ensure_ascii=False)
            )
            # Owner 2026-11-03: a second search request went out as "Секунду, уточню…".
            fresh = [query for query in turn.searches if query not in searches]
            if not fresh or self._web is None or round_ == rounds:
                return turn
            searches += fresh
            found = await self._look_up(fresh)
            web = context.setdefault("web", {"searches": {}, "pages": []})
            web["searches"].update(found["searches"])
            web["pages"] += found["pages"]
        raise AssertionError("unreachable")

    async def _hear(self, entry: AssistantEntry, voice: dict[str, Any]) -> AssistantEntry:
        if self._download is None or self._transcribe is None:
            text = "[голосовое: расшифровка недоступна]"
        else:
            content = await self._download(str(voice["file_id"]))
            text = await self._transcribe(content, str(voice.get("mime") or "audio/ogg"))
        async with self._factory.begin() as session:
            stored = await session.get(AssistantEntry, entry.id, with_for_update=True)
            assert stored is not None
            stored.body = text or "[пустое голосовое]"
            await schedule(
                session,
                operation="reply",
                key=f"assistant:heard:{entry.id}",
                payload={
                    "chat_id": entry.chat_id,
                    "thread_id": entry.message_thread_id,
                    "html": escape(voice_heard_text(stored.body, uncertain=False)),
                    "reply_to": entry.telegram_message_id,
                    "buttons": [],
                },
            )
        async with self._factory() as session:
            refreshed = await session.get(AssistantEntry, entry.id)
            assert refreshed is not None
            return refreshed

    async def _context(self, entry: AssistantEntry, refs: dict[str, ProjectRef]) -> dict[str, Any]:
        async with self._factory() as session:
            history = list(
                await session.scalars(
                    select(AssistantEntry)
                    .where(
                        AssistantEntry.chat_id == entry.chat_id,
                        AssistantEntry.message_thread_id == entry.message_thread_id,
                    )
                    .order_by(AssistantEntry.created_at.desc())
                    .limit(self._settings.history_turns + 8)
                )
            )
            goals = {
                goal.project_id: goal
                for goal in await session.scalars(
                    select(ManagedGoal)
                    .where(
                        ManagedGoal.project_id.in_(list(refs)),
                        ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
                    )
                    .order_by(ManagedGoal.created_at)
                )
            }
        history.reverse()
        # The burst since the last reply is the latest message; earlier turns are history.
        last_reply = max(
            (i for i, item in enumerate(history) if item.role == "assistant"), default=-1
        )
        earlier, latest = history[: last_reply + 1], history[last_reply + 1 :]
        earlier = earlier[-self._settings.history_turns :] if self._settings.history_turns else []
        return {
            "projects": [
                {
                    "project_id": ref.project_id,
                    "name": ref.name,
                    "live_goal": _goal_view(goals.get(ref.project_id)),
                }
                for ref in refs.values()
            ],
            "conversation": [_entry_view(item) for item in earlier],
            "latest_owner_message": "\n".join(item.body for item in latest if item.body),
        }

    async def _look_up(self, queries: Sequence[str]) -> dict[str, Any]:
        assert self._web is not None
        hits = {
            query: await self._web.search(query)
            for query in queries[: self._settings.searches_per_round]
        }
        seen: list[str] = []
        for found in hits.values():
            seen += [hit.url for hit in found[:2] if hit.url not in seen]
        pages = [await self._read_link(url) for url in seen[:_PAGES]]
        return {
            "searches": {q: [hit.as_dict() for hit in found] for q, found in hits.items()},
            "pages": [page[:_PAGE_CHARS] for page in pages],
        }

    async def _answer(
        self,
        entry: AssistantEntry,
        reply: str,
        dispatches: list[dict[str, Any]],
        names: dict[str, str],
    ) -> None:
        async with self._factory.begin() as session:
            if dispatches:
                # A new plan replaces any plan still waiting for the owner.
                for old in await session.scalars(
                    select(AssistantEntry).where(
                        AssistantEntry.chat_id == entry.chat_id,
                        AssistantEntry.message_thread_id == entry.message_thread_id,
                        AssistantEntry.plan_status.in_(("pending", "editing")),
                    )
                ):
                    old.plan_status = "superseded"
                    if old.telegram_message_id is not None:
                        await schedule(
                            session,
                            operation="reply",
                            key=f"assistant:clear:{old.id}",
                            payload={"clear": old.telegram_message_id, "chat_id": old.chat_id},
                        )
            answer = AssistantEntry(
                id=uuid.uuid4(),
                chat_id=entry.chat_id,
                message_thread_id=entry.message_thread_id,
                role="assistant",
                body=reply,
                dispatches=dispatches,
                plan_status="pending" if dispatches else None,
            )
            session.add(answer)
            await session.flush()
            await schedule(
                session,
                operation="reply",
                key=f"assistant:reply:{entry.id}",
                payload={
                    "chat_id": entry.chat_id,
                    "thread_id": entry.message_thread_id,
                    "html": plan_html(reply, dispatches, names),
                    "reply_to": entry.telegram_message_id,
                    "buttons": plan_buttons(answer.id) if dispatches else [],
                    "entry_id": str(answer.id),
                },
            )

    async def _button(self, payload: dict[str, Any]) -> None:
        entry_id = uuid.UUID(payload["entry_id"])
        if payload["action"] == "e":
            await self._edit(entry_id)
        else:
            await self._send_plan(entry_id, int(payload["user_id"]))

    async def _edit(self, entry_id: uuid.UUID) -> None:
        async with self._factory.begin() as session:
            entry = await session.get(AssistantEntry, entry_id, with_for_update=True)
            if entry is None or entry.plan_status != "pending":
                return
            entry.plan_status = "editing"
            await self._say(session, entry, "✏️ Напиши, что поменять в плане.", key="edit")

    async def _send_plan(self, entry_id: uuid.UUID, user_id: int) -> None:
        async with self._factory() as session:
            entry = await session.get(AssistantEntry, entry_id)
            if entry is None:
                return
            if entry.plan_status not in ("pending", "editing", "sending"):
                async with self._factory.begin() as write:
                    await self._say(write, entry, "Этот план уже не актуален.", key="stale")
                return
            sessions = {
                row.project_id: row.id
                for row in await session.scalars(
                    select(ProjectDiscussionSession).where(
                        ProjectDiscussionSession.project_id.in_(
                            [d["project_id"] for d in entry.dispatches]
                        ),
                        ProjectDiscussionSession.status == DiscussionSessionStatus.ACTIVE,
                    )
                )
            }
        async with self._factory.begin() as session:
            stored = await session.get(AssistantEntry, entry_id, with_for_update=True)
            assert stored is not None
            stored.plan_status = "sending"
        names = {ref.project_id: ref.name for ref in self._projects()}
        sent: list[str] = []
        failed: list[str] = []
        for index, dispatch in enumerate(entry.dispatches):
            project_id = dispatch["project_id"]
            name = names.get(project_id, project_id)
            session_id = sessions.get(project_id)
            if session_id is None:
                failed.append(f"{name} (нет активного топика)")
                continue
            try:
                result = await self._submit(
                    user_id=user_id,
                    project_id=project_id,
                    session_id=session_id,
                    # Stable per plan line: a retried button never sends twice.
                    request_id=uuid.uuid5(entry_id, str(index)),
                    text=dispatch["request"],
                )
            except (PermissionError, ValueError) as error:
                failed.append(f"{name} ({error})")
                continue
            if result.get("status") in ("accepted", "duplicate"):
                sent.append(name)
            else:
                failed.append(f"{name} ({result.get('reason') or result.get('status')})")
        lines = []
        if sent:
            lines.append("🚀 Отправил: " + ", ".join(sent))
        if failed:
            lines.append("⚠️ Не отправил: " + ", ".join(failed))
        async with self._factory.begin() as session:
            stored = await session.get(AssistantEntry, entry_id, with_for_update=True)
            assert stored is not None
            stored.plan_status = "sent" if sent else "pending"
            if stored.telegram_message_id is not None and sent:
                await schedule(
                    session,
                    operation="reply",
                    key=f"assistant:clear:{stored.id}",
                    payload={"clear": stored.telegram_message_id, "chat_id": stored.chat_id},
                )
            await self._say(session, stored, "\n".join(lines) or "Нечего отправлять.", key="sent")

    async def _say(self, session: AsyncSession, entry: AssistantEntry, text: str, key: str) -> None:
        await schedule(
            session,
            operation="reply",
            key=f"assistant:{key}:{entry.id}:{uuid.uuid4()}",
            payload={
                "chat_id": entry.chat_id,
                "thread_id": entry.message_thread_id,
                "html": escape(text),
                "reply_to": entry.telegram_message_id,
                "buttons": [],
            },
        )

    async def _reply(self, payload: dict[str, Any]) -> None:
        if payload.get("clear"):
            await self._clear_buttons(
                chat_id=int(payload["chat_id"]), message_id=int(payload["clear"])
            )
            return
        message_id = await self._send(
            chat_id=int(payload["chat_id"]),
            thread_id=int(payload["thread_id"]),
            html=str(payload["html"]),
            reply_to=payload.get("reply_to"),
            buttons=payload.get("buttons") or [],
        )
        if payload.get("entry_id"):
            async with self._factory.begin() as session:
                stored = await session.get(AssistantEntry, uuid.UUID(payload["entry_id"]))
                if stored is not None:
                    stored.telegram_message_id = message_id

    def _prompt(self) -> str:
        now = datetime.now(ZoneInfo(self._settings.timezone))
        return TURN_PROMPT.format(
            tz=self._settings.timezone,
            now=now.isoformat(timespec="minutes"),
            per_round=self._settings.searches_per_round,
        )


def _goal_view(goal: ManagedGoal | None) -> dict[str, Any] | None:
    if goal is None:
        return None
    request = goal.original_request
    return {
        "request": request[:_GOAL_CHARS] + ("…" if len(request) > _GOAL_CHARS else ""),
        "lifecycle": goal.lifecycle,
        "waiting_reason": goal.waiting_reason,
        "since": goal.created_at.date().isoformat(),
    }


def _entry_view(entry: AssistantEntry) -> dict[str, Any]:
    view: dict[str, Any] = {"role": entry.role, "text": entry.body}
    if entry.dispatches:
        view["plan"] = entry.dispatches
        view["plan_status"] = entry.plan_status
    return view
