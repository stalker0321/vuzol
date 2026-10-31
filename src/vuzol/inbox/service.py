"""The inbox worker: settle bursts, build records, answer questions, send replies."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, time, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.settings import InboxSettings
from vuzol.inbox import INBOX_DESTINATION
from vuzol.inbox.fetch import describe_link
from vuzol.inbox.ingress import schedule
from vuzol.inbox.model import Image, InboxModel, InboxModelError, ModelChain
from vuzol.inbox.render import answer_html, card_html
from vuzol.inbox.schemas import (
    ASK_PROMPT,
    CARD_PROMPT,
    LOOKUP_PROMPT,
    ROUTE_PROMPT,
    SEARCH_PROMPT,
    Answer,
    Card,
    Cards,
    Lookup,
    Patch,
    Route,
)
from vuzol.inbox.search import WebSearch
from vuzol.observability import get_logger
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import InboxItem, InboxLink, InboxMessage, TransactionalOutbox

_LOG = get_logger(__name__)
_MAX_IMAGES = 4
_MAX_IMAGE_BYTES = 4_500_000
_MAX_LINKS = 3
_EVIDENCE_CHARS = 30_000
_SEARCH_LIMIT = 40
_ATTEMPTS = 3
_PAGES = 3
_PAGE_CHARS = 6_000


class Sender(Protocol):
    async def __call__(self, *, chat_id: int, html: str, reply_to: int | None) -> int: ...


Downloader = Callable[[str], Awaitable[bytes]]
Transcriber = Callable[[bytes, str], Awaitable[str]]
LinkReader = Callable[[str], Awaitable[str]]


class InboxWorker:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: InboxSettings,
        *,
        model: InboxModel | ModelChain,
        send: Sender,
        download: Downloader,
        transcribe: Transcriber | None = None,
        read_link: LinkReader = describe_link,
        search: WebSearch | None = None,
        owner: str = "inbox",
    ) -> None:
        self._web = search
        self._factory = factory
        self._settings = settings
        self._model = model
        self._send = send
        self._download = download
        self._transcribe = transcribe
        self._read_link = read_link
        self._owner = f"inbox:{owner}"

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=900,
                allowed_destinations=frozenset({INBOX_DESTINATION}),
            )
            if token is None:
                return False
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            operation, payload, attempts = (
                item.operation_type,
                dict(item.payload),
                item.attempt_count,
            )
        error: str | None = None
        try:
            if operation == "settle":
                await self._settle(int(payload["chat_id"]))
            elif operation == "analyze":
                await self._analyze(uuid.UUID(payload["batch_id"]), final=attempts >= _ATTEMPTS)
            elif operation == "answer":
                await self._answer_op(payload)
            elif operation == "reply":
                await self._reply(payload)
            else:
                raise ValueError(f"unknown inbox operation {operation}")
        except Exception as failure:  # every failure is retried, then parked
            error = f"inbox_{type(failure).__name__}"[:100]
            _LOG.warning(
                "inbox operation failed",
                extra={"event": "inbox.failed", "operation": operation, "error": error},
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

    # A burst of messages becomes one batch once nothing new arrived for a moment.
    async def _settle(self, chat_id: int) -> None:
        async with self._factory.begin() as session:
            await session.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(f"inbox:{chat_id}", 0)))
            )
            pending = list(
                await session.scalars(
                    select(InboxMessage)
                    .where(InboxMessage.chat_id == chat_id, InboxMessage.batch_id.is_(None))
                    .order_by(InboxMessage.message_id)
                )
            )
            if not pending:
                return
            newest = max(message.received_at for message in pending)
            quiet = timedelta(seconds=self._settings.settle_seconds * 0.8)
            if newest > datetime.now(UTC) - quiet:
                return  # The newest message's own settle comes later.
            batch_id = uuid.uuid4()
            for message in pending:
                message.batch_id = batch_id
            await schedule(
                session,
                operation="analyze",
                key=f"inbox:analyze:{batch_id}",
                payload={"batch_id": str(batch_id), "chat_id": chat_id},
            )

    async def _analyze(self, batch_id: uuid.UUID, *, final: bool) -> None:
        async with self._factory() as session:
            messages = list(
                await session.scalars(
                    select(InboxMessage)
                    .where(InboxMessage.batch_id == batch_id)
                    .order_by(InboxMessage.message_id)
                )
            )
            if not messages or await session.scalar(
                select(InboxItem.id).where(InboxItem.batch_id == batch_id).limit(1)
            ):
                return
        await self._transcribe_voices(messages)
        content = [message for message in messages if _is_content(message)]
        words = "\n".join(
            text
            for message in messages
            if not _is_content(message)
            for text in [_words(message)]
            if text
        )
        chat_id, user_id = messages[0].chat_id, messages[0].user_id
        replied = await self._replied_item(chat_id, messages)
        try:
            if content:
                await self._save(batch_id, chat_id, user_id, content, words, messages)
            elif words:
                await self._route(batch_id, chat_id, user_id, words, replied, messages)
        except InboxModelError:
            if not final:
                raise
            # Never lose what the owner sent: keep it raw and say so.
            await self._keep_raw(batch_id, chat_id, user_id, messages)

    async def _save(
        self,
        batch_id: uuid.UUID,
        chat_id: int,
        user_id: int,
        content: Sequence[InboxMessage],
        comment: str,
        messages: Sequence[InboxMessage],
    ) -> None:
        evidence, images = await self._evidence(content)
        cards = await self._model.ask(
            Cards,
            system=self._prompt(CARD_PROMPT),
            user=json.dumps(
                {"owner_comment": comment or None, "material": evidence}, ensure_ascii=False
            ),
            images=images,
        )
        await self._store(
            batch_id,
            chat_id,
            user_id,
            cards.cards,
            evidence,
            reply_to=messages[-1].message_id,
            question=cards.question,
        )

    async def _route(
        self,
        batch_id: uuid.UUID,
        chat_id: int,
        user_id: int,
        words: str,
        replied: InboxItem | None,
        messages: Sequence[InboxMessage],
    ) -> None:
        recent = None if replied else await self._recent_item(chat_id)
        route = await self._model.ask(
            Route,
            system=self._prompt(ROUTE_PROMPT),
            user=json.dumps(
                {
                    "owner_words": words,
                    "replied_record": _brief(replied) if replied else None,
                    "recent_record": _brief(recent) if recent else None,
                },
                ensure_ascii=False,
            ),
        )
        anchor = messages[-1].message_id
        target = (
            replied if route.target == "replied" else recent if route.target == "recent" else None
        )
        target = target or replied
        if route.action == "save":
            await self._save(batch_id, chat_id, user_id, messages, "", messages)
        elif route.action == "ask" and target is not None:
            await self._answer([target], words, chat_id, reply_to=anchor, key=str(batch_id))

        elif route.action in {"ask", "web"}:
            # A question for the internet, or one whose record could not be found.
            await self._answer([], words, chat_id, reply_to=anchor, key=str(batch_id))
        elif route.action == "search":
            await self._search(route, words, chat_id, user_id, reply_to=anchor, key=str(batch_id))
        elif route.action == "update" and target is not None and route.patch is not None:
            await self._update(target, route.patch, chat_id, reply_to=anchor, key=str(batch_id))
        elif route.action == "delete" and target is not None:
            await self._delete(target, chat_id, reply_to=anchor, key=str(batch_id))
        else:
            text = route.reply or "Не понял, к какой записи это. Ответь реплаем на её карточку."
            await self._queue(chat_id, answer_html(text), reply_to=anchor, key=f"chat:{batch_id}")

    async def _answer_op(self, payload: dict[str, Any]) -> None:
        async with self._factory() as session:
            items = [
                item
                for raw in payload["item_ids"]
                if (item := await session.get(InboxItem, uuid.UUID(raw))) is not None
            ]
        if items:
            await self._answer(
                items,
                str(payload["question"]),
                int(payload["chat_id"]),
                reply_to=int(payload["reply_to"]),
                key=str(payload["batch_id"]),
            )

    async def _answer(
        self,
        items: Sequence[InboxItem],
        question: str,
        chat_id: int,
        *,
        reply_to: int,
        key: str,
    ) -> None:
        records = [
            {
                **_brief(item),
                "evidence": item.evidence[:_EVIDENCE_CHARS],
                "earlier_answers": item.answers[-3:],
            }
            for item in items
        ]
        web = await self._look_up(question, [_brief(item) for item in items])
        answer = await self._model.ask(
            Answer,
            system=self._prompt(ASK_PROMPT),
            user=json.dumps(
                {"question": question, "records": records, "web": web},
                ensure_ascii=False,
                default=str,
            ),
        )
        async with self._factory.begin() as session:
            for item in items:
                stored = await session.get(InboxItem, item.id, with_for_update=True)
                if stored is not None:
                    stored.answers = [
                        *stored.answers,
                        {
                            "question": question,
                            "answer": answer.text,
                            "at": datetime.now(UTC).isoformat(),
                        },
                    ][-20:]
            await self._enqueue(
                session,
                chat_id,
                answer_html(answer.text),
                reply_to=reply_to,
                key=f"answer:{key}",
                item_ids=[items[0].id] if items else [],
            )

    async def _search(
        self,
        route: Route,
        question: str,
        chat_id: int,
        user_id: int,
        *,
        reply_to: int,
        key: str,
    ) -> None:
        spec = route.search
        tz = ZoneInfo(self._settings.timezone)
        conditions: list[Any] = [InboxItem.owner_user_id == user_id, InboxItem.removed_at.is_(None)]
        order: list[Any] = [InboxItem.created_at.desc()]
        if spec is not None and spec.kinds:
            conditions.append(InboxItem.kind.in_(spec.kinds))
        if spec is not None and (spec.date_from or spec.date_to):
            if spec.date_from:
                start = datetime.combine(spec.date_from, time.min, tz)
                conditions.append(or_(InboxItem.event_end >= start, InboxItem.event_start >= start))
            if spec.date_to:
                end = datetime.combine(spec.date_to, time.max, tz)
                conditions.append(InboxItem.event_start <= end)
            order = [InboxItem.event_start.asc()]
        words = [
            w
            for w in re.findall(r"\w+", ((spec.words if spec else None) or "").lower())
            if len(w) >= 3
        ]
        if words:
            query = func.to_tsquery(
                "simple", " | ".join(sorted({f"{w[: max(4, len(w) - 2)]}:*" for w in words}))
            )
            conditions.append(InboxItem.search.op("@@")(query))
            order = [func.ts_rank(InboxItem.search, query).desc(), *order]
        async with self._factory() as session:
            found = list(
                await session.scalars(
                    select(InboxItem).where(*conditions).order_by(*order).limit(_SEARCH_LIMIT)
                )
            )
        web = await self._look_up(question, [_brief(item) for item in found[:10]])
        answer = await self._model.ask(
            Answer,
            system=self._prompt(SEARCH_PROMPT),
            user=json.dumps(
                {"question": question, "records": [_brief(item) for item in found], "web": web},
                ensure_ascii=False,
                default=str,
            ),
        )
        await self._queue(chat_id, answer_html(answer.text), reply_to=reply_to, key=f"search:{key}")

    async def _look_up(self, question: str, records: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Let the model pick searches, run them and open the best pages."""
        if self._web is None:
            return None
        plan = await self._model.ask(
            Lookup,
            system=self._prompt(LOOKUP_PROMPT),
            user=json.dumps({"question": question, "records": records}, ensure_ascii=False),
        )
        if not plan.queries and not plan.github:
            return None
        hits = {query: await self._web.search(query) for query in plan.queries}
        seen: list[str] = []
        for found in hits.values():
            seen += [hit.url for hit in found[:2] if hit.url not in seen]
        pages = [await self._read_link(url) for url in seen[:_PAGES]]
        return {
            "searches": {q: [hit.as_dict() for hit in found] for q, found in hits.items()},
            "pages": [page[:_PAGE_CHARS] for page in pages],
            "github": {query: await self._web.github(query) for query in plan.github},
        }

    async def _update(
        self, item: InboxItem, patch: Patch, chat_id: int, *, reply_to: int, key: str
    ) -> None:
        async with self._factory.begin() as session:
            stored = await session.get(InboxItem, item.id, with_for_update=True)
            if stored is None:
                return
            for name in ("kind", "title", "summary", "tags", "event_start", "event_end"):
                value = getattr(patch, name)
                if value is not None:
                    setattr(stored, name, value)
            if patch.fields:
                stored.fields = {**stored.fields, **patch.fields}
            await session.flush()
            await self._enqueue(
                session,
                chat_id,
                card_html(stored, self._settings.timezone, header="✏️ Поправил"),
                reply_to=reply_to,
                key=f"update:{key}",
                item_ids=[stored.id],
            )

    async def _delete(self, item: InboxItem, chat_id: int, *, reply_to: int, key: str) -> None:
        async with self._factory.begin() as session:
            stored = await session.get(InboxItem, item.id, with_for_update=True)
            if stored is None:
                return
            stored.removed_at = datetime.now(UTC)
            await self._enqueue(
                session,
                chat_id,
                answer_html(f"🗑 Удалил: {stored.title}"),
                reply_to=reply_to,
                key=f"delete:{key}",
                item_ids=[],
            )

    async def _keep_raw(
        self,
        batch_id: uuid.UUID,
        chat_id: int,
        user_id: int,
        messages: Sequence[InboxMessage],
    ) -> None:
        text = "\n".join(filter(None, (_words(m) or m.forward_label for m in messages)))
        urls = [url for message in messages for url in message.urls]
        card = Card(
            kind="note",
            title=(text.splitlines()[0] if text else urls[0] if urls else "Без текста")[:200],
            summary=text[:1500],
            fields={"url": urls[0]} if urls else {},
        )
        await self._store(
            batch_id,
            chat_id,
            user_id,
            [card],
            text,
            reply_to=messages[-1].message_id,
            header="⚠️ Не смог разобрать, сохранил как есть",
        )

    # Helpers below: evidence, storage and the reply queue.
    async def _evidence(self, content: Sequence[InboxMessage]) -> tuple[str, tuple[Image, ...]]:
        parts: list[str] = []
        images: list[Image] = []
        links: list[str] = []
        for message in content:
            head = f"forwarded from {message.forward_label}" if message.forward_label else "sent"
            if message.forward_url:
                head += f" ({message.forward_url})"
            parts.append(f"--- message {message.message_id}, {head}")
            if message.body:
                parts.append(message.body)
            if message.transcript:
                parts.append("voice: " + message.transcript)
            for attachment in message.attachments:
                if attachment["kind"] == "photo" and len(images) < _MAX_IMAGES:
                    if attachment.get("size", 0) <= _MAX_IMAGE_BYTES:
                        images.append(Image(await self._download(attachment["file_id"])))
                        parts.append(f"[image {len(images)} attached]")
                elif attachment["kind"] in {"document", "video", "audio"}:
                    name = attachment.get("filename") or attachment["kind"]
                    parts.append(f"[{attachment['kind']}: {name}, {attachment.get('mime')}]")
            links += [url for url in message.urls if url not in links]
        for url in links[:_MAX_LINKS]:
            parts.append("--- link\n" + await self._read_link(url))
        return "\n".join(parts)[:_EVIDENCE_CHARS], tuple(images)

    async def _transcribe_voices(self, messages: Sequence[InboxMessage]) -> None:
        if self._transcribe is None:
            return
        for message in messages:
            # A forwarded music file is not speech; a forwarded voice note is.
            kinds = {"voice"} if message.forward_label else {"voice", "audio"}
            voice = next((a for a in message.attachments if a["kind"] in kinds), None)
            if voice is None or message.transcript is not None:
                continue
            transcript = await self._transcribe(
                await self._download(voice["file_id"]), voice.get("mime") or "audio/ogg"
            )
            async with self._factory.begin() as session:
                stored = await session.get(InboxMessage, message.id)
                if stored is not None:
                    stored.transcript = transcript
            message.transcript = transcript

    async def _store(
        self,
        batch_id: uuid.UUID,
        chat_id: int,
        user_id: int,
        cards: Sequence[Card],
        evidence: str,
        *,
        reply_to: int,
        question: str | None = None,
        header: str = "📥 Сохранил",
    ) -> None:
        """Records, their cards and the question's answer job commit together."""
        async with self._factory.begin() as session:
            items: list[InboxItem] = []
            for ordinal, card in enumerate(cards):
                fields = dict(card.fields)
                if card.verdict:
                    fields["verdict"] = card.verdict
                item = InboxItem(
                    owner_user_id=user_id,
                    chat_id=chat_id,
                    batch_id=batch_id,
                    ordinal=ordinal,
                    kind=card.kind,
                    title=card.title,
                    summary=card.summary,
                    tags=[tag.strip().lower() for tag in card.tags if tag.strip()],
                    fields=fields,
                    event_start=_aware(card.event_start, self._settings.timezone),
                    event_end=_aware(card.event_end, self._settings.timezone),
                    evidence=evidence,
                    answers=[],
                )
                session.add(item)
                items.append(item)
            await session.flush()
            for item in items:
                await self._enqueue(
                    session,
                    chat_id,
                    card_html(item, self._settings.timezone, header=header),
                    reply_to=reply_to,
                    key=f"cards:{batch_id}:{item.ordinal}",
                    item_ids=[item.id],
                )
            if question:
                await schedule(
                    session,
                    operation="answer",
                    key=f"inbox:answer:{batch_id}",
                    payload={
                        "batch_id": str(batch_id),
                        "chat_id": chat_id,
                        "reply_to": reply_to,
                        "question": question,
                        "item_ids": [str(item.id) for item in items],
                    },
                )

    async def _queue(self, chat_id: int, html: str, *, reply_to: int, key: str) -> None:
        async with self._factory.begin() as session:
            await self._enqueue(session, chat_id, html, reply_to=reply_to, key=key, item_ids=[])

    async def _enqueue(
        self,
        session: AsyncSession,
        chat_id: int,
        html: str,
        *,
        reply_to: int | None,
        key: str,
        item_ids: Sequence[uuid.UUID],
    ) -> None:
        await schedule(
            session,
            operation="reply",
            key=f"inbox:reply:{key}",
            payload={
                "chat_id": chat_id,
                "html": html,
                "reply_to": reply_to,
                "item_ids": [str(item_id) for item_id in item_ids],
            },
        )

    async def _reply(self, payload: dict[str, Any]) -> None:
        html = payload.get("html") or answer_html(str(payload.get("text") or ""))
        message_id = await self._send(
            chat_id=int(payload["chat_id"]), html=html, reply_to=payload.get("reply_to")
        )
        async with self._factory.begin() as session:
            for raw in payload.get("item_ids") or []:
                session.add(
                    InboxLink(
                        chat_id=int(payload["chat_id"]),
                        message_id=message_id,
                        item_id=uuid.UUID(raw),
                    )
                )
                break  # One record per bot message; replies resolve to it.

    async def _replied_item(
        self, chat_id: int, messages: Sequence[InboxMessage]
    ) -> InboxItem | None:
        replies = [m.reply_to_message_id for m in messages if m.reply_to_message_id]
        if not replies:
            return None
        async with self._factory() as session:
            item_id = await session.scalar(
                select(InboxLink.item_id).where(
                    InboxLink.chat_id == chat_id, InboxLink.message_id.in_(replies)
                )
            )
            if item_id is None:
                # A reply to the owner's own forward: the record built from its batch.
                item_id = await session.scalar(
                    select(InboxItem.id)
                    .join(InboxMessage, InboxMessage.batch_id == InboxItem.batch_id)
                    .where(InboxMessage.chat_id == chat_id, InboxMessage.message_id.in_(replies))
                    .order_by(InboxItem.ordinal)
                    .limit(1)
                )
            item = await session.get(InboxItem, item_id) if item_id else None
            return item if item is not None and item.removed_at is None else None

    async def _recent_item(self, chat_id: int) -> InboxItem | None:
        since = datetime.now(UTC) - timedelta(minutes=self._settings.followup_minutes)
        async with self._factory() as session:
            recent: InboxItem | None = await session.scalar(
                select(InboxItem)
                .where(
                    InboxItem.chat_id == chat_id,
                    InboxItem.removed_at.is_(None),
                    InboxItem.created_at >= since,
                )
                .order_by(InboxItem.created_at.desc(), InboxItem.ordinal)
                .limit(1)
            )
            return recent

    def _prompt(self, template: str) -> str:
        now = datetime.now(ZoneInfo(self._settings.timezone))
        return template.format(tz=self._settings.timezone, now=now.isoformat(timespec="minutes"))


def _is_content(message: InboxMessage) -> bool:
    """Material to keep, as opposed to the owner's own words about it."""
    return bool(
        message.forward_label
        or message.urls
        or any(a["kind"] not in {"voice", "audio"} for a in message.attachments)
    )


def _words(message: InboxMessage) -> str:
    return (message.body or message.transcript or "").strip()


def _brief(item: InboxItem) -> dict[str, Any]:
    return {
        "kind": item.kind,
        "title": item.title,
        "summary": item.summary,
        "tags": item.tags,
        "fields": item.fields,
        "event_start": item.event_start.isoformat() if item.event_start else None,
        "event_end": item.event_end.isoformat() if item.event_end else None,
        "saved_at": item.created_at.date().isoformat() if item.created_at else None,
    }


def _aware(moment: datetime | None, tz: str) -> datetime | None:
    if moment is None or moment.tzinfo is not None:
        return moment
    return moment.replace(tzinfo=ZoneInfo(tz))
