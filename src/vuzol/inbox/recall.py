"""Project leads find what the owner saved in the inbox when they recall memory."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import InboxItem


async def inbox_hits(
    session: AsyncSession, *, owner_user_id: int, query: str, limit: int = 4
) -> list[dict[str, Any]]:
    words = [word for word in re.findall(r"\w+", query.lower()) if len(word) >= 3]
    if not words:
        return []
    search = func.to_tsquery(
        "simple", " | ".join(sorted({f"{word[: max(4, len(word) - 2)]}:*" for word in words}))
    )
    items = await session.scalars(
        select(InboxItem)
        .where(
            InboxItem.owner_user_id == owner_user_id,
            InboxItem.removed_at.is_(None),
            InboxItem.search.op("@@")(search),
        )
        .order_by(func.ts_rank(InboxItem.search, search).desc(), InboxItem.created_at.desc())
        .limit(limit)
    )
    return [
        {
            "kind": f"owner_inbox_{item.kind}",
            "project_id": None,
            "text": "\n".join(
                filter(
                    None,
                    (
                        item.title,
                        item.summary,
                        item.fields.get("verdict"),
                        item.fields.get("url"),
                    ),
                )
            )[:2000],
            "created_at": item.created_at.isoformat(),
            "goal_id": None,
            "links": None,
        }
        for item in items
    ]
