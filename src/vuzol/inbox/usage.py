"""Owner 2026-11-26: the tokens of system work (news, mail, inbox, the assistant) are counted.

Project work is counted through budget reservations; these calls are not reserved, so each
answered call leaves a usage record of its own, with its purpose.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.observability import get_logger
from vuzol.storage.models import UsageRecord

_LOG = get_logger(__name__)


def _tokens(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def usage_record(
    *,
    provider: str,
    profile_id: str,
    model: str,
    purpose: str,
    usage: dict[str, Any],
    duration_ms: int,
    outcome: str,
) -> UsageRecord:
    """A record from an OpenAI-style usage object or a provider result's usage."""
    details = usage.get("prompt_tokens_details")
    cached = usage.get("cached_tokens")
    if cached is None and isinstance(details, dict):
        cached = details.get("cached_tokens")
    return UsageRecord(
        provider=provider[:100],
        profile_id=profile_id[:100],
        model=model[:200],
        input_tokens=_tokens(usage.get("input_tokens", usage.get("prompt_tokens"))),
        output_tokens=_tokens(usage.get("output_tokens", usage.get("completion_tokens"))),
        cached_tokens=_tokens(cached),
        duration_ms=max(0, int(duration_ms)),
        outcome=outcome[:100],
        purpose=purpose[:30],
    )


class UsageRecorder:
    """Writes one usage record per answered call; a failed write never fails the call."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        provider: str,
        profile_id: str,
        purpose: str,
    ) -> None:
        self._factory = factory
        self._provider = provider
        self._profile_id = profile_id
        self._purpose = purpose

    async def __call__(self, *, model: str, usage: dict[str, Any], duration_ms: int) -> None:
        try:
            async with self._factory.begin() as session:
                session.add(
                    usage_record(
                        provider=self._provider,
                        profile_id=self._profile_id,
                        model=model,
                        purpose=self._purpose,
                        usage=usage,
                        duration_ms=duration_ms,
                        outcome="succeeded",
                    )
                )
        except Exception:
            _LOG.warning("usage record failed", extra={"event": "usage.record_failed"})
