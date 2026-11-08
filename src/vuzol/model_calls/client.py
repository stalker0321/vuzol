"""The inbox model's shape over the executor, so a CLI subscription model can answer a service."""

from __future__ import annotations

import asyncio
import time

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.inbox.model import Image, InboxModelError, M
from vuzol.leadership.provider_model import strict_response_schema
from vuzol.model_calls.queue import submit_call
from vuzol.model_calls.request import MAX_INPUT_CHARS
from vuzol.storage.models import ModelCall

# Queueing, sandbox start and cleanup, on top of the model's own time.
_START_SECONDS = 120


class RelayedModel:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        profile_id: str,
        model: str,
        purpose: str,
        timeout_seconds: float,
        poll_seconds: float = 1.0,
    ) -> None:
        self._factory = factory
        self._profile_id = profile_id
        self.model = model
        self._purpose = purpose
        self._timeout = int(timeout_seconds)
        self._poll = poll_seconds

    async def ask(
        self,
        output: type[M],
        *,
        system: str,
        user: str,
        images: tuple[Image, ...] = (),
    ) -> M:
        if images:
            raise InboxModelError("images_unsupported")
        if len(user) > MAX_INPUT_CHARS:
            raise InboxModelError("input_too_large")
        wait = self._timeout + _START_SECONDS
        async with self._factory.begin() as session:
            call_id = await submit_call(
                session,
                purpose=self._purpose,
                profile_id=self._profile_id,
                system=system,
                user=user,
                output_schema=strict_response_schema(output),
                timeout_seconds=self._timeout,
                wait_seconds=wait,
            )
        give_up = time.monotonic() + wait + 60
        while time.monotonic() < give_up:
            await asyncio.sleep(self._poll)
            async with self._factory() as session:
                call = await session.get(ModelCall, call_id)
            assert call is not None
            if call.state == "succeeded":
                try:
                    return output.model_validate(call.output)
                except ValidationError as error:
                    raise InboxModelError("invalid_model_output") from error
            if call.state == "failed":
                raise InboxModelError(call.error or "model_call_failed")
        raise InboxModelError("model_call_timeout")
