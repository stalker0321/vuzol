import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from vuzol.config.models import ProviderProfileConfig
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.inference import _send_guarded
from vuzol.leadership.invocation_context import TurnInvocationContext
from vuzol.providers.domain import ProviderRequest
from vuzol.providers.errors import ProviderFailure


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_failed_preflight_is_authoritatively_unsent():
    class UnavailableDatabase:
        @asynccontextmanager
        async def begin(self):
            raise TimeoutError("database unavailable before send")
            yield  # pragma: no cover

    adapter = AsyncMock()
    context = TurnInvocationContext(
        TurnClaim(uuid.uuid4(), uuid.uuid4(), "owner", 1, 1),
        uuid.uuid4(),
        "project",
        1,
        1,
        1,
    )
    with pytest.raises(ProviderFailure) as raised:
        await _send_guarded(
            UnavailableDatabase(),
            adapter,
            Mock(spec=ProviderRequest),
            Mock(spec=ProviderProfileConfig),
            context,
        )
    assert raised.value.request_sent is False
    adapter.execute.assert_not_called()
