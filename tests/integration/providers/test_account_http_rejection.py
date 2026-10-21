"""Review regression: real HTTP adapter and account admission, fake HTTP only.

The repeated-rejection assertions come from the independent 2026-10-21 probe.
The client needs base_url: without it httpx raises a URL error after MockTransport,
before the adapter can classify the HTTP response. The external probe is unmodified.
"""

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from tests.unit.providers._test_providers_helpers import profile, provider_request
from vuzol.config import ProfileRegistry, ProviderProfileConfig, ProviderRole, ScopedSecretResolver
from vuzol.providers.account_persistence import ProviderAccountLease
from vuzol.providers.accounts import AccountBoundAdapter
from vuzol.providers.domain import ProviderErrorCategory, ProviderResultStatus
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.openai import OpenAICompatibleAdapter
from vuzol.providers.registry import AdapterRegistry
from vuzol.workflows.ports import CancellationContext

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("status", [429, 401, 400, 403, 404, 413, 422])
def test_completed_http_rejection_releases_slot(postgres_dsn: str, status: int) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("review-probe", account_id="review-account")
        calls = []

        def response(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(status, json={"error": {"message": "rejected"}})

        try:
            async with httpx.AsyncClient(
                base_url=str(configured.api_base_url), transport=httpx.MockTransport(response)
            ) as client:
                wrapper = AccountBoundAdapter(
                    OpenAICompatibleAdapter(credential=SecretStr("test-only"), client=client),
                    configured,
                    factory,
                    account_profile_ids=(configured.id,),
                )
                with pytest.raises(ProviderFailure) as first:
                    await wrapper.execute(provider_request(), configured, CancellationContext())
                assert first.value.request_sent
                assert first.value.category is {
                    429: ProviderErrorCategory.RATE_LIMITED,
                    401: ProviderErrorCategory.AUTHENTICATION,
                    403: ProviderErrorCategory.AUTHENTICATION,
                    413: ProviderErrorCategory.CONTEXT_TOO_LARGE,
                }.get(status, ProviderErrorCategory.PERMANENT_REQUEST)
                assert first.value.execution_completed
                assert first.value.usage is None and first.value.receipt_result() is None
                assert len(calls) == 1
                with pytest.raises(ProviderFailure) as second:
                    await wrapper.execute(provider_request(), configured, CancellationContext())
                async with factory() as session:
                    ticket = await session.get(ProviderAccountLease, ("review-account", 0))
                assert ticket is None and len(calls) == 2, (
                    f"HTTP {status}: ticket_retained={ticket is not None}, "
                    f"http_calls={len(calls)}, second_failure={second.value}"
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def _registry(
    profiles: tuple[ProviderProfileConfig, ...],
    factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    credential: str,
) -> AdapterRegistry:
    return AdapterRegistry(
        ProfileRegistry(profiles),
        ScopedSecretResolver(
            access_policy={"env:SHARED_KEY": frozenset(f"profile:{p.id}" for p in profiles)},
            secret_file_root=tmp_path,
            environment={"SHARED_KEY": credential},
        ),
        session_factory=factory,
    )


@pytest.mark.parametrize("status", [429, 401, 400])
def test_registry_recovery_reaches_http_on_same_account_after_rejection(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """Correct credentials/request, reconstruct registry, then another role can execute."""
    calls = []

    def response(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/v1/chat/completions"
        if len(calls) == 1:
            assert request.headers["authorization"] == "Bearer fixture-old"
            return httpx.Response(
                status,
                json={"error": {"message": "rejected"}},
                headers={"retry-after": "7", "x-request-id": "rejected-request"},
            )
        assert request.headers["authorization"] == "Bearer fixture-corrected"
        payload = json.loads(request.content)
        assert json.loads(payload["messages"][1]["content"])["original_input"] == "corrected"
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "recovered"}}],
                "usage": {"prompt_tokens": 9, "completion_tokens": 2},
            },
        )

    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original_client(transport=httpx.MockTransport(response), **kwargs),
    )

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        profiles = tuple(
            profile(
                role,
                account_id="shared",
                credential_reference="env:SHARED_KEY",
                roles=frozenset({role}),
            )
            for role in ("planner", "reviewer")
        )
        try:
            registry = _registry(profiles, factory, tmp_path, "fixture-old")
            with pytest.raises(ProviderFailure) as failure:
                await registry.get("planner").execute(
                    provider_request().model_copy(update={"role": ProviderRole.PLANNER}),
                    profiles[0],
                    CancellationContext(),
                )
            assert failure.value.request_sent and failure.value.execution_completed
            assert failure.value.usage is None
            assert failure.value.provider_request_id == "rejected-request"
            assert failure.value.retry_after_seconds == 7
            # New process/config composition must not inherit a stale admission ticket.
            registry = _registry(profiles, factory, tmp_path, "fixture-corrected")
            result = await registry.get("reviewer").execute(
                provider_request().model_copy(
                    update={"original_input": "corrected", "role": ProviderRole.REVIEWER}
                ),
                profiles[1],
                CancellationContext(),
            )
            assert result.status is ProviderResultStatus.SUCCEEDED and result.text == "recovered"
            assert result.usage.input_tokens == 9 and len(calls) == 2
            async with factory() as session:
                assert await session.get(ProviderAccountLease, ("shared", 0)) is None
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", ["timeout", "disconnect", "protocol", 408, 499, 500, 502, 504])
def test_unknown_http_outcome_retains_ticket_across_registry_restart(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str | int
) -> None:
    calls = []

    def response(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if isinstance(outcome, int):
            return httpx.Response(outcome, json={"error": {"message": "uncertain"}})
        error_type = {
            "timeout": httpx.ReadTimeout,
            "disconnect": httpx.ReadError,
            "protocol": httpx.RemoteProtocolError,
        }[outcome]
        raise error_type("response lost after send", request=request)

    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original_client(transport=httpx.MockTransport(response), **kwargs),
    )

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("first", account_id="shared", credential_reference="env:SHARED_KEY")
        try:
            registry = _registry((configured,), factory, tmp_path, "fixture-only")
            with pytest.raises(ProviderFailure) as failure:
                await registry.get("first").execute(
                    provider_request(), configured, CancellationContext()
                )
            assert failure.value.request_sent and not failure.value.execution_completed
            assert failure.value.usage is None
            async with factory() as session:
                ticket = await session.get(ProviderAccountLease, ("shared", 0))
                assert ticket is not None
                token = ticket.token
            registry = _registry((configured,), factory, tmp_path, "fixture-only")
            with pytest.raises(ProviderFailure, match="unresolved process/ticket") as retry:
                await registry.get("first").execute(
                    provider_request(), configured, CancellationContext()
                )
            assert not retry.value.request_sent and len(calls) == 1
            async with factory() as session:
                ticket = await session.get(ProviderAccountLease, ("shared", 0))
                assert ticket is not None and ticket.token == token
        finally:
            await engine.dispose()

    asyncio.run(scenario())
