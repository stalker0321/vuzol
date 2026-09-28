import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.projects.node_claim import claim_node_step
from vuzol.projects.nodes import (
    heartbeat_node,
    mark_node_offline,
    register_node,
    revoke_node,
)
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import complete_step
from vuzol.storage.models import CapabilityInstallation, Node, NodeSlot, Step
from vuzol.storage.records import LeaseToken
from vuzol.storage.slots import (
    claim_slot,
    find_expired_slots,
    reconcile_slot,
    release_slot,
)
from vuzol.storage.types import StepStatus

from .helpers import seed_task_run_step, storage


async def _register(
    factory: async_sessionmaker[AsyncSession],
    *,
    node_id: str = "remote-1",
    trust_class: str = "remote",
) -> None:
    async with factory.begin() as session:
        await register_node(session, node_id=node_id, trust_class=trust_class, detail="test")


async def _installation(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    from datetime import UTC, datetime

    moment = datetime.now(UTC) + timedelta(seconds=600)
    async with factory.begin() as session:
        session.add(
            CapabilityInstallation(
                capability_key="toolchain-x",
                version="1.0",
                status="installed",
                probe_status="healthy",
                installation_root="/opt/toolchains",
                node_id="remote-1",
                health_until=moment,
            )
        )


async def _node_claim(
    session: AsyncSession, node_id: str, owner: str, capabilities: frozenset[str]
) -> LeaseToken | None:
    return await claim_node_step(
        session,
        node_id=node_id,
        owner=owner,
        lease_seconds=60,
        capabilities=capabilities,
    )


@pytest.mark.postgresql
def test_requirement_routes_to_healthy_node_and_wrong_scope_excluded(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            await _register(factory)
            await _installation(factory)
            await seed_task_run_step(factory, capabilities=["toolchain-x"])
            async with factory.begin() as session:
                token = await _node_claim(
                    session, "remote-1", "remote-1:100", frozenset({"toolchain-x"})
                )
            assert token is not None
            # Wrong trust scope is not eligible.
            async with factory.begin() as session:
                assert (
                    await claim_node_step(
                        session,
                        node_id="remote-1",
                        owner="remote-1:100",
                        lease_seconds=60,
                        capabilities=frozenset({"toolchain-x"}),
                        require_trust_class="local",
                    )
                    is None
                )
            # Stale installation excludes the requirement.
            async with factory.begin() as session:
                await session.execute(
                    update(CapabilityInstallation)
                    .where(CapabilityInstallation.node_id == "remote-1")
                    .values(
                        status="installed",
                        health_until=func.now() - timedelta(seconds=1),
                    )
                )
            await seed_task_run_step(factory, capabilities=["toolchain-x"])
            async with factory.begin() as session:
                assert (
                    await _node_claim(
                        session, "remote-1", "remote-1:100", frozenset({"toolchain-x"})
                    )
                    is None
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_offline_revoked_unknown_and_expired_nodes_get_no_claims(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            await seed_task_run_step(factory, capabilities=["code_edit"])
            caps = frozenset({"code_edit"})
            async with factory.begin() as session:
                assert await _node_claim(session, "ghost", "w:1", caps) is None
            await _register(factory, node_id="off-1")
            async with factory.begin() as session:
                await mark_node_offline(session, node_id="off-1")
                assert await _node_claim(session, "off-1", "w:1", caps) is None
            await _register(factory, node_id="rev-1")
            async with factory.begin() as session:
                await revoke_node(session, node_id="rev-1", detail="key rotation")
                assert await _node_claim(session, "rev-1", "w:1", caps) is None
            await _register(factory, node_id="old-1")
            async with factory.begin() as session:
                await session.execute(
                    update(Node)
                    .where(Node.node_id == "old-1")
                    .values(last_heartbeat_at=func.now() - timedelta(seconds=10_000))
                )
                assert await _node_claim(session, "old-1", "w:1", caps) is None
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_stale_completion_cannot_finish_new_attempt(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            await _register(factory, node_id="local", trust_class="local")
            _, _, step = await seed_task_run_step(factory)
            async with factory.begin() as session:
                first = await _node_claim(session, "local", "local:1", frozenset())
            assert first is not None
            async with factory.begin() as session:
                await session.execute(
                    update(Step)
                    .where(Step.id == step.id)
                    .values(status=StepStatus.QUEUED, lease_owner=None, lease_expires_at=None)
                )
            async with factory.begin() as session:
                second = await _node_claim(session, "local", "local:2", frozenset())
            assert second is not None and second.generation == first.generation + 1
            with pytest.raises(LeaseLost):
                async with factory.begin() as session:
                    await complete_step(session, first, result_payload={"late": True})
            async with factory.begin() as session:
                await complete_step(session, second, result_payload={"ok": True})
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_disconnect_before_and_after_launch(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            await _register(factory, node_id="local", trust_class="local")
            await seed_task_run_step(factory)
            async with factory.begin() as session:
                await mark_node_offline(session, node_id="local")
            # Disconnect before launch: no new claims.
            async with factory.begin() as session:
                assert await _node_claim(session, "local", "local:1", frozenset()) is None
            # Launch while online, then disconnect: in-flight work stays fenced.
            async with factory.begin() as session:
                await register_node(session, node_id="local", trust_class="local")
                token = await _node_claim(session, "local", "local:1", frozenset())
            assert token is not None
            async with factory.begin() as session:
                await mark_node_offline(session, node_id="local")
            async with factory.begin() as session:
                await complete_step(session, token, result_payload={"ok": True})
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_slot_contention_and_stale_release(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            await _register(factory, node_id="local", trust_class="local")
            async with factory.begin() as session:
                first = await claim_slot(
                    session, node_id="local", slot_name="gpu:0", owner="a", lease_seconds=60
                )
            assert first is not None and first.generation == 1
            async with factory.begin() as session:
                assert (
                    await claim_slot(
                        session, node_id="local", slot_name="gpu:0", owner="b", lease_seconds=60
                    )
                    is None
                )
            async with factory.begin() as session:
                await release_slot(session, first)
            async with factory.begin() as session:
                second = await claim_slot(
                    session, node_id="local", slot_name="gpu:0", owner="b", lease_seconds=60
                )
            assert second is not None and second.generation == 2
            # Stale holder cannot release the new generation.
            with pytest.raises(LeaseLost):
                async with factory.begin() as session:
                    await release_slot(session, first)
            async with factory.begin() as session:
                await release_slot(session, second)
                with pytest.raises(LeaseLost):
                    await release_slot(session, second)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_restart_reconciles_slot_before_reuse(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            await _register(factory, node_id="local", trust_class="local")
            async with factory.begin() as session:
                token = await claim_slot(
                    session, node_id="local", slot_name="gpu:0", owner="old", lease_seconds=60
                )
            assert token is not None
            async with factory.begin() as session:
                await session.execute(
                    update(NodeSlot).values(lease_expires_at=func.now() - timedelta(seconds=1))
                )
            async with factory() as session:
                expired = await find_expired_slots(session)
            assert [(row.node_id, row.slot_name) for row in expired] == [("local", "gpu:0")]
            # A new claim alone never reuses the old writer's slot.
            async with factory.begin() as session:
                assert (
                    await claim_slot(
                        session, node_id="local", slot_name="gpu:0", owner="new", lease_seconds=60
                    )
                    is None
                )
                assert (
                    await reconcile_slot(session, node_id="local", slot_name="gpu:0") == "released"
                )
            async with factory.begin() as session:
                retaken = await claim_slot(
                    session, node_id="local", slot_name="gpu:0", owner="new", lease_seconds=60
                )
            assert retaken is not None and retaken.generation == token.generation + 1
            # Live slots are never reconciled away.
            async with factory.begin() as session:
                assert await reconcile_slot(session, node_id="local", slot_name="gpu:0") == "held"
                assert await reconcile_slot(session, node_id="local", slot_name="gpu:9") == "free"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_local_node_registration_and_validation(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            # Fresh DB (per-test truncate): local node onboards like any node.
            # The migration backfill for pre-registry deployments is verified
            # separately via downgrade/upgrade (see result.md).
            async with factory.begin() as session:
                local = await register_node(
                    session, node_id="local", trust_class="local", detail="test"
                )
            assert local.status == "online"
            async with factory.begin() as session:
                row = await register_node(
                    session, node_id="remote-9", trust_class="remote", credential_ref="alias-1"
                )
            assert row.status == "online"
            bad_requests = (
                {"node_id": "remote-9", "trust_class": "nope"},
                {"node_id": "remote-9", "trust_class": "remote", "protocol_version": "v0"},
                {
                    "node_id": "remote-9",
                    "trust_class": "remote",
                    "credential_ref": "Not An Alias!",
                },
                {"node_id": "BAD ID!", "trust_class": "remote"},
            )
            for bad in bad_requests:
                with pytest.raises(ValueError):
                    async with factory.begin() as session:
                        await register_node(session, **bad)  # type: ignore[arg-type]
            with pytest.raises(ValueError):
                async with factory.begin() as session:
                    await heartbeat_node(session, node_id="ghost")
            with pytest.raises(ValueError):
                async with factory.begin() as session:
                    await register_node(
                        session, node_id="remote-9", trust_class="remote", credential_ref=None
                    )
                    await revoke_node(session, node_id="remote-9", detail="compromised")
                    await heartbeat_node(session, node_id="remote-9")
        finally:
            await engine.dispose()

    asyncio.run(scenario())
