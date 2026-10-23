"""PostgreSQL command boundary for human and agent callers. Stage 1 has no worker."""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.integration.storage.helpers import storage

from vuzol.leadership.commands import DELEGATED_ACTIONS, LeadCommand
from vuzol.leadership.persistence import GoalDelegation, LeadershipCommandReceipt, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.storage.models import Event, WorkPackage
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.application import Principal

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]

PROJECT = "demo"
BRANCH = "vuzol-managed/demo/goal"
OWNER = 42


@asynccontextmanager
async def _factory(postgres_dsn: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine, factory = storage(postgres_dsn)
    try:
        yield factory
    finally:
        await engine.dispose()


def _service(
    factory: async_sessionmaker[AsyncSession], *, enabled: bool
) -> LeadershipCommandService:
    return LeadershipCommandService(
        factory,
        enabled=enabled,
        default_branches={PROJECT: "main"},
    )


def _enable(session_id: uuid.UUID, key: str = "enable-1") -> LeadCommand:
    return LeadCommand(
        name="enable_managed_goal",
        project_id=PROJECT,
        idempotency_key=key,
        payload={
            "session_id": str(session_id),
            "integration_branch": BRANCH,
            "request": "ship the marker",
            "scope": "marker file only",
            "criteria": [{"id": "marker", "description": "the marker file exists"}],
            "owner_user_id_is_not_this": True,
            "environments": ["secrets", "production"],
        },
    )


def _mutation(
    name: str,
    goal_id: uuid.UUID,
    version: int,
    key: str,
    *,
    payload: dict[str, object] | None = None,
    reason: str = "",
    project_id: str = PROJECT,
) -> LeadCommand:
    return LeadCommand(
        name=name,
        project_id=project_id,
        goal_id=goal_id,
        expected_version=version,
        idempotency_key=key,
        payload={} if payload is None else payload,
        reason=reason,
    )


async def _session(
    factory: async_sessionmaker[AsyncSession],
    project: str = PROJECT,
    thread_id: int = 10,
) -> uuid.UUID:
    async with UnitOfWork(factory) as uow:
        return await uow.discussions.create_session(
            project_id=project, chat_id=-1001, message_thread_id=thread_id
        )


async def _open(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[uuid.UUID, int, str]:
    session_id = await _session(factory)
    result = await _service(factory, enabled=True).apply_human(
        Principal(user_id=OWNER, ingress_source="telegram"),
        _enable(session_id),
    )
    assert result.status == "applied"
    assert result.goal_id is not None
    assert result.version == 1
    assert result.result["agent_principal_id"] == f"agent:project-lead:{PROJECT}"
    return result.goal_id, result.version, str(result.result["agent_principal_id"])


async def test_human_and_agent_share_one_boundary_and_audit(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id, version, principal = await _open(factory)
        service = _service(factory, enabled=True)
        human = await service.apply_human(
            Principal(user_id=OWNER, ingress_source="cli"),
            _mutation("get_goal", goal_id, version, "read-human"),
        )
        agent = await service.apply_agent(
            principal,
            "worker",
            _mutation("get_capabilities", goal_id, version, "read-agent"),
        )
        assert human.status == "applied"
        assert agent.status == "applied"
        assert "secrets" in agent.result["unsupported"]
        assert "accept_criterion" not in agent.result["actions"]
        async with factory() as session:
            events = (await session.scalars(select(Event).where(Event.entity_id == goal_id))).all()
            delegation = await session.scalar(select(GoalDelegation))
        actors = {(event.actor_type, event.actor_id) for event in events}
        assert ("human", str(OWNER)) in actors
        assert ("agent", principal) in actors
        assert delegation is not None
        assert delegation.owner_user_id == OWNER
        assert delegation.agent_principal_id == principal
        assert set(delegation.allowed_actions) == set(DELEGATED_ACTIONS) - {
            "propose_batch",
            "request_revision",
        }
        assert delegation.environments == ["isolated"]
        assert "accept_criterion" not in delegation.allowed_actions


async def test_denied_paths_leave_no_effect(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        session_id = await _session(factory)
        service = _service(factory, enabled=True)
        human = Principal(user_id=OWNER, ingress_source="telegram")
        opened = await service.apply_human(human, _enable(session_id))
        assert opened.goal_id is not None
        principal = str(opened.result["agent_principal_id"])
        other_project = await service.apply_agent(
            principal,
            "worker",
            _mutation(
                "request_plan",
                opened.goal_id,
                1,
                "other-project",
                project_id="elsewhere",
                payload={"plan": {"title": "nope", "items": []}},
            ),
        )
        side_session = await _session(factory, project="side", thread_id=11)
        protected = await service.apply_human(
            human,
            LeadCommand(
                name="enable_managed_goal",
                project_id="side",
                idempotency_key="prod-branch",
                payload={
                    "session_id": str(side_session),
                    "integration_branch": "vuzol-managed/prod/goal",
                    "request": "prod",
                    "scope": "prod",
                    "criteria": [{"id": "marker", "description": "no"}],
                },
            ),
        )
        identity = await service.apply_agent(
            principal,
            "worker",
            _mutation(
                "pause_goal",
                opened.goal_id,
                1,
                "forged-identity",
                payload={"owner_user_id": OWNER, "allowed_actions": ["reconcile_cost"]},
            ),
        )
        waiver = await service.apply_agent(
            principal,
            "api",
            _mutation("accept_criterion", opened.goal_id, 1, "waiver"),
        )
        human_waiver = await service.apply_human(
            human,
            _mutation("reconcile_cost", opened.goal_id, 1, "human-waiver"),
        )
        assert other_project.status == "denied"
        assert other_project.reason == "outside_scope"
        assert protected.status == "denied"
        assert protected.reason == "protected_branch"
        assert identity.status == "denied"
        assert identity.reason == "caller_supplied_identity"
        assert waiver.status == "denied"
        assert waiver.reason == "owner_decision_required"
        assert human_waiver.status == "denied"
        assert human_waiver.reason == "owner_decision_required"
        async with factory() as session:
            goal = await session.get(ManagedGoal, opened.goal_id)
            packages = await session.scalar(select(func.count()).select_from(WorkPackage))
        assert goal is not None
        assert goal.lifecycle == "active"
        assert goal.version == 1
        assert packages == 0


async def test_revocation_stale_version_retry_and_key_collision(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id, version, principal = await _open(factory)
        service = _service(factory, enabled=True)
        human = Principal(user_id=OWNER, ingress_source="telegram")
        paused = await service.apply_human(
            human, _mutation("pause_goal", goal_id, version, "pause-1", reason="hold")
        )
        assert paused.status == "applied"
        assert paused.version == 2
        again = await service.apply_human(
            human, _mutation("pause_goal", goal_id, version, "pause-1", reason="hold")
        )
        assert again.status == "duplicate"
        assert again.receipt_id == paused.receipt_id
        collision = await service.apply_human(
            human, _mutation("pause_goal", goal_id, version, "pause-1", reason="other")
        )
        assert collision.status == "denied"
        assert collision.reason == "idempotency_conflict"
        stale = await service.apply_agent(
            principal,
            "worker",
            _mutation("cancel_goal", goal_id, version, "stale-cancel"),
        )
        assert stale.status == "stale"
        assert stale.reason == "stale_version"
        revoked = await service.apply_human(
            human, _mutation("revoke_delegation", goal_id, 2, "revoke-1")
        )
        assert revoked.status == "applied"
        after = await service.apply_agent(
            principal,
            "worker",
            _mutation(
                "request_plan",
                goal_id,
                3,
                "after-revoke",
                payload={
                    "plan": {
                        "title": "should not land",
                        "items": [
                            {
                                "summary": "write marker",
                                "goal": "write marker",
                                "expected_outcome": "file",
                                "completion_criteria": ["file exists"],
                                "allowed_scope": "marker",
                            }
                        ],
                    }
                },
            ),
        )
        assert after.status == "denied"
        assert after.reason == "delegation_revoked"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            receipts = (
                await session.scalars(
                    select(LeadershipCommandReceipt).where(
                        LeadershipCommandReceipt.idempotency_key == "pause-1"
                    )
                )
            ).all()
            delegation = await session.scalar(select(GoalDelegation))
        assert goal is not None
        assert goal.lifecycle == "paused"
        assert goal.waiting_reason == "delegation_revoked"
        assert goal.proposed_plan is None
        assert len(receipts) == 1
        assert receipts[0].status == "applied"
        assert delegation is not None
        assert delegation.revoked_at is not None


async def test_feature_off_blocks_agent_and_keeps_human_pause(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id, version, principal = await _open(factory)
        disabled = _service(factory, enabled=False)
        human = Principal(user_id=OWNER, ingress_source="telegram")
        agent = await disabled.apply_agent(
            principal,
            "worker",
            _mutation("start_batch", goal_id, version, "agent-while-off"),
        )
        forged = await disabled.apply_agent(
            "agent:7",
            "cli",
            _mutation("start_batch", goal_id, version, "numeric-agent"),
        )
        paused = await disabled.apply_human(
            human, _mutation("pause_goal", goal_id, version, "human-pause-off")
        )
        enable_off = await disabled.apply_human(
            human,
            _enable(uuid.uuid4(), key="enable-while-off"),
        )
        assert agent.status == "denied"
        assert agent.reason == "feature_disabled"
        assert forged.status == "denied"
        assert forged.reason == "agent_principal_invalid"
        assert forged.receipt_id is None
        assert paused.status == "applied"
        assert paused.result["lifecycle"] == "paused"
        assert enable_off.status == "denied"
        assert enable_off.reason == "feature_disabled"
        async with factory() as session:
            packages = await session.scalar(select(func.count()).select_from(WorkPackage))
            goal = await session.get(ManagedGoal, goal_id)
        assert packages == 0
        assert goal is not None
        assert goal.lifecycle == "paused"


async def test_unbound_start_does_not_create_a_package(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id, version, principal = await _open(factory)
        result = await _service(factory, enabled=True).apply_agent(
            principal,
            "worker",
            _mutation("start_batch", goal_id, version, "start-unbound"),
        )
        assert result.status == "unsupported"
        assert result.reason == "batch_executor_unbound"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            packages = await session.scalar(select(func.count()).select_from(WorkPackage))
        assert goal is not None
        assert goal.version == 1
        assert goal.current_package_id is None
        assert packages == 0
