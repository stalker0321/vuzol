"""Owner cost grants preserve independent limits and apply defaults only at creation."""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from vuzol.config.settings import ProjectLeadSettings, Settings
from vuzol.leadership.actor import Actor
from vuzol.leadership.commands import LeadCommandName
from vuzol.leadership.events import live_or_draft
from vuzol.leadership.persistence import GoalDelegation, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.leadership.telegram_commands import _command
from vuzol.telegram.layout import parse_lead_command
from vuzol.workflows.application import Principal


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize(
    "argument,patch",
    [
        ("cost 1.5", {"max_cost": 1.5}),
        ("cost unlimited", {"max_cost": None}),
        ("3 4", {"max_packages": 3, "max_items_per_package": 4}),
    ],
)
@pytest.mark.anyio
async def test_telegram_cost_and_legacy_grants_merge_with_existing_limits(argument, patch):
    kind, parsed = parse_lead_command("/lead limits " + argument)
    assert kind == "limits"
    goal = ManagedGoal(
        id=uuid.uuid4(),
        version=1,
        fence_epoch=1,
        scope_revision=1,
        lifecycle="active",
        constraints={"work_limits": {"max_cost": 2.0, "max_attempts": 7, "max_packages": 2}},
    )
    command = _command(
        kind=kind,
        argument=parsed,
        project_id="p",
        goal_id=goal.id,
        version=goal.version,
        update=SimpleNamespace(bot_id="bot", update_id=1),
    )
    assert command is not None and command.payload["work_limits"] == patch
    expected = {**goal.constraints["work_limits"], "max_items_per_package": None, **patch}
    result = await LeadershipCommandService(AsyncMock(), enabled=True)._perform(
        SimpleNamespace(session=AsyncMock()),
        Actor.from_principal(Principal(user_id=42, ingress_source="telegram")),
        LeadCommandName.SET_WORK_LIMITS,
        command,
        goal,
        GoalDelegation(),
    )
    assert result.status.value == "applied"
    assert goal.constraints["work_limits"] == result.result["work_limits"] == expected
    assert goal.lifecycle == "active"


def test_legacy_underscore_command_keeps_working():
    assert parse_lead_command("/lead_limits 2 3") == ("limits", "2 3")


@pytest.mark.parametrize("argument", ["cost nope", "cost -1", "cost nan", "cost inf"])
def test_invalid_cost_is_rejected(argument):
    assert (
        _command(
            kind="limits",
            argument=argument,
            project_id="p",
            goal_id=uuid.uuid4(),
            version=1,
            update=SimpleNamespace(bot_id="bot", update_id=1),
        )
        is None
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "default,requested,expected",
    [
        (None, {}, None),
        (1.5, {}, 1.5),
        (1.5, {"max_cost": 0.7}, 0.7),
        (1.5, {"max_cost": None}, None),
        (0.0, {"max_packages": 3}, 0.0),
    ],
)
async def test_new_goal_cost_default_respects_explicit_request(default, requested, expected):
    settings = ProjectLeadSettings(default_goal_max_cost=default)
    session = AsyncMock()
    session.scalar.return_value = None
    session.add = Mock()
    goal = await live_or_draft(
        session,
        project_id="p",
        owner_user_id=42,
        discussion=SimpleNamespace(id=uuid.uuid4(), chat_id=1, message_thread_id=1),
        text="task",
        scope="task",
        criteria=[],
        constraints={"work_limits": requested},
        branch_prefix="managed/",
        default_goal_max_cost=settings.default_goal_max_cost,
    )
    assert goal.constraints["work_limits"]["max_cost"] == expected
    if "max_packages" in requested:
        assert goal.constraints["work_limits"]["max_packages"] == requested["max_packages"]
    # Reusing an existing live goal never rewrites its owner grant.
    session.scalar.return_value = goal
    before = dict(goal.constraints)
    assert (
        await live_or_draft(
            session,
            project_id="p",
            owner_user_id=42,
            discussion=SimpleNamespace(),
            text="later message",
            scope="later message",
            criteria=[],
            constraints={},
            branch_prefix="managed/",
            default_goal_max_cost=99.0,
        )
        is goal
    )
    assert goal.constraints == before


def test_composition_passes_default_to_terminal_and_handoff_admission():
    from vuzol.config.registries import ProfileRegistry
    from vuzol.leadership.composition import build_project_lead_worker

    settings = Settings(_env_file=None, project_lead={"default_goal_max_cost": 1.5})
    worker = build_project_lead_worker(
        AsyncMock(), settings=settings, profiles=ProfileRegistry(()), owner="test"
    )
    assert worker._default_goal_max_cost == 1.5
    assert worker._service()._default_goal_max_cost == 1.5


@pytest.mark.anyio
@pytest.mark.parametrize("requested", [{}, {"max_cost": None}, {"max_cost": 0.5}])
async def test_assistant_submission_preserves_absent_versus_explicit_cost(monkeypatch, requested):
    from contextlib import asynccontextmanager

    from vuzol.leadership.access import ProjectAccess, ProjectApplication, WorkRequest
    from vuzol.leadership.events import EventReceipt

    @asynccontextmanager
    async def uow(_factory):
        yield SimpleNamespace()

    submit = AsyncMock(return_value=EventReceipt("accepted", None, None, None))
    monkeypatch.setattr("vuzol.leadership.access.UnitOfWork", uow)
    monkeypatch.setattr(
        "vuzol.leadership.access.ProjectEventService",
        lambda _: SimpleNamespace(submit_event=submit),
    )
    runtime = Mock()
    runtime.registries.projects.get.return_value = SimpleNamespace(id="p")
    app = ProjectApplication(runtime, AsyncMock())
    app._project = Mock()
    await app.submit(
        ProjectAccess(user_id=42, projects={"p": uuid.uuid4()}, allow_submit=True),
        WorkRequest(project_id="p", request_id=uuid.uuid4(), request="task", work_limits=requested),
    )
    assert submit.call_args.kwargs["work_limits"] == requested
