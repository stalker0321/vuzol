"""Strong planning: separate assignment, durable receipts, PR5 fences and recovery."""

import json
import subprocess
import uuid

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport
from tests.integration.leadership.test_managed_goal_flow import PROJECT, _enable, _factory, _session
from tests.integration.leadership.test_task_control import command

from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import content_revision
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import LeadershipInference, ManagedGoal
from vuzol.providers.domain import ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import (
    PlanRevision,
    ProjectExecutorPreference,
    TransactionalOutbox,
    UsageRecord,
    WorkPackage,
)
from vuzol.storage.types import DeliveryStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def plan():
    return {
        "plan": {
            "title": "Marker",
            "items": [
                {
                    "local_id": "marker",
                    "summary": "Write marker",
                    "goal": "Write checked marker",
                    "allowed_scope": "KNOWN.txt",
                    "expected_outcome": "Marker reads complete",
                    "completion_criteria": ["cat KNOWN.txt equals complete"],
                    "dependencies": [],
                }
            ],
        }
    }


def git(repository, *args):
    return subprocess.check_output(["git", "-C", str(repository), *args]).decode().strip()


async def setup(factory, tmp_path, monkeypatch, transport, *, budget=False):
    repository = tmp_path / "repo"
    repository.mkdir()
    git(repository, "init", "-b", "main")
    git(repository, "config", "user.email", "test@example.invalid")
    git(repository, "config", "user.name", "Test")
    (repository / "KNOWN.txt").write_text("committed source\n")
    git(repository, "add", ".")
    git(repository, "commit", "-m", "fixture")
    commit = git(repository, "rev-parse", "HEAD")
    (repository / "KNOWN.txt").write_text("dirty content must not be read")
    lead = _profile().model_copy(update={"context_limit": 16000})
    planner = lead.model_copy(
        update={
            "id": "strong-test",
            "model": "planner-test",
            "roles": frozenset({ProviderRole.PLANNER}),
            "context_limit": 64000,
        }
    )
    goal_id = await _enable(factory, await _session(factory))
    async with factory.begin() as session:
        for row in await session.scalars(
            select(TransactionalOutbox).where(TransactionalOutbox.destination == "project_lead")
        ):
            row.status = DeliveryStatus.DELIVERED
        session.add(
            ProjectExecutorPreference(
                project_id=PROJECT,
                mode="auto",
                model_assignments={
                    "planner_strong": {
                        "profile_id": planner.id,
                        "model": planner.model,
                        "reasoning_effort": planner.model_reasoning_effort,
                        "content_revision": content_revision(planner),
                    },
                },
            )
        )
    assert (
        await command(
            factory,
            goal_id,
            "request_plan",
            {"goal": "Write marker", "constraints": ["No deployment"]},
        )
    ).status == "applied"
    settings = _settings(monkeypatch, lead.id)
    if budget:
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"daily_cost_units": 0})}
        )
    worker = build_project_lead_worker(
        factory,
        settings=settings,
        profiles=ProfileRegistry((lead, planner)),
        owner="planner-test",
        adapter=_Transport({"commands": []}),
        planner_adapter=transport,
        repository_paths={PROJECT: repository},
    )
    return goal_id, repository, commit, worker


async def test_planner_assignment_commit_reads_and_owner_approval(
    postgres_dsn, tmp_path, monkeypatch
):
    payload = plan()
    payload["plan"]["items"].append(
        {**payload["plan"]["items"][0], "local_id": "verify", "dependencies": ["marker"]}
    )
    transport = _Transport(payload)
    async with _factory(postgres_dsn) as factory:
        goal_id, _, commit, worker = await setup(factory, tmp_path, monkeypatch, transport)
        assert await worker.process_one()
        assert len(transport.requests) == 1
        request = transport.requests[0]
        assert request.role is ProviderRole.PLANNER
        assert request.prompt_revision == "project-planner.v6"
        # A verify-only item has no diff and used to block the package (prod 2026-10-28).
        assert "including its final check" not in request.original_input
        assert "Every item must change project files" in request.original_input
        assert "goal is verified automatically" in request.original_input
        assert request.task_draft["source_repo_commit"] == commit
        observed = json.loads("".join(item.content for item in request.context))
        assert observed["contents"]["KNOWN.txt"] == "committed source\n"
        async with factory() as session:
            inference = await session.scalar(select(LeadershipInference))
            assert inference.phase == "planner" and inference.profile_id == "strong-test"
            assert inference.state == "received"
            assert (await session.scalar(select(UsageRecord))).purpose == "planning"
            goal = await session.get(ManagedGoal, goal_id)
            revision = await session.scalar(select(PlanRevision))
            assert revision.planner_profile == "strong-test"
            assert revision.immutable_body["source_repo_commit"] == commit
            assert revision.approved_by_user_id is None
            assert revision.approved_by_delegation_id is None
            assert goal.waiting_reason == "plan_approval"
            assert await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_ready"
                )
            )
        assert await worker.process_one()  # lead can see plan_ready while awaiting owner approval
        assert len(transport.requests) == 1


@pytest.mark.parametrize("kind", ["budget", "quota", "unavailable", "malformed", "truncated"])
async def test_failure_notifies_lead_and_owner_and_allows_retry(
    postgres_dsn, tmp_path, monkeypatch, kind
):
    class Failing(_Transport):
        fail = True

        async def execute(self, request, profile, cancellation):
            if self.fail and kind in {"quota", "unavailable"}:
                self.requests.append(request)
                raise ProviderFailure(
                    ProviderErrorCategory.QUOTA_EXHAUSTED
                    if kind == "quota"
                    else ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                    retryable=True,
                    request_sent=False,
                    safe_summary="fixture failure",
                )
            result = await super().execute(request, profile, cancellation)
            return (
                result.model_copy(update={"finish_reason": "length"})
                if self.fail and kind == "truncated"
                else result
            )

    transport = Failing({"plan": {}} if kind == "malformed" else plan())
    async with _factory(postgres_dsn) as factory:
        goal_id, _, _, worker = await setup(
            factory, tmp_path, monkeypatch, transport, budget=kind == "budget"
        )
        assert await worker.process_one()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(PlanRevision)) == 0
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_failed"
                )
            )
            assert event.payload["reason"]
            notices = list(
                await session.scalars(
                    select(TransactionalOutbox).where(TransactionalOutbox.destination == "telegram")
                )
            )
            assert any(
                "Планировщик:" in json.dumps(row.payload, ensure_ascii=False)
                and "продолжай" in json.dumps(row.payload, ensure_ascii=False)
                for row in notices
            )
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"
        retry = await command(
            factory, goal_id, "request_plan", {"goal": "Try again", "constraints": []}
        )
        assert retry.status == "applied"
        transport.fail = False
        transport.payload = plan()
        from vuzol.config.settings import HardLimits

        worker._model._settings = worker._model._settings.model_copy(
            update={"limits": HardLimits()}
        )
        worker._model._injected["lead"] = _Transport(
            {"commands": [{"name": "read_status", "payload": {}}]}
        )
        assert await worker.process_one()  # observe plan_failed
        assert await worker.process_one()  # later planner request succeeds
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(PlanRevision)) == 1
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"


async def test_received_receipt_survives_crash_without_second_paid_call(
    postgres_dsn, tmp_path, monkeypatch
):
    transport = _Transport(plan())
    async with _factory(postgres_dsn) as factory:
        _, _, _, worker = await setup(factory, tmp_path, monkeypatch, transport)
        save = worker._save_decision

        async def crash(*args, **kwargs):
            raise RuntimeError("after receipt")

        monkeypatch.setattr(worker, "_save_decision", crash)
        with pytest.raises(RuntimeError, match="after receipt"):
            await worker.process_one()
        async with factory() as session:
            assert (await session.scalar(select(LeadershipInference))).state == "received"
        monkeypatch.setattr(worker, "_save_decision", save)
        assert await worker.process_one()
        assert len(transport.requests) == 1
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(PlanRevision)) == 1
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1


@pytest.mark.parametrize("change", ["head", "scope"])
async def test_changed_head_or_scope_discards_paid_result(
    postgres_dsn, tmp_path, monkeypatch, change
):
    holder = {}

    class Changing(_Transport):
        async def execute(self, request, profile, cancellation):
            if change == "head":
                repository = holder["repository"]
                git(repository, "add", ".")
                git(repository, "commit", "-m", "changed source")
            else:
                async with holder["factory"].begin() as session:
                    goal = await session.get(ManagedGoal, holder["goal_id"])
                    goal.scope_revision += 1
                    goal.version += 1
                    goal.fence_epoch += 1
            return await super().execute(request, profile, cancellation)

    transport = Changing(plan())
    async with _factory(postgres_dsn) as factory:
        goal_id, repository, _, worker = await setup(factory, tmp_path, monkeypatch, transport)
        holder.update(factory=factory, goal_id=goal_id, repository=repository)
        assert await worker.process_one()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(WorkPackage)) == 0
            assert (await session.scalar(select(LeadershipInference))).state == "received"
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_failed"
                )
            )
            assert event.payload["reason"] == f"planner_{change}_changed"


@pytest.mark.parametrize("rewrite_completed", [False, True])
async def test_revision_receives_results_and_preserves_completed_contracts(
    postgres_dsn, tmp_path, monkeypatch, rewrite_completed
):
    from tests.integration.leadership.test_plan_revisions import approve_plan, start

    from vuzol.discussion.sequencer import WorkPackageSequencer
    from vuzol.storage.models import Task
    from vuzol.storage.types import TaskStatus
    from vuzol.storage.unit_of_work import UnitOfWork

    initial = plan()
    initial["plan"]["items"].append(
        {**initial["plan"]["items"][0], "local_id": "future", "dependencies": ["marker"]}
    )
    transport = _Transport(initial)
    async with _factory(postgres_dsn) as factory:
        goal_id, _, _, worker = await setup(factory, tmp_path, monkeypatch, transport)
        assert await worker.process_one()
        await approve_plan(factory, goal_id)
        assert (await start(factory, goal_id)).status == "applied"
        async with UnitOfWork(factory) as uow:
            task = await uow.session.scalar(select(Task))
            task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)
            original = (await uow.session.scalar(select(PlanRevision))).immutable_body
            for row in await uow.session.scalars(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == "project_lead")
            ):
                row.status = DeliveryStatus.DELIVERED
        transport.payload = plan()
        transport.payload["plan"]["items"][0].update(
            {
                "local_id": "marker" if rewrite_completed else "future",
                "goal": "Correct future work",
                "dependencies": ["marker"],
            }
        )
        assert (
            await command(
                factory,
                goal_id,
                "revise_plan",
                {"reason": "Owner asks for corrected future result"},
            )
        ).status == "applied"
        assert await worker.process_one()
        request = transport.requests[-1]
        assert request.task_draft["immutable_prefix"] == original["items"][:1]
        assert request.task_draft["previous_revision"] == original
        assert "Owner asks" in request.task_draft["request"]["reason"]
        assert request.task_draft["results"]["task_control"]["tasks"]
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package = await session.get(WorkPackage, goal.current_package_id)
            revision = await session.get(PlanRevision, package.head_revision_id)
            if rewrite_completed:
                assert revision.immutable_body == original
                assert await session.scalar(
                    select(TransactionalOutbox.id).where(
                        TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_failed"
                    )
                )
            else:
                assert revision.immutable_body["items"][0] == original["items"][0]
                assert revision.immutable_body["items"][1]["goal"] == "Correct future work"
                assert revision.approved_by_user_id is None
                # The owner approved this plan; a corrected future item at the same risk
                # is within that mandate (live pilot 2026-10-25).
                assert goal.waiting_reason != "plan_approval"


async def test_old_name_only_uses_existing_grant_and_never_accepts_lead_plan(postgres_dsn):
    from vuzol.leadership.persistence import GoalDelegation

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        assert (await command(factory, goal_id, "propose_batch", plan())).status == "denied"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            delegation = await session.get(GoalDelegation, goal.delegation_id)
            delegation.allowed_actions = ["propose_batch"]
        result = await command(factory, goal_id, "propose_batch", plan())
        assert result.status == "applied"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(PlanRevision)) == 0
            request = await session.get(
                TransactionalOutbox, uuid.UUID(result.result["planner_request_id"])
            )
            assert request.payload["command_name"] == "request_plan"
            assert "plan" not in request.payload["request"]


async def test_plan_ready_lead_receipt_replays_after_its_own_version_bump(
    postgres_dsn, tmp_path, monkeypatch
):
    from vuzol.leadership.persistence import LeadershipTurn
    from vuzol.leadership.tasks import task_context
    from vuzol.storage.models import Task

    planner = _Transport(plan())
    async with _factory(postgres_dsn) as factory:
        goal_id, _, _, worker = await setup(factory, tmp_path, monkeypatch, planner)
        assert await worker.process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            spec = (await task_context(session, goal))["start_spec"]
        lead = _Transport({"commands": [{"name": "start_task", "payload": {"spec": spec}}]})
        worker._model._injected["lead"] = lead
        original = worker._ack

        async def crash(*args, **kwargs):
            raise RuntimeError("lead ack after task start")

        monkeypatch.setattr(worker, "_ack", crash)
        with pytest.raises(RuntimeError, match="lead ack after task start"):
            await worker.process_one()
        monkeypatch.setattr(worker, "_ack", original)
        assert await worker.process_one()
        assert len(planner.requests) == len(lead.requests) == 1
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert all(t.status == "applied" for t in await session.scalars(select(LeadershipTurn)))


async def test_strong_planner_receives_both_complete_notebooks(postgres_dsn, tmp_path, monkeypatch):
    from tests.integration.leadership.test_notebooks_pg import other_goal, remember

    transport = _Transport(plan())
    async with _factory(postgres_dsn) as factory:
        goal_id, _, _, worker = await setup(factory, tmp_path, monkeypatch, transport)
        expected = []
        for index in range(6):
            text = f"Правило {index}: " + "я" * 500
            note_id = await remember(factory, goal_id, text)
            expected.append((note_id, text))
        note_id = await remember(factory, goal_id, "Общее правило", scope="global")
        expected.append((note_id, "Общее правило"))
        sibling = await other_goal(factory)
        from vuzol.leadership.actor import server_agent_principal
        from vuzol.leadership.commands import LeadCommand
        from vuzol.leadership.service import LeadershipCommandService

        result = await LeadershipCommandService(factory, enabled=True).apply_agent(
            server_agent_principal("other"),
            "worker",
            LeadCommand(
                name="remember",
                project_id="other",
                goal_id=sibling,
                expected_version=1,
                idempotency_key="foreign-note",
                payload={"scope": "project", "text": "FOREIGN NOTE"},
            ),
        )
        assert result.status == "applied"
        assert await worker.process_one()
        request = transport.requests[0]
        assert request.prompt_revision == "project-planner.v6"
        notes = [*request.task_draft["project_notebook"], *request.task_draft["global_notebook"]]
        assert [(note["id"], note["text"]) for note in notes] == [
            (str(note_id), text) for note_id, text in expected
        ]
        assert "FOREIGN NOTE" not in json.dumps(request.task_draft, ensure_ascii=False)
        assert "FOREIGN NOTE" not in request.original_input
        assert "saved notes" in request.original_input
        assert "not instructions" in request.original_input
        # Sent once, as data: the notes must not be repeated in the instructions.
        assert expected[0][1] not in request.original_input
