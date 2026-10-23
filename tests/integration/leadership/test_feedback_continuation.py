"""R8/R9: actual provider feedback and durable, bounded next-turn admission."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from tests.integration.leadership import test_managed_goal_flow as flow
from tests.integration.leadership.test_lead_surface import _profile, _settings

from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.leadership import continuation, provider_model
from vuzol.leadership import worker as worker_module
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.model import LeadDecision
from vuzol.leadership.persistence import (
    LeadershipCommandReceipt,
    LeadershipTurn,
    ManagedGoal,
    ManagedGoalEvidence,
)
from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus
from vuzol.storage.models import Step, TransactionalOutbox, WorkPackage
from vuzol.storage.types import (
    DeliveryStatus,
    StepStatus,
    WorkPackagePauseReason,
    WorkPackageStatus,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_failed_review_explanation_reaches_lead(postgres_dsn, tmp_path, monkeypatch):
    """Original independent R8 probe; inspect real provider inputs, not a getter."""
    prompts = []
    original = provider_model._prompt

    def capture(snapshot):
        prompt = original(snapshot)
        prompts.append(prompt)
        return prompt

    monkeypatch.setattr(provider_model, "_prompt", capture)
    await flow.run_managed_goal_flow(
        postgres_dsn,
        tmp_path,
        monkeypatch,
        request="Implement working OAuth login",
        expected_completed=False,
    )
    async with flow._factory(postgres_dsn) as factory, factory() as session:
        evidence = list(await session.scalars(select(ManagedGoalEvidence)))
        reasons = [
            row.observation["verdict"]["explanation"]
            for row in evidence
            if row.checker == "goal_review_v2" and not row.satisfied
        ]
    assert reasons
    assert any(reason in prompt for reason in reasons for prompt in prompts)


async def test_valid_proposal_only_response_has_a_continuation(postgres_dsn):
    """Original independent R9 probe, with the successful continuation also asserted."""
    proposal = flow._start_decision().commands[0]
    accepted = provider_model.parse_lead_response(
        {
            "commands": [
                {"name": proposal.name, "payload": proposal.payload, "reason": "Prepare next batch"}
            ]
        }
    )

    class Model:
        calls = 0

        async def decide(self, snapshot):
            self.calls += 1
            return accepted if self.calls == 1 else flow._start_decision()

    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        model = Model()
        worker = flow._worker(factory, model)
        await worker.process_one()
        for _ in range(4):
            await worker.process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.proposed_plan is not None
            state = goal.lifecycle
        assert model.calls > 1 or state != "active" or await flow._package_count(factory) > 0
        assert model.calls == 2
        assert await flow._package_count(factory) == 1


def context_of(request):
    return json.loads(request.original_input.split("\nresult_context: ", 1)[1])


def response(payload):
    return ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        structured_output=payload,
        usage=NormalizedUsage(duration_ms=1),
        adapter_version="feedback-fixture",
    )


async def pending(factory):
    async with factory() as session:
        return list(
            await session.scalars(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "project_lead",
                    TransactionalOutbox.status == DeliveryStatus.PENDING,
                )
            )
        )


async def test_review_drives_a_corrective_package_through_actual_provider_inputs(
    postgres_dsn, tmp_path, monkeypatch
):
    reason = "SECOND.txt is missing. Add SECOND.txt with exactly marker-b followed by a newline."
    prompts, decisions = [], []
    original_worker = flow._provider_worker

    class Transport:
        async def execute(self, request, profile, cancellation):
            if request.role is ProviderRole.REVIEWER:
                body = json.loads(request.original_input.split("\n", 1)[1])
                good = body["files"].get("SECOND.txt") == "marker-b\n"
                return response(
                    {
                        "result_commit": body["result_commit"],
                        "criteria": [
                            {
                                "criterion_id": "requested-outcome",
                                "verdict": "pass" if good else "insufficient",
                                "explanation": "Both requested markers are implemented."
                                if good
                                else reason,
                                "evidence_paths": ["MARKER.txt", "SECOND.txt"]
                                if good
                                else ["MARKER.txt"],
                            }
                        ],
                    }
                )
            context = context_of(request)
            prompts.append(context)
            if context["binding"]["package_id"] is None:
                commands = [
                    {"name": draft.name, "payload": draft.payload}
                    for draft in flow._start_decision().commands
                ]
            elif context["criterion_reviews"][0]["verdict"] == "pass":
                commands = [{"name": "propose_completion", "payload": {}}]
            else:
                critique = context["criterion_reviews"][0]
                assert critique["explanation"] == reason
                assert critique["result_commit"] == context["binding"]["result_commit"]
                assert context["executed_plan"]["plan"]["title"] == "Package A"
                assert any(
                    step["implementation_summary"] == "Wrote the marker."
                    for step in context["steps"]
                )
                if context["proposed_plan"]["title"] == "Add the missing SECOND marker":
                    commands = [
                        {
                            "name": "start_task",
                            "payload": {"spec": context_of(request)["task_control"]["start_spec"]},
                        }
                    ]
                else:
                    commands = [
                        {
                            "name": "propose_batch",
                            "payload": {
                                "plan": flow._plan(
                                    "Add the missing SECOND marker",
                                    reason,
                                    ["SECOND.txt contains marker-b"],
                                    "SECOND.txt",
                                )
                            },
                            "reason": f"Address evidence {critique['evidence_id']}: {reason}",
                        }
                    ]
            decisions.append(commands)
            return response({"commands": commands})

    transport = Transport()
    monkeypatch.setattr(
        flow,
        "_provider_worker",
        lambda factory, settings, *, owner: original_worker(
            factory, settings, owner=owner, transport=transport
        ),
    )
    await flow.run_managed_goal_flow(
        postgres_dsn, tmp_path, monkeypatch, split_corrective_turn=True
    )
    assert len(prompts) == 4
    assert [c["name"] for c in decisions[1]] == ["propose_batch"]
    assert [c["name"] for c in decisions[2]] == ["start_task"]
    assert prompts[1]["criterion_reviews"][0]["explanation"] == reason
    assert prompts[-1]["criterion_reviews"][0]["verdict"] == "pass"
    assert prompts[1]["binding"]["result_commit"] != prompts[-1]["binding"]["result_commit"]
    async with flow._factory(postgres_dsn) as factory, factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(LeadershipCommandReceipt)
                .where(LeadershipCommandReceipt.command_name == "answer_permission")
            )
            == 0
        )
        turns = list(await session.scalars(select(LeadershipTurn)))
        assert any(reason in json.dumps(turn.decision) for turn in turns)
    print(
        "FEEDBACK_TRACE "
        + json.dumps(
            {
                "negative": prompts[1]["criterion_reviews"][0],
                "next_plan": decisions[1][0]["payload"]["plan"],
                "start": decisions[2],
                "positive": prompts[-1]["criterion_reviews"][0],
            }
        )
    )


def provider_worker(factory, settings, transport, owner="feedback-provider"):
    return build_project_lead_worker(
        factory,
        settings=settings,
        profiles=ProfileRegistry((_profile().model_copy(update={"context_limit": 16000}),)),
        owner=owner,
        adapter=transport,
        default_branches={flow.PROJECT: "main"},
    )


async def test_provider_can_propose_then_start_in_a_fresh_worker(postgres_dsn, monkeypatch):
    class Transport:
        calls = 0

        async def execute(self, request, profile, cancellation):
            self.calls += 1
            context = context_of(request)
            if self.calls == 1:
                assert context["proposed_plan"] is None
                draft = flow._start_decision().commands[0]
                return response({"commands": [{"name": draft.name, "payload": draft.payload}]})
            assert context["proposed_plan"]["title"] == "Package A"
            return response(
                {
                    "commands": [
                        {
                            "name": "start_task",
                            "payload": {"spec": context_of(request)["task_control"]["start_spec"]},
                        }
                    ]
                }
            )

    settings, transport = _settings(monkeypatch, "lead-local"), Transport()
    async with flow._factory(postgres_dsn) as factory:
        await flow._enable(factory, await flow._session(factory))
        assert await provider_worker(factory, settings, transport).process_one()
        wakeups = await pending(factory)
        assert len(wakeups) == 1 and wakeups[0].payload["wakeup_kind"] == "continuation"
        restarted = provider_worker(factory, settings, transport, owner="restarted")
        assert await restarted.process_one()
        assert await flow._package_count(factory) == 1
        assert not await restarted.process_one()
        assert transport.calls == 2
        assert await pending(factory) == []


@pytest.mark.parametrize("change_wording", [False, True])
async def test_repeated_proposals_park_with_a_real_question(postgres_dsn, change_wording):
    class Model:
        calls = 0

        async def decide(self, snapshot):
            self.calls += 1
            draft = flow._start_decision().commands[0]
            if change_wording:
                draft.payload["plan"]["title"] = f"Renamed proposal {self.calls}"
            return LeadDecision(commands=(draft,))

    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        model = Model()
        worker = flow._worker(factory, model, repeat_limit=2)
        for _ in range(8):
            await worker.process_one()
        assert model.calls == 3
        assert await flow._package_count(factory) == 0
        assert await pending(factory) == []
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "waiting"
            assert goal.waiting_reason == "continuation_without_progress"
            assert goal.wakeup_condition["kind"] == "owner"
            assert goal.wakeup_condition["asked_version"] == goal.version


@pytest.mark.parametrize("point", ["before_ack", "after_enqueue", "after_commit"])
async def test_continuation_is_atomic_with_ack_and_replay_safe(postgres_dsn, monkeypatch, point):
    class Model:
        calls = 0

        async def decide(self, snapshot):
            self.calls += 1
            if self.calls == 1:
                return LeadDecision(commands=(flow._start_decision().commands[0],))
            return provider_model.parse_lead_response(
                {
                    "commands": [
                        {
                            "name": "start_task",
                            "payload": {
                                "spec": snapshot.result_context["task_control"]["start_spec"]
                            },
                        }
                    ]
                }
            )

    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        model = Model()
        worker = flow._worker(factory, model, owner="crashed")
        original_ack, original_enqueue = worker._ack, continuation.enqueue_goal_wakeup
        raised = False

        async def crash_ack(*args, **kwargs):
            nonlocal raised
            if not raised and point == "before_ack":
                raised = True
                raise RuntimeError("injected crash")
            await original_ack(*args, **kwargs)
            if not raised and point == "after_commit":
                raised = True
                raise RuntimeError("injected crash")

        async def crash_enqueue(*args, **kwargs):
            nonlocal raised
            await original_enqueue(*args, **kwargs)
            if not raised and point == "after_enqueue":
                raised = True
                raise RuntimeError("injected crash")

        monkeypatch.setattr(worker, "_ack", crash_ack)
        monkeypatch.setattr(continuation, "enqueue_goal_wakeup", crash_enqueue)
        with pytest.raises(RuntimeError, match="injected crash"):
            await worker.process_one()
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.version == 2
            # Simulate process death/lease expiry, never repair business state.
            goal.lease_until = datetime.now(UTC) - timedelta(seconds=1)
            continuations = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key.like("%:continue:%")
                    )
                )
            )
            assert len(continuations) == (1 if point == "after_commit" else 0)
        restarted = flow._worker(factory, model, owner="replacement")
        for _ in range(4):
            await restarted.process_one()
        assert model.calls == 2
        assert await flow._package_count(factory) == 1
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            receipts = list(
                await session.scalars(
                    select(LeadershipCommandReceipt)
                    .where(LeadershipCommandReceipt.actor_kind == "agent")
                    .order_by(LeadershipCommandReceipt.created_at)
                )
            )
            assert goal.version == 3
            assert [(r.command_name, r.expected_version) for r in receipts] == [
                ("propose_batch", 1),
                ("start_task", 2),
            ]
            continuations = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key.like("%:continue:%")
                    )
                )
            )
            assert len(continuations) == 1
            assert continuations[0].status is DeliveryStatus.DELIVERED


async def test_reconciler_recovers_a_pre_fix_orphan_without_reopening_delivered_rows(
    postgres_dsn, monkeypatch
):
    class Proposal:
        async def decide(self, snapshot):
            return LeadDecision(commands=(flow._start_decision().commands[0],))

    class Start:
        calls = 0

        async def decide(self, snapshot):
            self.calls += 1
            return provider_model.parse_lead_response(
                {
                    "commands": [
                        {
                            "name": "start_task",
                            "payload": {
                                "spec": snapshot.result_context["task_control"]["start_spec"]
                            },
                        }
                    ]
                }
            )

    async def old_behavior(*args, **kwargs):
        pass

    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        with monkeypatch.context() as patched:
            patched.setattr(worker_module, "continue_if_idle", old_behavior)
            await flow._worker(factory, Proposal()).process_one()
        assert await pending(factory) == []
        async with factory() as session:
            enabled = await session.scalar(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == "project_lead")
            )
            assert enabled.status is DeliveryStatus.DELIVERED
            original_id = enabled.id
        model = Start()
        workers = [flow._worker(factory, model, owner=f"reconcile-{i}") for i in range(2)]
        barrier = asyncio.Barrier(2)
        claim = worker_module.claim_outbox_item

        async def concurrent_empty_claim(*args, **kwargs):
            token = await claim(*args, **kwargs)
            assert token is None
            await asyncio.wait_for(barrier.wait(), 5)
            return token

        with monkeypatch.context() as patched:
            patched.setattr(worker_module, "claim_outbox_item", concurrent_empty_claim)
            assert await asyncio.gather(*(w.process_one() for w in workers)) == [False, False]
        assert len(await pending(factory)) == 1
        assert await workers[0].process_one()
        assert model.calls == 1
        assert await flow._package_count(factory) == 1
        async with factory() as session:
            assert (
                await session.get(TransactionalOutbox, original_id)
            ).status is DeliveryStatus.DELIVERED
            assert (await session.get(ManagedGoal, goal_id)).version == 3


@pytest.mark.parametrize("command", ["pause_goal", "revoke_delegation"])
async def test_queued_continuation_obeys_owner_stop(postgres_dsn, command):
    from tests.integration.leadership.test_lead_review_regressions import control

    class Proposal:
        calls = 0

        async def decide(self, snapshot):
            self.calls += 1
            return LeadDecision(commands=(flow._start_decision().commands[0],))

    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        model = Proposal()
        worker = flow._worker(factory, model)
        await worker.process_one()
        assert len(await pending(factory)) == 1
        assert (await control(factory, goal_id, command)).status == "applied"
        for _ in range(3):
            await worker.process_one()
        assert model.calls == 1
        assert await flow._package_count(factory) == 0
        assert await pending(factory) == []


async def test_feedback_is_scoped_bounded_and_contains_structured_failures(
    postgres_dsn, tmp_path, monkeypatch
):
    from vuzol.leadership.feedback import MAX_CONTEXT_BYTES, result_context

    await flow.run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    async with flow._factory(postgres_dsn) as factory:
        for drift in ("scope", "description", "package", "head"):
            async with factory() as session:
                goal = await session.scalar(select(ManagedGoal))
                package = await session.get(WorkPackage, goal.current_package_id)
                if drift == "scope":
                    goal.scope_revision += 1
                elif drift == "description":
                    goal.criteria = [{"id": "requested-outcome", "description": "Different goal"}]
                elif drift == "package":
                    package = None
                else:
                    package.integration_head_commit = "d" * 40
                # Fault injection in a rolled-back test transaction, not a runtime repair.
                context = await result_context(session, goal, package)
                assert context["criterion_reviews"] == []
                assert context["stale_evidence_count"] > 0
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            package = await session.get(WorkPackage, goal.current_package_id)
            package.status = WorkPackageStatus.PAUSED
            package.pause_reason = WorkPackagePauseReason.ITEM_BLOCKED
            step = await session.scalar(
                select(Step)
                .where(Step.step_type == "validate")
                .order_by(Step.created_at.desc())
                .limit(1)
            )
            step.status = StepStatus.BLOCKED
            step.failure_category = "required_runtime_missing"
            step.failure_summary = "The WASM runtime is missing; request the owner's decision."
            step.result = {
                "summary": "я" * 200_000,
                "owner_user_id": "NEVER_COPY_OWNER",
                "credentials": "NEVER_COPY_CREDENTIALS",
                "structured_output": {
                    "result_commit": "a" * 40,
                    "findings": [{"description": "WASM validation is unavailable."}],
                    "secret": "NEVER_COPY_SECRET",
                },
            }
            await session.flush()  # Session factory disables autoflush; project stored outcomes.
            context = await result_context(session, goal, package)
            assert context["package"]["pause_reason"] == "item_blocked"
            problem = next(item for item in context["steps"] if item["step_id"] == str(step.id))
            assert problem["failure"]["category"] == "required_runtime_missing"
            assert "WASM runtime" in problem["failure"]["summary"]
            assert (
                problem["review"]["findings"][0]["description"] == "WASM validation is unavailable."
            )
            assert problem["review"]["binding"] == "historical_commit"
            encoded = json.dumps(context, ensure_ascii=False)
            assert len(encoded.encode()) <= MAX_CONTEXT_BYTES
            assert "truncated" in problem["summary"]
            assert "NEVER_COPY" not in encoded
