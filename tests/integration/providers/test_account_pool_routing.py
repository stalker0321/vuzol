"""Pinned work follows its model within an explicit subscription account pool."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from tests.integration.providers._test_routing_helpers import (
    bundle,
    pin_project,
    profile,
    seed_provider_step,
)
from tests.integration.providers.test_model_assignments import _pin
from tests.integration.storage.helpers import storage
from vuzol.config.models import Capability, ProviderRole
from vuzol.config.revision import content_revision
from vuzol.projects.model_matrix import MATRIX_KEY, ModelAssignment, assignment_profile
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.models import (
    ProjectExecutorPreference,
    RoutingDecision,
    Run,
    Step,
    SubscriptionLimitSnapshotRow,
    Task,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def account_profile(tmp_path, account, *, reviewer=False):
    state = tmp_path / account
    state.mkdir(exist_ok=True)
    return profile(
        f"pi-opencode-go-{account}-{'mimo' if reviewer else 'deepseek'}",
        provider="pi",
        model="opencode-go/mimo-v2.5-pro" if reviewer else "opencode-go/deepseek-v4-pro",
        account_id=account,
        account_pool="opencode-go",
        launch_mode="cli",
        api_base_url=None,
        runtime_identity=f"account-{account}",
        state_directory=state,
        sandbox_required=True,
        supported_task_types=frozenset({"coding"}),
        roles=frozenset({ProviderRole.REVIEWER if reviewer else ProviderRole.EXECUTOR}),
        capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
    )


def missing_entitlement(configured):
    return SubscriptionLimitSnapshotRow(
        profile_id=configured.id,
        company="OpenCode",
        plan_label="Go",
        ok=False,
        detail="go_entitlement_missing",
        observed_at=datetime.now(UTC),
    )


@pytest.mark.parametrize(
    ("source", "case"),
    [
        ("matrix", "unusable"),
        ("agent", "unusable"),
        ("matrix", "metered"),
        ("matrix", "healthy"),
        ("matrix", "busy_sibling"),
        ("matrix", "unpooled_pin"),
        ("matrix", "wrong_model"),
        ("review", "unusable"),
        ("default", "unusable"),
        ("legacy", "unusable"),
    ],
)
async def test_pinned_work_account(postgres_dsn, tmp_path, source, case):
    engine, factory = storage(postgres_dsn)
    try:
        pinned = account_profile(tmp_path, "c", reviewer=source == "review")
        sibling = account_profile(tmp_path, "a", reviewer=source == "review")
        if case == "metered":
            sibling = sibling.model_copy(update={"account_pool": None})
        if case == "unpooled_pin":
            pinned = pinned.model_copy(update={"account_pool": None})
        if case == "wrong_model":
            sibling = sibling.model_copy(update={"model": "other-model"})
        if source == "legacy":
            pinned = pinned.model_copy(update={"provider": "codex", "model": "gpt-6.1-sol"})
            sibling = sibling.model_copy(update={"provider": "codex", "model": "gpt-6.1-sol"})
        settings, registries = bundle(tmp_path, pinned, sibling, projects=(pin_project(),))
        if source == "default":
            settings = settings.model_copy(
                update={
                    "execution": settings.execution.model_copy(
                        update={
                            "default_executor_profile_id": pinned.id,
                        }
                    ),
                }
            )
        step_type = (
            "review"
            if source == "review"
            else ("execute_agent" if source == "agent" else "execute_code")
        )
        task_id, run_id, step_id = await seed_provider_step(
            factory,
            step_type=step_type,
            capabilities=["repository_read"],
        )
        slot = "reviewer_light" if source == "review" else "executor"
        assignments = {slot: _pin(pinned)} if source not in {"default", "legacy"} else {}
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            task.task_type = "coding"
            task.project_id = "bill-buddy"
            run = await session.get(Run, run_id)
            run.selected_route = {MATRIX_KEY: assignments} if source != "legacy" else {}
            session.add(
                ProjectExecutorPreference(
                    project_id="bill-buddy",
                    model_assignments=assignments,
                    mode="pin" if source == "legacy" else "auto",
                    worker_key="sol" if source == "legacy" else None,
                    profile_id=pinned.id if source == "legacy" else None,
                )
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            if case != "healthy":
                session.add(missing_entitlement(pinned))
            if case == "busy_sibling":
                busy = await session.get(Step, step_id)
                busy.executor_profile_id = sibling.id
                busy.status = "running"
                busy.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        if case == "busy_sibling":
            _, _, step_id = await seed_provider_step(
                factory, step_type=step_type, capabilities=["repository_read"]
            )
            async with factory.begin() as session:
                step = await session.get(Step, step_id)
                run = await session.get(Run, step.run_id)
                run.selected_route = {MATRIX_KEY: assignments}
                task = await session.get(Task, run.task_id)
                task.task_type = "coding"
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="pool-test",
                lease_seconds=60,
                candidate_limit=20,
            )
        expected = sibling if case == "unusable" else pinned
        async with factory() as session:
            step = await session.get(Step, step_id)
            decision = await session.scalar(
                select(RoutingDecision).where(RoutingDecision.step_id == step_id)
            )
            assert token is not None
            assert step.executor_profile_id == expected.id
            assert decision.selected_profile_id == expected.id
            if assignments:
                launch = ModelAssignment.model_validate(step.payload["project_model_assignment"])
                assert assignment_profile(launch, registries, slot) == expected
            preference = await session.get(ProjectExecutorPreference, "bill-buddy")
            assert preference.model_assignments == assignments
            assert preference.revision == 1
            if source == "legacy":
                assert preference.profile_id == pinned.id
            original_run = await session.get(Run, run_id)
            if source != "legacy":
                assert original_run.selected_route == {MATRIX_KEY: assignments}
    finally:
        await engine.dispose()


@pytest.mark.parametrize("pin_source", ["matrix", "default", "previous-cli"])
async def test_independent_reviewer_account(postgres_dsn, tmp_path, pin_source):
    from vuzol.execution.domain import GitInspection
    from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus
    from vuzol.review.independent import DatabaseReviewAccounting, IndependentModelReviewer
    from vuzol.storage.leasing import claim_step, start_step
    from vuzol.storage.models import UsageRecord
    from vuzol.storage.types import RiskLevel
    from vuzol.workflows.ports import CancellationContext

    engine, factory = storage(postgres_dsn)
    try:
        pinned = account_profile(tmp_path, "c", reviewer=True)
        sibling = account_profile(tmp_path, "a", reviewer=True)
        settings, registries = bundle(tmp_path, pinned, sibling)
        task_id, run_id, step_id = await seed_provider_step(factory, step_type="review")
        assignments = {"reviewer_light": _pin(pinned)} if pin_source == "matrix" else {}
        async with factory.begin() as session:
            run = await session.get(Run, run_id)
            run.selected_route = {MATRIX_KEY: assignments}
            if pin_source == "previous-cli":
                step = await session.get(Step, step_id)
                step.payload = {
                    "independent_reviewer_pin": _pin(pinned),
                    "cli_reviewer_pin": {
                        "profile_id": pinned.id,
                        "profile_revision": content_revision(pinned),
                    },
                }
            session.add(missing_entitlement(pinned))
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        selected = []

        class Adapters:
            def get(self, profile_id):
                selected.append(profile_id)
                return self

            async def execute(self, request, configured, cancellation):
                assert configured == sibling
                if pin_source == "previous-cli":
                    async with factory() as session:
                        step = await session.get(Step, step_id)
                        assert step.payload["cli_reviewer_pin"] == {
                            "profile_id": sibling.id,
                            "profile_revision": content_revision(sibling),
                        }
                return ProviderResult(
                    status=ProviderResultStatus.SUCCEEDED,
                    text='{"verdict":"pass","summary":"reviewed","findings":[]}',
                    structured_output={"verdict": "pass", "summary": "reviewed", "findings": []},
                    usage=NormalizedUsage(input_tokens=10, output_tokens=10, duration_ms=1),
                    adapter_version="fake.v1",
                )

        reviewer = IndependentModelReviewer(
            registries,
            Adapters(),
            DatabaseReviewAccounting(factory, settings.limits),
            profile_id=pinned.id,
            session_factory=factory,
        )
        for attempt in range(2):
            async with factory.begin() as session:
                step = await session.get(Step, step_id)
                if attempt:
                    step.status = "queued"
                    step.lease_owner = step.lease_expires_at = None
                    await session.flush()
                lease = await claim_step(
                    session,
                    owner="review",
                    lease_seconds=60,
                    capabilities=frozenset(),
                    step_types=frozenset({"review"}),
                )
                await start_step(session, lease)
                task = await session.get(Task, task_id)
            result = await reviewer.review(
                task=task,
                risk=RiskLevel.MEDIUM,
                inspection=GitInspection(
                    head="b" * 40,
                    branch="review",
                    changed_files=("src/module.py",),
                    diff=b"+safe\n",
                ),
                base_commit="a" * 40,
                result_commit="b" * 40,
                diff_hash=None,
                gates=[],
                mechanical_findings=(),
                request_ids=(task_id, run_id, step_id),
                timeout_seconds=30,
                cancellation=CancellationContext(),
                lease=lease,
            )
            assert result.allows_progress
        assert selected == [sibling.id, sibling.id]
        async with factory() as session:
            step = await session.get(Step, step_id)
            assert step.executor_profile_id == sibling.id
            decisions = (await session.scalars(select(RoutingDecision))).all()
            assert len(decisions) == 2
            assert {row.selected_profile_id for row in decisions} == {sibling.id}
            assert step.payload["independent_reviewer_pin"] == _pin(pinned)
            assert (await session.get(Run, run_id)).selected_route == {MATRIX_KEY: assignments}
            assert {row.profile_id for row in await session.scalars(select(UsageRecord))} == {
                sibling.id
            }
    finally:
        await engine.dispose()
