"""D3 context/budget PostgreSQL tests (dossier pp.2-7 + W1/W3/W5 flows)."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from tests.integration.providers._test_routing_helpers import (
    BudgetExceeded,
    Decimal,
    IdempotencyClass,
    StepStatus,
    UnitOfWork,
    bundle,
    estimate_reservation,
    profile,
    reserve_budget,
    seed_provider_step,
    storage,
)
from vuzol.context.bindings import bind_task_output
from vuzol.context.resolver import BindingError, pack_context, resolve_context
from vuzol.execution.artifacts import ArtifactStore
from vuzol.providers.budgets import (
    _lifetime_totals,
    accounting_for_profile,
    reserve_invocation_budget,
    settle_invocation_budget,
)
from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus
from vuzol.research.report import validate_source_report_bytes
from vuzol.research.retrieval import FixtureRetrieval, RetrievedSource
from vuzol.research.source_backed import SourceFetcher
from vuzol.storage.models import (
    Artifact,
    Event,
    InputBinding,
    Step,
    Task,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _fixture_fetch(fixtures: FixtureRetrieval) -> SourceFetcher:

    def go(uri: str, *, now: str) -> RetrievedSource:
        return fixtures.fetch(uri, now=now)

    return go


def _research_result(
    *, uris: tuple[str, ...] = ("fixture://adapter.md",), raw_len: int = 26
) -> ProviderResult:
    return ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        text="research text",
        structured_output={
            "research": {
                "question": "Which adapter backs CI?",
                "sources": [{"uri": uri} for uri in uris],
                "claims": [
                    {
                        "claim_id": "c1",
                        "statement": "CI is fixture-based.",
                        "support": "supported",
                        "citations": [[uri, f"offset:0-{raw_len}"] for uri in uris],
                    }
                ],
            }
        },
        provider_request_id="req-1",
        usage=NormalizedUsage(input_tokens=10, output_tokens=5, duration_ms=1),
        finish_reason="stop",
        adapter_version="test",
    )


@pytest.mark.anyio
async def test_d3_source_backed_research_end_to_end(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """W1: question → retrieval → report → artifact → binding → synthesize."""

    from vuzol.providers.handlers import ProviderStepHandler
    from vuzol.storage.types import QueueClass, RetryClass
    from vuzol.workflows.ports import StepExecutionRequest

    raw = b"adapter backed by fixtures"
    engine, factory = storage(postgres_dsn)
    try:
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.tasks.create(
                user_id=1,
                chat_id=-100,
                original_text="Which adapter backs CI?",
                task_type="research",
                project_id="vuzol",
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="research",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
                status="running",  # type: ignore[arg-type]
            )
            producer = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="research_execute",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.RUNNING,
                queue_class=QueueClass.LIGHT,
                retry_class=RetryClass.NEVER,
                max_attempts=1,
            )
            consumer = await uow.steps.create(
                run_id=run_id,
                ordinal=2,
                step_type="synthesize",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.RUNNING,
                queue_class=QueueClass.LIGHT,
                retry_class=RetryClass.NEVER,
                max_attempts=1,
                dependency_metadata={"predecessor_ordinals": [1]},
            )
            task_id, producer_id, consumer_id = task.id, producer.id, consumer.id
        store = ArtifactStore(
            tmp_path, max_bytes=5_000_000, retention_days=7, redaction_patterns=()
        )
        fixtures = FixtureRetrieval(fixtures={"fixture://adapter.md": raw})
        handler = ProviderStepHandler(
            factory,
            MagicMock(),
            MagicMock(),
            artifacts=store,
            research_fetcher=_fixture_fetch(fixtures),
            research_retriever="local-docs-fixture",
        )
        request = StepExecutionRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=producer_id,
            step_type="research_execute",
            payload={},
            timeout_seconds=60,
            lease=MagicMock(),
        )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            await handler._persist_input_bindings(
                uow.session,
                request=request,
                result=_research_result(raw_len=len(raw)),
            )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            bindings = (
                await uow.session.scalars(
                    select(InputBinding).where(
                        InputBinding.consumer_step_id == consumer_id
                    )
                )
            ).all()
            assert len(bindings) == 1
            binding = bindings[0]
            assert binding.schema_name == "research-result"
            assert binding.schema_version == "research-result.v1"
            assert binding.source_retrieved_at is not None
            assert binding.freshness_max_age_seconds is not None
            artifact = await uow.session.get(Artifact, binding.artifact_id)
            assert artifact is not None
            assert validate_source_report_bytes(store.read(artifact.content_uri)) == ()
            raws = (
                await uow.session.scalars(
                    select(Artifact).where(Artifact.artifact_type == "research_source")
                )
            ).all()
            assert len(raws) == 1
            assert store.read(raws[0].content_uri) == raw
            # the consumer resolves the verified report through the pair gate
            resolved = await resolve_context(
                uow.session, store, consumer_step_id=consumer_id, project_id="vuzol"
            )
            assert not resolved.is_empty
            manifest, items = pack_context(resolved, role="summarizer")
            assert len(items) >= 1
            assert manifest.entries[0].schema_name == "research-result"
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_d3_lifetime_gate_and_epoch_canon(postgres_dsn: str, tmp_path: Path) -> None:
    """pp.2/Q2: lifetime enforced on horizon scope; epoch resets caps only."""

    from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
    from vuzol.storage.types import PlanRevisionCreatedBy

    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    try:
        task_id, run_id, step = await seed_provider_step(factory)
        package_id: uuid.UUID | None = None
        async with UnitOfWork(factory) as uow:
            session_id = await uow.discussions.create_session(
                project_id="vuzol", chat_id=-100, message_thread_id=10
            )
            created = await WorkPackageService(uow).create_draft(
                session_id=session_id,
                project_id="vuzol",
                plan=PlanDraft(
                    title="t",
                    items=(
                        PlanItemDraft(
                            summary="s",
                            goal="g",
                            expected_outcome="o",
                            completion_criteria=("c",),
                            allowed_scope="src/**",
                        ),
                    ),
                ),
                created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
                actor_type="planner_model",
                lifetime_budget={"max_cost": 0.015, "max_attempts": 100},
            )
            package_id = created.package_id
        assert package_id is not None
        api = profile("api")
        estimate = estimate_reservation(api, input_tokens=1, output_tokens=1)
        horizon = package_id
        # first reserve inside the lifetime budget passes
        async with factory.begin() as session:
            await reserve_budget(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step,
                profile_id="api",
                provider_attempt=1,
                estimate=estimate,
                limits=settings.limits,
                accounting=accounting_for_profile(
                    api, purpose="coding", horizon_id=horizon
                ),
            )
        # epoch bump does not erase lifetime: still over budget on retry
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.session.get(Task, task_id, with_for_update=True)
            assert task is not None
            task.budget_epoch += 1
        big = estimate_reservation(api, input_tokens=10, output_tokens=10)
        assert big.cost_units >= Decimal("0.01")
        with pytest.raises(BudgetExceeded):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=step,
                    profile_id="api",
                    provider_attempt=2,
                    estimate=big,
                    limits=settings.limits,
                    accounting=accounting_for_profile(
                        api, purpose="coding", attempt_kind="retry", horizon_id=horizon
                    ),
                )
        # lifetime totals see the spend across the epoch bump (canon, no filter)
        async with factory.begin() as session:
            totals = await _lifetime_totals(session, horizon)
            assert totals[2] >= estimate.cost_units
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_d3_counters_and_allowance_exhaustion(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """W4: admission counters + deductible review allowance pool."""

    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    settings = settings.model_copy(
        update={
            "limits": settings.limits.model_copy(
                update={
                    "task_input_tokens": 1,
                    "review_allowance_input_tokens": 1,
                    "review_allowance_output_tokens": 10,
                    "review_allowance_cost_units": 10.0,
                }
            )
        }
    )
    try:
        task_id, run_id, first_step = await seed_provider_step(factory)
        estimate = estimate_reservation(profile("api"), input_tokens=1, output_tokens=1)
        async with factory.begin() as session:
            first = await reserve_budget(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=first_step,
                profile_id="api",
                provider_attempt=1,
                estimate=estimate,
                limits=settings.limits,
            )
            assert first.allowance_input_tokens == 0
        async with UnitOfWork(factory) as uow:
            second = await uow.steps.create(
                run_id=run_id,
                ordinal=2,
                step_type="execute_model",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.QUEUED,
                max_attempts=3,
            )
            reviewer = await uow.steps.create(
                run_id=run_id,
                ordinal=3,
                step_type="review",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.QUEUED,
                max_attempts=3,
            )
        # task caps exhausted (1/1 used): a plain second reserve fails
        with pytest.raises(BudgetExceeded):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=second.id,
                    profile_id="api",
                    provider_attempt=1,
                    estimate=estimate,
                    limits=settings.limits,
                )
        # ...but the review allowance (pool 1) covers exactly this overage once
        async with factory.begin() as session:
            review = await reserve_budget(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=reviewer.id,
                profile_id="api",
                provider_attempt=1,
                estimate=estimate,
                limits=settings.limits,
                review_allowance=True,
            )
            assert review.allowance_input_tokens == 1
        # pool consumed: a second over-cap review is refused (no second ledger)
        async with UnitOfWork(factory) as uow:
            reviewer2 = await uow.steps.create(
                run_id=run_id,
                ordinal=4,
                step_type="review",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.QUEUED,
                max_attempts=3,
            )
        with pytest.raises(BudgetExceeded):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=reviewer2.id,
                    profile_id="api",
                    provider_attempt=1,
                    estimate=estimate,
                    limits=settings.limits,
                    review_allowance=True,
                )
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_d3_step_less_reserve_settle_idempotent(postgres_dsn: str, tmp_path: Path) -> None:
    """pp.3/W4: step-less reserve+settle without fake Step; duplicate safe."""

    from vuzol.providers.budgets import _lifetime_totals as _lt

    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    try:
        api = profile("api")
        invocation = uuid.uuid4()
        horizon = uuid.uuid4()
        estimate = estimate_reservation(api, input_tokens=4, output_tokens=1)
        scouting = accounting_for_profile(api, purpose="scout", horizon_id=horizon)
        async with factory.begin() as session:
            first = await reserve_invocation_budget(
                session,
                invocation_id=invocation,
                profile=api,
                estimate=estimate,
                limits=settings.limits,
                accounting=scouting,
            )
            assert first.step_id is None and first.invocation_id == invocation
            duplicate = await reserve_invocation_budget(
                session,
                invocation_id=invocation,
                profile=api,
                estimate=estimate,
                limits=settings.limits,
                accounting=scouting,
            )
            assert duplicate.id == first.id
            record = await settle_invocation_budget(
                session,
                reservation=first,
                profile=api,
                usage=NormalizedUsage(input_tokens=4, output_tokens=1, duration_ms=5),
                provider_request_id=None,
                outcome="succeeded",
            )
            assert record.invocation_id == invocation
            assert record.horizon_id == horizon
            # tokens without price stay unknown: conservative floor, not zero
            assert record.cost_known is False
            assert record.cost_units is not None and record.cost_units > Decimal("0")
            totals = await _lt(session, horizon)
            assert totals[0] >= 4
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_d3_scout_partial_retry_capacity(postgres_dsn: str, tmp_path: Path) -> None:
    """pp.7/W3: partial persists on probe failure; retry runs missing only."""

    from vuzol.scout import ScoutProbe, ScoutRequest, retry_scout, run_scout

    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    try:
        fixtures = FixtureRetrieval(fixtures={"fixture://a": b"alpha beta"})
        request = ScoutRequest(
            question="q?",
            scope="vuzol",
            probes=(
                ScoutProbe(name="p1", kind="fetch", uri="fixture://a"),
                ScoutProbe(name="p2", kind="fetch", uri="fixture://missing"),
            ),
            deadline="2026-10-01T00:00:00Z",
            max_calls=5,
            stop_condition="all_required",
        )
        async with factory.begin() as session:
            partial = await run_scout(
                session,
                request=request,
                fetch=_fixture_fetch(fixtures),
                profile=profile("api"),
                limits=settings.limits,
                artifacts=None,
                consumer_step_id=None,
                project_id="vuzol",
                now="2026-09-30T00:00:00Z",
            )
            assert partial.status == "partial"
            assert [fact.probe for fact in partial.facts] == ["p1"]
            assert partial.unresolved == ["p2"]
            events = (
                await session.scalars(
                    select(Event).where(Event.event_type == "scout.packet_partial")
                )
            ).all()
            assert len(events) >= 1
        fixtures.fixtures["fixture://missing"] = b"gamma delta"
        async with factory.begin() as session:
            complete = await retry_scout(
                session,
                packet=partial,
                request=request,
                fetch=_fixture_fetch(fixtures),
                profile=profile("api"),
                limits=settings.limits,
                artifacts=None,
                consumer_step_id=None,
                project_id="vuzol",
                now="2026-09-30T01:00:00Z",
            )
            assert complete.status == "complete"
            assert [fact.probe for fact in complete.facts] == ["p1", "p2"]
            assert complete.unresolved == []
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_d3_scope_freshness_manifest_task_pair(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """pp.5/6 W5: missing scope, retrieved_at anchor, manifest persist, pair."""



    engine, factory = storage(postgres_dsn)
    store = ArtifactStore(
        tmp_path, max_bytes=5_000_000, retention_days=7, redaction_patterns=()
    )
    try:
        task_id, run_id, first_step = await seed_provider_step(factory)
        task_project = "vuzol"

        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.session.get(Task, task_id)
            assert task is not None
            task.project_id = task_project
            missing_consumer = await uow.steps.create(
                run_id=run_id,
                ordinal=2,
                step_type="plan",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.RUNNING,
                max_attempts=3,
            )
            stale_consumer = await uow.steps.create(
                run_id=run_id,
                ordinal=3,
                step_type="plan",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.RUNNING,
                max_attempts=3,
            )
            ok_consumer = await uow.steps.create(
                run_id=run_id,
                ordinal=4,
                step_type="plan",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.RUNNING,
                max_attempts=3,
            )
            artifact = await store.persist(
                uow.session,
                task_id=task_id,
                run_id=run_id,
                step_id=first_step,
                artifact_type="task_result",
                content=b"upstream bytes",
                media_type="application/octet-stream",
                sensitivity="internal",
                visibility="private",
            )
            # missing scope owner fails closed even though hashes match
            await bind_task_output(
                uow.session,
                artifact_id=artifact.id,
                content_hash=artifact.content_hash,
                consumer_step_id=missing_consumer.id,
                producer_step_id=first_step,
                scope_project_id=None,
                access_scope="private",
            )
            try:
                await resolve_context(
                    uow.session,
                    store,
                    consumer_step_id=missing_consumer.id,
                    project_id="vuzol",
                )
            except BindingError as error:
                assert error.category == "missing_scope"
            else:
                raise AssertionError("expected missing_scope")
            # same bytes, old retrieved_at anchor: stale, not rejuvenated
            old = datetime.now(UTC) - timedelta(days=2)
            await bind_task_output(
                uow.session,
                artifact_id=artifact.id,
                content_hash=artifact.content_hash,
                consumer_step_id=stale_consumer.id,
                producer_step_id=first_step,
                scope_project_id="vuzol",
                access_scope="private",
                required=False,
                freshness_max_age_seconds=3600,
                source_retrieved_at=old,
            )
            stale = await resolve_context(
                uow.session,
                store,
                consumer_step_id=stale_consumer.id,
                project_id="vuzol",
            )
            assert not stale.is_empty
            assert stale.bindings[0].freshness == "stale"
            # success path: scoped binding resolves to items for the pair
            scoped = await bind_task_output(
                uow.session,
                artifact_id=artifact.id,
                content_hash=artifact.content_hash,
                consumer_step_id=ok_consumer.id,
                producer_step_id=first_step,
                scope_project_id="vuzol",
                access_scope="private",
            )
            assert scoped.schema_name == "task-result"
            resolved_ok = await resolve_context(
                uow.session,
                store,
                consumer_step_id=ok_consumer.id,
                project_id="vuzol",
            )
            assert not resolved_ok.is_empty
            # pair manifest persists exactly what the invocation received
            from vuzol.providers.handlers import ProviderStepHandler

            handler = ProviderStepHandler(factory, MagicMock(), MagicMock(), artifacts=store)
            manifest, items = pack_context(resolved_ok, role="planner")
            assert len(items) >= 1
            ok_step = await uow.session.get(Step, ok_consumer.id)
            assert ok_step is not None
            await handler._persist_manifest(
                uow.session,
                step=ok_step,
                task=task,
                run_id=run_id,
                manifest=manifest,
                role="planner",
            )
            records = (
                await uow.session.scalars(
                    select(Artifact).where(Artifact.artifact_type == "context_manifest")
                )
            ).all()
            assert len(records) == 1
            stored = json.loads(store.read(records[0].content_uri).decode())
            assert stored["policy"] == "context-resolver.v1"
            assert stored["manifest"]["entries"][0]["content_hash"] == (
                artifact.content_hash
            )
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_d3_provider_call_counter_blocks_independently(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """W4: max_provider_calls fires even with roomy token caps."""

    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    settings = settings.model_copy(
        update={"limits": settings.limits.model_copy(update={"max_provider_calls": 1})}
    )
    try:
        task_id, run_id, first_step = await seed_provider_step(factory)
        estimate = estimate_reservation(profile("api"), input_tokens=1, output_tokens=1)
        async with factory.begin() as session:
            await reserve_budget(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=first_step,
                profile_id="api",
                provider_attempt=1,
                estimate=estimate,
                limits=settings.limits,
            )
        async with UnitOfWork(factory) as uow:
            second = await uow.steps.create(
                run_id=run_id,
                ordinal=2,
                step_type="execute_model",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.QUEUED,
                max_attempts=3,
            )
        with pytest.raises(BudgetExceeded):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=second.id,
                    profile_id="api",
                    provider_attempt=1,
                    estimate=estimate,
                    limits=settings.limits,
                )
    finally:
        await engine.dispose()
