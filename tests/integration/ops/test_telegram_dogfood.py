from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from tests.integration.storage.helpers import storage

from vuzol.config import Settings, TelegramDogfoodSettings
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.ops.telegram_dogfood import (
    DogfoodCase,
    DogfoodCaseResult,
    DogfoodError,
    DogfoodFault,
    arm_fault,
    build_report,
    consume_fault,
    diagnose_package,
    record_case_result,
    start_session,
)
from vuzol.storage.models import Run, Step, Task, TransactionalOutbox
from vuzol.storage.types import (
    DeliveryStatus,
    IdempotencyClass,
    PlanRevisionCreatedBy,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.model_command import PROJECT_MODEL_PICKER_ROLE
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.worker import WorkflowWorker

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _settings(*, faults: bool = True) -> TelegramDogfoodSettings:
    return TelegramDogfoodSettings(
        enabled=True,
        fault_injection_enabled=faults,
        allowed_project_ids=("vuzol-test",),
    )


async def test_session_fault_is_audited_and_consumed_once(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with factory.begin() as session:
        session_id = await start_session(
            session,
            _settings(),
            project_id="vuzol-test",
            configuration_revision="a" * 64,
            git_sha="b" * 40,
            actor_id="tester",
        )
        fault_id = await arm_fault(
            session,
            _settings(),
            session_id=session_id,
            project_id="vuzol-test",
            fault=DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS,
            actor_id="tester",
        )

    async with factory.begin() as session:
        consumed = await consume_fault(
            session,
            _settings(),
            project_id="vuzol-test",
            fault=DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS,
            consumer="worker-a",
        )
    async with factory.begin() as session:
        duplicate = await consume_fault(
            session,
            _settings(),
            project_id="vuzol-test",
            fault=DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS,
            consumer="worker-b",
        )
    async with factory() as session:
        report = await build_report(session, _settings(), session_id)

    assert consumed == fault_id and duplicate is None
    assert report.project_id == "vuzol-test"
    assert report.armed_faults == report.consumed_faults == 1
    assert report.package_counts == report.task_counts == {}
    assert report.case_results == {} and not report.release_ready
    await engine.dispose()


async def test_all_latest_checkpoints_are_required_for_release_readiness(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    async with factory.begin() as session:
        session_id = await start_session(
            session,
            _settings(),
            project_id="vuzol-test",
            configuration_revision="a" * 64,
            git_sha="b" * 40,
            actor_id="tester",
        )
        for case in DogfoodCase:
            await record_case_result(
                session,
                _settings(),
                session_id=session_id,
                case=case,
                result=DogfoodCaseResult.SUCCESS,
                actor_id="tester",
            )
    async with factory() as session:
        ready = await build_report(session, _settings(), session_id)
    assert ready.release_ready
    assert ready.case_results == {case.value: "pass" for case in DogfoodCase}

    async with factory.begin() as session:
        await record_case_result(
            session,
            _settings(),
            session_id=session_id,
            case=DogfoodCase.T06,
            result=DogfoodCaseResult.FAIL,
            actor_id="tester",
        )
    async with factory() as session:
        failed = await build_report(session, _settings(), session_id)
    assert failed.case_results["T06"] == "fail"
    assert not failed.release_ready
    await engine.dispose()


async def test_session_and_faults_fail_closed_outside_allowlist(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with factory.begin() as session:
        with pytest.raises(DogfoodError, match="not allowlisted"):
            await start_session(
                session,
                _settings(),
                project_id="vuzol",
                configuration_revision="a" * 64,
                git_sha="b" * 40,
                actor_id="tester",
            )
    async with factory.begin() as session:
        assert (
            await consume_fault(
                session,
                _settings(),
                project_id="vuzol",
                fault=DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS,
                consumer="worker-a",
            )
            is None
        )
    await engine.dispose()


async def test_package_diagnostic_is_redacted_canonical_state(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        discussion_id = await uow.discussions.create_session(
            project_id="vuzol-test", chat_id=-100, message_thread_id=99
        )
        created = await WorkPackageService(uow).create_draft(
            session_id=discussion_id,
            project_id="vuzol-test",
            plan=PlanDraft(
                title="Dogfood package",
                items=(
                    PlanItemDraft(
                        summary="Check lifecycle",
                        goal="Verify state",
                        expected_outcome="Diagnostic",
                        completion_criteria=("Visible",),
                        allowed_scope="src/**",
                    ),
                ),
            ),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
        )
    async with factory.begin() as session:
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_status",
                linked_entity_type="work_package",
                linked_entity_id=created.package_id,
                idempotency_key=f"test:diagnostic:{created.package_id}",
                payload={},
            )
        )
    async with factory() as session:
        diagnostic = await diagnose_package(session, _settings(), created.package_id)
    assert diagnostic.status == "draft"
    assert diagnostic.revision_number == 1
    assert diagnostic.task_id is None and not diagnostic.safe_retry
    assert diagnostic.outbox_counts == {"work_package_projection:pending": 1}
    assert diagnostic.outbox_errors == {}
    assert set(diagnostic.to_dict()) == {
        "package_id",
        "project_id",
        "status",
        "pause_reason",
        "revision_number",
        "cursor_ordinal",
        "task_id",
        "task_status",
        "run_id",
        "run_status",
        "step_id",
        "step_type",
        "step_status",
        "failure_category",
        "failure_summary",
        "safe_retry",
        "outbox_counts",
        "outbox_errors",
    }
    await engine.dispose()


async def test_provider_fault_is_consumed_before_handler_effects(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    dogfood = _settings()
    async with UnitOfWork(factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="controlled provider timeout",
            task_type="general",
            project_id="vuzol-test",
        )
        assert uow.session is not None
        task = await uow.session.get(Task, task_record.id)
        assert task is not None
        task.status = TaskStatus.EXECUTING
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="simple_model",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type="execute_model",
            idempotency_class=IdempotencyClass.READ_ONLY,
            status=StepStatus.QUEUED,
        )
        session_id = await start_session(
            uow.session,
            dogfood,
            project_id="vuzol-test",
            configuration_revision="a" * 64,
            git_sha="b" * 40,
            actor_id="tester",
        )
        await arm_fault(
            uow.session,
            dogfood,
            session_id=session_id,
            project_id="vuzol-test",
            fault=DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS,
            actor_id="tester",
        )

    handler = AsyncMock()
    worker = WorkflowWorker(
        Settings(environment="test", telegram_dogfood=dogfood),
        factory,
        owner="dogfood-worker",
        handlers={"execute_model": handler},
    )
    assert await worker.process_one()
    handler.execute.assert_not_awaited()
    async with factory() as session:
        step = await session.scalar(select(Step).where(Step.run_id == run_id))
        run = await session.get(Run, run_id)
        task = await session.get(Task, task_record.id)
    assert step is not None and step.status is StepStatus.BLOCKED
    assert step.failure_category == "timeout" and not step.unknown_effects
    assert run is not None and run.status is RunStatus.BLOCKED
    assert task is not None and task.status is TaskStatus.BLOCKED
    await engine.dispose()


async def test_telegram_fault_requeues_before_bot_request(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    dogfood = _settings()
    async with factory.begin() as session:
        session_id = await start_session(
            session,
            dogfood,
            project_id="vuzol-test",
            configuration_revision="a" * 64,
            git_sha="b" * 40,
            actor_id="tester",
        )
        await arm_fault(
            session,
            dogfood,
            session_id=session_id,
            project_id="vuzol-test",
            fault=DogfoodFault.TELEGRAM_TRANSIENT_BEFORE_REQUEST,
            actor_id="tester",
        )
        outbox = TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="telegram_inbox",
            linked_entity_id=session_id,
            idempotency_key=f"dogfood-telegram:{session_id}",
            payload={
                "role": PROJECT_MODEL_PICKER_ROLE,
                "chat_id": -100,
                "message_thread_id": 99,
                "project_id": "vuzol-test",
                "html": "<b>test</b>",
                "callback_buttons": [],
            },
        )
        session.add(outbox)
        await session.flush()
        outbox_id = outbox.id
    client = FakeTelegramClient()
    delivery = TelegramDeliveryService(
        factory,
        client,
        owner="dogfood-delivery",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=1,
        retry_max_seconds=10,
        dogfood_settings=dogfood,
    )
    assert await delivery.deliver_one()
    assert client.sent == []
    async with factory() as session:
        loaded_outbox = await session.get(TransactionalOutbox, outbox_id)
    assert loaded_outbox is not None and loaded_outbox.status is DeliveryStatus.PENDING
    assert loaded_outbox.last_error_category == "timedout"
    await engine.dispose()
