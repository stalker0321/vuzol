import io
import os
import uuid
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select

from tests.fakes.telegram import FakeTelegramClient
from tests.integration.storage.helpers import seed_task_run_step, storage
from tests.unit.execution.test_candidate_archive import committed
from vuzol.execution.access import grant_artifact_read
from vuzol.execution.artifact_production import ArtifactProductionHandler
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.git import LocalGit
from vuzol.storage.models import (
    Artifact,
    Step,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import DeliveryStatus, IdempotencyClass, StepStatus, WorktreeDeliveryState
from vuzol.telegram.delivery import (
    PermanentDeliveryError,
    TelegramDeliveryService,
    prepare_delivery,
)
from vuzol.telegram.projections import LostTelegramResponse
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def artifact_read_grant(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    grant_read = AsyncMock(wraps=grant_artifact_read)
    # Keep real ACL coverage on hosts where the trusted tool owner is mapped.
    acl_tool = Path("/usr/bin/setfacl")
    if not acl_tool.is_file() or acl_tool.stat().st_uid != 0:
        grant_read = AsyncMock()
    monkeypatch.setattr("vuzol.execution.artifact_production.grant_artifact_read", grant_read)
    return grant_read


@pytest.mark.parametrize("mode", ["success", "lost-send", "secret", "wrong-review", "stale-lease"])
async def test_checked_archive_to_confirmed_telegram_link(
    postgres_dsn, tmp_path, monkeypatch, mode, artifact_read_grant
):
    grant_read = artifact_read_grant
    engine, factory = storage(postgres_dsn)
    try:
        root = tmp_path / "candidate"
        root.mkdir()
        (root / "index.html").write_text("sk-" + "a" * 25 if mode == "secret" else "checked site")
        commit = committed(root)
        (root / "untracked.txt").write_text("not included")
        store = ArtifactStore(tmp_path / "artifacts", max_bytes=4096, retention_days=14)
        record, run_id, step_record = await seed_task_run_step(
            factory, step_type="produce_artifacts"
        )
        expires = datetime.now(UTC) + timedelta(minutes=10)
        async with factory.begin() as session:
            task = await session.get(Task, record.id)
            task.project_id, task.ingress_source = "afterword", "chat_work"
            task.source_chat_id, task.source_thread_id = -100, 577
            step = await session.get(Step, step_record.id)
            step.status, step.lease_owner, step.lease_generation = StepStatus.RUNNING, "executor", 1
            step.lease_expires_at = (
                expires if mode != "stale-lease" else datetime.now(UTC) - timedelta(seconds=1)
            )
            session.add(
                Step(
                    run_id=run_id,
                    ordinal=0,
                    step_type="review",
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    timeout_seconds=60,
                    status=StepStatus.COMPLETED,
                    result={
                        "result_commit": commit if mode != "wrong-review" else "a" * 40,
                        "verdict": "pass",
                    },
                )
            )
            session.add(
                Worktree(
                    task_id=task.id,
                    run_id=run_id,
                    project_id="afterword",
                    repository_identity_hash="c" * 64,
                    base_commit=commit,
                    result_commit=commit,
                    default_branch="main",
                    expected_target_head=commit,
                    branch="main",
                    path=str(root),
                    owner="executor",
                    delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                    retention_until=expires,
                )
            )
        monkeypatch.setattr(
            "vuzol.execution.artifact_production.current_environment",
            AsyncMock(return_value=SimpleNamespace(contract={"components": {}})),
        )
        request = StepExecutionRequest(
            task_id=record.id,
            run_id=run_id,
            step_id=step_record.id,
            step_type="produce_artifacts",
            payload={},
            timeout_seconds=60,
            lease=LeaseToken(
                step=StepRecord(
                    id=step_record.id,
                    run_id=run_id,
                    status=StepStatus.RUNNING,
                    lease_generation=1,
                    lease_owner="executor",
                    lease_expires_at=expires,
                ),
                owner="executor",
                generation=1,
            ),
        )
        producer = ArtifactProductionHandler(
            factory,
            MagicMock(),
            LocalGit(),
            MagicMock(),
            MagicMock(),
            store,
            worktree_root=tmp_path,
            delivery_uid=os.getuid(),
        )
        outcome = await producer.execute(request, CancellationContext())
        if mode in {"secret", "wrong-review", "stale-lease"}:
            assert outcome.kind is OutcomeKind.BLOCKED
            grant_read.assert_not_awaited()
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Artifact)) == 0
                assert (
                    await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 0
                )
            return
        assert outcome.kind is OutcomeKind.SUCCEEDED
        grant_read.assert_awaited_once()
        assert grant_read.call_args.kwargs == {"delivery_uid": os.getuid()}
        assert grant_read.call_args.args[0].is_file()
        assert (
            await producer.execute(request, CancellationContext())
        ).kind is OutcomeKind.SUCCEEDED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Artifact)) == 1
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
            item = await session.scalar(select(TransactionalOutbox))
            prepared = await prepare_delivery(session, item, artifacts=store)
            with zipfile.ZipFile(io.BytesIO(prepared.document_content)) as archive:
                assert archive.namelist() == ["index.html"]
                assert archive.read("index.html") == b"checked site"
            item.payload = {**item.payload, "task_id": str(uuid.uuid4())}
            with pytest.raises(PermanentDeliveryError):
                await prepare_delivery(session, item, artifacts=store)
        telegram = FakeTelegramClient(
            fail=LostTelegramResponse("unknown") if mode == "lost-send" else None
        )
        delivery = TelegramDeliveryService(
            factory,
            telegram,
            owner="delivery",
            lease_seconds=30,
            max_attempts=2,
            retry_min_seconds=1,
            retry_max_seconds=2,
            artifacts=store,
        )
        assert await delivery.deliver_one()
        assert not await delivery.deliver_one()
        async with factory() as session:
            link = await session.scalar(select(TelegramMessageLink))
            item = await session.scalar(select(TransactionalOutbox))
            if mode == "lost-send":
                assert link is None and item.status is DeliveryStatus.AMBIGUOUS
            else:
                assert link.message_role == "task_result_archive" and link.task_id == record.id
                assert telegram.documents[0][:2] == (-100, 577)
    finally:
        await engine.dispose()
