"""Project status dashboard projection helpers."""

import uuid
from types import SimpleNamespace

import pytest

from vuzol.config import TopicKind
from vuzol.storage.types import TaskStatus
from vuzol.telegram.layout import (
    STATUS_DASHBOARD_DISPLAY_NAME,
    STATUS_DASHBOARD_TOPIC_KIND,
    is_status_dashboard_topic,
)
from vuzol.telegram.projections import (
    dashboard_revision_for,
    model_label_for_profile,
    task_number_label,
    task_sense_sentence,
    telegram_html,
)


def test_status_dashboard_binds_to_existing_task_dashboard_kind() -> None:
    """Product policy posts into kind=task_dashboard («Статус проектов»), not a new topic."""

    assert STATUS_DASHBOARD_TOPIC_KIND is TopicKind.TASK_DASHBOARD
    assert STATUS_DASHBOARD_DISPLAY_NAME == "Статус проектов"
    assert is_status_dashboard_topic(TopicKind.TASK_DASHBOARD)
    assert is_status_dashboard_topic("task_dashboard")
    assert not is_status_dashboard_topic(TopicKind.APPROVALS)


def test_task_number_prefers_public_then_local() -> None:
    public = SimpleNamespace(public_task_number=730001, topic_task_number=1)
    local = SimpleNamespace(public_task_number=None, topic_task_number=7)
    missing = SimpleNamespace(public_task_number=None, topic_task_number=None)
    assert task_number_label(public) == "730001"  # type: ignore[arg-type]
    assert task_number_label(local) == "0007"  # type: ignore[arg-type]
    assert task_number_label(missing) == "—"  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_dashboard_surfaces_project_default_executor_pin() -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock
    from uuid import uuid4

    from vuzol.projects.executor_preference import (
        ExecutorPreferenceMode,
        ExecutorPreferenceView,
        ExecutorWorkerKey,
    )
    from vuzol.storage.types import TaskStatus
    from vuzol.telegram.projections import build_project_status_dashboard

    task = SimpleNamespace(
        id=uuid4(),
        version=1,
        status=TaskStatus.EXECUTING,
        project_id="bill-buddy",
        original_text="fix the form",
        task_draft={"task_summary": "Fix the form"},
        topic_task_number=1,
        public_task_number=200001,
        source_chat_id=-100,
        source_thread_id=20,
        created_at=None,
    )
    session = MagicMock()
    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: [task]))
    session.get = AsyncMock(return_value=None)

    async def fake_active(_session: object, _task_id: object) -> None:
        return None

    async def fake_step_model(_session: object, _task_id: object) -> None:
        return None

    async def fake_pref(_session: object, project_id: str) -> ExecutorPreferenceView:
        assert project_id == "bill-buddy"
        return ExecutorPreferenceView(
            project_id=project_id,
            mode=ExecutorPreferenceMode.PIN,
            worker_key=ExecutorWorkerKey.GROK,
            reasoning_effort=None,
            revision=2,
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("vuzol.telegram.projections._active_executor_profile", fake_active)
    monkeypatch.setattr("vuzol.telegram.projections._latest_step_model", fake_step_model)
    monkeypatch.setattr("vuzol.telegram.projections.load_preference", fake_pref)
    monkeypatch.setattr(
        "vuzol.telegram.projections.load_subscription_limits",
        AsyncMock(return_value=()),
    )
    try:
        card = await build_project_status_dashboard(
            session,
            chat_id=-100,
            project_names={"bill-buddy": "Bill Buddy"},
        )
    finally:
        monkeypatch.undo()
    assert "Grok (project default)" in card.html


def test_model_label_is_english_and_friendly() -> None:
    from vuzol.telegram.projections import format_executor_model, model_label_for_profile

    assert model_label_for_profile(None) == "not assigned yet"
    assert (
        model_label_for_profile(
            "codex-subscription-prod",
            profile_models={"codex-subscription-prod": "gpt-5.6-sol"},
            profile_efforts={"codex-subscription-prod": "medium"},
            profile_providers={"codex-subscription-prod": "codex"},
        )
        == "Codex Sol · medium"
    )
    assert (
        model_label_for_profile(
            "grok-subscription-a",
            profile_models={"grok-subscription-a": "grok-build"},
            profile_providers={"grok-subscription-a": "grok"},
        )
        == "Grok Build"
    )
    assert format_executor_model("gpt-5.6-terra", effort="high", provider="codex") == (
        "Codex Terra · high"
    )
    assert format_executor_model("gpt-5-nano-2025-08-07") == "GPT-5 Nano"
    assert format_executor_model("gpt-5.1-codex") == "GPT-5.1 Codex"


def test_task_sense_is_one_sentence() -> None:
    task = SimpleNamespace(
        task_draft={
            "task_summary": "Подготовить адаптивный лендинг для нового продукта.",
            "normalized_title": "Сделать лендинг. Потом API.",
            "goal": "ignored",
        },
        original_text="fallback",
    )
    assert task_sense_sentence(task) == (  # type: ignore[arg-type]
        "Подготовить адаптивный лендинг для нового продукта"
    )
    long = SimpleNamespace(
        task_draft={"goal": "x" * 200},
        original_text="",
    )
    assert task_sense_sentence(long).endswith("…")  # type: ignore[arg-type]
    assert len(task_sense_sentence(long)) <= 160  # type: ignore[arg-type]


def test_format_executor_model_edge_branches() -> None:
    from vuzol.telegram.projections import format_executor_model

    assert format_executor_model(None, profile_id="codex-x") == "Codex"
    assert format_executor_model(None, profile_id="grok-x") == "Grok Build"
    assert format_executor_model("grok", provider="grok") == "Grok"
    assert format_executor_model("custom-thing", provider="grok") == "Custom Thing"
    assert format_executor_model("gpt-5.6-luna", provider="codex", effort="low") == (
        "Codex Luna · low"
    )
    assert format_executor_model(None) == "not assigned yet"
    assert format_executor_model("", profile_id="other") == "other"


def test_model_label_uses_registry_model() -> None:
    assert model_label_for_profile(None) == "not assigned yet"
    assert model_label_for_profile("other-profile") == "other-profile"
    assert (
        model_label_for_profile("other-profile", profile_models={"other-profile": "gpt-5.1-codex"})
        == "GPT-5.1 Codex"
    )
    # Explicit step model overrides a generic registry token.
    assert (
        model_label_for_profile(
            "codex-subscription-prod",
            profile_models={"codex-subscription-prod": "codex"},
            profile_efforts={"codex-subscription-prod": "medium"},
            model="gpt-5.6-sol",
        )
        == "Codex Sol · medium"
    )


@pytest.mark.anyio
async def test_latest_step_model_prefers_concrete_slug() -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.telegram.projections import _latest_step_model

    run = SimpleNamespace(id=uuid.uuid4())
    step_generic = SimpleNamespace(result={"model": "codex"}, ordinal=1)
    step_concrete = SimpleNamespace(result={"model": "gpt-5.6-sol"}, ordinal=2)
    session = MagicMock()
    session.scalar = AsyncMock(return_value=run)
    session.scalars = AsyncMock(
        return_value=SimpleNamespace(all=lambda: [step_concrete, step_generic])
    )
    assert await _latest_step_model(session, uuid.uuid4()) == "gpt-5.6-sol"

    session.scalar = AsyncMock(return_value=None)
    assert await _latest_step_model(session, uuid.uuid4()) is None


@pytest.mark.anyio
async def test_task_status_projection_enqueues_project_card_and_dashboard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.telegram.projections import enqueue_task_status_projection

    task = SimpleNamespace(
        id=uuid.uuid4(),
        version=4,
        source_chat_id=-100,
        source_thread_id=10,
    )
    run = SimpleNamespace(id=uuid.uuid4())
    intake = SimpleNamespace(id=uuid.uuid4())
    dashboard = AsyncMock()
    monkeypatch.setattr("vuzol.telegram.projections.enqueue_project_status_dashboard", dashboard)
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[intake, None])
    session.add = MagicMock()

    await enqueue_task_status_projection(session, task, run)  # type: ignore[arg-type]

    item = session.add.call_args.args[0]
    assert item.payload["role"] == "intake_ack"
    assert item.payload["run_id"] == str(run.id)
    assert item.payload["message_thread_id"] == 10
    dashboard.assert_awaited_once_with(session, -100)


def test_dashboard_revision_changes_with_content() -> None:
    task_a = SimpleNamespace(id=uuid.uuid4(), version=1, status=TaskStatus.EXECUTING)
    task_b = SimpleNamespace(id=task_a.id, version=2, status=TaskStatus.EXECUTING)
    first = dashboard_revision_for([task_a], {task_a.id: "m1"})  # type: ignore[list-item]
    second = dashboard_revision_for([task_b], {task_b.id: "m1"})  # type: ignore[list-item]
    same = dashboard_revision_for([task_a], {task_a.id: "m1"})  # type: ignore[list-item]
    assert first != second
    assert first == same
    assert telegram_html("<x>") == "&lt;x&gt;"
