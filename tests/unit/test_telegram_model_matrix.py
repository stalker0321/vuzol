"""Authorized profile picker callbacks, including long IDs and stale catalogs."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unit.projects.test_model_matrix import bundle, configured
from tests.unit.test_telegram_model_command import _Update
from vuzol.config.models import TopicKind
from vuzol.projects.executor_preference import ExecutorPreferenceError
from vuzol.projects.model_matrix import SLOT_LABELS
from vuzol.storage.models import ProjectExecutorPreference
from vuzol.telegram.adapter import control_update
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus
from vuzol.telegram.model_command import ProjectModelController, enqueue_worker_picker
from vuzol.telegram.model_matrix import enqueue_matrix, profile_token


@pytest.fixture
def picker(monkeypatch):
    profile = configured("pi", profile_id="account-" + "x" * 90)
    runtime = MagicMock()
    runtime.registries = bundle(profile)
    runtime.registries.topics.resolve.return_value.kind = TopicKind.PROJECT
    runtime.registries.topics.resolve.return_value.project_id = "project"
    row = ProjectExecutorPreference(
        project_id="project", mode="auto", revision=7, model_assignments={}
    )
    session = AsyncMock()
    session.get.return_value = row
    session.add = MagicMock()
    monkeypatch.setattr(
        "vuzol.telegram.model_command.enqueue_project_status_dashboard", AsyncMock()
    )
    monkeypatch.setattr("vuzol.telegram.model_command._enqueue_project_topic_refresh", AsyncMock())
    return profile, runtime, row, session


@pytest.mark.anyio
async def test_model_command_opens_all_roles_and_long_profile_ids_remain_selectable(picker):
    profile, runtime, row, session = picker
    await enqueue_worker_picker(
        session,
        runtime=runtime,
        project_id="project",
        chat_id=-100,
        message_thread_id=42,
        inbox_id=uuid.uuid4(),
    )
    menu = session.add.call_args.args[0].payload
    assert len(menu["callback_buttons"]) == len(SLOT_LABELS)
    await enqueue_matrix(
        session,
        runtime=runtime,
        project_id="project",
        chat_id=-100,
        thread_id=42,
        identity=uuid.uuid4(),
        slot="reviewer_strong",
    )
    payload = session.add.call_args.args[0].payload
    choices = [button for group in payload["callback_buttons"] for button in group]
    selected = next(data for label, data in choices if profile.id in label)
    assert all(len(data.encode()) <= 64 for _, data in choices)
    update = control_update(_Update(selected), "bot")
    assert isinstance(update, ControlUpdate)
    assert update.preference_profile_token == profile_token(profile)
    await ProjectModelController(runtime).apply(session, update, action_id=uuid.uuid4())
    assert row.model_assignments["reviewer_strong"]["profile_id"] == profile.id
    assert row.updated_by_user_id == update.user_id and row.revision == 8


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["revision", "catalog", "topic"])
async def test_stale_or_wrong_topic_callback_cannot_change_assignment(picker, change):
    profile, runtime, row, session = picker
    update = control_update(_Update(f"v3:pm:p:7:lead:{profile_token(profile)}"), "bot")
    assert isinstance(update, ControlUpdate)
    if change == "revision":
        row.revision += 1
    elif change == "catalog":
        runtime.registries.profiles.items.return_value = (
            profile.model_copy(update={"model": "changed"}),
        )
    else:
        runtime.registries.topics.resolve.return_value.kind = TopicKind.TASK_DASHBOARD
    with pytest.raises(ExecutorPreferenceError):
        await ProjectModelController(runtime).apply(session, update, action_id=uuid.uuid4())
    assert row.model_assignments == {}
    session.flush.assert_not_awaited()


@pytest.mark.parametrize(
    "callback",
    [
        "v3:pm:p:7:lead:unknown",
        "v3:pm:p:0:lead:auto",
        "v3:pm:s:7:invalid",
        "v3:pm:p:7:lead",
        "v3:pm:p:7:lead:auto:extra",
        "v3:pm:b:7:extra",
    ],
)
def test_malformed_matrix_callbacks_are_rejected(callback):
    assert control_update(_Update(callback), "bot") is None


@pytest.mark.anyio
async def test_matrix_callback_uses_existing_user_authorization(picker):
    profile, runtime, row, _session = picker
    runtime.settings.allowed_chat_ids = {-100}
    runtime.settings.allowed_user_ids = {999}
    factory = MagicMock()
    update = control_update(_Update(f"v3:pm:p:7:lead:{profile_token(profile)}"), "bot")
    assert isinstance(update, ControlUpdate)
    result = await TelegramControlService(runtime, factory).accept(update)
    assert result.status is IngressStatus.REJECTED
    assert row.model_assignments == {}
    factory.assert_not_called()


@pytest.mark.anyio
async def test_menu_displays_effective_defaults_and_transport_account_labels(picker):
    profile, runtime, _row, session = picker
    runtime.settings.project_lead.profile_id = profile.id
    runtime.settings.project_lead.reviewer_profile_id = profile.id
    runtime.settings.execution.reviewer_profile_id = profile.id
    runtime.settings.execution.default_executor_profile_id = profile.id
    await enqueue_worker_picker(
        session,
        runtime=runtime,
        project_id="project",
        chat_id=-100,
        message_thread_id=42,
        inbox_id=uuid.uuid4(),
    )
    html = session.add.call_args.args[0].payload["html"]
    assert profile.model in html and profile.provider in html
    assert profile.id in html and profile.runtime_identity in html
    assert profile.launch_mode.value in html
    assert "по умолчанию:" in html and ">auto<" not in html
