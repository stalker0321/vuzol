"""Product forum layout: names, pin order, project pin intent."""

from vuzol.config import TopicConfig, TopicKind
from vuzol.telegram.layout import (
    DASHBOARD_CARD_TITLE,
    STATUS_DASHBOARD_TOPIC_KIND,
    effective_display_name,
    is_plan_command,
    is_status_dashboard_topic,
    is_system_workspace_kind,
    project_topic_should_pin_on_create,
    topic_wants_pin,
)


def test_plan_command_accepts_only_bare_command() -> None:
    assert is_plan_command("/plan") is True
    assert is_plan_command("/plan@vuzol_bot") is True
    assert is_plan_command("/plan now") is False
    assert is_plan_command(None) is False


def test_dashboard_title_is_canonical_russian_label() -> None:
    assert DASHBOARD_CARD_TITLE == "Статус проектов"


def test_system_display_names_override_registry_text() -> None:
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=1,
        kind=TopicKind.APPROVALS,
        display_name="Whatever local ops typed",
        default_workflow="simple_model_task",
    )
    assert effective_display_name(topic) == "🔔 Внимание"


def test_project_display_name_is_project_owned() -> None:
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=9,
        kind=TopicKind.PROJECT,
        display_name="Bill Buddy",
        project_id="bill-buddy",
        default_workflow="adaptive_task",
        pinned=True,
    )
    assert effective_display_name(topic) == "Bill Buddy"


def test_topic_wants_pin_policy() -> None:
    inbox = TopicConfig(
        chat_id=-100,
        message_thread_id=1,
        kind=TopicKind.INBOX,
        default_workflow="simple_model_task",
    )
    system = TopicConfig(
        chat_id=-100,
        message_thread_id=2,
        kind=TopicKind.SYSTEM,
        default_workflow="simple_model_task",
    )
    project_active = TopicConfig(
        chat_id=-100,
        message_thread_id=3,
        kind=TopicKind.PROJECT,
        project_id="notes",
        display_name="Notes",
        default_workflow="adaptive_task",
        pinned=True,
    )
    project_paused = TopicConfig(
        chat_id=-100,
        message_thread_id=4,
        kind=TopicKind.PROJECT,
        project_id="old",
        display_name="Old",
        default_workflow="adaptive_task",
        pinned=False,
    )
    project_legacy = TopicConfig(
        chat_id=-100,
        message_thread_id=5,
        kind=TopicKind.PROJECT,
        project_id="legacy",
        display_name="Legacy",
        default_workflow="adaptive_task",
    )
    assert topic_wants_pin(inbox) is True
    assert topic_wants_pin(system) is False
    assert topic_wants_pin(project_active) is True
    assert topic_wants_pin(project_paused) is False
    assert topic_wants_pin(project_legacy) is False
    assert project_topic_should_pin_on_create() is True


def test_status_dashboard_helpers() -> None:
    assert is_status_dashboard_topic(STATUS_DASHBOARD_TOPIC_KIND)
    assert is_status_dashboard_topic("task_dashboard")
    assert not is_status_dashboard_topic(TopicKind.INBOX)
    assert is_system_workspace_kind(TopicKind.APPROVALS)
