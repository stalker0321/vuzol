"""Product forum layout: names, pin order, project pin intent."""

from vuzol.config import TopicConfig, TopicKind
from vuzol.telegram.layout import (
    STATUS_DASHBOARD_TOPIC_KIND,
    SYSTEM_PINNED_TOPIC_ORDER,
    SYSTEM_TOPIC_DISPLAY_NAMES,
    effective_display_name,
    is_status_dashboard_topic,
    is_system_workspace_kind,
    ordered_pinned_topics,
    project_topic_should_pin_on_create,
    project_topic_should_pin_when_active,
    project_topic_should_pin_when_paused_or_finished,
    system_pin_rank,
    topic_wants_pin,
)


def test_system_pin_order_is_history_status_approvals_inbox() -> None:
    assert SYSTEM_PINNED_TOPIC_ORDER == (
        TopicKind.CHANGELOG,
        TopicKind.TASK_DASHBOARD,
        TopicKind.APPROVALS,
        TopicKind.INBOX,
    )
    assert [SYSTEM_TOPIC_DISPLAY_NAMES[kind] for kind in SYSTEM_PINNED_TOPIC_ORDER] == [
        "История",
        "Статус проектов",
        "Апрувы",
        "Новый проект",
    ]
    assert system_pin_rank(TopicKind.INBOX) == 3
    assert system_pin_rank(TopicKind.SYSTEM) is None


def test_system_display_names_override_registry_text() -> None:
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=1,
        kind=TopicKind.APPROVALS,
        display_name="Whatever local ops typed",
        default_workflow="simple_model_task",
    )
    assert effective_display_name(topic) == "Апрувы"


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
    assert project_topic_should_pin_when_paused_or_finished() is False


def test_ordered_pinned_topics_puts_system_then_projects() -> None:
    topics = (
        TopicConfig(
            chat_id=-100,
            message_thread_id=10,
            kind=TopicKind.PROJECT,
            project_id="alpha",
            display_name="Alpha",
            default_workflow="adaptive_task",
            pinned=True,
        ),
        TopicConfig(
            chat_id=-100,
            message_thread_id=4,
            kind=TopicKind.INBOX,
            default_workflow="simple_model_task",
        ),
        TopicConfig(
            chat_id=-100,
            message_thread_id=1,
            kind=TopicKind.CHANGELOG,
            default_workflow="simple_model_task",
        ),
        TopicConfig(
            chat_id=-100,
            message_thread_id=3,
            kind=TopicKind.APPROVALS,
            default_workflow="simple_model_task",
        ),
        TopicConfig(
            chat_id=-100,
            message_thread_id=2,
            kind=TopicKind.TASK_DASHBOARD,
            default_workflow="simple_model_task",
        ),
        TopicConfig(
            chat_id=-100,
            message_thread_id=11,
            kind=TopicKind.PROJECT,
            project_id="beta",
            display_name="Beta",
            default_workflow="adaptive_task",
            pinned=True,
        ),
        TopicConfig(
            chat_id=-100,
            message_thread_id=5,
            kind=TopicKind.SYSTEM,
            default_workflow="simple_model_task",
        ),
    )
    ordered = ordered_pinned_topics(topics)
    assert [topic.kind for topic in ordered] == [
        TopicKind.CHANGELOG,
        TopicKind.TASK_DASHBOARD,
        TopicKind.APPROVALS,
        TopicKind.INBOX,
        TopicKind.PROJECT,
        TopicKind.PROJECT,
    ]
    assert [topic.project_id for topic in ordered if topic.kind is TopicKind.PROJECT] == [
        "alpha",
        "beta",
    ]


def test_status_dashboard_helpers() -> None:
    assert is_status_dashboard_topic(STATUS_DASHBOARD_TOPIC_KIND)
    assert is_status_dashboard_topic("task_dashboard")
    assert not is_status_dashboard_topic(TopicKind.INBOX)
    assert is_system_workspace_kind(TopicKind.APPROVALS)
    assert project_topic_should_pin_when_active() is True
