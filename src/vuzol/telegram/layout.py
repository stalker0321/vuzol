"""Product-level Telegram forum workspace layout.

This is Vuzol policy, not a property of any single live group. Topic routing still
uses stable chat/thread IDs; display names and pin intent are derived here so every
control forum presents the same structure.
"""

from __future__ import annotations

from vuzol.config.models import TopicConfig, TopicKind

# Permanently pinned system topics, top → bottom in the forum pin stack.
SYSTEM_PINNED_TOPIC_ORDER: tuple[TopicKind, ...] = (
    TopicKind.CHANGELOG,
    TopicKind.TASK_DASHBOARD,
    TopicKind.APPROVALS,
    TopicKind.INBOX,
)

# Canonical UI labels. System topics always use these names regardless of TOML overrides.
SYSTEM_TOPIC_DISPLAY_NAMES: dict[TopicKind, str] = {
    TopicKind.CHANGELOG: "История",
    TopicKind.TASK_DASHBOARD: "Статус проектов",
    TopicKind.APPROVALS: "Апрувы",
    TopicKind.INBOX: "Новый проект",
    TopicKind.SYSTEM: "Система",
}

# Optional global roles that exist in the workspace but are not part of the fixed pin stack.
UNPINNED_SYSTEM_TOPIC_KINDS: frozenset[TopicKind] = frozenset({TopicKind.SYSTEM})

# Exclusive product destination for the global in-progress task list.
# Does not create a topic: every control forum already maps this kind in the registry
# (display name «Статус проектов»). Content is chat-scoped but the role is product-global.
STATUS_DASHBOARD_TOPIC_KIND = TopicKind.TASK_DASHBOARD
STATUS_DASHBOARD_DISPLAY_NAME = SYSTEM_TOPIC_DISPLAY_NAMES[STATUS_DASHBOARD_TOPIC_KIND]
DASHBOARD_CARD_TITLE = "Project status"

# Completed-task reports land in «История» (kind=changelog).
HISTORY_TOPIC_KIND = TopicKind.CHANGELOG
HISTORY_TOPIC_DISPLAY_NAME = SYSTEM_TOPIC_DISPLAY_NAMES[HISTORY_TOPIC_KIND]


def is_system_workspace_kind(kind: TopicKind) -> bool:
    return kind in SYSTEM_TOPIC_DISPLAY_NAMES


def is_update_command(text: str | None) -> bool:
    """True for bare ``/update`` (optional @bot suffix), no extra arguments required."""

    if text is None:
        return False
    parts = text.strip().split()
    if not parts:
        return False
    command = parts[0].split("@", 1)[0]
    return command == "/update"


def is_status_dashboard_topic(kind: TopicKind | str) -> bool:
    value = kind.value if isinstance(kind, TopicKind) else kind
    return value == STATUS_DASHBOARD_TOPIC_KIND.value


def effective_display_name(topic: TopicConfig) -> str | None:
    """Resolve the name that Telegram should show for a configured topic."""

    if topic.kind is TopicKind.PROJECT:
        return topic.display_name
    canonical = SYSTEM_TOPIC_DISPLAY_NAMES.get(topic.kind)
    if canonical is not None:
        return canonical
    return topic.display_name


def topic_wants_pin(topic: TopicConfig) -> bool:
    """Desired forum-topic pin state for the product layout.

    System control topics in :data:`SYSTEM_PINNED_TOPIC_ORDER` are always pinned when
    enabled. Project topics pin only when explicitly marked (new active projects);
    pause/complete later clears that mark. ``system`` and other kinds stay unpinned.
    """

    if not topic.enabled:
        return False
    if topic.pinned is not None:
        return topic.pinned
    return topic.kind in SYSTEM_PINNED_TOPIC_ORDER


def project_topic_should_pin_on_create() -> bool:
    """New project topics are pinned immediately after the fixed system stack."""

    return True


def project_topic_should_pin_when_active() -> bool:
    """Active project work keeps the topic pinned (pause/complete clear this later)."""

    return True


def project_topic_should_pin_when_paused_or_finished() -> bool:
    """Paused or finished projects leave the pin stack (lifecycle wiring is later)."""

    return False


def system_pin_rank(kind: TopicKind) -> int | None:
    """0-based rank in the permanent pin stack, or None if the kind is not fixed-pinned."""

    try:
        return SYSTEM_PINNED_TOPIC_ORDER.index(kind)
    except ValueError:
        return None


def ordered_pinned_topics(
    topics: tuple[TopicConfig, ...] | list[TopicConfig],
) -> tuple[TopicConfig, ...]:
    """Topics that should be pinned, ordered for a single chat's pin stack.

    Permanent system topics come first in product order. Project topics that want a
    pin follow in the order they appear in the registry (provisioning appends new
    projects, so newly created topics become the next pin).
    """

    by_chat: dict[int, list[TopicConfig]] = {}
    for topic in topics:
        if not topic_wants_pin(topic):
            continue
        by_chat.setdefault(topic.chat_id, []).append(topic)

    ordered: list[TopicConfig] = []
    for chat_id in sorted(by_chat):
        chat_topics = by_chat[chat_id]
        system = sorted(
            (topic for topic in chat_topics if topic.kind in SYSTEM_PINNED_TOPIC_ORDER),
            key=lambda topic: SYSTEM_PINNED_TOPIC_ORDER.index(topic.kind),
        )
        projects = [topic for topic in chat_topics if topic.kind is TopicKind.PROJECT]
        others = [
            topic
            for topic in chat_topics
            if topic.kind not in SYSTEM_PINNED_TOPIC_ORDER and topic.kind is not TopicKind.PROJECT
        ]
        ordered.extend((*system, *projects, *others))
    return tuple(ordered)
