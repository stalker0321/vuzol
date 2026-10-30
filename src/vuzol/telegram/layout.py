"""Product-level Telegram forum workspace layout.

This is Vuzol policy, not a property of any single live group. Topic routing still
uses stable chat/thread IDs; display names and pin intent are derived here so every
control forum presents the same structure.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

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
    TopicKind.APPROVALS: "🔔 Внимание",
    TopicKind.INBOX: "Новый проект",
    TopicKind.SYSTEM: "Система",
}

# Optional global roles that exist in the workspace but are not part of the fixed pin stack.

# Exclusive product destination for the global in-progress task list.
# Does not create a topic: every control forum already maps this kind in the registry
# (display name «Статус проектов»). Content is chat-scoped but the role is product-global.
STATUS_DASHBOARD_TOPIC_KIND = TopicKind.TASK_DASHBOARD
STATUS_DASHBOARD_DISPLAY_NAME = SYSTEM_TOPIC_DISPLAY_NAMES[STATUS_DASHBOARD_TOPIC_KIND]
DASHBOARD_CARD_TITLE = "Статус проектов"

# Completed-task reports land in «История» (kind=changelog).
HISTORY_TOPIC_KIND = TopicKind.CHANGELOG
HELP_CARD_ROLE = "help_card"


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


def is_receipt_command(text: str | None) -> bool:
    """True when the message is a ``/receipt`` command, with or without an amount."""

    if text is None:
        return False
    parts = text.strip().split(maxsplit=1)
    if not parts:
        return False
    return parts[0].split("@", 1)[0] == "/receipt"


def parse_receipt_amount(text: str) -> Decimal:
    """Positive decimal from ``/receipt AMOUNT``. A bad amount is rejected."""

    parts = text.strip().split()
    command = parts[0].split("@", 1)[0] if parts else ""
    if command != "/receipt" or len(parts) != 2:
        raise ValueError("usage: /receipt AMOUNT")
    try:
        amount = Decimal(parts[1])
    except InvalidOperation as error:
        raise ValueError("usage: /receipt AMOUNT") from error
    if not amount.is_finite() or amount <= 0:
        raise ValueError("a measured receipt is not zero")
    return amount


def is_help_command(text: str | None) -> bool:
    """True for bare ``/help`` (optional @bot suffix), with no arguments."""

    if text is None:
        return False
    parts = text.strip().split()
    if len(parts) != 1:
        return False
    command = parts[0].split("@", 1)[0]
    return command == "/help"


def is_model_command(text: str | None) -> bool:
    """True for bare ``/model`` (optional @bot suffix) with no extra arguments."""

    if text is None:
        return False
    parts = text.strip().split()
    if len(parts) != 1:
        return False
    command = parts[0].split("@", 1)[0]
    return command == "/model"


def is_plan_command(text: str | None) -> bool:
    """True for bare ``/plan`` (optional @bot suffix) with no arguments."""

    if text is None:
        return False
    parts = text.strip().split()
    return len(parts) == 1 and parts[0].split("@", 1)[0] == "/plan"


def parse_lead_command(text: str | None) -> tuple[str, str] | None:
    """Return ``(kind, argument)`` for an owner project-lead command."""

    if text is None:
        return None
    parts = text.strip().split(maxsplit=1)
    if not parts:
        return None
    command = parts[0].split("@", 1)[0]
    if command == "/lead":
        argument = parts[1].strip() if len(parts) > 1 else ""
        if not argument:
            return None
        subcommand, _, rest = argument.partition(" ")
        if subcommand == "limits":
            return ("limits", rest.strip())
        return ("message", argument)
    if command in {"/lead_answer", "/lead_limits"}:
        argument = parts[1].strip() if len(parts) > 1 else ""
        if not argument:
            return None
        return ("answer" if command == "/lead_answer" else "limits", argument)
    kinds = {
        "/lead_pause": "pause",
        "/lead_resume": "resume",
        "/lead_cancel": "cancel",
        "/lead_revoke": "revoke",
        "/lead_status": "status",
        "/lead_goal": "goal",
        "/lead_details": "details",
        "/lead_debug": "debug",
        "/lead_memory": "memory",
    }
    kind = kinds.get(command)
    if kind is None or len(parts) != 1:
        return None
    return (kind, "")


def is_import_command(text: str | None) -> bool:
    """True for bare ``/import`` (optional @bot suffix)."""

    if text is None:
        return False
    parts = text.strip().split()
    return len(parts) == 1 and parts[0].split("@", 1)[0] == "/import"


def build_help_card(kind: TopicKind) -> str:
    """Return concise, context-aware help for one forum topic."""

    lines = [
        "<b>Как пользоваться Vuzol</b>",
        "",
    ]
    if kind is TopicKind.PROJECT:
        lines.extend(
            (
                "Задача: текстом, голосом или документом.",
                "<code>/model</code> — модели.",
                "<code>/think</code> — обсуждение; <code>/lead</code> — руководитель.",
                "Режим топика, не команда запуска.",
                "<code>/plan</code> — план.",
                "<code>/lead_status</code> — статус.",
                "<code>/lead_goal</code> — задание.",
                "<code>/lead_details</code> — ход работы.",
                "<code>/lead_debug</code> — диагностика.",
                "<code>/lead_memory</code> — память.",
                "<code>/source</code> — источники.",
            )
        )
    elif kind is TopicKind.INBOX:
        lines.extend(
            (
                "Опишите идею и цель — Vuzol предложит названия и создаст проект.",
                "<code>/import</code> — подключить существующий GitHub-репозиторий.",
            )
        )
    elif kind is TopicKind.TASK_DASHBOARD:
        lines.append("<code>/update</code> — обновить статусы проектов и лимиты.")
    elif kind is TopicKind.APPROVALS:
        lines.extend(
            (
                "Здесь собраны проекты, которым нужно твоё внимание.",
                "🟡 — нужно решение; 🔴 — срочно нужно внимание; ✅ — готово или решено.",
                "Нажми «Открыть», чтобы перейти к вопросу, плану или результату в проекте.",
                "Проектные топики можно отключить от уведомлений. Завершение приходит без звука.",
                "Для отдельных задач здесь по-прежнему появляются карточки принятия результата.",
            )
        )
    elif kind is TopicKind.CHANGELOG:
        lines.append("Здесь сохраняются результаты завершённых задач по всем проектам.")
    else:
        lines.append("Отправьте запрос в подходящий топик проекта или управления.")
    lines.extend(("", "Кнопки под сообщениями управляют задачами."))
    return "\n".join(lines)


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
