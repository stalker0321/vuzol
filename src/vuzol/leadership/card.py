"""Readable project-lead projection. The criterion id is never presented as proof."""
# ruff: noqa: RUF001

from __future__ import annotations

from collections.abc import Mapping


def open_question(condition: object) -> str | None:
    """The owner's pending question, when this wait is actually an owner wait."""

    if not isinstance(condition, dict) or condition.get("kind") != "owner":
        return None
    question = condition.get("question")
    if not isinstance(question, str) or not question.strip():
        return None
    return question.strip()


def owner_answer_text(constraints: object) -> str | None:
    """The answer bound to an owner question, when one has been accepted."""

    if not isinstance(constraints, dict):
        return None
    raw = constraints.get("owner_answer")
    if not isinstance(raw, dict):
        return None
    answer = raw.get("answer")
    if not isinstance(answer, str) or not answer.strip():
        return None
    return answer.strip()


def render_goal_card(view: Mapping[str, object]) -> str:
    """Status, blocker, package, next operation, and result. Not a status word alone."""

    def line(label: str, key: str) -> str:
        value = view.get(key)
        text = "нет" if value is None or value == "" else str(value)
        return f"{label}: {text}"

    rows = [
        "Руководитель",
        line("Цель", "request"),
        line("Статус", "lifecycle"),
        line("Блокер", "waiting_reason"),
        line("Пакет", "package_line"),
        line("Дальше", "next_operation"),
        line("Результат", "result_summary"),
    ]
    limits = view.get("work_limits")
    if isinstance(limits, dict) and any(value is not None for value in limits.values()):
        packages = limits.get("max_packages")
        items = limits.get("max_items_per_package")
        rows.append(
            f"Лимит: пакетов {packages if packages is not None else 'без ограничения'}, "
            f"задач в пакете {items if items is not None else 'без ограничения'}"
        )
    question = view.get("question")
    if isinstance(question, str) and question.strip():
        rows.append(f"Вопрос: {question.strip()}")
    answer = view.get("owner_answer")
    if isinstance(answer, str) and answer.strip():
        rows.append(f"Ответ: {answer.strip()}")
    return "\n".join(rows)[:3500]


def render_question(question: str, version: int, *, goal_id: str = "") -> str:
    """Owner-facing question bound to the goal version that asked it."""

    return (
        f"Руководитель\nВопрос: {question.strip()[:2500]}\nВерсия: {version}\n"
        f"Дальше: ответ владельца\n/lead_answer {goal_id} {version} <ответ>"
    )
