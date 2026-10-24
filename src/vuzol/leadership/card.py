"""Readable project-lead projection. The criterion id is never presented as proof."""
# ruff: noqa: RUF001

from __future__ import annotations

import re
from collections.abc import Mapping
from string import punctuation

from vuzol.leadership.text import owner_text, short_owner_text

_ESCAPES = str.maketrans({character: "\\" + character for character in punctuation})
_REASONS = {
    "owner": "Нужно твоё решение, чтобы продолжить.",
    "owner_pause": "Ты поставил работу на паузу.",
    "delegation_revoked": "Разрешение руководителю отозвано.",
    "feature_disabled": "Руководитель отключён в настройках.",
    "work_package_limit_reached": "Достигнут заданный тобой лимит пакетов работы.",
    "work_item_limit_reached": "Предложенный пакет превышает заданный тобой лимит задач.",
    "repeat_without_new_evidence": "Повторный план не добавляет новых доказательств результата.",
    "continuation_without_progress": "Несколько ходов подряд не дали нового результата.",
    "model_idle": "Руководитель не выбрал следующий шаг.",
    "lead_inference_reconciliation": (
        "Исход прошлого обращения к модели нужно проверить. Повтор не запущен."
    ),
    "lead_budget_exceeded": "Лимит расходов не позволяет сделать следующий вызов модели.",
    "lead_context_limit": "Материалы не помещаются в доступный контекст руководителя.",
    "goal_review_context_limit": "Материалы не помещаются в доступный контекст проверки.",
    "lead_model_unconfigured": "Модель руководителя не подключена.",
    "lead_profile_unselected": "Нужно подключить доступную модель руководителя.",
    "lead_model_unavailable": "Обращение к модели не завершилось успешно. Работа не продолжена.",
    "lead_response_invalid": "Модель вернула ответ, который нельзя безопасно применить.",
    "goal_review_invalid": "Независимая проверка не вернула пригодного заключения.",
    "goal_review_unavailable": "Не удалось выполнить независимую приёмку цели. Результат сохранён.",
}
_ERROR_REASONS = {
    "lead_model_unavailable",
    "lead_response_invalid",
    "goal_review_invalid",
    "goal_review_unavailable",
}


def literal_markdown(value: object) -> str:
    """System layout is Markdown; all goal/model/owner text remains literal data."""
    return str(value).translate(_ESCAPES)


def _literal_block(value: object) -> str:
    """Keep source indentation/blank lines; embedded fences cannot escape the block."""
    text = str(value)
    fence = "`" * max(3, max((len(run) for run in re.findall(r"`+", text)), default=0) + 1)
    return f"{fence}\n{text}\n{fence}"


def _preview(value: object, limit: int = 110) -> str:
    return literal_markdown(short_owner_text(value, limit))


def reason_text(reason: object) -> str:
    return _REASONS.get(str(reason), "Продолжение остановлено. Нужно уточнить следующий шаг.")


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
    """One live status: current work and the owner's next action."""
    lifecycle, reason = view.get("lifecycle"), view.get("waiting_reason")
    if lifecycle == "paused":
        heading = "⏸ **НА ПАУЗЕ · Руководитель**"
        now = reason_text(reason or "owner_pause")
        action = "Когда будешь готов, разреши продолжить работу."
    elif lifecycle == "cancelled":
        heading = "⏹ **ОТМЕНЕНО · Руководитель**"
        now = "Работа отменена. Сохранённые результаты не удалены."
        action = "Сейчас от тебя ничего не требуется."
    elif lifecycle == "completed":
        confirmed = view.get("completion_confirmed") is True
        heading = (
            "✅ **ГОТОВО · Руководитель**" if confirmed else "🟡 **НУЖНА ПРОВЕРКА · Руководитель**"
        )
        now = (
            "Независимая проверка подтвердила результат."
            if confirmed
            else "Готовность результата пока не подтверждена."
        )
        action = "Можно дать новое поручение." if confirmed else "Нужно проверить результат."
    elif reason == "plan_approval":
        heading = "🟡 **ПЛАН ГОТОВ · Руководитель**"
        now = "План подготовлен. Работа ещё не запущена."
        action = "Выбери действие под карточкой плана."
    elif lifecycle == "waiting" and reason != "wait_for":
        heading = (
            "🔴 **ОШИБКА · Руководитель**"
            if reason in _ERROR_REASONS
            else "🟡 **НУЖНО РЕШЕНИЕ · Руководитель**"
        )
        now = reason_text(reason)
        action = (
            "Ответь на сообщение с вопросом." if view.get("question") else "Уточни следующий шаг."
        )
    elif view.get("current_package_status") == "paused":
        heading = "🟡 **РАБОТА ПРИОСТАНОВЛЕНА · Руководитель**"
        now = "Текущая задача остановлена. Готовность цели пока не подтверждена."
        action = "Нужно уточнить следующий шаг."
    else:
        heading = "⏳ **В РАБОТЕ · Руководитель**"
        now = (
            "Выполняю задачи по плану."
            if view.get("current_package_status") == "running"
            else "Проверяю результат и выбираю следующий шаг."
            if view.get("current_package_status") in {"completed", "failed", "blocked", "stopped"}
            else "Готовлю план работы."
        )
        action = "Сейчас от тебя ничего не требуется."
    return "\n\n".join((heading, _preview(view.get("request")) or "Текущая цель", now, action))


def render_question(
    question: str, version: int, *, goal_id: str = "", has_options: bool = False
) -> str:
    """Identity/version live in the delivery binding, never in the visible text."""
    return "\n\n".join(
        (
            "🟡 **ВОПРОС · Руководитель**",
            literal_markdown(owner_text(question)),
            "Ответь на это сообщение или выбери кнопку ниже."
            if has_options
            else "Ответь на это сообщение.",
        )
    )


def render_goal_details(view: Mapping[str, object]) -> str:
    covered, missing = view.get("covered"), view.get("missing")
    done = len(covered) if isinstance(covered, list) else 0
    left = len(missing) if isinstance(missing, list) else 0
    reason = view.get("waiting_reason")
    blocker = (
        "Нужно одобрить план кнопкой под его карточкой."
        if reason == "plan_approval"
        else owner_text(view["question"])
        if view.get("question")
        else reason_text(reason)
        if reason and reason != "wait_for"
        else "Сейчас препятствий нет."
    )
    rows = [
        "**ПОДРОБНОСТИ · Руководитель**",
        "**Цель:** " + _preview(view.get("request"), 240),
        f"**Сделано:** подтверждено условий готовности — {done}.",
        f"**Осталось:** проверить условий — {left}."
        if left
        else "**Осталось:** подтвердить итоговый результат."
        if not view.get("completion_confirmed")
        else "**Осталось:** работа завершена.",
        "**Что мешает:** " + literal_markdown(blocker),
        "**Что можно сделать:** "
        + (
            "выбрать действие под карточкой плана."
            if reason == "plan_approval"
            else "ответить на сообщение с вопросом."
            if view.get("question")
            else "разрешить продолжение работы."
            if view.get("lifecycle") == "paused"
            else "написать руководителю, если нужно изменить задачу."
        ),
    ]
    progress = view.get("progress")
    if isinstance(progress, list) and progress:
        done_items = [item for item in progress if item.get("status") == "completed"]
        left_items = [item for item in progress if item.get("status") != "completed"]
        rows[2] = "**Сделано:** " + (
            "; ".join(_preview(item["summary"], 240) for item in done_items)
            or "пока нет завершённых задач."
        )
        rows[3] = "**Осталось:** " + (
            "; ".join(_preview(item["summary"], 240) for item in left_items)
            or "проверить итоговый результат."
        )
    if view.get("owner_answer"):
        rows.append(
            "**Твой последний ответ**\n\n" + _literal_block(owner_text(view["owner_answer"]))
        )
    return "\n\n".join(rows)


def render_goal_contract(view: Mapping[str, object]) -> str:
    """Lossless source text: no preview clipping, inferred summary or model HTML."""
    rows = ["**ПОЛНОЕ ЗАДАНИЕ · Руководитель**", _literal_block(view.get("request") or "")]
    if view.get("scope") and view["scope"] != view.get("request"):
        rows.append("**Область работы**\n\n" + _literal_block(view["scope"]))
    criteria = view.get("criteria")
    if isinstance(criteria, list):
        rows.append(
            "**Критерии готовности**\n\n"
            + "\n\n".join(
                f"Критерий {index}\n\n{_literal_block(item['description'])}"
                for index, item in enumerate(criteria, 1)
                if isinstance(item, dict) and "description" in item
            )
        )
    rows.append("/lead_status — текущее состояние")
    return "\n\n".join(rows)


def render_goal_debug(view: Mapping[str, object]) -> str:
    rows = ["**Технические подробности**"]
    for label, key in (
        ("Цель", "goal_id"),
        ("Версия", "version"),
        ("Версия задания", "scope_revision"),
        ("Состояние", "lifecycle"),
        ("Причина", "waiting_reason"),
        ("Пакет", "current_package_id"),
        ("Ветка", "integration_branch"),
        ("Коммит", "result_commit"),
    ):
        if view.get(key) is not None:
            rows.append(f"{label}: {literal_markdown(view[key])}")
    limits = view.get("work_limits")
    if isinstance(limits, dict):
        rows.append(
            "Лимиты: "
            + "; ".join(
                f"{label} — "
                + (
                    literal_markdown(limits[key])
                    if limits.get(key) is not None
                    else "без ограничения"
                )
                for label, key in (
                    ("пакетов", "max_packages"),
                    ("задач в пакете", "max_items_per_package"),
                )
            )
        )
    covered = view.get("covered")
    criteria = view.get("criteria")
    if isinstance(covered, list) and isinstance(criteria, list):
        rows.append(
            "**Проверка критериев**\n"
            + "\n".join(
                ("Подтверждён: " if item.get("id") in covered else "Не подтверждён: ")
                + literal_markdown(item.get("description", ""))
                for item in criteria
                if isinstance(item, dict)
            )
        )
    if view.get("owner_answer"):
        rows.append("**Твой последний ответ**\n\n" + _literal_block(view["owner_answer"]))
    return "\n\n".join(rows)
