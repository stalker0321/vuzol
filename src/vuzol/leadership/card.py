"""Readable project-lead projection. The criterion id is never presented as proof."""
# ruff: noqa: RUF001

from __future__ import annotations

import re
from collections.abc import Mapping
from string import punctuation

_ESCAPES = str.maketrans({character: "\\" + character for character in punctuation})
_PACKAGE_STATES = {
    "draft": "план подготовлен",
    "approved": "готов к запуску",
    "running": "в работе",
    "paused": "на паузе",
    "completed": "завершён",
    "stopped": "остановлен",
    "failed": "не выполнен",
    "blocked": "нужно решение",
}
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
    "lead_profile_unselected": "Нужно выбрать доступную модель руководителя через /model.",
    "lead_model_unavailable": "Обращение к модели не завершилось успешно. Работа не продолжена.",
    "lead_response_invalid": "Модель вернула ответ, который нельзя безопасно применить.",
    "goal_review_invalid": "Независимая проверка не вернула пригодного заключения.",
}
_ERROR_REASONS = {"lead_model_unavailable", "lead_response_invalid", "goal_review_invalid"}


def literal_markdown(value: object) -> str:
    """System layout is Markdown; all goal/model/owner text remains literal data."""
    return str(value).translate(_ESCAPES)


def _literal_block(value: object) -> str:
    """Keep source indentation/blank lines; embedded fences cannot escape the block."""
    text = str(value)
    fence = "`" * max(3, max((len(run) for run in re.findall(r"`+", text)), default=0) + 1)
    return f"{fence}\n{text}\n{fence}"


def _preview(value: object, limit: int = 110) -> str:
    text = " ".join(str(value or "").split())
    return literal_markdown(text if len(text) <= limit else text[: limit - 1].rstrip() + "…")


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
    """Compact ordinary Telegram card. Full authoritative text lives in /lead_goal."""
    lifecycle = view.get("lifecycle")
    reason = view.get("waiting_reason")
    question = view.get("question")
    confirmed = view.get("completion_confirmed") is True
    if lifecycle == "paused":
        heading = "⏸ **НА ПАУЗЕ · Руководитель**"
        explanation = reason_text(reason or "owner_pause")
        next_step = (
            "Нужно новое разрешение владельца; обычное продолжение его не заменяет."
            if reason == "delegation_revoked"
            else "Продолжить: /lead_resume.\nНеопределённые изменения сначала требуют проверки."
        )
    elif lifecycle == "cancelled":
        heading = "⏹ **ОТМЕНЕНО · Руководитель**"
        explanation = "Работа отменена. Сохранённые результаты не удалены."
        next_step = "Продолжение этой цели не запланировано."
    elif lifecycle == "completed" and confirmed:
        heading = "✅ **ГОТОВО · Руководитель**"
        explanation = "Независимая проверка подтвердила все критерии результата."
        next_step = "Цель завершена. Новая работа требует нового поручения."
    elif lifecycle == "completed":
        heading = "🟡 **НУЖНА ПРОВЕРКА · Руководитель**"
        explanation = (
            "Цель помечена завершённой, "
            "но подтверждения всех критериев для текущего результата нет."
        )
        next_step = "Проверь сведения о результате: /lead_details."
    elif lifecycle == "waiting" and reason != "wait_for":
        heading = (
            "🔴 **ОШИБКА · Руководитель**"
            if reason in _ERROR_REASONS
            else "🟡 **НУЖНО РЕШЕНИЕ · Руководитель**"
        )
        explanation = reason_text(reason)
        next_step = (
            "Ответь на вопрос командой выше." if question else "Проверь подробности: /lead_details."
        )
    elif view.get("current_package_status") == "paused":
        heading = "🟡 **ПАКЕТ НА ПАУЗЕ · Руководитель**"
        explanation = "Текущий пакет остановлен. Готовность цели пока не подтверждена."
        next_step = "Проверь причину остановки: /lead_details."
    else:
        heading = "⏳ **В РАБОТЕ · Руководитель**"
        package_status = view.get("current_package_status")
        explanation = (
            "Выполняется текущий пакет работы."
            if package_status == "running"
            else "Руководитель проверяет результат и выбирает следующий шаг."
            if package_status in {"completed", "failed", "blocked", "stopped"}
            else "Руководитель готовит следующий шаг."
        )
        next_step = "Сейчас от тебя ничего не требуется."

    rows = [heading, f"**{_preview(view.get('request')) or 'Текущая цель'}**", explanation]
    if isinstance(question, str) and question.strip():
        rows.append("**Вопрос**\n" + literal_markdown(question.strip()))
        version = view.get("question_version")
        if version is not None and view.get("goal_id"):
            rows.append(_answer_instruction(str(view["goal_id"]), version))
    result = "Готовность пока не подтверждена."
    covered, missing = view.get("covered"), view.get("missing")
    if isinstance(covered, list) and isinstance(missing, list) and covered:
        result = f"Подтверждено критериев: {len(covered)} из {len(covered) + len(missing)}."
    commit = view.get("result_commit")
    if isinstance(commit, str) and commit:
        result += f"\nКоммит: {literal_markdown(commit[:12])}"
    rows.append("**Результат**\n" + result)
    rows.append("**Дальше**\n" + next_step)
    if view.get("owner_answer"):
        rows.append("*Твой ответ сохранён. Полный текст — в подробностях.*")
    if view.get("current_package_status"):
        status = _PACKAGE_STATES.get(str(view["current_package_status"]), "состояние уточняется")
        title = _preview(view.get("package_title"), 80)
        rows.append(f"*Пакет: {title + ' · ' if title else ''}{status}.*")
    rows.append("/lead_goal — полное задание\n/lead_details — подробности")
    return "\n\n".join(rows)


def _answer_instruction(goal_id: str, version: object) -> str:
    command = f"/lead_answer {goal_id} {version} <ответ>"
    return "Замени «<ответ>» своим текстом:\n\n" + _literal_block(command)


def render_question(question: str, version: int, *, goal_id: str = "") -> str:
    """Owner-facing question bound to the goal version that asked it."""

    return "\n\n".join(
        (
            "🟡 **НУЖНО РЕШЕНИЕ · Руководитель**",
            "**Вопрос**\n" + literal_markdown(question.strip()),
            _answer_instruction(goal_id, version),
            "*Ответ относится именно к этому вопросу. Он не возобновляет работу на паузе.*",
        )
    )


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


def render_goal_details(view: Mapping[str, object]) -> str:
    rows = [render_goal_card(view), "**Технические подробности**"]
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
