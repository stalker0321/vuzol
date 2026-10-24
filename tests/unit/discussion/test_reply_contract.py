"""Chat is text, while machine-readable worker and review contracts stay strict."""

import json

import pytest

from vuzol.discussion.agent import normalize_discussion_reply


@pytest.mark.parametrize(
    "text",
    [
        "**Ответ**\n\nPlain Markdown.",
        'Example format:\n{"reply":"example, not the entire answer"}',
        '```json\n{"example": true}\n```',
        '```python\nprint("ok")\n```',
        '[{"reply":"элемент массива"}]',
    ],
)
def test_plain_text_is_preserved_verbatim(text: str) -> None:
    assert normalize_discussion_reply(text).reply == text


@pytest.mark.parametrize("fenced", [False, True])
def test_legacy_envelope_and_optional_memory(fenced: bool) -> None:
    text = '{"reply":"Ответ", "memory_summary":"Память"}'
    if fenced:
        text = f"```json\n{text}\n```"
    result = normalize_discussion_reply(text)
    assert result.reply == "Ответ"
    assert result.memory_summary == "Память"


def test_bad_optional_memory_does_not_destroy_answer() -> None:
    result = normalize_discussion_reply('{"reply":"Ответ", "memory_summary":17}')
    assert result.reply == "Ответ"
    assert result.memory_summary is None


@pytest.mark.parametrize("text", [None, "", " \n\t"])
def test_empty_answer_is_not_success(text: str | None) -> None:
    with pytest.raises(ValueError):
        normalize_discussion_reply(text)


def test_structured_contract_remains_strict() -> None:
    with pytest.raises(ValueError):
        normalize_discussion_reply(None, {"reply": "Ответ", "unknown": True})


@pytest.mark.parametrize("structured", [False, True])
@pytest.mark.parametrize("scope", [None, "Только поиск"])
def test_handoff_envelope(structured, scope):
    brief = {"request": "Добавь поиск", "scope": scope, "criteria": ["Поиск по названию"]}
    payload = {"reply": "Согласовали.", "memory_summary": "Нужен поиск", "handoff": brief}
    result = (
        normalize_discussion_reply(None, payload)
        if structured
        else normalize_discussion_reply(json.dumps(payload))
    )
    assert result.reply == "Согласовали."
    assert result.memory_summary == "Нужен поиск"
    assert result.handoff.model_dump() == brief


@pytest.mark.parametrize("structured", [False, True])
@pytest.mark.parametrize(
    "brief",
    [
        None,
        "сделай",
        [],
        {},
        {"request": "", "criteria": []},
        {"request": "два\nдействия", "criteria": []},  # noqa: RUF001
        {"request": 1, "criteria": []},
        {"request": "Сделай", "scope": {}, "criteria": []},
        {"request": "Сделай", "criteria": "готово"},
        {"request": "Сделай", "criteria": [17]},
        {"request": "Сделай", "criteria": [" "]},
        {"request": "Сделай", "criteria": [], "start": True},
    ],
)
def test_malformed_handoff_never_loses_reply(structured, brief):
    payload = {"reply": "Ответ", "handoff": brief}
    result = (
        normalize_discussion_reply(None, payload)
        if structured
        else normalize_discussion_reply(json.dumps(payload))
    )
    assert result.reply == "Ответ"
    assert result.handoff is None


def test_handoff_allows_empty_criteria():
    result = normalize_discussion_reply(
        None, {"reply": "Ответ", "handoff": {"request": "Сделай", "criteria": []}}
    )
    assert result.handoff.scope is None
    assert result.handoff.criteria == []
