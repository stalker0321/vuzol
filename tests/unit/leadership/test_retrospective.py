"""Strict retrospective response and byte-bounded historical data."""

import json

import pytest
from pydantic import ValidationError

from vuzol.leadership.provider_model import strict_response_schema
from vuzol.leadership.retrospective_input import MAX_INPUT_BYTES, bound_input
from vuzol.leadership.retrospective_model import RetrospectiveResponse


def test_response_schema_and_rendering():
    response = RetrospectiveResponse.model_validate(
        {
            "summary": "Выбрали устойчивое решение.",
            "decisions": [
                {
                    "title": "Транзакция",
                    "context": "Возможны повторы",
                    "options": ["Одна транзакция", "Раздельные записи"],
                    "choice": "Одна транзакция",
                    "reason": "Защита от частичных записей",
                    "links": {"task_ids": [], "commits": []},
                }
            ],
            "notebook": [],
        }
    )
    assert "Защита от частичных записей" in response.decisions[0].render()
    assert len(response.decisions[0].render()) < 4000
    schema = strict_response_schema(RetrospectiveResponse)
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"decisions", "notebook", "summary"}


@pytest.mark.parametrize(
    "change",
    [
        {"summary": " "},
        {"summary": 12},
        {"extra": "not allowed"},
        {"decisions": [{}] * 9},
        {"notebook": [{"action": "forget", "note_id": "bad"}]},
        {"notebook": [{"action": "remember", "scope": "session", "text": "fact"}]},
        {"notebook": [{"action": "remember", "scope": "project", "text": "я" * 601}]},
        {"notebook": [{"action": "remember", "scope": "global", "text": "fact"}] * 6},
    ],
)
def test_invalid_response(change):
    with pytest.raises(ValidationError):
        RetrospectiveResponse.model_validate(
            {"summary": "Итог.", "decisions": [], "notebook": [], **change}
        )


def test_input_bound_keeps_recent_reviews_and_explicit_truncation():
    context = {
        name: [{"summary": 'я"\\' * 10000, "id": i} for i in range(100)]
        for name in ("results", "plans", "tasks", "events", "packages", "reviews")
    }
    context["goal"] = {"request": "я" * 100000, "scope": "я" * 100000}
    result = bound_input(context)
    encoded = json.dumps(result, ensure_ascii=False).encode()
    assert len(encoded) <= MAX_INPUT_BYTES
    assert json.loads(encoded) == result
    assert result["truncated"] is True
    assert result["reviews"][0]["id"] == 0
    assert "[truncated]" in result["reviews"][0]["summary"]


def test_complete_small_input_and_notebooks_are_preserved():
    context = {
        "goal": {"request": "Fix"},
        "project_notebook": {
            "notes": [{"id": str(i), "text": "Правило"} for i in range(30)],
            "text_bytes": 420,
        },
        "reviews": [{"verdict": "BLOCKED", "summary": "Причина"}],
    }
    assert bound_input(context) == context
