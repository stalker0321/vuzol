"""Tests for the discussion agent reply unwrapping sanitizer."""

from vuzol.discussion.agent import unwrap_agent_reply


def test_clean_markdown_reply_is_returned_unchanged() -> None:
    reply = "**Готово**\n\n- пункт один\n- пункт два"

    assert unwrap_agent_reply(reply) == reply


def test_trailing_json_envelope_after_prose_is_unwrapped() -> None:
    reply = (
        "Ок, «го» — фиксирую scope PR и быстро сверю по коду, что именно трогаем."  # noqa: RUF001
        '{\n  "reply": "**Принято — делаем одним PR**\\n\\nScope зафиксирован."}'
    )

    assert unwrap_agent_reply(reply) == "**Принято — делаем одним PR**\n\nScope зафиксирован."


def test_bare_envelope_is_unwrapped() -> None:
    reply = '{"reply": "Готово к реализации."}'

    assert unwrap_agent_reply(reply) == "Готово к реализации."


def test_fenced_envelope_is_unwrapped() -> None:
    reply = '```json\n{"reply": "План согласован."}\n```'

    assert unwrap_agent_reply(reply) == "План согласован."


def test_nested_envelopes_unwrap_to_innermost() -> None:
    reply = '{"reply": "Черновик.{\\"reply\\": \\"Финальный ответ.\\"}"}'

    assert unwrap_agent_reply(reply) == "Финальный ответ."


def test_json_without_single_reply_key_is_left_alone() -> None:
    reply = 'Пример структуры:\n{"verdict": "pass", "warnings": []}'

    assert unwrap_agent_reply(reply) == reply


def test_invalid_trailing_json_is_left_alone() -> None:
    reply = "Мысль вслух: {не json, а просто скобка"  # noqa: RUF001

    assert unwrap_agent_reply(reply) == reply
