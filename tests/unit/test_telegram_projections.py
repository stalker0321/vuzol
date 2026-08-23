# ruff: noqa: RUF001

import asyncio
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from vuzol.storage.types import ApprovalStatus, StepStatus, TaskStatus, WorktreeDeliveryState
from vuzol.telegram.projections import (
    EditRateLimiter,
    _approval_buttons,
    _approval_display_summary,
    _approval_fact_lines,
    _approval_status_label,
    _concise_completion_report,
    _format_bytes,
    _format_duration_ru,
    _humanize_model_slug,
    _task_identity_footer,
    delivery_state_label,
    format_executor_model,
    model_label_for_profile,
    split_message,
    status_buttons,
    step_status_label,
    step_type_label,
    task_sense_sentence,
    telegram_html,
    user_status_label,
)
from vuzol.workflows.definitions import WORKFLOW_DEFINITIONS


def test_html_is_escaped_and_messages_are_bounded() -> None:
    assert telegram_html('<b x="1">&') == "&lt;b x=&quot;1&quot;&gt;&amp;"
    assert split_message("abcdef", limit=2) == ("ab", "cd", "ef")
    assert split_message("") == ("",)
    with pytest.raises(ValueError):
        split_message("x", limit=0)


def test_edit_rate_limiter_serializes_one_projection() -> None:
    async def scenario() -> None:
        limiter = EditRateLimiter(2)
        task_id = uuid.uuid4()
        now = datetime.now(UTC)
        assert await limiter.reserve(task_id, now) == now
        assert (await limiter.reserve(task_id, now) - now).total_seconds() == 2
        assert await limiter.reserve(uuid.uuid4(), now) == now

    asyncio.run(scenario())


def test_all_status_and_delivery_values_have_russian_labels() -> None:
    for task_status in TaskStatus:
        assert user_status_label(task_status)
        assert user_status_label(task_status) != task_status.value
    for step_status in StepStatus:
        assert step_status_label(step_status)
        assert step_status_label(step_status) != step_status.value
    for state in WorktreeDeliveryState:
        assert delivery_state_label(state)
        assert delivery_state_label(state) != state.value


def test_all_registered_workflow_steps_have_dedicated_labels() -> None:
    step_types = {
        step.step_type for definition in WORKFLOW_DEFINITIONS for step in definition.steps
    }
    for step_type in step_types:
        assert step_type_label(step_type) != step_type


def test_unknown_labels_are_safely_escaped() -> None:
    assert user_status_label("<future>") == "&lt;future&gt;"
    assert step_status_label("<future>") == "&lt;future&gt;"
    assert step_type_label("<future>") == "&lt;future&gt;"
    assert delivery_state_label("<future>") == "&lt;future&gt;"


def test_russian_duration_format() -> None:
    assert _format_duration_ru(0) == "0 с"
    assert _format_duration_ru(45) == "45 с"
    assert _format_duration_ru(125) == "2 мин 5 с"
    assert _format_duration_ru(3723) == "1 ч 2 мин"


def test_approval_decision_labels_are_russian() -> None:
    assert _approval_status_label(ApprovalStatus.APPROVED) == "Принято"
    assert _approval_status_label(ApprovalStatus.CONSUMED) == "Принято"
    assert _approval_status_label(ApprovalStatus.REJECTED) == "Отклонено"
    assert _approval_status_label(ApprovalStatus.EXPIRED) == "Истекло"


def test_approval_summary_hides_legacy_provider_transcript() -> None:
    transcript = "I'll inspect first. I'll run tests next. Next I'll deploy. " * 20
    assert _approval_display_summary(transcript) == (
        "Изменения подготовлены и прошли настроенные проверки."
    )
    assert _approval_display_summary("## Готово") == "Готово"


def test_approval_facts_show_trusted_artifact_types() -> None:
    lines = _approval_fact_lines(
        {
            "gates": [],
            "artifact_evidence": {
                "artifacts": [
                    {"artifact_type": "cli_transcript"},
                    {"artifact_type": "cli_transcript_evidence"},
                ]
            },
        },
        "CLI готов",
    )

    assert "✅ Артефакты: <code>cli_transcript, cli_transcript_evidence</code>" in lines


def test_capability_approval_explains_separate_offline_installation() -> None:
    lines = _approval_fact_lines(
        {
            "schema_version": "capability-provisioning-approval.v1",
            "bundles": [
                {
                    "capability_key": "android-sdk",
                    "version": "35.0.0",
                    "archive_bytes": 2 * 1024 * 1024,
                    "archive_sha256": "a" * 64,
                    "source_provider": "Google Android",
                }
            ],
        },
        "Установить Android SDK",
    )
    approval = MagicMock(requested_action="install_capabilities")

    assert any("android-sdk" in line for line in lines)
    assert any("35.0.0" in line for line in lines)
    assert any("Google Android" in line for line in lines)
    assert any("2.0 МБ" in line for line in lines)
    assert _approval_buttons(approval) == ("approve", "reject")


def test_dependency_approval_explains_registry_and_immutable_environment() -> None:
    lines = _approval_fact_lines(
        {
            "schema_version": "dependency-provisioning-approval.v1",
            "requirements": [
                {
                    "ecosystem": "python",
                    "registry_provider": "Python Packaging Authority",
                    "manifest_sha256": "b" * 64,
                    "direct_dependencies": ["httpx==1.0", "pydantic==2.0"],
                    "custom_sources": [
                        {
                            "package_name": "internal-demo",
                            "source_kind": "git",
                            "source_url": "https://github.com/acme/internal-demo.git",
                            "source_pin": "c" * 40,
                        }
                    ],
                }
            ],
        },
        "Собрать зависимости Python",
    )
    approval = MagicMock(requested_action="install_dependencies")

    assert any("python" in line and "зависимостей: 2" in line for line in lines)
    assert any("Python Packaging Authority" in line for line in lines)
    assert any("internal-demo" in line and "github.com" in line for line in lines)
    assert any("только для чтения" in line for line in lines)
    assert _approval_buttons(approval) == ("approve", "reject")


def test_task_status_button_matrix_is_exhaustive_and_has_no_retry_ui() -> None:
    pause_cancel = {
        TaskStatus.RECEIVED,
        TaskStatus.CONTEXT_PREPARED,
        TaskStatus.PLANNED,
        TaskStatus.WAITING_APPROVAL,
        TaskStatus.EXECUTING,
        TaskStatus.VALIDATING,
        TaskStatus.REVIEWING,
        TaskStatus.RETRYING,
    }
    expected = {status: ("pause", "cancel") for status in pause_cancel} | {
        TaskStatus.PAUSED: ("resume", "cancel")
    }

    for status in TaskStatus:
        buttons = tuple(status_buttons(status.value))
        assert buttons == expected.get(status, ())
        assert "retry" not in buttons


def test_task_sense_sentence_cuts_at_first_separator_and_bounds_length() -> None:
    def sense(draft: object, original: str = "") -> str:
        return task_sense_sentence(SimpleNamespace(task_draft=draft, original_text=original))

    assert sense({"task_summary": "Add retries. Then backoff!"}) == "Add retries"
    assert sense({"normalized_title": "Stop! Hammer time"}) == "Stop"
    # Whitespace collapses before separator search, so newlines never split.
    assert sense({"goal": "Fix\nthe\nleak"}) == "Fix the leak"
    assert sense({}, original="Ship it. Today") == "Ship it"
    long = "слово " * 40
    bounded = sense({"task_summary": long})
    assert bounded.endswith("…")
    assert len(bounded) <= 158
    assert sense("not-a-dict") == "Без описания"
    assert sense({"task_summary": "   "}) == "Без описания"
    assert sense({"task_summary": ".!?."}) == "Без описания"


def test_numberless_tasks_show_stable_short_identity_code() -> None:
    task_id = uuid.uuid4()
    footer = _task_identity_footer(
        SimpleNamespace(public_task_number=None, topic_task_number=None, id=task_id)
    )
    assert footer == f"<i>код ·{task_id.hex[-8:]}</i>"


def test_executor_labels_infer_provider_family_from_profile_or_model() -> None:
    assert model_label_for_profile(None) == "ещё не назначен"
    assert model_label_for_profile("grok-build-prod") == "Grok Build"
    assert (
        model_label_for_profile(
            "api",
            profile_models={"api": "qwen3-max"},
            profile_efforts={"api": "high"},
        )
        == "Qwen3 Max · high"
    )
    # A step model without a codex/grok profile renders as a generic humanized slug.
    assert model_label_for_profile("api", profile_models={"api": "x"}, model="gpt-5.6-sol") == (
        "GPT-5.6 Sol"
    )

    assert format_executor_model(None, profile_id="api") == "api"
    assert format_executor_model("", profile_id="api", effort="high") == "api · high"
    assert format_executor_model("grok-build") == "Grok Build"
    assert format_executor_model("grok") == "Grok"
    assert format_executor_model("kimi-k2") == "Kimi K2"
    assert format_executor_model("gpt-5.6-mini", provider="codex") == "Codex GPT-5.6 Mini"
    assert format_executor_model("o3-pro", provider="codex") == "Codex O3 Pro"


def test_model_slug_humanizer_drops_calendar_versions_and_keeps_case() -> None:
    assert _humanize_model_slug("grok-build-2025-08-07") == "Grok Build"
    assert _humanize_model_slug("gpt") == "GPT"
    assert _humanize_model_slug("gpt-5.1") == "GPT-5.1"
    assert _humanize_model_slug("Claude-Sonnet-4") == "Claude Sonnet 4"


def test_byte_formatter_uses_binary_units() -> None:
    assert _format_bytes(2 * 1024**3) == "2.0 ГБ"
    assert _format_bytes(1536 * 1024) == "1.5 МБ"
    assert _format_bytes(2048) == "2.0 КБ"
    assert _format_bytes(512) == "512 Б"


def test_completion_report_keeps_bounded_bullets_and_drops_handoff_sections() -> None:
    report = _concise_completion_report(
        "Auth rewritten.\n"
        "\n"
        "Реализовано:\n"
        "- added retry loop\n"
        "- hardened parser\n"
        "- third fix\n"
        "- fourth fix\n"
        "- fifth fix\n"
        "- sixth fix\n"
        "- seventh fix\n"
        "\n"
        "Следующие шаги:\n"
        "- deploy to prod",
        bullet_limit=6,
    )

    assert report.startswith("Auth rewritten.")
    assert "• seventh fix" not in report
    for bullet in ("added retry loop", "hardened parser", "sixth fix"):
        assert f"• {bullet}" in report
    assert "Следующие шаги" not in report and "deploy" not in report

    assert _concise_completion_report("\n \n") == "Без описания"
