"""Project status dashboard projection helpers."""

import uuid
from types import SimpleNamespace

from vuzol.config import TopicKind
from vuzol.storage.types import TaskStatus
from vuzol.telegram.layout import (
    STATUS_DASHBOARD_DISPLAY_NAME,
    STATUS_DASHBOARD_TOPIC_KIND,
    is_status_dashboard_topic,
)
from vuzol.telegram.projections import (
    dashboard_revision_for,
    model_label_for_profile,
    task_number_label,
    task_sense_sentence,
    telegram_html,
)


def test_status_dashboard_binds_to_existing_task_dashboard_kind() -> None:
    """Product policy posts into kind=task_dashboard («Статус проектов»), not a new topic."""

    assert STATUS_DASHBOARD_TOPIC_KIND is TopicKind.TASK_DASHBOARD
    assert STATUS_DASHBOARD_DISPLAY_NAME == "Статус проектов"
    assert is_status_dashboard_topic(TopicKind.TASK_DASHBOARD)
    assert is_status_dashboard_topic("task_dashboard")
    assert not is_status_dashboard_topic(TopicKind.APPROVALS)


def test_task_number_prefers_public_then_local() -> None:
    public = SimpleNamespace(public_task_number=730001, topic_task_number=1)
    local = SimpleNamespace(public_task_number=None, topic_task_number=7)
    missing = SimpleNamespace(public_task_number=None, topic_task_number=None)
    assert task_number_label(public) == "730001"  # type: ignore[arg-type]
    assert task_number_label(local) == "0007"  # type: ignore[arg-type]
    assert task_number_label(missing) == "—"  # type: ignore[arg-type]


def test_model_label_is_english_and_friendly() -> None:
    from vuzol.telegram.projections import model_label_for_profile

    assert model_label_for_profile(None) == "not assigned yet"
    assert (
        model_label_for_profile(
            "codex-subscription-prod", profile_models={"codex-subscription-prod": "codex"}
        )
        == "Codex"
    )
    assert (
        model_label_for_profile(
            "grok-subscription-a", profile_models={"grok-subscription-a": "grok-build"}
        )
        == "Grok"
    )


def test_task_sense_is_one_sentence() -> None:
    task = SimpleNamespace(
        task_draft={
            "normalized_title": "Сделать лендинг. Потом API.",
            "goal": "ignored",
        },
        original_text="fallback",
    )
    assert task_sense_sentence(task) == "Сделать лендинг"  # type: ignore[arg-type]
    long = SimpleNamespace(
        task_draft={"goal": "x" * 200},
        original_text="",
    )
    assert task_sense_sentence(long).endswith("…")  # type: ignore[arg-type]
    assert len(task_sense_sentence(long)) <= 160  # type: ignore[arg-type]


def test_model_label_uses_registry_model() -> None:
    assert model_label_for_profile(None) == "not assigned yet"
    assert model_label_for_profile("other-profile") == "other-profile"
    assert (
        model_label_for_profile("other-profile", profile_models={"other-profile": "gpt-5.1-codex"})
        == "gpt-5.1-codex"
    )


def test_dashboard_revision_changes_with_content() -> None:
    task_a = SimpleNamespace(id=uuid.uuid4(), version=1, status=TaskStatus.EXECUTING)
    task_b = SimpleNamespace(id=task_a.id, version=2, status=TaskStatus.EXECUTING)
    first = dashboard_revision_for([task_a], {task_a.id: "m1"})  # type: ignore[list-item]
    second = dashboard_revision_for([task_b], {task_b.id: "m1"})  # type: ignore[list-item]
    same = dashboard_revision_for([task_a], {task_a.id: "m1"})  # type: ignore[list-item]
    assert first != second
    assert first == same
    assert telegram_html("<x>") == "&lt;x&gt;"
