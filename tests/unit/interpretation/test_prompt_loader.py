"""J3 versioned prompt loader tests."""

from __future__ import annotations

import pytest

from vuzol.interpretation.prompt_loader import (
    PromptError,
    PromptKind,
    PromptStatus,
    compose_prompt,
    load_prompt,
    prompt_hash,
    require_active,
)


def test_prompts_are_versioned_draft_and_hashed() -> None:
    base = load_prompt(PromptKind.BASE, "v1")
    intake = load_prompt(PromptKind.INTAKE, "v1")
    assert base.status is PromptStatus.DRAFT
    assert intake.status is PromptStatus.DRAFT
    assert len(base.content_hash) == 64
    assert base.content_hash == base.content_hash


def test_compose_prompt_is_base_plus_rubric_and_deterministic() -> None:
    composed = compose_prompt(PromptKind.INTAKE)
    assert load_prompt(PromptKind.BASE).body in composed
    assert load_prompt(PromptKind.INTAKE).body in composed
    assert composed == compose_prompt(PromptKind.INTAKE)
    assert prompt_hash(PromptKind.INTAKE) == prompt_hash(PromptKind.INTAKE)
    assert prompt_hash(PromptKind.INTAKE) != prompt_hash(PromptKind.TARGET_RESOLUTION)


def test_require_active_rejects_draft() -> None:
    with pytest.raises(PromptError):
        require_active(load_prompt(PromptKind.INTAKE))


def test_unknown_prompt_fails_closed() -> None:
    with pytest.raises(PromptError):
        load_prompt(PromptKind.INTAKE, "v99")
    with pytest.raises(PromptError):
        load_prompt(PromptKind.WORK_SHAPE, "v1")
