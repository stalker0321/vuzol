"""Research choices use the nested environment JSON settings boundary."""

import json

from vuzol.config.settings import Settings


def test_research_models_parse_nested_env_json(monkeypatch):
    choices = [
        {"profile_id": "codex-a", "model": "gpt-6.1-terra", "reasoning_effort": "high"},
        {"profile_id": "pi-opencode-go-c"},
    ]
    monkeypatch.setenv("VUZOL_PROJECT_LEAD__RESEARCH_MODELS", json.dumps(choices))
    settings = Settings(_env_file=None)
    assert [
        choice.model_dump(exclude_none=True) for choice in settings.project_lead.research_models
    ] == choices


def test_research_models_default_to_planner_pin():
    assert Settings(_env_file=None).project_lead.research_models == []
