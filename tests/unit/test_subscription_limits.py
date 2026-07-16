"""Subscription limit collectors for Codex/Grok dashboard section."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from vuzol.config.models import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.providers.subscription_limits import (
    LimitWindow,
    SubscriptionLimitSnapshot,
    _classify_codex_window,
    _human_plan,
    _windows_from_codex_rate_limit,
    collect_profile_limits,
    format_subscription_limits_html,
    subscription_profiles,
)
from vuzol.telegram.projections import telegram_html


def _cli_profile(profile_id: str, provider: str, state: Path) -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id=profile_id,
        provider=provider,
        model="test-model",
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.STRONG,
        roles=frozenset({ProviderRole.EXECUTOR}),
        routing_priority=100,
        supported_task_types=frozenset({"coding"}),
        sandbox_required=True,
        runtime_identity=f"id-{profile_id}",
        state_directory=state,
        enabled=True,
    )


def test_human_plan_labels() -> None:
    assert _human_plan("codex", "plus") == "Plus"
    assert _human_plan("grok", "SuperGrok") == "Super"
    assert _human_plan("grok", "supergrok_heavy") == "Super Heavy"


def test_codex_window_classification() -> None:
    observed = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    five_raw = {
        "used_percent": 40,
        "limit_window_seconds": 18_000,
        "reset_after_seconds": 3_600,
    }
    week_raw = {
        "used_percent": 72,
        "limit_window_seconds": 604_800,
        "reset_at": int(datetime(2026, 7, 19, 15, 0, tzinfo=UTC).timestamp()),
    }
    five = _classify_codex_window(five_raw, observed)
    week = _classify_codex_window(week_raw, observed)
    assert five is not None and five[0] == "five_hour"
    assert five[1].remaining_percent == 60
    assert week is not None and week[0] == "weekly"
    assert week[1].remaining_percent == 28
    mapped = _windows_from_codex_rate_limit(
        {"primary_window": week_raw, "secondary_window": five_raw}, observed
    )
    assert mapped[0].remaining_percent == 60
    assert mapped[1].remaining_percent == 28


def test_collect_codex_from_auth_and_http(tmp_path: Path, monkeypatch) -> None:
    state = tmp_path / "codex"
    state.mkdir()
    (state / "auth.json").write_text(
        json.dumps({"tokens": {"access_token": "tok", "account_id": "acc"}}),
        encoding="utf-8",
    )
    profile = _cli_profile("codex-subscription-prod", "codex", state)

    def fake_http(url: str, *, headers: dict[str, str]):
        assert "Bearer tok" in headers["Authorization"]
        return {
            "plan_type": "plus",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 10,
                    "limit_window_seconds": 604_800,
                    "reset_at": 1_800_000_000,
                },
                "secondary_window": {
                    "used_percent": 25,
                    "limit_window_seconds": 18_000,
                    "reset_after_seconds": 100,
                },
            },
        }

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        fake_http,
    )
    snap = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap.ok
    assert snap.company == "OpenAI"
    assert snap.plan_label == "Plus"
    assert snap.five_hour.remaining_percent == 75
    assert snap.weekly.remaining_percent == 90


def test_collect_grok_from_local_billing_log(tmp_path: Path) -> None:
    state = tmp_path / "grok"
    logs = state / "logs"
    logs.mkdir(parents=True)
    line = {
        "msg": "billing: fetched credits config",
        "ctx": {
            "subscriptionTier": "SuperGrok",
            "config": {
                "creditUsagePercent": 7.0,
                "currentPeriod": {
                    "type": "USAGE_PERIOD_TYPE_WEEKLY",
                    "end": "2026-07-19T17:18:21.366612+00:00",
                },
                "billingPeriodEnd": "2026-07-19T17:18:21.366612+00:00",
            },
        },
    }
    (logs / "unified.jsonl").write_text(json.dumps(line) + "\n", encoding="utf-8")
    profile = _cli_profile("grok-subscription-a", "grok", state)
    snap = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap.ok
    assert snap.company == "xAI"
    assert snap.plan_label == "Super"
    assert snap.weekly.remaining_percent == 93
    assert snap.five_hour.available is False


def test_subscription_profiles_filters_cli_only(tmp_path: Path) -> None:
    codex = _cli_profile("codex-a", "codex", tmp_path / "c")
    api = ProviderProfileConfig(
        id="openai-api",
        provider="openai-compatible",
        model="gpt",
        launch_mode=LaunchMode.API,
        api_base_url="https://api.example.com/v1",  # type: ignore[arg-type]
        credential_reference="env:VUZOL_X",
        capabilities=frozenset({Capability.REPOSITORY_READ}),
        concurrency_limit=1,
        cost_class=CostClass.CHEAP,
        roles=frozenset({ProviderRole.EXECUTOR}),
        supported_task_types=frozenset({"general"}),
        sandbox_required=False,
        enabled=True,
    )
    assert [p.id for p in subscription_profiles((codex, api))] == ["codex-a"]


def test_format_subscription_limits_html() -> None:
    snap = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(
            remaining_percent=40,
            reset_at=datetime(2026, 7, 16, 18, 0, tzinfo=UTC),
        ),
        weekly=LimitWindow(
            remaining_percent=80,
            reset_at=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        ),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=True,
    )
    lines = format_subscription_limits_html((snap,), html_escape=telegram_html)
    assert "OpenAI" in lines[0]
    assert "Plus" in lines[0]
    assert "5ч" not in lines[0]
    assert "40%" in lines[1]
    assert "80%" in lines[2]
