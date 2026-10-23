"""Plans are observations, never paid defaults inferred from model/profile names."""

from dataclasses import replace
from datetime import timedelta
from html import escape
from pathlib import Path

import httpx
import pytest

from tests.unit.providers._test_subscription_limits_helpers import _cli_profile
from tests.unit.providers.test_opencode_usage import NOW, account, collector, document, payload
from vuzol.config.loader import load_document
from vuzol.config.models import AccountConfig
from vuzol.config.revision import content_revision
from vuzol.providers.subscription_limits import (
    collect_profile_limits,
    format_subscription_limits_html,
    subscription_inventory,
)
from vuzol.providers.subscription_plan import SubscriptionPlan, reported_plan


@pytest.mark.parametrize(
    "tier,label",
    [
        ("plus", "Plus"),
        ("pro", "Pro"),
        ("business", "Business"),
        ("free", "Free"),
        ("pro_new", "pro_new"),
    ],
)
def test_codex_reports_current_plan_without_model_or_profile_guess(
    tmp_path: Path, monkeypatch, tier: str, label: str
) -> None:
    profile = _cli_profile("misleading-plus-profile", "codex", tmp_path)
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._codex_access_token", lambda _: "fixture"
    )
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json", lambda *a, **k: {"plan_type": tier}
    )
    snapshot = collect_profile_limits(profile, now=NOW)
    assert snapshot.plan_label == label
    assert snapshot.plan and snapshot.plan.tier_id == tier
    assert snapshot.plan.observed_at == NOW and snapshot.plan.source == "codex_usage"
    card = "\n".join(format_subscription_limits_html([snapshot], html_escape=escape))
    assert "OpenAI / Codex · " + label in card


@pytest.mark.parametrize("raw", [None, "", True, {}, "<b>Pro</b>", "secret\nPro", "x" * 49])
def test_missing_or_invalid_tier_never_defaults_to_plus_or_exposes_raw(raw) -> None:
    plan = reported_plan("codex", raw, source="codex_usage", observed_at=NOW)
    assert plan.tier_id is None and plan.status == "unknown"
    assert plan.label == "тариф неизвестен"


@pytest.mark.parametrize("provider", ["codex", "grok"])
def test_unreadable_auth_is_not_evidence_of_paid_subscription(
    tmp_path: Path, provider: str
) -> None:
    snapshot = collect_profile_limits(_cli_profile("paid-pro", provider, tmp_path), now=NOW)
    assert not snapshot.ok and snapshot.plan is None
    assert snapshot.plan_label == "тариф неизвестен"
    card = "\n".join(format_subscription_limits_html([snapshot], html_escape=escape))
    assert "тариф неизвестен" in card and "Plus" not in card and "Super" not in card


@pytest.mark.parametrize(
    "status,body,expected",
    [
        (200, payload(), "reported"),
        (403, {"error": {"type": "EntitlementError"}}, "not_entitled"),
        (403, {"error": {"type": "AccessDenied"}}, None),
        (401, {}, None),
        (429, {}, None),
        (200, {}, None),
    ],
)
def test_go_tariff_requires_valid_meter_not_a_provider_name(
    tmp_path: Path, monkeypatch, status: int, body: dict, expected: str | None
) -> None:
    account(tmp_path)
    profile = load_document(document(tmp_path)).profiles[0]
    monkeypatch.setattr(
        "vuzol.providers.opencode_usage.go_usage_collector",
        collector(lambda r: httpx.Response(status, json=body)),
    )
    snapshot = collect_profile_limits(profile, now=NOW)
    assert (snapshot.plan.status if snapshot.plan else None) == expected
    if expected == "reported":
        assert snapshot.plan.label == "Go"
    elif expected is None:
        assert snapshot.plan_label == "тариф неизвестен"


def test_plan_upgrade_changes_card_identity_not_account_profile_or_execution(
    tmp_path: Path, monkeypatch
) -> None:
    profile = _cli_profile("codex-a", "codex", tmp_path)
    before = content_revision(profile)
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._codex_access_token", lambda _: "fixture"
    )
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json", lambda *a, **k: {"plan_type": "plus"}
    )
    plus = collect_profile_limits(profile, now=NOW)
    pro = replace(
        plus,
        plan_label="Pro",
        plan=reported_plan("codex", "pro", source="codex_usage", observed_at=NOW),
    )
    assert plus.fingerprint() != pro.fingerprint()
    assert content_revision(profile) == before
    current = subscription_inventory([profile], [pro], now=NOW)[0]
    assert current.plan and not current.plan.stale
    stale = subscription_inventory([profile], [pro], now=NOW + timedelta(minutes=16))[0]
    assert stale.plan and stale.plan.stale
    assert "Pro (данные устарели)" in "\n".join(
        format_subscription_limits_html([stale], html_escape=escape)
    )
    legacy = replace(pro, plan=None)
    card = "\n".join(format_subscription_limits_html([legacy], html_escape=escape))
    assert "Pro" not in card and "тариф неизвестен" in card


def test_cross_service_plan_provenance_and_missing_evidence_rejected() -> None:
    with pytest.raises(ValueError):
        SubscriptionPlan(
            service="opencode",
            tier_id="pro",
            status="reported",
            source="codex_usage",
            observed_at=NOW,
        )
    with pytest.raises(ValueError):
        SubscriptionPlan(service="codex", status="reported", source="codex_usage", observed_at=NOW)


def test_explicit_no_collector_does_not_fall_back_to_legacy_http(
    tmp_path: Path, monkeypatch
) -> None:
    binding = AccountConfig(
        id="codex-test",
        service="chatgpt",
        provider="codex",
        launch_mode="cli",
        auth_method="cli_oauth",
        auth_owner="serialized_cli",
        state_directory=tmp_path,
        runtime_identity="fixture",
        quota_collector="none",
    )
    profile = _cli_profile("codex-test", "codex", tmp_path).model_copy(update={"account": binding})
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        lambda *a, **k: pytest.fail("collector explicitly disabled"),
    )
    assert collect_profile_limits(profile).detail == "collector_not_supported"
