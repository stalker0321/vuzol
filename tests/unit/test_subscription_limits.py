"""Subscription limit collectors for Codex/Grok dashboard section."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

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
    _http_json,
    _human_plan,
    _parse_datetime,
    _windows_from_codex_rate_limit,
    collect_profile_limits,
    collect_subscription_limits,
    format_subscription_limits_html,
    progress_bar,
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


def test_collect_codex_from_auth_and_http(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state = tmp_path / "codex"
    state.mkdir()
    (state / "auth.json").write_text(
        json.dumps({"tokens": {"access_token": "tok", "account_id": "acc"}}),
        encoding="utf-8",
    )
    profile = _cli_profile("codex-subscription-prod", "codex", state)

    def fake_http(url: str, *, headers: dict[str, str]) -> dict[str, object]:
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


def test_progress_bar_fills_used_portion() -> None:
    # 28% left → 72% used → about 7 of 10 cells filled
    assert progress_bar(28) == "[███████░░░]"
    assert progress_bar(100) == "[░░░░░░░░░░]"
    assert progress_bar(0) == "[██████████]"
    assert progress_bar(150) == "[░░░░░░░░░░]"
    assert progress_bar(-5) == "[██████████]"


def test_format_subscription_limits_html() -> None:
    snap = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(
            remaining_percent=None,
            reset_at=None,
            available=False,
            detail="no 5h data",
        ),
        weekly=LimitWindow(
            remaining_percent=28,
            reset_at=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        ),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=True,
    )
    lines = format_subscription_limits_html((snap,), html_escape=telegram_html)
    joined = "\n".join(lines)
    assert "OpenAI" in lines[0]
    assert "Plus" in lines[0]
    assert "5h" not in joined
    assert "week" in joined
    assert "[███████░░░]" in joined
    assert "28% left" in joined
    assert "reset 2026-07-19 12:00 UTC" in joined
    assert snap.fingerprint()


@pytest.mark.anyio
async def test_collect_subscription_limits_async_wrapper(tmp_path: Path) -> None:
    profile = _cli_profile("codex-x", "codex", tmp_path / "missing")
    snaps = await collect_subscription_limits((profile,))
    assert len(snaps) == 1
    assert snaps[0].ok is False


def test_format_empty_and_unavailable_and_both_windows() -> None:
    empty = format_subscription_limits_html((), html_escape=telegram_html)
    assert "No subscription" in empty[0]
    bad = SubscriptionLimitSnapshot(
        profile_id="x",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(None, None, available=False),
        weekly=LimitWindow(None, None, available=False),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=False,
        detail="auth missing",
    )
    bad_lines = format_subscription_limits_html((bad,), html_escape=telegram_html)
    assert "unavailable" in bad_lines[1]
    both = SubscriptionLimitSnapshot(
        profile_id="codex",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(remaining_percent=50, reset_at=None, available=True),
        weekly=LimitWindow(
            remaining_percent=10,
            reset_at=datetime(2026, 7, 20, tzinfo=UTC),
            available=True,
        ),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=True,
    )
    both_lines = "\n".join(format_subscription_limits_html((both,), html_escape=telegram_html))
    assert "5h" in both_lines
    assert "week" in both_lines
    assert "50% left" in both_lines
    no_windows = SubscriptionLimitSnapshot(
        profile_id="codex",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(None, None, available=False),
        weekly=LimitWindow(None, None, available=False),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=True,
    )
    assert "no limit windows" in "\n".join(
        format_subscription_limits_html((no_windows,), html_escape=telegram_html)
    )


def test_codex_missing_auth_and_http_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = _cli_profile("codex-a", "codex", tmp_path / "empty")
    (tmp_path / "empty").mkdir()
    assert collect_profile_limits(profile).ok is False
    (tmp_path / "empty" / "auth.json").write_text("{}", encoding="utf-8")
    assert collect_profile_limits(profile).ok is False
    (tmp_path / "empty" / "auth.json").write_text(
        json.dumps({"tokens": {"access_token": "tok"}}), encoding="utf-8"
    )
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        lambda *a, **k: None,
    )
    assert collect_profile_limits(profile).detail == "usage endpoint failed"


def test_grok_http_billing_and_optional_five_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "grok"
    state.mkdir()
    (state / "auth.json").write_text(
        json.dumps({"https://auth.x.ai::x": {"key": "g-token"}}),
        encoding="utf-8",
    )
    profile = _cli_profile("grok-a", "grok", state)

    def fake_http(url: str, *, headers: dict[str, str]) -> dict[str, object]:
        assert "Bearer g-token" in headers["Authorization"]
        return {
            "subscriptionTier": "SuperGrok",
            "config": {
                "creditUsagePercent": 20,
                "billingPeriodEnd": "2026-07-20T00:00:00+00:00",
                "five_hour": {
                    "used_percent": 30,
                    "reset_at": "2026-07-16T20:00:00+00:00",
                },
            },
        }

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        fake_http,
    )
    snap = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap.ok
    assert snap.plan_label == "Super"
    assert snap.weekly.remaining_percent == 80
    assert snap.five_hour.remaining_percent == 70


def test_parse_datetime_and_http_json_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _parse_datetime(None) is None
    assert _parse_datetime("not-a-date") is None
    assert _parse_datetime(1_800_000_000) is not None
    assert _parse_datetime("2026-07-16T12:00:00Z") is not None
    assert _parse_datetime("2026-07-16T12:00:00") is not None
    assert _http_json("file:///etc/passwd", headers={}) is None

    class FakeResp:
        def read(self) -> bytes:
            return b'{"ok": true}'

        def __enter__(self) -> FakeResp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits.urllib.request.urlopen",
        lambda *a, **k: FakeResp(),
    )
    assert _http_json("https://example.com/x", headers={}) == {"ok": True}


def test_unsupported_provider_and_exception_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    other = ProviderProfileConfig(
        id="weird",
        provider="other",
        model="m",
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.STRONG,
        roles=frozenset({ProviderRole.EXECUTOR}),
        supported_task_types=frozenset({"coding"}),
        sandbox_required=True,
        runtime_identity="id-weird",
        state_directory=tmp_path / "w",
        enabled=True,
    )
    (tmp_path / "w").mkdir()
    snap = collect_profile_limits(other)
    assert snap.ok is False
    assert "unsupported" in snap.detail

    profile = _cli_profile("codex-b", "codex", tmp_path / "c")
    (tmp_path / "c").mkdir()
    (tmp_path / "c" / "auth.json").write_text(
        json.dumps({"tokens": {"access_token": "t"}}), encoding="utf-8"
    )

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("network")

    monkeypatch.setattr("vuzol.providers.subscription_limits._http_json", boom)
    # Exception is caught inside collect_profile_limits
    failed = collect_profile_limits(profile)
    assert failed.ok is False


def test_codex_window_remaining_percent_field() -> None:
    observed = datetime(2026, 7, 16, tzinfo=UTC)
    raw = {
        "remaining_percent": 33,
        "limit_window_seconds": 18_000,
        "reset_at": "2026-07-16T18:00:00+00:00",
    }
    classified = _classify_codex_window(raw, observed)
    assert classified is not None
    assert classified[1].remaining_percent == 33


def test_edge_auth_token_and_log_paths(tmp_path: Path) -> None:
    from vuzol.providers.subscription_limits import (
        _codex_access_token,
        _grok_access_token,
        _latest_grok_billing_from_logs,
        _parse_reset,
    )

    assert _codex_access_token(tmp_path / "missing.json") is None
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    assert _codex_access_token(bad) is None
    no_tokens = tmp_path / "notokens.json"
    no_tokens.write_text(json.dumps({"tokens": "x"}), encoding="utf-8")
    assert _codex_access_token(no_tokens) is None
    alt = tmp_path / "alt.json"
    alt.write_text(json.dumps({"tokens": {"accessToken": "abc"}}), encoding="utf-8")
    assert _codex_access_token(alt) == "abc"

    nested = tmp_path / "g"
    nested.mkdir()
    nested_auth = nested / ".grok" / "auth.json"
    nested_auth.parent.mkdir()
    nested_auth.write_text(json.dumps({"key": "top-level"}), encoding="utf-8")
    assert _grok_access_token(nested) == "top-level"

    logs = nested / ".grok" / "logs"
    logs.mkdir()
    (logs / "unified.jsonl").write_text(
        "not-json\n"
        + json.dumps({"msg": "billing: fetched credits config", "ctx": "bad"})
        + "\n"
        + json.dumps(
            {
                "msg": "billing: fetched credits config",
                "ctx": {"config": {"creditUsagePercent": 1}},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    assert _latest_grok_billing_from_logs(nested) == {"config": {"creditUsagePercent": 1}}

    observed = datetime(2026, 7, 16, tzinfo=UTC)
    assert _parse_reset({"reset_at": "not-int", "end": "2026-07-17T00:00:00Z"}, observed)
    assert _parse_reset({"reset_after_seconds": "x"}, observed) is None
    assert _parse_reset({"reset_after_seconds": 60}, observed) is not None
    assert _parse_reset({}, observed) is None
    assert _parse_datetime(10**20) is None  # overflow
    assert _parse_datetime("") is None
    assert _parse_datetime("   ") is None


def test_collect_profile_without_state_directory(tmp_path: Path) -> None:
    profile = ProviderProfileConfig.model_construct(
        id="codex-nostate",
        provider="codex",
        model="m",
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.STRONG,
        roles=frozenset({ProviderRole.EXECUTOR}),
        routing_priority=100,
        supported_task_types=frozenset({"coding"}),
        sandbox_required=True,
        runtime_identity="id",
        state_directory=None,
        enabled=True,
    )
    snap = collect_profile_limits(profile)
    assert snap.ok is False
    assert "state_directory" in snap.detail


def test_http_json_invalid_body(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResp:
        def read(self) -> bytes:
            return b"not-json"

        def __enter__(self) -> FakeResp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits.urllib.request.urlopen",
        lambda *a, **k: FakeResp(),
    )
    assert _http_json("https://example.com/x", headers={}) is None

    class Boom:
        def __enter__(self) -> Boom:
            raise OSError("down")

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits.urllib.request.urlopen",
        lambda *a, **k: Boom(),
    )
    assert _http_json("https://example.com/x", headers={}) is None


def test_grok_five_hour_key_variants(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state = tmp_path / "g"
    state.mkdir()
    (state / "auth.json").write_text(json.dumps({"x": {"access_token": "t"}}), encoding="utf-8")
    profile = _cli_profile("grok-b", "grok", state)
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        lambda *a, **k: {
            "subscription_tier": "supergrok_lite",
            "config": {
                "creditUsagePercent": "bad",
                "primaryWindow": {
                    "remaining_percent": 12,
                    "resetAt": "2026-07-16T21:00:00+00:00",
                },
            },
        },
    )
    snap = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap.ok
    assert snap.plan_label == "Super Lite"
    assert snap.five_hour.remaining_percent == 12


def test_http_json_non_dict_and_log_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from vuzol.providers.subscription_limits import _latest_grok_billing_from_logs

    class FakeResp:
        def read(self) -> bytes:
            return b'["not", "object"]'

        def __enter__(self) -> FakeResp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits.urllib.request.urlopen",
        lambda *a, **k: FakeResp(),
    )
    assert _http_json("https://example.com/x", headers={}) is None

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "unified.jsonl").write_text("billing: fetched credits config {}\n", encoding="utf-8")

    def boom(*args: object, **kwargs: object) -> str:
        raise OSError("unreadable")

    monkeypatch.setattr(Path, "read_text", boom)
    assert _latest_grok_billing_from_logs(tmp_path) is None
