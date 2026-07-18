"""Subscription collect tests (split for cohesion)."""

from __future__ import annotations

from ._test_subscription_limits_helpers import *


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


@pytest.mark.anyio
async def test_collect_subscription_limits_async_wrapper(tmp_path: Path) -> None:
    profile = _cli_profile("codex-x", "codex", tmp_path / "missing")
    snaps = await collect_subscription_limits((profile,))
    assert len(snaps) == 1
    assert snaps[0].ok is False


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


def test_host_grok_billing_logs_matched_by_jwt_subject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sandbox state can reuse interactive host logs for the same OAuth subject."""

    from vuzol.providers.subscription_limits import (
        _billing_ctx_from_log_file,
        _host_grok_billing_log_paths,
        _jwt_subject,
        collect_profile_limits,
    )

    # Minimal unsigned JWT with sub claim.
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    payload = (
        base64.urlsafe_b64encode(b'{"sub":"user-abc","principal_id":"user-abc"}')
        .decode()
        .rstrip("=")
    )
    token = f"{header}.{payload}.sig"
    assert _jwt_subject(token) == "user-abc"
    assert _jwt_subject("not-a-jwt") is None
    assert _jwt_subject("a.%%% .c") is None

    state = tmp_path / "provider-state"
    state.mkdir()
    (state / ".grok").mkdir()
    (state / ".grok" / "auth.json").write_text(
        json.dumps({"https://auth.x.ai::x": {"key": token}}),
        encoding="utf-8",
    )

    # Real host-root discovery uses Path.home() / ".grok-profiles".
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    host_root = tmp_path / ".grok-profiles"
    account = host_root / "account-x"
    logs = account / "logs"
    logs.mkdir(parents=True)
    (account / "auth.json").write_text(
        json.dumps({"https://auth.x.ai::x": {"key": token}}),
        encoding="utf-8",
    )
    other = host_root / "account-other"
    other_logs = other / "logs"
    other_logs.mkdir(parents=True)
    other_payload = base64.urlsafe_b64encode(b'{"sub":"someone-else"}').decode().rstrip("=")
    (other / "auth.json").write_text(
        json.dumps({"https://auth.x.ai::x": {"key": f"{header}.{other_payload}.sig"}}),
        encoding="utf-8",
    )
    (other_logs / "unified.jsonl").write_text("nope\n", encoding="utf-8")

    billing_line = json.dumps(
        {
            "msg": "billing: fetched credits config",
            "ctx": {
                "subscriptionTier": "SuperGrok",
                "config": {
                    "creditUsagePercent": 15.0,
                    "billingPeriodEnd": "2026-07-20T00:00:00+00:00",
                },
            },
        }
    )
    (logs / "unified.jsonl").write_text(
        ("noise\n" * 50) + billing_line + "\n" + ("later\n" * 20),
        encoding="utf-8",
    )

    paths = _host_grok_billing_log_paths(state)
    assert paths == (logs / "unified.jsonl",)
    ctx = _billing_ctx_from_log_file(logs / "unified.jsonl")
    assert ctx is not None
    assert ctx["config"]["creditUsagePercent"] == 15.0
    profile = _cli_profile("grok-subscription-a", "grok", state)
    snap = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap.ok
    assert snap.weekly.remaining_percent == 85

    # Host auth.json may be unreadable (0600); match via subject string in logs.
    (account / "auth.json").unlink()
    (logs / "unified.jsonl").write_text(
        json.dumps(
            {
                "msg": "AuthManager::new",
                "ctx": {"principal": "user-abc", "sub": "user-abc"},
            }
        )
        + "\n"
        + billing_line
        + "\n",
        encoding="utf-8",
    )
    paths2 = _host_grok_billing_log_paths(state)
    assert paths2 == (logs / "unified.jsonl",)
    snap2 = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap2.ok
    assert snap2.weekly.remaining_percent == 85


def test_grok_billing_unavailable_and_html_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "g"
    state.mkdir()
    profile = _cli_profile("grok-empty", "grok", state)
    # No auth, no logs → billing unavailable
    assert collect_profile_limits(profile).detail == "billing unavailable"

    (state / "auth.json").write_text(json.dumps({"k": {"key": "tok"}}), encoding="utf-8")

    def html_then_none(url: str, *, headers: dict[str, str]) -> dict[str, object] | None:
        if "grok.com" in url:
            return {"title": "Not billing"}  # looks like HTML / non-billing
        return None

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        html_then_none,
    )
    assert collect_profile_limits(profile).detail == "billing unavailable"

    monkeypatch.setattr(
        "vuzol.providers.subscription_limits._http_json",
        lambda *a, **k: {"config": "not-a-dict", "creditUsagePercent": 50},
    )
    # Non-dict config falls back to the outer payload; still a valid snapshot.
    snap = collect_profile_limits(profile, now=datetime(2026, 7, 16, tzinfo=UTC))
    assert snap.ok is True
    assert snap.weekly.remaining_percent == 50


def test_codex_window_edges_and_invalid_percents() -> None:
    from vuzol.providers.subscription_limits import (
        _classify_codex_window,
        _weekly_from_grok_config,
        _window_from_generic,
    )

    observed = datetime(2026, 7, 16, tzinfo=UTC)
    assert _classify_codex_window({"limit_window_seconds": "bad"}, observed) is None
    assert _classify_codex_window({"limit_window_seconds": 12_000}, observed) is not None
    # Between 6h and 3d → unclassified
    assert _classify_codex_window({"limit_window_seconds": 86_400}, observed) is None
    # Invalid percent values fall back to None remaining
    window = _window_from_generic(
        {"used_percent": "nope", "limit_window_seconds": 18_000},
        observed,
        default_seconds=18_000,
    )
    assert window.remaining_percent is None
    weekly = _weekly_from_grok_config(
        {
            "creditUsagePercent": "x",
            "currentPeriod": {"end": "2026-07-20T00:00:00Z"},
        },
        observed,
    )
    assert weekly.remaining_percent is None
    assert weekly.reset_at is not None
