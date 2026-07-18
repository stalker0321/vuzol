"""Subscription format persist tests (split for cohesion)."""

from __future__ import annotations

from ._test_subscription_limits_helpers import (
    UTC,
    Any,
    Capability,
    CostClass,
    LaunchMode,
    LimitWindow,
    Path,
    ProviderProfileConfig,
    ProviderRole,
    SimpleNamespace,
    SubscriptionLimitSnapshot,
    _cli_profile,
    _http_json,
    _parse_datetime,
    collect_profile_limits,
    datetime,
    format_subscription_limits_html,
    json,
    progress_bar,
    pytest,
    telegram_html,
)


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

    def boom_open(*args: object, **kwargs: object) -> object:
        raise OSError("unreadable")

    monkeypatch.setattr(Path, "open", boom_open)
    assert _latest_grok_billing_from_logs(tmp_path) is None


@pytest.mark.anyio
async def test_persist_and_load_subscription_limits() -> None:
    """Persist/load round-trip using a lightweight in-memory session mock."""

    from unittest.mock import AsyncMock, MagicMock

    from vuzol.providers.subscription_limits import (
        load_subscription_limits,
        persist_subscription_limits,
        refresh_and_store_subscription_limits,
    )

    snap = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(remaining_percent=40, reset_at=None, available=True),
        weekly=LimitWindow(
            remaining_percent=70,
            reset_at=datetime(2026, 7, 20, tzinfo=UTC),
            available=True,
        ),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=True,
    )
    failed = SubscriptionLimitSnapshot(
        profile_id="grok-subscription-a",
        company="xAI",
        plan_label="Super",
        five_hour=LimitWindow(None, None, available=False, detail="—"),
        weekly=LimitWindow(None, None, available=False, detail="—"),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=False,
        detail="PermissionError",
    )

    stored: dict[str, Any] = {}
    session = MagicMock()

    async def _get(_model: object, key: str) -> Any:
        return stored.get(key)

    def _add(row: Any) -> None:
        stored[row.profile_id] = row

    session.get = AsyncMock(side_effect=_get)
    session.add = MagicMock(side_effect=_add)
    session.flush = AsyncMock()

    await persist_subscription_limits(session, (snap, failed))
    assert set(stored) == {"codex-subscription-prod", "grok-subscription-a"}
    codex_row = stored["codex-subscription-prod"]
    assert codex_row.company == "OpenAI"
    assert codex_row.weekly_remaining_percent == 70

    # Second persist updates the existing ORM row in place.
    snap2 = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Pro",
        five_hour=LimitWindow(remaining_percent=None, reset_at=None, available=False),
        weekly=LimitWindow(
            remaining_percent=55,
            reset_at=datetime(2026, 7, 21, tzinfo=UTC),
            available=True,
        ),
        observed_at=datetime(2026, 7, 16, 1, tzinfo=UTC),
        ok=True,
    )
    await persist_subscription_limits(session, (snap2,))
    assert codex_row.plan_label == "Pro"
    assert codex_row.weekly_remaining_percent == 55
    assert codex_row.five_hour_remaining_percent is None

    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: list(stored.values())))
    loaded = await load_subscription_limits(session)
    assert len(loaded) == 2
    by_id = {item.profile_id: item for item in loaded}
    assert by_id["codex-subscription-prod"].plan_label == "Pro"
    assert by_id["codex-subscription-prod"].weekly.remaining_percent == 55
    assert by_id["grok-subscription-a"].ok is False
    assert by_id["grok-subscription-a"].detail == "PermissionError"
    assert by_id["grok-subscription-a"].five_hour.available is False

    from vuzol.providers import subscription_limits as limits_mod

    async def fake_collect(
        profiles: object, *, now: object = None
    ) -> tuple[SubscriptionLimitSnapshot, ...]:
        return (snap,)

    original = limits_mod.collect_subscription_limits
    limits_mod.collect_subscription_limits = fake_collect
    try:
        out = await refresh_and_store_subscription_limits(session, ())
        assert len(out) == 1
        assert out[0].profile_id == "codex-subscription-prod"
    finally:
        limits_mod.collect_subscription_limits = original


@pytest.mark.anyio
async def test_dashboard_loads_limits_from_db_not_filesystem() -> None:
    """Delivery path must use DB snapshots (no collect_subscription_limits)."""

    from unittest.mock import AsyncMock, MagicMock, patch

    from vuzol.telegram.projections import build_project_status_dashboard

    snap = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(remaining_percent=80, reset_at=None, available=True),
        weekly=LimitWindow(
            remaining_percent=28,
            reset_at=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
            available=True,
        ),
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
        ok=True,
    )
    session = MagicMock()
    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: []))

    with patch(
        "vuzol.telegram.projections.load_subscription_limits",
        new=AsyncMock(return_value=(snap,)),
    ) as load_mock:
        card = await build_project_status_dashboard(session, chat_id=1)
    load_mock.assert_awaited_once()
    assert "Subscription limits" in card.html
    assert "28% left" in card.html
    assert "OpenAI" in card.html
    assert "auth.json" not in card.html
