"""Visible quota content, compact failures and rich layout."""
# ruff: noqa: RUF001

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from xml.etree import ElementTree

import pytest

from tests.unit.providers._test_subscription_limits_helpers import _cli_profile
from vuzol.providers.subscription_limits import (
    _SAFE_UNAVAILABLE_DETAILS,
    LimitWindow,
    SubscriptionLimitSnapshot,
    format_subscription_limits_html,
    subscription_inventory,
)
from vuzol.telegram.projections import telegram_html

NOW = datetime(2026, 11, 9, 18, 26, tzinfo=UTC)


def snapshot() -> SubscriptionLimitSnapshot:
    return SubscriptionLimitSnapshot(
        "codex<a>", "OpenAI & Co", "Plus", LimitWindow(72, NOW), LimitWindow(None, None), NOW, True
    )


@pytest.mark.parametrize("rich", [False, True])
def test_usage_bar_escaping_and_compact_errors_after_working_accounts(rich):
    good = snapshot()
    bad = replace(good, profile_id="bad<b>", ok=False, detail="auth.json unreadable")
    unknown = replace(bad, profile_id="pi", company="pi", detail="collector_not_supported")
    lines = format_subscription_limits_html(
        (bad, good, unknown), html_escape=telegram_html, rich=rich, now=NOW
    )
    rendered = "\n".join(lines)
    assert rendered.index("codex&lt;a&gt;") < rendered.index("bad&lt;b&gt;")
    assert "███░░░░░░░" in rendered and "28%" in rendered
    assert "09.11 19:26" in rendered
    assert "тариф неизвестен" not in lines[-1]
    assert "• OpenAI &amp; Co · <code>bad&lt;b&gt;</code> — ⚠️ нет доступа к логину" in lines[-2]
    assert "• pi · <code>pi</code> — ⚠️ нет сборщика лимитов" in lines[-1]
    assert "\n\n• pi" not in rendered
    if rich:
        assert "<table>" in rendered
        assert "<th>Окно</th><th>Использовано</th><th>Сброс</th>" in rendered
    else:
        assert "  <code>5h   ███░░░░░░░   28%</code> · сброс 09.11 19:26" in rendered
        assert "<table>" not in rendered
    ElementTree.fromstring(f"<root>{rendered}</root>")  # noqa: S314 - only generated HTML from fixed fixtures


def test_all_safe_errors_have_short_fixed_text_and_unknown_detail_is_hidden():
    from vuzol.providers.subscription_limits import (
        _SHORT_UNAVAILABLE_DETAILS,
        _SHORT_UNAVAILABLE_FALLBACK,
    )

    assert _SHORT_UNAVAILABLE_DETAILS.keys() == _SAFE_UNAVAILABLE_DETAILS.keys()
    assert all(0 < len(text) <= 35 for text in _SHORT_UNAVAILABLE_DETAILS.values())
    assert 0 < len(_SHORT_UNAVAILABLE_FALLBACK) <= 35
    bad = replace(snapshot(), ok=False, detail="private diagnostics <secret>")
    rendered = "\n".join(format_subscription_limits_html((bad,), html_escape=telegram_html))
    assert _SHORT_UNAVAILABLE_FALLBACK in rendered
    assert "private" not in rendered and "secret" not in rendered


def test_observation_times_only_change_content_when_crossing_stale_boundary():
    good = snapshot()
    fresh = replace(good, observed_at=NOW + timedelta(minutes=2))
    assert good.fingerprint(now=NOW) == fresh.fingerprint(now=NOW + timedelta(minutes=2))
    at_boundary = good.fingerprint(now=NOW + timedelta(minutes=30))
    stale = good.fingerprint(now=NOW + timedelta(minutes=30, seconds=1))
    assert at_boundary != stale and "данные устарели" in stale
    assert stale == good.fingerprint(now=NOW + timedelta(hours=2))
    assert good.fingerprint(now=NOW) != replace(good, five_hour=LimitWindow(71, NOW)).fingerprint(
        now=NOW
    )


def test_dashboard_inventory_keeps_last_successful_measurement():
    from pathlib import Path

    good = replace(snapshot(), profile_id="codex-a")
    profile = _cli_profile("codex-a", "codex", Path("/unused"))
    kept = subscription_inventory(
        (profile,), (good,), now=NOW + timedelta(minutes=31), max_age=None
    )[0]
    assert kept.ok and kept.five_hour.remaining_percent == 72
    assert "данные устарели" in "\n".join(
        format_subscription_limits_html(
            (kept,), html_escape=telegram_html, now=NOW + timedelta(minutes=31)
        )
    )
    assert not subscription_inventory((profile,), (good,), now=NOW + timedelta(minutes=31))[0].ok


@pytest.mark.parametrize(
    ("remaining", "bar", "percent"),
    [
        (100, "░░░░░░░░░░", "0%"),
        (72, "███░░░░░░░", "28%"),
        (41, "██████░░░░", "59%"),
        (0, "██████████", "100%"),
    ],
)
def test_used_percent_bar_extremes(remaining, bar, percent):
    snap = replace(snapshot(), five_hour=LimitWindow(remaining, None))
    rendered = "\n".join(
        format_subscription_limits_html((snap,), html_escape=telegram_html, now=NOW)
    )
    assert bar in rendered and percent in rendered


def test_opencode_windows_preserve_fractional_used_percent_rate_limit_and_balance_warning():
    from vuzol.providers.quota import QuotaWindow

    snap = replace(
        snapshot(),
        source="opencode_go_oauth_v1",
        use_balance=True,
        windows=(
            QuotaWindow(
                id="monthly",
                used_percent=12.5,
                reset_at=NOW,
                status="rate-limited",
                scope_id="test",
            ),
        ),
    )
    for rich in (False, True):
        rendered = "\n".join(
            format_subscription_limits_html((snap,), html_escape=telegram_html, now=NOW, rich=rich)
        )
        assert "1mo" in rendered and "12.5%" in rendered and "█░░░░░░░░░" in rendered
        assert "⛔" in rendered
        assert rendered.count("платный перерасход") == 1
