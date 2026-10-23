"""Account discovery is independent of collector support and old DB rows."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.unit.providers._test_subscription_limits_helpers import _cli_profile
from vuzol.config.revision import content_revision
from vuzol.providers.subscription_limits import (
    LimitWindow,
    SubscriptionLimitSnapshot,
    collect_profile_limits,
    collect_subscription_limits,
    format_subscription_limits_html,
    subscription_inventory,
    subscription_profiles,
)
from vuzol.telegram.projections import telegram_html

NOW = datetime(2026, 10, 22, 14, tzinfo=UTC)


def snapshot(profile_id: str, *, age: int = 0) -> SubscriptionLimitSnapshot:
    return SubscriptionLimitSnapshot(
        profile_id,
        "Test",
        "Pro",
        LimitWindow(50, None),
        LimitWindow(80, None),
        NOW - timedelta(minutes=age),
        True,
    )


def test_all_enabled_cli_accounts_discovered_without_duplicate_models() -> None:
    base = _cli_profile("pi-opencode-c", "pi", Path("/unused")).model_copy(
        update={"account_id": "opencode-c"}
    )
    other_model = base.model_copy(update={"id": "pi-opencode-c-muse", "model": "muse"})
    disabled = base.model_copy(update={"id": "old-account", "enabled": False})
    hidden = base.model_copy(update={"id": "expired-account", "subscription_limits_visible": False})
    assert subscription_profiles((base, other_model, disabled, hidden)) == (base,)


def test_inventory_ignores_ghosts_and_uses_newest_snapshot_for_account() -> None:
    base = _cli_profile("pi-c", "pi", Path("/unused")).model_copy(
        update={"account_id": "opencode-c"}
    )
    derived = base.model_copy(update={"id": "pi-c-muse"})
    hidden = _cli_profile("grok-a", "grok", Path("/unused")).model_copy(
        update={"subscription_limits_visible": False}
    )
    inventory = subscription_inventory(
        (base, derived, hidden),
        (snapshot("pi-c", age=2), snapshot("pi-c-muse"), snapshot("deleted"), snapshot("grok-a")),
        now=NOW,
    )
    assert len(inventory) == 1
    assert inventory[0].profile_id == "opencode-c"
    assert inventory[0].observed_at == NOW


def test_new_account_is_visible_before_first_refresh_and_stale_quota_is_unknown() -> None:
    fresh = _cli_profile("pi-b", "pi", Path("/unused"))
    stale = _cli_profile("codex-a", "codex", Path("/unused"))
    inventory = subscription_inventory((fresh, stale), (snapshot("codex-a", age=16),), now=NOW)
    assert [item.profile_id for item in inventory] == ["pi-b", "codex-a"]
    assert all(not item.ok and item.five_hour.remaining_percent is None for item in inventory)
    text = "\n".join(format_subscription_limits_html(inventory, html_escape=telegram_html))
    assert "50%" not in text and "100%" not in text
    assert "ещё не получены" in text and "старше 15 минут" in text


@pytest.mark.anyio
async def test_unsupported_provider_is_visible_as_unknown_not_full() -> None:
    profile = _cli_profile("pi-b", "pi", Path("/unused"))
    snapshots = await collect_subscription_limits((profile,), now=NOW)
    assert len(snapshots) == 1
    assert not snapshots[0].ok
    assert snapshots[0].detail == "collector_not_supported"
    text = "\n".join(format_subscription_limits_html(snapshots, html_escape=telegram_html))
    assert "сборщик не подключён" in text


def test_visibility_is_not_an_execution_policy_revision() -> None:
    profile = _cli_profile("grok-a", "grok", Path("/unused"))
    hidden = profile.model_copy(update={"subscription_limits_visible": False})
    assert content_revision(hidden) == content_revision(profile)


@pytest.mark.parametrize("timestamp", [None, "2026-08-04T17:15:12Z"])
def test_old_or_undated_log_cannot_be_relabelled_as_a_fresh_measurement(
    tmp_path: Path, timestamp: str | None
) -> None:
    log = tmp_path / "logs"
    log.mkdir()
    (log / "unified.jsonl").write_text(
        json.dumps(
            {
                "ts": timestamp,
                "msg": "billing: fetched credits config",
                "ctx": {"subscriptionTier": "SuperGrok", "config": {"creditUsagePercent": 10}},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    snap = collect_profile_limits(_cli_profile("grok-b", "grok", tmp_path), now=NOW)
    assert not snap.ok
    assert snap.weekly.remaining_percent is None
    if timestamp:
        assert snap.observed_at < NOW - timedelta(days=1)
