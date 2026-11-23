"""Only fresh, definitive account failures justify changing a pinned account."""

from datetime import UTC, datetime, timedelta

import pytest

from vuzol.providers.subscription_limits import subscription_account_unusable
from vuzol.storage.models import SubscriptionLimitSnapshotRow

NOW = datetime(2026, 11, 23, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    ("detail", "unusable"),
    [
        ("go_entitlement_missing", True),
        ("auth_expired", True),
        ("reauth_required", True),
        ("auth_invalid", True),
        ("usage_permission_denied", True),
        ("usage endpoint failed", False),
        ("auth_unreadable", False),
        ("auth_store_busy", False),
        ("usage_timeout", False),
        ("usage_rate_limited", False),
        ("usage_schema_invalid", False),
        ("unknown", False),
        (None, False),
    ],
)
def test_definitive_account_failures(detail, unusable):
    row = SubscriptionLimitSnapshotRow(ok=False, detail=detail, observed_at=NOW)
    assert subscription_account_unusable(row, now=NOW) is unusable


@pytest.mark.parametrize("hours", [3, -1])
def test_stale_or_future_failure_is_unknown(hours):
    row = SubscriptionLimitSnapshotRow(
        ok=False, detail="auth_expired", observed_at=NOW - timedelta(hours=hours)
    )
    assert not subscription_account_unusable(row, now=NOW)


@pytest.mark.parametrize("window", ["five_hour", "weekly"])
@pytest.mark.parametrize("reset_seconds", [60, -60, 0, None])
@pytest.mark.parametrize("remaining", [0, 1, None])
def test_exhausted_window_requires_future_reset(window, reset_seconds, remaining):
    row = SubscriptionLimitSnapshotRow(ok=True, observed_at=NOW)
    setattr(row, f"{window}_remaining_percent", remaining)
    setattr(
        row,
        f"{window}_reset_at",
        NOW + timedelta(seconds=reset_seconds) if reset_seconds is not None else None,
    )
    assert subscription_account_unusable(row, now=NOW) is (
        remaining == 0 and reset_seconds is not None and reset_seconds > 0
    )


@pytest.mark.parametrize("reset_seconds", [60, -60])
def test_go_monthly_window(reset_seconds):
    row = SubscriptionLimitSnapshotRow(
        ok=True,
        observed_at=NOW,
        payload={
            "windows": [
                {
                    "id": "monthly",
                    "used_percent": 100,
                    "reset_at": (NOW + timedelta(seconds=reset_seconds)).isoformat(),
                    "status": "ok",
                    "scope_id": "account-c",
                }
            ]
        },
    )
    assert subscription_account_unusable(row, now=NOW) is (reset_seconds > 0)


@pytest.mark.parametrize("ok", [True, False])
def test_unknown_or_stale_quota_is_not_unusable(ok):
    row = SubscriptionLimitSnapshotRow(
        ok=ok,
        detail="usage endpoint failed",
        observed_at=NOW - timedelta(hours=3) if ok else NOW,
        weekly_remaining_percent=0,
        weekly_reset_at=NOW + timedelta(hours=1),
    )
    assert not subscription_account_unusable(row, now=NOW)


@pytest.mark.parametrize("payload", [None, [], {"windows": None}, {"windows": [{}]}])
def test_unknown_quota_payload_is_not_unusable(payload):
    row = SubscriptionLimitSnapshotRow(ok=True, observed_at=NOW, payload=payload)
    assert not subscription_account_unusable(row, now=NOW)


def test_pool_label_keeps_existing_pins_and_revisions() -> None:
    from tests.integration.leadership.test_lead_surface import _profile
    from vuzol.config.revision import assignment_revision, content_revision

    profile = _profile()
    pooled = profile.model_copy(update={"account_pool": "opencode-go"})
    assert content_revision(pooled) == content_revision(profile)
    assert assignment_revision(pooled) == assignment_revision(profile)


def test_a_run_of_account_failures_marks_the_account() -> None:
    from datetime import UTC, datetime, timedelta

    from vuzol.providers.account_pool import failing_accounts

    t = datetime(2026, 11, 23, 12, tzinfo=UTC)

    def calls(account: str, *failed: bool) -> list[tuple[str, datetime, bool]]:
        return [(account, t + timedelta(minutes=i), f) for i, f in enumerate(failed)]

    assert failing_accounts(
        [
            *calls("dead", True, True, True),
            *calls("recovered", True, True, True, False),
            *calls("flaky", True, False, True, True),
            *calls("few", True, True),
            *calls("came-back-dead", False, True, True, True),
        ]
    ) == {"dead", "came-back-dead"}
