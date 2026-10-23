"""Auth-owner transport rejects stale, swapped and partial observations."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.unit.providers._test_subscription_limits_helpers import _cli_profile
from tests.unit.providers.test_opencode_oauth import oauth_payload
from vuzol.config.settings import OpenCodeSnapshotBinding, SubscriptionLimitSettings
from vuzol.providers.opencode_snapshot import (
    UsageSnapshot,
    execution_binding,
    load_usage_snapshot,
    publish_usage_snapshot,
)
from vuzol.providers.opencode_usage import parse_oauth_go_usage
from vuzol.providers.subscription_limits import _format_reset, collect_profile_limits

NOW = datetime(2026, 10, 23, 1, tzinfo=UTC)


def fixture(tmp_path):
    tmp_path.chmod(0o700)
    profile = _cli_profile("go-c", "pi", tmp_path / "inference")
    binding = OpenCodeSnapshotBinding(
        account_id="go-c", scope_id="org-c:account-c", path=tmp_path / "usage.json"
    )
    snapshot = UsageSnapshot(
        account_id="go-c",
        scope_id=binding.scope_id,
        binding_digest=execution_binding(profile),
        generated_at=NOW,
        attempted_at=NOW,
        observed_at=NOW,
        windows=parse_oauth_go_usage(
            oauth_payload(NOW), observed_at=NOW, scope_id=binding.scope_id
        ),
    )
    return profile, binding, snapshot


def test_snapshot_new_reader_and_shared_models_without_credentials(tmp_path, monkeypatch):
    profile, binding, snapshot = fixture(tmp_path)
    monkeypatch.setattr(
        "vuzol.providers.opencode_usage.OpenCodeUsageCollector.collect",
        lambda *a: pytest.fail("snapshot consumer must not open auth or HTTP"),
    )
    publish_usage_snapshot(binding.path, snapshot)
    result = collect_profile_limits(profile, now=NOW, opencode_snapshots=(binding,))
    assert result.ok and result.windows[0].remaining_percent == 86.3
    assert result.plan.label == "Go"
    assert load_usage_snapshot(binding, profile=profile, now=NOW).windows == snapshot.windows
    assert binding.path.stat().st_mode & 0o777 == 0o640
    other = profile.model_copy(update={"model": "different-model", "roles": frozenset({"lead"})})
    assert execution_binding(other) == execution_binding(profile)
    raw = binding.path.read_text()
    assert all(word not in raw for word in ("access_token", "refresh_token", "email", "store_path"))


@pytest.mark.parametrize(
    "change,detail",
    [
        ({"scope_id": "foreign"}, "limits_binding_mismatch"),
        ({"account_id": "foreign"}, "limits_binding_mismatch"),
        ({"binding_digest": "f" * 64}, "limits_binding_mismatch"),
        ({"observed_at": (NOW - timedelta(hours=1)).isoformat()}, "limits_snapshot_stale"),
        ({"generated_at": (NOW + timedelta(hours=1)).isoformat()}, "limits_snapshot_stale"),
        ({"attempted_at": NOW.replace(tzinfo=None).isoformat()}, "limits_snapshot_invalid"),
        ({"windows": []}, "limits_snapshot_invalid"),
        ({"access_token": "forbidden"}, "limits_snapshot_invalid"),
    ],
)
def test_invalid_evidence_does_not_fall_back(tmp_path, change, detail):
    profile, binding, snapshot = fixture(tmp_path)
    raw = snapshot.model_dump(mode="json")
    raw.update(change)
    if "scope_id" in change:
        for window in raw["windows"]:
            window["scope_id"] = change["scope_id"]
    binding.path.write_text(json.dumps(raw))
    binding.path.chmod(0o640)
    result = load_usage_snapshot(binding, profile=profile, now=NOW)
    assert result.detail == detail and not result.windows


def test_failed_export_replaces_previous_success_and_expires(tmp_path):
    profile, binding, snapshot = fixture(tmp_path)
    publish_usage_snapshot(binding.path, snapshot)
    failed = UsageSnapshot(
        account_id=snapshot.account_id,
        scope_id=snapshot.scope_id,
        binding_digest=snapshot.binding_digest,
        generated_at=NOW,
        attempted_at=NOW,
        detail="reauth_required",
    )
    publish_usage_snapshot(binding.path, failed)
    result = load_usage_snapshot(binding, profile=profile, now=NOW)
    assert result.detail == "reauth_required" and not result.windows
    assert load_usage_snapshot(
        binding, profile=profile, now=NOW + timedelta(minutes=16)
    ).detail == ("limits_snapshot_stale")
    swapped = profile.model_copy(update={"state_directory": Path("/var/lib/different")})
    assert (
        load_usage_snapshot(binding, profile=swapped, now=NOW).detail == "limits_binding_mismatch"
    )


def test_unsafe_files_and_duplicate_configuration(tmp_path):
    profile, binding, snapshot = fixture(tmp_path)
    publish_usage_snapshot(binding.path, snapshot)
    binding.path.chmod(0o666)
    assert (
        load_usage_snapshot(binding, profile=profile, now=NOW).detail == "limits_snapshot_invalid"
    )
    binding.path.unlink()
    binding.path.symlink_to(tmp_path / "absent")
    assert (
        load_usage_snapshot(binding, profile=profile, now=NOW).detail == "limits_snapshot_invalid"
    )
    with pytest.raises(ValueError):
        SubscriptionLimitSettings(opencode_snapshots=(binding, binding))


@pytest.mark.parametrize(
    "utc,expected",
    [
        ("2026-10-23T10:17:00+00:00", "23.10 12:17"),
        ("2026-10-29T23:04:00+00:00", "30.10 00:04"),
        ("2026-10-25T00:30:00+00:00", "25.10 02:30"),
        ("2026-10-25T01:30:00+00:00", "25.10 02:30"),
    ],
)
def test_amsterdam_including_dst_switch(utc, expected):
    assert _format_reset(datetime.fromisoformat(utc)) == expected
