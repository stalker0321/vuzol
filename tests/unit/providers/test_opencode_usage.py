"""Sanitized provider contract through real HTTP transport and auth boundary."""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from tests.unit.config.test_account_descriptors import descriptor, document
from vuzol.config.loader import load_document
from vuzol.config.models import AccountConfig
from vuzol.providers.opencode_usage import GO_USAGE_URL, OpenCodeUsageCollector, parse_go_usage
from vuzol.providers.subscription_limits import (
    collect_profile_limits,
    format_subscription_limits_html,
    subscription_inventory,
    subscription_profiles,
)
from vuzol.telegram.projections import telegram_html

NOW = datetime(2026, 10, 23, 1, tzinfo=UTC)


def payload() -> dict[str, object]:
    return {
        "useBalance": False,
        "rollingUsage": {"status": "ok", "usagePercent": 12.5, "resetInSec": 100},
        "weeklyUsage": {"status": "rate-limited", "usagePercent": 100, "resetInSec": 200},
        "monthlyUsage": {"status": "ok", "usagePercent": 0, "resetInSec": 300},
    }


def account(tmp_path: Path) -> AccountConfig:
    result = AccountConfig.model_validate(descriptor(tmp_path))
    state = result.state_directory
    assert state is not None
    state.mkdir()
    auth = state / "auth.json"
    auth.write_text(json.dumps({"opencode-go": {"type": "api_key", "key": "offline-fixture-only"}}))
    auth.chmod(0o600)
    return result


def collector(handler, **kwargs) -> OpenCodeUsageCollector:
    return OpenCodeUsageCollector(
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
        **kwargs,
    )


def test_three_windows_zero_fractional_and_rate_limited_are_real_values() -> None:
    windows = parse_go_usage(payload(), observed_at=NOW, scope_id="meter-c")
    assert [w.id for w in windows] == ["rolling", "weekly", "monthly"]
    assert [w.remaining_percent for w in windows] == [87.5, 0, 100]
    assert [w.reset_at for w in windows] == [NOW + timedelta(seconds=n) for n in (100, 200, 300)]
    assert windows[1].status == "rate-limited"


@pytest.mark.parametrize(
    "field,value",
    [
        ("usagePercent", None),
        ("usagePercent", True),
        ("usagePercent", "50"),
        ("usagePercent", -1),
        ("usagePercent", float("nan")),
        ("usagePercent", 101),
        ("resetInSec", -1),
        ("resetInSec", True),
        ("status", "new-format"),
    ],
)
def test_invalid_fields_never_become_full_quota(field: str, value: object) -> None:
    data = payload()
    data["monthlyUsage"][field] = value
    with pytest.raises(ValueError):
        parse_go_usage(data, observed_at=NOW, scope_id="meter-c")


def test_missing_window_is_an_explicit_schema_failure() -> None:
    data = payload()
    del data["monthlyUsage"]
    with pytest.raises(ValueError):
        parse_go_usage(data, observed_at=NOW, scope_id="meter-c")


@pytest.mark.parametrize(
    "status,body,expected",
    [
        (401, {}, "reauth_required"),
        (403, {}, "usage_permission_denied"),
        (403, {"error": {"type": "EntitlementError"}}, "go_entitlement_missing"),
        (429, {}, "usage_rate_limited"),
        (500, {}, "usage_unavailable"),
        (200, {}, "usage_schema_invalid"),
        (302, {}, "usage_redirect_denied"),
    ],
)
def test_http_failures_are_distinct_and_never_follow_redirect(
    tmp_path: Path, status: int, body: dict, expected: str
) -> None:
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(
            status, json=body, headers={"location": "https://evil.example/", "retry-after": "123"}
        )

    value = collector(transport).collect(account(tmp_path))
    assert value.detail == expected and not value.windows and value.observed_at is None
    assert len(calls) == 1 and str(calls[0].url) == GO_USAGE_URL
    assert calls[0].method == "GET"
    assert "offline-fixture-only" not in repr(value)
    if status == 429:
        assert value.retry_after_seconds == 123


@pytest.mark.parametrize(
    "headers,body,expected",
    [
        ({"content-type": "text/html"}, b"<html>sign in</html>", "usage_schema_invalid"),
        ({"content-type": "application/json"}, b" " * 65537, "usage_oversized"),
        (
            {"content-type": "application/json", "date": "Fri, 23 Oct 2026 02:00:00 GMT"},
            json.dumps(payload()).encode(),
            "usage_clock_skew",
        ),
        (
            {"content-type": "application/json", "date": "Thu, 22 Oct 2026 01:00:00 GMT"},
            json.dumps(payload()).encode(),
            "usage_stale",
        ),
    ],
)
def test_bad_body_and_clock(tmp_path: Path, headers, body, expected) -> None:
    result = collector(lambda request: httpx.Response(200, headers=headers, content=body)).collect(
        account(tmp_path)
    )
    assert result.detail == expected and result.observed_at is None


def test_timeout_has_no_fake_measurement(tmp_path: Path) -> None:
    def transport(request):
        raise httpx.ReadTimeout("must not leak token")

    result = collector(transport).collect(account(tmp_path))
    assert result.detail == "usage_timeout" and result.observed_at is None


def test_coalescing_retry_after_and_measurement_time(tmp_path: Path) -> None:
    binding = account(tmp_path)
    calls = []
    mono = [0.0]
    now = [NOW]

    def transport(request):
        calls.append(request)
        return httpx.Response(200, json=payload())

    service = OpenCodeUsageCollector(
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(transport)),
        monotonic=lambda: mono[0],
        clock=lambda: now[0],
    )
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: service.collect(binding), range(12)))
    assert len(calls) == 1 and all(r.windows for r in results)
    now[0] += timedelta(seconds=20)
    mono[0] = 20
    cached = service.collect(binding)
    assert cached.observed_at == NOW and cached.attempted_at == now[0]
    mono[0] = 31
    service.collect(binding)
    assert len(calls) == 2


def test_real_config_collection_render_and_content_identity(tmp_path: Path, monkeypatch) -> None:
    account(tmp_path)
    profiles = load_document(document(tmp_path)).profiles
    service = collector(lambda request: httpx.Response(200, json=payload()))
    monkeypatch.setattr("vuzol.providers.opencode_usage.go_usage_collector", service)
    selected = subscription_profiles(profiles)
    assert len(selected) == 1
    snap = collect_profile_limits(selected[0], now=NOW)
    assert snap.ok and snap.observed_at == NOW
    assert snap.five_hour.remaining_percent is None  # no lossy legacy integer mirror
    rendered = "\n".join(format_subscription_limits_html((snap,), html_escape=telegram_html))
    assert "87.5%" in rendered and "1mo" in rendered and "⛔" in rendered
    stale = subscription_inventory(profiles, (snap,), now=NOW + timedelta(minutes=16))[0]
    assert not stale.ok and not stale.windows
    changed = payload()
    changed["monthlyUsage"]["usagePercent"] = 1
    from dataclasses import replace

    assert (
        snap.fingerprint()
        != replace(
            snap, windows=parse_go_usage(changed, observed_at=NOW, scope_id="go:account-meter:test")
        ).fingerprint()
    )


@pytest.mark.parametrize(
    "auth",
    [
        {"opencode": {"type": "api_key", "key": "wrong-product"}},
        {"opencode-go": {"type": "oauth", "access": "not-a-key"}},
        {"opencode-go": {"type": "api_key", "key": "!echo unsafe"}},
    ],
)
def test_auth_never_falls_back_or_executes(tmp_path: Path, auth: dict) -> None:
    binding = account(tmp_path)
    (binding.state_directory / "auth.json").write_text(json.dumps(auth))

    def transport(request):
        pytest.fail("unsupported auth must not reach any endpoint")

    assert collector(transport).collect(binding).detail == "auth_method_unsupported"


def test_distinct_accounts_are_not_coalesced_and_retry_after_is_respected(tmp_path: Path) -> None:
    b_dir, c_dir = tmp_path / "b", tmp_path / "c"
    b_dir.mkdir()
    c_dir.mkdir()
    b = account(b_dir).model_copy(update={"id": "b", "quota_scope_id": "b-meter"})
    c = account(c_dir).model_copy(update={"id": "c", "quota_scope_id": "c-meter"})
    for binding in (b, c):
        (binding.state_directory / "auth.json").write_text(
            json.dumps({"opencode-go": {"type": "api_key", "key": "fixture-" + binding.id}})
        )
    calls = []
    mono = [0.0]

    def transport(request):
        calls.append(request.headers["authorization"])
        if calls[-1] == "Bearer fixture-b":
            return httpx.Response(429, headers={"retry-after": "120"})
        return httpx.Response(200, json=payload())

    service = collector(transport, monotonic=lambda: mono[0])
    assert service.collect(b).detail == "usage_rate_limited"
    assert service.collect(c).windows[0].scope_id == "c-meter"
    mono[0] = 100
    assert service.collect(b).detail == "usage_rate_limited"
    assert calls.count("Bearer fixture-b") == 1
    mono[0] = 121
    service.collect(b)
    assert calls.count("Bearer fixture-b") == 2


def test_cached_entitlement_evidence_does_not_become_a_fresh_measurement(tmp_path: Path) -> None:
    binding = account(tmp_path)
    now = [NOW]
    service = OpenCodeUsageCollector(
        clock=lambda: now[0],
        monotonic=lambda: 0,
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(403, json={"error": {"type": "EntitlementError"}})
            )
        ),
    )
    first = service.collect(binding)
    now[0] += timedelta(minutes=2)
    again = service.collect(binding)
    assert again.entitlement_observed_at == first.entitlement_observed_at == NOW
    assert again.attempted_at == now[0] and again.observed_at is None


def test_auth_fifo_does_not_block_and_never_reaches_http(tmp_path: Path) -> None:
    binding = AccountConfig.model_validate(descriptor(tmp_path))
    binding.state_directory.mkdir()
    os.mkfifo(binding.state_directory / "auth.json", mode=0o600)
    result = collector(lambda r: pytest.fail("invalid auth must not reach HTTP")).collect(binding)
    assert result.detail == "auth_permissions"
