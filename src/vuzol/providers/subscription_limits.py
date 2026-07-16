"""Best-effort subscription limit snapshots for Codex and Grok profiles.

Codex ChatGPT Plus/Pro limits come from the ChatGPT usage endpoint using the
profile's isolated ``auth.json``. Grok Super limits come from the Grok billing
endpoint when reachable, otherwise the latest local ``billing: fetched credits
config`` log line under the profile state directory.

Network and filesystem failures are non-fatal: the dashboard still renders and
marks the profile as unavailable.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from vuzol.config.models import LaunchMode, ProviderProfileConfig

CODEX_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
GROK_BILLING_URLS = (
    "https://grok.com/billing?format=credits",
    "https://grok.x.ai/billing?format=credits",
)
_FIVE_HOUR_SECONDS = 5 * 3600
_WEEKLY_SECONDS = 7 * 24 * 3600
_FETCH_TIMEOUT_SECONDS = 4.0


@dataclass(frozen=True, slots=True)
class LimitWindow:
    remaining_percent: int | None
    reset_at: datetime | None
    window_seconds: int | None = None
    available: bool = True
    detail: str = ""


@dataclass(frozen=True, slots=True)
class SubscriptionLimitSnapshot:
    profile_id: str
    company: str
    plan_label: str
    five_hour: LimitWindow
    weekly: LimitWindow
    observed_at: datetime
    ok: bool
    detail: str = ""

    def fingerprint(self) -> str:
        five = self.five_hour.remaining_percent
        five_reset = self.five_hour.reset_at.isoformat() if self.five_hour.reset_at else ""
        week = self.weekly.remaining_percent
        week_reset = self.weekly.reset_at.isoformat() if self.weekly.reset_at else ""
        return (
            f"{self.profile_id}:{self.company}:{self.plan_label}:"
            f"{five}:{five_reset}:{week}:{week_reset}:{self.ok}:{self.detail}"
        )


def subscription_profiles(
    profiles: Iterable[ProviderProfileConfig],
) -> tuple[ProviderProfileConfig, ...]:
    """Enabled CLI subscription identities that own isolated state directories."""

    return tuple(
        profile
        for profile in profiles
        if profile.enabled
        and profile.launch_mode is LaunchMode.CLI
        and profile.provider in {"codex", "grok"}
        and profile.state_directory is not None
    )


async def collect_subscription_limits(
    profiles: Sequence[ProviderProfileConfig],
    *,
    now: datetime | None = None,
) -> tuple[SubscriptionLimitSnapshot, ...]:
    """Fetch limit snapshots for every configured subscription profile."""

    observed = now or datetime.now(UTC)
    selected = subscription_profiles(profiles)
    return tuple(collect_profile_limits(profile, now=observed) for profile in selected)


def collect_profile_limits(
    profile: ProviderProfileConfig, *, now: datetime | None = None
) -> SubscriptionLimitSnapshot:
    observed = now or datetime.now(UTC)
    if profile.state_directory is None:
        return _unavailable(profile, observed, "state_directory missing")
    try:
        if profile.provider == "codex":
            return _collect_codex(profile, observed)
        if profile.provider == "grok":
            return _collect_grok(profile, observed)
    except Exception as error:  # pragma: no cover - defensive boundary
        return _unavailable(profile, observed, type(error).__name__)
    return _unavailable(profile, observed, f"unsupported provider {profile.provider}")


def format_subscription_limits_html(
    snapshots: Sequence[SubscriptionLimitSnapshot],
    *,
    html_escape: Callable[[object], str],
) -> list[str]:
    """Render the limits section as HTML lines (without the section header)."""

    if not snapshots:
        # Russian UI copy for the Telegram control forum.
        return ["Подключённых subscription-профилей нет."]
    lines: list[str] = []
    for snap in snapshots:
        title = (
            f"• <b>{html_escape(snap.company)}</b> · {html_escape(snap.plan_label)} · "
            f"<code>{html_escape(snap.profile_id)}</code>"
        )
        lines.append(title)
        if not snap.ok:
            detail = html_escape(snap.detail or "unknown")
            lines.append(f"  лимиты: недоступны ({detail})")
            continue
        lines.append(f"  5ч: {_window_label(snap.five_hour, html_escape)}")
        lines.append(f"  неделя: {_window_label(snap.weekly, html_escape)}")
    return lines


def _window_label(window: LimitWindow, html_escape: Callable[[object], str]) -> str:
    if not window.available:
        return html_escape(window.detail or "—")
    if window.remaining_percent is None and window.reset_at is None:
        return "—"
    parts: list[str] = []
    if window.remaining_percent is not None:
        parts.append(f"осталось {window.remaining_percent}%")
    if window.reset_at is not None:
        parts.append(f"сброс {_format_reset(window.reset_at)}")  # noqa: RUF001
    return html_escape(" · ".join(parts) if parts else "—")


def _format_reset(when: datetime) -> str:
    local = when.astimezone(UTC)
    return local.strftime("%Y-%m-%d %H:%M UTC")


def _unavailable(
    profile: ProviderProfileConfig, observed: datetime, detail: str
) -> SubscriptionLimitSnapshot:
    company, default_plan = _company_and_default_plan(profile.provider)
    empty = LimitWindow(remaining_percent=None, reset_at=None, available=False, detail="—")
    return SubscriptionLimitSnapshot(
        profile_id=profile.id,
        company=company,
        plan_label=default_plan,
        five_hour=empty,
        weekly=empty,
        observed_at=observed,
        ok=False,
        detail=detail,
    )


def _company_and_default_plan(provider: str) -> tuple[str, str]:
    if provider == "codex":
        return "OpenAI", "Plus"
    if provider == "grok":
        return "xAI", "Super"
    return provider, "—"


def _collect_codex(profile: ProviderProfileConfig, observed: datetime) -> SubscriptionLimitSnapshot:
    company, default_plan = _company_and_default_plan("codex")
    auth_path = Path(profile.state_directory) / "auth.json"  # type: ignore[arg-type]
    token = _codex_access_token(auth_path)
    if token is None:
        return _unavailable(profile, observed, "auth.json unreadable")
    payload = _http_json(
        CODEX_USAGE_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "vuzol-subscription-limits",
        },
    )
    if payload is None:
        return _unavailable(profile, observed, "usage endpoint failed")
    plan = str(payload.get("plan_type") or default_plan).strip()
    plan_label = _human_plan("codex", plan)
    rate = payload.get("rate_limit") if isinstance(payload.get("rate_limit"), dict) else {}
    five, weekly = _windows_from_codex_rate_limit(rate, observed)
    return SubscriptionLimitSnapshot(
        profile_id=profile.id,
        company=company,
        plan_label=plan_label,
        five_hour=five,
        weekly=weekly,
        observed_at=observed,
        ok=True,
    )


def _collect_grok(profile: ProviderProfileConfig, observed: datetime) -> SubscriptionLimitSnapshot:
    company, default_plan = _company_and_default_plan("grok")
    state = Path(profile.state_directory)  # type: ignore[arg-type]
    token = _grok_access_token(state)
    payload = None
    if token is not None:
        for url in GROK_BILLING_URLS:
            payload = _http_json(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "User-Agent": "GrokBuild/0.2.93",
                    "x-grok-client-version": "0.2.93",
                },
            )
            if payload is not None and not _looks_like_html(payload):
                break
            payload = None
    if payload is None:
        payload = _latest_grok_billing_from_logs(state)
    if payload is None:
        return _unavailable(profile, observed, "billing unavailable")
    config = payload.get("config") if isinstance(payload.get("config"), dict) else payload
    if not isinstance(config, dict):
        return _unavailable(profile, observed, "billing shape unknown")
    tier = str(
        payload.get("subscriptionTier")
        or payload.get("subscription_tier")
        or config.get("subscriptionTier")
        or default_plan
    )
    plan_label = _human_plan("grok", tier)
    weekly = _weekly_from_grok_config(config, observed)
    five = LimitWindow(
        remaining_percent=None,
        reset_at=None,
        available=False,
        detail="no 5h data",
    )
    # Optional short window fields if upstream starts exposing them.
    for key in ("fiveHour", "five_hour", "primaryWindow", "primary_window", "shortPeriod"):
        raw = config.get(key)
        if isinstance(raw, dict):
            five = _window_from_generic(raw, observed, default_seconds=_FIVE_HOUR_SECONDS)
            break
    return SubscriptionLimitSnapshot(
        profile_id=profile.id,
        company=company,
        plan_label=plan_label,
        five_hour=five,
        weekly=weekly,
        observed_at=observed,
        ok=True,
    )


def _human_plan(provider: str, raw: str) -> str:
    value = raw.strip()
    lowered = value.lower().replace(" ", "").replace("_", "")
    if provider == "codex":
        mapping = {
            "plus": "Plus",
            "pro": "Pro",
            "free": "Free",
            "team": "Team",
            "enterprise": "Enterprise",
        }
        return mapping.get(lowered, value.title() if value else "Plus")
    mapping = {
        "supergrok": "Super",
        "super": "Super",
        "supergrokheavy": "Super Heavy",
        "supergroklite": "Super Lite",
        "xpremiumplus": "Premium+",
        "xpremium": "Premium",
        "xbasic": "Basic",
    }
    return mapping.get(lowered, value or "Super")


def _windows_from_codex_rate_limit(
    rate: dict[str, Any], observed: datetime
) -> tuple[LimitWindow, LimitWindow]:
    primary = rate.get("primary_window") if isinstance(rate.get("primary_window"), dict) else None
    secondary = (
        rate.get("secondary_window") if isinstance(rate.get("secondary_window"), dict) else None
    )
    windows = [w for w in (primary, secondary) if w is not None]
    five = LimitWindow(None, None, available=False, detail="no 5h data")
    weekly = LimitWindow(None, None, available=False, detail="no data")
    for raw in windows:
        classified = _classify_codex_window(raw, observed)
        if classified is None:
            continue
        kind, window = classified
        if kind == "five_hour":
            five = window
        elif kind == "weekly":
            weekly = window
    return five, weekly


def _classify_codex_window(
    raw: dict[str, Any], observed: datetime
) -> tuple[str, LimitWindow] | None:
    seconds = raw.get("limit_window_seconds")
    try:
        window_seconds = int(seconds) if seconds is not None else None
    except (TypeError, ValueError):
        window_seconds = None
    window = _window_from_generic(raw, observed, default_seconds=window_seconds)
    if window_seconds is None:
        return None
    if abs(window_seconds - _FIVE_HOUR_SECONDS) <= 300 or window_seconds <= 6 * 3600:
        return "five_hour", window
    if abs(window_seconds - _WEEKLY_SECONDS) <= 3600 or window_seconds >= 3 * 24 * 3600:
        return "weekly", window
    return None


def _window_from_generic(
    raw: dict[str, Any], observed: datetime, *, default_seconds: int | None
) -> LimitWindow:
    used = raw.get("used_percent")
    remaining = raw.get("remaining_percent")
    remaining_percent: int | None = None
    try:
        if remaining is not None:
            remaining_percent = max(0, min(100, round(float(remaining))))
        elif used is not None:
            remaining_percent = max(0, min(100, round(100.0 - float(used))))
    except (TypeError, ValueError):
        remaining_percent = None
    reset_at = _parse_reset(raw, observed)
    return LimitWindow(
        remaining_percent=remaining_percent,
        reset_at=reset_at,
        window_seconds=default_seconds,
        available=True,
    )


def _weekly_from_grok_config(config: dict[str, Any], observed: datetime) -> LimitWindow:
    used = config.get("creditUsagePercent")
    remaining_percent: int | None = None
    try:
        if used is not None:
            remaining_percent = max(0, min(100, round(100.0 - float(used))))
    except (TypeError, ValueError):
        remaining_percent = None
    reset_at = None
    for key in ("billingPeriodEnd", "end"):
        raw = config.get(key)
        if raw is None and isinstance(config.get("currentPeriod"), dict):
            raw = config["currentPeriod"].get("end")
        if raw is not None:
            reset_at = _parse_datetime(raw)
            if reset_at is not None:
                break
    if remaining_percent is None and reset_at is None:
        return LimitWindow(None, None, available=False, detail="no data")
    return LimitWindow(
        remaining_percent=remaining_percent,
        reset_at=reset_at,
        window_seconds=_WEEKLY_SECONDS,
        available=True,
    )


def _parse_reset(raw: dict[str, Any], observed: datetime) -> datetime | None:
    if raw.get("reset_at") is not None:
        try:
            return datetime.fromtimestamp(int(raw["reset_at"]), tz=UTC)
        except (TypeError, ValueError, OSError, OverflowError):
            parsed = _parse_datetime(raw.get("reset_at"))
            if parsed is not None:
                return parsed
    if raw.get("reset_after_seconds") is not None:
        try:
            return observed + timedelta(seconds=max(0, int(raw["reset_after_seconds"])))
        except (TypeError, ValueError):
            return None
    for key in ("end", "resetAt", "resets_at"):
        parsed = _parse_datetime(raw.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (OSError, OverflowError, ValueError):
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _codex_access_token(auth_path: Path) -> str | None:
    try:
        data = json.loads(auth_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    tokens = data.get("tokens")
    if not isinstance(tokens, dict):
        return None
    token = tokens.get("access_token") or tokens.get("accessToken")
    return token if isinstance(token, str) and token else None


def _grok_access_token(state_directory: Path) -> str | None:
    candidates = (
        state_directory / "auth.json",
        state_directory / ".grok" / "auth.json",
    )
    for path in candidates:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, dict):
                    token = value.get("key") or value.get("access_token")
                    if isinstance(token, str) and token:
                        return token
            token = data.get("key") or data.get("access_token")
            if isinstance(token, str) and token:
                return token
    return None


def _latest_grok_billing_from_logs(state_directory: Path) -> dict[str, Any] | None:
    log_candidates = (
        state_directory / "logs" / "unified.jsonl",
        state_directory / ".grok" / "logs" / "unified.jsonl",
    )
    for path in log_candidates:
        if not path.is_file():
            continue
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line in reversed(lines[-500:]):
            if "billing: fetched credits config" not in line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            ctx = payload.get("ctx")
            if isinstance(ctx, dict):
                return ctx
    return None


def _http_json(url: str, *, headers: dict[str, str]) -> dict[str, Any] | None:
    scheme = urlparse(url).scheme
    if scheme not in {"http", "https"}:
        return None
    request = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=_FETCH_TIMEOUT_SECONDS) as response:  # noqa: S310
            body = response.read()
    except (urllib.error.URLError, TimeoutError, OSError):
        return None
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _looks_like_html(payload: dict[str, Any]) -> bool:
    # HTML error pages occasionally parse as invalid/empty structures; treat non-billing shapes.
    return not any(
        key in payload
        for key in (
            "config",
            "creditUsagePercent",
            "currentPeriod",
            "subscriptionTier",
            "rate_limit",
            "plan_type",
        )
    )
