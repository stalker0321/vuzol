"""Best-effort subscription limit snapshots for Codex and Grok profiles.

Codex ChatGPT Plus/Pro limits come from the ChatGPT usage endpoint using the
profile's isolated ``auth.json``. Grok Super limits come from the Grok billing
endpoint when reachable, otherwise the latest local ``billing: fetched credits
config`` log line under the profile state directory.

Network and filesystem failures are non-fatal: the dashboard still renders and
marks the profile as unavailable.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.storage.models import SubscriptionLimitSnapshotRow

# Outbox destination claimed by the executor (provider-state ACL for auth/logs).
SUBSCRIPTION_LIMITS_DESTINATION = "subscription_limits"

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


async def persist_subscription_limits(
    session: AsyncSession,
    snapshots: Sequence[SubscriptionLimitSnapshot],
) -> None:
    """Upsert the latest host-collected limit observation for each profile."""

    for snap in snapshots:
        row = await session.get(SubscriptionLimitSnapshotRow, snap.profile_id)
        if row is None:
            row = SubscriptionLimitSnapshotRow(profile_id=snap.profile_id)
            session.add(row)
        row.company = snap.company
        row.plan_label = snap.plan_label
        row.five_hour_remaining_percent = (
            snap.five_hour.remaining_percent if snap.five_hour.available else None
        )
        row.five_hour_reset_at = snap.five_hour.reset_at if snap.five_hour.available else None
        row.weekly_remaining_percent = (
            snap.weekly.remaining_percent if snap.weekly.available else None
        )
        row.weekly_reset_at = snap.weekly.reset_at if snap.weekly.available else None
        row.ok = snap.ok
        row.detail = snap.detail or None
        row.payload = {
            "five_hour": {
                "remaining_percent": snap.five_hour.remaining_percent,
                "reset_at": (
                    snap.five_hour.reset_at.isoformat() if snap.five_hour.reset_at else None
                ),
                "available": snap.five_hour.available,
                "detail": snap.five_hour.detail,
            },
            "weekly": {
                "remaining_percent": snap.weekly.remaining_percent,
                "reset_at": snap.weekly.reset_at.isoformat() if snap.weekly.reset_at else None,
                "available": snap.weekly.available,
                "detail": snap.weekly.detail,
            },
        }
        row.observed_at = snap.observed_at
    await session.flush()


async def load_subscription_limits(
    session: AsyncSession,
) -> tuple[SubscriptionLimitSnapshot, ...]:
    """Load the latest persisted snapshots for dashboard rendering."""

    rows = list(
        (
            await session.scalars(
                select(SubscriptionLimitSnapshotRow).order_by(
                    SubscriptionLimitSnapshotRow.profile_id.asc()
                )
            )
        ).all()
    )
    snapshots: list[SubscriptionLimitSnapshot] = []
    for row in rows:
        five = LimitWindow(
            remaining_percent=row.five_hour_remaining_percent,
            reset_at=row.five_hour_reset_at,
            available=row.five_hour_remaining_percent is not None,
        )
        weekly = LimitWindow(
            remaining_percent=row.weekly_remaining_percent,
            reset_at=row.weekly_reset_at,
            available=row.weekly_remaining_percent is not None or row.weekly_reset_at is not None,
        )
        if not row.ok:
            five = LimitWindow(None, None, available=False, detail=row.detail or "unavailable")
            weekly = LimitWindow(None, None, available=False, detail=row.detail or "unavailable")
        snapshots.append(
            SubscriptionLimitSnapshot(
                profile_id=row.profile_id,
                company=row.company,
                plan_label=row.plan_label,
                five_hour=five,
                weekly=weekly,
                observed_at=row.observed_at,
                ok=row.ok,
                detail=row.detail or "",
            )
        )
    return tuple(snapshots)


async def refresh_and_store_subscription_limits(
    session: AsyncSession,
    profiles: Sequence[ProviderProfileConfig],
) -> tuple[SubscriptionLimitSnapshot, ...]:
    """Collect host-visible limits and persist them for Telegram delivery."""

    snapshots = await collect_subscription_limits(profiles)
    await persist_subscription_limits(session, snapshots)
    return snapshots


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


_BAR_WIDTH = 10
_BAR_FILLED = "█"
_BAR_EMPTY = "░"


def format_subscription_limits_html(
    snapshots: Sequence[SubscriptionLimitSnapshot],
    *,
    html_escape: Callable[[object], str],
) -> list[str]:
    """Render the limits section as HTML lines (without the section header)."""

    if not snapshots:
        return ["No subscription profiles are connected."]
    lines: list[str] = []
    for snap in snapshots:
        title = (
            f"• <b>{html_escape(snap.company)}</b> · {html_escape(snap.plan_label)} · "
            f"<code>{html_escape(snap.profile_id)}</code>"
        )
        lines.append(title)
        if not snap.ok:
            detail = html_escape(snap.detail or "unknown")
            lines.append(f"  limits unavailable ({detail})")
            continue
        window_lines = (
            *_format_window_block("5h", snap.five_hour, html_escape),
            *_format_window_block("week", snap.weekly, html_escape),
        )
        if not window_lines:
            lines.append("  no limit windows reported")
            continue
        lines.extend(window_lines)
    return lines


def progress_bar(remaining_percent: int, *, width: int = _BAR_WIDTH) -> str:
    """Filled bar for used capacity; empty cells are still remaining."""

    remaining = max(0, min(100, int(remaining_percent)))
    used = 100 - remaining
    filled = max(0, min(width, round(used * width / 100)))
    empty = width - filled
    return f"[{_BAR_FILLED * filled}{_BAR_EMPTY * empty}]"


def _format_window_block(
    label: str,
    window: LimitWindow,
    html_escape: Callable[[object], str],
) -> tuple[str, ...]:
    """Skip windows without usable remaining %; never invent a missing 5h row."""

    if not window.available or window.remaining_percent is None:
        return ()
    bar = progress_bar(window.remaining_percent)
    # Monospace bar so cells stay aligned in Telegram HTML.
    line = (
        f"  <b>{html_escape(label)}</b>  "
        f"<code>{html_escape(bar)}</code>  "
        f"{html_escape(f'{window.remaining_percent}% left')}"
    )
    if window.reset_at is None:
        return (line,)
    reset = f"reset {_format_reset(window.reset_at)}"
    return (line, f"  <i>{html_escape(reset)}</i>")


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
    raw_rate = payload.get("rate_limit")
    rate: dict[str, Any] = raw_rate if isinstance(raw_rate, dict) else {}
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
    for _scope, token in _grok_auth_entries(state_directory):
        return token
    return None


def _grok_auth_entries(state_directory: Path) -> tuple[tuple[str, str], ...]:
    """Return ``(scope, access_token)`` pairs from a Grok state directory."""

    candidates = (
        state_directory / "auth.json",
        state_directory / ".grok" / "auth.json",
    )
    found: list[tuple[str, str]] = []
    for path in candidates:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        for key, value in data.items():
            if isinstance(value, dict):
                token = value.get("key") or value.get("access_token")
                if isinstance(token, str) and token:
                    found.append((str(key), token))
        token = data.get("key") or data.get("access_token")
        if isinstance(token, str) and token:
            found.append(("", token))
    return tuple(found)


def _jwt_subject(token: str) -> str | None:
    """Best-effort JWT ``sub`` / ``principal_id`` without signature verification."""

    parts = token.split(".")
    if len(parts) < 2:
        return None
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("sub", "principal_id"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _host_grok_billing_log_paths(state_directory: Path) -> tuple[Path, ...]:
    """Interactive host Grok profiles that share the same OAuth subject.

    Sandboxed provider state rarely records ``billing: fetched credits config``
    lines. Interactive CLI sessions under ``~/.grok-profiles`` do, and for the
    same Super accounts the OAuth subject matches.
    """

    subjects = {
        sub
        for _scope, token in _grok_auth_entries(state_directory)
        if (sub := _jwt_subject(token)) is not None
    }
    if not subjects:
        return ()
    # Hard-coded host layout used by this dogfood host; missing roots are skipped.
    roots = (
        Path("/home/vodkolyan/.grok-profiles"),
        Path.home() / ".grok-profiles",
    )
    matched: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.is_dir():
            continue
        try:
            accounts = list(root.iterdir())
        except OSError:
            continue
        for account in accounts:
            if not account.is_dir():
                continue
            account_subjects = {
                sub
                for _scope, token in _grok_auth_entries(account)
                if (sub := _jwt_subject(token)) is not None
            }
            if not subjects.intersection(account_subjects):
                continue
            for rel in ("logs/unified.jsonl", ".grok/logs/unified.jsonl"):
                path = account / rel
                if path in seen:
                    continue
                if path.is_file():
                    seen.add(path)
                    matched.append(path)
    return tuple(matched)


def _latest_grok_billing_from_logs(state_directory: Path) -> dict[str, Any] | None:
    log_candidates = (
        state_directory / "logs" / "unified.jsonl",
        state_directory / ".grok" / "logs" / "unified.jsonl",
        *_host_grok_billing_log_paths(state_directory),
    )
    for path in log_candidates:
        ctx = _billing_ctx_from_log_file(path)
        if ctx is not None:
            return ctx
    return None


def _billing_ctx_from_log_file(path: Path) -> dict[str, Any] | None:
    """Scan a Grok unified log for the latest credits-config payload.

    Interactive logs can be multi-MB; scan a large tail rather than the whole
    file, but far enough back that billing lines are not lost among shell noise.
    """

    if not path.is_file():
        return None
    try:
        # ~4 MiB tail is enough for days of interactive noise while staying cheap.
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - 4_000_000))
            raw = handle.read()
    except OSError:
        return None
    text = raw.decode("utf-8", errors="ignore")
    for line in reversed(text.splitlines()):
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
