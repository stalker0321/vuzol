"""Fixed-endpoint Go usage, never inference or Console/PAYG fallback.

Contract provenance: anomalyco/opencode@2b8a5969e932c15083e599c82d34ce0268f81b9e
packages/console/{app/src/routes/zen/go/v1/usage.ts,core/src/subscription.ts}.
Live schemas are validated independently; unsupported responses fail closed.
OAuth endpoint/schema: get-bb/bb@2884950f17e76f78b28c64ccf4a2e9a7c74188a8,
packages/provider-bridge-acp/src/bridge/opencode-usage.ts (protocol reference).
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import httpx

from vuzol.config.models import AccountConfig
from vuzol.providers.account_auth import AccountAuthError, read_go_key
from vuzol.providers.opencode_oauth import read_go_oauth
from vuzol.providers.quota import QuotaWindow

GO_USAGE_URL = "https://opencode.ai/zen/go/v1/usage"
GO_OAUTH_USAGE_URL = "https://opencode.ai/inference/go/v1/usage"
MAX_BODY = 65536


@dataclass(frozen=True)
class GoUsage:
    attempted_at: datetime
    observed_at: datetime | None = None
    windows: tuple[QuotaWindow, ...] = ()
    detail: str = ""
    retry_after_seconds: float = 30
    use_balance: bool | None = None
    entitlement_observed_at: datetime | None = None


def parse_go_usage(raw: object, *, observed_at: datetime, scope_id: str) -> tuple[QuotaWindow, ...]:
    if not isinstance(raw, dict):
        raise ValueError("usage_schema_invalid")
    windows = []
    for source, name, maximum in (
        ("rollingUsage", "rolling", 5 * 3600),
        ("weeklyUsage", "weekly", 7 * 86400),
        ("monthlyUsage", "monthly", 32 * 86400),
    ):
        value = raw.get(source)
        if not isinstance(value, dict):
            raise ValueError("usage_schema_invalid")
        reset = value.get("resetInSec")
        if (
            isinstance(reset, bool)
            or not isinstance(reset, (int, float))
            or not 0 <= reset <= maximum
        ):
            raise ValueError("usage_schema_invalid")
        windows.append(
            QuotaWindow.model_validate(
                {
                    "id": name,
                    "used_percent": value.get("usagePercent"),
                    "reset_at": observed_at + timedelta(seconds=reset),
                    "status": value.get("status"),
                    "scope_id": scope_id,
                }
            )
        )
    return tuple(windows)


def _retry_after(value: str | None, now: datetime) -> float:
    try:
        if value is None:
            return 60
        seconds = (
            float(value)
            if value.isdecimal()
            else (parsedate_to_datetime(value) - now).total_seconds()
        )
        return max(1, min(86400, seconds))
    except (ValueError, TypeError, OverflowError):
        return 60


def parse_oauth_go_usage(
    raw: object, *, observed_at: datetime, scope_id: str
) -> tuple[QuotaWindow, ...]:
    """New Console contract; never infer a schema from a legacy fallback field."""
    if not isinstance(raw, dict) or not isinstance(raw.get("usage"), dict):
        raise ValueError("usage_schema_invalid")
    windows = []
    for name, maximum in (("rolling", 5 * 3600), ("weekly", 7 * 86400), ("monthly", 32 * 86400)):
        value = raw["usage"].get(name)
        if not isinstance(value, dict):
            raise ValueError("usage_schema_invalid")
        reset = value.get("resetsAt")
        if not isinstance(reset, str) or len(reset) > 64 or "T" not in reset:
            raise ValueError("usage_schema_invalid")
        reset_at = datetime.fromisoformat(reset.replace("Z", "+00:00"))
        if (
            reset_at.tzinfo is None
            or not -120 <= (reset_at - observed_at).total_seconds() <= maximum + 120
        ):
            raise ValueError("usage_schema_invalid")
        windows.append(
            QuotaWindow.model_validate(
                {
                    "id": name,
                    "used_percent": value.get("percent"),
                    "reset_at": reset_at,
                    "status": value.get("status"),
                    "scope_id": scope_id,
                }
            )
        )
    return tuple(windows)


class OpenCodeUsageCollector:
    """Bounded in-process cache and same-account single flight across threads.

    The executor owns collection. No model slot, no background daemon, and no
    retry loop: subsequent refresh ticks obey cooldown. Cache reads retain the
    original measurement time. Restart may re-read usage, never inference.
    """

    def __init__(
        self,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client_factory = client_factory or (
            lambda: httpx.Client(
                timeout=httpx.Timeout(4),
                follow_redirects=False,
                trust_env=False,
            )
        )
        self._clock, self._monotonic = clock, monotonic
        self._cache: OrderedDict[tuple[str, ...], tuple[float, GoUsage]] = OrderedDict()
        # Fixed stripes bound memory even under arbitrary config churn.
        self._locks = tuple(threading.Lock() for _ in range(32))
        self._cache_lock = threading.Lock()
        self._capacity = threading.BoundedSemaphore(4)

    def collect(self, account: AccountConfig) -> GoUsage:
        binding = account.quota_oauth
        key = (
            account.id,
            account.quota_scope_id or "",
            str(account.state_directory),
            binding.model_dump_json() if binding else "static",
        )
        attempted = self._clock()
        with self._locks[hash(key) % len(self._locks)]:
            if account.quota_collector != "opencode_go" or account.quota_scope_id is None:
                return GoUsage(attempted, detail="usage_binding_invalid")
            try:
                oauth = read_go_oauth(binding, now=attempted) if binding else None
                token = oauth.access if oauth else read_go_key(account)
            except AccountAuthError as error:
                return GoUsage(attempted, detail=str(error), retry_after_seconds=30)
            # Check local authorization before cache hits, and observe rotations
            # immediately even during cached 401/403 cooldown. No secrets stored.
            generation = hashlib.sha256(token.get_secret_value().encode()).hexdigest()
            cache_key = (*key, generation)
            with self._cache_lock:
                cached = self._cache.get(cache_key)
            if cached and cached[0] > self._monotonic():
                return replace(cached[1], attempted_at=attempted)
            with self._capacity:
                result = self._fetch(account, attempted, token.get_secret_value())
            # A logout/rotation while HTTP was in flight fences the old result.
            if binding:
                try:
                    current = read_go_oauth(binding, now=self._clock())
                    if current != oauth:
                        return GoUsage(
                            attempted, detail="auth_store_changed", retry_after_seconds=1
                        )
                except AccountAuthError as error:
                    return GoUsage(attempted, detail=str(error), retry_after_seconds=30)
            with self._cache_lock:
                self._cache[cache_key] = (self._monotonic() + result.retry_after_seconds, result)
                self._cache.move_to_end(cache_key)
                while len(self._cache) > 256:
                    self._cache.popitem(last=False)
            return result

    def _fetch(self, account: AccountConfig, attempted: datetime, token: str) -> GoUsage:
        try:
            if account.quota_collector != "opencode_go" or account.quota_scope_id is None:
                return GoUsage(attempted, detail="usage_binding_invalid")
            binding = account.quota_oauth
            deadline = self._monotonic() + 4
            with (
                self._client_factory() as client,
                client.stream(
                    "GET",
                    GO_OAUTH_USAGE_URL if binding else GO_USAGE_URL,
                    headers={
                        "Authorization": "Bearer " + token,
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                        "User-Agent": "vuzol-usage/1",
                        **({"x-opencode-org-id": binding.org_id} if binding else {}),
                    },
                    timeout=httpx.Timeout(4),
                    follow_redirects=False,
                ) as response,
            ):
                if response.status_code == 429:
                    return GoUsage(
                        attempted,
                        detail="usage_rate_limited",
                        retry_after_seconds=_retry_after(
                            response.headers.get("retry-after"), attempted
                        ),
                    )
                if response.is_redirect:
                    return GoUsage(attempted, detail="usage_redirect_denied")
                if response.status_code >= 500:
                    return GoUsage(attempted, detail="usage_unavailable", retry_after_seconds=60)
                if response.status_code == 401:
                    return GoUsage(attempted, detail="reauth_required", retry_after_seconds=300)
                if response.headers.get("content-encoding", "identity") != "identity":
                    return GoUsage(attempted, detail="usage_schema_invalid")
                body = bytearray()
                for chunk in response.iter_bytes():
                    if self._monotonic() > deadline:
                        return GoUsage(attempted, detail="usage_timeout", retry_after_seconds=60)
                    body.extend(chunk)
                    if len(body) > MAX_BODY:
                        return GoUsage(attempted, detail="usage_oversized")
                if response.status_code == 403:
                    try:
                        raw_error = json.loads(body)
                        entitlement = (
                            isinstance(raw_error, dict)
                            and isinstance(raw_error.get("error"), dict)
                            and raw_error["error"].get("type") == "EntitlementError"
                        )
                    except (ValueError, UnicodeError):
                        entitlement = False
                    return GoUsage(
                        attempted,
                        detail="go_entitlement_missing"
                        if entitlement
                        else "usage_permission_denied",
                        retry_after_seconds=300,
                        entitlement_observed_at=self._clock() if entitlement else None,
                    )
                if response.status_code != 200:
                    return GoUsage(attempted, detail="usage_unavailable")
                if (
                    response.headers.get("content-type", "").split(";")[0].strip()
                    != "application/json"
                ):
                    return GoUsage(attempted, detail="usage_schema_invalid")
                observed = self._clock()
                date = response.headers.get("date")
                if date:
                    server_time = parsedate_to_datetime(date)
                    age = float(response.headers.get("age", "0"))
                    if (
                        server_time.tzinfo is None
                        or not 0 <= age <= 900
                        or server_time > observed + timedelta(seconds=120)
                    ):
                        return GoUsage(attempted, detail="usage_clock_skew")
                    if (observed - server_time).total_seconds() > 900:
                        return GoUsage(attempted, detail="usage_stale")
                    observed = server_time
                raw = json.loads(body)
                parser = parse_oauth_go_usage if binding else parse_go_usage
                windows = parser(raw, observed_at=observed, scope_id=account.quota_scope_id)
                balance = None if binding else raw.get("useBalance")
                if balance is not None and not isinstance(balance, bool):
                    raise ValueError("usage_schema_invalid")
                return GoUsage(attempted, observed, windows, use_balance=balance)
        except AccountAuthError as error:
            return GoUsage(attempted, detail=str(error), retry_after_seconds=300)
        except httpx.TimeoutException:
            return GoUsage(attempted, detail="usage_timeout", retry_after_seconds=60)
        except httpx.HTTPError:
            return GoUsage(attempted, detail="usage_unavailable", retry_after_seconds=60)
        except (ValueError, TypeError, OverflowError, RecursionError):
            return GoUsage(attempted, detail="usage_schema_invalid")


go_usage_collector = OpenCodeUsageCollector()
