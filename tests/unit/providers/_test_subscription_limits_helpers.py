"""Subscription limit collectors for Codex/Grok dashboard section."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from vuzol.config.models import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.providers.subscription_limits import (
    LimitWindow,
    SubscriptionLimitSnapshot,
    _classify_codex_window,
    _http_json,
    _human_plan,
    _parse_datetime,
    _windows_from_codex_rate_limit,
    collect_profile_limits,
    collect_subscription_limits,
    format_subscription_limits_html,
    progress_bar,
    subscription_profiles,
)
from vuzol.telegram.projections import telegram_html

__all__ = [
    "UTC",
    "Any",
    "Capability",
    "CostClass",
    "LaunchMode",
    "LimitWindow",
    "Path",
    "ProviderProfileConfig",
    "ProviderRole",
    "SimpleNamespace",
    "SubscriptionLimitSnapshot",
    "_classify_codex_window",
    "_cli_profile",
    "_http_json",
    "_human_plan",
    "_parse_datetime",
    "_windows_from_codex_rate_limit",
    "annotations",
    "base64",
    "collect_profile_limits",
    "collect_subscription_limits",
    "datetime",
    "format_subscription_limits_html",
    "json",
    "progress_bar",
    "pytest",
    "subscription_profiles",
    "telegram_html",
]


def _cli_profile(profile_id: str, provider: str, state: Path) -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id=profile_id,
        provider=provider,
        model="test-model",
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.STRONG,
        roles=frozenset({ProviderRole.EXECUTOR}),
        routing_priority=100,
        supported_task_types=frozenset({"coding"}),
        sandbox_required=True,
        runtime_identity=f"id-{profile_id}",
        state_directory=state,
        enabled=True,
    )
