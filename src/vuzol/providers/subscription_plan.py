"""Informational subscription evidence, never an execution/entitlement policy.

A configured model, login, API key or failed usage request does not establish
a paid plan. Only explicit provider fields (or a validated Go meter) do.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

PlanSource = Literal["codex_usage", "grok_billing", "opencode_go_usage"]


class SubscriptionPlan(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    service: Literal["codex", "grok", "opencode"]
    tier_id: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9_+.-]{0,47}$")
    status: Literal["reported", "not_entitled", "unknown"] = "unknown"
    source: PlanSource
    observed_at: AwareDatetime
    stale: bool = False

    @model_validator(mode="after")
    def coherent(self) -> SubscriptionPlan:
        if (self.status == "reported") != (self.tier_id is not None):
            raise ValueError("plan_evidence_invalid")
        expected = {"codex": "codex_usage", "grok": "grok_billing", "opencode": "opencode_go_usage"}
        if self.source != expected[self.service]:
            raise ValueError("plan_source_invalid")
        if self.status == "not_entitled" and self.service != "opencode":
            raise ValueError("plan_entitlement_unsupported")
        return self

    @property
    def label(self) -> str:
        if self.status == "not_entitled":
            return "Go: нет доступа для подключения"
        if self.tier_id is None:
            return "тариф неизвестен"
        labels = {
            "codex": {
                "plus": "Plus",
                "pro": "Pro",
                "free": "Free",
                "go": "Go",
                "team": "Team",
                "business": "Business",
                "enterprise": "Enterprise",
                "edu": "Edu",
            },
            "grok": {
                "supergrok": "Super",
                "super": "Super",
                "supergrokheavy": "Super Heavy",
                "supergroklite": "Super Lite",
                "xpremiumplus": "Premium+",
                "xpremium": "Premium",
                "xbasic": "Basic",
            },
            "opencode": {"go": "Go"},
        }
        # New provider tiers remain distinguishable; no silently guessed mapping.
        normalized = self.tier_id.replace("_", "")
        return labels[self.service].get(normalized, self.tier_id)


def reported_plan(
    service: Literal["codex", "grok", "opencode"],
    raw_tier: object,
    *,
    source: PlanSource,
    observed_at: datetime,
) -> SubscriptionPlan:
    tier = raw_tier.strip().lower().replace(" ", "_") if isinstance(raw_tier, str) else ""
    valid = re.fullmatch(r"[a-z0-9][a-z0-9_+.-]{0,47}", tier) is not None
    return SubscriptionPlan(
        service=service,
        tier_id=tier if valid else None,
        status="reported" if valid else "unknown",
        source=source,
        observed_at=observed_at,
    )
