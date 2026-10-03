"""One-kind canary allowlist with a deterministic cohort and kill switch (J5).

Admission is deterministic and scope-based, never based on model
self-confidence: a decision kind is enabled only if it is the single allowlisted
kind, the whitelist gate allows it, the kill switch is off, and the opportunity
falls into a stable hash bucket of the configured cohort. A denied admission
keeps the previous limited path; no unauthorized transition can occur.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from vuzol.config.settings import Settings
from vuzol.experiments.decision import WhitelistGate
from vuzol.experiments.domain import FrozenModel


class CanaryDecision(FrozenModel):
    decision_kind: str
    opportunity_id: str
    allowed: bool
    reason: str
    bucket: int


def cohort_bucket(opportunity_id: str) -> int:
    """Stable 0..99 bucket for an opportunity (deterministic, no randomness)."""

    return int(hashlib.sha256(opportunity_id.encode("utf-8")).hexdigest()[:8], 16) % 100


def in_cohort(opportunity_id: str, percent: int) -> bool:
    if percent <= 0:
        return False
    if percent >= 100:
        return True
    return cohort_bucket(opportunity_id) < percent


@dataclass(slots=True)
class KillSwitch:
    """Per-kind freeze. Freezing returns the previous limited path immediately."""

    frozen_kinds: set[str] = field(default_factory=set)

    def freeze(self, decision_kind: str) -> None:
        self.frozen_kinds.add(decision_kind)

    def thaw(self, decision_kind: str) -> None:
        self.frozen_kinds.discard(decision_kind)

    def is_frozen(self, decision_kind: str) -> bool:
        return decision_kind in self.frozen_kinds


@dataclass(slots=True)
class CanaryPolicy:
    """Exactly one allowlisted kind in a small deterministic cohort."""

    enabled_kind: str
    cohort_percent: int
    allowlist: WhitelistGate = field(default_factory=WhitelistGate)
    kill_switch: KillSwitch = field(default_factory=KillSwitch)

    def admit(self, *, decision_kind: str, opportunity_id: str) -> CanaryDecision:
        bucket = cohort_bucket(opportunity_id)
        if decision_kind != self.enabled_kind:
            return self._deny(decision_kind, opportunity_id, bucket, "kind_not_allowlisted")
        if self.kill_switch.is_frozen(decision_kind):
            return self._deny(decision_kind, opportunity_id, bucket, "kill_switch")
        if not self.allowlist.allows(decision_kind):
            return self._deny(decision_kind, opportunity_id, bucket, "not_whitelisted")
        if not in_cohort(opportunity_id, self.cohort_percent):
            return self._deny(decision_kind, opportunity_id, bucket, "outside_cohort")
        return CanaryDecision(
            decision_kind=decision_kind,
            opportunity_id=opportunity_id,
            allowed=True,
            reason="admitted",
            bucket=bucket,
        )

    def rollback(self) -> None:
        """Return to the previous limited path: freeze the canary kind."""

        self.kill_switch.freeze(self.enabled_kind)

    @staticmethod
    def _deny(decision_kind: str, opportunity_id: str, bucket: int, reason: str) -> CanaryDecision:
        return CanaryDecision(
            decision_kind=decision_kind,
            opportunity_id=opportunity_id,
            allowed=False,
            reason=reason,
            bucket=bucket,
        )


def canary_policy_from_settings(settings: Settings) -> CanaryPolicy | None:
    """Build the canary policy from settings; no enabled kind => disabled."""

    if not settings.jev_enabled_kinds:
        return None
    enabled_kind = settings.jev_enabled_kinds[0]
    allowlist = WhitelistGate(enabled_kinds=frozenset(settings.jev_enabled_kinds))
    return CanaryPolicy(
        enabled_kind=enabled_kind,
        cohort_percent=settings.jev_canary_percent,
        allowlist=allowlist,
        kill_switch=KillSwitch(set(settings.jev_kill_switch_kinds)),
    )
