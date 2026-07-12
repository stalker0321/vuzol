"""Closed provider failure taxonomy safe for business logic and logs."""

from dataclasses import dataclass

from vuzol.providers.domain import ProviderErrorCategory


@dataclass(frozen=True, slots=True)
class ProviderFailure(Exception):
    category: ProviderErrorCategory
    retryable: bool
    request_sent: bool
    retry_after_seconds: float | None = None
    safe_summary: str = "provider call failed"

    def __str__(self) -> str:
        return self.safe_summary
