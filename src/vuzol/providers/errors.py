"""Closed provider failure taxonomy safe for business logic and logs."""

from dataclasses import dataclass, replace
from datetime import datetime

from vuzol.providers.domain import (
    NormalizedUsage,
    OutputDiagnostic,
    ProviderErrorCategory,
    ProviderFailureDetails,
    ProviderResult,
    ProviderResultStatus,
)


class ProviderNotStartedError(ValueError):
    """Trusted transport proved that no provider process could have started."""


@dataclass(frozen=True, slots=True)
class ProviderFailure(Exception):
    category: ProviderErrorCategory
    retryable: bool
    request_sent: bool
    retry_after_seconds: float | None = None
    safe_summary: str = "provider call failed"
    # A rejected answer is still a measured invocation. Keep the receipt
    # independent of output validation so accounting never substitutes a
    # reservation for facts the provider already reported.
    usage: NormalizedUsage | None = None
    provider_request_id: str | None = None
    # Positive transport evidence that the invocation ended, independent of
    # whether it was sent or its cost was measured. Defaults to unknown. This
    # does not waive account ownership or supervised-runtime cleanup checks.
    execution_completed: bool = False
    diagnostic: OutputDiagnostic | None = None
    retry_at: datetime | None = None

    def details(self) -> ProviderFailureDetails:
        # Do not persist safe_summary: some adapters include response-derived
        # paths there. The closed category and structural facts are sufficient.
        return ProviderFailureDetails(
            category=self.category,
            retryable=self.retryable,
            request_sent=self.request_sent,
            execution_completed=self.execution_completed,
            diagnostic=self.diagnostic,
            retry_at=self.retry_at,
        )

    def with_receipt(
        self, usage: NormalizedUsage, provider_request_id: str | None = None
    ) -> "ProviderFailure":
        return replace(
            self,
            usage=self.usage if self.usage is not None else usage,
            provider_request_id=self.provider_request_id or provider_request_id,
        )

    def receipt_result(self) -> ProviderResult | None:
        """Adapt a failure receipt for accounting ports shared with successes."""
        if self.usage is None:
            return None
        return ProviderResult(
            status=ProviderResultStatus.FAILED,
            usage=self.usage,
            provider_request_id=self.provider_request_id,
            adapter_version="failure-receipt.v1",
            failure=self.details(),
        )

    def __str__(self) -> str:
        return self.safe_summary
