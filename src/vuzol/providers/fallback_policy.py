"""Classify failures that justify changing provider profiles."""

PROVIDER_FALLBACK_FAILURES = frozenset(
    {
        "authentication",
        "context_too_large",
        "invalid_structured_output",
        "permanent_request",
        "provider_execution_unexpected",
        "provider_unavailable",
        "quota_exhausted",
        "rate_limited",
        "safety_refusal",
        "timeout",
        "unsupported_capability",
    }
)


def should_fallback_provider(category: str | None) -> bool:
    """Return true only when changing provider can plausibly fix the failure."""

    return category in PROVIDER_FALLBACK_FAILURES
