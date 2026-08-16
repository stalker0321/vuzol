from vuzol.providers.fallback_policy import should_fallback_provider


def test_provider_failures_enable_fallback() -> None:
    for category in (
        "authentication",
        "quota_exhausted",
        "rate_limited",
        "provider_unavailable",
        "timeout",
        "invalid_structured_output",
        "unsupported_capability",
        "safety_refusal",
    ):
        assert should_fallback_provider(category)


def test_work_failures_stay_with_same_provider() -> None:
    for category in (
        "review_changes_required",
        "validation_failed",
        "review_failed",
        "static_build_failed",
        "no_compatible_profile",
        "budget_exhausted",
        None,
    ):
        assert not should_fallback_provider(category)
