"""Shared fail-closed policy for user-requested step retries."""

from vuzol.storage.models import Step
from vuzol.storage.types import IdempotencyClass, StepStatus

EXPLICITLY_RETRYABLE_FAILURES = frozenset(
    {
        "authentication",
        "budget_exhausted",
        "capability_setup_required",
        "capability_provisioning_failed",
        "cancelled",
        "dependency_provisioning_failed",
        "independent_review_required",
        "admission_only_consent_required",
        "money_cap_reached",
        "money_cap_uncertified",
        "acceptance_evidence_missing",
        "quota_exhausted",
        "rate_limited",
        "timeout",
        "provider_unavailable",
        "provider_attempts_exhausted",
    }
)

# A label of read-only does not authorize a repeat when the step can still mutate.
MUTATING_CAPABILITIES = frozenset(
    {
        "filesystem_write",
        "code_edit",
        "git",
        "host_admin",
        "project_shell",
        "telegram_send",
        "secrets",
        "network",
    }
)
LEASE_EXPIRED_ATTEMPTS_EXHAUSTED = "lease_expired_attempts_exhausted"
LEASE_EXPIRED_UNKNOWN_EFFECTS = "lease_expired_unknown_effects"


def repeatable_without_new_mutation(step: Step) -> bool:
    """True only when a repeat cannot invent a second external write.

    Read-only is accepted when the step also lacks mutating capabilities.
    An idempotent step needs a real external key; the key is what makes the
    write comparable. Every other class stays unknown.
    """

    capabilities = {str(item) for item in (step.required_capabilities or [])}
    labeled_read_only = step.idempotency_class is IdempotencyClass.READ_ONLY
    if labeled_read_only:
        return capabilities.isdisjoint(MUTATING_CAPABILITIES)
    key = step.external_idempotency_key
    return (
        step.idempotency_class is IdempotencyClass.IDEMPOTENT
        and isinstance(key, str)
        and bool(key.strip())
    )


def current_retry_target(steps: tuple[Step, ...]) -> Step | None:
    """One blocked leaf, allowing only mutually bound repair/preflight pairs."""
    blocked = tuple(step for step in steps if step.status is StepStatus.BLOCKED)
    suspended: set[object] = set()
    for parent in blocked:
        if parent.unknown_effects:
            continue
        parent_payload = parent.payload if isinstance(parent.payload, dict) else {}
        if parent.step_type == "validate":
            for child in blocked:
                payload = child.payload if isinstance(child.payload, dict) else {}
                metadata = (
                    child.dependency_metadata if isinstance(child.dependency_metadata, dict) else {}
                )
                if (
                    child.step_type == "ensure_dependencies"
                    and child.run_id == parent.run_id
                    and parent_payload.get("environment_recovery_step_id") == str(child.id)
                    and payload.get("validation_environment_for_step_id") == str(parent.id)
                    and metadata.get("template_key") == "validation_environment_recovery"
                ):
                    suspended.add(parent.id)
        if (
            parent.step_type == "ensure_dependencies"
            and parent.failure_category == "capability_preflight_required"
        ):
            for child in blocked:
                child_payload = child.payload if isinstance(child.payload, dict) else {}
                child_metadata = (
                    child.dependency_metadata if isinstance(child.dependency_metadata, dict) else {}
                )
                if (
                    child.step_type == "ensure_capabilities"
                    and child.run_id == parent.run_id
                    and child_payload.get("capability_for_step_id") == str(parent.id)
                    and parent_payload.get("capability_preflight_step_id") == str(child.id)
                    and child_metadata.get("template_key") == "ensure_capabilities_after_code"
                ):
                    suspended.add(parent.id)
        if parent.step_type not in {"validate", "review"}:
            continue
        for child in blocked:
            payload = child.payload if isinstance(child.payload, dict) else {}
            metadata = (
                child.dependency_metadata if isinstance(child.dependency_metadata, dict) else {}
            )
            if (
                child.step_type == "execute_code"
                and child.run_id == parent.run_id
                and payload.get("repair_for_step_id") == str(parent.id)
                and parent_payload.get("repair_scheduled_step_id") == str(child.id)
                and metadata.get("template_key") == "repair_code"
                and metadata.get("repair_for_ordinal") == parent.ordinal
            ):
                suspended.add(parent.id)
    leaves = [step for step in blocked if step.id not in suspended]
    return leaves[0] if len(leaves) == 1 else None


def blocked_step_is_retryable(step: Step) -> bool:
    if step.unknown_effects:
        return False
    category = step.failure_category or ""
    return (
        step.attempt_count < step.max_attempts
        or category in EXPLICITLY_RETRYABLE_FAILURES
        # Validation is deterministic and has no external side effects. An explicit
        # retry is safe after code, configuration, or a guardrail changes.
        or category.startswith("validation_")
    )


def failed_item_is_rematerializable(step: Step) -> bool:
    """A manual package retry creates a fresh Task; terminal evidence is immutable."""

    return (
        step.status is StepStatus.FAILED
        and not step.unknown_effects
        and step.step_type
        in {
            "prepare_context",
            "prepare_worktree",
            "validate",
            "review",
            "build_static",
            "publish_preview",
        }
    )
