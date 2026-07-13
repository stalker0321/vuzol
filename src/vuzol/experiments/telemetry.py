"""Persistence and aggregation using the existing durable event ledger."""

import uuid
from collections import Counter
from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.experiments.domain import ExperimentTelemetry, ReviewOutcome, stable_json_hash
from vuzol.storage.models import Event

EVENT_TYPE = "step09a.trial_recorded"


async def record_trial(session: AsyncSession, telemetry: ExperimentTelemetry) -> uuid.UUID:
    event = Event(
        entity_type="step09a_experiment",
        entity_id=uuid.uuid5(uuid.NAMESPACE_URL, telemetry.experiment_id),
        event_type=EVENT_TYPE,
        actor_type="experiment_orchestrator",
        actor_id=telemetry.worker_profile,
        correlation_id=telemetry.experiment_id,
        payload={
            "telemetry": telemetry.model_dump(mode="json"),
            "telemetry_sha256": stable_json_hash(telemetry),
        },
    )
    session.add(event)
    await session.flush()
    return event.id


async def load_trials(session: AsyncSession, experiment_id: str) -> tuple[ExperimentTelemetry, ...]:
    events = await session.scalars(
        select(Event)
        .where(Event.event_type == EVENT_TYPE, Event.correlation_id == experiment_id)
        .order_by(Event.created_at, Event.id)
    )
    return tuple(ExperimentTelemetry.model_validate(event.payload["telemetry"]) for event in events)


def aggregate_trials(trials: Sequence[ExperimentTelemetry]) -> dict[str, Any]:
    outcomes = Counter(trial.final_outcome.value for trial in trials)
    provider_input = sum(
        invocation.usage.input_tokens or 0 for trial in trials for invocation in trial.invocations
    )
    provider_output = sum(
        invocation.usage.output_tokens or 0 for trial in trials for invocation in trial.invocations
    )
    total_context = sum(trial.total_context_bytes for trial in trials)
    repeated_context = sum(trial.repeated_context_bytes for trial in trials)
    false_accepts = sum(
        trial.shadow_would_accept and not trial.shadow_decision_correct for trial in trials
    )
    false_rejects = sum(
        not trial.shadow_would_accept and not trial.shadow_decision_correct for trial in trials
    )
    return {
        "task_count": len(trials),
        "outcomes": dict(outcomes),
        "provider_input_tokens": provider_input,
        "provider_output_tokens": provider_output,
        "context_bytes": total_context,
        "repeated_context_bytes": repeated_context,
        "repeated_context_ratio": repeated_context / total_context if total_context else 0.0,
        "shadow_false_accepts": false_accepts,
        "shadow_false_rejects": false_rejects,
        "accepted_first_pass": outcomes[ReviewOutcome.ACCEPTED_FIRST_PASS.value],
    }
