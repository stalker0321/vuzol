"""Private Step 09A seed, record, inspect, and export command."""

import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import get_runtime_configuration
from vuzol.experiments.domain import ExperimentTelemetry
from vuzol.experiments.service import TrialSeedRequest, seed_trial
from vuzol.experiments.telemetry import (
    INVOCATION_ROLES,
    aggregate_trials,
    aggregate_usage_by_role,
    load_trials,
    record_trial,
)
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.models import Artifact, Run, Step, SupervisedProcess, UsageRecord, Worktree


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded Step 09A experiment administration")
    subparsers = parser.add_subparsers(dest="command", required=True)
    seed = subparsers.add_parser("seed")
    seed.add_argument("request", type=Path)
    record = subparsers.add_parser("record")
    record.add_argument("telemetry", type=Path)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("experiment_id")
    export = subparsers.add_parser("export")
    export.add_argument("experiment_id")
    export.add_argument("--json", type=Path, required=True)
    export.add_argument("--csv", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(_run(args))


async def _run(args: argparse.Namespace) -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    engine = create_engine(runtime.settings, resolve_database_dsn(runtime.settings))
    factory = create_session_factory(engine)
    try:
        if args.command == "seed":
            request = TrialSeedRequest.model_validate_json(args.request.read_text())
            async with factory.begin() as session:
                trial = await seed_trial(session, runtime.registries, request)
            _print_json(
                {
                    "task_uuid": str(trial.task_uuid),
                    "run_uuid": str(trial.run_uuid),
                    "interpretation_uuid": str(trial.interpretation_uuid),
                    "capsule": trial.capsule.model_dump(mode="json"),
                }
            )
        elif args.command == "record":
            telemetry = ExperimentTelemetry.model_validate_json(args.telemetry.read_text())
            async with factory.begin() as session:
                event_id = await record_trial(session, telemetry)
            _print_json({"event_id": str(event_id)})
        elif args.command == "inspect":
            _print_json(await _inspect(factory, args.experiment_id))
        elif args.command == "export":
            async with factory() as session:
                trials = await load_trials(session, args.experiment_id)
            payload = {
                "schema_version": "step09a-export.v1",
                "experiment_id": args.experiment_id,
                "summary": aggregate_trials(trials),
                "trials": [trial.model_dump(mode="json") for trial in trials],
            }
            args.json.parent.mkdir(parents=True, exist_ok=True)
            args.csv.parent.mkdir(parents=True, exist_ok=True)
            args.json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            _write_csv(args.csv, trials)
            _print_json({"json": str(args.json), "csv": str(args.csv), "trials": len(trials)})
    finally:
        await engine.dispose()


async def _inspect(factory: async_sessionmaker[AsyncSession], experiment_id: str) -> dict[str, Any]:
    async with factory() as session:
        runs = (
            await session.scalars(
                select(Run)
                .where(Run.workflow_type == "adaptive_worker_trial")
                .order_by(Run.created_at)
            )
        ).all()
        selected = [run for run in runs if run.selected_route.get("experiment_id") == experiment_id]
        result: list[dict[str, Any]] = []
        for run in selected:
            steps = (
                await session.scalars(
                    select(Step).where(Step.run_id == run.id).order_by(Step.ordinal, Step.id)
                )
            ).all()
            worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
            usage = (
                await session.scalars(select(UsageRecord).where(UsageRecord.run_id == run.id))
            ).all()
            processes = (
                await session.scalars(
                    select(SupervisedProcess)
                    .where(SupervisedProcess.run_id == run.id)
                    .order_by(SupervisedProcess.created_at, SupervisedProcess.id)
                )
            ).all()
            artifacts = (
                await session.scalars(
                    select(Artifact)
                    .where(Artifact.run_id == run.id)
                    .order_by(Artifact.created_at, Artifact.artifact_type, Artifact.id)
                )
            ).all()
            steps = sorted(
                (step for step in steps if step.run_id == run.id),
                key=lambda step: (step.ordinal, str(step.id)),
            )
            if worktree is not None and worktree.run_id != run.id:
                worktree = None
            usage = [item for item in usage if item.run_id == run.id]
            processes = sorted(
                (process for process in processes if process.run_id == run.id),
                key=lambda process: (process.created_at, str(process.id)),
            )
            artifacts = sorted(
                (artifact for artifact in artifacts if artifact.run_id == run.id),
                key=lambda artifact: (
                    artifact.created_at,
                    artifact.artifact_type,
                    str(artifact.id),
                ),
            )
            result.append(
                {
                    "task_id": run.selected_route.get("experiment_task_id"),
                    "task_uuid": str(run.task_id),
                    "run_uuid": str(run.id),
                    "status": run.status.value,
                    "profile_id": run.selected_route.get("trusted_profile_id"),
                    "steps": [
                        {
                            "step_uuid": str(step.id),
                            "ordinal": step.ordinal,
                            "type": step.step_type,
                            "status": step.status.value,
                            "attempt_count": step.attempt_count,
                            "failure_category": step.failure_category,
                        }
                        for step in steps
                    ],
                    "worktree": (
                        {
                            "branch": worktree.branch,
                            "base_commit": worktree.base_commit,
                            "result_commit": worktree.result_commit,
                            "delivery_state": worktree.delivery_state.value,
                            "diff_hash": worktree.diff_hash,
                            "patch_artifact_id": _uuid_or_none(worktree.patch_artifact_id),
                            "changed_files_artifact_id": _uuid_or_none(
                                worktree.changed_files_artifact_id
                            ),
                        }
                        if worktree is not None
                        else None
                    ),
                    "usage": [
                        {
                            "profile_id": item.profile_id,
                            "model": item.model,
                            "input_tokens": item.input_tokens,
                            "cached_tokens": item.cached_tokens,
                            "output_tokens": item.output_tokens,
                            "duration_ms": item.duration_ms,
                            "cost_units": str(item.cost_units)
                            if item.cost_units is not None
                            else None,
                        }
                        for item in usage
                    ],
                    "processes": [_serialize_process(process) for process in processes],
                    "artifacts": [_serialize_artifact(artifact) for artifact in artifacts],
                }
            )
    return {"experiment_id": experiment_id, "runs": result}


def _serialize_process(process: SupervisedProcess) -> dict[str, Any]:
    return {
        "process_uuid": str(process.id),
        "step_uuid": str(process.step_id),
        "profile_id": process.profile_id,
        "provider_attempt": process.provider_attempt,
        "status": process.status.value,
        "outcome": process.outcome.value if process.outcome is not None else None,
        "image_digest": process.image_digest,
        "exit_code": process.exit_code,
        "duration_ms": _safe_duration_ms(process.runtime_metadata),
        "provider_events_artifact_id": _uuid_or_none(process.provider_events_artifact_id),
        "provider_result_artifact_id": _uuid_or_none(process.provider_result_artifact_id),
    }


def _serialize_artifact(artifact: Artifact) -> dict[str, Any]:
    return {
        "artifact_uuid": str(artifact.id),
        "step_uuid": _uuid_or_none(artifact.step_id),
        "producer_process_uuid": _uuid_or_none(artifact.producer_process_id),
        "type": artifact.artifact_type,
        "size_bytes": artifact.size_bytes,
        "content_hash": artifact.content_hash,
        "media_type": artifact.media_type,
        "storage_state": artifact.storage_state.value,
        "verified_at": artifact.verified_at.isoformat() if artifact.verified_at else None,
    }


def _safe_duration_ms(runtime_metadata: object) -> int | None:
    if not isinstance(runtime_metadata, dict):
        return None
    value = runtime_metadata.get("actual_elapsed_ms")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _uuid_or_none(value: object) -> str | None:
    return str(value) if value is not None else None


def _write_csv(path: Path, trials: tuple[ExperimentTelemetry, ...]) -> None:
    existing_fields = (
        "experiment_id",
        "task_id",
        "task_class",
        "predicted_mode",
        "actual_mode",
        "worker_profile",
        "final_outcome",
        "worker_attempts",
        "repair_count",
        "repair_severity",
        "execution_duration_ms",
        "review_duration_ms",
        "total_wall_time_ms",
        "context_bytes",
        "repeated_context_bytes",
        "repeated_context_ratio",
        "shadow_would_accept",
        "shadow_decision_correct",
        "estimated_cost",
    )
    role_fields = tuple(
        field
        for role in INVOCATION_ROLES
        for field in (
            f"{role}_invocation_count",
            f"{role}_usage_unavailable_invocations",
            f"{role}_input_tokens",
            f"{role}_cached_input_tokens",
            f"{role}_output_tokens",
            f"{role}_reasoning_tokens",
        )
    )
    fields = (*existing_fields, *role_fields)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for trial in trials:
            row: dict[str, object] = {
                "experiment_id": trial.experiment_id,
                "task_id": trial.task_id,
                "task_class": trial.task_class.value,
                "predicted_mode": trial.predicted_mode.value,
                "actual_mode": trial.actual_mode.value,
                "worker_profile": trial.worker_profile,
                "final_outcome": trial.final_outcome.value,
                "worker_attempts": trial.worker_attempts,
                "repair_count": trial.repair_count,
                "repair_severity": trial.repair_severity.value,
                "execution_duration_ms": trial.execution_duration_ms,
                "review_duration_ms": trial.review_duration_ms,
                "total_wall_time_ms": trial.total_wall_time_ms,
                "context_bytes": trial.total_context_bytes,
                "repeated_context_bytes": trial.repeated_context_bytes,
                "repeated_context_ratio": trial.repeated_context_ratio,
                "shadow_would_accept": trial.shadow_would_accept,
                "shadow_decision_correct": trial.shadow_decision_correct,
                "estimated_cost": trial.estimated_cost,
            }
            for role, usage in aggregate_usage_by_role(trial.invocations).items():
                row.update(
                    {
                        f"{role}_invocation_count": usage["invocation_count"],
                        f"{role}_usage_unavailable_invocations": usage[
                            "unavailable_invocation_count"
                        ],
                        f"{role}_input_tokens": usage["input_tokens"],
                        f"{role}_cached_input_tokens": usage["cached_input_tokens"],
                        f"{role}_output_tokens": usage["output_tokens"],
                        f"{role}_reasoning_tokens": usage["reasoning_tokens"],
                    }
                )
            writer.writerow(row)


def _print_json(value: object) -> None:
    json.dump(value, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
