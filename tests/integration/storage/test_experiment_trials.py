import asyncio
import subprocess
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from vuzol.cli.experiment import _inspect
from vuzol.config import (
    Capability,
    CostClass,
    LaunchMode,
    ProjectConfig,
    ProviderProfileConfig,
    ProviderRole,
    RegistryDocument,
    SandboxProfileConfig,
    Settings,
    build_bundle,
)
from vuzol.experiments.domain import (
    BoundedLevel,
    ContextEntry,
    ContextManifest,
    ExecutionMode,
    ExperimentTelemetry,
    RepairSeverity,
    RequiredGate,
    ReviewOutcome,
    RiskLevel,
    TaskClass,
    TaskClassification,
)
from vuzol.experiments.service import TrialSeedRequest, seed_trial
from vuzol.experiments.telemetry import load_trials, record_trial
from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import RunStatus, StepStatus

from .helpers import storage


@pytest.mark.postgresql
def test_trial_seed_uses_existing_workflow_records_and_profile_pin(
    postgres_dsn: str, tmp_path: Path
) -> None:
    repository_root = tmp_path / "repositories"
    repository_root.mkdir()
    repository = repository_root / "vuzol"
    repository.mkdir()
    subprocess.run(("git", "init", "-b", "main", str(repository)), check=True, capture_output=True)
    subprocess.run(
        ("git", "-C", str(repository), "config", "user.email", "test@example.invalid"),
        check=True,
    )
    subprocess.run(("git", "-C", str(repository), "config", "user.name", "Test"), check=True)
    (repository / "README.md").write_text("base\n")
    subprocess.run(("git", "-C", str(repository), "add", "."), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "commit", "-m", "base"),
        check=True,
        capture_output=True,
    )
    base_commit = subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        state_root = tmp_path / "states"
        worktree_root.mkdir()
        artifact_root.mkdir()
        settings = Settings(
            repository_root=repository_root,
            worktree_root=worktree_root,
            artifact_root=artifact_root,
        )
        profile = ProviderProfileConfig(
            id="grok-subscription-a",
            provider="grok",
            model="grok-build",
            launch_mode=LaunchMode.CLI,
            credential_required=False,
            capabilities=frozenset(
                {
                    Capability.REPOSITORY_READ,
                    Capability.CODE_EDIT,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            concurrency_limit=1,
            cost_class=CostClass.STRONG,
            roles=frozenset({ProviderRole.EXECUTOR}),
            supported_task_types=frozenset({"coding"}),
            runtime_identity="grok-a",
            state_directory=state_root / "a",
        )
        document = RegistryDocument(
            sandboxes=(
                SandboxProfileConfig(
                    id="sandbox",
                    image="sandbox@sha256:" + "a" * 64,
                ),
            ),
            projects=(
                ProjectConfig(
                    id="vuzol",
                    display_name="Vuzol",
                    repository_path=Path("vuzol"),
                    default_branch="main",
                    allowed_capabilities=frozenset(profile.capabilities),
                    sandbox_profile="sandbox",
                ),
            ),
            profiles=(profile,),
        )
        registries = build_bundle(document, settings, validate_profile_credentials=False)
        context = ContextManifest(
            role="worker",
            entries=(
                ContextEntry.from_content(
                    source_type="repository_file",
                    reference="src/a.py",
                    content=b"source",
                ),
            ),
        )
        request = TrialSeedRequest(
            experiment_id="step09a-integration",
            task_id="validator",
            worker_profile=profile.id,
            project_id="vuzol",
            base_commit=base_commit,
            goal="Add validator",
            classification=TaskClassification(
                task_class=TaskClass.PURE_MODEL_VALIDATOR,
                complexity=BoundedLevel.LOW,
                risk=RiskLevel.LOW,
                testability=BoundedLevel.HIGH,
                blast_radius=BoundedLevel.LOW,
                coupling=BoundedLevel.LOW,
                novelty=BoundedLevel.LOW,
                expected_file_count=2,
            ),
            allowed_paths=("src/a.py", "tests/test_a.py"),
            acceptance_criteria=("reject malformed input",),
            required_gates=(RequiredGate(name="focused", command_id="pytest-focused"),),
            context_manifest=context,
        )
        async with factory.begin() as session:
            seeded = await seed_trial(session, registries, request)
        async with factory() as session:
            task = await session.get(Task, seeded.task_uuid)
            run = await session.get(Run, seeded.run_uuid)
            steps = (
                await session.scalars(
                    select(Step).where(Step.run_id == seeded.run_uuid).order_by(Step.ordinal)
                )
            ).all()
            assert (
                task is not None
                and task.task_draft["step09a_capsule"]["base_commit"] == base_commit
            )
            assert run is not None and run.status is RunStatus.RUNNING
            assert run.selected_route["trusted_profile_id"] == profile.id
            assert [step.step_type for step in steps] == [
                "interpret",
                "prepare_worktree",
                "execute_code",
            ]
            assert [step.status for step in steps] == [
                StepStatus.COMPLETED,
                StepStatus.QUEUED,
                StepStatus.PENDING,
            ]
            assert "/home/" not in task.original_text
        inspection = await _inspect(factory, "step09a-integration")
        assert inspection["runs"][0]["task_id"] == "validator"
        assert inspection["runs"][0]["profile_id"] == profile.id
        assert inspection["runs"][0]["worktree"] is None
        assert inspection["runs"][0]["usage"] == []
        telemetry = ExperimentTelemetry(
            experiment_id="step09a-integration",
            task_id="validator",
            task_class=TaskClass.PURE_MODEL_VALIDATOR,
            predicted_mode=ExecutionMode.GROK_REVIEWED,
            actual_mode=ExecutionMode.GROK_REVIEWED,
            worker_profile=profile.id,
            base_commit=base_commit,
            allowed_paths=("src/a.py",),
            queue_wait_ms=1,
            execution_duration_ms=1,
            gate_duration_ms=1,
            review_duration_ms=1,
            repair_duration_ms=0,
            total_wall_time_ms=4,
            invocations=(),
            estimated_cost=Decimal("0.01"),
            worker_attempts=1,
            repair_count=0,
            repair_severity=RepairSeverity.NONE,
            final_outcome=ReviewOutcome.ACCEPTED_FIRST_PASS,
            human_intervention=False,
            shadow_would_accept=True,
            shadow_decision_correct=True,
            egress_unavailable_reason="not exposed",
            network_policy_id="test",
            image_identity="image@sha256:" + "a" * 64,
            worktree_identity="worktree:validator",
        )
        async with factory.begin() as session:
            event_id = await record_trial(session, telemetry)
            assert event_id is not None
        async with factory() as session:
            loaded = await load_trials(session, "step09a-integration")
            assert loaded == (telemetry,)
        await engine.dispose()

    asyncio.run(scenario())
