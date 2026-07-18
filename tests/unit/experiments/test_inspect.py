"""Inspect tests (split for cohesion)."""

from __future__ import annotations

from ._test_experiments_helpers import *


def test_inspect_serializers_expose_only_safe_process_and_artifact_fields() -> None:
    step_id = uuid.UUID(int=1)
    process_id = uuid.UUID(int=2)
    events_id = uuid.UUID(int=3)
    artifact_id = uuid.UUID(int=4)
    verified_at = datetime(2026, 7, 14, 12, 30, tzinfo=UTC)
    process = _mock_row(
        id=process_id,
        step_id=step_id,
        profile_id="grok-subscription-a",
        provider_attempt=2,
        status=ProcessStatus.EXITED,
        outcome=ProcessOutcome.SUCCEEDED,
        image_digest="image@sha256:" + "a" * 64,
        exit_code=0,
        runtime_metadata={"actual_elapsed_ms": 1250, "private": "do-not-expose"},
        provider_events_artifact_id=events_id,
        provider_result_artifact_id=None,
        command_envelope={"argv": ["secret"]},
        command_envelope_hash="b" * 64,
        container_id="private-container",
        host_pid=999,
        working_directory="/private/worktree",
    )
    artifact = _mock_row(
        id=artifact_id,
        step_id=step_id,
        producer_process_id=process_id,
        artifact_type="provider-event-summary",
        size_bytes=42,
        content_hash="c" * 64,
        media_type="application/json",
        storage_state=ArtifactStorageState.AVAILABLE,
        verified_at=verified_at,
        content_uri="artifact:private",
        storage_key="private-key",
        metadata_json={"private": True},
        retention_until=verified_at,
    )

    rendered_process = _serialize_process(process)
    rendered_artifact = _serialize_artifact(artifact)

    assert rendered_process == {
        "process_uuid": str(process_id),
        "step_uuid": str(step_id),
        "profile_id": "grok-subscription-a",
        "provider_attempt": 2,
        "status": "exited",
        "outcome": "succeeded",
        "image_digest": "image@sha256:" + "a" * 64,
        "exit_code": 0,
        "duration_ms": 1250,
        "provider_events_artifact_id": str(events_id),
        "provider_result_artifact_id": None,
    }
    assert rendered_artifact == {
        "artifact_uuid": str(artifact_id),
        "step_uuid": str(step_id),
        "producer_process_uuid": str(process_id),
        "type": "provider-event-summary",
        "size_bytes": 42,
        "content_hash": "c" * 64,
        "media_type": "application/json",
        "storage_state": "available",
        "verified_at": "2026-07-14T12:30:00+00:00",
    }


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        ({"actual_elapsed_ms": 0}, 0),
        ({"actual_elapsed_ms": 987}, 987),
        ({"actual_elapsed_ms": -1}, None),
        ({"actual_elapsed_ms": 1.5}, None),
        ({"actual_elapsed_ms": True}, None),
        ({"actual_elapsed_ms": "12"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_process_duration_requires_safe_non_negative_integer(
    metadata: object, expected: int | None
) -> None:
    process = _mock_row(
        id=uuid.UUID(int=1),
        step_id=uuid.UUID(int=2),
        profile_id="profile",
        provider_attempt=1,
        status=ProcessStatus.EXITED,
        outcome=None,
        image_digest="image@sha256:" + "a" * 64,
        exit_code=None,
        runtime_metadata=metadata,
        provider_events_artifact_id=None,
        provider_result_artifact_id=None,
    )

    assert _serialize_process(process)["duration_ms"] == expected


def test_inspect_latest_flag_is_optional() -> None:
    default = _parse_args(["inspect", "experiment"])
    latest = _parse_args(["inspect", "experiment", "--latest"])

    assert default.command == "inspect"
    assert default.experiment_id == "experiment"
    assert default.latest is False
    assert latest.latest is True


@pytest.mark.anyio
async def test_inspect_latest_selects_newest_with_deterministic_uuid_tie() -> None:
    experiment_id = "step09a-inspect-latest"
    created = datetime(2026, 7, 15, 12, tzinfo=UTC)
    older = _mock_row(
        id=uuid.UUID(int=40),
        task_id=uuid.UUID(int=140),
        workflow_type="adaptive_worker_trial",
        created_at=datetime(2026, 7, 15, 11, tzinfo=UTC),
        status=RunStatus.COMPLETED,
        selected_route={"experiment_id": experiment_id, "experiment_task_id": "older"},
    )
    tied_lower = _mock_row(
        id=uuid.UUID(int=41),
        task_id=uuid.UUID(int=141),
        workflow_type="adaptive_worker_trial",
        created_at=created,
        status=RunStatus.COMPLETED,
        selected_route={"experiment_id": experiment_id, "experiment_task_id": "lower"},
    )
    tied_higher = _mock_row(
        id=uuid.UUID(int=42),
        task_id=uuid.UUID(int=142),
        workflow_type="adaptive_worker_trial",
        created_at=created,
        status=RunStatus.FAILED,
        selected_route={"experiment_id": experiment_id, "experiment_task_id": "higher"},
    )
    foreign = _mock_row(
        id=uuid.UUID(int=99),
        task_id=uuid.UUID(int=199),
        workflow_type="adaptive_worker_trial",
        created_at=datetime(2026, 7, 15, 13, tzinfo=UTC),
        status=RunStatus.COMPLETED,
        selected_route={"experiment_id": "another-experiment"},
    )
    runs: list[object] = [older, tied_lower, tied_higher, foreign]

    def factory_for(selected_runs: list[object]) -> MagicMock:
        session = AsyncMock(spec=AsyncSession)
        session.scalars.side_effect = [
            _scalar_result(selected_runs),
            *(_scalar_result([]) for _ in range(4 * len(selected_runs))),
        ]
        session.scalar.return_value = None
        context = AsyncMock()
        context.__aenter__.return_value = session
        context.__aexit__.return_value = False
        factory = MagicMock(spec=async_sessionmaker)
        factory.return_value = context
        return factory

    default = await _inspect(factory_for(runs), experiment_id)
    latest = await _inspect(factory_for(runs), experiment_id, latest=True)
    missing = await _inspect(factory_for([]), "missing", latest=True)

    assert [run["task_id"] for run in default["runs"]] == ["older", "lower", "higher"]
    assert [run["task_id"] for run in latest["runs"]] == ["higher"]
    assert latest["runs"][0]["run_uuid"] == str(tied_higher.id)
    assert missing == {"experiment_id": "missing", "runs": []}


@pytest.mark.anyio
async def test_inspect_preserves_fields_orders_evidence_and_excludes_foreign_rows() -> None:
    experiment_id = "step09a-inspect-safe"
    run_id = uuid.UUID(int=10)
    foreign_run_id = uuid.UUID(int=11)
    task_id = uuid.UUID(int=12)
    step_first_id = uuid.UUID(int=13)
    step_second_id = uuid.UUID(int=14)
    process_first_id = uuid.UUID(int=15)
    process_second_id = uuid.UUID(int=16)
    artifact_first_id = uuid.UUID(int=17)
    artifact_second_id = uuid.UUID(int=18)
    created = datetime(2026, 7, 14, 12, tzinfo=UTC)
    later = datetime(2026, 7, 14, 13, tzinfo=UTC)
    run = _mock_row(
        id=run_id,
        task_id=task_id,
        workflow_type="adaptive_worker_trial",
        created_at=created,
        status=RunStatus.COMPLETED,
        selected_route={
            "experiment_id": experiment_id,
            "experiment_task_id": "inspect-safe",
            "trusted_profile_id": "grok-subscription-a",
        },
    )
    foreign_run = _mock_row(
        id=foreign_run_id,
        task_id=uuid.UUID(int=19),
        workflow_type="adaptive_worker_trial",
        created_at=later,
        status=RunStatus.FAILED,
        selected_route={"experiment_id": "another-experiment"},
    )
    step_second = _mock_row(
        id=step_second_id,
        run_id=run_id,
        ordinal=2,
        step_type="execute_code",
        status=StepStatus.COMPLETED,
        attempt_count=1,
        failure_category=None,
    )
    step_first = _mock_row(
        id=step_first_id,
        run_id=run_id,
        ordinal=1,
        step_type="prepare_worktree",
        status=StepStatus.COMPLETED,
        attempt_count=1,
        failure_category=None,
    )
    foreign_step = _mock_row(
        id=uuid.UUID(int=20),
        run_id=foreign_run_id,
        ordinal=0,
        step_type="foreign",
        status=StepStatus.FAILED,
        attempt_count=1,
        failure_category="foreign",
    )
    patch_id = uuid.UUID(int=21)
    worktree = _mock_row(
        run_id=run_id,
        branch="vuzol/task-inspect",
        base_commit="a" * 40,
        result_commit="b" * 40,
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        diff_hash="c" * 64,
        patch_artifact_id=patch_id,
        changed_files_artifact_id=None,
    )
    usage = _mock_row(
        run_id=run_id,
        profile_id="grok-subscription-a",
        model="grok-build",
        input_tokens=10,
        cached_tokens=20,
        output_tokens=30,
        duration_ms=40,
        cost_units=Decimal("0.010000"),
    )
    foreign_usage = _mock_row(run_id=foreign_run_id)
    process_second = _mock_row(
        id=process_second_id,
        run_id=run_id,
        step_id=step_second_id,
        created_at=later,
        profile_id="grok-subscription-a",
        provider_attempt=2,
        status=ProcessStatus.EXITED,
        outcome=None,
        image_digest="image@sha256:" + "d" * 64,
        exit_code=None,
        runtime_metadata={},
        provider_events_artifact_id=None,
        provider_result_artifact_id=None,
    )
    process_first = _mock_row(
        id=process_first_id,
        run_id=run_id,
        step_id=step_second_id,
        created_at=created,
        profile_id="grok-subscription-a",
        provider_attempt=1,
        status=ProcessStatus.EXITED,
        outcome=ProcessOutcome.SUCCEEDED,
        image_digest="image@sha256:" + "d" * 64,
        exit_code=0,
        runtime_metadata={"actual_elapsed_ms": 500},
        provider_events_artifact_id=uuid.UUID(int=22),
        provider_result_artifact_id=uuid.UUID(int=23),
    )
    foreign_process = _mock_row(id=uuid.UUID(int=24), run_id=foreign_run_id, created_at=created)
    artifact_second = _mock_row(
        id=artifact_second_id,
        run_id=run_id,
        step_id=step_second_id,
        producer_process_id=process_first_id,
        created_at=later,
        artifact_type="worker_finalization_evidence",
        size_bytes=200,
        content_hash="e" * 64,
        media_type="application/json",
        storage_state=ArtifactStorageState.AVAILABLE,
        verified_at=None,
    )
    artifact_first = _mock_row(
        id=artifact_first_id,
        run_id=run_id,
        step_id=step_second_id,
        producer_process_id=process_first_id,
        created_at=created,
        artifact_type="provider-event-summary",
        size_bytes=100,
        content_hash="f" * 64,
        media_type="application/json",
        storage_state=ArtifactStorageState.AVAILABLE,
        verified_at=created,
    )
    foreign_artifact = _mock_row(id=uuid.UUID(int=25), run_id=foreign_run_id, created_at=created)
    session = AsyncMock(spec=AsyncSession)
    session.scalars.side_effect = [
        _scalar_result([run, foreign_run]),
        _scalar_result([step_second, foreign_step, step_first]),
        _scalar_result([foreign_usage, usage]),
        _scalar_result([process_second, foreign_process, process_first]),
        _scalar_result([artifact_second, foreign_artifact, artifact_first]),
    ]
    session.scalar.return_value = worktree
    context = AsyncMock()
    context.__aenter__.return_value = session
    context.__aexit__.return_value = False
    factory = MagicMock(spec=async_sessionmaker)
    factory.return_value = context

    output = await _inspect(factory, experiment_id)

    assert output["experiment_id"] == experiment_id
    assert len(output["runs"]) == 1
    rendered = output["runs"][0]
    assert set(rendered) == {
        "task_id",
        "task_uuid",
        "run_uuid",
        "status",
        "profile_id",
        "steps",
        "worktree",
        "usage",
        "processes",
        "artifacts",
    }
    assert rendered["task_id"] == "inspect-safe"
    assert rendered["task_uuid"] == str(task_id)
    assert rendered["run_uuid"] == str(run_id)
    assert rendered["status"] == "completed"
    assert rendered["profile_id"] == "grok-subscription-a"
    assert [item["step_uuid"] for item in rendered["steps"]] == [
        str(step_first_id),
        str(step_second_id),
    ]
    assert [item["ordinal"] for item in rendered["steps"]] == [1, 2]
    assert set(rendered["steps"][0]) == {
        "step_uuid",
        "ordinal",
        "type",
        "status",
        "attempt_count",
        "failure_category",
    }
    assert rendered["worktree"] == {
        "branch": "vuzol/task-inspect",
        "base_commit": "a" * 40,
        "result_commit": "b" * 40,
        "delivery_state": "worktree_retained",
        "diff_hash": "c" * 64,
        "patch_artifact_id": str(patch_id),
        "changed_files_artifact_id": None,
    }
    assert rendered["usage"] == [
        {
            "profile_id": "grok-subscription-a",
            "model": "grok-build",
            "input_tokens": 10,
            "cached_tokens": 20,
            "output_tokens": 30,
            "duration_ms": 40,
            "cost_units": "0.010000",
        }
    ]
    assert [item["process_uuid"] for item in rendered["processes"]] == [
        str(process_first_id),
        str(process_second_id),
    ]
    assert rendered["processes"][0]["duration_ms"] == 500
    assert rendered["processes"][1]["duration_ms"] is None
    assert [item["artifact_uuid"] for item in rendered["artifacts"]] == [
        str(artifact_first_id),
        str(artifact_second_id),
    ]
    assert rendered["artifacts"][0]["verified_at"] == "2026-07-14T12:00:00+00:00"
    assert rendered["artifacts"][1]["verified_at"] is None
    statements = [str(call.args[0]) for call in session.scalars.await_args_list]
    assert "supervised_processes.run_id =" in statements[3]
    assert "artifacts.run_id =" in statements[4]
    serialized = json.dumps(output, sort_keys=True)
    for forbidden in (
        "content_uri",
        "storage_key",
        "metadata_json",
        "command_envelope",
        "command_envelope_hash",
        "container_id",
        "host_pid",
        "working_directory",
        "runtime_metadata",
        "private-container",
        "/private/worktree",
    ):
        assert forbidden not in serialized


@pytest.mark.anyio
async def test_inspect_handles_missing_optional_evidence() -> None:
    experiment_id = "step09a-inspect-empty"
    run_id = uuid.UUID(int=30)
    run = _mock_row(
        id=run_id,
        task_id=uuid.UUID(int=31),
        workflow_type="adaptive_worker_trial",
        created_at=datetime(2026, 7, 14, tzinfo=UTC),
        status=RunStatus.FAILED,
        selected_route={
            "experiment_id": experiment_id,
            "experiment_task_id": "inspect-empty",
            "trusted_profile_id": "grok-subscription-a",
        },
    )
    session = AsyncMock(spec=AsyncSession)
    session.scalars.side_effect = [
        _scalar_result([run]),
        _scalar_result([]),
        _scalar_result([]),
        _scalar_result([]),
        _scalar_result([]),
    ]
    session.scalar.return_value = None
    context = AsyncMock()
    context.__aenter__.return_value = session
    context.__aexit__.return_value = False
    factory = MagicMock(spec=async_sessionmaker)
    factory.return_value = context

    output = await _inspect(factory, experiment_id)

    rendered = output["runs"][0]
    assert rendered["steps"] == []
    assert rendered["worktree"] is None
    assert rendered["usage"] == []
    assert rendered["processes"] == []
    assert rendered["artifacts"] == []
