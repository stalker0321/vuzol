"""Routing handler lifecycle tests (split for cohesion)."""

from __future__ import annotations

from ._test_routing_helpers import (
    AdapterRegistry,
    AsyncMock,
    BudgetReservationStatus,
    Decimal,
    FailingAdapter,
    FakeAdapter,
    LeaseLost,
    LeaseToken,
    LocalGit,
    MagicMock,
    Path,
    ProfileHealthObservation,
    ProjectConfig,
    ProviderBudgetReservation,
    ProviderErrorCategory,
    ProviderFailure,
    ProviderProfile,
    ProviderStepHandler,
    RoutedWorkflowWorker,
    Run,
    RunStatus,
    ScopedSecretResolver,
    Step,
    StepExecutionRequest,
    StepStatus,
    SupervisedProcess,
    Task,
    TaskStatus,
    UsageRecord,
    Worktree,
    WorktreeDeliveryState,
    WorktreeService,
    asyncio,
    bundle,
    claim_routed_step,
    effective_health,
    executor_provider_handlers,
    func,
    profile,
    provider_handlers,
    pytest,
    record_failure_observation,
    record_success_observation,
    seed_provider_step,
    select,
    start_step,
    storage,
    subprocess,
    synchronize_profiles,
)


@pytest.mark.postgresql
def test_fake_provider_completes_model_only_workflow_step(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        resolver = ScopedSecretResolver(
            access_policy={}, secret_file_root=tmp_path / "secrets", environment={}
        )
        adapters = AdapterRegistry(
            registries.profiles,
            resolver,
            adapters={"api": FakeAdapter()},
        )
        handler = ProviderStepHandler(factory, registries, adapters)
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="provider-worker",
            handlers=provider_handlers(handler),
        )

        assert await worker.process_one()

        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            usage = await session.scalar(select(UsageRecord).where(UsageRecord.step_id == step_id))
            assert task is not None and task.status is TaskStatus.COMPLETED
            assert run is not None and run.status is RunStatus.COMPLETED
            assert step is not None and step.status is StepStatus.COMPLETED
            assert step.result is not None and step.result["text"] == "safe answer"
            assert usage is not None and usage.cost_units == Decimal("0.005000")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("request_sent", "retryable", "expected_status", "expected_usage"),
    [
        (False, False, StepStatus.FAILED, 0),
        (True, True, StepStatus.QUEUED, 1),
    ],
)
def test_provider_failure_release_or_conservative_charge(
    postgres_dsn: str,
    tmp_path: Path,
    request_sent: bool,
    retryable: bool,
    expected_status: StepStatus,
    expected_usage: int,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        _task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        failure = ProviderFailure(
            ProviderErrorCategory.TIMEOUT if retryable else ProviderErrorCategory.PERMANENT_REQUEST,
            retryable=retryable,
            request_sent=request_sent,
            safe_summary="safe failure",
        )
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"api": FailingAdapter(failure)},
        )
        handler = ProviderStepHandler(factory, registries, adapters)
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="provider-worker",
            handlers=provider_handlers(handler),
        )

        assert await worker.process_one()

        async with factory() as session:
            step = await session.get(Step, step_id)
            usage_rows = tuple(
                (
                    await session.scalars(select(UsageRecord).where(UsageRecord.step_id == step_id))
                ).all()
            )
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert step is not None and step.status is expected_status
            assert len(usage_rows) == expected_usage
            assert reservation is not None
            expected_reservation = (
                BudgetReservationStatus.CONSERVATIVE
                if request_sent
                else BudgetReservationStatus.RELEASED
            )
            assert reservation.status is expected_reservation
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_pre_provider_failure_unwinds_reservation_and_worktree(
    postgres_dsn: str, tmp_path: Path
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(("git", "init", "-b", "main"), cwd=repository, check=True)
    subprocess.run(
        ("git", "config", "user.email", "test@example.invalid"),
        cwd=repository,
        check=True,
    )
    subprocess.run(("git", "config", "user.name", "Test"), cwd=repository, check=True)
    (repository / "tracked.txt").write_text("base\n")
    subprocess.run(("git", "add", "tracked.txt"), cwd=repository, check=True)
    subprocess.run(("git", "commit", "-m", "base"), cwd=repository, check=True)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile(
            "cli",
            launch_mode="cli",
            sandbox_required=True,
            runtime_identity="test-cli",
            state_directory=tmp_path / "provider-state",
        )
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory, step_type="execute_code")
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=ProjectConfig(
                    id="project",
                    display_name="Project",
                    repository_path=repository,
                    default_branch="main",
                    allowed_capabilities=frozenset(),
                    sandbox_profile="default",
                ),
                owner="provider-worker",
            )

        adapter = FakeAdapter()
        adapter.execute = AsyncMock(side_effect=AssertionError("provider must not be called"))  # type: ignore[method-assign]
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"cli": adapter},
        )
        handler = ProviderStepHandler(factory, registries, adapters, worktrees=worktrees)
        handler._build_request = AsyncMock(  # type: ignore[method-assign]
            side_effect=ValueError("injected pre-provider preparation failure")
        )
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="provider-worker",
            handlers=executor_provider_handlers(handler),
        )

        assert await worker.process_one()

        async with factory() as session:
            step = await session.get(Step, step_id)
            run = await session.get(Run, run_id)
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
            process_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(SupervisedProcess)
                    .where(SupervisedProcess.run_id == run_id)
                )
                or 0
            )
            usage_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(UsageRecord)
                    .where(UsageRecord.run_id == run_id)
                )
                or 0
            )
            health_count = int(
                await session.scalar(select(func.count()).select_from(ProfileHealthObservation))
                or 0
            )
            assert step is not None and step.status is StepStatus.FAILED
            assert step.failure_category == "provider_request_invalid"
            assert step.failure_summary == "ValueError"
            assert run is not None and run.failure_category == "provider_request_invalid"
            assert reservation is not None
            assert reservation.status is BudgetReservationStatus.RELEASED
            assert worktree is not None
            assert worktree.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
            assert process_count == usage_count == health_count == 0
        adapter.execute.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_pre_provider_unwind_is_idempotent_and_fenced(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="provider-worker",
                lease_seconds=60,
                candidate_limit=20,
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
        request = StepExecutionRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            step_type="research_execute",
            payload={"budget_reservation_id": "malformed", "provider_attempt": 1},
            timeout_seconds=60,
            lease=token,
        )
        handler = ProviderStepHandler(factory, registries, MagicMock())

        await handler._unwind_pre_provider(request, reservation_id=None)
        await handler._unwind_pre_provider(request, reservation_id=None)

        mismatched = StepExecutionRequest(
            task_id=request.task_id,
            run_id=request.run_id,
            step_id=request.step_id,
            step_type=request.step_type,
            payload=request.payload,
            timeout_seconds=request.timeout_seconds,
            lease=LeaseToken(
                step=token.step,
                owner=token.owner,
                generation=token.generation + 1,
            ),
        )
        with pytest.raises(LeaseLost):
            await handler._unwind_pre_provider(mismatched, reservation_id=None)

        async with factory() as session:
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert reservation is not None
            assert reservation.status is BudgetReservationStatus.RELEASED
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_profile_health_is_revision_scoped_and_snapshot_sync_is_idempotent(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api", credential_reference="env:PRIVATE_KEY")
        _settings, registries = bundle(tmp_path, configured)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        async with factory() as session:
            snapshot = await session.scalar(
                select(ProviderProfile).where(ProviderProfile.stable_id == "api")
            )
            assert snapshot is not None
            assert "credential_reference" not in snapshot.metadata_json
            initial = await effective_health(session, configured, configuration_revision="a" * 64)
            assert initial.healthy and initial.quota_state.value == "unknown"

        failure = ProviderFailure(
            ProviderErrorCategory.AUTHENTICATION,
            retryable=False,
            request_sent=True,
            safe_summary="authentication failed",
        )
        async with factory.begin() as session:
            await record_failure_observation(
                session,
                configured,
                configuration_revision="a" * 64,
                failure=failure,
            )
        async with factory() as session:
            unhealthy = await effective_health(session, configured, configuration_revision="a" * 64)
            unrelated_revision = await effective_health(
                session, configured, configuration_revision="c" * 64
            )
            assert not unhealthy.healthy
            assert unrelated_revision.healthy

        async with factory.begin() as session:
            await record_success_observation(session, configured, configuration_revision="a" * 64)
        async with factory() as session:
            recovered = await effective_health(session, configured, configuration_revision="a" * 64)
            assert recovered.healthy
        await engine.dispose()

    asyncio.run(scenario())
