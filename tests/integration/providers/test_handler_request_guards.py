"""Provider step handler request guards and worker finalization paths."""

from __future__ import annotations

from tests.integration.providers._test_routing_helpers import (
    AdapterRegistry,
    AsyncMock,
    AsyncSession,
    BudgetReservationStatus,
    CancellationContext,
    Capability,
    ConfigurationBundle,
    Decimal,
    EffectiveProfileState,
    FailingAdapter,
    LeaseToken,
    LocalGit,
    MagicMock,
    NormalizedUsage,
    Path,
    ProfileHealthObservation,
    ProjectConfig,
    ProviderBudgetReservation,
    ProviderErrorCategory,
    ProviderFailure,
    ProviderProfileConfig,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
    ProviderStepHandler,
    RegistryDocument,
    RoutedWorkflowWorker,
    ScopedSecretResolver,
    Settings,
    Step,
    StepExecutionRequest,
    StepStatus,
    Task,
    TaskStatus,
    UsageRecord,
    Worktree,
    WorktreeDeliveryState,
    WorktreeService,
    async_sessionmaker,
    asyncio,
    build_bundle,
    bundle,
    func,
    profile,
    provider_handlers,
    pytest,
    seed_provider_step,
    select,
    storage,
    subprocess,
    synchronize_profiles,
    uuid,
)
from vuzol.config import SandboxProfileConfig
from vuzol.execution.access import WorktreeAccessError
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.result_schema import result_schema_for_step
from vuzol.storage.records import StepRecord
from vuzol.workflows.domain import OutcomeKind


def _sandbox(sandbox_id: str, *, enabled: bool = True) -> SandboxProfileConfig:
    return SandboxProfileConfig.model_validate(
        {"id": sandbox_id, "image": f"vuzol-sandbox@sha256:{'0' * 64}", "enabled": enabled}
    )


def _project(repository: Path, **changes: object) -> ProjectConfig:
    values: dict[str, object] = {
        "id": "proj",
        "display_name": "Proj",
        "repository_path": repository,
        "default_branch": "main",
        "allowed_capabilities": frozenset({Capability.REPOSITORY_READ}),
        "sandbox_profile": "dev",
        "validation_sandbox_profile": "vld",
        "enabled": False,
    }
    values.update(changes)
    return ProjectConfig.model_validate(values)


def _registries_with(
    tmp_path: Path,
    *projects: ProjectConfig,
    sandboxes: tuple[SandboxProfileConfig, ...] = (),
) -> tuple[Settings, ConfigurationBundle]:
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    document = RegistryDocument(
        profiles=(profile("api"),),
        projects=projects,
        sandboxes=sandboxes,
    )
    return settings, build_bundle(
        document, settings, environment={}, validate_profile_credentials=False
    )


def _git_repository(tmp_path: Path) -> Path:
    repositories = tmp_path / "repositories"
    repository = repositories / "repository"
    repository.mkdir(parents=True)
    subprocess.run(("git", "init", "-b", "main"), cwd=repository, check=True)
    subprocess.run(
        ("git", "config", "user.email", "test@example.invalid"), cwd=repository, check=True
    )
    subprocess.run(("git", "config", "user.name", "Test"), cwd=repository, check=True)
    (repository / "tracked.txt").write_text("base\n")
    subprocess.run(("git", "add", "tracked.txt"), cwd=repository, check=True)
    subprocess.run(("git", "commit", "-m", "base"), cwd=repository, check=True)
    return repository


def _probe_token(step_id: uuid.UUID, run_id: uuid.UUID) -> LeaseToken:
    record = StepRecord(
        id=step_id,
        run_id=run_id,
        status=StepStatus.RUNNING,
        lease_generation=1,
        lease_owner="probe",
        lease_expires_at=None,
    )
    return LeaseToken(step=record, owner="probe", generation=1)


async def _running_step(
    factory: async_sessionmaker[AsyncSession],
    *,
    step_type: str = "execute_model",
    executor_profile_id: str | None = "api",
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, LeaseToken]:
    task_id, run_id, step_id = await seed_provider_step(factory, step_type=step_type)
    async with factory.begin() as session:
        task = await session.get(Task, task_id)
        step = await session.get(Step, step_id)
        assert task is not None and step is not None
        task.project_id = "proj"
        step.status = StepStatus.RUNNING
        step.lease_owner = "probe"
        step.lease_generation = 1
        step.executor_profile_id = executor_profile_id
    return task_id, run_id, step_id, _probe_token(step_id, run_id)


async def _set_step_payload(
    factory: async_sessionmaker[AsyncSession], step_id: uuid.UUID, payload: dict[str, object]
) -> None:
    async with factory.begin() as session:
        step = await session.get(Step, step_id)
        assert step is not None
        step.payload = dict(payload)


async def _insert_reservation(
    factory: async_sessionmaker[AsyncSession],
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    attempt: int = 1,
) -> uuid.UUID:
    reservation = ProviderBudgetReservation(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        profile_id="api",
        provider_attempt=attempt,
        reserved_input_tokens=1000,
        reserved_output_tokens=1000,
        reserved_cost_units=Decimal("1"),
        reserved_quota_units=Decimal("1"),
    )
    async with factory.begin() as session:
        session.add(reservation)
        await session.flush()
        return reservation.id


def _request(
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    token: LeaseToken,
    *,
    step_type: str = "execute_model",
    payload: dict[str, object] | None = None,
) -> StepExecutionRequest:
    return StepExecutionRequest(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        step_type=step_type,
        payload=payload if payload is not None else {},
        timeout_seconds=60,
        lease=token,
    )


def _never_called_adapter() -> tuple[FailingAdapter, AsyncMock]:
    adapter = FailingAdapter(
        ProviderFailure(
            ProviderErrorCategory.UNKNOWN,
            retryable=False,
            request_sent=False,
            safe_summary="provider must not be called",
        )
    )
    execute_mock = AsyncMock(side_effect=AssertionError("provider must not be called"))
    adapter.execute = execute_mock  # type: ignore[method-assign]
    return adapter, execute_mock


def _adapters_for(
    registries: ConfigurationBundle,
    tmp_path: Path,
    profile_id: str,
    adapter: ProviderAdapter,
) -> AdapterRegistry:
    return AdapterRegistry(
        registries.profiles,
        ScopedSecretResolver(
            access_policy={}, secret_file_root=tmp_path / "secrets", environment={}
        ),
        adapters={profile_id: adapter},
    )


class _StaticAdapter:
    """Returns one fixed provider result and records every request."""

    def __init__(self, result: ProviderResult) -> None:
        self._result = result
        self.requests: list[ProviderRequest] = []

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        del profile, cancellation
        self.requests.append(request)
        return self._result

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _static_result(
    *,
    text: str | None = None,
    structured_output: dict[str, object] | None = None,
) -> ProviderResult:
    return ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        text=text,
        structured_output=structured_output,
        usage=NormalizedUsage(
            input_tokens=0,
            output_tokens=0,
            cost_units=Decimal("0"),
            quota_units=Decimal("0"),
            duration_ms=0,
        ),
        finish_reason="stop",
        adapter_version="static.v1",
    )


@pytest.mark.postgresql
@pytest.mark.parametrize(
    "case",
    [
        "state_incomplete",
        "identity_inconsistent",
        "lease_invalid",
        "reference_missing",
        "reservation_missing",
        "reasoning_invalid",
    ],
)
def test_build_request_guards_fail_closed(postgres_dsn: str, tmp_path: Path, case: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        payload: dict[str, object] = {}
        if case == "identity_inconsistent":
            task_id, _run_a, step_id = await seed_provider_step(factory)
            _task_b, request_run_id, _step_b = await seed_provider_step(factory)
        else:
            task_id, request_run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            step = await session.get(Step, step_id)
            assert task is not None and step is not None
            task.project_id = "proj"
            step.status = StepStatus.RUNNING
            step.lease_owner = "probe"
            step.lease_generation = 1
            step.executor_profile_id = None if case == "state_incomplete" else "api"
        token = _probe_token(step_id, request_run_id)
        if case == "lease_invalid":
            async with factory.begin() as session:
                step = await session.get(Step, step_id)
                assert step is not None
                step.status = StepStatus.QUEUED
        elif case == "reservation_missing":
            step_payload = {"budget_reservation_id": str(uuid.uuid4()), "provider_attempt": 1}
            await _set_step_payload(factory, step_id, step_payload)
            payload = dict(step_payload)
        elif case == "reasoning_invalid":
            reservation_id = await _insert_reservation(
                factory, task_id=task_id, run_id=request_run_id, step_id=step_id
            )
            step_payload = {
                "budget_reservation_id": str(reservation_id),
                "provider_attempt": 1,
                "reasoning_max_tokens": "many",
            }
            await _set_step_payload(factory, step_id, step_payload)
            payload = dict(step_payload)
        adapter, execute_mock = _never_called_adapter()
        handler = ProviderStepHandler(
            factory, registries, _adapters_for(registries, tmp_path, "api", adapter)
        )

        outcome = await handler.execute(
            _request(task_id, request_run_id, step_id, token, payload=dict(payload)),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        if case in {"identity_inconsistent", "lease_invalid"}:
            # The unwind refuses state it cannot prove ownership of and fails closed.
            assert outcome.category == "pre_provider_unwind_failed"
            assert outcome.summary is not None
            assert outcome.summary.startswith("provider_request_invalid followed by unwind failure")
        else:
            assert outcome.category == "provider_request_invalid"
        execute_mock.assert_not_awaited()
        if case == "reasoning_invalid":
            async with factory() as session:
                reservation = await session.scalar(
                    select(ProviderBudgetReservation).where(
                        ProviderBudgetReservation.step_id == step_id
                    )
                )
                assert reservation is not None
                assert reservation.status is BudgetReservationStatus.RELEASED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_execute_code_requires_prepared_worktree(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        reservation_id = await _insert_reservation(
            factory, task_id=task_id, run_id=run_id, step_id=step_id
        )
        payload = {"budget_reservation_id": str(reservation_id), "provider_attempt": 1}
        await _set_step_payload(factory, step_id, payload)
        adapter, execute_mock = _never_called_adapter()
        handler = ProviderStepHandler(
            factory, registries, _adapters_for(registries, tmp_path, "api", adapter)
        )

        outcome = await handler.execute(
            _request(
                task_id, run_id, step_id, token, step_type="execute_code", payload=dict(payload)
            ),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "provider_request_invalid"
        execute_mock.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_reservation_unwind_falls_back_to_attempt_lookup(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id, token = await _running_step(factory)
        await _insert_reservation(factory, task_id=task_id, run_id=run_id, step_id=step_id)
        adapter, _execute_mock = _never_called_adapter()
        handler = ProviderStepHandler(
            factory, registries, _adapters_for(registries, tmp_path, "api", adapter)
        )

        outcome = await handler.execute(
            _request(task_id, run_id, step_id, token, payload={"provider_attempt": 1}),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "provider_request_invalid"
        async with factory() as session:
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert reservation is not None
            assert reservation.status is BudgetReservationStatus.RELEASED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_unexpected_pre_provider_failure_is_reported(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id, token = await _running_step(factory)
        adapter, execute_mock = _never_called_adapter()
        handler = ProviderStepHandler(
            factory, registries, _adapters_for(registries, tmp_path, "api", adapter)
        )
        handler._build_request = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("boom")
        )

        outcome = await handler.execute(
            _request(task_id, run_id, step_id, token),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "pre_provider_unexpected"
        assert outcome.summary == "RuntimeError"
        execute_mock.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_authentication_failure_releases_reservation_without_usage(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        failure = ProviderFailure(
            ProviderErrorCategory.AUTHENTICATION,
            retryable=False,
            request_sent=True,
            safe_summary="authentication failed",
        )
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(
                access_policy={}, secret_file_root=tmp_path / "secrets", environment={}
            ),
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
            task = await session.get(Task, task_id)
            usage_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(UsageRecord)
                    .where(UsageRecord.step_id == step_id)
                )
                or 0
            )
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            health_count = int(
                await session.scalar(select(func.count()).select_from(ProfileHealthObservation))
                or 0
            )
            assert step is not None and step.status is StepStatus.FAILED
            assert step.failure_category == "authentication"
            assert task is not None and task.status is TaskStatus.FAILED
            assert usage_count == 0
            assert reservation is not None
            assert reservation.status is BudgetReservationStatus.RELEASED
            assert health_count == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_retain_active_worktree_marks_worktree_retained(postgres_dsn: str, tmp_path: Path) -> None:
    repository = _git_repository(tmp_path)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
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
        handler = ProviderStepHandler(factory, registries, MagicMock(), worktrees=worktrees)

        await handler._retain_active_worktree(
            _request(task_id, run_id, step_id, token, step_type="execute_code")
        )

        async with factory() as session:
            worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
            assert worktree is not None
            assert worktree.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("case", "expected_error"),
    [
        ("no_worktree", LookupError),
        ("validation_missing", WorktreeAccessError),
        ("validation_disabled", WorktreeAccessError),
    ],
)
def test_grant_worktree_access_fails_closed(
    postgres_dsn: str,
    tmp_path: Path,
    case: str,
    expected_error: type[Exception],
) -> None:
    repository = _git_repository(tmp_path)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        validation_profile: str | None = "vld"
        dev, vld = _sandbox("dev"), _sandbox("vld")
        if case == "validation_missing":
            validation_profile = None
        elif case == "validation_disabled":
            vld = _sandbox("vld", enabled=False)
        project = _project(repository, validation_sandbox_profile=validation_profile)
        _settings, registries = _registries_with(tmp_path, project, sandboxes=(dev, vld))
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        if case != "no_worktree":
            worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
            async with factory.begin() as session:
                await worktrees.prepare(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    project=project,
                    owner="provider-worker",
                )
        grant = AsyncMock(side_effect=AssertionError("grant must not be called"))
        handler = ProviderStepHandler(
            factory, registries, MagicMock(), worktree_access=MagicMock(grant=grant)
        )

        with pytest.raises(expected_error):
            await handler._grant_worktree_access(
                _request(task_id, run_id, step_id, token, step_type="execute_code")
            )

        grant.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_grant_worktree_access_grants_with_sandbox_ids(postgres_dsn: str, tmp_path: Path) -> None:
    repository = _git_repository(tmp_path)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        project = _project(repository)
        _settings, registries = _registries_with(
            tmp_path, project, sandboxes=(_sandbox("dev"), _sandbox("vld"))
        )
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            worktree = await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=project,
                owner="provider-worker",
            )
        access = MagicMock(revoke=AsyncMock())
        grant = AsyncMock(return_value=access)
        handler = ProviderStepHandler(
            factory, registries, MagicMock(), worktree_access=MagicMock(grant=grant)
        )

        lease = await handler._grant_worktree_access(
            _request(task_id, run_id, step_id, token, step_type="execute_code")
        )

        assert lease is access
        assert grant.await_count == 1
        assert grant.await_args is not None
        assert grant.await_args.args == (Path(worktree.path),)
        assert grant.await_args.kwargs == {"sandbox_uid": 10001, "sandbox_gid": 10001}
        access.revoke.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_worktree_access_grant_failure_is_unexpected(postgres_dsn: str, tmp_path: Path) -> None:
    repository = _git_repository(tmp_path)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        project = _project(repository)
        _settings, registries = _registries_with(
            tmp_path, project, sandboxes=(_sandbox("dev"), _sandbox("vld"))
        )
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        reservation_id = await _insert_reservation(
            factory, task_id=task_id, run_id=run_id, step_id=step_id
        )
        await _set_step_payload(
            factory,
            step_id,
            {"budget_reservation_id": str(reservation_id), "provider_attempt": 1},
        )
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=project,
                owner="provider-worker",
            )
        adapter, execute_mock = _never_called_adapter()
        handler = ProviderStepHandler(
            factory,
            registries,
            _adapters_for(registries, tmp_path, "api", adapter),
            worktrees=worktrees,
            worktree_access=MagicMock(
                grant=AsyncMock(side_effect=RuntimeError("setfacl exploded"))
            ),
        )
        payload = {"budget_reservation_id": str(reservation_id), "provider_attempt": 1}

        outcome = await handler.execute(
            _request(
                task_id, run_id, step_id, token, step_type="execute_code", payload=dict(payload)
            ),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "pre_provider_unexpected"
        execute_mock.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_worktree_access_manager_unavailable_fails_before_provider(
    postgres_dsn: str, tmp_path: Path
) -> None:
    repository = _git_repository(tmp_path)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        project = _project(repository)
        _settings, registries = _registries_with(
            tmp_path, project, sandboxes=(_sandbox("dev"), _sandbox("vld"))
        )
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        reservation_id = await _insert_reservation(
            factory, task_id=task_id, run_id=run_id, step_id=step_id
        )
        await _set_step_payload(
            factory,
            step_id,
            {"budget_reservation_id": str(reservation_id), "provider_attempt": 1},
        )
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=project,
                owner="provider-worker",
            )
        adapter, execute_mock = _never_called_adapter()
        handler = ProviderStepHandler(
            factory,
            registries,
            _adapters_for(registries, tmp_path, "api", adapter),
            worktrees=worktrees,
        )
        payload = {"budget_reservation_id": str(reservation_id), "provider_attempt": 1}

        outcome = await handler.execute(
            _request(
                task_id, run_id, step_id, token, step_type="execute_code", payload=dict(payload)
            ),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "worker_access_unavailable"
        assert outcome.summary == "worktree access manager is unavailable"
        execute_mock.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_execute_code_builds_system_contexts_and_succeeds(
    postgres_dsn: str, tmp_path: Path
) -> None:
    repository = _git_repository(tmp_path)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        project = _project(
            repository,
            static_deployment={"url_path": "site", "source_directory": "public"},
        )
        _settings, registries = _registries_with(
            tmp_path, project, sandboxes=(_sandbox("dev"), _sandbox("vld"))
        )
        task_id, run_id, step_id, token = await _running_step(factory, step_type="execute_code")
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        reservation_id = await _insert_reservation(
            factory, task_id=task_id, run_id=run_id, step_id=step_id
        )
        await _set_step_payload(
            factory,
            step_id,
            {
                "budget_reservation_id": str(reservation_id),
                "provider_attempt": 1,
                "repair_context": {
                    "source": "validation",
                    "category": "tests_failed",
                    "summary": "2 failing",
                },
            },
        )
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=project,
                owner="provider-worker",
            )
        adapter = _StaticAdapter(_static_result(structured_output={"ok": True}))
        handler = ProviderStepHandler(
            factory,
            registries,
            _adapters_for(registries, tmp_path, "api", adapter),
            worktrees=worktrees,
            worktree_access=MagicMock(grant=AsyncMock()),
        )
        access = MagicMock(revoke=AsyncMock())
        handler._grant_worktree_access = AsyncMock(return_value=access)  # type: ignore[method-assign]
        payload = {"budget_reservation_id": str(reservation_id), "provider_attempt": 1}

        outcome = await handler.execute(
            _request(
                task_id, run_id, step_id, token, step_type="execute_code", payload=dict(payload)
            ),
            CancellationContext(),
        )

        assert outcome.kind is OutcomeKind.SUCCEEDED
        assert outcome.result["structured_output"] == {"ok": True}
        assert "implementation_summary" not in outcome.result
        built = adapter.requests[0]
        sources = [item.source for item in built.context]
        assert "system_skill" in sources
        assert "system_repair" in sources
        assert built.sandbox_reference is not None
        assert built.sandbox_reference.startswith("worktree:")
        access.revoke.assert_awaited_once()
        async with factory() as session:
            worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
            assert worktree is not None
            assert worktree.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
        await engine.dispose()

    asyncio.run(scenario())


def test_result_schema_discussion_branch() -> None:
    name, version, schema = result_schema_for_step(
        "execute_agent", {"discussion_agent_contract": {"channel": "x"}}
    )
    assert name == "DiscussionTextReply"
    assert version == "discussion-text-reply.v2"
    assert schema is None

    plain = result_schema_for_step("execute_model", {})
    assert plain == (None, None, None)
