"""D6 Q2 kill switch: dispatch freeze defers new dispatches (PG)."""

from __future__ import annotations

from datetime import UTC, datetime

from ._test_runtime_helpers import (
    RegistryDocument,
    Run,
    RuntimeConfiguration,
    Settings,
    TransactionalOutbox,
    WorkflowDispatcher,
    asyncio,
    build_bundle,
    pytest,
    seed_interpreted,
    select,
    storage,
)


def _frozen_runtime(settings: Settings) -> RuntimeConfiguration:
    return RuntimeConfiguration(
        settings=settings.model_copy(
            update={"workflow": settings.workflow.model_copy(update={"dispatch_freeze": True})}
        ),
        registries=build_bundle(RegistryDocument(), settings),
    )


@pytest.mark.postgresql
def test_dispatch_freeze_defers_without_materializing(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        async with factory.begin() as session:
            session.add(
                TransactionalOutbox(
                    destination="workflow_dispatch",
                    operation_type="dispatch_interpretation",
                    linked_entity_type="interpretation",
                    linked_entity_id=interpretation_id,
                    idempotency_key=f"workflow:dispatch:{interpretation_id}",
                    payload={"task_id": str(task_id)},
                )
            )
        settings = Settings(environment="test")
        assert settings.workflow.dispatch_freeze is False
        dispatcher = WorkflowDispatcher(_frozen_runtime(settings), factory, owner="dispatcher")
        assert await dispatcher.process_one() is True
        async with factory() as session:
            assert tuple((await session.scalars(select(Run))).all()) == ()
            (item,) = tuple(
                (
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "workflow_dispatch"
                        )
                    )
                ).all()
            )
            assert item.status.value == "pending"
            assert item.last_error_category == "dispatch_frozen"
        # Unfrozen, the same item dispatches normally: freeze downgrades nothing.
        live = WorkflowDispatcher(
            RuntimeConfiguration(
                settings=settings, registries=build_bundle(RegistryDocument(), settings)
            ),
            factory,
            owner="dispatcher",
        )
        async with factory.begin() as session:
            target = await session.get(TransactionalOutbox, item.id)
            assert target is not None
            target.available_at = datetime.now(UTC)
            target.last_error_category = None
        assert await live.process_one() is True
        async with factory() as session:
            runs = tuple((await session.scalars(select(Run))).all())
            assert len(runs) == 1
        await engine.dispose()

    asyncio.run(scenario())
