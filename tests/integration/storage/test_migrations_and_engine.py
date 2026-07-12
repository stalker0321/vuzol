import asyncio
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from pytest import MonkeyPatch
from sqlalchemy import inspect

from vuzol.config import Settings, get_settings
from vuzol.storage import create_engine, resolve_database_dsn
from vuzol.storage.types import IdempotencyClass, RunStatus, StepStatus
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage


@pytest.mark.postgresql
def test_initial_migration_contains_complete_foundation(postgres_dsn: str) -> None:
    async def inspect_schema() -> set[str]:
        engine, _ = storage(postgres_dsn)
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
        await engine.dispose()
        return tables

    tables = asyncio.run(inspect_schema())
    assert {
        "tasks",
        "runs",
        "steps",
        "events",
        "external_inbox",
        "transactional_outbox",
        "approvals",
        "artifacts",
        "usage_records",
        "provider_budget_reservations",
        "topic_mappings",
        "telegram_message_links",
        "worktrees",
        "supervised_processes",
    }.issubset(tables)


def test_engine_requires_psycopg_and_masks_password() -> None:
    settings = Settings(environment="test")
    password = "database-test-value"  # noqa: S105  # pragma: allowlist secret
    engine = create_engine(
        settings,
        SecretStr(f"postgresql+psycopg://user:{password}@localhost/database"),
    )
    assert password not in repr(engine.url)
    asyncio.run(engine.dispose())

    with pytest.raises(ValueError, match=r"must use postgresql\+psycopg"):
        create_engine(settings, SecretStr("sqlite+aiosqlite:///test.db"))


def test_database_secret_resolution_is_scope_limited(tmp_path: Path) -> None:
    settings = Settings(
        environment="test",
        database_dsn_reference="env:TEST_DATABASE_DSN",
        secret_file_root=tmp_path,
    )
    secret = resolve_database_dsn(settings, environment={"TEST_DATABASE_DSN": "dsn-value"})
    assert secret.get_secret_value() == "dsn-value"
    assert "dsn-value" not in repr(secret)


@pytest.mark.postgresql
def test_workflow_downgrade_maps_new_awaiting_states(
    postgres_dsn: str, monkeypatch: MonkeyPatch
) -> None:
    async def seed() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=1, chat_id=-100, original_text="waiting", task_type="general"
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="simple_model",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.AWAITING_USER,
            )
            await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="question",
                idempotency_class=IdempotencyClass.READ_ONLY,
                status=StepStatus.AWAITING_USER,
            )
        await engine.dispose()

    asyncio.run(seed())
    async_dsn = postgres_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    sync_dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", async_dsn)
    alembic = Config("alembic.ini")
    get_settings.cache_clear()
    try:
        command.downgrade(alembic, "cf3ae0c222db")  # pragma: allowlist secret
        with psycopg.connect(sync_dsn) as connection:
            run_status = connection.execute("SELECT status FROM runs").fetchone()
            step_status = connection.execute("SELECT status FROM steps").fetchone()
        assert run_status == ("blocked",)
        assert step_status == ("blocked",)
    finally:
        get_settings.cache_clear()
        command.upgrade(alembic, "head")
        get_settings.cache_clear()
