import asyncio
from pathlib import Path

import pytest
from pydantic import SecretStr
from sqlalchemy import inspect

from vuzol.config import Settings
from vuzol.storage import create_engine, resolve_database_dsn

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
