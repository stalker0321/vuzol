"""Async Alembic environment with one PostgreSQL advisory migration lock."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import async_engine_from_config

from vuzol.config import get_settings
from vuzol.storage import Base, resolve_database_dsn

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    raise RuntimeError("offline migrations are disabled; PostgreSQL validation is required")


def run_sync_migrations(connection: Connection) -> None:
    settings = get_settings()
    lock_key = settings.database.migration_advisory_lock_key
    connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": lock_key})
    connection.commit()
    try:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            transactional_ddl=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    finally:
        if connection.in_transaction():
            connection.rollback()
        connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
        connection.commit()


async def run_async_migrations() -> None:
    settings = get_settings()
    dsn = resolve_database_dsn(settings)
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = dsn.get_secret_value()
    connectable = async_engine_from_config(
        section,
        prefix="sqlalchemy.",
        pool_pre_ping=True,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(run_sync_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
