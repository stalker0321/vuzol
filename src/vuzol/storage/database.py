"""Scoped PostgreSQL engine and session composition."""

from collections.abc import Mapping

from pydantic import SecretStr
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from vuzol.config import ScopedSecretResolver, Settings


def resolve_database_dsn(
    settings: Settings, *, environment: Mapping[str, str] | None = None
) -> SecretStr:
    """Resolve only the database secret for the storage composition scope."""

    reference = settings.database_dsn_reference
    if reference is None:
        raise ValueError("database_dsn_reference is required for PostgreSQL storage")
    resolver = ScopedSecretResolver(
        access_policy={reference: frozenset({"system:database"})},
        secret_file_root=settings.secret_file_root,
        environment=environment,
    )
    return resolver.get(reference, "system:database")


def create_engine(settings: Settings, dsn: SecretStr) -> AsyncEngine:
    """Create a bounded async engine without logging the plaintext DSN."""

    database = settings.database
    url = make_url(dsn.get_secret_value())
    if url.drivername != "postgresql+psycopg":
        raise ValueError("database DSN must use postgresql+psycopg")
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=database.pool_size,
        max_overflow=database.max_overflow,
        pool_timeout=database.pool_timeout_seconds,
        connect_args={
            "options": (
                f"-c statement_timeout={database.statement_timeout_ms} "
                f"-c lock_timeout={database.lock_timeout_ms}"
            )
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
