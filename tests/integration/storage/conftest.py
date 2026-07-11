import os
from collections.abc import Iterator

import psycopg
import pytest

DEFAULT_TEST_DSN = (
    "postgresql://vuzol:vuzol-local-only@127.0.0.1:5432/vuzol_test"  # pragma: allowlist secret
)


@pytest.fixture
def postgres_dsn() -> str:
    dsn = os.getenv("VUZOL_TEST_DATABASE_DSN", DEFAULT_TEST_DSN)
    try:
        with psycopg.connect(dsn, connect_timeout=2):
            pass
    except psycopg.OperationalError:
        pytest.skip("real PostgreSQL test database is unavailable")
    return dsn.replace("postgresql://", "postgresql+psycopg://", 1)


@pytest.fixture(autouse=True)
def clean_postgres(request: pytest.FixtureRequest) -> Iterator[None]:
    if request.node.get_closest_marker("postgresql") is None:
        yield
        return
    postgres_dsn = str(request.getfixturevalue("postgres_dsn"))
    sync_dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(sync_dsn, autocommit=True) as connection:
        tables = connection.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename <> 'alembic_version'
            """
        ).fetchall()
        if tables:
            quoted = ", ".join(f'"{row[0]}"' for row in tables)
            connection.execute(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE")
    yield
