"""Notebook migration backfills legacy scopes and enforces provenance constraints."""

import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from vuzol.config import get_settings


@pytest.mark.postgresql
def test_notebook_migration_backfill_constraints_and_round_trip(postgres_dsn, monkeypatch):
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", postgres_dsn)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://")
    try:
        command.downgrade(config, "c8e4f2a7d1b9")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            for project in ("demo", None):
                identity = str(uuid.uuid4())
                connection.execute(
                    "INSERT INTO memory_units "
                    "(id, project_id, unit_type, status, text, extractor_version, "
                    "extraction_identity, effective_at) "
                    "VALUES (%s, %s, 'observation', 'observation', 'legacy', 'test', %s, now())",
                    (uuid.uuid4(), project, identity),
                )
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            assert set(
                connection.execute("SELECT project_id, scope, goal_id FROM memory_units").fetchall()
            ) == {("demo", "project", None), (None, "session", None)}
            for scope, project, text, goal in (
                ("project", None, "note", None),
                ("global", "demo", "note", None),
                ("session", None, "note", None),
                ("global", None, "x" * 601, None),
                ("global", None, "note", uuid.uuid4()),
            ):
                with pytest.raises(psycopg.IntegrityError), connection.transaction():
                    connection.execute(
                        "INSERT INTO memory_units "
                        "(id, scope, project_id, goal_id, unit_type, status, text, "
                        "extractor_version, extraction_identity, effective_at) "
                        "VALUES (%s, %s, %s, %s, 'notebook_note', 'observation', "
                        "%s, 'test', %s, now())",
                        (uuid.uuid4(), scope, project, goal, text, str(uuid.uuid4())),
                    )
            connection.execute(
                "INSERT INTO memory_units "
                "(id, scope, unit_type, status, text, extractor_version, "
                "extraction_identity, effective_at) "
                "VALUES (%s, 'global', 'notebook_note', 'observation', 'note', 'test', %s, now())",
                (uuid.uuid4(), str(uuid.uuid4())),
            )
        with pytest.raises(RuntimeError, match="archived"):
            command.downgrade(config, "c8e4f2a7d1b9")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            connection.execute("DELETE FROM memory_units WHERE unit_type = 'notebook_note'")
        command.downgrade(config, "c8e4f2a7d1b9")  # pragma: allowlist secret
        command.upgrade(config, "head")
    finally:
        get_settings.cache_clear()
        command.upgrade(config, "head")
        get_settings.cache_clear()
