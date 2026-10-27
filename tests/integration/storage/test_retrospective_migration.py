"""Decision migration retains notebooks and refuses lossy downgrade."""

import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from vuzol.config import get_settings


@pytest.mark.postgresql
def test_decision_migration_constraints_and_round_trip(postgres_dsn, monkeypatch):
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", postgres_dsn)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://")
    try:
        command.downgrade(config, "d9a2f6b4c8e1")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "INSERT INTO memory_units (id, scope, unit_type, status, text, "
                "extractor_version, extraction_identity, effective_at) "
                "VALUES (%s, 'global', 'notebook_note', 'observation', 'note', 'test', %s, now())",
                (uuid.uuid4(), str(uuid.uuid4())),
            )
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            assert connection.execute("SELECT details FROM memory_units").fetchall() == [(None,)]
            for scope, project, kind, size in (
                ("global", None, "decision_entry", 10),
                ("session", None, "decision_entry", 10),
                ("project", None, "decision_entry", 10),
                ("project", "demo", "decision_entry", 4001),
                ("project", "demo", "notebook_note", 601),
            ):
                with pytest.raises(psycopg.IntegrityError), connection.transaction():
                    connection.execute(
                        "INSERT INTO memory_units (id, scope, project_id, unit_type, status, text, "
                        "extractor_version, extraction_identity, effective_at) "
                        "VALUES (%s, %s, %s, %s, 'observation', %s, 'test', %s, now())",
                        (uuid.uuid4(), scope, project, kind, "x" * size, str(uuid.uuid4())),
                    )
            connection.execute(
                "INSERT INTO memory_units "
                "(id, scope, project_id, unit_type, status, text, details, "
                "extractor_version, extraction_identity, effective_at) "
                "VALUES (%s, 'project', 'demo', 'decision_entry', 'observation', %s, "
                "'{\"choice\": \"transaction\"}'::jsonb, 'test', %s, now())",
                (uuid.uuid4(), "x" * 4000, str(uuid.uuid4())),
            )
        with pytest.raises(RuntimeError, match="decision entries must be archived"):
            command.downgrade(config, "d9a2f6b4c8e1")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            connection.execute("DELETE FROM memory_units WHERE unit_type = 'decision_entry'")
        command.downgrade(config, "d9a2f6b4c8e1")  # pragma: allowlist secret
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            assert connection.execute("SELECT text, details FROM memory_units").fetchall() == [
                ("note", None)
            ]
    finally:
        get_settings.cache_clear()
        command.upgrade(config, "head")
        get_settings.cache_clear()
