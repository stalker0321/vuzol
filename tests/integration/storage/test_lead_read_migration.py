"""Round-zero receipts and existing decision details survive migration both ways."""

import asyncio
import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from tests.integration.leadership.test_lead_surface import _settings, _Transport
from tests.integration.leadership.test_managed_goal_flow import _enable, _factory, _session
from tests.integration.leadership.test_model_assignment_receipts import _claim, _worker
from vuzol.config import get_settings


@pytest.mark.postgresql
def test_existing_receipt_and_decision_backfill_round_trip(postgres_dsn, monkeypatch):
    settings = _settings(monkeypatch, "lead-local")
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", postgres_dsn)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://")

    async def seed():
        async with _factory(postgres_dsn) as factory:
            await _enable(factory, await _session(factory))
            worker = _worker(factory, settings, _Transport({"commands": []}), owner="migration")
            await worker._infer(await _claim(factory, worker, "migration"))

    try:
        command.upgrade(config, "head")
        asyncio.run(seed())
        command.downgrade(config, "e1b7c9d4a6f2")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            before = connection.execute(
                "SELECT id, request, result FROM leadership_inferences"
            ).fetchall()
            assert len(before) == 1
            connection.execute(
                "INSERT INTO memory_units (id, scope, project_id, unit_type, status, text, "
                "details, extractor_version, extraction_identity, effective_at) VALUES "
                "(%s, 'project', 'demo', 'decision_entry', 'observation', 'choice', "
                "'{\"options\":[\"clementine\"]}', 'test', %s, now())",
                (uuid.uuid4(), str(uuid.uuid4())),
            )
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            assert (
                connection.execute(
                    "SELECT id, request, result FROM leadership_inferences"
                ).fetchall()
                == before
            )
            assert connection.execute(
                'SELECT "round", tool_audit FROM leadership_inferences'
            ).fetchall() == [(0, [])]
            assert connection.execute(
                "SELECT text_search @@ plainto_tsquery('simple', 'clementine') FROM memory_units"
            ).fetchall() == [(True,)]
        command.downgrade(config, "e1b7c9d4a6f2")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            assert (
                connection.execute(
                    "SELECT id, request, result FROM leadership_inferences"
                ).fetchall()
                == before
            )
            assert connection.execute(
                "SELECT text_search @@ plainto_tsquery('simple', 'choice') FROM memory_units"
            ).fetchall() == [(True,)]
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            connection.execute('UPDATE leadership_inferences SET "round" = 1')
        with pytest.raises(RuntimeError, match=r"archive.*round"):
            command.downgrade(config, "e1b7c9d4a6f2")  # pragma: allowlist secret
        with psycopg.connect(dsn) as connection:
            connection.execute('UPDATE leadership_inferences SET "round" = 0')
    finally:
        command.upgrade(config, "head")
        get_settings.cache_clear()
