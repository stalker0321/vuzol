"""The additive jobs table enforces bounded input/results and downgrades cleanly."""

import asyncio
import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from tests.integration.leadership.test_managed_goal_flow import _enable, _factory, _session
from vuzol.config.settings import get_settings

pytestmark = [pytest.mark.postgresql]


def test_jobs_migration_upgrade_constraints_and_downgrade(postgres_dsn, monkeypatch):
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", postgres_dsn)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://")
    try:
        command.upgrade(config, "head")

        async def goal():
            # Alembic's env runs its own event loop, so only this setup is async.
            async with _factory(postgres_dsn) as factory:
                return await _enable(factory, await _session(factory))

        goal_id = asyncio.run(goal())
        with psycopg.connect(dsn) as connection:

            def insert(question, result=None, state="queued", kind="research"):
                connection.execute(
                    "INSERT INTO lead_jobs "
                    "(id, goal_id, kind, input, pinned_commit, scope_revision, state, result_text) "
                    "VALUES (%s, %s, %s, jsonb_build_object('question', %s::text), %s, 1, %s, %s)",
                    (uuid.uuid4(), goal_id, kind, question, "a" * 40, state, result),
                )

            insert("q" * 2000)
            for question, result, state, kind in (
                ("", None, "queued", "research"),
                (None, None, "queued", "research"),
                ("q" * 2001, None, "queued", "research"),
                ("q", "я" * 8193, "queued", "research"),
                ("q", None, "running", "research"),  # Requires a lease.
                ("q", None, "succeeded", "research"),  # Requires finished_at.
                ("q", None, "queued", "web"),
            ):
                with pytest.raises(psycopg.IntegrityError), connection.transaction():
                    insert(question, result, state, kind)
        command.downgrade(config, "f2c8a4e6b901")
        with psycopg.connect(dsn) as connection:
            assert connection.execute("SELECT to_regclass('public.lead_jobs')").fetchone() == (
                None,
            )
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            assert connection.execute("SELECT count(*) FROM lead_jobs").fetchone() == (0,)
    finally:
        command.upgrade(config, "head")
        get_settings.cache_clear()
