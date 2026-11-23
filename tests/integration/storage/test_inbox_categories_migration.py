"""The catalog migration seeds instructions and only reclassifies active game records."""

import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from vuzol.config import get_settings

pytestmark = pytest.mark.postgresql


def test_inbox_catalog_migration(postgres_dsn, monkeypatch):
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", postgres_dsn)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://")
    try:
        command.upgrade(config, "head")
        command.downgrade(config, "c3f5a7b9d102")
        cases = [
            ("other", '["игра"]', False, "game"),
            ("product", '["игры"]', False, "game"),
            ("note", '["игры", "coop"]', False, "game"),
            ("film", '["игра"]', False, "film"),
            ("other", '["игровой"]', False, "other"),
            ("other", '["игра"]', True, "other"),
        ]
        with psycopg.connect(dsn) as connection:
            for ordinal, (kind, tags, removed, _) in enumerate(cases):
                connection.execute(
                    "INSERT INTO inbox_items "
                    "(id, owner_user_id, chat_id, ordinal, kind, title, summary, tags, fields, "
                    "evidence, answers, removed_at) "
                    "VALUES (%s, 1, 1, %s, %s, 'test', '', %s, '{}', '', '[]', "
                    "CASE WHEN %s THEN now() END)",
                    (uuid.uuid4(), ordinal, kind, tags, removed),
                )
        command.upgrade(config, "head")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                "SELECT kind FROM inbox_items ORDER BY ordinal"
            ).fetchall() == [(case[3],) for case in cases]
            rows = connection.execute(
                "SELECT slug, name, emoji, description, created_by, created_at "
                "FROM inbox_categories"
            ).fetchall()
            assert len(rows) == 17
            assert all(row[1] and row[2] and row[3] and row[4] == "seed" and row[5] for row in rows)
            assert next(row for row in rows if row[0] == "game")[1] == "Игры"
            with pytest.raises(psycopg.IntegrityError), connection.transaction():
                connection.execute(
                    "INSERT INTO inbox_categories (slug, name, emoji, description, created_by) "
                    "VALUES ('Bad_slug', 'Тест', '🧪', 'Тест.', 'model')"
                )
    finally:
        command.upgrade(config, "head")
        get_settings.cache_clear()
