"""Choices pinned with full profile revisions move to assignment revisions once."""

import pytest

from tests.integration.storage.helpers import storage
from tests.unit.projects.test_model_matrix import assignment, configured
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import assignment_revision, content_revision
from vuzol.projects.executor_preference import ensure_preference_row
from vuzol.projects.model_matrix import assignment_snapshot, upgrade_assignment_revisions


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.postgresql
@pytest.mark.anyio
async def test_legacy_pins_are_rewritten_once_and_only_when_they_still_match(postgres_dsn):
    current = configured(profile_id="current")
    changed = configured(profile_id="changed")
    legacy = assignment(current).model_copy(update={"content_revision": content_revision(current)})
    stale = assignment(changed).model_copy(
        update={"model": "older-model", "content_revision": content_revision(changed)}
    )
    unknown = assignment(configured(profile_id="removed"))
    engine, factory = storage(postgres_dsn)
    try:
        async with factory.begin() as session:
            row = await ensure_preference_row(session, "project")
            row.model_assignments = {
                "lead": legacy.model_dump(mode="json"),
                "executor": stale.model_dump(mode="json"),
                "reviewer_light": unknown.model_dump(mode="json"),
            }
        profiles = ProfileRegistry((current, changed))
        async with factory.begin() as session:
            assert await upgrade_assignment_revisions(session, profiles) == 1
        async with factory.begin() as session:
            assert await upgrade_assignment_revisions(session, profiles) == 0
            row = await ensure_preference_row(session, "project")
            pins = assignment_snapshot(row.model_assignments)
        assert pins["lead"]["content_revision"] == assignment_revision(current)
        assert pins["executor"] == stale.model_dump(mode="json")
        assert pins["reviewer_light"] == unknown.model_dump(mode="json")
    finally:
        await engine.dispose()
