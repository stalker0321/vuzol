"""Run materialization freezes project role choices without database I/O."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from tests.unit.projects.test_model_matrix import assignment, configured

from vuzol.projects.model_matrix import LEGACY_EXECUTOR_KEY, MATRIX_KEY
from vuzol.storage.models import ProjectExecutorPreference, Task
from vuzol.storage.types import TaskStatus
from vuzol.workflows.domain import MaterializedWorkflow
from vuzol.workflows.service import materialize_run


@pytest.mark.anyio
@pytest.mark.parametrize("legacy", [False, True])
async def test_new_run_keeps_independent_assignment_snapshot(legacy):
    profile = configured()
    values = {"planner_light": assignment(profile).model_dump(mode="json")}
    preference = ProjectExecutorPreference(
        project_id="project",
        mode="pin" if legacy else "auto",
        worker_key="sol" if legacy else None,
        profile_id="legacy-account" if legacy else None,
        reasoning_effort=None,
        revision=4,
        model_assignments=values,
    )
    task = Task(id=uuid.uuid4(), project_id="project", status=TaskStatus.INTERPRETED, version=1)
    session = AsyncMock()
    session.scalar.side_effect = [None, task]
    session.get.return_value = preference
    session.add = MagicMock()
    workflow = MaterializedWorkflow("adaptive", "1", uuid.uuid4(), ())
    run = await materialize_run(
        session,
        task_id=task.id,
        workflow=workflow,
        configuration_revision="config",
        policy_revision="policy",
        prompt_revision=None,
        automatic_start=False,
    )
    assert run.selected_route[MATRIX_KEY] == values
    values["planner_light"]["model"] = "later-model"
    assert run.selected_route[MATRIX_KEY]["planner_light"]["model"] == profile.model
    preference.model_assignments = {}
    assert "planner_light" in run.selected_route[MATRIX_KEY]
    if legacy:
        assert run.selected_route[LEGACY_EXECUTOR_KEY]["profile_id"] == "legacy-account"
        preference.profile_id = "later-account"
        assert run.selected_route[LEGACY_EXECUTOR_KEY]["profile_id"] == "legacy-account"


@pytest.mark.anyio
async def test_existing_run_does_not_resnapshot_preferences():
    existing = MagicMock()
    session = AsyncMock()
    session.scalar.return_value = existing
    result = await materialize_run(
        session,
        task_id=uuid.uuid4(),
        workflow=MaterializedWorkflow("adaptive", "1", uuid.uuid4(), ()),
        configuration_revision="config",
        policy_revision="policy",
        prompt_revision=None,
        automatic_start=False,
    )
    assert result is existing
    session.get.assert_not_awaited()
