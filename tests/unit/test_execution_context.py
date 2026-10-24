"""Shared runtime constraints remain available after planner retirement."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from vuzol.context.execution import execution_constraints_context


@pytest.mark.parametrize("enabled", [False, True])
def test_actual_execution_policy_is_explicit_bounded_context(enabled):
    project = SimpleNamespace(
        id="afterword", validation_commands=[SimpleNamespace(argv=("make", "test"))]
    )
    item = execution_constraints_context(project, dependency_provisioning_enabled=enabled)
    body = json.loads(item.content)
    assert body["trusted_validation_commands"] == [["make", "test"]]
    assert body["dependency_provisioning_enabled"] is enabled
    assert item.content_hash == hashlib.sha256(item.content.encode()).hexdigest()
    assert execution_constraints_context(None, dependency_provisioning_enabled=enabled) is None
