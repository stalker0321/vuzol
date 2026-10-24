"""Keep the upgrade inventory coupled to the consumers actually shipped."""

import ast
from pathlib import Path

import pytest

from vuzol.providers.handlers import SAFE_PROVIDER_STEP_TYPES
from vuzol.providers.routing import PROVIDER_STEP_ROLES
from vuzol.workflows.upgrade import SUPPORTED_STEP_TYPES


def test_upgrade_inventory_matches_service_composition():
    root = Path(__file__).resolve().parents[2] / "src/vuzol"
    registered = set(SAFE_PROVIDER_STEP_TYPES)
    for path in (
        "cli/worker.py",
        "cli/executor.py",
        "cli/applier.py",
        "cli/static_publisher_worker.py",
        "providers/handlers.py",
        "workflows/worker.py",
    ):
        tree = ast.parse((root / path).read_text())
        dictionaries = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id.lower().endswith("handlers")
                and isinstance(node.value, ast.Dict)
            ):
                dictionaries.append(node.value)
            if (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Dict)
                and any(
                    isinstance(t, ast.Name) and t.id.lower().endswith("handlers")
                    for t in node.targets
                )
            ):
                dictionaries.append(node.value)
            if (
                isinstance(node, ast.keyword)
                and node.arg == "handlers"
                and isinstance(node.value, ast.Dict)
            ):
                dictionaries.append(node.value)
            if isinstance(node, ast.FunctionDef) and node.name.endswith("handlers"):
                dictionaries.extend(
                    n.value
                    for n in ast.walk(node)
                    if isinstance(n, ast.Return) and isinstance(n.value, ast.Dict)
                )
        registered.update(
            k.value
            for d in dictionaries
            for k in d.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        )
    assert registered == SUPPORTED_STEP_TYPES
    assert "plan" not in PROVIDER_STEP_ROLES


@pytest.mark.parametrize("service", ["worker", "executor", "applier"])
def test_upgrade_guard_precedes_runtime_mutations(service):
    root = Path(__file__).resolve().parents[2] / "src/vuzol/cli"
    tree = ast.parse((root / f"{service}.py").read_text())
    calls = {
        n.func.id: n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }
    assert calls["require_migration_head"] < calls["require_workflow_upgrade"]
    assert calls["require_workflow_upgrade"] < calls["create_session_factory"]
