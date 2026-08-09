import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from vuzol.config import StaticDeploymentConfig
from vuzol.providers.handlers import repair_context_item, static_delivery_context
from vuzol.storage.models import Step


def test_static_delivery_context_is_a_bounded_verified_system_skill() -> None:
    project = SimpleNamespace(
        static_deployment=StaticDeploymentConfig(
            url_path="demo",
            source_directory=Path("dist"),
            include=(Path("."),),
        )
    )
    item = static_delivery_context(project)
    assert item is not None
    assert item.source == "system_skill"
    assert item.reference == "skill:static-site-delivery:v1"
    assert "Vuzol publish_static" in item.content
    assert "local server" in item.content
    assert '"configured_source_directory":"dist"' in item.content


def test_static_delivery_context_is_absent_without_project_contract() -> None:
    assert static_delivery_context(None) is None
    assert static_delivery_context(SimpleNamespace(static_deployment=None)) is None


def test_repair_context_is_bounded_system_evidence() -> None:
    step = cast(
        Step,
        cast(
            Any,
            SimpleNamespace(
                id=uuid.uuid4(),
                step_type="execute_code",
                payload={
                    "repair_context": {
                        "source": "validate",
                        "category": "validation_gate_failed",
                        "summary": "tests failed",
                        "validation_result": {"exit_code": 1},
                        "untrusted_extra": "ignored",
                    }
                },
            ),
        ),
    )
    item = repair_context_item(step)
    assert item is not None and item.source == "system_repair"
    assert "tests failed" in item.content
    assert "untrusted_extra" not in item.content
    assert len(item.content) <= 4_000


def test_repair_context_rejects_non_system_shape() -> None:
    step = cast(
        Step,
        cast(Any, SimpleNamespace(id=uuid.uuid4(), step_type="execute_code", payload={})),
    )
    assert repair_context_item(step) is None
