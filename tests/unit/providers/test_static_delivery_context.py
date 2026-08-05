from pathlib import Path
from types import SimpleNamespace

from vuzol.config import StaticDeploymentConfig
from vuzol.providers.handlers import static_delivery_context


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
