from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.project_environment import (
    _merge_environment,
    apply_approved_environment_delta,
    current_environment,
    detect_environment_contract,
    environment_hash,
    record_detected_environment,
)
from vuzol.storage.models import ProjectEnvironmentRevision


def test_detects_node_web_service_without_executing_repository(tmp_path: Path) -> None:
    (tmp_path / "server.js").write_text("require('http').createServer()\n")
    (tmp_path / "index.html").write_text("<main>App</main>\n")

    contract = detect_environment_contract(tmp_path)

    components = cast(dict[str, Any], contract["components"])
    capabilities = cast(dict[str, Any], contract["capabilities"])
    web = cast(dict[str, Any], components["web"])
    assert web["kind"] == "web_service"
    assert web["run_command"] == ["node", "server.js"]
    assert web["port"] == 8080
    assert capabilities["node-runtime"]["provisioning"] == "automatic"


def test_non_web_project_does_not_invent_preview_component(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("# Library\n")

    contract = detect_environment_contract(tmp_path)

    assert contract["components"] == {}
    assert contract["capabilities"] == {}


def test_detects_static_site_and_python_runtime(tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text("<main>Static</main>\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='demo'\n")

    contract = detect_environment_contract(tmp_path)

    components = cast(dict[str, Any], contract["components"])
    capabilities = cast(dict[str, Any], contract["capabilities"])
    assert components["web"]["kind"] == "static_site"
    assert capabilities["python-runtime"]["key"] == "python-runtime"


def test_detects_android_artifact_contract(tmp_path: Path) -> None:
    (tmp_path / "gradlew").write_text("#!/bin/sh\n")
    app = tmp_path / "app"
    app.mkdir()
    (app / "build.gradle.kts").write_text("plugins {}\n")

    contract = detect_environment_contract(tmp_path)

    components = cast(dict[str, Any], contract["components"])
    capabilities = cast(dict[str, Any], contract["capabilities"])
    android = cast(dict[str, Any], components["android"])
    assert android["kind"] == "android_app"
    assert android["artifact_patterns"] == ["**/build/outputs/apk/**/*.apk"]
    assert "android-sdk" in capabilities


def test_environment_delta_merges_removes_and_sorts_contract() -> None:
    previous: dict[str, object] = {
        "components": {
            "z-old": {"key": "z-old", "kind": "other"},
            "web": {"key": "web", "kind": "static_site"},
        },
        "capabilities": {"git": {"key": "git", "label": "Git"}},
    }
    merged = _merge_environment(
        previous,
        {
            "remove_components": ["z-old", 7],
            "upsert_components": [
                {"key": "api", "kind": "web_service"},
                {"not-a-key": "ignored"},
                "ignored",
            ],
            "required_capabilities": [
                {"key": "node-runtime", "label": "Node"},
                {"bad": "ignored"},
                None,
            ],
        },
    )

    assert list(cast(dict[str, object], merged["components"])) == ["api", "web"]
    assert list(cast(dict[str, object], merged["capabilities"])) == ["git", "node-runtime"]


def test_environment_merge_recovers_from_malformed_previous_contract() -> None:
    merged = _merge_environment(
        {"components": [], "capabilities": "bad"},
        {
            "remove_components": "bad",
            "upsert_components": "bad",
            "required_capabilities": "bad",
        },
    )

    assert merged["components"] == {}
    assert merged["capabilities"] == {}


def test_environment_hash_is_canonical() -> None:
    assert environment_hash({"b": 2, "a": "привіт"}) == environment_hash({"a": "привіт", "b": 2})


@pytest.mark.anyio
async def test_current_environment_accepts_only_environment_revision() -> None:
    revision = ProjectEnvironmentRevision(
        project_id="demo",
        revision_number=1,
        source="detected",
        contract={},
        content_hash="a" * 64,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[revision, object()])

    assert await current_environment(session, "demo", for_update=True) is revision
    assert await current_environment(session, "demo") is None


@pytest.mark.anyio
async def test_approved_environment_delta_is_optional_and_idempotent() -> None:
    session = MagicMock()
    existing = ProjectEnvironmentRevision(
        project_id="demo",
        revision_number=1,
        source="plan_approval",
        contract={},
        content_hash="a" * 64,
    )
    session.scalar = AsyncMock(return_value=existing)

    absent = await apply_approved_environment_delta(
        session,
        project_id="demo",
        plan_revision=cast(Any, SimpleNamespace(immutable_body={}, id="plan")),
        approved_by_user_id=42,
    )
    repeated = await apply_approved_environment_delta(
        session,
        project_id="demo",
        plan_revision=cast(
            Any, SimpleNamespace(immutable_body={"environment_delta": {}}, id="plan")
        ),
        approved_by_user_id=42,
    )

    assert absent is None
    assert repeated is existing
    session.execute.assert_not_called()


@pytest.mark.anyio
async def test_approved_environment_delta_appends_child_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = ProjectEnvironmentRevision(
        project_id="demo",
        revision_number=3,
        source="detected",
        contract={"components": {"old": {"key": "old"}}},
        content_hash="a" * 64,
    )
    session = MagicMock()
    session.scalar = AsyncMock(return_value=None)
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    monkeypatch.setattr(
        "vuzol.project_environment.current_environment", AsyncMock(return_value=parent)
    )
    plan = SimpleNamespace(
        id="plan",
        immutable_body={
            "environment_delta": {
                "remove_components": ["old"],
                "upsert_components": [{"key": "web", "kind": "static_site"}],
            }
        },
    )

    revision = await apply_approved_environment_delta(
        session,
        project_id="demo",
        plan_revision=cast(Any, plan),
        approved_by_user_id=42,
    )

    assert revision is not None
    assert revision.revision_number == 4
    assert revision.parent_revision_id == parent.id
    assert revision.contract["components"] == {"web": {"key": "web", "kind": "static_site"}}
    session.add.assert_called_once_with(revision)
    session.flush.assert_awaited_once()


@pytest.mark.anyio
async def test_detected_environment_reuses_existing_or_records_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    existing = ProjectEnvironmentRevision(
        project_id="demo",
        revision_number=1,
        source="detected",
        contract={},
        content_hash="a" * 64,
    )
    session = MagicMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    current = AsyncMock(side_effect=[existing, None])
    monkeypatch.setattr("vuzol.project_environment.current_environment", current)
    (tmp_path / "index.html").write_text("<main>Demo</main>\n")

    reused = await record_detected_environment(session, project_id="demo", repository=tmp_path)
    created = await record_detected_environment(session, project_id="new", repository=tmp_path)

    assert reused is existing
    assert created.revision_number == 1
    assert created.source == "detected"
    assert created.contract["components"]["web"]["kind"] == "static_site"
    session.add.assert_called_once_with(created)
    session.flush.assert_awaited_once()
