"""The root host step for a project the owner approved; only the restart commands are fake."""

import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from tests.integration.leadership.test_managed_goal_flow import _factory
from vuzol.config import (
    Capability,
    CommandDefinition,
    DeliveryMode,
    GitDeliveryPolicy,
    ProjectConfig,
    RegistryDocument,
    RuntimeConfiguration,
    SandboxProfileConfig,
    Settings,
    build_bundle,
    load_document,
    merge_documents,
)
from vuzol.config.models import NetworkPolicy, SandboxNetworkMode
from vuzol.config.settings import ProjectSetupSettings
from vuzol.projects.setup import (
    ProjectSetupService,
    SetupRequest,
    new_project,
    new_topic,
    queue_setup,
    render_block,
)
from vuzol.storage.models import (
    ProjectDiscussionSession,
    ProjectExecutorPreference,
    TopicMapping,
    TransactionalOutbox,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
CHAT = -1001234567777


@pytest.fixture
def anyio_backend():
    return "asyncio"


def template() -> ProjectConfig:
    return ProjectConfig(
        id="tmpl",
        display_name="Шаблон",
        repository_path=Path("tmpl"),
        default_branch="main",
        allowed_capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),),
        sandbox_profile="fixture",
        git_delivery=GitDeliveryPolicy(
            allowed_modes=frozenset({DeliveryMode.RETAIN, DeliveryMode.APPLY}),
            approval_required=frozenset({DeliveryMode.APPLY}),
        ),
        network=NetworkPolicy(
            enabled=True,
            destinations=(
                {"url": "https://chatgpt.com", "purpose": "Codex"},
                {"url": "https://opencode.ai", "purpose": 'Pi "go"'},
            ),
        ),
    )


def request(entry=None, **changes):
    values = {
        "entry_id": entry or uuid.uuid4(),
        "project_id": "marathon",
        "name": 'Марафон "весна"',
        "description": "Подготовка к марафону.",
        "chat_id": CHAT,
        "thread_id": 2001,
    }
    return SetupRequest(**{**values, **changes})


def test_the_registry_block_round_trips_with_quotes_and_cyrillic() -> None:
    wanted = request()
    block = render_block(new_project(template(), wanted), new_topic(wanted), note="test")
    assert 'display_name = "Марафон \\"весна\\""' in block
    assert "[[projects.network.destinations]]" in block


def runtime(tmp_path):
    repos = tmp_path / "repos"
    (repos / "tmpl").mkdir(parents=True)
    base = tmp_path / "base.json"
    document = RegistryDocument(
        projects=(template(),),
        sandboxes=(
            SandboxProfileConfig(
                id="fixture",
                image="fixture@sha256:" + "a" * 64,
                network_mode=SandboxNetworkMode.HTTPS_PROXY,
            ),
        ),
    )
    base.write_text(document.model_dump_json())
    overlay = tmp_path / "overlay.toml"
    overlay.write_text("# hand-kept accounts live here\n")
    settings = Settings(
        environment="test",
        repository_root=repos,
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
        registry_file=base,
        registry_overlay_file=overlay,
        project_setup=ProjectSetupSettings(
            enabled=True,
            template_project_id="tmpl",
            idle_wait_seconds=0,
            compose_command=("compose",),
        ),
    )
    return RuntimeConfiguration(settings=settings, registries=build_bundle(document, settings))


async def test_setup_creates_everything_once_and_tells_the_assistant(postgres_dsn, tmp_path):
    configured = runtime(tmp_path)
    commands = []

    async def run(argv):
        commands.append(tuple(argv))

    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                ProjectExecutorPreference(
                    project_id="tmpl", mode="auto", model_assignments={"lead": {"x": 1}}
                )
            )
            await queue_setup(session, request())
        service = ProjectSetupService(configured, factory, owner="setup", run=run)
        assert await service.process_one()

        repository = tmp_path / "repos" / "marathon"
        assert (repository / "README.md").read_text().startswith('# Марафон "весна"')
        overlay = configured.settings.registry_overlay_file
        assert overlay.read_text().startswith("# hand-kept accounts live here\n")
        merged = build_bundle(
            merge_documents(
                load_document(configured.settings.registry_file), load_document(overlay)
            ),
            configured.settings,
        )
        project = merged.projects.get("marathon")
        assert project.network == template().network and project.summary_path.name == "README.md"
        assert {t.project_id for t in merged.topics.items()} == {"marathon"}
        assert commands == [
            ("systemctl", "restart", *configured.settings.project_setup.systemd_units),
            ("compose", "restart", *configured.settings.project_setup.compose_services),
        ]
        async with factory() as session:
            mapping = await session.scalar(select(TopicMapping))
            discussion = await session.scalar(select(ProjectDiscussionSession))
            preference = await session.get(ProjectExecutorPreference, "marathon")
            ready = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.operation_type == "project_ready"
                )
            )
        assert (mapping.project_id, mapping.message_thread_id) == ("marathon", 2001)
        assert discussion.project_id == "marathon"
        assert preference.model_assignments == {"lead": {"x": 1}}
        assert ready.destination == "assistant" and ready.payload["project_id"] == "marathon"

        # A repeated request (a retry after a crash) changes nothing twice.
        async with factory.begin() as session:
            await queue_setup(session, request())
        assert await service.process_one()
        assert overlay.read_text().count('\nid = "marathon"\n') == 1
        async with factory() as session:
            assert len((await session.scalars(select(ProjectDiscussionSession))).all()) == 1


async def test_a_bad_request_fails_at_once_and_says_so(postgres_dsn, tmp_path):
    configured = runtime(tmp_path)
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                TransactionalOutbox(
                    destination="project_setup",
                    operation_type="create",
                    linked_entity_type="assistant",
                    linked_entity_id=uuid.uuid4(),
                    idempotency_key="bad",
                    payload={**request().model_dump(mode="json"), "project_id": "../etc"},
                )
            )
        service = ProjectSetupService(configured, factory, owner="setup", run=None)
        assert await service.process_one()
        async with factory() as session:
            failed = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.operation_type == "project_failed"
                )
            )
        assert failed.payload["reason"] == "ValidationError"
        assert not (tmp_path / "etc").exists()
