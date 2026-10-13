"""Worker account defaults are independent of model strength and discussion."""

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import select

from tests.integration.providers._test_routing_helpers import (
    bundle,
    pin_project,
    profile,
    seed_provider_step,
)
from tests.integration.storage.helpers import storage
from vuzol.config.revision import content_revision
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.models import ProjectExecutorPreference, RoutingDecision, Run, Step, Task
from vuzol.storage.types import StepStatus

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    "case",
    ("cheap", "balanced", "strong", "fallback", "unavailable", "missing", "chat", "api", "pin"),
)
def test_default_worker_is_fenced_and_does_not_replace_other_choices(
    postgres_dsn: str, tmp_path: Path, case: str
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        profiles = []
        for profile_id, provider in (("oc3", "pi"), ("oc2", "pi"), ("sol", "codex")):
            state = tmp_path / profile_id
            state.mkdir()
            profiles.append(
                profile(
                    profile_id,
                    provider=provider,
                    model="deepseek-v4-flash" if provider == "pi" else "gpt-6.1-sol",
                    api_base_url=None,
                    credential_reference=None,
                    launch_mode="cli",
                    runtime_identity=profile_id,
                    state_directory=state,
                    sandbox_required=True,
                    enabled=not (
                        (case == "unavailable"
                        and provider == "pi")
                        or (case == "fallback"
                        and profile_id == "oc3")
                    ),
                    supported_task_types=frozenset({"coding", "architecture"}),
                    capabilities=frozenset(
                        {"repository_read", "code_edit", "git", "project_shell"}
                    ),
                    cost_class="strong" if provider == "codex" else "balanced",
                    routing_priority=1 if provider == "codex" else 100,
                    fallback_profile_ids=("oc2",) if profile_id == "oc3" else (),
                )
            )
        settings, registries = bundle(
            tmp_path, *profiles, profile("api"), projects=(pin_project(),)
        )
        settings = settings.model_copy(
            update={
                "execution": settings.execution.model_copy(
                    update={"default_executor_profile_id": "absent" if case == "missing" else "oc3"}
                )
            }
        )
        task_id, run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_model"
            if case == "api"
            else "execute_agent"
            if case == "chat"
            else "execute_code",
            capabilities=[]
            if case == "api"
            else ["repository_read"]
            if case == "chat"
            else ["code_edit"],
        )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            assert task is not None and run is not None
            if case != "api":
                task.task_type = "coding"
            run.budget_mode = case if case in {"cheap", "balanced", "strong"} else "strong"
            if case == "chat":
                task.task_type = "discussion_agent_internal"
                run.workflow_type = "discussion_agent"
                run.selected_route = {
                    "schema_version": "discussion-chat-route.v1",
                    "trusted_profile_id": "sol",
                    "profile_revision": content_revision(profiles[2]),
                }
            if case == "pin":
                task.project_id = "bill-buddy"
                session.add(
                    ProjectExecutorPreference(
                        project_id="bill-buddy",
                        mode="pin",
                        worker_key="sol",
                        profile_id="sol",
                        reasoning_effort="high",
                        revision=1,
                    )
                )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="test",
                lease_seconds=60,
                candidate_limit=20,
            )
        async with factory() as session:
            step = await session.get(Step, step_id)
            assert step is not None
            if case in {"missing", "unavailable"}:
                assert token is None and step.status is StepStatus.BLOCKED
                assert step.executor_profile_id is None
            else:
                expected = (
                    "sol"
                    if case in {"chat", "pin"}
                    else "api"
                    if case == "api"
                    else "oc2"
                    if case == "fallback"
                    else "oc3"
                )
                assert token is not None and step.executor_profile_id == expected, (
                    step.failure_category,
                    step.failure_summary,
                )
                decision = await session.scalar(
                    select(RoutingDecision).where(RoutingDecision.step_id == step_id)
                )
                assert decision is not None
                if case not in {"chat", "pin", "api"}:
                    assert decision.inputs["default_executor_profile_id"] == "oc3"
        await engine.dispose()

    asyncio.run(scenario())
