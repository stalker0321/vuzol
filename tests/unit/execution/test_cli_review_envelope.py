"""Supported reviewers retain canonical commands and read-only sandbox fences."""

import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unit.providers.test_cli_readonly import review_call as review_call
from vuzol.config.models import Capability, ProviderRole, SandboxProfileConfig
from vuzol.config.revision import content_revision
from vuzol.execution.codex import ExecutionEnvelopeFactory, _require_provider_command
from vuzol.execution.domain import MountMode
from vuzol.projects.executor_preference import ExecutorPreferenceError
from vuzol.projects.model_matrix import ModelAssignment
from vuzol.providers import accounts
from vuzol.providers.codex import CodexCliAdapter, canonical_codex_argv
from vuzol.providers.grok import GrokCliAdapter, canonical_grok_argv
from vuzol.providers.kimi import KimiCliAdapter, canonical_kimi_argv
from vuzol.providers.pi import PiCliAdapter, canonical_pi_argv
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.storage.types import StepStatus

from ._execution_helpers import _seccomp_profile


def _argv(profile, *, read_only=True):
    if profile.provider == "codex":
        return canonical_codex_argv(
            model=profile.model,
            reasoning_effort=profile.model_reasoning_effort,
            read_only=read_only,
        )
    if profile.provider == "grok":
        return canonical_grok_argv(
            profile.model, reasoning_effort=profile.model_reasoning_effort, read_only=read_only
        )
    if profile.provider == "kimi":
        return canonical_kimi_argv(
            profile.model, reasoning_effort=profile.model_reasoning_effort, read_only=read_only
        )
    return canonical_pi_argv(
        profile.model, reasoning_effort=profile.model_reasoning_effort, read_only=read_only
    )


@pytest.fixture
def envelope_call(review_call, tmp_path):
    c = review_call
    settings = c.runtime.settings
    settings.worktree_root = tmp_path
    settings.artifact_root = tmp_path / "artifacts"
    settings.dependency_provisioning.enabled = False
    seccomp, digest = _seccomp_profile(tmp_path)
    settings.execution.sandbox_seccomp_profile = seccomp
    settings.execution.sandbox_seccomp_profile_sha256 = digest
    worktree = Path(c.worktree.path)
    worktree.mkdir()
    (worktree / ".git").write_text("gitdir: /unmounted/metadata\n")
    c.profile.state_directory.mkdir()
    c.runtime.registries.profiles.get.return_value = c.profile
    c.runtime.registries.sandboxes.get.return_value = SandboxProfileConfig(
        id="sandbox", image="test@sha256:" + "a" * 64, enabled=True
    )
    c.session.add = MagicMock(side_effect=lambda row: setattr(row, "id", uuid.uuid4()))
    c.envelopes = ExecutionEnvelopeFactory(c.factory, settings, c.runtime.registries)
    c.invocation = CodexInvocation(
        argv=_argv(c.profile),
        stdin="review prompt",
        runtime_identity=c.profile.runtime_identity,
        state_directory=str(c.profile.state_directory),
        timeout_seconds=30,
        sandbox_reference=f"worktree:{c.worktree.id}",
        task_id=c.request.task_id,
        run_id=c.request.run_id,
        step_id=c.request.step_id,
        profile_id=c.profile.id,
        provider_attempt=c.request.provider_attempt,
        lease_generation=c.request.lease_generation,
    )
    c.session.scalar.side_effect = None
    c.session.scalar.return_value = None
    c.step.payload.update(
        executor_model_override=c.profile.model,
        executor_reasoning_effort=c.profile.model_reasoning_effort,
    )
    return c


@pytest.mark.anyio
async def test_native_review_adapter_uses_canonical_read_only_envelope(envelope_call):
    c = envelope_call
    outputs = {
        "codex": '{"text":"reviewed"}',
        "grok": '{"type":"text","data":"reviewed"}\n{"type":"end","stopReason":"EndTurn"}',
        "kimi": '{"role":"assistant","content":"reviewed"}',
        "pi": '{"type":"agent_end","messages":[{"role":"assistant",'
        '"content":[{"type":"text","text":"reviewed"}]}]}\n'
        '{"type":"agent_settled"}',
    }
    adapters = {
        "codex": CodexCliAdapter,
        "grok": GrokCliAdapter,
        "kimi": KimiCliAdapter,
        "pi": PiCliAdapter,
    }
    built = []

    async def run(invocation, cancellation):
        assert cancellation is c.cancellation
        built.append(await c.envelopes.build(invocation))
        return CodexProcessResult(0, outputs[c.profile.provider], "", 1)

    transport = MagicMock(run=AsyncMock(side_effect=run))
    c.wrapper._adapter = adapters[c.profile.provider](transport)
    c.session.scalar.side_effect = [0, c.worktree, None]
    result = await c.wrapper.execute(c.request, c.profile, c.cancellation)

    assert result.text == "reviewed"
    envelope, process_id = built[0]
    assert envelope.argv == _argv(c.profile)
    assert envelope.task_id == c.request.task_id
    assert envelope.run_id == c.request.run_id
    assert envelope.step_id == c.request.step_id
    assert envelope.lease_generation == c.request.lease_generation
    mounts = {mount.purpose: mount for mount in envelope.sandbox.mounts}
    assert mounts["task-worktree"].mode is MountMode.READ_ONLY
    assert mounts["worktree-git-metadata"].mode is MountMode.READ_ONLY
    assert mounts["provider-state"].mode is (
        MountMode.READ_ONLY if c.profile.provider == "pi" else MountMode.READ_WRITE
    )
    assert mounts["provider-state"].source == c.profile.state_directory
    assert mounts["provider-state"].target == Path(f"/{c.profile.provider}-home")
    assert envelope.sandbox.network_disabled
    process = c.session.add.call_args.args[0]
    assert process.id == process_id and process.command_envelope_hash == envelope.stable_hash
    c.access.revoke.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("tamper", ["writable", "extra-argument", "wrong-model", "wrong-effort"])
@pytest.mark.parametrize("step_type", ["review", "execute_agent"])
async def test_review_rejects_noncanonical_commands_before_process_record(
    envelope_call, tamper, step_type
):
    c = envelope_call
    c.step.step_type = step_type
    if step_type == "execute_agent":
        # Conversation profiles use their native model, not legacy coding overrides.
        c.step.payload = {}
    if tamper == "writable":
        argv = _argv(c.profile, read_only=False)
    elif tamper == "extra-argument":
        argv = (*c.invocation.argv, "--unsafe")
    elif tamper == "wrong-model":
        argv = tuple(part.replace(c.profile.model, "wrong-model") for part in c.invocation.argv)
        if c.profile.provider == "kimi":
            argv = tuple(part.replace("tokenrouter/kimi-k3-free", "wrong-model") for part in argv)
    else:
        # Pi and Kimi do not expose a reasoning CLI flag: adding one is also noncanonical.
        argv = (*c.invocation.argv, "--reasoning-effort", "low")
    with pytest.raises(ValueError, match="canonical read-only"):
        await c.envelopes.build(replace(c.invocation, argv=argv))
    c.session.add.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("key", "value"),
    [("executor_model_override", "other-model"), ("executor_reasoning_effort", "low")],
)
async def test_review_rejects_claim_overrides_that_differ_from_profile(envelope_call, key, value):
    c = envelope_call
    c.step.payload[key] = value
    with pytest.raises(ValueError, match="claim does not match"):
        await c.envelopes.build(c.invocation)
    c.session.add.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "field", ["task_id", "run_id", "step_id", "profile_id", "provider_attempt", "lease_generation"]
)
async def test_review_requires_complete_execution_identity(envelope_call, field):
    c = envelope_call
    with pytest.raises(ValueError, match="lacks fenced execution identity"):
        await c.envelopes.build(replace(c.invocation, **{field: None}))
    c.session.add.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("mismatch", ["task", "run", "generation", "status"])
async def test_review_rejects_stale_execution_binding(envelope_call, mismatch):
    c = envelope_call
    if mismatch == "task":
        c.worktree.task_id = uuid.uuid4()
    elif mismatch == "run":
        c.worktree.run_id = uuid.uuid4()
    elif mismatch == "generation":
        c.step.lease_generation += 1
    else:
        c.step.status = StepStatus.COMPLETED
    with pytest.raises(ValueError, match="current fenced lease"):
        await c.envelopes.build(c.invocation)
    c.session.add.assert_not_called()


def test_non_review_canonical_command_policy_is_unchanged(envelope_call):
    c = envelope_call
    for read_only in (False, True):
        _require_provider_command(
            _argv(c.profile, read_only=read_only),
            c.profile.provider,
            c.profile.model,
            reasoning_effort=c.profile.model_reasoning_effort,
        )
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(("unexpected",), c.profile.provider, c.profile.model)


def test_review_rejects_unsupported_adapter_even_with_canonical_command(envelope_call):
    c = envelope_call
    with pytest.raises(ValueError, match="canonical read-only"):
        _require_provider_command(
            c.invocation.argv, "unsupported", c.profile.model, require_read_only=True
        )


@pytest.mark.anyio
@pytest.mark.parametrize("has_account", [False, True])
async def test_process_records_optional_account_metadata_without_overwriting_runtime_fields(
    envelope_call, has_account
):
    c = envelope_call
    lease_token = uuid.uuid4()
    token = accounts._execution.set(
        accounts.AccountExecution("shared-account", 2, lease_token) if has_account else None
    )
    try:
        envelope, _ = await c.envelopes.build(c.invocation)
    finally:
        accounts._execution.reset(token)
    expected = {
        "executor_model": c.profile.model,
        "executor_reasoning_effort": c.profile.model_reasoning_effort,
        "configured_deadline_seconds": envelope.sandbox.timeout_seconds,
        "cancellation_classification": None,
        "cancellation_initiator": None,
        "cleanup_initiator": "sandbox_transport_finally",
    }
    if has_account:
        expected.update(
            account_id="shared-account", account_slot=2, account_lease_token=str(lease_token)
        )
    assert c.session.add.call_args.args[0].runtime_metadata == expected


@pytest.mark.anyio
async def test_account_metadata_cannot_overwrite_transport_metadata(envelope_call, monkeypatch):
    from vuzol.execution import codex

    metadata = {"account_id": "account", "cleanup_initiator": "other"}
    monkeypatch.setattr(
        codex, "current_account_execution", lambda: SimpleNamespace(runtime_metadata=metadata)
    )
    await envelope_call.envelopes.build(envelope_call.invocation)
    persisted = envelope_call.session.add.call_args.args[0].runtime_metadata
    assert persisted["cleanup_initiator"] == "sandbox_transport_finally"
    assert persisted["account_id"] == "account"
    assert metadata["cleanup_initiator"] == "other"


@pytest.mark.anyio
@pytest.mark.parametrize("changed", [False, True])
async def test_envelope_enforces_exact_matrix_profile_revision(envelope_call, changed):
    c = envelope_call
    c.runtime.registries.profiles.items.return_value = (c.profile,)
    pinned = ModelAssignment(
        profile_id=c.profile.id,
        model=c.profile.model,
        reasoning_effort=c.profile.model_reasoning_effort,
        content_revision=content_revision(c.profile),
    )
    c.step.payload.update(
        project_model_slot="reviewer_light",
        project_model_assignment=pinned.model_dump(mode="json"),
    )
    if changed:
        updated = c.profile.model_copy(update={"account_id": "different-account"})
        c.runtime.registries.profiles.items.return_value = (updated,)
        c.runtime.registries.profiles.get.return_value = updated
        with pytest.raises(ExecutorPreferenceError, match="configuration changed"):
            await c.envelopes.build(c.invocation)
        c.session.add.assert_not_called()
    else:
        envelope, _ = await c.envelopes.build(c.invocation)
        assert envelope.profile_id == pinned.profile_id
        assert envelope.argv == _argv(c.profile)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["think", "lead"])
@pytest.mark.parametrize("changed", [False, True])
async def test_conversation_host_envelope_is_read_only_and_revision_fenced(
    envelope_call, mode, changed
):
    c = envelope_call
    c.profile = c.profile.model_copy(
        update={
            "roles": frozenset({ProviderRole.LEAD if mode == "lead" else ProviderRole.EXECUTOR}),
            "capabilities": frozenset({Capability.REPOSITORY_READ}),
            "supported_task_types": frozenset({"architecture"}),
            "sandbox_required": True,
        }
    )
    assignment = ModelAssignment(
        profile_id=c.profile.id,
        model=c.profile.model,
        reasoning_effort=c.profile.model_reasoning_effort,
        content_revision=content_revision(c.profile),
    )
    c.step.step_type = "execute_agent"
    c.step.payload = {
        "project_model_slot": mode,
        "project_model_assignment": assignment.model_dump(mode="json"),
    }
    if changed:
        c.profile = c.profile.model_copy(update={"model": "unexpected-replacement"})
    c.runtime.registries.profiles.get.return_value = c.profile
    c.runtime.registries.profiles.items.return_value = (c.profile,)
    if changed:
        with pytest.raises(ExecutorPreferenceError, match="configuration changed"):
            await c.envelopes.build(c.invocation)
        c.session.add.assert_not_called()
    else:
        envelope, _ = await c.envelopes.build(c.invocation)
        mounts = {mount.purpose: mount for mount in envelope.sandbox.mounts}
        assert mounts["task-worktree"].mode is MountMode.READ_ONLY
        assert mounts["worktree-git-metadata"].mode is MountMode.READ_ONLY


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["task_id", "run_id", "step_id"])
async def test_task_transport_cannot_complete_turn_process_without_task_identity(
    envelope_call, missing
):
    c = envelope_call
    process = SimpleNamespace(
        task_id=c.request.task_id, run_id=c.request.run_id, step_id=c.request.step_id
    )
    setattr(process, missing, None)
    c.session.get.side_effect = None
    c.session.get.return_value = process
    artifacts = AsyncMock()
    with pytest.raises(ValueError, match="lacks fenced task identity"):
        await c.envelopes.complete(uuid.uuid4(), CodexProcessResult(0, "output", "", 1), artifacts)
    artifacts.persist.assert_not_awaited()
