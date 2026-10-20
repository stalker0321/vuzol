"""Tagged turn resources reuse exact proxy/runtime ownership checks."""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.domain import ProcessEnvelope, TurnProcessBinding
from vuzol.execution.proxy_networks import _make_network_name
from vuzol.execution.proxy_service import ProxyServiceError, _make_proxy_name, _ownership_labels
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxError

from ._execution_helpers import envelope
from ._test_proxy_service_helpers import (
    FakeNetworks,
    _manager,
    _render_policy,
    _target,
    hashlib_sha256,
)


def binding():
    return TurnProcessBinding(turn_id=uuid.uuid4(), invocation_id=uuid.uuid4())


@pytest.mark.anyio
async def test_v2_turn_manifest_roundtrip_and_cleanup(tmp_path, monkeypatch):
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    turn = binding()
    identity = (None, None, None, 4)
    directory = manager._runtime_directory(*identity, turn_binding=turn)
    policy = _render_policy((_target(),))
    manager._write_private_policy(directory, directory / "policy.json", policy)
    manager._write_state(directory, *identity, hashlib_sha256(policy), turn_binding=turn)
    (manifest,) = manager.recovery_manifests()
    assert manifest.turn_binding == turn
    assert manifest.identity == identity
    state = json.loads((directory / "execution.json").read_text())
    assert state["version"] == 2
    assert not {"task_id", "run_id", "step_id"}.intersection(state)
    remove_sandbox, remove_proxy = AsyncMock(), AsyncMock()
    monkeypatch.setattr(manager, "_remove_owned_sandbox", remove_sandbox)
    monkeypatch.setattr(manager, "_remove_owned_container", remove_proxy)
    monkeypatch.setattr(manager, "_container_exists", AsyncMock(return_value=False))
    await manager.cleanup_turn(turn, 4)
    remove_sandbox.assert_awaited_once_with(*identity, turn_binding=turn)
    remove_proxy.assert_awaited_once_with(
        _make_proxy_name(*identity, turn_binding=turn), *identity, turn_binding=turn
    )
    assert networks.cleaned[0].turn_binding == turn
    assert networks.cleaned[0].internal_name == _make_network_name(
        *identity, "internal", turn_binding=turn
    )
    assert not directory.exists()


@pytest.mark.anyio
async def test_no_manifest_is_not_proof_of_absence_or_foreign_ownership(tmp_path, monkeypatch):
    manager = _manager(tmp_path, FakeNetworks())
    turn = binding()
    monkeypatch.setattr(manager, "_container_exists", AsyncMock(return_value=True))
    inspect = AsyncMock(return_value={"Config": {"Labels": {"vuzol.managed": "true"}}})
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    remove = AsyncMock()
    monkeypatch.setattr(manager, "_remove_owned_sandbox", remove)
    with pytest.raises(ProxyServiceError):
        await manager.cleanup_turn(turn, 4)
    remove.assert_not_awaited()


@pytest.mark.anyio
async def test_turn_container_filters_do_not_authorize_foreign_extra_labels(tmp_path, monkeypatch):
    original = envelope(tmp_path)
    turn = binding()
    configured = ProcessEnvelope.model_validate(
        {
            **original.model_dump(),
            "task_id": None,
            "run_id": None,
            "step_id": None,
            "worktree_id": None,
            "turn_binding": turn,
        }
    )
    process = MagicMock(returncode=0)
    process.communicate = AsyncMock(return_value=(("a" * 64 + "\n").encode(), b""))
    monkeypatch.setattr(
        "vuzol.execution.sandbox.asyncio.create_subprocess_exec", AsyncMock(return_value=process)
    )
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    docker = AsyncMock(
        return_value=json.dumps(
            {
                "Id": "a" * 64,
                "Name": "/" + configured.container_name,
                "Config": {"Labels": {**configured.ownership_labels, "foreign": "true"}},
            }
        )
    )
    monkeypatch.setattr(runtime, "_docker", docker)
    with pytest.raises(SandboxError, match="foreign"):
        await runtime._remove_owned_container(configured.container_name, configured)
    assert [call.args[0] for call in docker.await_args_list] == ["inspect"]


def test_turn_resource_labels_do_not_claim_workflow_identity():
    turn = binding()
    labels = _ownership_labels(None, None, None, 4, turn_binding=turn)
    assert labels["vuzol.invocation_id"] == str(turn.invocation_id)
    assert not {"vuzol.task_id", "vuzol.run_id", "vuzol.step_id"}.intersection(labels)
    with pytest.raises(ProxyServiceError, match="workflow identity"):
        _make_proxy_name(uuid.uuid4(), None, None, 4, turn_binding=turn)
