"""Agent certificate tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import *


def test_agent_certificate_is_keyed_to_exact_runtime_tuple(tmp_path: Path) -> None:
    profile = _certified_codex_profile()
    sandbox = SandboxProfileConfig(
        id="provider",
        image=f"provider@sha256:{'a' * 64}",
        network_mode=SandboxNetworkMode.HTTPS_PROXY,
    )
    key = certification_key(profile, sandbox)
    store = AgentCertificateStore(tmp_path / "certificates")
    issued = new_certificate(
        key=key,
        profile_id=profile.id,
        task_uuid=str(uuid.uuid4()),
        run_uuid=str(uuid.uuid4()),
    )
    store.issue(issued)
    assert store.require(profile, sandbox) == issued

    stale_sandbox = sandbox.model_copy(update={"image": f"provider@sha256:{'b' * 64}"})
    with pytest.raises(ValueError, match="uncertified"):
        store.require(profile, stale_sandbox)


def test_agent_certificate_rejects_invalid_and_incomplete_evidence(tmp_path: Path) -> None:
    from pydantic import ValidationError

    from vuzol.execution.runtime_contract import AgentRuntimeCertificate

    profile = _certified_codex_profile()
    sandbox = SandboxProfileConfig(id="provider", image=f"provider@sha256:{'a' * 64}")
    key = certification_key(profile, sandbox)
    with pytest.raises(ValidationError, match="every runtime invariant"):
        AgentRuntimeCertificate.model_validate(
            {
                "key": key,
                "profile_id": profile.id,
                "certified_at": "2026-07-15T00:00:00Z",
                "ordinary_file_read": True,
                "ordinary_file_edited": True,
                "git_protected": False,
                "structured_output_valid": True,
                "cleanup_succeeded": True,
                "task_uuid": "task",
                "run_uuid": "run",
            }
        )

    store = AgentCertificateStore(tmp_path / "certificates")
    path = store._path(key)
    path.parent.mkdir()
    path.write_text("not-json")
    with pytest.raises(ValueError, match="invalid"):
        store.require(profile, sandbox)

    uncertified_profile = profile.model_copy(update={"agent_runtime_contract": None})
    with pytest.raises(ValueError, match="no agent runtime contract"):
        certification_key(uncertified_profile, sandbox)

    unsafe_root = tmp_path / "unsafe-certificates"
    unsafe_root.symlink_to(tmp_path / "certificates", target_is_directory=True)
    with pytest.raises(ValueError, match="cannot be a symlink"):
        AgentCertificateStore(unsafe_root).issue(
            new_certificate(
                key=key,
                profile_id=profile.id,
                task_uuid="task",
                run_uuid="run",
            )
        )


def test_runtime_certificate_bypass_is_limited_to_fixed_probe_shape() -> None:
    from vuzol.providers.handlers import _is_runtime_certification

    capsule = {
        "runtime_certification": True,
        "task_id": "agent-certification-123",
        "allowed_paths": ["certification/agent-runtime-probe.txt"],
        "maximum_repair_count": 0,
        "parent_attempt": None,
        "required_gates": [{"name": "format-check", "command_id": "make format-check"}],
    }
    assert _is_runtime_certification({"step09a_capsule": capsule}) is True
    assert (
        _is_runtime_certification(
            {"step09a_capsule": {**capsule, "allowed_paths": ["src/vuzol/app.py"]}}
        )
        is False
    )


@pytest.mark.anyio
async def test_validation_image_preflight_uses_fixed_offline_commands_and_fails_closed(
    tmp_path: Path,
) -> None:
    from vuzol.cli.executor import (
        VALIDATION_IMAGE_PREFLIGHT_COMMANDS,
        _preflight_validation_images,
    )

    sandbox = SandboxProfileConfig(
        id="validation",
        image="validation@sha256:" + "c" * 64,
        network_mode=SandboxNetworkMode.NONE,
    )
    registries = MagicMock()
    registries.projects.items.return_value = (
        MagicMock(enabled=True, validation_sandbox_profile="validation"),
    )
    registries.sandboxes.get.return_value = sandbox
    runtime = MagicMock()
    runtime.run = AsyncMock(return_value=CodexProcessResult(0, "version", "", 1))
    seccomp, digest = _seccomp_profile(tmp_path)

    await _preflight_validation_images(
        runtime, registries, seccomp_profile=seccomp, seccomp_digest=digest
    )

    assert runtime.run.await_count == 3
    envelopes = [call.args[0] for call in runtime.run.await_args_list]
    assert tuple(envelope.argv for envelope in envelopes) == VALIDATION_IMAGE_PREFLIGHT_COMMANDS
    assert all(envelope.sandbox.image == sandbox.image for envelope in envelopes)
    assert all(envelope.sandbox.network_disabled for envelope in envelopes)
    assert all(not envelope.sandbox.mounts for envelope in envelopes)

    runtime.run.reset_mock()
    runtime.run.return_value = CodexProcessResult(127, "", "missing", 1)
    with pytest.raises(RuntimeError, match="failed toolchain preflight"):
        await _preflight_validation_images(
            runtime, registries, seccomp_profile=seccomp, seccomp_digest=digest
        )
    assert runtime.run.await_count == 1


@pytest.mark.anyio
async def test_agent_contract_preflight_verifies_exact_cli_version_and_image(
    tmp_path: Path,
) -> None:
    from vuzol.cli.executor import _preflight_agent_contracts

    profile = _certified_codex_profile()
    sandbox = SandboxProfileConfig(
        id="provider",
        image="provider@sha256:" + "d" * 64,
        network_mode=SandboxNetworkMode.HTTPS_PROXY,
    )
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    registries.projects.items.return_value = (MagicMock(enabled=True, sandbox_profile="provider"),)
    registries.sandboxes.get.return_value = sandbox
    runtime = MagicMock()
    runtime.run = AsyncMock(return_value=CodexProcessResult(0, "codex-cli 0.144.1\n", "", 1))
    seccomp, digest = _seccomp_profile(tmp_path)

    await _preflight_agent_contracts(
        runtime, registries, seccomp_profile=seccomp, seccomp_digest=digest
    )

    envelope = runtime.run.await_args.args[0]
    assert envelope.argv == ("codex", "--version")
    assert envelope.sandbox.image == sandbox.image
    assert envelope.sandbox.network_disabled is True
    assert envelope.sandbox.mounts == ()

    runtime.run.return_value = CodexProcessResult(0, "codex-cli stale\n", "", 1)
    with pytest.raises(RuntimeError, match="contract preflight failed"):
        await _preflight_agent_contracts(
            runtime, registries, seccomp_profile=seccomp, seccomp_digest=digest
        )
