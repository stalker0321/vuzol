"""Unit tests for WP03 descriptors, local node and discovery boundaries."""

from __future__ import annotations

from pathlib import Path

from vuzol.config import Settings
from vuzol.projects.capabilities import CapabilityState, preflight_capabilities
from vuzol.projects.descriptors import (
    CAPABILITY_DESCRIPTORS_SCHEMA,
    LOCAL_NODE_SCHEMA,
    CapabilityDescriptor,
    builtin_descriptors,
    descriptor_for,
    host_executables,
    local_node_descriptor,
)


def test_descriptors_are_versioned_and_stable() -> None:
    descriptors = builtin_descriptors()
    keys = [descriptor.key for descriptor in descriptors]
    assert len(keys) == len(set(keys))
    for descriptor in descriptors:
        canonical = descriptor.canonical()
        assert canonical["schema_version"] == CAPABILITY_DESCRIPTORS_SCHEMA
        assert descriptor.descriptor_hash == descriptor.descriptor_hash
    assert descriptor_for("node-runtime") is not None
    assert descriptor_for("missing") is None


def test_host_executables_match_the_legacy_map() -> None:
    assert host_executables() == {
        "git": "git",
        "python-runtime": "python3",
        "node-runtime": "node",
    }


def test_descriptors_carry_no_permission_grant() -> None:
    fields = set(CapabilityDescriptor.__dataclass_fields__)
    assert not fields.intersection({"permissions", "grants", "allowed_capabilities", "capability"})
    for descriptor in builtin_descriptors():
        serialized = descriptor.canonical()
        assert not {
            "permissions",
            "grants",
            "allowed_capabilities",
        }.intersection(serialized)


def test_local_node_descriptor_uses_settings_and_refs_only() -> None:
    from vuzol.config.settings import CapabilityProvisioningSettings

    settings = Settings(
        environment="test",
        capability_provisioning=CapabilityProvisioningSettings(
            node_id="node-a", secret_refs=("env:GITHUB_TOKEN",)
        ),
    )
    node = local_node_descriptor(settings)
    assert node.schema_version == LOCAL_NODE_SCHEMA
    assert node.node_id == "node-a"
    assert node.secret_refs == ("env:GITHUB_TOKEN",)
    assert all(ref.startswith(("env:", "file:")) for ref in node.secret_refs)
    assert node.descriptor_hash == local_node_descriptor(settings).descriptor_hash


CONTRACT: dict[str, object] = {
    "capabilities": {"node-runtime": {"label": "Node", "provisioning": "automatic"}}
}


def test_discovery_marks_stale_or_failed_installations_needs_setup() -> None:
    contract = CONTRACT
    stale = preflight_capabilities(
        contract,
        which=lambda _name: "/usr/bin/node",
        installation_status={"node-runtime": "stale"},
    )
    assert stale[0].state is CapabilityState.NEEDS_SETUP
    assert "stale" in stale[0].detail
    failed = preflight_capabilities(
        contract,
        which=lambda _name: "/usr/bin/node",
        installation_status={"node-runtime": "failed"},
    )
    assert failed[0].state is CapabilityState.NEEDS_SETUP


def test_discovery_rejects_executable_outside_approved_roots() -> None:
    contract = CONTRACT
    nvm = "/home/user/.nvm/versions/node/v24/bin/node"
    # Legacy behavior (no roots) still trusts PATH; with roots it must fail closed.
    assert (
        preflight_capabilities(contract, which=lambda _name: nvm)[0].state
        is CapabilityState.READY
    )
    confined = preflight_capabilities(
        contract, which=lambda _name: nvm, approved_roots=(Path("/usr"),)
    )
    assert confined[0].state is CapabilityState.NEEDS_SETUP
    assert "confined" in confined[0].detail


def test_discovery_never_installs_or_grants_permissions() -> None:
    # preflight only classifies; it takes no installer and returns no grants.
    contract = CONTRACT
    checks = preflight_capabilities(contract, which=lambda _name: "/usr/bin/node")
    assert all(check.state in set(CapabilityState) for check in checks)
    assert all(not hasattr(check, "grant") for check in checks)
