"""Versioned capability descriptors and the static local node descriptor (WP03).

A descriptor states *what a capability means* (actions, schemas, effect class,
prerequisites) — never *where it is installed* (installation) nor *who may use
it* (permission grant, `Capability`/`allowed_capabilities`). Declared,
verified and permitted stay separate (ADR-A01).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum

from vuzol.config.settings import Settings

CAPABILITY_DESCRIPTORS_SCHEMA = "capability-descriptors.v1"
LOCAL_NODE_SCHEMA = "local-node.v1"


class CapabilityKind(StrEnum):
    HOST_TOOL = "host_tool"
    MANAGED_TOOLCHAIN = "managed_toolchain"
    ACTION = "action"


class EffectClass(StrEnum):
    READ_ONLY = "read_only"
    ISOLATED_MUTATION = "isolated_mutation"
    EXTERNAL_MUTATION = "external_mutation"
    HOST_PRIVILEGED = "host_privileged"


@dataclass(frozen=True, slots=True)
class ActionSchema:
    name: str
    version: str


@dataclass(frozen=True, slots=True)
class CapabilityDescriptor:
    key: str
    version: str
    label: str
    kind: CapabilityKind
    effect_class: EffectClass
    input_schema: ActionSchema
    output_schema: ActionSchema
    # PATH-resolved name for a host tool; None means managed-toolchain only.
    host_executable: str | None = None
    requires: tuple[str, ...] = ()
    # Secret references only (aliases resolved by narrow adapters); never values.
    secret_refs: tuple[str, ...] = ()

    def canonical(self) -> dict[str, object]:
        return {
            "schema_version": CAPABILITY_DESCRIPTORS_SCHEMA,
            "key": self.key,
            "version": self.version,
            "label": self.label,
            "kind": self.kind.value,
            "effect_class": self.effect_class.value,
            "host_executable": self.host_executable,
            "input_schema": {"name": self.input_schema.name, "version": self.input_schema.version},
            "output_schema": {
                "name": self.output_schema.name,
                "version": self.output_schema.version,
            },
            "requires": list(self.requires),
            "secret_refs": list(self.secret_refs),
        }

    @property
    def descriptor_hash(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


def _descriptor(
    key: str,
    *,
    label: str,
    kind: CapabilityKind,
    effect_class: EffectClass,
    host_executable: str | None = None,
    requires: tuple[str, ...] = (),
    secret_refs: tuple[str, ...] = (),
    version: str = "1",
) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        key=key,
        version=version,
        label=label,
        kind=kind,
        effect_class=effect_class,
        input_schema=ActionSchema(name=f"{key}.input", version=f"{key}.input.v1"),
        output_schema=ActionSchema(name=f"{key}.output", version=f"{key}.output.v1"),
        host_executable=host_executable,
        requires=requires,
        secret_refs=secret_refs,
    )


def builtin_descriptors() -> tuple[CapabilityDescriptor, ...]:
    """Static v1 descriptors for the capabilities that already exist."""

    return (
        _descriptor(
            "git",
            label="Git",
            kind=CapabilityKind.HOST_TOOL,
            effect_class=EffectClass.ISOLATED_MUTATION,
            host_executable="git",
        ),
        _descriptor(
            "python-runtime",
            label="Python runtime",
            kind=CapabilityKind.HOST_TOOL,
            effect_class=EffectClass.READ_ONLY,
            host_executable="python3",
        ),
        _descriptor(
            "node-runtime",
            label="Node.js runtime",
            kind=CapabilityKind.MANAGED_TOOLCHAIN,
            effect_class=EffectClass.READ_ONLY,
            host_executable="node",
        ),
        _descriptor(
            "java-runtime",
            label="Java runtime",
            kind=CapabilityKind.MANAGED_TOOLCHAIN,
            effect_class=EffectClass.READ_ONLY,
        ),
        _descriptor(
            "go-toolchain",
            label="Go toolchain",
            kind=CapabilityKind.MANAGED_TOOLCHAIN,
            effect_class=EffectClass.READ_ONLY,
        ),
        _descriptor(
            "gradle-toolchain",
            label="Gradle toolchain",
            kind=CapabilityKind.MANAGED_TOOLCHAIN,
            effect_class=EffectClass.READ_ONLY,
            requires=("java-runtime",),
        ),
        _descriptor(
            "repo.validate",
            label="Repository validation",
            kind=CapabilityKind.ACTION,
            effect_class=EffectClass.READ_ONLY,
            requires=("git",),
        ),
        _descriptor(
            "repo.apply",
            label="Local result apply",
            kind=CapabilityKind.ACTION,
            effect_class=EffectClass.ISOLATED_MUTATION,
            requires=("git",),
        ),
        _descriptor(
            "capability.install",
            label="Capability installation",
            kind=CapabilityKind.ACTION,
            effect_class=EffectClass.HOST_PRIVILEGED,
            secret_refs=(),
        ),
        _descriptor(
            "web-research",
            label="Web research",
            kind=CapabilityKind.ACTION,
            effect_class=EffectClass.READ_ONLY,
            secret_refs=(),
        ),
    )


_DESCRIPTORS_BY_KEY: dict[str, CapabilityDescriptor] = {
    descriptor.key: descriptor for descriptor in builtin_descriptors()
}


def descriptor_for(key: str) -> CapabilityDescriptor | None:
    return _DESCRIPTORS_BY_KEY.get(key)


# Capability enum value (vuzol.config.models.Capability) -> descriptor key.
# Only capabilities with a vetted retrieval/execution meaning are mapped;
# unmapped capabilities have no registry selection.
CAPABILITY_DESCRIPTOR_KEYS: dict[str, str] = {
    "web_research": "web-research",
}


def descriptor_for_capability(capability: str) -> CapabilityDescriptor | None:
    """Select the registry descriptor backing a capability label (WP06)."""

    key = CAPABILITY_DESCRIPTOR_KEYS.get(capability)
    return descriptor_for(key) if key is not None else None


def host_executables() -> dict[str, str]:
    """The host-tool name per capability key (the old ``_EXECUTABLES`` map)."""

    return {
        descriptor.key: descriptor.host_executable
        for descriptor in _DESCRIPTORS_BY_KEY.values()
        if descriptor.host_executable is not None
    }


@dataclass(frozen=True, slots=True)
class LocalNodeDescriptor:
    node_id: str
    labels: tuple[str, ...]
    trust_class: str
    capability_keys: tuple[str, ...]
    approved_toolchain_root: str
    secret_refs: tuple[str, ...] = ()
    schema_version: str = LOCAL_NODE_SCHEMA

    def canonical(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "node_id": self.node_id,
            "labels": list(self.labels),
            "trust_class": self.trust_class,
            "capability_keys": list(self.capability_keys),
            "approved_toolchain_root": self.approved_toolchain_root,
            "secret_refs": list(self.secret_refs),
        }

    @property
    def descriptor_hash(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


def local_node_descriptor(settings: Settings) -> LocalNodeDescriptor:
    """Build the single static local node descriptor from configuration."""

    provisioning = settings.capability_provisioning
    return LocalNodeDescriptor(
        node_id=provisioning.node_id,
        labels=("linux", "local"),
        trust_class="local",
        capability_keys=tuple(sorted(_DESCRIPTORS_BY_KEY)),
        approved_toolchain_root=str(provisioning.toolchain_root),
        secret_refs=provisioning.secret_refs,
    )
