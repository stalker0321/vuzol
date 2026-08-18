"""Typed acceptance artifacts derived from approved project components."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArtifactExpectation:
    component_key: str
    kind: str
    label: str
    validation: str
    patterns: tuple[str, ...] = ()


_DEFAULTS: dict[str, tuple[str, str]] = {
    "static_site": ("web_preview", "HTTP smoke check"),
    "web_service": ("web_preview", "healthcheck and HTTP smoke check"),
    "android_app": ("android_apk", "APK exists and installs in the test target"),
    "cli": ("cli_transcript", "declared command exits successfully"),
    "library": ("package_archive", "package build and consumer smoke test"),
    "bot": ("bot_protocol_report", "protocol-level smoke test"),
    "mcp_server": ("mcp_conformance_report", "MCP initialize and tool listing"),
    "worker": ("worker_run_report", "bounded job execution and output check"),
    "database": ("migration_report", "migration applies and schema check passes"),
    "other": ("validation_report", "declared trusted checks pass"),
}


def expected_artifacts(contract: dict[str, object]) -> tuple[ArtifactExpectation, ...]:
    components = contract.get("components")
    if not isinstance(components, dict):
        return ()
    expectations: list[ArtifactExpectation] = []
    for key in sorted(components):
        component = components[key]
        if not isinstance(key, str) or not isinstance(component, dict):
            continue
        component_kind = str(component.get("kind") or "other")
        artifact_kind, validation = _DEFAULTS.get(component_kind, _DEFAULTS["other"])
        raw_patterns = component.get("artifact_patterns")
        patterns = (
            tuple(value for value in raw_patterns if isinstance(value, str))
            if isinstance(raw_patterns, list)
            else ()
        )
        expectations.append(
            ArtifactExpectation(
                component_key=key,
                kind=artifact_kind,
                label=str(component.get("label") or key),
                validation=validation,
                patterns=patterns,
            )
        )
    return tuple(expectations)
