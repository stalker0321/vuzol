"""Root-shipped source catalogue for approved toolchains and package registries."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import PurePosixPath
from typing import cast
from urllib.parse import urlsplit

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.projects.toolchains import (
    TOOLCHAIN_RECEIPT_SCHEMA,
    ToolchainReceiptError,
    ToolchainSpec,
    parse_toolchain_spec,
)

SOURCE_CATALOG_SCHEMA = "vuzol-source-catalog.v1"
_ECOSYSTEM = re.compile(r"[a-z][a-z0-9-]{0,63}")


class SourceCatalogError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ToolchainSource:
    spec: ToolchainSpec
    provider: str
    url: str
    redirect_hosts: tuple[str, ...]
    archive_bytes: int
    archive_format: str

    @property
    def capability_key(self) -> str:
        return self.spec.capability_key


@dataclass(frozen=True, slots=True)
class PackageRegistry:
    ecosystem: str
    provider: str
    hosts: tuple[str, ...]
    manifests: tuple[str, ...]
    lockfiles: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SourceCatalog:
    toolchains: tuple[ToolchainSource, ...]
    registries: tuple[PackageRegistry, ...]

    @classmethod
    def builtin(cls) -> SourceCatalog:
        resource = files("vuzol.projects").joinpath("source_catalog.v1.json")
        return parse_source_catalog(json.loads(resource.read_text(encoding="utf-8")))

    def toolchain(self, capability_key: str) -> ToolchainSource | None:
        matches = tuple(item for item in self.toolchains if item.capability_key == capability_key)
        if len(matches) > 1:
            raise SourceCatalogError(f"catalogue has ambiguous toolchain: {capability_key}")
        return matches[0] if matches else None

    def registry(self, ecosystem: str) -> PackageRegistry | None:
        return next((item for item in self.registries if item.ecosystem == ecosystem), None)


def parse_source_catalog(raw: object) -> SourceCatalog:
    if not isinstance(raw, dict) or raw.get("schema_version") != SOURCE_CATALOG_SCHEMA:
        raise SourceCatalogError("source catalogue schema is unsupported")
    raw_toolchains = raw.get("toolchains")
    raw_registries = raw.get("registries")
    if not isinstance(raw_toolchains, list) or not isinstance(raw_registries, list):
        raise SourceCatalogError("source catalogue collections are invalid")
    toolchains = tuple(_parse_toolchain(item) for item in raw_toolchains)
    registries = tuple(_parse_registry(item) for item in raw_registries)
    toolchain_keys = [item.capability_key for item in toolchains]
    ecosystems = [item.ecosystem for item in registries]
    if len(toolchain_keys) != len(set(toolchain_keys)):
        raise SourceCatalogError("source catalogue toolchain keys must be unique")
    if len(ecosystems) != len(set(ecosystems)):
        raise SourceCatalogError("source catalogue ecosystems must be unique")
    return SourceCatalog(toolchains=toolchains, registries=registries)


def _parse_toolchain(raw: object) -> ToolchainSource:
    if not isinstance(raw, dict):
        raise SourceCatalogError("source catalogue toolchain is invalid")
    provider = raw.get("provider")
    url = raw.get("url")
    redirect_hosts = raw.get("redirect_hosts")
    archive_bytes = raw.get("bytes")
    archive_format = raw.get("archive_format")
    if not isinstance(provider, str) or not provider.strip() or len(provider) > 120:
        raise SourceCatalogError("toolchain provider is invalid")
    _trusted_https_url(url)
    if not isinstance(redirect_hosts, list) or len(redirect_hosts) > 5:
        raise SourceCatalogError("toolchain redirect hosts are invalid")
    normalized_hosts = tuple(_trusted_host(host) for host in redirect_hosts)
    if len(normalized_hosts) != len(set(normalized_hosts)):
        raise SourceCatalogError("toolchain redirect hosts must be unique")
    if not isinstance(archive_bytes, int) or isinstance(archive_bytes, bool) or archive_bytes <= 0:
        raise SourceCatalogError("toolchain archive size is invalid")
    if archive_format not in {"tar", "zip"}:
        raise SourceCatalogError("toolchain archive format is invalid")
    try:
        spec = parse_toolchain_spec(
            {
                "schema_version": TOOLCHAIN_RECEIPT_SCHEMA,
                "capability_key": raw.get("capability_key"),
                "version": raw.get("version"),
                "archive_sha256": raw.get("sha256"),
                "executables": raw.get("executables"),
                "environment": raw.get("environment", {}),
            },
            expected_key=str(raw.get("capability_key")),
        )
    except ToolchainReceiptError as error:
        raise SourceCatalogError(str(error)) from error
    return ToolchainSource(
        spec=spec,
        provider=provider.strip(),
        url=str(url),
        redirect_hosts=normalized_hosts,
        archive_bytes=archive_bytes,
        archive_format=str(archive_format),
    )


def _parse_registry(raw: object) -> PackageRegistry:
    if not isinstance(raw, dict):
        raise SourceCatalogError("package registry is invalid")
    ecosystem = raw.get("ecosystem")
    provider = raw.get("provider")
    hosts = raw.get("hosts")
    manifests = raw.get("manifests")
    lockfiles = raw.get("lockfiles")
    if not isinstance(ecosystem, str) or _ECOSYSTEM.fullmatch(ecosystem) is None:
        raise SourceCatalogError("package registry ecosystem is invalid")
    if not isinstance(provider, str) or not provider.strip() or len(provider) > 120:
        raise SourceCatalogError("package registry provider is invalid")
    if not isinstance(hosts, list) or not hosts or len(hosts) > 10:
        raise SourceCatalogError("package registry hosts are invalid")
    normalized_hosts = tuple(_trusted_host(host) for host in hosts)
    if not _safe_project_names(manifests) or not _safe_project_names(lockfiles):
        raise SourceCatalogError("package registry project files are invalid")
    safe_manifests = cast(list[str], manifests)
    safe_lockfiles = cast(list[str], lockfiles)
    return PackageRegistry(
        ecosystem=ecosystem,
        provider=provider.strip(),
        hosts=normalized_hosts,
        manifests=tuple(safe_manifests),
        lockfiles=tuple(safe_lockfiles),
    )


def _trusted_https_url(value: object) -> str:
    if not isinstance(value, str) or len(value) > 2_000 or "\x00" in value:
        raise SourceCatalogError("toolchain source URL is invalid")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in {None, 443}
        or not parsed.hostname
        or parsed.fragment
    ):
        raise SourceCatalogError("toolchain source URL must be bounded HTTPS")
    _trusted_host(parsed.hostname)
    return value


def _trusted_host(value: object) -> str:
    if not isinstance(value, str):
        raise SourceCatalogError("source host is invalid")
    try:
        return AllowedConnectTarget(hostname=value, port=443, purpose="source catalogue").hostname
    except ValueError as error:
        raise SourceCatalogError("source host is invalid") from error


def _safe_project_names(value: object) -> bool:
    if not isinstance(value, list) or not value or len(value) > 10:
        return False
    return all(
        isinstance(item, str)
        and item
        and len(item) <= 120
        and not PurePosixPath(item).is_absolute()
        and ".." not in PurePosixPath(item).parts
        and "\x00" not in item
        for item in value
    )
