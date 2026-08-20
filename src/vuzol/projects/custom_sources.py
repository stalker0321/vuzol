"""User-originated, project-scoped dependency source contracts."""

from __future__ import annotations

import re
import shlex
import uuid
from dataclasses import dataclass
from urllib.parse import urlsplit

from vuzol.projects.source_catalog import SourceCatalogError, _trusted_https_url

_PACKAGE = re.compile(r"(?:@[a-z0-9._-]+/)?[A-Za-z0-9][A-Za-z0-9._-]*")
_ECOSYSTEMS = {"python", "node"}
_HEX = re.compile(r"[0-9a-f]+")
_PYTHON_SEPARATOR = re.compile(r"[-_.]+")


class CustomSourceError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CustomDependencySource:
    id: uuid.UUID
    project_id: str
    ecosystem: str
    package_name: str
    source_kind: str
    source_url: str
    source_pin: str

    @property
    def hostname(self) -> str:
        hostname = urlsplit(self.source_url).hostname
        if hostname is None:  # pragma: no cover - constructor parser guarantees it
            raise CustomSourceError("custom dependency source has no hostname")
        return hostname

    def approval_record(self) -> dict[str, str]:
        return {
            "source_id": str(self.id),
            "ecosystem": self.ecosystem,
            "package_name": self.package_name,
            "source_kind": self.source_kind,
            "source_url": self.source_url,
            "source_pin": self.source_pin,
        }


@dataclass(frozen=True, slots=True)
class AddSourceCommand:
    ecosystem: str
    package_name: str
    source_kind: str
    source_url: str
    source_pin: str


@dataclass(frozen=True, slots=True)
class RemoveSourceCommand:
    source_id: uuid.UUID


SourceCommand = AddSourceCommand | RemoveSourceCommand


def is_source_command(text: str | None) -> bool:
    if text is None:
        return False
    try:
        parts = shlex.split(text)
    except ValueError:
        return text.lstrip().startswith("/source")
    return bool(parts) and parts[0].split("@", 1)[0] == "/source"


def parse_source_command(text: str | None) -> SourceCommand:
    if text is None:
        raise CustomSourceError(_usage())
    try:
        parts = shlex.split(text)
    except ValueError as error:
        raise CustomSourceError("invalid /source quoting") from error
    if not parts or parts[0].split("@", 1)[0] != "/source":
        raise CustomSourceError(_usage())
    if len(parts) == 3 and parts[1] == "remove":
        try:
            return RemoveSourceCommand(uuid.UUID(parts[2]))
        except ValueError as error:
            raise CustomSourceError("/source remove requires a source UUID") from error
    if len(parts) != 7 or parts[1] != "add":
        raise CustomSourceError(_usage())
    _command, _verb, ecosystem, package_name, source_kind, source_url, source_pin = parts
    ecosystem = ecosystem.casefold()
    source_kind = source_kind.casefold()
    if ecosystem not in _ECOSYSTEMS:
        raise CustomSourceError("custom sources currently support python or node")
    if _PACKAGE.fullmatch(package_name) is None or len(package_name) > 214:
        raise CustomSourceError("custom source package name is invalid")
    if source_kind not in {"git", "https"}:
        raise CustomSourceError("custom source kind must be git or https")
    if ecosystem == "node" and source_kind == "https":
        raise CustomSourceError(
            "Node custom sources currently require an exact Git commit; "
            "HTTPS artifact digest enforcement is not available"
        )
    try:
        _trusted_https_url(source_url)
    except SourceCatalogError as error:
        raise CustomSourceError("custom source URL must be bounded public HTTPS") from error
    if len(source_url) > 500:
        raise CustomSourceError("custom source URL exceeds 500 characters")
    parsed = urlsplit(source_url)
    if parsed.query or parsed.fragment:
        raise CustomSourceError("custom source URL cannot contain query or fragment")
    expected_length = 40 if source_kind == "git" else 64
    if len(source_pin) != expected_length or _HEX.fullmatch(source_pin) is None:
        label = "40-character commit" if source_kind == "git" else "SHA-256"
        raise CustomSourceError(f"custom {source_kind} source requires lowercase {label}")
    return AddSourceCommand(
        ecosystem=ecosystem,
        package_name=normalize_package_name(ecosystem, package_name),
        source_kind=source_kind,
        source_url=source_url,
        source_pin=source_pin,
    )


def normalize_package_name(ecosystem: str, package_name: str) -> str:
    normalized = package_name.casefold()
    return _PYTHON_SEPARATOR.sub("-", normalized) if ecosystem == "python" else normalized


def _usage() -> str:
    return "usage: /source add <python|node> <package> <git|https> <https-url> <commit-or-sha256>"
