"""Safe intake helpers for importing an existing remote repository."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit


class ProjectImportError(ValueError):
    """A bounded, user-facing repository import error."""


@dataclass(frozen=True, slots=True)
class ImportedRepository:
    url: str
    project_id: str
    display_name: str


def parse_github_repository_url(value: str) -> ImportedRepository:
    """Validate one public GitHub HTTPS repository URL and derive its identity.

    Keeping the first version host-bounded prevents Git URL helpers, local paths,
    credentials, and internal-network targets from crossing the privileged boundary.
    """

    raw = value.strip()
    if any(char.isspace() for char in raw):
        raise ProjectImportError("send one repository URL without extra text")
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or parsed.hostname != "github.com":
        raise ProjectImportError("only https://github.com repository URLs are supported")
    if parsed.username is not None or parsed.password is not None or parsed.port is not None:
        raise ProjectImportError("repository URL must not contain credentials or a port")
    if parsed.query or parsed.fragment:
        raise ProjectImportError("repository URL must not contain query parameters or a fragment")
    segments = [unquote(part) for part in parsed.path.strip("/").split("/")]
    if len(segments) != 2:
        raise ProjectImportError("expected https://github.com/owner/repository")
    owner, raw_repository_name = segments
    if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?", owner):
        raise ProjectImportError("GitHub owner name is invalid")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,104}", raw_repository_name):
        raise ProjectImportError("GitHub repository name is invalid")
    repository_name = raw_repository_name
    if repository_name.endswith(".git"):
        repository_name = repository_name[:-4]
    project_id = re.sub(r"[^a-z0-9]+", "-", repository_name.lower()).strip("-")
    if not project_id or len(project_id) > 63 or not project_id[0].isalpha():
        raise ProjectImportError("repository name cannot be converted to a project identifier")
    canonical = f"https://github.com/{owner}/{repository_name}.git"
    display_name = repository_name.replace("-", " ").replace("_", " ").strip().title()
    return ImportedRepository(canonical, project_id, display_name[:100])
