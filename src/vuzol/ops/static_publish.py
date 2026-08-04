"""Rootless, allowlisted and atomic publication of static project snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import uuid
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

SOURCE_ROOT = Path("/srv/vuzol/repositories")
SITE_ROOT = Path("/srv/vuzol/sites")
DEFAULT_CONFIG = Path("/etc/vuzol/static-sites.json")
MAX_FILES = 10_000
MAX_BYTES = 250 * 1024 * 1024


class StaticPublishError(RuntimeError):
    """A bounded publication failure safe to expose in service logs."""


@dataclass(frozen=True, slots=True)
class StaticSite:
    id: str
    source: Path
    destination: Path
    include: tuple[Path, ...]
    entrypoint: Path = Path("index.html")
    keep_releases: int = 5


@dataclass(frozen=True, slots=True)
class PublishResult:
    site_id: str
    release: str
    changed: bool
    files: int
    bytes: int


def load_sites(path: Path) -> tuple[StaticSite, ...]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise StaticPublishError("static site configuration is unreadable") from error
    if not isinstance(raw, dict) or set(raw) != {"sites"} or not isinstance(raw["sites"], list):
        raise StaticPublishError("static site configuration must contain only a sites list")
    sites = tuple(_parse_site(item) for item in raw["sites"])
    ids = [site.id for site in sites]
    if len(ids) != len(set(ids)):
        raise StaticPublishError("static site ids must be unique")
    return sites


def publish(
    site: StaticSite,
    *,
    source_root: Path | None = None,
    site_root: Path | None = None,
) -> PublishResult:
    source = _contained_directory(site.source, source_root or SOURCE_ROOT, "source")
    destination = _contained_destination(site.destination, site_root or SITE_ROOT)
    releases = destination / "releases"
    releases.mkdir(parents=True, exist_ok=True, mode=0o755)
    staging = releases / f".staging-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o755)
    try:
        files, total_bytes = _copy_allowlist(source, staging, site.include)
        if not _regular_file(staging / site.entrypoint):
            raise StaticPublishError(f"required entrypoint is missing: {site.entrypoint}")
        _require_closed_html_entrypoint(staging, site.entrypoint)
        digest = _tree_digest(staging)
        release = releases / digest[:20]
        if release.exists():
            shutil.rmtree(staging)
        else:
            staging.rename(release)
        changed = _activate(destination, release)
        _prune(releases, current=release, keep=site.keep_releases)
        return PublishResult(site.id, release.name, changed, files, total_bytes)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def rollback(site: StaticSite, *, site_root: Path | None = None) -> PublishResult:
    destination = _contained_destination(site.destination, site_root or SITE_ROOT)
    releases = destination / "releases"
    current = _current_release(destination)
    candidates = sorted(
        (
            path
            for path in releases.iterdir()
            if path.is_dir() and not path.name.startswith(".") and path != current
        ),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    if not candidates:
        raise StaticPublishError(f"no previous release exists for {site.id}")
    target = candidates[0]
    _activate(destination, target)
    files, total_bytes = _tree_size(target)
    return PublishResult(site.id, target.name, True, files, total_bytes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--site")
    parser.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    sites = load_sites(args.config)
    selected = tuple(site for site in sites if args.site is None or site.id == args.site)
    if args.site is not None and not selected:
        raise SystemExit(f"unknown static site: {args.site}")
    for site in selected:
        result = rollback(site) if args.rollback else publish(site)
        print(json.dumps(asdict(result), sort_keys=True))


def _parse_site(raw: object) -> StaticSite:
    allowed = {"id", "source", "destination", "include", "entrypoint", "keep_releases"}
    if not isinstance(raw, dict) or not set(raw).issubset(allowed):
        raise StaticPublishError("static site entry contains unsupported fields")
    try:
        site_id = raw["id"]
        source = Path(raw["source"])
        destination = Path(raw["destination"])
        include = tuple(Path(item) for item in raw["include"])
        entrypoint = Path(raw.get("entrypoint", "index.html"))
        keep = int(raw.get("keep_releases", 5))
    except (KeyError, TypeError, ValueError) as error:
        raise StaticPublishError("static site entry is invalid") from error
    if not isinstance(site_id, str) or not site_id or not site_id.replace("-", "").isalnum():
        raise StaticPublishError("static site id is invalid")
    if not source.is_absolute() or not destination.is_absolute() or not include:
        raise StaticPublishError("static site paths and include list are required")
    if keep < 2 or keep > 20:
        raise StaticPublishError("keep_releases must be between 2 and 20")
    for relative in (*include, entrypoint):
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise StaticPublishError("published paths must be safe relative paths")
    return StaticSite(site_id, source, destination, include, entrypoint, keep)


def _copy_allowlist(source: Path, staging: Path, include: tuple[Path, ...]) -> tuple[int, int]:
    copied: set[Path] = set()
    files = 0
    total_bytes = 0
    for relative in include:
        candidate = source / relative
        if not candidate.exists():
            continue
        if candidate.is_symlink():
            raise StaticPublishError(f"allowlisted path is unsafe: {relative}")
        paths = (candidate,) if candidate.is_file() else tuple(sorted(candidate.rglob("*")))
        for item in paths:
            if item.is_dir():
                if item.is_symlink():
                    relative = item.relative_to(source)
                    raise StaticPublishError(f"symlink directory is forbidden: {relative}")
                continue
            rel = item.relative_to(source)
            if rel in copied:
                continue
            info = item.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise StaticPublishError(f"only regular files may be published: {rel}")
            files += 1
            total_bytes += info.st_size
            if files > MAX_FILES or total_bytes > MAX_BYTES:
                raise StaticPublishError("static publication exceeds configured size limits")
            target = staging / rel
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            with item.open("rb") as source_file, target.open("xb") as target_file:
                shutil.copyfileobj(source_file, target_file)
            target.chmod(0o644)
            copied.add(rel)
    return files, total_bytes


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


class _LocalAssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.references: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "script" and values.get("src"):
            self.references.append(str(values["src"]))
        elif tag == "link" and values.get("rel") == "stylesheet" and values.get("href"):
            self.references.append(str(values["href"]))


def _require_closed_html_entrypoint(root: Path, entrypoint: Path) -> None:
    """Reject releases whose HTML points outside the published subpath or snapshot."""

    try:
        html = (root / entrypoint).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise StaticPublishError("static entrypoint is not readable UTF-8 HTML") from error
    parser = _LocalAssetParser()
    parser.feed(html)
    for reference in parser.references:
        parsed = urlsplit(reference)
        if parsed.scheme or parsed.netloc or not parsed.path:
            continue
        if parsed.path.startswith("/"):
            raise StaticPublishError(
                f"entrypoint asset must be relative for subpath hosting: {reference}"
            )
        relative = entrypoint.parent / Path(unquote(parsed.path))
        candidate = (root / relative).resolve(strict=False)
        try:
            candidate.relative_to(root.resolve(strict=True))
        except ValueError as error:
            raise StaticPublishError(f"entrypoint asset escapes release: {reference}") from error
        if not _regular_file(candidate):
            raise StaticPublishError(f"entrypoint asset is missing from release: {reference}")


def _activate(destination: Path, release: Path) -> bool:
    current = destination / "current"
    if current.is_symlink() and current.resolve() == release.resolve():
        return False
    temporary = destination / f".current-{uuid.uuid4().hex}"
    temporary.symlink_to(Path("releases") / release.name)
    os.replace(temporary, current)
    return True


def _prune(releases: Path, *, current: Path, keep: int) -> None:
    ordered = sorted(
        (path for path in releases.iterdir() if path.is_dir() and not path.name.startswith(".")),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    retained = {current, *ordered[:keep]}
    for path in ordered:
        if path not in retained:
            shutil.rmtree(path)


def _contained_directory(path: Path, root: Path, label: str) -> Path:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(root.resolve(strict=True))
    except ValueError as error:
        raise StaticPublishError(f"{label} escapes its trusted root") from error
    if path.is_symlink() or not resolved.is_dir():
        raise StaticPublishError(f"{label} must be a real directory")
    return resolved


def _contained_destination(path: Path, root: Path) -> Path:
    root_resolved = root.resolve(strict=True)
    try:
        path.resolve(strict=False).relative_to(root_resolved)
    except ValueError as error:
        raise StaticPublishError("destination escapes its trusted root") from error
    try:
        path.parent.resolve(strict=True).relative_to(root_resolved)
    except FileNotFoundError as error:
        raise StaticPublishError("destination parent does not exist") from error
    if path.exists() and (path.is_symlink() or not path.is_dir()):
        raise StaticPublishError("destination must be a real directory")
    path.mkdir(mode=0o755, exist_ok=True)
    return path.resolve(strict=True)


def _current_release(destination: Path) -> Path:
    current = destination / "current"
    if not current.is_symlink():
        raise StaticPublishError("site has no active release")
    resolved = current.resolve(strict=True)
    try:
        resolved.relative_to((destination / "releases").resolve(strict=True))
    except ValueError as error:
        raise StaticPublishError("active release escapes the destination") from error
    return resolved


def _tree_size(root: Path) -> tuple[int, int]:
    files = [path for path in root.rglob("*") if path.is_file()]
    return len(files), sum(path.stat().st_size for path in files)


def _regular_file(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except FileNotFoundError:
        return False
