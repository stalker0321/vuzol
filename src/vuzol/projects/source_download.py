"""Bounded content-addressed downloads from an approved catalogue entry."""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx

from vuzol.execution.paths import contained, trusted_root
from vuzol.projects.source_catalog import SourceCatalogError, ToolchainSource, _trusted_https_url


class SourceDownloadError(RuntimeError):
    pass


class TrustedSourceDownloader:
    def __init__(
        self,
        cache_root: Path,
        *,
        maximum_bytes: int,
        timeout_seconds: float = 120,
        client: httpx.Client | None = None,
    ) -> None:
        self._cache_root = cache_root
        self._maximum_bytes = maximum_bytes
        self._timeout = timeout_seconds
        self._client = client

    def materialize(self, source: ToolchainSource) -> Path:
        if source.archive_bytes > self._maximum_bytes:
            raise SourceDownloadError("catalogued archive exceeds download policy")
        root = trusted_root(self._cache_root, create=True)
        suffix = ".zip" if source.archive_format == "zip" else ".tar"
        destination = contained(
            root, root / f"{source.spec.archive_sha256}{suffix}", must_exist=False
        )
        if destination.exists():
            if _trusted_cached_file(destination, source):
                return destination
            raise SourceDownloadError("content-addressed download cache is corrupted")
        temporary = contained(root, root / f".{uuid.uuid4().hex}.download", must_exist=False)
        client = self._client or httpx.Client(timeout=self._timeout, follow_redirects=False)
        owns_client = self._client is None
        try:
            self._download(client, source, temporary)
            temporary.chmod(0o444)
            os.replace(temporary, destination)
            return destination
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            if owns_client:
                client.close()

    def _download(self, client: httpx.Client, source: ToolchainSource, temporary: Path) -> None:
        allowed_hosts = {
            str(urlsplit(source.url).hostname),
            *source.redirect_hosts,
        }
        url = source.url
        response: httpx.Response | None = None
        for _redirect in range(6):
            try:
                request = client.build_request("GET", url, headers={"Accept-Encoding": "identity"})
                streamed = client.send(request, stream=True)
            except httpx.HTTPError as error:
                raise SourceDownloadError("trusted source download failed") from error
            if streamed.status_code in {301, 302, 303, 307, 308}:
                location = streamed.headers.get("location")
                streamed.close()
                if location is None:
                    raise SourceDownloadError("trusted source redirect is malformed")
                candidate = urljoin(url, location)
                try:
                    _trusted_https_url(candidate)
                except SourceCatalogError as error:
                    raise SourceDownloadError("trusted source redirect is unsafe") from error
                if urlsplit(candidate).hostname not in allowed_hosts:
                    raise SourceDownloadError("trusted source redirected to an unapproved host")
                url = candidate
                continue
            response = streamed
            break
        if response is None or response.is_redirect:
            raise SourceDownloadError("trusted source exceeded redirect policy")
        try:
            response.raise_for_status()
            digest = hashlib.sha256()
            total = 0
            with temporary.open("xb") as output:
                for chunk in response.iter_bytes(1024 * 1024):
                    total += len(chunk)
                    if total > self._maximum_bytes or total > source.archive_bytes:
                        raise SourceDownloadError("trusted source exceeded approved size")
                    digest.update(chunk)
                    output.write(chunk)
            if total != source.archive_bytes:
                raise SourceDownloadError("trusted source size does not match catalogue")
            if digest.hexdigest() != source.spec.archive_sha256:
                raise SourceDownloadError("trusted source hash does not match catalogue")
        except httpx.HTTPError as error:
            raise SourceDownloadError("trusted source download failed") from error
        finally:
            response.close()


def _trusted_cached_file(path: Path, source: ToolchainSource) -> bool:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not path.is_file() or metadata.st_mode & 0o022:
            return False
        if metadata.st_size != source.archive_bytes:
            return False
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest() == source.spec.archive_sha256
    except OSError:
        return False
