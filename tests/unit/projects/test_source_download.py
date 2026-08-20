import hashlib
from pathlib import Path

import httpx
import pytest

from vuzol.projects.source_catalog import ToolchainSource
from vuzol.projects.source_download import SourceDownloadError, TrustedSourceDownloader
from vuzol.projects.toolchains import ToolchainSpec


def _source(content: bytes, *, redirects: tuple[str, ...] = ()) -> ToolchainSource:
    return ToolchainSource(
        spec=ToolchainSpec(
            capability_key="go-toolchain",
            version="1.2.3",
            archive_sha256=hashlib.sha256(content).hexdigest(),
            executables=(("go", "go/bin/go"),),
        ),
        provider="Example",
        url="https://downloads.example.com/go.tar.gz",
        redirect_hosts=redirects,
        archive_bytes=len(content),
        archive_format="tar",
    )


def test_download_is_hash_bound_atomic_and_cached(tmp_path: Path) -> None:
    content = b"trusted archive"
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=content, request=request)

    downloader = TrustedSourceDownloader(
        tmp_path / "cache",
        maximum_bytes=1_000,
        client=httpx.Client(transport=httpx.MockTransport(respond)),
    )

    first = downloader.materialize(_source(content))
    second = downloader.materialize(_source(content))

    assert first == second
    assert first.read_bytes() == content
    assert first.stat().st_mode & 0o222 == 0
    assert calls == 1


def test_download_accepts_only_catalogued_redirect_hosts(tmp_path: Path) -> None:
    content = b"archive"

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "downloads.example.com":
            return httpx.Response(
                302,
                headers={"location": "https://assets.example.com/go.tar.gz"},
                request=request,
            )
        return httpx.Response(200, content=content, request=request)

    client = httpx.Client(transport=httpx.MockTransport(respond))
    downloader = TrustedSourceDownloader(tmp_path / "cache", maximum_bytes=1_000, client=client)

    with pytest.raises(SourceDownloadError, match="unapproved host"):
        downloader.materialize(_source(content))
    assert downloader.materialize(_source(content, redirects=("assets.example.com",))).is_file()


@pytest.mark.parametrize("failure", ("hash", "size", "maximum"))
def test_download_fails_closed_on_content_mismatch(tmp_path: Path, failure: str) -> None:
    content = b"archive"
    source = _source(content)
    if failure == "hash":
        source = ToolchainSource(
            spec=ToolchainSpec(
                capability_key="go-toolchain",
                version="1.2.3",
                archive_sha256="a" * 64,
                executables=(("go", "go/bin/go"),),
            ),
            provider=source.provider,
            url=source.url,
            redirect_hosts=(),
            archive_bytes=len(content),
            archive_format="tar",
        )
    elif failure == "size":
        source = ToolchainSource(
            spec=source.spec,
            provider=source.provider,
            url=source.url,
            redirect_hosts=(),
            archive_bytes=len(content) + 1,
            archive_format="tar",
        )
    maximum = len(content) - 1 if failure == "maximum" else 1_000
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=content, request=request)
        )
    )
    downloader = TrustedSourceDownloader(tmp_path / "cache", maximum_bytes=maximum, client=client)

    with pytest.raises(SourceDownloadError):
        downloader.materialize(source)
    assert not tuple((tmp_path / "cache").glob("*.tar"))
