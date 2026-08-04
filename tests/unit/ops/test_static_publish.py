"""Atomic and contained static publication."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from vuzol.ops import static_publish
from vuzol.ops.static_publish import StaticPublishError, StaticSite, load_sites, publish, rollback


@pytest.fixture
def roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    sources = tmp_path / "repositories"
    sites = tmp_path / "sites"
    sources.mkdir()
    sites.mkdir()
    monkeypatch.setattr(static_publish, "SOURCE_ROOT", sources)
    monkeypatch.setattr(static_publish, "SITE_ROOT", sites)
    return sources, sites


def test_publish_is_atomic_idempotent_and_rollbackable(roots: tuple[Path, Path]) -> None:
    sources, sites = roots
    source = sources / "bill-buddy"
    source.mkdir()
    (source / "index.html").write_text("first")
    (source / "app.js").write_text("console.log(1)")
    destination_parent = sites / "hryshyn.dev"
    destination_parent.mkdir()
    site = StaticSite(
        id="bill-buddy",
        source=source,
        destination=destination_parent / "bill-buddy",
        include=(Path("index.html"), Path("app.js")),
    )

    first = publish(site)
    repeated = publish(site)
    assert first.changed is True
    assert repeated.changed is False
    assert (site.destination / "current" / "index.html").read_text() == "first"

    (source / "index.html").write_text("second")
    second = publish(site)
    assert second.changed is True and second.release != first.release
    assert (site.destination / "current" / "index.html").read_text() == "second"

    restored = rollback(site)
    assert restored.release == first.release
    assert (site.destination / "current" / "index.html").read_text() == "first"


def test_publish_rejects_symlinked_allowlisted_content(
    roots: tuple[Path, Path], tmp_path: Path
) -> None:
    sources, sites = roots
    source = sources / "unsafe"
    source.mkdir()
    outside = tmp_path / "secret"
    outside.write_text("secret")
    (source / "index.html").symlink_to(outside)
    parent = sites / "hryshyn.dev"
    parent.mkdir()
    site = StaticSite("unsafe", source, parent / "unsafe", (Path("index.html"),))
    with pytest.raises(StaticPublishError, match="unsafe"):
        publish(site)


def test_load_sites_is_strict_and_rejects_destination_escape(
    roots: tuple[Path, Path], tmp_path: Path
) -> None:
    sources, _sites = roots
    source = sources / "project"
    source.mkdir()
    config = tmp_path / "sites.json"
    config.write_text(
        json.dumps(
            {
                "sites": [
                    {
                        "id": "project",
                        "source": str(source),
                        "destination": str(tmp_path / "outside" / "project"),
                        "include": ["index.html"],
                    }
                ]
            }
        )
    )
    site = load_sites(config)[0]
    with pytest.raises(StaticPublishError, match="destination escapes"):
        publish(site)


def test_load_sites_rejects_unknown_fields(tmp_path: Path) -> None:
    config = tmp_path / "sites.json"
    config.write_text(json.dumps({"sites": [{"id": "x", "secret": "no"}]}))
    with pytest.raises(StaticPublishError, match="unsupported fields"):
        load_sites(config)


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"sites": {}, "extra": True},
        {"sites": [{"id": "x"}]},
        {"sites": [{"id": "bad/id", "source": "/x", "destination": "/y", "include": ["x"]}]},
        {"sites": [{"id": "x", "source": "relative", "destination": "/y", "include": ["x"]}]},
        {"sites": [{"id": "x", "source": "/x", "destination": "/y", "include": []}]},
        {"sites": [{"id": "x", "source": "/x", "destination": "/y", "include": ["../x"]}]},
        {
            "sites": [
                {
                    "id": "x",
                    "source": "/x",
                    "destination": "/y",
                    "include": ["x"],
                    "keep_releases": 1,
                }
            ]
        },
    ],
)
def test_load_sites_rejects_invalid_documents(tmp_path: Path, payload: object) -> None:
    config = tmp_path / "sites.json"
    config.write_text(json.dumps(payload))
    with pytest.raises(StaticPublishError):
        load_sites(config)


def test_load_sites_rejects_unreadable_and_duplicate_configuration(tmp_path: Path) -> None:
    with pytest.raises(StaticPublishError, match="unreadable"):
        load_sites(tmp_path / "missing.json")

    config = tmp_path / "sites.json"
    item = {"id": "same", "source": "/x", "destination": "/y", "include": ["index.html"]}
    config.write_text(json.dumps({"sites": [item, item]}))
    with pytest.raises(StaticPublishError, match="unique"):
        load_sites(config)


def test_publish_requires_entrypoint_and_cleans_staging(roots: tuple[Path, Path]) -> None:
    sources, sites = roots
    source = sources / "missing-entrypoint"
    source.mkdir()
    (source / "app.js").write_text("ok")
    parent = sites / "example"
    parent.mkdir()
    site = StaticSite("missing", source, parent / "missing", (Path("app.js"),))

    with pytest.raises(StaticPublishError, match="entrypoint"):
        publish(site)
    assert not tuple((site.destination / "releases").glob(".staging-*"))


def test_publish_enforces_file_limit_and_skips_missing_allowlist(
    roots: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    sources, sites = roots
    source = sources / "large"
    source.mkdir()
    (source / "index.html").write_text("ok")
    parent = sites / "example"
    parent.mkdir()
    site = StaticSite(
        "large",
        source,
        parent / "large",
        (Path("not-there"), Path("index.html")),
    )
    monkeypatch.setattr(static_publish, "MAX_FILES", 0)
    with pytest.raises(StaticPublishError, match="size limits"):
        publish(site)


def test_publish_rejects_symlinked_directory_and_special_file(
    roots: tuple[Path, Path], tmp_path: Path
) -> None:
    sources, sites = roots
    source = sources / "unsafe-tree"
    source.mkdir()
    outside = tmp_path / "outside-tree"
    outside.mkdir()
    (outside / "index.html").write_text("secret")
    (source / "linked").symlink_to(outside, target_is_directory=True)
    parent = sites / "example"
    parent.mkdir()
    linked = StaticSite(
        "linked-tree",
        source,
        parent / "linked-tree",
        (Path("."),),
        entrypoint=Path("linked/index.html"),
    )
    with pytest.raises(StaticPublishError, match="symlink directory"):
        publish(linked)

    special_source = sources / "special-tree"
    special_source.mkdir()
    fifo = special_source / "pipe"
    os.mkfifo(fifo)
    special = StaticSite(
        "special-file",
        special_source,
        parent / "special-file",
        (Path("."),),
        entrypoint=Path("pipe"),
    )
    with pytest.raises(StaticPublishError, match="regular files"):
        publish(special)


def test_rollback_requires_an_active_and_previous_release(roots: tuple[Path, Path]) -> None:
    sources, sites = roots
    source = sources / "single"
    source.mkdir()
    (source / "index.html").write_text("only")
    parent = sites / "example"
    parent.mkdir()
    site = StaticSite("single", source, parent / "single", (Path("index.html"),))

    with pytest.raises(StaticPublishError, match="active release"):
        rollback(site)
    publish(site)
    with pytest.raises(StaticPublishError, match="no previous release"):
        rollback(site)

    outside = sources / "outside-release"
    outside.mkdir()
    current = site.destination / "current"
    current.unlink()
    current.symlink_to(outside)
    with pytest.raises(StaticPublishError, match="escapes"):
        rollback(site)


def test_publish_rejects_source_and_destination_boundary_violations(
    roots: tuple[Path, Path], tmp_path: Path
) -> None:
    sources, sites = roots
    outside = tmp_path / "outside-source"
    outside.mkdir()
    (outside / "index.html").write_text("no")
    destination_parent = sites / "example"
    destination_parent.mkdir()
    escaped_source = StaticSite(
        "escaped-source",
        outside,
        destination_parent / "escaped-source",
        (Path("index.html"),),
    )
    with pytest.raises(StaticPublishError, match="source escapes"):
        publish(escaped_source)

    missing_parent = StaticSite(
        "missing-parent",
        sources,
        sites / "missing" / "nested",
        (Path("index.html"),),
    )
    with pytest.raises(StaticPublishError, match="parent does not exist"):
        publish(missing_parent)

    source_file = sources / "not-a-directory"
    source_file.write_text("no")
    invalid_source = StaticSite(
        "source-file",
        source_file,
        destination_parent / "source-file",
        (Path("index.html"),),
    )
    with pytest.raises(StaticPublishError, match="real directory"):
        publish(invalid_source)

    destination_file = destination_parent / "destination-file"
    destination_file.write_text("occupied")
    invalid_destination = StaticSite(
        "destination-file",
        sources,
        destination_file,
        (Path("index.html"),),
    )
    with pytest.raises(StaticPublishError, match="real directory"):
        publish(invalid_destination)


def test_static_publish_main_selects_publish_and_rollback(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    first = StaticSite("first", Path("/source"), Path("/site"), (Path("index.html"),))
    second = StaticSite("second", Path("/source2"), Path("/site2"), (Path("index.html"),))
    monkeypatch.setattr(static_publish, "load_sites", lambda _path: (first, second))
    published = static_publish.PublishResult("first", "abc", True, 1, 2)
    publish_mock = pytest.MonkeyPatch()
    calls: list[str] = []

    def record_publish(site: StaticSite) -> static_publish.PublishResult:
        calls.append("publish:" + site.id)
        return published

    def record_rollback(site: StaticSite) -> static_publish.PublishResult:
        calls.append("rollback:" + site.id)
        return published

    monkeypatch.setattr(static_publish, "publish", record_publish)
    monkeypatch.setattr(static_publish, "rollback", record_rollback)
    monkeypatch.setattr("sys.argv", ["vuzol-static-publish", "--site", "first"])
    static_publish.main()
    assert calls == ["publish:first"]
    assert '"release": "abc"' in capsys.readouterr().out

    monkeypatch.setattr("sys.argv", ["vuzol-static-publish", "--site", "first", "--rollback"])
    static_publish.main()
    assert calls[-1] == "rollback:first"

    monkeypatch.setattr("sys.argv", ["vuzol-static-publish", "--site", "missing"])
    with pytest.raises(SystemExit, match="unknown static site"):
        static_publish.main()
    publish_mock.undo()
