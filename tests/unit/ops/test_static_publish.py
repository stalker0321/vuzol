"""Atomic and contained static publication."""

from __future__ import annotations

import json
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
    with pytest.raises(StaticPublishError, match="missing or unsafe"):
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
