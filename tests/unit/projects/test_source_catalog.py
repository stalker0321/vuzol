import copy
import json

import pytest

from vuzol.projects.source_catalog import SourceCatalog, SourceCatalogError, parse_source_catalog


def _raw_catalog() -> dict[str, object]:
    return {
        "schema_version": "vuzol-source-catalog.v1",
        "toolchains": [
            {
                "capability_key": "go-toolchain",
                "version": "1.2.3",
                "provider": "Example",
                "url": "https://downloads.example.com/go.tar.gz",
                "redirect_hosts": ["assets.example.com"],
                "sha256": "a" * 64,
                "bytes": 123,
                "archive_format": "tar",
                "executables": {"go": "go/bin/go"},
                "environment": {"GOROOT": "go"},
            }
        ],
        "registries": [
            {
                "ecosystem": "python",
                "provider": "PyPA",
                "hosts": ["pypi.org", "files.pythonhosted.org"],
                "manifests": ["pyproject.toml"],
                "lockfiles": ["uv.lock"],
            }
        ],
    }


def test_builtin_catalogue_exposes_pinned_toolchains_and_registries() -> None:
    catalog = SourceCatalog.builtin()

    go = catalog.toolchain("go-toolchain")
    assert go is not None
    assert go.spec.version == "1.27.0"
    assert (
        go.spec.archive_sha256 == "675c26c449cbb18fc24b74650de1eabbae6e16f64326fd85a283fb3b58280685"
    )
    assert catalog.registry("python").hosts == ("pypi.org", "files.pythonhosted.org")  # type: ignore[union-attr]
    node = catalog.toolchain("node-runtime")
    assert node is not None
    assert dict(node.spec.executables)["npm"].endswith("npm/bin/npm-cli.js")
    assert catalog.registry("unknown") is None


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("schema_version",), "unknown"),
        (("toolchains", 0, "url"), "http://downloads.example.com/go.tar.gz"),
        (("toolchains", 0, "url"), "https://127.0.0.1/go.tar.gz"),
        (("toolchains", 0, "redirect_hosts"), ["localhost"]),
        (("toolchains", 0, "archive_format"), "shell"),
        (("registries", 0, "hosts"), ["*.example.com"]),
        (("registries", 0, "manifests"), ["../pyproject.toml"]),
    ),
)
def test_catalogue_rejects_unsafe_source_policy(path: tuple[str | int, ...], value: object) -> None:
    raw = copy.deepcopy(_raw_catalog())
    target: object = raw
    for part in path[:-1]:
        target = target[part]  # type: ignore[index]
    target[path[-1]] = value  # type: ignore[index]

    with pytest.raises(SourceCatalogError):
        parse_source_catalog(raw)


def test_catalogue_round_trip_is_json_data() -> None:
    raw = _raw_catalog()

    catalog = parse_source_catalog(json.loads(json.dumps(raw)))

    assert catalog.toolchain("go-toolchain") is not None
