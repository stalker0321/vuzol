import hashlib
import json
import uuid
from pathlib import Path

import pytest

from vuzol.execution.codex import _apply_dependency_environment
from vuzol.projects.custom_sources import CustomDependencySource
from vuzol.projects.dependencies import (
    DEPENDENCY_RECEIPT,
    DependencyEnvironment,
    DependencyError,
    dependency_environment_path,
    inspect_dependency_requests,
    load_dependency_environment,
)
from vuzol.projects.source_catalog import SourceCatalog


def test_python_and_node_manifests_become_hash_bound_registry_requests(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname="demo"\ndependencies=["fastapi>=1", "httpx==2"]\n'
    )
    (tmp_path / "package.json").write_text(
        json.dumps({"dependencies": {"react": "^19.0.0"}, "devDependencies": {"vite": "7.0.0"}})
    )

    requests = inspect_dependency_requests(
        tmp_path, SourceCatalog.builtin(), maximum_direct_dependencies=10
    )

    assert [request.ecosystem for request in requests] == ["python", "node"]
    assert requests[0].direct_dependencies == ("fastapi>=1", "httpx==2")
    assert requests[1].direct_dependencies == ("react@^19.0.0", "vite@7.0.0")
    assert len(requests[0].environment_key) == 64


@pytest.mark.parametrize(
    ("name", "content"),
    (
        ("pyproject.toml", '[project]\ndependencies=["demo @ https://evil.test/x.whl"]'),
        ("package.json", '{"dependencies":{"demo":"git+https://evil.test/repo"}}'),
    ),
)
def test_registry_request_rejects_custom_dependency_sources(
    tmp_path: Path, name: str, content: str
) -> None:
    (tmp_path / name).write_text(content)

    with pytest.raises(DependencyError, match="custom source"):
        inspect_dependency_requests(
            tmp_path, SourceCatalog.builtin(), maximum_direct_dependencies=10
        )


@pytest.mark.parametrize(
    ("ecosystem", "manifest_name", "content", "package"),
    (
        (
            "python",
            "pyproject.toml",
            '[project]\ndependencies=["demo @ git+https://github.com/acme/demo.git@'
            + "a" * 40
            + '"]',
            "demo",
        ),
        (
            "node",
            "package.json",
            json.dumps(
                {"dependencies": {"@acme/demo": "git+https://github.com/acme/demo.git#" + "a" * 40}}
            ),
            "@acme/demo",
        ),
    ),
)
def test_user_registered_git_source_is_bound_into_dependency_request(
    tmp_path: Path,
    ecosystem: str,
    manifest_name: str,
    content: str,
    package: str,
) -> None:
    (tmp_path / manifest_name).write_text(content)
    source = CustomDependencySource(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        project_id="demo-project",
        ecosystem=ecosystem,
        package_name=package,
        source_kind="git",
        source_url="https://github.com/acme/demo.git",
        source_pin="a" * 40,
    )

    request = inspect_dependency_requests(
        tmp_path,
        SourceCatalog.builtin(),
        maximum_direct_dependencies=10,
        custom_sources=(source,),
    )[0]

    assert request.custom_sources == (source,)
    assert "github.com" in request.registry_hosts
    assert request.approval_record()["custom_sources"] == [source.approval_record()]


def test_unregistered_pinned_source_returns_copyable_registration_hint(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[project]\ndependencies=["demo @ git+https://github.com/acme/demo.git@' + "a" * 40 + '"]'
    )

    with pytest.raises(DependencyError, match=r"/source add python demo git"):
        inspect_dependency_requests(
            tmp_path, SourceCatalog.builtin(), maximum_direct_dependencies=10
        )


def test_registered_python_https_artifact_requires_exact_sha256(tmp_path: Path) -> None:
    digest = "b" * 64
    url = "https://packages.example.com/demo-1.0-py3-none-any.whl"
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\ndependencies=["demo @ {url}#sha256={digest}"]'
    )
    source = CustomDependencySource(
        id=uuid.uuid4(),
        project_id="demo-project",
        ecosystem="python",
        package_name="demo",
        source_kind="https",
        source_url=url,
        source_pin=digest,
    )

    request = inspect_dependency_requests(
        tmp_path,
        SourceCatalog.builtin(),
        maximum_direct_dependencies=10,
        custom_sources=(source,),
    )[0]

    assert request.custom_sources == (source,)
    assert "packages.example.com" in request.registry_hosts


def test_immutable_environment_receipt_is_reused_and_fails_closed(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / "pyproject.toml").write_text('[project]\ndependencies=["fastapi"]')
    request = inspect_dependency_requests(
        worktree, SourceCatalog.builtin(), maximum_direct_dependencies=10
    )[0]
    storage = tmp_path / "environments"
    target = dependency_environment_path(storage, "demo-project", request)
    (target / "venv").mkdir(parents=True)
    lock = target / "uv.lock"
    lock.write_bytes(b"locked")
    environment = DependencyEnvironment(
        request=request,
        root=target,
        lockfile_name="uv.lock",
        lockfile_sha256=hashlib.sha256(b"locked").hexdigest(),
    )
    receipt = target / DEPENDENCY_RECEIPT
    receipt.write_text(json.dumps(environment.receipt()))
    receipt.chmod(0o444)

    assert load_dependency_environment(storage, "demo-project", request) == environment
    lock.write_bytes(b"changed")
    assert load_dependency_environment(storage, "demo-project", request) is None


def test_approved_python_environment_maps_only_site_packages(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / "pyproject.toml").write_text('[project]\ndependencies=["fastapi"]')
    request = inspect_dependency_requests(
        worktree, SourceCatalog.builtin(), maximum_direct_dependencies=10
    )[0]
    root = tmp_path / "environment"
    (root / "venv" / "lib" / "python3.12" / "site-packages").mkdir(parents=True)
    environment = DependencyEnvironment(request, root, "uv.lock", "a" * 64)
    variables: dict[str, str] = {}

    _apply_dependency_environment(variables, environment, Path("/dependencies/python"))

    assert variables == {
        "PYTHONPATH": ("/dependencies/python/venv/lib/python3.12/site-packages:/workspace/src")
    }
