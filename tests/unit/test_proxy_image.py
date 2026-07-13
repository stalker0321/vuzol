"""Static invariants for the hardened CONNECT proxy image."""

from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]
DOCKERFILE = REPO_ROOT / "Dockerfile.proxy"


def _content() -> str:
    return DOCKERFILE.read_text()


def test_proxy_image_uses_digest_pinned_python_runtime() -> None:
    content = _content()
    assert "FROM python:3.12.10-alpine3.20@sha256:" in content
    assert "FROM python:3.12.10-alpine3.20\n" not in content


def test_proxy_image_runs_as_dedicated_numeric_non_root() -> None:
    content = _content()
    assert "addgroup -S -g 10002 vuzol-proxy" in content
    assert "adduser -S -D -H -u 10002 -G vuzol-proxy" in content
    assert "USER 10002:10002" in content
    assert "USER root" not in content


def test_proxy_image_has_direct_isolated_entrypoint() -> None:
    content = _content()
    assert 'ENTRYPOINT ["python", "-I", "/opt/vuzol/connect_proxy.py"]' in content
    assert 'CMD ["--policy", "/etc/vuzol-proxy/policy.json"]' in content
    assert "sh -c" not in content.lower()


def test_proxy_image_copies_only_auditable_connector_code() -> None:
    copy_lines = [line for line in _content().splitlines() if line.startswith(("COPY", "ADD"))]
    assert copy_lines == [
        "COPY --chown=10002:10002 src/vuzol/execution/connect_proxy.py /opt/vuzol/connect_proxy.py"
    ]
    forbidden = (".env", "secret", "token", "auth", ".git", "vuzol-local", "credential")
    assert not any(item in copy_lines[0].lower() for item in forbidden)


def test_proxy_image_has_no_docker_socket_or_runtime_bootstrap() -> None:
    content = _content()
    assert "docker.sock" not in content.lower()
    assert "curl" not in content
    assert "wget" not in content
    assert "apk add" not in content


def test_proxy_image_does_not_swallow_identity_setup_failures() -> None:
    commands = "\n".join(
        line for line in _content().splitlines() if not line.strip().startswith("#")
    )
    assert "|| true" not in commands
    assert "2>/dev/null" not in commands
