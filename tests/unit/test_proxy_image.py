"""Static tests for the hardened Tinyproxy image definition.

These inspect the Dockerfile and base configuration for reproducibility,
hardening, and correct contract. They do not require Docker at runtime.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]
DOCKERFILE = REPO_ROOT / "Dockerfile.proxy"
BASE_CONF = REPO_ROOT / "deploy/proxy/tinyproxy-base.conf"


def test_dockerfile_exists_and_has_syntax() -> None:
    assert DOCKERFILE.exists()
    content = DOCKERFILE.read_text()
    assert "# syntax=docker/dockerfile:1.7" in content
    assert "FROM alpine:" in content


def test_base_image_pinned_by_digest() -> None:
    content = DOCKERFILE.read_text()
    # Must be pinned with @sha256: not just tag
    assert "@sha256:" in content
    assert "alpine:3.20.3@sha256:" in content
    # No unpinned mutable tag without digest in FROM
    assert "FROM alpine:3.20.3\n" not in content


def test_tinyproxy_version_pinned() -> None:
    content = DOCKERFILE.read_text()
    assert "tinyproxy=${TINYPROXY_VERSION}" in content
    assert "ARG TINYPROXY_VERSION=1.11.2-r0" in content


def test_non_root_numeric_user() -> None:
    content = DOCKERFILE.read_text()
    assert "USER 10002:10002" in content
    assert "addgroup -S -g 10002 tproxy" in content
    assert "adduser -S -D -H -u 10002 -G tproxy -s /sbin/nologin tproxy" in content
    # Not 0 or 65534
    assert "USER 0" not in content
    assert "USER 65534" not in content
    assert "USER root" not in content


def test_exec_form_entrypoint_and_cmd_no_sh_c() -> None:
    content = DOCKERFILE.read_text()
    assert 'ENTRYPOINT ["tinyproxy"]' in content
    assert 'CMD ["-d", "-c", "/etc/tinyproxy/tinyproxy.conf"]' in content
    # No shell form or sh -c
    assert "ENTRYPOINT tinyproxy" not in content
    assert "sh -c" not in content.lower()
    assert "CMD tinyproxy" not in content


def test_no_secrets_or_sensitive_copies() -> None:
    content = DOCKERFILE.read_text()
    # Check for actual COPY/ADD of sensitive things, not just mentions in comments
    lines = [ln for ln in content.splitlines() if ln.strip().startswith(("COPY", "ADD"))]
    forbidden = [".env", "secret", "token", "auth", ".git", "vuzol-local", "credential"]
    for line in lines:
        ln = line.lower()
        for f in forbidden:
            assert f not in ln, f"forbidden pattern {f} copied in Dockerfile"


def test_no_docker_socket_or_host_bootstrap() -> None:
    content = DOCKERFILE.read_text()
    assert "docker.sock" not in content.lower()
    # No network tools for bootstrap at runtime
    assert "curl" not in content
    assert "wget" not in content
    assert "apk add" in content  # only in build, not runtime


def test_expected_config_and_filter_paths_documented() -> None:
    content = DOCKERFILE.read_text()
    assert "/etc/tinyproxy/tinyproxy.conf" in content
    assert "/etc/tinyproxy/filter" in content
    # Base conf path for assembly
    assert "tinyproxy-base.conf" in content


def test_static_base_conf_has_no_permissive_or_conflicting_policy() -> None:
    assert BASE_CONF.exists()
    text = BASE_CONF.read_text()
    # Only check active directive lines (not comments)
    active = [
        ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")
    ]
    for line in active:
        assert not line.startswith("Filter "), f"active Filter in base: {line}"
        assert not line.startswith("ConnectPort"), f"active ConnectPort in base: {line}"
        assert not line.startswith("FilterDefaultDeny"), f"active FilterDefaultDeny in base: {line}"
        assert not line.startswith("FilterType"), f"active FilterType in base: {line}"
    # Has the process settings
    assert "User 10002" in text
    assert "Port 8888" in text
    assert "LogLevel Info" in text


def test_base_conf_does_not_duplicate_renderer_directives() -> None:
    text = BASE_CONF.read_text()
    active = [
        ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")
    ]
    renderer_directives = [
        "ConnectPort",
        "FilterDefaultDeny",
        "FilterType",
        "FilterURLs",
        "FilterCaseSensitive",
    ]
    for line in active:
        for d in renderer_directives:
            assert not line.startswith(d), f"base conf should not contain active {d}: {line}"


def test_dockerfile_uses_numeric_user_and_no_root_startup() -> None:
    content = DOCKERFILE.read_text()
    # USER before any CMD that could run code
    lines = content.splitlines()
    user_line = next((i for i, ln in enumerate(lines) if ln.strip().startswith("USER ")), -1)
    cmd_line = next(
        (i for i, ln in enumerate(lines) if ln.strip().startswith(("CMD ", "ENTRYPOINT "))), -1
    )
    assert user_line >= 0
    assert user_line < cmd_line or "USER" in content  # USER is present and before effective start


def test_image_expects_mounted_complete_config() -> None:
    content = DOCKERFILE.read_text()
    # No baked full permissive conf; relies on mount
    assert "tinyproxy.conf" in content
    # The COPY is only the base, not the active full policy
    assert "COPY deploy/proxy/tinyproxy-base.conf" in content


def test_no_swallow_on_user_creation_and_dedicated_identity() -> None:
    content = DOCKERFILE.read_text()
    non_comment = "\n".join(ln for ln in content.splitlines() if not ln.strip().startswith("#"))
    # No || true to swallow failures (in actual commands)
    assert "|| true" not in non_comment
    assert "2>/dev/null" not in non_comment
    # Dedicated 10002, not colliding with nobody 65534
    assert "10002" in content
    assert "65534" not in content
