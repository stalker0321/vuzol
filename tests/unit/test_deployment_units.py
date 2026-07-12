"""Tests for deployment unit invariants (systemd user service layout for rootless Docker).

These are static text checks on the checked-in unit files. They enforce the
repository-side decisions from the Step 08 rootless user-service migration:

- The production daemon unit is a real systemd USER unit.
- No hard-coded numeric UIDs in portable files.
- No reference to the rootful Docker socket.
- Executor unit does not depend on the (now-legacy) system daemon unit name.
- Readiness logic resolves identity dynamically.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
USER_DAEMON_UNIT = REPO_ROOT / "deploy/systemd/user/vuzol-rootless-docker.service"
LEGACY_DAEMON_UNIT = REPO_ROOT / "deploy/systemd/vuzol-rootless-docker.service"
EXECUTOR_UNIT = REPO_ROOT / "deploy/systemd/vuzol-executor.service"


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def test_user_daemon_unit_exists_and_is_user_service() -> None:
    text = _read(USER_DAEMON_UNIT)
    lines = text.splitlines()

    # Must be a real USER unit: no active User= or Group= directives
    # (comments may mention them, so we only reject non-comment active lines)
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        assert not stripped.startswith("User="), f"Found active User= directive: {stripped}"
        assert not stripped.startswith("Group="), f"Found active Group= directive: {stripped}"

    # Uses user-runtime specifier for socket (portable, no numeric UID)
    assert "%t/docker.sock" in text
    assert "unix://%t/docker.sock" in text

    # Does not reference the rootful socket
    assert "/var/run/docker.sock" not in text
    assert '"/var/run/docker.sock"' not in text

    # Uses the expected dockerd-rootless.sh invocation
    assert "dockerd-rootless.sh" in text

    # Bounded restart + readiness probe present
    assert "Restart=always" in text
    assert "ExecStartPost=" in text
    assert "docker --host" in text and "info >" in text

    # Clear indication this is the user unit
    assert "USER UNIT" in text or "user unit" in text.lower() or "linger" in text.lower()


def test_user_daemon_unit_does_not_hardcode_uid() -> None:
    text = _read(USER_DAEMON_UNIT)
    # Absolutely no /run/user/NNN/ paths
    assert "/run/user/994" not in text
    assert "/run/user/1000" not in text
    # No bare numeric UIDs in paths
    assert any(f"/run/user/{n}" in text for n in range(100, 2000)) is False


def test_legacy_unit_is_clearly_labelled_and_not_the_production_definition() -> None:
    text = _read(LEGACY_DAEMON_UNIT)
    assert "LEGACY" in text
    assert "MIGRATION REFERENCE" in text or "DO NOT USE" in text
    assert "deploy/systemd/user/vuzol-rootless-docker.service" in text
    # The legacy file may still contain the old body for rollback reference,
    # but must not be the one we document as current.
    assert "User=vuzol-executor" in text  # old form is present for reference only


def test_executor_unit_no_longer_depends_on_system_daemon_unit() -> None:
    text = _read(EXECUTOR_UNIT)
    lines = text.splitlines()
    # Must not reference the old system daemon unit in After= / Wants= / Requires=
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        dep_directives = ("After=", "Wants=", "Requires=")
        if stripped.startswith(dep_directives):
            assert "vuzol-rootless-docker.service" not in stripped, (
                f"Found old daemon dep in: {stripped}"
            )
    # The comment must explain the new readiness model (string may appear in comments)
    assert "user unit" in text.lower()
    assert "fail-closed" in text.lower() or "preflight" in text.lower()
    assert "ExecStartPre" in text


def test_executor_readiness_resolves_identity_dynamically() -> None:
    text = _read(EXECUTOR_UNIT)
    # Uses id -u (dynamic, as the User= context)
    assert "id -u" in text
    # Prefers the env var (populated at deploy time without hard-coded UID in repo)
    assert "VUZOL_EXECUTION__ROOTLESS_DOCKER_SOCKET" in text
    # Waits for a socket (-S)
    assert "-S " in text or '[ -S "$SOCKET" ]' in text or "-S" in text
    # No numeric UID literal in the readiness logic
    assert "/run/user/994" not in text


def test_no_rootful_socket_anywhere_in_units() -> None:
    for unit in (USER_DAEMON_UNIT, LEGACY_DAEMON_UNIT, EXECUTOR_UNIT):
        text = _read(unit)
        assert "/var/run/docker.sock" not in text
        assert '"/var/run/docker.sock"' not in text


def test_user_daemon_unit_has_delegate_yes_in_service_section() -> None:
    text = _read(USER_DAEMON_UNIT)
    lines = text.splitlines()

    in_service = False
    delegate_line = None
    for line in lines:
        stripped = line.strip()
        if stripped == "[Service]":
            in_service = True
            continue
        if stripped.startswith("[") and stripped != "[Service]":
            in_service = False
            continue
        if in_service and stripped == "Delegate=yes":
            delegate_line = stripped
            break

    assert delegate_line is not None, (
        "Delegate=yes must be present as active directive in [Service]"
    )

    # Also re-assert no User/Group in production user unit (from repair requirements)
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        assert not stripped.startswith("User="), f"Found active User= in user unit: {stripped}"
        assert not stripped.startswith("Group="), f"Found active Group= in user unit: {stripped}"

    # No hard-coded numeric UID path
    assert "/run/user/994" not in text
    assert any(f"/run/user/{n}/" in text for n in range(100, 2000)) is False

    # No rootful
    assert "/var/run/docker.sock" not in text


def test_readiness_loops_are_bounded_and_fail_closed() -> None:
    for unit_path, name in [
        (USER_DAEMON_UNIT, "user-daemon"),
        (EXECUTOR_UNIT, "executor"),
    ]:
        text = _read(unit_path)
        # Must contain a bounded loop (seq or for i + limit) instead of bare infinite until
        has_bound = ("seq 1" in text) or ("for i in" in text and ("150" in text or "seq" in text))
        has_timeout_sec = "TimeoutStartSec" in text
        assert has_bound or has_timeout_sec, (
            f"{name} readiness must be bounded (loop or TimeoutStartSec)"
        )

        # The until must not be the old infinite form without guard
        if "until " in text and "do sleep 0.2; done" in text:
            assert "for i in" in text or "seq" in text, (
                f"{name} must not have unguarded infinite until"
            )

        # Must fail closed on timeout (exit 1 on fail)
        assert "exit 1" in text or "exit 1" in text.replace(" ", ""), (
            f"{name} must fail closed on timeout"
        )
