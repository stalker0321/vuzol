"""Unit tests for the transport-neutral task application service (WP09)."""

import pytest

from vuzol.workflows.application import (
    INGRESS_SOURCES,
    Principal,
    TaskCommand,
    validate_command,
    validate_principal,
)


def test_principal_rejects_fake_ids_and_unknown_ingress() -> None:
    validate_principal(Principal(user_id=42, ingress_source="cli"))
    assert frozenset({"telegram", "cli", "api"}) == INGRESS_SOURCES
    with pytest.raises(ValueError, match="principal_invalid"):
        validate_principal(Principal(user_id=0, ingress_source="cli"))
    with pytest.raises(ValueError, match="principal_invalid"):
        validate_principal(Principal(user_id=42, ingress_source="carrier-pigeon"))


def test_unknown_command_rejected() -> None:
    assert validate_command("pause") is TaskCommand.PAUSE
    assert validate_command(TaskCommand.INSPECT) is TaskCommand.INSPECT
    with pytest.raises(ValueError, match="unknown task command"):
        validate_command("self-destruct")


def test_command_vocabulary_covers_lifecycle() -> None:
    assert {command.value for command in TaskCommand} == {
        "start",
        "pause",
        "cancel",
        "resume",
        "inspect",
    }
