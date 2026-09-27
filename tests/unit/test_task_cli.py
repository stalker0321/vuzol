"""Unit tests for the vuzol-task operator CLI (WP09)."""

import uuid

import pytest

from vuzol.cli.task import _parse_args


def test_parse_commands_and_ids() -> None:
    task_id = str(uuid.uuid4())
    args = _parse_args(["pause", "--task-id", task_id, "--user-id", "7"])
    assert args.command == "pause"
    assert args.task_id == task_id
    assert args.user_id == 7
    assert args.expected_version is None
    args = _parse_args(
        ["inspect", "--task-id", task_id, "--user-id", "7", "--expected-version", "3"]
    )
    assert args.command == "inspect" and args.expected_version == 3


def test_parse_rejects_unknown_command() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["self-destruct", "--task-id", str(uuid.uuid4()), "--user-id", "7"])


def test_parse_requires_task_and_user() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["pause", "--user-id", "7"])
    with pytest.raises(SystemExit):
        _parse_args(["pause", "--task-id", str(uuid.uuid4())])
