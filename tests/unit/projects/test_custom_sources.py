import uuid

import pytest

from vuzol.projects.custom_sources import (
    AddSourceCommand,
    CustomSourceError,
    RemoveSourceCommand,
    is_source_command,
    parse_source_command,
)


def test_add_git_source_is_exact_and_user_parseable() -> None:
    command = parse_source_command(
        "/source add python demo git https://github.com/example/demo.git " + "a" * 40
    )

    assert isinstance(command, AddSourceCommand)
    assert command.package_name == "demo"
    assert command.source_pin == "a" * 40
    assert is_source_command("/source@vuzol_bot remove " + str(uuid.uuid4()))


def test_remove_source_requires_uuid() -> None:
    source_id = uuid.uuid4()

    assert parse_source_command(f"/source remove {source_id}") == RemoveSourceCommand(source_id)
    with pytest.raises(CustomSourceError, match="UUID"):
        parse_source_command("/source remove nope")


@pytest.mark.parametrize(
    "command",
    (
        "/source add rust demo git https://github.com/example/demo.git " + "a" * 40,
        "/source add python demo git http://github.com/example/demo.git " + "a" * 40,
        "/source add python demo git https://127.0.0.1/demo.git " + "a" * 40,
        "/source add python demo git https://github.com/example/demo.git " + "A" * 40,
        "/source add python demo https https://example.com/demo.whl " + "a" * 40,
        "/source add node demo https https://example.com/demo.tgz " + "a" * 64,
    ),
)
def test_source_command_rejects_unbounded_or_unpinned_sources(command: str) -> None:
    with pytest.raises(CustomSourceError):
        parse_source_command(command)
