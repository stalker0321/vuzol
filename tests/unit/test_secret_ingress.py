import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import HttpUrl, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.settings import SecretIngressSettings
from vuzol.security.secret_ingress import (
    _atomic_write,
    cancel_request,
    consume_request,
    create_request,
    inspect_request,
    parse_secret_command,
    token_hash,
)

SECRET_NAME = "TOKENROUTER_API_KEY"  # noqa: S105  # pragma: allowlist secret
TEST_TOKEN = "opaque-test-token"  # noqa: S105 -- synthetic test input


def _session(*, scalar: object = None) -> AsyncSession:
    session = MagicMock(spec=AsyncSession)
    session.scalar = AsyncMock(return_value=scalar)
    session.flush = AsyncMock()
    return cast(AsyncSession, session)


def _configured(tmp_path: Path) -> SecretIngressSettings:
    return SecretIngressSettings(
        enabled=True,
        public_base_url=HttpUrl("https://vuzol.example"),
        storage_root=tmp_path,
        allowed_names=(SECRET_NAME,),
        maximum_secret_bytes=8,
    )


def test_secret_command_and_token_hash_are_strict() -> None:
    assert parse_secret_command("/secret TOKENROUTER_API_KEY") == "TOKENROUTER_API_KEY"
    assert parse_secret_command("/secret@vuzol_bot OPENAI_API_KEY") == "OPENAI_API_KEY"
    assert parse_secret_command("/secret key=value") is None
    assert parse_secret_command("hello") is None
    assert token_hash("opaque-token") != "opaque-token"
    assert len(token_hash("opaque-token")) == 64


def test_enabled_secret_ingress_requires_portable_explicit_configuration(
    tmp_path: Path,
) -> None:
    configured = SecretIngressSettings(
        enabled=True,
        public_base_url=HttpUrl("https://vuzol.example"),
        storage_root=tmp_path,
        allowed_names=("TOKENROUTER_API_KEY",),
    )
    assert str(configured.public_base_url) == "https://vuzol.example/"
    with pytest.raises(ValidationError):
        SecretIngressSettings(enabled=True, storage_root=tmp_path)
    with pytest.raises(ValidationError):
        SecretIngressSettings(allowed_names=("bad-name",))


def test_atomic_secret_write_replaces_exact_file_without_env_concatenation(
    tmp_path: Path,
) -> None:
    _atomic_write(tmp_path, "TOKENROUTER_API_KEY", b"first")
    _atomic_write(tmp_path, "TOKENROUTER_API_KEY", b"second")

    destination = tmp_path / "TOKENROUTER_API_KEY"
    assert destination.read_bytes() == b"second"
    assert destination.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.anyio
async def test_request_creation_is_allowlisted(tmp_path: Path) -> None:
    settings = _configured(tmp_path)
    session = _session()

    request, token = await create_request(
        session,
        settings,
        secret_name=SECRET_NAME,
        user_id=1,
        chat_id=2,
        thread_id=3,
    )

    assert request.token_hash == token_hash(token)
    assert request.token_hash != token
    with pytest.raises(ValueError):
        await create_request(
            session,
            settings,
            secret_name="OTHER_KEY",  # noqa: S106  # pragma: allowlist secret
            user_id=1,
            chat_id=2,
            thread_id=3,
        )


@pytest.mark.anyio
async def test_cancel_and_inspect_fail_closed() -> None:
    assert await cancel_request(_session(), uuid.uuid4(), user_id=1) is False
    assert await inspect_request(_session(), "token") is None

    foreign = MagicMock(requested_by_user_id=2, status="pending")
    assert await cancel_request(_session(scalar=foreign), uuid.uuid4(), user_id=1) is False
    consumed = MagicMock(requested_by_user_id=1, status="consumed")
    assert await cancel_request(_session(scalar=consumed), uuid.uuid4(), user_id=1) is False

    expired = MagicMock(status="pending", expires_at=datetime.now(UTC) - timedelta(seconds=1))
    assert await inspect_request(_session(scalar=expired), "token") is None
    pending = MagicMock(status="pending", expires_at=datetime.now(UTC) + timedelta(seconds=1))
    assert await inspect_request(_session(scalar=pending), "token") is pending


@pytest.mark.anyio
async def test_consume_rejects_bad_values_and_unavailable_request(tmp_path: Path) -> None:
    settings = _configured(tmp_path)
    for value in (b"", b"123456789", b"bad\x00value"):
        with pytest.raises(ValueError):
            await consume_request(_session(), settings, token=TEST_TOKEN, value=value)

    with pytest.raises(LookupError):
        await consume_request(_session(), settings, token=TEST_TOKEN, value=b"valid")
    expired = MagicMock(status="pending", expires_at=datetime.now(UTC) - timedelta(seconds=1))
    with pytest.raises(LookupError):
        await consume_request(_session(scalar=expired), settings, token=TEST_TOKEN, value=b"valid")

    pending = MagicMock(
        status="pending",
        expires_at=datetime.now(UTC) + timedelta(seconds=1),
        secret_name=SECRET_NAME,
    )
    assert (
        await consume_request(_session(scalar=pending), settings, token=TEST_TOKEN, value=b"valid")
        == SECRET_NAME
    )
    assert (tmp_path / SECRET_NAME).read_bytes() == b"valid"
    assert pending.status == "consumed"


def test_atomic_write_refuses_symlink_storage_root(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)

    with pytest.raises(ValueError):
        _atomic_write(linked, "TOKENROUTER_API_KEY", b"secret")
