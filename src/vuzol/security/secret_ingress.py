"""One-time, content-free secret request lifecycle and atomic file installation."""

import hashlib
import os
import re
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.settings import SecretIngressSettings
from vuzol.storage.models import SecretIngressRequest

SECRET_COMMAND = re.compile(r"^/secret(?:@\w+)?\s+([A-Z][A-Z0-9_]{0,99})\s*$")


def parse_secret_command(text: str | None) -> str | None:
    match = SECRET_COMMAND.fullmatch(text or "")
    return match.group(1) if match else None


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def create_request(
    session: AsyncSession,
    settings: SecretIngressSettings,
    *,
    secret_name: str,
    user_id: int,
    chat_id: int,
    thread_id: int,
) -> tuple[SecretIngressRequest, str]:
    if not settings.enabled or secret_name not in settings.allowed_names:
        raise ValueError("secret name is not enabled")
    token = secrets.token_urlsafe(32)
    request = SecretIngressRequest(
        id=uuid.uuid4(),
        token_hash=token_hash(token),
        secret_name=secret_name,
        requested_by_user_id=user_id,
        chat_id=chat_id,
        message_thread_id=thread_id,
        status="pending",
        expires_at=datetime.now(UTC) + timedelta(seconds=settings.ttl_seconds),
    )
    session.add(request)
    await session.flush()
    return request, token


async def cancel_request(session: AsyncSession, request_id: uuid.UUID, *, user_id: int) -> bool:
    request = await session.scalar(
        select(SecretIngressRequest).where(SecretIngressRequest.id == request_id).with_for_update()
    )
    if request is None or request.requested_by_user_id != user_id or request.status != "pending":
        return False
    request.status = "cancelled"
    request.cancelled_at = datetime.now(UTC)
    return True


async def inspect_request(session: AsyncSession, token: str) -> SecretIngressRequest | None:
    request = await session.scalar(
        select(SecretIngressRequest).where(SecretIngressRequest.token_hash == token_hash(token))
    )
    if request is None or request.status != "pending" or request.expires_at <= datetime.now(UTC):
        return None
    return request


async def consume_request(
    session: AsyncSession,
    settings: SecretIngressSettings,
    *,
    token: str,
    value: bytes,
) -> str:
    if not value or len(value) > settings.maximum_secret_bytes or b"\x00" in value:
        raise ValueError("invalid secret value")
    request = await session.scalar(
        select(SecretIngressRequest)
        .where(SecretIngressRequest.token_hash == token_hash(token))
        .with_for_update()
    )
    now = datetime.now(UTC)
    if request is None or request.status != "pending" or request.expires_at <= now:
        raise LookupError("secret request is unavailable")
    _atomic_write(settings.storage_root, request.secret_name, value)
    request.status = "consumed"
    request.consumed_at = now
    return request.secret_name


def _atomic_write(root: Path, name: str, value: bytes) -> None:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink():
        raise ValueError("secret storage root cannot be a symlink")
    destination = root / name
    temporary = root / f".{name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
