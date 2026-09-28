"""Narrow remote artifact transport for the second node (WP12).

Exactly one transport exists: client-side pull over HTTPS with a bounded
response, verified against the content hash on receipt. There is no server
side here (no network server invariant holds), no push, no streaming, no
autoscaling, no consensus, no global filesystem: the remote end serves
content-addressed bytes, this end pulls and re-hashes. SSH and agent
transports were considered and rejected: SSH needs key management beyond
the credential-ref boundary, an agent channel needs a protocol that does
not exist yet. Pull-HTTP reuses the ``httpx`` dependency already in use.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import httpx

TRANSFER_PROTOCOL_VERSION = "node-transfer.v1"
_MAX_TRANSFER_BYTES = 64_000_000


class TransferError(RuntimeError):
    """Remote transfer failed or was rejected."""


class TransferHashMismatch(TransferError):
    """Received bytes do not match the requested content hash."""


@dataclass(frozen=True, slots=True)
class PulledArtifact:
    content_hash: str
    content: bytes
    source_url: str


async def pull_artifact(
    base_url: str,
    content_hash: str,
    *,
    client: httpx.AsyncClient | None = None,
    timeout_seconds: float = 60.0,
    max_bytes: int = _MAX_TRANSFER_BYTES,
) -> PulledArtifact:
    """Pull content-addressed bytes and verify the hash before returning.

    The URL carries only the hex digest (``GET {base}/artifacts/{sha256}``);
    no secret, credential or path material travels. Oversize, network
    failure and hash mismatch all raise — a corrupt transfer is never
    returned as bytes.
    """

    if len(content_hash) != 64 or any(c not in "0123456789abcdef" for c in content_hash):
        raise TransferError("transfer requires a sha256 content hash")
    url = base_url.rstrip("/") + f"/artifacts/{content_hash}"
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout_seconds)
        close = True
    try:
        try:
            response = await client.get(
                url, headers={"Accept": "application/octet-stream"}, follow_redirects=False
            )
        except httpx.HTTPError as error:
            raise TransferError(f"transfer request failed: {type(error).__name__}") from error
        if response.status_code != 200:
            raise TransferError(f"transfer refused with status {response.status_code}")
        length = response.headers.get("content-length")
        if length is not None and int(length) > max_bytes:
            raise TransferError("transfer exceeds the byte limit")
        content = response.content
        if len(content) > max_bytes:
            raise TransferError("transfer exceeds the byte limit")
    finally:
        if close:
            await client.aclose()
    if hashlib.sha256(content).hexdigest() != content_hash:
        raise TransferHashMismatch("received bytes do not match the content hash")
    return PulledArtifact(content_hash=content_hash, content=content, source_url=url)
