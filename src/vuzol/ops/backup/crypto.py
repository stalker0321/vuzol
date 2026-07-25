"""Chunked AES-256-GCM backup framing and DEK wrap (B2 normative wire formats).

Blob: vuzol-backup-blob.v2 (magic VBULB002)
Wrap: vuzol-backup-wrap.v1 (magic VBULW001)

Errata E2: intermediate plaintext chunks are exactly CHUNK_PLAINTEXT_MAX.
Errata E3: chunk record = BE32(plaintext_len) || AESGCM(ciphertext||tag).
"""

from __future__ import annotations

import hashlib
import secrets
import struct
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

CHUNK_PLAINTEXT_MAX = 1_048_576
MAX_CHUNKS = 2_097_152
BLOB_MAGIC = b"VBULB002"
WRAP_MAGIC = b"VBULW001"
BLOB_HEADER_VERSION = 0x02
WRAP_VERSION = 0x01
NONCE_DOMAIN = 0x0000_0001
KEK_LEN = 32
DEK_LEN = 32
WRAP_NONCE_LEN = 12
WRAP_FILE_SIZE = 86  # 8+1+1+16+12+48
# Single read() size bound for streaming decrypt (never unbounded read()).
_STREAM_READ_MAX = 65_536
# Max component/format name lengths (matches encrypt_blob_stream).
_MAX_COMPONENT_LEN = 64
_MAX_FORMAT_LEN = 32


class BackupCryptoError(ValueError):
    """Crypto framing, KEK, or AEAD verification failed."""


def generate_dek() -> bytes:
    return secrets.token_bytes(DEK_LEN)


def load_kek_from_file_bytes(body: bytes) -> bytes:
    """E4: file secret body is exactly 32 raw bytes."""

    if len(body) != KEK_LEN:
        raise BackupCryptoError("KEK file must be exactly 32 raw bytes")
    return body


def load_kek_from_env_value(value: str) -> bytes:
    """E4: env KEK is exactly 64 lowercase hex characters."""

    cleaned = value.strip().lower()
    if len(cleaned) != 64 or any(c not in "0123456789abcdef" for c in cleaned):
        raise BackupCryptoError("KEK env must be exactly 64 lowercase hex characters")
    return bytes.fromhex(cleaned)


def chunk_nonce(index: int) -> bytes:
    if index < 0 or index >= MAX_CHUNKS:
        raise BackupCryptoError("chunk index out of range")
    return struct.pack(">I", NONCE_DOMAIN) + struct.pack(">Q", index)


def blob_aad(
    *,
    run_id: uuid.UUID,
    component: str,
    fmt: str,
    chunk_index: int,
    plaintext_len: int,
) -> bytes:
    return (
        b"vuzol-backup-blob.v2\0"
        + run_id.bytes
        + component.encode("utf-8")
        + fmt.encode("utf-8")
        + struct.pack(">Q", chunk_index)
        + struct.pack(">Q", plaintext_len)
    )


def wrap_aad(*, run_id: uuid.UUID) -> bytes:
    return b"vuzol-backup-dek-wrap.v1\0" + run_id.bytes


def wrap_dek(*, kek: bytes, dek: bytes, run_id: uuid.UUID, out_path: Path) -> None:
    if len(kek) != KEK_LEN or len(dek) != DEK_LEN:
        raise BackupCryptoError("KEK and DEK must be 32 bytes")
    nonce = secrets.token_bytes(WRAP_NONCE_LEN)
    ct = AESGCM(kek).encrypt(nonce, dek, wrap_aad(run_id=run_id))
    if len(ct) != DEK_LEN + 16:
        raise BackupCryptoError("unexpected wrap ciphertext length")
    payload = WRAP_MAGIC + bytes([WRAP_VERSION, 0x00]) + run_id.bytes + nonce + ct
    if len(payload) != WRAP_FILE_SIZE:
        raise BackupCryptoError("wrap file size invariant broken")
    out_path.write_bytes(payload)
    _fsync_file(out_path)


def unwrap_dek(*, kek: bytes, wrap_path: Path, expected_run_id: uuid.UUID) -> bytes:
    raw = wrap_path.read_bytes()
    if len(raw) != WRAP_FILE_SIZE:
        raise BackupCryptoError("dek.wrap has invalid size")
    if raw[:8] != WRAP_MAGIC:
        raise BackupCryptoError("dek.wrap magic mismatch")
    if raw[8] != WRAP_VERSION or raw[9] != 0:
        raise BackupCryptoError("dek.wrap version/flags unsupported")
    run_id = uuid.UUID(bytes=raw[10:26])
    if run_id != expected_run_id:
        raise BackupCryptoError("dek.wrap run_id mismatch")
    nonce = raw[26:38]
    ct = raw[38:]
    try:
        dek = AESGCM(kek).decrypt(nonce, ct, wrap_aad(run_id=run_id))
    except InvalidTag as error:
        raise BackupCryptoError("dek.wrap authentication failed") from error
    if len(dek) != DEK_LEN:
        raise BackupCryptoError("unwrapped DEK length invalid")
    return dek


@dataclass(frozen=True, slots=True)
class EncryptResult:
    sha256_ciphertext: str
    size_ciphertext: int


def encrypt_blob_stream(
    *,
    dek: bytes,
    run_id: uuid.UUID,
    component: str,
    fmt: str,
    plaintext_iter: Iterable[bytes],
    out_path: Path,
) -> EncryptResult:
    """Encrypt plaintext stream; intermediate chunks are exactly CHUNK_PLAINTEXT_MAX."""

    if len(dek) != DEK_LEN:
        raise BackupCryptoError("DEK must be 32 bytes")
    if not component or len(component.encode()) > 64:
        raise BackupCryptoError("invalid component name")
    if not fmt or len(fmt.encode()) > 32:
        raise BackupCryptoError("invalid format name")

    aead = AESGCM(dek)
    digest = hashlib.sha256()
    size = 0
    with out_path.open("wb") as handle:
        header = _blob_header(run_id=run_id, component=component, fmt=fmt)
        handle.write(header)
        digest.update(header)
        size += len(header)

        buffer = bytearray()
        chunk_index = 0
        for piece in plaintext_iter:
            if not piece:
                continue
            buffer.extend(piece)
            while len(buffer) >= CHUNK_PLAINTEXT_MAX:
                plain = bytes(buffer[:CHUNK_PLAINTEXT_MAX])
                del buffer[:CHUNK_PLAINTEXT_MAX]
                record = _encrypt_chunk(
                    aead,
                    run_id=run_id,
                    component=component,
                    fmt=fmt,
                    chunk_index=chunk_index,
                    plaintext=plain,
                )
                handle.write(record)
                digest.update(record)
                size += len(record)
                chunk_index += 1
                if chunk_index >= MAX_CHUNKS:
                    raise BackupCryptoError("chunk limit exceeded")

        # Final remainder 0..MAX (empty dump → one empty chunk).
        plain = bytes(buffer)
        record = _encrypt_chunk(
            aead,
            run_id=run_id,
            component=component,
            fmt=fmt,
            chunk_index=chunk_index,
            plaintext=plain,
        )
        handle.write(record)
        digest.update(record)
        size += len(record)
        handle.flush()
        _fsync_handle(handle)
    return EncryptResult(sha256_ciphertext=digest.hexdigest(), size_ciphertext=size)


class _Readable(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


def decrypt_blob_stream(
    *,
    dek: bytes,
    blob_path: Path,
    run_id: uuid.UUID,
    component: str,
    fmt: str,
) -> Iterator[bytes]:
    """Decrypt a VBULB002 blob with bounded streaming reads (no full-file read)."""

    if len(dek) != DEK_LEN:
        raise BackupCryptoError("DEK must be 32 bytes")
    aead = AESGCM(dek)
    with blob_path.open("rb") as handle:
        yield from _decrypt_blob_stream_from_handle(
            handle,
            aead=aead,
            run_id=run_id,
            component=component,
            fmt=fmt,
        )


def _decrypt_blob_stream_from_handle(
    handle: _Readable,
    *,
    aead: AESGCM,
    run_id: uuid.UUID,
    component: str,
    fmt: str,
) -> Iterator[bytes]:
    _parse_blob_header_stream(handle, run_id=run_id, component=component, fmt=fmt)
    chunk_index = 0
    while True:
        length_bytes = _read_until_eof_or_exact(handle, 4, what="chunk header")
        if not length_bytes:
            if chunk_index == 0:
                raise BackupCryptoError("blob has no chunks")
            # Encrypt always writes a short final remainder (including empty
            # terminator after exact CHUNK_PLAINTEXT_MAX multiples). EOF after
            # any authenticated full frame is truncated / incomplete.
            raise BackupCryptoError("truncated blob: missing final chunk")
        if len(length_bytes) < 4:
            raise BackupCryptoError("truncated chunk header")
        (plain_len,) = struct.unpack(">I", length_bytes)
        if plain_len > CHUNK_PLAINTEXT_MAX:
            raise BackupCryptoError("chunk plaintext_len exceeds max")
        need = plain_len + 16
        try:
            ct = _read_exact(handle, need, what="chunk ciphertext")
        except BackupCryptoError as error:
            if "truncated" in str(error):
                raise BackupCryptoError("truncated chunk ciphertext") from error
            raise
        aad = blob_aad(
            run_id=run_id,
            component=component,
            fmt=fmt,
            chunk_index=chunk_index,
            plaintext_len=plain_len,
        )
        try:
            plain = aead.decrypt(chunk_nonce(chunk_index), ct, aad)
        except InvalidTag as error:
            raise BackupCryptoError("blob authentication failed") from error
        if len(plain) != plain_len:
            raise BackupCryptoError("decrypted length mismatch")
        yield plain
        chunk_index += 1
        if plain_len < CHUNK_PLAINTEXT_MAX:
            trailing = _read_at_most(handle, 1, what="trailing probe")
            if trailing:
                raise BackupCryptoError("trailing garbage after final chunk")
            return
        if chunk_index >= MAX_CHUNKS:
            raise BackupCryptoError("chunk limit exceeded")


def _read_exact(handle: _Readable, n: int, *, what: str) -> bytes:
    """Read exactly ``n`` bytes using only bounded positive ``read(size)`` calls."""

    if n < 0:
        raise BackupCryptoError(f"invalid read size for {what}")
    if n == 0:
        return b""
    parts = bytearray()
    remaining = n
    while remaining > 0:
        to_read = min(remaining, _STREAM_READ_MAX)
        chunk = handle.read(to_read)
        if not isinstance(chunk, (bytes, bytearray)):
            raise BackupCryptoError(f"invalid read result for {what}")
        if not chunk:
            raise BackupCryptoError(f"truncated {what}")
        if len(chunk) > to_read:
            raise BackupCryptoError(f"oversized read for {what}")
        parts.extend(chunk)
        remaining -= len(chunk)
    return bytes(parts)


def _read_until_eof_or_exact(handle: _Readable, n: int, *, what: str) -> bytes:
    """Read up to ``n`` bytes, accumulating short reads; empty means clean EOF."""

    if n <= 0:
        return b""
    parts = bytearray()
    remaining = n
    while remaining > 0:
        to_read = min(remaining, _STREAM_READ_MAX)
        chunk = handle.read(to_read)
        if chunk is None:
            break
        if not isinstance(chunk, (bytes, bytearray)):
            raise BackupCryptoError(f"invalid read result for {what}")
        if not chunk:
            break
        if len(chunk) > to_read:
            raise BackupCryptoError(f"oversized read for {what}")
        parts.extend(chunk)
        remaining -= len(chunk)
    return bytes(parts)


def _read_at_most(handle: _Readable, n: int, *, what: str) -> bytes:
    """Read up to ``n`` bytes with a single bounded ``read`` (may return fewer / empty)."""

    if n <= 0:
        return b""
    to_read = min(n, _STREAM_READ_MAX)
    chunk = handle.read(to_read)
    if chunk is None:
        return b""
    if not isinstance(chunk, (bytes, bytearray)):
        raise BackupCryptoError(f"invalid read result for {what}")
    if len(chunk) > to_read:
        raise BackupCryptoError(f"oversized read for {what}")
    return bytes(chunk)


def _blob_header(*, run_id: uuid.UUID, component: str, fmt: str) -> bytes:
    c_bytes = component.encode("utf-8")
    f_bytes = fmt.encode("utf-8")
    return (
        BLOB_MAGIC
        + bytes([BLOB_HEADER_VERSION, 0x00])
        + run_id.bytes
        + bytes([len(c_bytes)])
        + c_bytes
        + bytes([len(f_bytes)])
        + f_bytes
        + struct.pack(">I", CHUNK_PLAINTEXT_MAX)
        + struct.pack(">Q", 0)
    )


def _parse_blob_header_stream(
    handle: _Readable, *, run_id: uuid.UUID, component: str, fmt: str
) -> None:
    """Parse and validate blob header using only bounded reads."""

    prefix = _read_exact(handle, 26, what="blob header")
    if prefix[:8] != BLOB_MAGIC:
        raise BackupCryptoError("blob magic mismatch")
    if prefix[8] != BLOB_HEADER_VERSION or prefix[9] != 0:
        raise BackupCryptoError("blob version/flags unsupported")
    file_run = uuid.UUID(bytes=prefix[10:26])
    if file_run != run_id:
        raise BackupCryptoError("blob run_id mismatch")
    c_len = _read_exact(handle, 1, what="blob header")[0]
    if c_len > _MAX_COMPONENT_LEN:
        raise BackupCryptoError("blob component name too long")
    c_raw = _read_exact(handle, c_len, what="blob header")
    try:
        c_name = c_raw.decode("utf-8")
    except UnicodeDecodeError as error:
        # Stable taxonomy; never embed raw header bytes in the message.
        raise BackupCryptoError("blob header encoding invalid") from error
    f_len = _read_exact(handle, 1, what="blob header")[0]
    if f_len > _MAX_FORMAT_LEN:
        raise BackupCryptoError("blob format name too long")
    f_raw = _read_exact(handle, f_len, what="blob header")
    try:
        f_name = f_raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BackupCryptoError("blob header encoding invalid") from error
    if c_name != component or f_name != fmt:
        raise BackupCryptoError("blob component/format mismatch")
    tail = _read_exact(handle, 12, what="blob header")
    (chunk_max,) = struct.unpack_from(">I", tail, 0)
    if chunk_max != CHUNK_PLAINTEXT_MAX:
        raise BackupCryptoError("blob chunk_size_max mismatch")
    # reserved 8 bytes already consumed in tail[4:12]


def _parse_blob_header(raw: bytes, *, run_id: uuid.UUID, component: str, fmt: str) -> int:
    """Parse header from an in-memory buffer (tests / tooling). Returns body offset."""

    from io import BytesIO

    buf = BytesIO(raw)
    _parse_blob_header_stream(buf, run_id=run_id, component=component, fmt=fmt)
    return buf.tell()


def _encrypt_chunk(
    aead: AESGCM,
    *,
    run_id: uuid.UUID,
    component: str,
    fmt: str,
    chunk_index: int,
    plaintext: bytes,
) -> bytes:
    plain_len = len(plaintext)
    if plain_len > CHUNK_PLAINTEXT_MAX:
        raise BackupCryptoError("chunk too large")
    aad = blob_aad(
        run_id=run_id,
        component=component,
        fmt=fmt,
        chunk_index=chunk_index,
        plaintext_len=plain_len,
    )
    ct = aead.encrypt(chunk_nonce(chunk_index), plaintext, aad)
    return struct.pack(">I", plain_len) + ct


def _fsync_file(path: Path) -> None:
    with path.open("rb+") as handle:
        _fsync_handle(handle)


def _fsync_handle(handle: object) -> None:
    fileno = getattr(handle, "fileno", None)
    if callable(fileno):
        import os

        os.fsync(fileno())
