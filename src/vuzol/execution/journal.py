"""Encrypted append-only invocation history, independent of workflow transactions.

Each stdout/stderr chunk is durable before the next read. A killed worker leaves
decryptable complete frames, not a JSON document requiring normal termination.
The key stays outside worktrees, containers and ordinary artifact delivery.
"""

import base64
import json
import os
import stat
import struct
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vuzol.execution.domain import ProcessEnvelope
from vuzol.execution.paths import contained, trusted_root

MAGIC = b"VUZOLTRACE1\n"
MAX_FRAME = 4 * 1024 * 1024


class ExecutionJournal:
    def __init__(self, root: Path, key_file: Path) -> None:
        info = key_file.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o027:
            raise ValueError("execution journal key must be a private regular file")
        key = bytes.fromhex(key_file.read_text().strip())
        if len(key) != 32:
            raise ValueError("execution journal requires a 256-bit key")
        self.root = trusted_root(root, create=True)
        self._cipher = AESGCM(key)

    def open(self, envelope: ProcessEnvelope) -> "InvocationJournal":
        # container_name is derived from trusted UUIDs and the lease generation.
        name = f"{envelope.container_name}.{envelope.provider_attempt}.{uuid.uuid4().hex}.vtrace"
        path = contained(self.root, self.root / name, must_exist=False)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        journal = InvocationJournal(descriptor, self._cipher)
        try:
            if os.write(descriptor, MAGIC) != len(MAGIC):
                raise OSError("incomplete execution journal header")
            journal.append("start", envelope.model_dump(mode="json", exclude={"stdin"}))
            journal.append("stdin", envelope.stdin.encode())
        except BaseException:
            journal.close()
            raise
        return journal

    def read(self, path: Path) -> Iterator[dict[str, Any]]:
        path = contained(self.root, path)
        if path.is_symlink():
            raise ValueError("journal path must not be a symlink")
        with path.open("rb") as stream:
            if stream.read(len(MAGIC)) != MAGIC:
                raise ValueError("invalid execution journal")
            while header := stream.read(4):
                if len(header) != 4:
                    return  # A crash may leave a partial final frame.
                size = struct.unpack(">I", header)[0]
                if not 28 <= size <= MAX_FRAME:
                    raise ValueError("invalid execution journal frame")
                frame = stream.read(size)
                if len(frame) != size:
                    return
                record = json.loads(self._cipher.decrypt(frame[:12], frame[12:], MAGIC))
                yield record


class InvocationJournal:
    def __init__(self, descriptor: int, cipher: AESGCM) -> None:
        self._descriptor = descriptor
        self._cipher = cipher
        self._sequence = 0

    def append(self, channel: str, value: bytes | dict[str, Any]) -> None:
        self._sequence += 1
        record: dict[str, Any] = {"seq": self._sequence, "time": time.time(), "channel": channel}
        if isinstance(value, bytes):
            record["base64"] = base64.b64encode(value).decode()
        else:
            record["data"] = value
        plaintext = json.dumps(record, ensure_ascii=False).encode()
        nonce = os.urandom(12)
        encrypted = nonce + self._cipher.encrypt(nonce, plaintext, MAGIC)
        if len(encrypted) > MAX_FRAME:
            raise ValueError("execution journal record too large")
        remaining = memoryview(struct.pack(">I", len(encrypted)) + encrypted)
        while remaining:
            written = os.write(self._descriptor, remaining)
            if written <= 0:
                raise OSError("execution journal write made no progress")
            remaining = remaining[written:]
        os.fsync(self._descriptor)

    def close(self) -> None:
        if self._descriptor >= 0:
            os.close(self._descriptor)
            self._descriptor = -1
