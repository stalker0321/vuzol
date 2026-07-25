"""Golden and fail-closed tests for B2 chunked AEAD and DEK wrap."""

from __future__ import annotations

import struct
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from vuzol.ops.backup.crypto import (
    CHUNK_PLAINTEXT_MAX,
    MAX_CHUNKS,
    WRAP_FILE_SIZE,
    BackupCryptoError,
    chunk_nonce,
    decrypt_blob_stream,
    encrypt_blob_stream,
    generate_dek,
    load_kek_from_env_value,
    load_kek_from_file_bytes,
    unwrap_dek,
    wrap_dek,
)

# Fixed material for golden vectors (not production secrets).
_DEK = bytes(range(32))
_KEK = bytes(reversed(range(32)))
_RUN = uuid.UUID("12345678-1234-5678-1234-567812345678")


def test_generate_dek_length() -> None:
    assert len(generate_dek()) == 32


def test_g1_empty_plaintext_round_trip(tmp_path: Path) -> None:
    out = tmp_path / "postgres.dump.enc"
    result = encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=(),
        out_path=out,
    )
    assert result.size_ciphertext > 0
    assert len(result.sha256_ciphertext) == 64
    assert out.read_bytes()[:8] == b"VBULB002"
    plain = b"".join(
        decrypt_blob_stream(
            dek=_DEK,
            blob_path=out,
            run_id=_RUN,
            component="postgres",
            fmt="pg_custom",
        )
    )
    assert plain == b""


def test_g2_small_plaintext_round_trip(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    payload = b"abc"
    result = encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[payload],
        out_path=out,
    )
    plain = b"".join(
        decrypt_blob_stream(
            dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
        )
    )
    assert plain == payload
    assert result.sha256_ciphertext == __import__("hashlib").sha256(out.read_bytes()).hexdigest()


def test_g3_max_plus_three_two_chunks(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    payload = b"x" * (CHUNK_PLAINTEXT_MAX + 3)
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[payload],
        out_path=out,
    )
    plain = b"".join(
        decrypt_blob_stream(
            dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
        )
    )
    assert plain == payload


def test_g4_wrong_dek_fails(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"data"],
        out_path=out,
    )
    with pytest.raises(BackupCryptoError, match="authentication"):
        list(
            decrypt_blob_stream(
                dek=bytes(32),
                blob_path=out,
                run_id=_RUN,
                component="postgres",
                fmt="pg_custom",
            )
        )


def test_g5_wrong_component_aad_fails(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"data"],
        out_path=out,
    )
    with pytest.raises(BackupCryptoError):
        list(
            decrypt_blob_stream(
                dek=_DEK,
                blob_path=out,
                run_id=_RUN,
                component="artifacts",
                fmt="pg_custom",
            )
        )


def test_g6_wrap_unwrap_and_wrong_kek(tmp_path: Path) -> None:
    path = tmp_path / "dek.wrap"
    wrap_dek(kek=_KEK, dek=_DEK, run_id=_RUN, out_path=path)
    assert path.stat().st_size == WRAP_FILE_SIZE
    assert path.read_bytes()[:8] == b"VBULW001"
    assert unwrap_dek(kek=_KEK, wrap_path=path, expected_run_id=_RUN) == _DEK
    with pytest.raises(BackupCryptoError):
        unwrap_dek(kek=bytes(32), wrap_path=path, expected_run_id=_RUN)


def test_g7_trailing_garbage_refused(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"hi"],
        out_path=out,
    )
    out.write_bytes(out.read_bytes() + b"\x00extra")
    with pytest.raises(BackupCryptoError, match=r"trailing|garbage|truncated|auth"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )


def test_kek_loaders_e4() -> None:
    assert load_kek_from_file_bytes(_KEK) == _KEK
    with pytest.raises(BackupCryptoError):
        load_kek_from_file_bytes(b"short")
    hex_kek = _KEK.hex()
    assert load_kek_from_env_value(hex_kek) == _KEK
    with pytest.raises(BackupCryptoError):
        load_kek_from_env_value("not-hex")
    # uppercase hex is accepted after strip().lower()
    assert load_kek_from_env_value(_KEK.hex().upper()) == _KEK
    with pytest.raises(BackupCryptoError):
        load_kek_from_env_value("g" * 64)
    with pytest.raises(BackupCryptoError):
        load_kek_from_env_value("ab" * 20)  # wrong length


def test_chunk_nonce_bounds() -> None:
    assert len(chunk_nonce(0)) == 12
    assert len(chunk_nonce(MAX_CHUNKS - 1)) == 12
    with pytest.raises(BackupCryptoError, match="out of range"):
        chunk_nonce(-1)
    with pytest.raises(BackupCryptoError, match="out of range"):
        chunk_nonce(MAX_CHUNKS)


def test_wrap_rejects_bad_key_lengths(tmp_path: Path) -> None:
    path = tmp_path / "dek.wrap"
    with pytest.raises(BackupCryptoError, match="32 bytes"):
        wrap_dek(kek=b"short", dek=_DEK, run_id=_RUN, out_path=path)
    with pytest.raises(BackupCryptoError, match="32 bytes"):
        wrap_dek(kek=_KEK, dek=b"x" * 16, run_id=_RUN, out_path=path)


def test_unwrap_structural_failures(tmp_path: Path) -> None:
    path = tmp_path / "dek.wrap"
    wrap_dek(kek=_KEK, dek=_DEK, run_id=_RUN, out_path=path)
    good = path.read_bytes()

    path.write_bytes(good[:-1])
    with pytest.raises(BackupCryptoError, match="invalid size"):
        unwrap_dek(kek=_KEK, wrap_path=path, expected_run_id=_RUN)

    path.write_bytes(b"BADMAGIC" + good[8:])
    with pytest.raises(BackupCryptoError, match="magic"):
        unwrap_dek(kek=_KEK, wrap_path=path, expected_run_id=_RUN)

    path.write_bytes(good[:8] + bytes([0x99, 0x00]) + good[10:])
    with pytest.raises(BackupCryptoError, match="version"):
        unwrap_dek(kek=_KEK, wrap_path=path, expected_run_id=_RUN)

    other = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    path.write_bytes(good)
    with pytest.raises(BackupCryptoError, match="run_id"):
        unwrap_dek(kek=_KEK, wrap_path=path, expected_run_id=other)


def test_encrypt_rejects_bad_params(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    with pytest.raises(BackupCryptoError, match="DEK"):
        encrypt_blob_stream(
            dek=b"x",
            run_id=_RUN,
            component="postgres",
            fmt="pg_custom",
            plaintext_iter=[b"a"],
            out_path=out,
        )
    with pytest.raises(BackupCryptoError, match="component"):
        encrypt_blob_stream(
            dek=_DEK,
            run_id=_RUN,
            component="",
            fmt="pg_custom",
            plaintext_iter=[b"a"],
            out_path=out,
        )
    with pytest.raises(BackupCryptoError, match="format"):
        encrypt_blob_stream(
            dek=_DEK,
            run_id=_RUN,
            component="postgres",
            fmt="",
            plaintext_iter=[b"a"],
            out_path=out,
        )
    with pytest.raises(BackupCryptoError, match="component"):
        encrypt_blob_stream(
            dek=_DEK,
            run_id=_RUN,
            component="c" * 65,
            fmt="pg_custom",
            plaintext_iter=[b"a"],
            out_path=out,
        )


def test_encrypt_skips_empty_pieces(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"", b"hello", b"", b" world"],
        out_path=out,
    )
    plain = b"".join(
        decrypt_blob_stream(
            dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
        )
    )
    assert plain == b"hello world"


def test_decrypt_rejects_bad_dek_and_header(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"data"],
        out_path=out,
    )
    with pytest.raises(BackupCryptoError, match="DEK"):
        list(
            decrypt_blob_stream(
                dek=b"x",
                blob_path=out,
                run_id=_RUN,
                component="postgres",
                fmt="pg_custom",
            )
        )
    raw = out.read_bytes()
    out.write_bytes(b"X" * 10)
    with pytest.raises(BackupCryptoError, match=r"truncated|magic"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )
    out.write_bytes(b"BADMAGIC" + raw[8:])
    with pytest.raises(BackupCryptoError, match="magic"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )
    out.write_bytes(raw[:8] + bytes([0x99, 0x00]) + raw[10:])
    with pytest.raises(BackupCryptoError, match="version"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )
    other = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    out.write_bytes(raw)
    with pytest.raises(BackupCryptoError, match="run_id"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=other, component="postgres", fmt="pg_custom"
            )
        )
    with pytest.raises(BackupCryptoError, match=r"component|format"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="other"
            )
        )


def test_decrypt_truncated_chunk(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"data"],
        out_path=out,
    )
    raw = out.read_bytes()
    # Drop last bytes so ciphertext is truncated after a valid plain_len header.
    out.write_bytes(raw[:-5])
    with pytest.raises(BackupCryptoError, match="truncated"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )


def test_decrypt_oversized_plaintext_len(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"x"],
        out_path=out,
    )
    raw = bytearray(out.read_bytes())
    # Header ends at offset after reserved; first chunk plain_len is next 4 bytes.
    # Find start of first chunk: parse header minimally via decrypt path by patching.
    # Locate VBULB002 and rewrite the first BE32 after header end to oversized.
    # Header: magic8 + ver2 + uuid16 + c_len1 + c + f_len1 + f + chunk_max4 + reserved8
    offset = 8 + 2 + 16
    c_len = raw[offset]
    offset += 1 + c_len
    f_len = raw[offset]
    offset += 1 + f_len + 4 + 8
    struct.pack_into(">I", raw, offset, CHUNK_PLAINTEXT_MAX + 1)
    out.write_bytes(bytes(raw))
    with pytest.raises(BackupCryptoError, match="exceeds max"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )


def test_decrypt_chunk_max_mismatch(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"x"],
        out_path=out,
    )
    raw = bytearray(out.read_bytes())
    offset = 8 + 2 + 16
    c_len = raw[offset]
    offset += 1 + c_len
    f_len = raw[offset]
    offset += 1 + f_len
    struct.pack_into(">I", raw, offset, 1024)  # wrong chunk max
    out.write_bytes(bytes(raw))
    with pytest.raises(BackupCryptoError, match="chunk_size_max"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )


def test_fsync_handle_without_fileno(tmp_path: Path) -> None:
    """_fsync_handle is a no-op when fileno is missing (defensive)."""
    from vuzol.ops.backup import crypto as crypto_mod

    class _NoFileno:
        pass

    crypto_mod._fsync_handle(_NoFileno())  # does not raise


def test_wrap_invariant_guards(tmp_path: Path) -> None:
    """Force wrap size invariant failure via patched encrypt return."""
    path = tmp_path / "dek.wrap"
    with patch("vuzol.ops.backup.crypto.AESGCM") as mock_cls:
        instance = mock_cls.return_value
        instance.encrypt.return_value = b"too-short"
        with pytest.raises(BackupCryptoError, match="unexpected wrap"):
            wrap_dek(kek=_KEK, dek=_DEK, run_id=_RUN, out_path=path)


class _BoundedReadWrapper:
    """File-like wrapper that refuses unbounded ``read()`` / ``read(-1)`` / ``read(0)``."""

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self.max_request = 0
        self.calls = 0

    def read(self, size: int = -1, /) -> bytes:
        self.calls += 1
        if size is None or size <= 0:
            raise AssertionError(f"unbounded read refused: size={size!r}")
        if size > self.max_request:
            self.max_request = size
        return self._inner.read(size)  # type: ignore[no-any-return]


def test_decrypt_uses_only_bounded_reads(tmp_path: Path) -> None:
    """Streaming decrypt must never call handle.read() without a positive size."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    from vuzol.ops.backup import crypto as crypto_mod

    out = tmp_path / "blob.enc"
    payload = b"bounded-read-proof"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[payload],
        out_path=out,
    )
    with out.open("rb") as raw:
        wrapper = _BoundedReadWrapper(raw)
        plain = b"".join(
            crypto_mod._decrypt_blob_stream_from_handle(
                wrapper,
                aead=AESGCM(_DEK),
                run_id=_RUN,
                component="postgres",
                fmt="pg_custom",
            )
        )
    assert plain == payload
    assert wrapper.calls > 0
    assert wrapper.max_request > 0
    assert wrapper.max_request <= crypto_mod._STREAM_READ_MAX


def test_decrypt_multi_chunk_yields_per_chunk(tmp_path: Path) -> None:
    """Multi-chunk ciphertext yields one plaintext piece per framed chunk."""
    out = tmp_path / "blob.enc"
    payload = b"A" * CHUNK_PLAINTEXT_MAX + b"BCD"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[payload],
        out_path=out,
    )
    pieces = list(
        decrypt_blob_stream(
            dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
        )
    )
    assert len(pieces) == 2
    assert pieces[0] == b"A" * CHUNK_PLAINTEXT_MAX
    assert pieces[1] == b"BCD"
    assert b"".join(pieces) == payload


def test_decrypt_exact_max_has_empty_final_chunk(tmp_path: Path) -> None:
    """Plaintext of exactly CHUNK_PLAINTEXT_MAX ends with an empty final chunk (E2)."""
    out = tmp_path / "blob.enc"
    payload = b"Z" * CHUNK_PLAINTEXT_MAX
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[payload],
        out_path=out,
    )
    pieces = list(
        decrypt_blob_stream(
            dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
        )
    )
    assert len(pieces) == 2
    assert pieces[0] == payload
    assert pieces[1] == b""


def test_decrypt_truncated_chunk_header(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"data"],
        out_path=out,
    )
    raw = out.read_bytes()
    # Keep full header, append only 2 of the 4-byte chunk length prefix.
    offset = 8 + 2 + 16
    c_len = raw[offset]
    offset += 1 + c_len
    f_len = raw[offset]
    offset += 1 + f_len + 4 + 8
    out.write_bytes(raw[:offset] + raw[offset : offset + 2])
    with pytest.raises(BackupCryptoError, match="truncated chunk header"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )


def test_decrypt_no_chunks_after_header(tmp_path: Path) -> None:
    out = tmp_path / "blob.enc"
    encrypt_blob_stream(
        dek=_DEK,
        run_id=_RUN,
        component="postgres",
        fmt="pg_custom",
        plaintext_iter=[b"x"],
        out_path=out,
    )
    raw = out.read_bytes()
    offset = 8 + 2 + 16
    c_len = raw[offset]
    offset += 1 + c_len
    f_len = raw[offset]
    offset += 1 + f_len + 4 + 8
    out.write_bytes(raw[:offset])
    with pytest.raises(BackupCryptoError, match="no chunks"):
        list(
            decrypt_blob_stream(
                dek=_DEK, blob_path=out, run_id=_RUN, component="postgres", fmt="pg_custom"
            )
        )


def test_decrypt_invalid_utf8_component_name(tmp_path: Path) -> None:
    """Invalid UTF-8 in component name → BackupCryptoError, no raw-byte leak."""
    from vuzol.ops.backup import crypto as crypto_mod

    out = tmp_path / "blob.enc"
    bad_comp = b"\xff\xfe"
    fmt = b"pg_custom"
    header = (
        crypto_mod.BLOB_MAGIC
        + bytes([crypto_mod.BLOB_HEADER_VERSION, 0x00])
        + _RUN.bytes
        + bytes([len(bad_comp)])
        + bad_comp
        + bytes([len(fmt)])
        + fmt
        + struct.pack(">I", CHUNK_PLAINTEXT_MAX)
        + struct.pack(">Q", 0)
    )
    out.write_bytes(header)
    with pytest.raises(BackupCryptoError, match="header encoding invalid") as raised:
        list(
            decrypt_blob_stream(
                dek=_DEK,
                blob_path=out,
                run_id=_RUN,
                component="postgres",
                fmt="pg_custom",
            )
        )
    message = str(raised.value)
    assert "\xff" not in message
    assert "\\xff" not in message
    assert bad_comp.decode("latin-1") not in message


def test_decrypt_invalid_utf8_format_name(tmp_path: Path) -> None:
    """Invalid UTF-8 in format name maps to the same stable encoding error."""
    from vuzol.ops.backup import crypto as crypto_mod

    out = tmp_path / "blob.enc"
    comp = b"postgres"
    bad_fmt = b"pg_\xff"
    header = (
        crypto_mod.BLOB_MAGIC
        + bytes([crypto_mod.BLOB_HEADER_VERSION, 0x00])
        + _RUN.bytes
        + bytes([len(comp)])
        + comp
        + bytes([len(bad_fmt)])
        + bad_fmt
        + struct.pack(">I", CHUNK_PLAINTEXT_MAX)
        + struct.pack(">Q", 0)
    )
    out.write_bytes(header)
    with pytest.raises(BackupCryptoError, match="header encoding invalid") as raised:
        list(
            decrypt_blob_stream(
                dek=_DEK,
                blob_path=out,
                run_id=_RUN,
                component="postgres",
                fmt="pg_custom",
            )
        )
    assert "\xff" not in str(raised.value)


def test_decrypt_max_chunks_boundary_synthetic(tmp_path: Path) -> None:
    """Decrypt raises chunk limit after MAX_CHUNKS full-size chunks (synthetic ceilings).

    Monkeypatches small CHUNK_PLAINTEXT_MAX and MAX_CHUNKS so the test never
    allocates or iterates millions of real-size chunks.
    """
    from vuzol.ops.backup import crypto as crypto_mod

    out = tmp_path / "blob.enc"
    small_max = 32
    # Encrypt under a small full-chunk size so two full chunks are tiny.
    with patch.object(crypto_mod, "CHUNK_PLAINTEXT_MAX", small_max):
        payload = b"A" * (small_max * 2) + b"z"
        encrypt_blob_stream(
            dek=_DEK,
            run_id=_RUN,
            component="postgres",
            fmt="pg_custom",
            plaintext_iter=[payload],
            out_path=out,
        )
    # Decrypt with same frame size but a ceiling of two full chunks: after the
    # second full chunk, decrypt must refuse before consuming the short final.
    with (
        patch.object(crypto_mod, "CHUNK_PLAINTEXT_MAX", small_max),
        patch.object(crypto_mod, "MAX_CHUNKS", 2),
        pytest.raises(BackupCryptoError, match="chunk limit exceeded"),
    ):
        list(
            decrypt_blob_stream(
                dek=_DEK,
                blob_path=out,
                run_id=_RUN,
                component="postgres",
                fmt="pg_custom",
            )
        )
