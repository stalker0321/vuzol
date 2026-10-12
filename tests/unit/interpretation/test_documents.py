import asyncio
import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

from vuzol.interpretation.documents import (
    MAX_TEXT_CHARS,
    DocumentError,
    document_format,
    extract_text,
    read_document,
)


def docx_bytes(text: str = "first", *, xml: bytes | None = None) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "word/document.xml",
            xml
            or (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                f"<w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p>"
                "<w:tbl><w:tr><w:tc><w:p><w:r><w:t>table cell</w:t></w:r></w:p>"
                "</w:tc></w:tr></w:tbl></w:body></w:document>"
            ),
        )
    return output.getvalue()


def pdf_bytes(
    *,
    text: str | None = "only discuss; do not start",
    encrypted: bool = False,
    scanned: bool = False,
) -> bytes:
    writer = PdfWriter()
    page = writer.add_blank_page(width=200, height=200)
    if scanned:
        image = DecodedStreamObject()
        image.set_data(b"\xff")
        image.update(
            {
                NameObject("/Type"): NameObject("/XObject"),
                NameObject("/Subtype"): NameObject("/Image"),
            }
        )
        page[NameObject("/Resources")] = DictionaryObject(
            {
                NameObject("/XObject"): DictionaryObject(
                    {NameObject("/Im1"): writer._add_object(image)}
                )
            }
        )
    if text is not None:
        font = DictionaryObject(
            {
                NameObject("/Type"): NameObject("/Font"),
                NameObject("/Subtype"): NameObject("/Type1"),
                NameObject("/BaseFont"): NameObject("/Helvetica"),
            }
        )
        page[NameObject("/Resources")] = DictionaryObject(
            {NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})}
        )
        stream = DecodedStreamObject()
        stream.set_data(f"BT /F1 12 Tf 10 100 Td ({text}) Tj ET".encode())
        page[NameObject("/Contents")] = writer._add_object(stream)
    if encrypted:
        writer.encrypt("fixture-password")
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


@pytest.mark.parametrize(
    "name,mime,kind",
    [
        ("a.md", "text/markdown", "text"),
        ("a.md", "application/octet-stream", "text"),
        ("a.json", "application/json", "text"),
        ("a.csv", "text/csv", "text"),
        ("a.docx", "application/zip", "docx"),
        ("a.pdf", "application/pdf", "pdf"),
        (None, "text/plain", "text"),
        ("A.DOCX", "application/octet-stream", "docx"),
    ],
)
def test_document_format(name, mime, kind):
    assert document_format(name, mime) == kind


@pytest.mark.parametrize(
    "name,mime",
    [
        ("a.zip", "application/zip"),
        ("a.doc", "application/msword"),
        ("a.exe", "text/plain"),
        ("a.txt", "image/jpeg"),
        (None, "image/png"),
    ],
)
def test_format_spoof_or_unsupported_is_rejected(name, mime):
    with pytest.raises(DocumentError):
        document_format(name, mime)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16"])
def test_text_is_lossless_including_conditions(encoding):
    raw = "Если всё работает, НЕ меняй.\nТолько обсуди; пока не запускай.\n"  # noqa: RUF001
    assert extract_text(raw.encode(encoding), "text") == raw


@pytest.mark.parametrize(
    "content,category",
    [
        (b"\x00\x01", "document_binary_or_invalid"),
        (b"\xff\xff", "document_binary_or_invalid"),
        (b"  \n", "document_no_text"),
        (b"x" * (MAX_TEXT_CHARS + 1), "document_text_too_large"),
    ],
)
def test_invalid_text_never_returns_partial_content(content, category):
    with pytest.raises(DocumentError, match=category):
        extract_text(content, "text")


def test_pdf_and_docx_actual_bounded_child(tmp_path):
    async def scenario():
        for kind, data, expected in (
            ("pdf", pdf_bytes(), "only discuss; do not start"),
            ("docx", docx_bytes(), "first\ntable cell"),
        ):
            path = tmp_path / kind
            path.write_bytes(data)
            assert (await read_document(path, kind)).strip() == expected

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "content,category",
    [
        (b"bad", "document_binary_or_invalid"),
        (pdf_bytes(encrypted=True), "document_encrypted"),
        (pdf_bytes(text=None), "document_no_text"),
        (pdf_bytes(text=None, scanned=True), "document_scan_needs_ocr"),
    ],
)
def test_pdf_failure_is_explicit(content, category):
    with pytest.raises(DocumentError, match=category):
        extract_text(content, "pdf")


@pytest.mark.parametrize(
    "xml",
    [
        b"<!DOCTYPE x [<!ENTITY e 'bad'>]><x>&e;</x>",
        "<!DOCTYPE x [<!ENTITY e 'bad'>]><x>&e;</x>".encode("utf-16"),
        b"<broken>",
        b"x" * 2_000_001,
    ],
)
def test_docx_malformed_entity_and_zip_expansion_are_rejected(xml):
    with pytest.raises(DocumentError):
        extract_text(docx_bytes(xml=xml), "docx")


def test_child_timeout_kills_and_reaps_process(monkeypatch):
    async def scenario():
        process = AsyncMock()
        process.returncode = None
        process.kill = lambda: None
        process.communicate.side_effect = TimeoutError
        monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
        with pytest.raises(DocumentError, match="document_parser_limit"):
            await read_document(Path("unused"), "pdf")
        process.wait.assert_awaited_once()

    asyncio.run(scenario())


def test_parser_child_uses_fixed_args_and_does_not_inherit_credentials(monkeypatch):
    async def scenario():
        process = AsyncMock()
        process.returncode = 0
        process.communicate.return_value = (b'{"text":"safe"}', b"")
        launch = AsyncMock(return_value=process)
        monkeypatch.setenv("BOT_TOKEN", "private-fixture")
        monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
        assert await read_document(Path("unused"), "docx") == "safe"
        args, kwargs = launch.await_args
        assert args[1:] == ("-m", "vuzol.interpretation.documents", "unused", "docx")
        assert set(kwargs["env"]) == {"PYTHONPATH", "LANG", "PYTHONDONTWRITEBYTECODE"}
        assert "private-fixture" not in repr(kwargs)

    asyncio.run(scenario())
