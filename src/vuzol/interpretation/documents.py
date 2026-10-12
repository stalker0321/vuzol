"""Bounded text extraction; document bytes are data, never executable content."""

from __future__ import annotations

import asyncio
import io
import json
import resource
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

TEXT_SUFFIXES = {".txt", ".md", ".markdown", ".csv", ".json"}
TEXT_MEDIA = {"text/plain", "text/markdown", "text/x-markdown", "text/csv", "application/json"}
DOCX_MEDIA = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
MAX_TEXT_CHARS = 40_000
MAX_XML_BYTES = 2_000_000
MAX_PDF_PAGES = 100
PARSER_TIMEOUT_SECONDS = 15


class DocumentError(ValueError):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def document_format(filename: str | None, media_type: str) -> str:
    suffix = Path(filename or "").suffix.lower()
    if suffix in TEXT_SUFFIXES and media_type in TEXT_MEDIA | {"application/octet-stream"}:
        return "text"
    if suffix == ".docx" and media_type in {
        DOCX_MEDIA,
        "application/octet-stream",
        "application/zip",
    }:
        return "docx"
    if media_type == "application/pdf" and suffix in {"", ".pdf"}:
        return "pdf"
    if suffix == ".pdf" and media_type == "application/octet-stream":
        return "pdf"
    if not suffix and media_type in TEXT_MEDIA:
        return "text"
    raise DocumentError("document_format_unsupported")


def _check_text(text: str) -> str:
    if not text.strip():
        raise DocumentError("document_no_text")
    if len(text) > MAX_TEXT_CHARS:
        raise DocumentError("document_text_too_large")
    if any(ord(c) < 32 and c not in "\n\r\t" for c in text):
        raise DocumentError("document_binary_or_invalid")
    return text


def extract_text(content: bytes, kind: str) -> str:
    """Executed inside a resource-limited child for PDF/DOCX, not the event loop."""
    try:
        if kind == "text":
            if len(content) > MAX_TEXT_CHARS * 4 + 4:
                raise DocumentError("document_text_too_large")
            encoding = "utf-16" if content.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
            return _check_text(content.decode(encoding))
        if kind == "docx":
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                if len(archive.infolist()) > 1000:
                    raise DocumentError("document_archive_too_large")
                info = archive.getinfo("word/document.xml")
                if info.file_size > MAX_XML_BYTES or info.flag_bits & 1:
                    raise DocumentError("document_archive_too_large")
                xml = archive.read(info)
            encoding = "utf-16" if xml.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
            xml_text = xml.decode(encoding)
            if (
                "\x00" in xml_text
                or "<!DOCTYPE" in xml_text.upper()
                or "<!ENTITY" in xml_text.upper()
            ):
                raise DocumentError("document_binary_or_invalid")
            root = ET.fromstring(xml_text)  # noqa: S314 - no DTD/entities; bounded child/XML
            w = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
            paragraphs = []
            for paragraph in root.iter(w + "p"):
                paragraphs.append(
                    "".join(
                        node.text or ""
                        if node.tag == w + "t"
                        else "\t"
                        if node.tag == w + "tab"
                        else "\n"
                        for node in paragraph.iter()
                        if node.tag in {w + "t", w + "tab", w + "br"}
                    )
                )
            return _check_text("\n".join(paragraphs))
        if kind == "pdf":
            from pypdf import PdfReader

            if not content.startswith(b"%PDF-"):
                raise DocumentError("document_binary_or_invalid")
            reader = PdfReader(io.BytesIO(content), strict=True)
            if reader.is_encrypted:
                raise DocumentError("document_encrypted")
            if len(reader.pages) > MAX_PDF_PAGES:
                raise DocumentError("document_too_many_pages")
            pages = []
            for page in reader.pages:
                text = page.extract_text() or ""
                # Do not claim to read a mixed scanned/text PDF completely.
                if not text.strip() and page.images:
                    raise DocumentError("document_scan_needs_ocr")
                pages.append(text)
                if sum(map(len, pages)) > MAX_TEXT_CHARS:
                    raise DocumentError("document_text_too_large")
            return _check_text("\n\n".join(pages))
        raise DocumentError("document_format_unsupported")
    except DocumentError:
        raise
    except Exception as error:
        raise DocumentError("document_binary_or_invalid") from error


async def read_document(path: Path, kind: str) -> str:
    if kind == "text":
        return extract_text(await asyncio.to_thread(path.read_bytes), kind)
    # No user-controlled program/arguments, shell, archive extraction or external
    # URLs. CPU/address-space bounds are installed in the child before parsing.
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "vuzol.interpretation.documents",
        str(path),
        kind,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env={"PYTHONPATH": ":".join(sys.path), "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), PARSER_TIMEOUT_SECONDS)
    except (TimeoutError, asyncio.CancelledError) as error:
        if process.returncode is None:
            process.kill()
        await process.wait()
        if isinstance(error, asyncio.CancelledError):
            raise
        raise DocumentError("document_parser_limit") from error
    if process.returncode != 0 or len(stdout) > MAX_TEXT_CHARS * 6 + 1024:
        raise DocumentError("document_parser_limit")
    try:
        result = json.loads(stdout)
        if "error" in result:
            raise DocumentError(result["error"])
        return _check_text(result["text"])
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise DocumentError("document_binary_or_invalid") from error


def _main() -> None:
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_CPU, (10, 10))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    try:
        result = {"text": extract_text(Path(sys.argv[1]).read_bytes(), sys.argv[2])}
    except DocumentError as error:
        result = {"error": error.category}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    _main()
