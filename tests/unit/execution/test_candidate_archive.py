import io
import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from vuzol.execution.git import GitError, LocalGit
from vuzol.telegram.adapter import PythonTelegramClient
from vuzol.telegram.projections import LostTelegramResponse


def committed(root: Path) -> str:
    for argv in (
        ("init", "-b", "main"),
        ("config", "user.name", "Test"),
        ("config", "user.email", "test@example.com"),
        ("add", "."),
        ("commit", "-m", "candidate"),
    ):
        subprocess.run(("git", "-C", str(root), *argv), check=True, capture_output=True)
    return subprocess.check_output(("git", "-C", str(root), "rev-parse", "HEAD")).decode().strip()


@pytest.mark.anyio
async def test_archive_contains_only_retained_commit(tmp_path):
    (tmp_path / "index.html").write_text("checked candidate")
    commit = committed(tmp_path)
    (tmp_path / "index.html").write_text("not checked")
    (tmp_path / "untracked.txt").write_text("do not deliver")
    content = await LocalGit().source_archive(tmp_path, commit, max_bytes=4096)
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        assert archive.namelist() == ["index.html"]
        assert archive.read("index.html") == b"checked candidate"


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["symlink", "oversize", "not-commit"])
async def test_archive_rejects_unsafe_or_unbounded_source(tmp_path, kind):
    (tmp_path / "index.html").write_text("x" * 1024)
    if kind == "symlink":
        (tmp_path / "link").symlink_to("index.html")
    commit = committed(tmp_path)
    with pytest.raises(GitError):
        await LocalGit().source_archive(
            tmp_path,
            "HEAD" if kind == "not-commit" else commit,
            max_bytes=100 if kind == "oversize" else 4096,
        )


@pytest.mark.anyio
async def test_document_adapter_preserves_bytes_and_confirmation():
    bot = SimpleNamespace(send_document=AsyncMock(return_value=SimpleNamespace(message_id=17)))
    result = await PythonTelegramClient(bot).send_document(
        chat_id=-100,
        thread_id=577,
        content=b"checked zip",
        filename="candidate.zip",
        caption="Checked candidate",
    )
    assert result == 17
    sent = bot.send_document.call_args.kwargs
    assert sent["document"].getvalue() == b"checked zip"
    assert sent["message_thread_id"] == 577
    bot.send_document.return_value = SimpleNamespace(message_id=0)
    with pytest.raises(LostTelegramResponse):
        await PythonTelegramClient(bot).send_document(
            chat_id=-100,
            thread_id=577,
            content=b"zip",
            filename="candidate.zip",
            caption="test",
        )
