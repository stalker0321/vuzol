"""Real-process regression tests for the managed runtime preview.

These tests exercise the full publish path: a real git repository exports its
retained result commit into a per-run runtime directory, and the real runtime
binary starts under Landlock confinement. They guard the failure mode where a
project writes next to its own files (the ``three-body-problem`` incident):
writes inside the per-run directory must succeed while writes anywhere else
must be denied.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from vuzol.security import landlock
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry, RuntimePreviewHandler

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(shutil.which("node") is None, reason="node runtime is required"),
    pytest.mark.skipif(
        landlock.landlock_abi_version() < 1, reason="kernel Landlock support is required"
    ),
]

SERVER_JS = """
const fs = require("fs");
const http = require("http");
const path = require("path");

const HOST = process.env.HOST || "127.0.0.1";
const PORT = Number(process.env.PORT || 8080);
const LOG_DIR = path.join(__dirname, "local");
fs.mkdirSync(LOG_DIR, { recursive: true });
fs.writeFileSync(path.join(LOG_DIR, "preview.ndjson"), "boot\\n");

let escape_blocked = false;
try {
  const target = path.resolve(__dirname, "..", "..", "escape-canary.txt");
  fs.writeFileSync(target, "escaped\\n");
} catch (error) {
  escape_blocked = error !== null && typeof error === "object" &&
    ["EPERM", "EACCES", "EROFS"].includes(error.code);
}

const server = http.createServer((request, response) => {
  if (request.url === "/probe") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ escape_blocked }));
    return;
  }
  response.writeHead(200, { "content-type": "text/plain" });
  response.end("ok");
});
server.listen(PORT, HOST);
"""


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _request() -> StepExecutionRequest:
    return cast(StepExecutionRequest, SimpleNamespace(task_id="task", run_id="run"))


def _handler(tmp_path: Path) -> tuple[RuntimePreviewHandler, MagicMock, MagicMock]:
    read_session = MagicMock()
    write_session = MagicMock()
    factory = MagicMock(return_value=AsyncContext(read_session))
    factory.begin.return_value = AsyncContext(write_session)
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            preview_site_root=tmp_path / "previews",
            preview_site_base_url="https://test.example",
            repository_root=tmp_path / "repositories",
            preview_export_max_bytes=10_000_000,
            preview_export_max_files=1_000,
            capability_provisioning=SimpleNamespace(toolchain_root=tmp_path / "toolchains"),
        )
    )
    handler = RuntimePreviewHandler(
        cast(Any, factory), cast(Any, runtime), PreviewRuntimeRegistry()
    )
    return handler, read_session, write_session


def _environment() -> SimpleNamespace:
    return SimpleNamespace(
        contract={
            "components": {
                "web": {
                    "kind": "web_service",
                    "run_command": ["node", "server.js"],
                    "healthcheck_path": "/",
                }
            },
            "capabilities": {
                "node-runtime": {"label": "Node.js runtime", "provisioning": "automatic"}
            },
        }
    )


def _git_repository(tmp_path: Path) -> tuple[Path, str]:
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / "server.js").write_text(SERVER_JS.lstrip(), encoding="utf-8")
    (repository / "untracked.txt").write_text("not committed\n", encoding="utf-8")
    subprocess.run(("git", "init", "-b", "main", str(repository)), check=True)
    subprocess.run(("git", "-C", str(repository), "config", "user.name", "Test"), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "config", "user.email", "test@example.invalid"),
        check=True,
    )
    subprocess.run(("git", "-C", str(repository), "add", "server.js"), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "commit", "-m", "server"),
        check=True,
        capture_output=True,
    )
    completed = subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    )
    return repository, completed.stdout.strip()


async def test_preview_serves_materialized_commit_and_confines_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    repository, commit = _git_repository(tmp_path)
    handler, read_session, _write = _handler(tmp_path)
    read_session.get = AsyncMock(return_value=SimpleNamespace(project_id="preview-regression"))
    read_session.scalar = AsyncMock(
        side_effect=[
            SimpleNamespace(path=str(repository), result_commit=commit),
            None,
        ]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )

    outcome = await handler.execute(_request(), CancellationContext())
    try:
        assert outcome.kind is OutcomeKind.SUCCEEDED, outcome.summary
        assert outcome.result["source_commit"] == commit
        target = handler._registry.targets["preview-regression"]
        assert target.runtime_dir is not None
        app_dir = target.runtime_dir / "app"

        assert (app_dir / "server.js").is_file()
        assert not (app_dir / "untracked.txt").exists()
        assert not (app_dir / ".git").exists()
        assert (app_dir / "local" / "preview.ndjson").is_file()
        assert not (repository / "local").exists()

        async with httpx.AsyncClient(timeout=5.0) as client:
            root = await client.get(f"http://127.0.0.1:{target.port}/")
            assert root.status_code == 200
            probe = await client.get(f"http://127.0.0.1:{target.port}/probe")
            assert probe.json() == {"escape_blocked": True}
        assert not list(tmp_path.rglob("escape-canary.txt"))  # noqa: ASYNC240
    finally:
        await handler._registry.close()
