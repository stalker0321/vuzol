"""Pi sandbox smoke: real container, mock provider, no external network.

Builds a throwaway image with @earendil-works/pi-coding-agent@1.0.3 and runs the
one-shot ``-p --mode json`` path against a local mock OpenAI-SSE server on the
host network. Verifies the agent_settled terminal and usage in agent_end.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

pytestmark = pytest.mark.docker

PI_VERSION = "1.0.3"
PORT = 8899


def _docker(
    *args: str, timeout: int = 600, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False, input=stdin
    )


class _MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args: object) -> None:
        pass

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", 0))
        self.rfile.read(length)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()

        def sse(obj: object) -> bytes:
            return f"data: {json.dumps(obj)}\n\n".encode()

        def write(chunk: bytes) -> None:
            self.wfile.write(f"{len(chunk):X}\r\n".encode() + chunk + b"\r\n")
            self.wfile.flush()

        base = {
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "m",
        }
        write(
            sse(
                {
                    **base,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": "sandbox reply"},
                            "finish_reason": None,
                        }
                    ],
                }
            )
        )
        write(sse({**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}))
        write(
            sse(
                {
                    **base,
                    "choices": [],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
                }
            )
        )
        write(b"data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")


@pytest.fixture(scope="module")
def mock_server() -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", PORT), _MockHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()


@pytest.fixture(scope="module")
def pi_image() -> Iterator[str]:
    if shutil.which("docker") is None:
        pytest.skip("docker is unavailable")
    tag = f"vuzol-pi-smoke-{uuid.uuid4().hex[:12]}"
    dockerfile = (
        "FROM node:22-bookworm-slim\n"
        f"RUN npm install --global @earendil-works/pi-coding-agent@{PI_VERSION}\n"
        "RUN mkdir -p /pi-home && chown 10001:10001 /pi-home\n"
        "USER 10001:10001\n"
    )
    tmp = Path(tempfile.mkdtemp(prefix="pi-smoke-"))
    (tmp / "Dockerfile").write_text(dockerfile)
    build = _docker("build", "-t", tag, str(tmp))
    if build.returncode != 0:
        pytest.skip(f"cannot build pi smoke image: {build.stderr[-300:]}")
    try:
        yield tag
    finally:
        _docker("image", "rm", "-f", tag)


def test_pi_runs_in_container_with_mock_provider(pi_image: str, mock_server: object) -> None:
    del mock_server
    work = Path(tempfile.mkdtemp(prefix="pi-smoke-work-"))
    (work / "pihome").mkdir(parents=True)
    (work / "pihome").chmod(0o777)
    (work / "pihome" / "models.json").write_text(
        json.dumps(
            {
                "providers": {
                    "mockprobe": {
                        "baseUrl": f"http://127.0.0.1:{PORT}/v1",
                        "api": "openai-completions",
                        "apiKey": "probe-key-not-real",
                        "authHeader": True,
                        "models": [
                            {
                                "id": "mock-model",
                                "name": "Mock",
                                "reasoning": False,
                                "input": ["text"],
                                "contextWindow": 128000,
                                "maxTokens": 8192,
                                "cost": {
                                    "input": 1,
                                    "output": 2,
                                    "cacheRead": 0.1,
                                    "cacheWrite": 0,
                                },
                            }
                        ],
                    }
                }
            }
        )
    )
    time.sleep(0.5)
    run = _docker(
        "run",
        "--rm",
        "--network",
        "host",
        "-i",
        "-e",
        "PI_CODING_AGENT_DIR=/pi-home",
        "-e",
        "PI_OFFLINE=1",
        "-v",
        f"{work / 'pihome'}:/pi-home",
        pi_image,
        "pi",
        "-p",
        "--mode",
        "json",
        "--no-session",
        "--provider",
        "mockprobe",
        "--model",
        "mock-model",
        stdin="sandbox probe",
    )
    assert run.returncode == 0, run.stderr
    events = [json.loads(line) for line in run.stdout.splitlines() if line.strip()]
    types = [event.get("type") for event in events]
    assert "agent_settled" in types
    assert types[-1] == "agent_settled"
    final = next(event for event in reversed(events) if event.get("type") == "agent_end")
    assistant = final["messages"][-1]
    assert assistant["stopReason"] == "stop"
    assert assistant["usage"]["input"] == 100
    assert assistant["usage"]["output"] == 20
