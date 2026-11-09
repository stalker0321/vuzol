"""Codex task workers get the executor's MCP servers; reviews and other CLIs do not."""

import pytest

from vuzol.config.settings import ExecutionSettings
from vuzol.execution.codex import _require_provider_command
from vuzol.providers.codex import canonical_codex_argv
from vuzol.providers.handlers import worker_mcp_servers

DOCS = {"context7": "https://mcp.context7.com/mcp"}


def test_only_codex_task_work_gets_worker_mcp_servers():
    assert worker_mcp_servers("codex", "execute_code", DOCS) == DOCS
    assert worker_mcp_servers("codex", "review", DOCS) == {}
    assert worker_mcp_servers("pi", "execute_code", DOCS) == {}
    assert ExecutionSettings().worker_mcp_servers == {}


def test_the_sandbox_accepts_exactly_the_configured_servers():
    argv = canonical_codex_argv(model="m", mcp_servers=DOCS)
    _require_provider_command(argv, "codex", "m", mcp_servers=DOCS)
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(argv, "codex", "m")
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(canonical_codex_argv(model="m"), "codex", "m", mcp_servers=DOCS)
