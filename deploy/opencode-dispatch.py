#!/usr/bin/python3
"""Preserve native OpenCode; serialize the explicitly managed C auth store."""
import os
import sys

NATIVE = "/usr/local/lib/node_modules/opencode-ai/bin/opencode.exe"
DATA = "/home/vodkolyan/.opencode-profiles/3/data"
if os.getuid() == 1000 and os.path.realpath(os.environ.get("XDG_DATA_HOME", "")) == DATA:
    python = "/opt/vuzol/releases/t137-r2/.venv/bin/python"
    os.execve(python, [python, "-m", "vuzol.cli.opencode_owner", "--config",
                      "/etc/vuzol/opencode-usage-c.json", "--", *sys.argv[1:]],
              {**os.environ, "PYTHONPATH": "/opt/vuzol/releases/t138/src",
               "PYTHONDONTWRITEBYTECODE": "1"})
os.execve(NATIVE, [NATIVE, *sys.argv[1:]], os.environ)
