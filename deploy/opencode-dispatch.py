#!/usr/bin/python3
"""Host `opencode`: the native CLI, except for the OpenCode login Vuzol shares.

That login (``xdg_root`` in the usage exporter's config) goes through
``vuzol.cli.opencode_owner``, which takes the exporter's lock, so only one process
refreshes the login at a time. Install as /usr/local/bin/opencode, mode 0755.
"""

import json
import os
import sys

NATIVE = "/usr/local/lib/node_modules/opencode-ai/bin/opencode.exe"
CONFIG = "/etc/vuzol/opencode-usage-c.json"
PYTHON = "/opt/vuzol/current/.venv/bin/python"

try:
    with open(CONFIG, encoding="utf-8") as handle:
        shared = os.path.join(json.load(handle)["xdg_root"], "data")
except (OSError, ValueError, KeyError):
    shared = None
data = os.environ.get("XDG_DATA_HOME", "")
if shared and data and os.path.realpath(data) == os.path.realpath(shared):
    os.execve(  # noqa: S606 - fixed interpreter and module
        PYTHON,
        [PYTHON, "-m", "vuzol.cli.opencode_owner", "--config", CONFIG, "--", *sys.argv[1:]],
        {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
os.execve(NATIVE, [NATIVE, *sys.argv[1:]], os.environ)  # noqa: S606 - the native CLI
