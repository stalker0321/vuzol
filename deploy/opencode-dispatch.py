#!/usr/bin/python3
"""Host `opencode`: the native CLI, except for the OpenCode logins Vuzol shares.

Those logins (``accounts_root/<account_id>`` in the usage exporter's setup) go through
``vuzol.cli.opencode_owner``, which takes that account's exporter lock, so only one
process refreshes a login at a time. Install as /usr/local/bin/opencode, mode 0755.
"""

import json
import os
import sys

NATIVE = "/usr/local/lib/node_modules/opencode-ai/bin/opencode.exe"
SETUP = "/etc/vuzol/opencode-usage.json"
PYTHON = "/opt/vuzol/current/.venv/bin/python"

try:
    with open(SETUP, encoding="utf-8") as handle:
        accounts = os.path.realpath(json.load(handle)["accounts_root"])
except (OSError, ValueError, KeyError):
    accounts = None
data = os.environ.get("XDG_DATA_HOME", "")
if accounts and data:
    login, leaf = os.path.split(os.path.realpath(data))
    if leaf == "data" and os.path.dirname(login) == accounts:
        os.execve(  # noqa: S606 - fixed interpreter and module
            PYTHON,
            [
                PYTHON,
                "-m",
                "vuzol.cli.opencode_owner",
                "--account",
                os.path.basename(login),
                "--",
                *sys.argv[1:],
            ],
            {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
os.execve(NATIVE, [NATIVE, *sys.argv[1:]], os.environ)  # noqa: S606 - the native CLI
