"""Operator OpenCode launcher sharing the usage exporter's auth-owner lock.

Log an account in once, and its limits are read from then on:

    python -m vuzol.cli.opencode_owner --account opencode-go-a -- console login
"""

import argparse
import os
import re
from pathlib import Path

from vuzol.cli.opencode_usage_export import (
    ExportConfig,
    Setup,
    cli_environment,
    login_root,
    owner_lock,
)
from vuzol.providers.account_auth import AccountAuthError

SETUP = Path("/etc/vuzol/opencode-usage.json")
_ACCOUNT = re.compile(r"^[a-z][a-z0-9_-]{0,99}$")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="one account's export config")
    parser.add_argument("--setup", type=Path, default=SETUP)
    parser.add_argument("--account", help="an account id of the registry, e.g. opencode-go-a")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.account is not None:
            if not _ACCOUNT.match(args.account):
                raise ValueError("account id")
            setup = Setup.model_validate_json(args.setup.read_bytes())
            root = login_root(setup, args.account)
            (root / "data/opencode").mkdir(mode=0o700, parents=True, exist_ok=True)
            binary, lock = setup.cli_binary, root / "data/opencode/vuzol-owner.lock"
        else:
            if args.config is None:
                raise ValueError("--account or --config is required")
            config = ExportConfig.model_validate_json(args.config.read_bytes())
            binary, root, lock = config.cli_binary, config.xdg_root, config.lock_file
        environment = cli_environment(root)
        arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
        with owner_lock(lock, wait=True) as lock_fd:
            # The native process retains the lock through Ctrl-C, parent exits,
            # and terminal lifetime. No Python supervisor can release it early.
            os.set_inheritable(lock_fd, True)
            os.execve(  # noqa: S606
                binary, [str(binary), *arguments], {**os.environ, **environment}
            )
    except (OSError, ValueError, AccountAuthError):
        print("OpenCode auth owner unavailable; check usage exporter configuration.")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
