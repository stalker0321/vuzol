"""Operator OpenCode launcher sharing the usage exporter's auth-owner lock."""

import argparse
import os
from pathlib import Path

from vuzol.cli.opencode_usage_export import ExportConfig, cli_environment, owner_lock
from vuzol.providers.account_auth import AccountAuthError


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        config = ExportConfig.model_validate_json(args.config.read_bytes())
        arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
        with owner_lock(config.lock_file, wait=True) as lock_fd:
            # The native process retains the lock through Ctrl-C, parent exits,
            # and terminal lifetime. No Python supervisor can release it early.
            os.set_inheritable(lock_fd, True)
            os.execve(  # noqa: S606
                config.cli_binary,
                [str(config.cli_binary), *arguments],
                {**os.environ, **cli_environment(config)},
            )
    except (OSError, ValueError, AccountAuthError):
        print("OpenCode auth owner unavailable; check usage exporter configuration.")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
