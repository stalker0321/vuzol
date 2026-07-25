"""Thin synchronous CLI for Grok limit snapshot export (S1b).

Wraps the accepted S1a library. No network, no settings/registry load, no auth
reads beyond what the library already forbids. Failure surfaces are fixed codes
only (no paths, principals, digests, or exception text on stdout/stderr).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from grp import getgrnam
from pathlib import Path
from typing import NoReturn

from vuzol.providers.grok_limit_snapshot import (
    GrokLimitSnapshotError,
    export_grok_limit_snapshot,
    load_bindings,
)

ENV_BINDINGS = "VUZOL_GROK_LIMIT_BINDINGS_FILE"
ENV_PROFILES_ROOT = "VUZOL_GROK_LIMIT_PROFILES_ROOT"
ENV_OUTPUT = "VUZOL_GROK_LIMIT_OUTPUT_FILE"
DEFAULT_EXECUTOR_GROUP = "vuzol-executor"

# CLI-only fixed codes (config / host identity) — never secrets or paths.
CODE_CLI_CONFIG = "limits_export_cli_config_invalid"
CODE_CLI_ROOT_MISMATCH = "limits_export_profiles_root_mismatch"
CODE_CLI_GROUP = "limits_export_group_unresolved"


def main(argv: list[str] | None = None) -> None:
    raise SystemExit(run(argv))


def run(argv: list[str] | None = None) -> int:
    """Return process exit code (0 success, 1 failure)."""

    try:
        args = _parse_args(argv)
    except _CliUsageError:
        _fail(CODE_CLI_CONFIG)
        return 1
    except SystemExit as error:
        # --help / clean exits
        if error.code in {0, None}:
            return 0
        _fail(CODE_CLI_CONFIG)
        return 1

    try:
        bindings_file, profiles_root, output_file = _resolve_paths(args)
        expected_gid = _resolve_group_gid(args.executor_group)
        expected_uid = os.getuid()
        document = load_bindings(bindings_file)
        if document.profiles_root != profiles_root:
            _fail(CODE_CLI_ROOT_MISMATCH)
            return 1
        count = export_grok_limit_snapshot(
            bindings_file,
            output_file,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
    except GrokLimitSnapshotError as error:
        _fail(error.code)
        return 1
    except OSError:
        _fail(CODE_CLI_CONFIG)
        return 1

    # Success: stable JSON only — no paths, digests, principals, or tokens.
    sys.stdout.write(json.dumps({"status": "ok", "entry_count": count}, sort_keys=True) + "\n")
    return 0


class _CliUsageError(Exception):
    """Argparse usage failure mapped to a fixed CLI code (no path echo)."""


class _QuietParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message  # may contain user-supplied paths — never echo
        raise _CliUsageError


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = _QuietParser(
        prog="vuzol-grok-limit-exporter",
        description="Export a sanitized Grok limit snapshot from bindings (oneshot).",
    )
    parser.add_argument(
        "--bindings-file",
        default=None,
        help=f"Absolute bindings JSON path (env {ENV_BINDINGS})",
    )
    parser.add_argument(
        "--profiles-root",
        default=None,
        help=f"Absolute profiles root; must match bindings (env {ENV_PROFILES_ROOT})",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help=f"Absolute snapshot output path (env {ENV_OUTPUT})",
    )
    parser.add_argument(
        "--executor-group",
        default=DEFAULT_EXECUTOR_GROUP,
        help=f"Expected snapshot file group name (default {DEFAULT_EXECUTOR_GROUP})",
    )
    return parser.parse_args(argv)


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    bindings_raw = args.bindings_file or os.environ.get(ENV_BINDINGS)
    root_raw = args.profiles_root or os.environ.get(ENV_PROFILES_ROOT)
    output_raw = args.output_file or os.environ.get(ENV_OUTPUT)
    if not bindings_raw or not root_raw or not output_raw:
        raise GrokLimitSnapshotError(CODE_CLI_CONFIG, "missing path")
    bindings_file = Path(bindings_raw)
    profiles_root = Path(root_raw)
    output_file = Path(output_raw)
    for path in (bindings_file, profiles_root, output_file):
        if not path.is_absolute():
            raise GrokLimitSnapshotError(CODE_CLI_CONFIG, "relative path")
    return bindings_file, profiles_root, output_file


def _resolve_group_gid(group_name: str) -> int:
    if not group_name or not isinstance(group_name, str):
        raise GrokLimitSnapshotError(CODE_CLI_GROUP, "group name")
    try:
        return int(getgrnam(group_name).gr_gid)
    except KeyError as error:
        raise GrokLimitSnapshotError(CODE_CLI_GROUP, "group missing") from error
    except OSError as error:
        raise GrokLimitSnapshotError(CODE_CLI_GROUP, "group lookup") from error


def _fail(code: str) -> None:
    """Write only a fixed code to stderr."""

    sys.stderr.write(f"{code}\n")


if __name__ == "__main__":
    main()
