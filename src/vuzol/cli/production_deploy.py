"""Root-only production deployment entry point."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from vuzol.ops.production_deploy import DeploymentConfig, DeploymentError, ProductionDeployer


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy one reviewed Vuzol commit to production")
    parser.add_argument("--sha", required=True, help="full reviewed Git commit SHA")
    parser.add_argument("--source", type=Path, default=Path.cwd())
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if os.geteuid() != 0:
        raise SystemExit("vuzol-production-deploy must run as root")
    try:
        result = ProductionDeployer(DeploymentConfig(source=args.source.resolve())).deploy(args.sha)
    except DeploymentError as error:
        raise SystemExit(str(error)) from error
    print(
        json.dumps(
            {
                "previous_sha": result.previous_sha,
                "deployed_sha": result.deployed_sha,
                "rolled_back": result.rolled_back,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
