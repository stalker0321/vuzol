"""Operator-only Telegram dogfood session and fault controls."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import uuid
from pathlib import Path

from vuzol.config import TopicKind, get_runtime_configuration
from vuzol.execution.git import LocalGit
from vuzol.ops.telegram_dogfood import (
    DogfoodFault,
    arm_fault,
    build_report,
    diagnose_package,
    start_session,
)
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head

SOURCE_CHECKOUT = Path(__file__).resolve().parents[3]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Operate an allowlisted Telegram dogfood session")
    subparsers = parser.add_subparsers(dest="command", required=True)
    start = subparsers.add_parser("start")
    start.add_argument("--project", required=True)
    report = subparsers.add_parser("report")
    report.add_argument("--session", required=True, type=uuid.UUID)
    fault = subparsers.add_parser("arm-fault")
    fault.add_argument("--session", required=True, type=uuid.UUID)
    fault.add_argument("--project", required=True)
    fault.add_argument("--fault", required=True, choices=tuple(DogfoodFault))
    diagnose = subparsers.add_parser("diagnose")
    diagnose.add_argument("--package", required=True, type=uuid.UUID)
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    engine = create_engine(settings, resolve_database_dsn(settings))
    try:
        await require_migration_head(engine)
        factory = create_session_factory(engine)
        if args.command == "start":
            project = runtime.registries.projects.get(args.project)
            topics = tuple(
                topic
                for topic in runtime.registries.topics.items()
                if topic.kind is TopicKind.PROJECT
                and topic.project_id == project.id
                and topic.enabled
            )
            if len(topics) != 1:
                raise ValueError("dogfood project must have exactly one enabled project topic")
            git_sha = await LocalGit().resolve_commit(SOURCE_CHECKOUT, "HEAD")
            async with factory.begin() as session:
                session_id = await start_session(
                    session,
                    settings.telegram_dogfood,
                    project_id=project.id,
                    configuration_revision=runtime.registries.revision,
                    git_sha=git_sha,
                    actor_id=getpass.getuser(),
                )
            print(json.dumps({"session_id": str(session_id), "project_id": project.id}))
            return 0
        if args.command == "arm-fault":
            async with factory.begin() as session:
                fault_id = await arm_fault(
                    session,
                    settings.telegram_dogfood,
                    session_id=args.session,
                    project_id=args.project,
                    fault=DogfoodFault(args.fault),
                    actor_id=getpass.getuser(),
                )
            print(json.dumps({"fault_id": str(fault_id), "fault": args.fault}))
            return 0
        async with factory() as session:
            payload = (
                (await diagnose_package(session, settings.telegram_dogfood, args.package)).to_dict()
                if args.command == "diagnose"
                else (
                    await build_report(session, settings.telegram_dogfood, args.session)
                ).to_dict()
            )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> None:
    raise SystemExit(asyncio.run(_run(_parse_args(argv))))


if __name__ == "__main__":
    main()
