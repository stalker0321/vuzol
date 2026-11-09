"""Daily health summary for the owner: what needs attention, in one Telegram message.

Problems used to pile up silently (a disk at 91%, runs blocked for weeks) until someone
went digging. Host facts (disk, systemd units, the OpenCode usage export) and database
facts are gathered separately so each part can be tested without the other.
"""

from __future__ import annotations

import html
import json
import os
import subprocess
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import LeadJob, ManagedGoal
from vuzol.storage.models import ModelCall, Run, Step, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus, RunStatus, StepStatus
from vuzol.telegram.attention import ATTENTION_REASONS

DISK_WARN_PERCENT = 85.0
DISK_FAIL_PERCENT = 92.0
BACKUP_MAX_AGE = timedelta(hours=26)
USAGE_EXPORT_MAX_AGE = timedelta(minutes=15)
QUOTA_WARN_PERCENT = 80.0
STUCK_LEASE_GRACE = timedelta(minutes=10)
STUCK_QUEUE_AGE = timedelta(hours=1)
WINDOW = timedelta(hours=24)
# Owner-chosen pauses are not problems.
_QUIET_WAITING_REASONS = frozenset({"owner_pause"})

Level = Literal["ok", "warn", "fail"]


@dataclass(frozen=True, slots=True)
class Finding:
    level: Level
    text: str


@dataclass(frozen=True, slots=True)
class UnitState:
    active: str
    restarts: int = 0
    result: str = "success"
    last_exit: datetime | None = None


@dataclass(frozen=True, slots=True)
class HostFacts:
    disk_used_percent: float
    services: dict[str, UnitState] = field(default_factory=dict)
    backup: UnitState | None = None
    usage_snapshot: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class DatabaseFacts:
    blocked_runs: int = 0
    oldest_blocked: datetime | None = None
    stuck_steps: int = 0
    waiting_goals: tuple[tuple[str, str], ...] = ()
    failed_jobs: dict[str, int] = field(default_factory=dict)
    failed_model_calls: dict[str, int] = field(default_factory=dict)
    undelivered: dict[str, int] = field(default_factory=dict)


def evaluate(host: HostFacts, db: DatabaseFacts, *, now: datetime) -> list[Finding]:
    findings: list[Finding] = []
    disk = host.disk_used_percent
    if disk >= DISK_WARN_PERCENT:
        level: Level = "fail" if disk >= DISK_FAIL_PERCENT else "warn"
        findings.append(Finding(level, f"Диск занят на {disk:.0f}%"))
    for name, unit in sorted(host.services.items()):
        if unit.active != "active":
            findings.append(Finding("fail", f"{name}: {unit.active}"))
        elif unit.restarts:
            findings.append(Finding("warn", f"{name}: перезапусков {unit.restarts}"))
    if host.backup is None or host.backup.last_exit is None:
        findings.append(Finding("fail", "Бэкап ещё ни разу не завершился"))
    elif host.backup.result != "success":
        findings.append(Finding("fail", f"Последний бэкап упал: {host.backup.result}"))
    elif now - host.backup.last_exit > BACKUP_MAX_AGE:
        findings.append(Finding("fail", f"Бэкапа не было {_age(now - host.backup.last_exit)}"))
    findings.extend(_usage(host.usage_snapshot, now=now))
    if db.blocked_runs:
        oldest = f", старейший {_age(now - db.oldest_blocked)}" if db.oldest_blocked else ""
        findings.append(Finding("warn", f"Задач ждут решения: {db.blocked_runs}{oldest}"))
    if db.stuck_steps:
        findings.append(Finding("fail", f"Зависших шагов: {db.stuck_steps}"))
    for project, reason in db.waiting_goals:
        label = ATTENTION_REASONS[reason][1] if reason in ATTENTION_REASONS else reason
        findings.append(Finding("warn", f"Цель в {project}: {label}"))
    for title, counts in (
        ("Исследования упали", db.failed_jobs),
        ("Вызовы моделей упали", db.failed_model_calls),
        ("Сообщения не доставлены", db.undelivered),
    ):
        if counts:
            detail = ", ".join(f"{key}: {count}" for key, count in sorted(counts.items()))
            findings.append(Finding("warn", f"{title} за сутки: {detail}"))
    return findings


def render(findings: list[Finding]) -> str:
    if not findings:
        return "🟢 <b>Vuzol: всё в порядке</b>"
    worst = "fail" if any(f.level == "fail" for f in findings) else "warn"
    icon = {"fail": "🔴", "warn": "🟡"}
    lines = [f"{icon[worst]} <b>Vuzol: требует внимания ({len(findings)})</b>"]
    ordered = sorted(findings, key=lambda f: f.level != "fail")
    lines.extend(f"{icon[f.level]} {html.escape(f.text)}" for f in ordered)
    return "\n".join(lines)


def _usage(snapshot: dict[str, object] | None, *, now: datetime) -> list[Finding]:
    if snapshot is None:
        return []
    generated = _parse_time(snapshot.get("generated_at"))
    if generated is None or now - generated > USAGE_EXPORT_MAX_AGE:
        return [Finding("warn", "Учёт лимитов OpenCode не обновляется")]
    findings = []
    windows = snapshot.get("windows")
    for window in windows if isinstance(windows, list) else []:
        if not isinstance(window, dict):
            continue
        used = window.get("used_percent")
        if isinstance(used, int | float) and used >= QUOTA_WARN_PERCENT:
            findings.append(Finding("warn", f"OpenCode, окно {window.get('id')}: {used:.0f}%"))
    return findings


async def database_facts(session: AsyncSession, *, now: datetime) -> DatabaseFacts:
    since = now - WINDOW
    blocked = (
        await session.execute(
            select(func.count(), func.min(Run.updated_at)).where(Run.status == RunStatus.BLOCKED)
        )
    ).one()
    expired = await session.scalar(
        select(func.count())
        .select_from(Step)
        .where(
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
            Step.lease_expires_at < now - STUCK_LEASE_GRACE,
        )
    )
    starved = await session.scalar(
        select(func.count())
        .select_from(Step)
        .join(Run, Run.id == Step.run_id)
        .where(
            Run.status == RunStatus.RUNNING,
            Step.status.in_((StepStatus.PENDING, StepStatus.QUEUED)),
            Step.available_at < now - STUCK_QUEUE_AGE,
        )
    )
    waiting = (
        await session.execute(
            select(ManagedGoal.project_id, ManagedGoal.waiting_reason)
            .where(
                ManagedGoal.lifecycle == "waiting",
                ManagedGoal.waiting_reason.is_not(None),
                ManagedGoal.waiting_reason.not_in(_QUIET_WAITING_REASONS),
            )
            .order_by(ManagedGoal.project_id)
        )
    ).all()
    jobs = await session.execute(
        select(LeadJob.failure_reason).where(
            LeadJob.state == "failed", LeadJob.finished_at >= since
        )
    )
    calls = await session.execute(
        select(ModelCall.error).where(ModelCall.state == "failed", ModelCall.updated_at >= since)
    )
    outbox = await session.execute(
        select(TransactionalOutbox.destination).where(
            TransactionalOutbox.status.in_((DeliveryStatus.DEAD_LETTER, DeliveryStatus.AMBIGUOUS)),
            TransactionalOutbox.updated_at >= since,
        )
    )
    return DatabaseFacts(
        blocked_runs=int(blocked[0] or 0),
        oldest_blocked=blocked[1],
        stuck_steps=int(expired or 0) + int(starved or 0),
        waiting_goals=tuple((str(project), str(reason)) for project, reason in waiting),
        failed_jobs=dict(Counter(str(reason or "unknown") for (reason,) in jobs)),
        failed_model_calls=dict(Counter(str(error or "unknown") for (error,) in calls)),
        undelivered=dict(Counter(str(destination) for (destination,) in outbox)),
    )


def host_facts(
    *, services: tuple[str, ...], backup_unit: str, usage_file: Path | None
) -> HostFacts:
    usage = os.statvfs("/")
    total = usage.f_blocks * usage.f_frsize
    free = usage.f_bavail * usage.f_frsize
    snapshot = None
    if usage_file is not None:
        try:
            loaded = json.loads(usage_file.read_text())
        except (OSError, ValueError):
            loaded = {}
        snapshot = loaded if isinstance(loaded, dict) else {}
    return HostFacts(
        disk_used_percent=100.0 * (total - free) / total if total else 0.0,
        services={name: _unit(name) for name in services},
        backup=_unit(backup_unit),
        usage_snapshot=snapshot,
    )


def _unit(name: str) -> UnitState:
    shown = subprocess.run(  # noqa: S603 - fixed binary, unit names from configuration
        [
            "/usr/bin/systemctl",
            "show",
            name,
            "--property=ActiveState,NRestarts,Result,ExecMainExitTimestamp",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    values = dict(line.split("=", 1) for line in shown.stdout.splitlines() if "=" in line)
    exited = values.get("ExecMainExitTimestamp", "")
    return UnitState(
        active=values.get("ActiveState", "unknown"),
        restarts=int(values.get("NRestarts") or 0),
        result=values.get("Result", "unknown"),
        last_exit=_systemd_time(exited),
    )


def _systemd_time(value: str) -> datetime | None:
    # systemd prints e.g. "Mon 2026-11-09 03:32:07 CET"; the weekday and zone name vary.
    parts = value.split()
    if len(parts) < 3:
        return None
    try:
        local = datetime.strptime(f"{parts[1]} {parts[2]}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return local.astimezone(UTC)


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _age(delta: timedelta) -> str:
    hours = int(delta.total_seconds() // 3600)
    return f"{hours // 24} д" if hours >= 48 else f"{hours} ч"
