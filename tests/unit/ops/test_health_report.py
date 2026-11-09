from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from vuzol.ops import health_report
from vuzol.ops.health_report import (
    DatabaseFacts,
    Finding,
    HostFacts,
    UnitState,
    evaluate,
    render,
)

NOW = datetime(2026, 11, 9, 8, 0, tzinfo=UTC)
BACKED_UP = UnitState("inactive", last_exit=NOW - timedelta(hours=5))
FRESH_USAGE = {
    "generated_at": "2026-11-09T07:59:00Z",
    "windows": [{"id": "weekly", "used_percent": 10.0}],
}


def healthy_host(**changes: object) -> HostFacts:
    values: dict[str, object] = {
        "disk_used_percent": 60.0,
        "services": {"vuzol-executor.service": UnitState("active")},
        "backup": BACKED_UP,
        "usage_snapshot": FRESH_USAGE,
    }
    values.update(changes)
    return HostFacts(**values)  # type: ignore[arg-type]


def test_a_healthy_system_says_so_in_one_quiet_line() -> None:
    findings = evaluate(healthy_host(), DatabaseFacts(), now=NOW)
    assert findings == []
    assert render(findings) == "🟢 <b>Vuzol: всё в порядке</b>"


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        (healthy_host(disk_used_percent=86.0), Finding("warn", "Диск занят на 86%")),
        (healthy_host(disk_used_percent=95.0), Finding("fail", "Диск занят на 95%")),
        (
            healthy_host(services={"vuzol-executor.service": UnitState("failed")}),
            Finding("fail", "vuzol-executor.service: failed"),
        ),
        (
            healthy_host(services={"vuzol-executor.service": UnitState("active", restarts=3)}),
            Finding("warn", "vuzol-executor.service: перезапусков 3"),
        ),
        (healthy_host(backup=None), Finding("fail", "Бэкап ещё ни разу не завершился")),
        (
            healthy_host(backup=UnitState("inactive", result="exit-code", last_exit=NOW)),
            Finding("fail", "Последний бэкап упал: exit-code"),
        ),
        (
            healthy_host(backup=UnitState("inactive", last_exit=NOW - timedelta(days=3))),
            Finding("fail", "Бэкапа не было 3 д"),
        ),
        (
            healthy_host(usage_snapshot={"generated_at": "2026-11-09T06:00:00Z"}),
            Finding("warn", "Учёт лимитов OpenCode не обновляется"),
        ),
        (
            healthy_host(
                usage_snapshot={**FRESH_USAGE, "windows": [{"id": "monthly", "used_percent": 91}]}
            ),
            Finding("warn", "OpenCode, окно monthly: 91%"),
        ),
    ],
)
def test_each_host_problem_is_named(host: HostFacts, expected: Finding) -> None:
    assert evaluate(host, DatabaseFacts(), now=NOW) == [expected]


def test_database_problems_are_named_and_owner_pauses_are_not() -> None:
    db = DatabaseFacts(
        blocked_runs=7,
        oldest_blocked=NOW - timedelta(days=28),
        stuck_steps=2,
        waiting_goals=(("diplom", "plan_approval"), ("vuzol", "evidence_not_recorded")),
        failed_jobs={"provider_busy": 2},
        failed_model_calls={},
        undelivered={"telegram": 1},
    )
    texts = [f.text for f in evaluate(healthy_host(), db, now=NOW)]
    assert texts == [
        "Задач ждут решения: 7, старейший 28 д",
        "Зависших шагов: 2",
        "Цель в diplom: Нужно одобрить план работы.",
        "Цель в vuzol: evidence_not_recorded",
        "Исследования упали за сутки: provider_busy: 2",
        "Сообщения не доставлены за сутки: telegram: 1",
    ]


def test_failures_lead_the_message_and_text_is_escaped() -> None:
    text = render([Finding("warn", "a <b>"), Finding("fail", "disk")])
    assert text.splitlines() == [
        "🔴 <b>Vuzol: требует внимания (2)</b>",
        "🔴 disk",
        "🟡 a &lt;b&gt;",
    ]


def test_host_facts_read_systemd_and_the_usage_file(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "vuzol.ops.health_report.os.statvfs",
        lambda _path: SimpleNamespace(f_blocks=100, f_bavail=25, f_frsize=1),
    )

    def show(command, **_kwargs):
        unit = command[2]
        output = (
            "ActiveState=inactive\nNRestarts=0\nResult=success\n"
            "ExecMainExitTimestamp=Mon 2026-11-09 03:32:07 UTC"
            if unit == "backup"
            else "ActiveState=active\nNRestarts=1\nResult=success\nExecMainExitTimestamp="
        )
        return SimpleNamespace(stdout=output)

    monkeypatch.setattr("vuzol.ops.health_report.subprocess.run", show)
    usage = tmp_path / "usage.json"
    usage.write_text('{"generated_at": "2026-11-09T07:59:00Z"}')
    facts = health_report.host_facts(services=("svc",), backup_unit="backup", usage_file=usage)
    assert facts.disk_used_percent == 75.0
    assert facts.services == {"svc": UnitState("active", restarts=1)}
    assert facts.backup is not None and facts.backup.last_exit is not None
    assert facts.usage_snapshot == {"generated_at": "2026-11-09T07:59:00Z"}
    broken = tmp_path / "broken.json"
    broken.write_text("{")
    assert (
        health_report.host_facts(
            services=(), backup_unit="backup", usage_file=broken
        ).usage_snapshot
        == {}
    )
