"""Agent certification tests (split for cohesion)."""

from __future__ import annotations

from ._test_mvp_helpers import *


def test_agent_certification_keeps_production_runtime_loading_unchanged() -> None:
    content = (ROOT / "src/vuzol/cli/agent_certify.py").read_text()
    assert "get_runtime_configuration(validate_profile_credentials=False)" in content
    assert "VUZOL_REGISTRY_FILE" not in content


def test_agent_certification_accepts_complete_measured_canary(tmp_path: Path) -> None:
    from vuzol.cli.agent_certify import AFTER, BEFORE, _verify_result

    artifacts: list[dict[str, object]] = []

    def artifact(kind: str, content: bytes) -> None:
        digest = hashlib.sha256(content).hexdigest()
        destination = tmp_path / digest[:2] / digest
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        artifacts.append({"type": kind, "content_hash": digest})

    artifact("git_diff", f"-{BEFORE}\n+{AFTER}\n".encode())
    artifact("provider_edit_report", b'{"claimed_complete":true}')
    artifact("worker_finalization_evidence", b'{"verification":{"passed":true}}')
    result = {
        "seed": {"task_uuid": "task", "run_uuid": "run"},
        "inspect": {
            "runs": [
                {
                    "status": "completed",
                    "processes": [{"outcome": "succeeded"}],
                    "worktree": {"result_commit": "a" * 40, "delivery_state": "retained"},
                    "artifacts": artifacts,
                }
            ]
        },
    }
    assert _verify_result(result, tmp_path) == ("task", "run")


def test_agent_certification_fails_closed_on_missing_invariant(tmp_path: Path) -> None:
    from vuzol.cli.agent_certify import _verify_result

    result = {
        "seed": {"task_uuid": "task", "run_uuid": "run"},
        "inspect": {
            "runs": [
                {
                    "status": "failed",
                    "processes": [],
                    "worktree": None,
                    "artifacts": [],
                }
            ]
        },
    }
    with pytest.raises(RuntimeError, match="did not complete"):
        _verify_result(result, tmp_path)


def test_agent_certification_command_builds_fixed_disposable_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from vuzol.cli import agent_certify
    from vuzol.config.models import ProviderProfileConfig, SandboxProfileConfig

    profile = ProviderProfileConfig.model_validate(
        {
            "id": "codex-cert",
            "provider": "codex",
            "model": "codex",
            "launch_mode": "cli",
            "credential_required": False,
            "capabilities": ["repository_read", "code_edit", "project_shell"],
            "concurrency_limit": 1,
            "cost_class": "strong",
            "supported_task_types": ["coding"],
            "runtime_identity": "codex-cert",
            "state_directory": "/var/lib/vuzol-provider-state/codex-cert",
            "agent_runtime_contract": {
                "cli_version": "codex-cli 0.144.1",
                "edit_mechanism": "shell_backed_repository_tools",
                "working_directory": "/workspace",
                "writable_roots": ["/workspace"],
                "protected_roots": ["/workspace/.git"],
                "structured_output_source": "final_agent_message_json",
                "inner_sandbox_mode": "provider_managed",
                "supports_read": True,
                "supports_search": True,
                "supports_edit": True,
                "supports_git": False,
                "supports_network": False,
                "supports_local_checks": False,
            },
        }
    )
    sandbox = SandboxProfileConfig(id="provider", image="provider@sha256:" + "a" * 64)
    runtime = MagicMock()
    runtime.settings.artifact_root = tmp_path / "artifacts"
    runtime.registries.profiles.get.return_value = profile
    runtime.registries.projects.get.return_value = MagicMock(
        id="vuzol", repository_path=ROOT, sandbox_profile="provider"
    )
    runtime.registries.sandboxes.get.return_value = sandbox
    captured_request: object | None = None

    def canary(request: Path, *, timeout_seconds: int) -> dict[str, object]:
        nonlocal captured_request
        from vuzol.experiments.service import TrialSeedRequest

        captured_request = TrialSeedRequest.model_validate_json(request.read_text())
        assert timeout_seconds == 20
        return {"safe": True}

    monkeypatch.setattr(agent_certify, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(agent_certify, "_canary", canary)
    monkeypatch.setattr(agent_certify, "_verify_result", lambda *_args: ("task", "run"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "vuzol-agent-certify",
            "--profile",
            profile.id,
            "--base",
            "a" * 40,
            "--timeout-seconds",
            "20",
        ],
    )

    agent_certify.main()

    from vuzol.experiments.service import TrialSeedRequest

    assert isinstance(captured_request, TrialSeedRequest)
    assert captured_request.runtime_certification is True
    assert captured_request.allowed_paths == ("certification/agent-runtime-probe.txt",)
    assert captured_request.maximum_repair_count == 0
    output = json.loads(capsys.readouterr().out)
    assert output["task_uuid"] == "task"
    assert Path(output["certificate_path"]).is_file()


def test_agent_certification_rejects_unexpected_probe_before_canary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from vuzol.cli import agent_certify

    probe = tmp_path / agent_certify.PROBE_PATH
    probe.parent.mkdir(parents=True)
    probe.write_text("unexpected\n")
    runtime = MagicMock()
    runtime.registries.profiles.get.return_value = MagicMock(id="codex-cert")
    runtime.registries.projects.get.return_value = MagicMock(
        repository_path=tmp_path, sandbox_profile="provider"
    )
    runtime.registries.sandboxes.get.return_value = MagicMock()
    monkeypatch.setattr(agent_certify, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(
        sys,
        "argv",
        ["vuzol-agent-certify", "--profile", "codex-cert", "--base", "a" * 40],
    )
    with pytest.raises(RuntimeError, match="unexpected base content"):
        agent_certify.main()


def test_agent_certification_canary_requires_canary_and_cleanup_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from vuzol.cli import agent_certify

    responses = iter(
        (
            subprocess.CompletedProcess([], 0, '{"seed":{},"inspect":{}}', ""),
            subprocess.CompletedProcess([], 0, "", ""),
        )
    )
    calls: list[tuple[object, ...]] = []

    def run(argv: tuple[object, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return next(responses)

    monkeypatch.setattr("vuzol.cli.agent_certify.subprocess.run", run)
    result = agent_certify._canary(tmp_path / "request.json", timeout_seconds=30)
    assert result == {"seed": {}, "inspect": {}}
    assert calls[1] == ("/usr/bin/make", "mvp-check")

    monkeypatch.setattr(
        "vuzol.cli.agent_certify.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 2, "", "failed"),
    )
    with pytest.raises(RuntimeError, match="canary failed"):
        agent_certify._canary(tmp_path / "request.json", timeout_seconds=30)

    monkeypatch.setattr(
        "vuzol.cli.agent_certify.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "[]", ""),
    )
    with pytest.raises(RuntimeError, match="invalid evidence"):
        agent_certify._canary(tmp_path / "request.json", timeout_seconds=30)

    responses = iter(
        (
            subprocess.CompletedProcess([], 0, '{"seed":{},"inspect":{}}', ""),
            subprocess.CompletedProcess([], 2, "", "dirty"),
        )
    )
    monkeypatch.setattr(
        "vuzol.cli.agent_certify.subprocess.run", lambda *_args, **_kwargs: next(responses)
    )
    with pytest.raises(RuntimeError, match="cleanup verification failed"):
        agent_certify._canary(tmp_path / "request.json", timeout_seconds=30)


@pytest.mark.parametrize(
    ("result", "message"),
    (
        ({}, "incomplete"),
        ({"seed": {}, "inspect": {"runs": []}}, "exactly one run"),
        (
            {
                "seed": {},
                "inspect": {
                    "runs": [
                        {
                            "status": "completed",
                            "processes": [],
                            "worktree": {},
                            "artifacts": [],
                        }
                    ]
                },
            },
            "exactly one provider process",
        ),
        (
            {
                "seed": {},
                "inspect": {
                    "runs": [
                        {
                            "status": "completed",
                            "processes": [{"outcome": "failed"}],
                            "worktree": {},
                            "artifacts": [],
                        }
                    ]
                },
            },
            "process did not succeed",
        ),
        (
            {
                "seed": {},
                "inspect": {
                    "runs": [
                        {
                            "status": "completed",
                            "processes": [{"outcome": "succeeded"}],
                            "worktree": {},
                            "artifacts": [],
                        }
                    ]
                },
            },
            "no system commit",
        ),
    ),
)
def test_agent_certification_rejects_incomplete_measured_shapes(
    tmp_path: Path, result: dict[str, object], message: str
) -> None:
    from vuzol.cli.agent_certify import _verify_result

    with pytest.raises(RuntimeError, match=message):
        _verify_result(result, tmp_path)


@pytest.mark.parametrize(
    "metadata",
    (None, {}, {"content_hash": "short"}, {"content_hash": "a" * 64}),
)
def test_agent_certification_artifact_read_is_bounded(tmp_path: Path, metadata: object) -> None:
    from vuzol.cli.agent_certify import _artifact_bytes

    with pytest.raises(RuntimeError, match=r"metadata|hash|unavailable"):
        _artifact_bytes(tmp_path, metadata)


@pytest.mark.parametrize(
    ("failure", "message"),
    (
        ("artifacts_type", "artifacts are unavailable"),
        ("missing_artifact", "lacks measured"),
        ("wrong_diff", "probe read/edit"),
        ("invalid_report", "structured output"),
        ("active", "active worktree"),
    ),
)
def test_agent_certification_rejects_incomplete_finalization_evidence(
    tmp_path: Path, failure: str, message: str
) -> None:
    from vuzol.cli.agent_certify import AFTER, BEFORE, _verify_result

    artifacts: list[dict[str, object]] = []

    def artifact(kind: str, content: bytes) -> None:
        digest = hashlib.sha256(content).hexdigest()
        destination = tmp_path / digest[:2] / digest
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        artifacts.append({"type": kind, "content_hash": digest})

    diff = b"unrelated" if failure == "wrong_diff" else f"-{BEFORE}\n+{AFTER}\n".encode()
    report = (
        b'{"claimed_complete":false}'
        if failure == "invalid_report"
        else b'{"claimed_complete":true}'
    )
    artifact("git_diff", diff)
    artifact("provider_edit_report", report)
    artifact("worker_finalization_evidence", b'{"verification":{"passed":true}}')
    if failure == "missing_artifact":
        artifacts.pop()
    worktree = {
        "result_commit": "a" * 40,
        "delivery_state": "active" if failure == "active" else "retained",
    }
    result = {
        "seed": {"task_uuid": "task", "run_uuid": "run"},
        "inspect": {
            "runs": [
                {
                    "status": "completed",
                    "processes": [{"outcome": "succeeded"}],
                    "worktree": worktree,
                    "artifacts": None if failure == "artifacts_type" else artifacts,
                }
            ]
        },
    }
    with pytest.raises(RuntimeError, match=message):
        _verify_result(result, tmp_path)
