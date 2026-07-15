"""Run one bounded production canary and certify its exact agent runtime tuple."""

import argparse
import json
import subprocess
import tempfile
import uuid
from pathlib import Path

from vuzol.config import get_runtime_configuration
from vuzol.execution.runtime_contract import (
    AgentCertificateStore,
    certification_key,
    new_certificate,
)
from vuzol.experiments.domain import (
    BoundedLevel,
    ContextEntry,
    ContextManifest,
    ExecutionMode,
    RequiredGate,
    RiskLevel,
    TaskClass,
    TaskClassification,
)
from vuzol.experiments.service import TrialSeedRequest

ROOT = Path(__file__).parents[3]
PROBE_PATH = Path("certification/agent-runtime-probe.txt")
BEFORE = "agent-runtime-certification: before"
AFTER = "agent-runtime-certification: after"


def main() -> None:
    parser = argparse.ArgumentParser(description="Certify one exact provider agent runtime")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--project", default="vuzol")
    parser.add_argument("--base", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    profile = runtime.registries.profiles.get(args.profile)
    project = runtime.registries.projects.get(args.project)
    sandbox = runtime.registries.sandboxes.get(project.sandbox_profile)
    probe = project.repository_path / PROBE_PATH
    content = probe.read_bytes()
    if content.decode().strip() != BEFORE:
        raise RuntimeError("agent certification probe has unexpected base content")
    marker = uuid.uuid4().hex[:12]
    request = TrialSeedRequest(
        experiment_id=f"agent-certification-{profile.id}-{marker}",
        task_id=f"agent-certification-{marker}",
        worker_profile=profile.id,
        project_id=project.id,
        base_commit=args.base,
        goal=(
            f"Read {PROBE_PATH.as_posix()}, replace the exact marker {BEFORE!r} with "
            f"{AFTER!r}, make no other change, and return the required edit report."
        ),
        classification=TaskClassification(
            task_class=TaskClass.FOCUSED_BUG_FIX,
            complexity=BoundedLevel.LOW,
            risk=RiskLevel.LOW,
            testability=BoundedLevel.HIGH,
            blast_radius=BoundedLevel.LOW,
            coupling=BoundedLevel.LOW,
            novelty=BoundedLevel.LOW,
            expected_file_count=1,
        ),
        actual_mode=ExecutionMode.SOL_SOLO,
        allowed_paths=(PROBE_PATH.as_posix(),),
        acceptance_criteria=(
            "The ordinary probe file is read and its exact before marker becomes after.",
            "No other file changes and the final response is a valid WorkerEditReport.",
        ),
        forbidden_changes=("No Git, tests, gates, network, dependencies, or other paths.",),
        required_gates=(RequiredGate(name="format-check", command_id="make format-check"),),
        maximum_repair_count=0,
        context_manifest=ContextManifest(
            role="worker",
            entries=(
                ContextEntry.from_content(
                    source_type="repository_file",
                    reference=PROBE_PATH.as_posix(),
                    content=content,
                ),
            ),
        ),
        runtime_certification=True,
    )
    with tempfile.TemporaryDirectory(prefix="vuzol-agent-certification-") as directory:
        request_path = Path(directory) / "request.json"
        request_path.write_text(request.model_dump_json(indent=2))
        result = _canary(request_path, timeout_seconds=args.timeout_seconds)
    task_uuid, run_uuid = _verify_result(result, runtime.settings.artifact_root)
    certificate = new_certificate(
        key=certification_key(profile, sandbox),
        profile_id=profile.id,
        task_uuid=task_uuid,
        run_uuid=run_uuid,
    )
    path = AgentCertificateStore(runtime.settings.artifact_root / "agent-certificates").issue(
        certificate
    )
    print(
        json.dumps(
            {
                "schema_version": certificate.schema_version,
                "certificate_key": certificate.key.digest,
                "certificate_path": str(path),
                "task_uuid": task_uuid,
                "run_uuid": run_uuid,
            },
            indent=2,
            sort_keys=True,
        )
    )


def _canary(request: Path, *, timeout_seconds: int) -> dict[str, object]:
    command = (
        str(ROOT / ".venv/bin/python"),
        str(ROOT / "deploy/mvp/canary.py"),
        str(request),
        "--timeout-seconds",
        str(timeout_seconds),
    )
    completed = subprocess.run(  # noqa: S603 - fixed executable and bounded arguments
        command, check=False, capture_output=True, text=True
    )
    if completed.returncode:
        raise RuntimeError("agent certification canary failed")
    decoded = json.loads(completed.stdout)
    if not isinstance(decoded, dict):
        raise RuntimeError("agent certification canary returned invalid evidence")
    cleanup = subprocess.run(
        ("/usr/bin/make", "mvp-check"),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if cleanup.returncode:
        raise RuntimeError("agent certification cleanup verification failed")
    return decoded


def _verify_result(result: dict[str, object], artifact_root: Path) -> tuple[str, str]:
    seed = result.get("seed")
    inspect = result.get("inspect")
    if not isinstance(seed, dict) or not isinstance(inspect, dict):
        raise RuntimeError("agent certification evidence is incomplete")
    runs = inspect.get("runs")
    if not isinstance(runs, list) or len(runs) != 1 or not isinstance(runs[0], dict):
        raise RuntimeError("agent certification must contain exactly one run")
    run = runs[0]
    processes = run.get("processes")
    worktree = run.get("worktree")
    artifacts = run.get("artifacts")
    if run.get("status") != "completed":
        raise RuntimeError("agent certification run did not complete")
    if not isinstance(processes, list) or len(processes) != 1:
        raise RuntimeError("agent certification must use exactly one provider process")
    process = processes[0]
    if not isinstance(process, dict) or process.get("outcome") != "succeeded":
        raise RuntimeError("agent certification provider process did not succeed")
    if not isinstance(worktree, dict) or not worktree.get("result_commit"):
        raise RuntimeError("agent certification produced no system commit")
    if not isinstance(artifacts, list):
        raise RuntimeError("agent certification artifacts are unavailable")
    by_type = {
        artifact.get("type"): artifact
        for artifact in artifacts
        if isinstance(artifact, dict) and isinstance(artifact.get("type"), str)
    }
    required = {"git_diff", "provider_edit_report", "worker_finalization_evidence"}
    if not required.issubset(by_type):
        raise RuntimeError("agent certification lacks measured finalization artifacts")
    diff = _artifact_bytes(artifact_root, by_type["git_diff"])
    if BEFORE.encode() not in diff or AFTER.encode() not in diff:
        raise RuntimeError("agent certification did not prove probe read/edit behavior")
    report = json.loads(_artifact_bytes(artifact_root, by_type["provider_edit_report"]))
    evidence = json.loads(_artifact_bytes(artifact_root, by_type["worker_finalization_evidence"]))
    if (
        report.get("claimed_complete") is not True
        or evidence.get("verification", {}).get("passed") is not True
    ):
        raise RuntimeError("agent certification structured output or Git verification failed")
    if worktree.get("delivery_state") == "active":
        raise RuntimeError("agent certification cleanup left an active worktree")
    return str(seed["task_uuid"]), str(seed["run_uuid"])


def _artifact_bytes(root: Path, metadata: object) -> bytes:
    if not isinstance(metadata, dict):
        raise RuntimeError("agent certification artifact metadata is invalid")
    digest = metadata.get("content_hash")
    if not isinstance(digest, str) or len(digest) != 64:
        raise RuntimeError("agent certification artifact hash is invalid")
    path = root / digest[:2] / digest
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("agent certification artifact is unavailable")
    return path.read_bytes()


if __name__ == "__main__":
    main()
