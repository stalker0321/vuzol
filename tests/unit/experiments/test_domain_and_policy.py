"""Domain and policy tests (split for cohesion)."""

from __future__ import annotations

from ._test_experiments_helpers import (
    BoundedLevel,
    BoundedRepairContext,
    ContextEntry,
    ContextManifest,
    ExecutionMode,
    Path,
    RepairGateDiagnostic,
    RepairSeverity,
    RepairSymbolContext,
    ReportedUsage,
    ReviewOutcome,
    Run,
    TaskClass,
    TrialSeedRequest,
    ValidationError,
    WorkerTaskCapsule,
    _trusted_profile_id,
    capsule,
    classification,
    classify_execution_mode,
    enforce_security_escalation,
    path_is_allowed,
    pytest,
    render_worker_prompt,
    scopes_conflict,
    seed_request,
)


def test_trial_seed_request_accepts_bounded_telegram_source_metadata() -> None:
    request = seed_request().model_copy(
        update={
            "source_user_id": 42,
            "source_chat_id": -100,
            "source_thread_id": 11,
        }
    )
    validated = TrialSeedRequest.model_validate(request.model_dump(mode="json"))
    assert validated.source_user_id == 42
    assert validated.source_chat_id == -100
    assert validated.source_thread_id == 11


def test_bounded_repair_context_accepts_only_measured_code_evidence() -> None:
    repair = BoundedRepairContext(
        current_diff="diff --git a/src/example.py b/src/example.py\n+VALUE = 2\n",
        changed_files=("src/example.py",),
        failed_gates=(
            RepairGateDiagnostic(
                command_id="make type-check",
                exit_code=2,
                sanitized_output="src/example.py:10: incompatible assignment",
            ),
        ),
        required_symbols=(
            RepairSymbolContext(reference="src/example.py:1-20", content="def example(): ..."),
        ),
    )
    request = seed_request().model_copy(update={"attempt": 2, "repair_context": repair})
    validated = TrialSeedRequest.model_validate(request.model_dump(mode="json"))
    assert validated.repair_context == repair
    assert validated.repair_context.changed_files == ("src/example.py",)


def test_repair_context_rejects_oversized_or_operational_history() -> None:
    with pytest.raises(ValidationError, match="prohibited operational history"):
        BoundedRepairContext(
            current_diff="private handoff contents",
            changed_files=("src/example.py",),
            failed_gates=(
                RepairGateDiagnostic(
                    command_id="make lint", exit_code=1, sanitized_output="failure"
                ),
            ),
        )
    with pytest.raises(ValidationError, match="linked repair requires"):
        TrialSeedRequest.model_validate(
            seed_request().model_copy(update={"attempt": 2}).model_dump(mode="json")
        )


def test_capsule_is_immutable_versioned_and_rejects_secrets() -> None:
    item = capsule("a" * 40)
    assert item.schema_version == "step09a-task-capsule.v1"
    with pytest.raises(ValidationError):
        item.goal = "changed"
    with pytest.raises(ValidationError, match="prohibited"):
        capsule("a" * 40).model_copy(
            update={"goal": "Read auth.json"},
        ).model_validate(capsule("a" * 40).model_dump() | {"goal": "Read auth.json"})


def test_worker_prompt_contains_exact_boundary_and_structured_result_requirement() -> None:
    prompt = render_worker_prompt(capsule("a" * 40), repository_id="vuzol")
    assert "Sandbox worktree: /workspace" in prompt
    assert "/workspace is writable" in prompt
    assert "Exact base SHA: " + "a" * 40 in prompt
    assert "Vuzol has already prepared and verified" in prompt
    assert "shell-backed repository tools" in prompt
    assert "read files, search repository contents" in prompt
    assert "create and edit ordinary files" in prompt
    assert "inspect the result of your edits without Git" in prompt
    assert "Do not invoke Git, shell commands" not in prompt
    assert "Do not touch another VPS project" in prompt
    assert "Do not invoke any Git command" in prompt
    assert "read or write .git" in prompt
    assert "Do not run required gates or tests" in prompt
    assert "install packages" in prompt
    assert "synchronize dependencies" in prompt
    assert "access the network" in prompt
    assert "access paths outside /workspace" in prompt
    assert "Vuzol will inspect the real diff" in prompt
    assert "run trusted gates, stage exact paths, create the commit" in prompt
    assert "authoritative result manifest" in prompt
    assert "actually inspect and edit those files" in prompt
    assert "claimed_complete=true only after making the intended changes" in prompt
    assert "claimed_complete=false only for a genuine inability" in prompt
    assert "lack of permission to run Git or tests does not prevent" in prompt
    assert "step09a-worker-edit-report.v1" in prompt
    assert "Do not claim changed files" in prompt
    assert "gate results, branch identity, or a result commit" in prompt
    assert '"goal":"Add a pure validator."' in prompt
    assert '"allowed_paths":["src/example.py","tests/test_example.py"]' in prompt
    assert '"acceptance_criteria":["Reject malformed input"]' in prompt
    assert '"forbidden_changes":["Do not relax tests"]' in prompt
    assert "/home/vodkolyan" not in prompt


def test_trial_seed_request_bounds_repairs_and_context_role() -> None:
    request = seed_request()
    assert request.maximum_repair_count == 2
    with pytest.raises(ValidationError):
        TrialSeedRequest.model_validate(request.model_dump() | {"maximum_repair_count": 3})


def test_mode_policy_is_explicit_and_security_cannot_be_lowered() -> None:
    assert classify_execution_mode(classification()) is ExecutionMode.GROK_REVIEWED
    risky = classification(security_boundary=True)
    assert classify_execution_mode(risky) is ExecutionMode.SOL_SOLO
    assert (
        enforce_security_escalation(risky, ExecutionMode.GROK_GATED_SHADOW)
        is ExecutionMode.SOL_SOLO
    )
    assert (
        classify_execution_mode(classification(testability=BoundedLevel.LOW))
        is ExecutionMode.SOL_SOLO
    )
    assert (
        classify_execution_mode(classification(task_class=TaskClass.SECURITY))
        is ExecutionMode.SOL_SOLO
    )


def test_profile_pin_only_accepts_internal_versioned_route() -> None:
    run = Run(selected_route={"schema_version": "step09a-route.v1", "trusted_profile_id": "grok-a"})
    assert _trusted_profile_id(run) == "grok-a"
    run.selected_route = {"trusted_profile_id": "grok-a"}
    assert _trusted_profile_id(run) is None
    run.selected_route = {"schema_version": "step09a-route.v1", "trusted_profile_id": 7}
    assert _trusted_profile_id(run) is None


def test_context_hashing_and_repeated_measurement() -> None:
    original = ContextEntry.from_content(
        source_type="repository_file", reference="src/a.py", content=b"abc"
    )
    repeated = ContextEntry.from_content(
        source_type="repository_file",
        reference="src/a.py",
        content=b"abc",
        repeated_from_roles=("planner",),
    )
    assert original.content_hash == repeated.content_hash
    manifest = ContextManifest(role="worker", entries=(original, repeated))
    assert manifest.total_bytes == 6
    assert manifest.repeated_bytes == 3
    assert manifest.estimated_tokens == 2
    with pytest.raises(ValidationError, match="both endpoints"):
        ContextEntry(
            source_type="file",
            reference="a",
            content_hash="a" * 64,
            line_start=1,
            byte_count=0,
            estimated_tokens=0,
        )
    with pytest.raises(ValidationError, match="reversed"):
        ContextEntry(
            source_type="file",
            reference="a",
            content_hash="a" * 64,
            line_start=2,
            line_end=1,
            byte_count=0,
            estimated_tokens=0,
        )


def test_missing_usage_is_never_fabricated() -> None:
    missing = ReportedUsage(unavailable_reason="CLI did not expose structured usage")
    assert missing.input_tokens is None
    with pytest.raises(ValidationError, match="explanation"):
        ReportedUsage()


def test_outcome_and_repair_taxonomies_are_closed() -> None:
    assert ReviewOutcome("accepted_after_minor_repair") is ReviewOutcome.ACCEPTED_AFTER_MINOR_REPAIR
    assert RepairSeverity("major") is RepairSeverity.MAJOR
    with pytest.raises(ValueError):
        ReviewOutcome("mostly_ok")


def test_capsule_repair_limit_and_override_reason() -> None:
    data = capsule("a" * 40).model_dump()
    with pytest.raises(ValidationError):
        WorkerTaskCapsule.model_validate(data | {"maximum_repair_count": 3})
    with pytest.raises(ValidationError, match="override"):
        WorkerTaskCapsule.model_validate(
            data | {"actual_mode": ExecutionMode.SOL_SOLO, "override_reason": None}
        )
    with pytest.raises(ValidationError, match="repository-relative"):
        WorkerTaskCapsule.model_validate(data | {"allowed_paths": ("/etc/passwd",)})
    wrong_context = ContextManifest(role="reviewer")
    with pytest.raises(ValidationError, match="worker context"):
        WorkerTaskCapsule.model_validate(data | {"context_manifest": wrong_context})


def test_scope_conflict_and_allowed_file_enforcement() -> None:
    assert scopes_conflict(("src/a",), ("src/a/file.py",))
    assert not scopes_conflict(("src/a.py",), ("docs/b.md",))
    assert path_is_allowed("src/a/file.py", ("src/a",))
    assert not path_is_allowed("src/ab/file.py", ("src/a",))
    assert not path_is_allowed("../secret", ("src",))


def test_step09a_execute_code_receives_non_authoritative_edit_report_schema() -> None:
    from vuzol.providers.handlers import _step09a_result_schema

    name, version, schema = _step09a_result_schema(
        "execute_code", {"step09a_capsule": {"schema_version": "step09a-task-capsule.v1"}}
    )
    assert name == "WorkerEditReport"
    assert version == "step09a-worker-edit-report.v1"
    assert schema is not None
    required = schema["required"]
    assert isinstance(required, list)
    assert set(required) >= {
        "experiment_id",
        "task_id",
        "claimed_complete",
        "implementation_summary",
    }
    properties = schema["properties"]
    assert isinstance(properties, dict)
    assert "attempt" in properties
    assert "result_commit" not in properties
    assert "changed_files" not in properties
    assert "gates" not in properties
    assert "branch" not in properties
    assert _step09a_result_schema("execute_code", {}) == (None, None, None)
    assert _step09a_result_schema("plan", {"step09a_capsule": {}}) == (None, None, None)


def test_no_automatic_merge_deploy_or_direct_grok_host_path_exists() -> None:
    package = Path(__file__).parents[3] / "src" / "vuzol" / "experiments"
    source = "\n".join(path.read_text() for path in package.glob("*.py"))
    assert "grok --" not in source
    assert "git merge" not in source
    assert "git push" not in source
    assert "systemctl" not in source
