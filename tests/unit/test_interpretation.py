import asyncio
import json
import uuid
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from vuzol.config import Capability, TopicKind
from vuzol.interpretation.adapters import (
    FakeInterpreter,
    FakeTranscriber,
    OpenAICompatibleInterpreter,
    OpenAICompatibleTranscriber,
)
from vuzol.interpretation.domain import (
    InterpretationInput,
    InterpretationResult,
    ProjectNameOption,
    SuggestedComplexity,
    TaskAction,
    TaskContext,
    TaskDraft,
    TaskOperation,
    TaskType,
    TranscriptionInput,
)
from vuzol.interpretation.evaluation import (
    EvaluationFixture,
    EvaluationReport,
    evaluate_interpreter,
    load_fixtures,
    require_eligible_report,
)
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.ports import (
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    TranscriptionUnavailable,
)
from vuzol.interpretation.service import interpret_with_recovery, regenerate_project_names
from vuzol.storage.types import RiskLevel


def request(*, voice: bool = False, uncertain: bool = False) -> InterpretationInput:
    return InterpretationInput(
        original_input="inspect the service",
        transcript="inspect the service" if voice else None,
        topic_kind=TopicKind.PERSONAL,
        capability_vocabulary=frozenset(Capability),
        source_is_voice=voice,
        transcription_uncertain=uncertain,
    )


def draft(**changes: object) -> TaskDraft:
    values: dict[str, object] = {
        "action": TaskAction.CREATE_TASK,
        "task_type": TaskType.INFRASTRUCTURE,
        "operation": TaskOperation.INSPECT,
        "goal": "Inspect service state",
        "suggested_complexity": SuggestedComplexity.SMALL,
        "suggested_risk": RiskLevel.LOW,
        "needs_planning": False,
        "needs_clarification": False,
        "normalized_title": "Inspect service",
    }
    values.update(changes)
    return TaskDraft.model_validate(values)


def result(value: TaskDraft, *, profile: str = "primary") -> InterpretationResult:
    return InterpretationResult(
        draft=value,
        profile_id=profile,
        model="model",
        duration_ms=1,
    )


def name_options(*, conflicting_id: str | None = None) -> tuple[ProjectNameOption, ...]:
    return tuple(
        ProjectNameOption(
            display_name=f"Notes {index + 1}",
            project_id=conflicting_id if index == 0 and conflicting_id else f"notes-{index + 1}",
        )
        for index in range(9)
    )


def test_task_draft_requires_consistent_clarification_and_continuation() -> None:
    with pytest.raises(ValidationError, match="clarification question is required"):
        draft(needs_clarification=True)
    with pytest.raises(ValidationError, match="referenced task"):
        draft(action=TaskAction.CONTINUE_TASK)
    with pytest.raises(ValidationError, match="new project fields"):
        draft(new_project_id="notes", new_project_name="Notes")


def test_inbox_is_explicit_project_provisioning_boundary() -> None:
    inbox = request().model_copy(update={"topic_kind": TopicKind.INBOX})
    value = draft(
        action=TaskAction.CREATE_PROJECT,
        new_project_id="notes",
        new_project_name="Notes",
        project_name_options=name_options(),
    )
    policy = enforce_interpretation_policy(
        inbox,
        value,
        known_project_ids=frozenset({"vuzol"}),
    )
    assert policy.draft.action is TaskAction.CREATE_PROJECT
    assert policy.draft.project_id is None
    assert policy.draft.new_project_id is None
    assert policy.draft.new_project_name is None
    assert len(policy.draft.project_name_options) == 9
    assert policy.draft.required_capabilities == frozenset(
        {Capability.FILESYSTEM_WRITE, Capability.GIT, Capability.TELEGRAM_SEND}
    )
    assert not policy.draft.needs_clarification
    assert policy.automatic_execution_eligible


def test_inbox_requires_name_options_and_rejects_configured_project_collision() -> None:
    inbox = request().model_copy(update={"topic_kind": TopicKind.INBOX})
    missing = enforce_interpretation_policy(
        inbox,
        draft(),
        known_project_ids=frozenset({"vuzol"}),
    )
    assert missing.draft.action is TaskAction.CREATE_PROJECT
    assert missing.draft.needs_clarification
    assert "project_name_options_missing" in missing.reasons

    collision = enforce_interpretation_policy(
        inbox,
        draft(
            action=TaskAction.CREATE_PROJECT,
            project_name_options=name_options(conflicting_id="vuzol"),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )
    assert collision.draft.needs_clarification
    assert "project_name_options_conflict" in collision.reasons


def test_project_topic_cannot_be_reinterpreted_as_new_project() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "bill-buddy"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            action=TaskAction.CREATE_PROJECT,
            task_type=TaskType.GENERAL,
            project_name_options=name_options(),
        ),
        known_project_ids=frozenset({"bill-buddy"}),
    )

    assert policy.draft.action is TaskAction.CREATE_TASK
    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.project_id == "bill-buddy"
    assert policy.draft.project_name_options == ()
    assert "project_creation_confined_to_inbox" in policy.reasons


def test_task_schema_exposes_architecture_as_a_distinct_agent_task() -> None:
    schema = TaskDraft.model_json_schema()
    task_type_schema = schema["$defs"]["TaskType"]
    assert "architecture" in task_type_schema["enum"]
    assert "task_summary" in schema["required"]


def test_legacy_task_draft_derives_summary_from_normalized_title() -> None:
    value = draft()
    assert value.task_summary == "Inspect service"


def test_explicit_task_summary_is_preserved() -> None:
    value = draft(task_summary="Inspect current service health and report anomalies")
    assert value.task_summary == "Inspect current service health and report anomalies"


def test_project_architecture_question_is_a_read_only_agent_task() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "vuzol"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            action=TaskAction.ANSWER_QUESTION,
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.EXPLAIN,
            required_capabilities=frozenset({Capability.CODE_EDIT}),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.action is TaskAction.CREATE_TASK
    assert policy.draft.operation is TaskOperation.INSPECT
    assert policy.draft.required_capabilities == frozenset({Capability.REPOSITORY_READ})
    assert policy.draft.project_id == "vuzol"
    assert "architecture_requires_agent_task" in policy.reasons
    assert "architecture_confined_to_read_only" in policy.reasons


def test_read_only_coding_inspection_is_reclassified_as_architecture() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "vuzol"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.CODING,
            operation=TaskOperation.INSPECT,
            required_capabilities=frozenset({Capability.REPOSITORY_READ, Capability.WEB_RESEARCH}),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.task_type is TaskType.ARCHITECTURE
    assert policy.draft.required_capabilities == frozenset({Capability.REPOSITORY_READ})
    assert "read_only_design_reclassified_as_architecture" in policy.reasons


def test_design_question_survives_coding_create_misclassification() -> None:
    contextual = request().model_copy(
        update={
            "original_input": "Как лучше всего это сделать? Я думаю в виде лёгкого сайта.",
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "vuzol",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.CODING,
            operation=TaskOperation.CREATE,
            required_capabilities=frozenset(),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.task_type is TaskType.ARCHITECTURE
    assert policy.draft.operation is TaskOperation.INSPECT
    assert policy.draft.required_capabilities == frozenset({Capability.REPOSITORY_READ})


def test_explicit_implementation_overrides_architecture_misclassification() -> None:
    contextual = request().model_copy(
        update={
            "original_input": (
                "Окей, давай приступать к реализации. Сделай пока сайт с нужным "  # noqa: RUF001
                "функционалом, а апи моделей я добавлю потом."  # noqa: RUF001
            ),
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "bill-buddy",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.INSPECT,
            project_id="bill-buddy",
            required_capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.action is TaskAction.CREATE_TASK
    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.operation is TaskOperation.CREATE
    assert policy.draft.required_capabilities == frozenset(
        {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
    )
    assert "explicit_implementation_reclassified_as_coding" in policy.reasons


def test_imperative_modification_is_never_read_only_architecture() -> None:
    contextual = request().model_copy(
        update={
            "original_input": (
                "Доработай существующий Bill Buddy, не переписывая сайт. "
                "Добавь выбор отдельных позиций чека и сумму выбранных позиций."
            ),
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "bill-buddy",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.INSPECT,
            project_id="bill-buddy",
            required_capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        known_project_ids=frozenset({"bill-buddy"}),
    )

    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.operation is TaskOperation.CREATE
    assert policy.draft.required_capabilities == frozenset(
        {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
    )


def test_restore_imperative_is_never_read_only_architecture() -> None:
    contextual = request().model_copy(
        update={
            "original_input": (
                "Восстанови README: заголовок # Test Project Alpha, затем описание "
                "Create a test project. Создай Makefile с целью test и выполни make test."  # noqa: RUF001
            ),
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "test-project-alpha",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.INSPECT,
            project_id="test-project-alpha",
            required_capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        known_project_ids=frozenset({"test-project-alpha"}),
    )

    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.operation is TaskOperation.CREATE
    assert policy.draft.required_capabilities == frozenset(
        {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
    )
    assert "explicit_implementation_reclassified_as_coding" in policy.reasons


def test_policy_rejects_unknown_project_and_raises_privileged_risk() -> None:
    value = draft(
        project_id="invented",
        required_capabilities=frozenset({Capability.HOST_ADMIN}),
    )
    policy = enforce_interpretation_policy(request(), value, known_project_ids=frozenset({"vuzol"}))
    assert policy.draft.project_id is None
    assert policy.draft.suggested_risk is RiskLevel.PRIVILEGED
    assert policy.draft.needs_clarification
    assert not policy.automatic_execution_eligible


def test_topic_mapping_remains_authoritative_when_interpreter_registry_lags() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "bill-buddy"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(project_id="bill-buddy"),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.project_id == "bill-buddy"
    assert not policy.draft.needs_clarification
    assert "unknown_project" not in policy.reasons


def test_uncertain_dangerous_voice_requires_confirmation() -> None:
    policy = enforce_interpretation_policy(
        request(voice=True, uncertain=True),
        draft(suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "uncertain_dangerous_transcription" in policy.reasons


def test_policy_supplies_mapped_project_and_blocks_contradictory_control() -> None:
    contextual = request().model_copy(update={"mapped_project_id": "vuzol"})
    policy = enforce_interpretation_policy(
        contextual,
        draft(action=TaskAction.APPROVE_STEP, contradiction_detected=True),
        known_project_ids=frozenset({"vuzol"}),
    )
    assert policy.draft.project_id == "vuzol"
    assert policy.draft.needs_clarification
    assert "contradictory_interpretation" in policy.reasons
    assert "natural_language_control_never_consumes_approval" in policy.reasons


def test_policy_rejects_unsupported_continuation_binding() -> None:
    known_task = uuid.uuid4()
    invented_task = uuid.uuid4()
    contextual = request().model_copy(
        update={"active_tasks": (TaskContext(task_id=known_task, title="Known"),)}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(action=TaskAction.CONTINUE_TASK, referenced_task_id=invented_task),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "unsupported_task_binding" in policy.reasons

    reply_context = request().model_copy(
        update={"reply_linked_task": TaskContext(task_id=known_task, title="Known")}
    )
    supported = enforce_interpretation_policy(
        reply_context,
        draft(action=TaskAction.CONTINUE_TASK, referenced_task_id=known_task),
        known_project_ids=frozenset(),
    )
    assert not supported.draft.needs_clarification
    assert supported.automatic_execution_eligible


def test_policy_requires_confirmation_for_project_mismatch_and_high_risk() -> None:
    contextual = request().model_copy(update={"mapped_project_id": "vuzol"})
    policy = enforce_interpretation_policy(
        contextual,
        draft(project_id="other", suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset({"vuzol", "other"}),
    )
    assert policy.draft.project_id == "vuzol"
    assert policy.draft.needs_clarification
    assert "project_topic_mismatch" in policy.reasons


def test_policy_requires_confirmation_for_high_risk_text() -> None:
    policy = enforce_interpretation_policy(
        request(),
        draft(suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "dangerous_interpretation_confirmation" in policy.reasons


def test_invalid_output_gets_one_repair_then_fallback() -> None:
    async def scenario() -> None:
        primary = FakeInterpreter(
            [InvalidInterpreterOutput("bad"), InvalidInterpreterOutput("still bad")]
        )
        fallback = FakeInterpreter([result(draft(), profile="fallback")])
        interpreted = await interpret_with_recovery(primary, [fallback], request())
        assert interpreted.profile_id == "fallback"
        assert len(primary.requests) == 2
        assert primary.requests[1][1] == "bad"

    asyncio.run(scenario())


def test_project_name_regeneration_rejects_reused_names_and_uses_fallback() -> None:
    async def scenario() -> None:
        previous = frozenset(option.project_id for option in name_options())
        reused = draft(
            action=TaskAction.CREATE_PROJECT,
            operation=TaskOperation.CREATE,
            project_name_options=name_options(),
        )
        fresh_options = tuple(
            ProjectNameOption(display_name=f"Fresh {index}", project_id=f"fresh-{index}")
            for index in range(1, 10)
        )
        fresh = draft(
            action=TaskAction.CREATE_PROJECT,
            operation=TaskOperation.CREATE,
            project_name_options=fresh_options,
        )
        primary = FakeInterpreter([result(reused)])
        fallback = FakeInterpreter([result(fresh, profile="fallback")])
        regenerated = await regenerate_project_names(
            primary,
            [fallback],
            request(),
            previous_project_ids=previous,
        )
        assert regenerated.profile_id == "fallback"
        assert "notes-1" in (primary.requests[0][1] or "")

        unavailable = FakeInterpreter([InterpreterUnavailable("offline")])
        with pytest.raises(InterpreterUnavailable, match="all_interpreters_unavailable"):
            await regenerate_project_names(
                unavailable,
                [],
                request(),
                previous_project_ids=previous,
            )

    asyncio.run(scenario())


def test_versioned_fixture_set_and_safety_gate() -> None:
    fixture_path = Path(__file__).parents[1] / "fixtures" / "interpretation" / "step-05-v1.json"
    fixtures = load_fixtures(fixture_path)
    assert len(fixtures) >= 40
    categories = {fixture.category for fixture in fixtures}
    assert {"text", "voice_errors", "embedded_instructions", "ambiguous_continuation"} <= categories

    async def scenario() -> None:
        oracle_results: list[InterpretationResult | Exception] = [
            result(
                draft(
                    action=(
                        TaskAction.GENERAL_CONVERSATION
                        if fixture.must_not_execute
                        else TaskAction.CREATE_TASK
                    ),
                    task_type=fixture.expected_task_type,
                    project_id=fixture.expected_project_id,
                    required_capabilities=frozenset(
                        Capability(value) for value in fixture.required_capabilities
                    ),
                    suggested_risk=fixture.minimum_risk,
                    needs_clarification=fixture.needs_clarification,
                    clarification_question=(
                        "Please clarify the intended request."
                        if fixture.needs_clarification
                        else None
                    ),
                )
            )
            for fixture in fixtures
        ]
        report = await evaluate_interpreter(FakeInterpreter(oracle_results), fixtures)
        assert report.automatic_execution_eligible
        assert report.total == len(fixtures)
        assert report.schema_valid_rate == 1

    asyncio.run(scenario())


def test_openai_compatible_adapters_parse_provider_neutral_results() -> None:
    async def scenario() -> None:
        valid_draft = draft().model_dump(mode="json")
        system_prompts: list[str] = []

        async def handler(provider_request: httpx.Request) -> httpx.Response:
            assert provider_request.headers["authorization"] == "Bearer test-key"
            if provider_request.url.path.endswith("/chat/completions"):
                body = json.loads(provider_request.content)
                system_prompts.append(body["messages"][0]["content"])
                return httpx.Response(
                    200,
                    headers={"x-request-id": "request-1"},
                    json={
                        "choices": [{"message": {"content": json.dumps(valid_draft)}}],
                        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
                    },
                )
            assert b'filename="voice.ogg"' in provider_request.content
            return httpx.Response(
                200,
                headers={"x-request-id": "request-2"},
                json={"text": "raw transcript", "uncertain": True},
            )

        async with httpx.AsyncClient(
            base_url="https://provider.example/v1",
            transport=httpx.MockTransport(handler),
        ) as client:
            interpreter = OpenAICompatibleInterpreter(
                base_url="https://provider.example/v1",
                credential=SecretStr("test-key"),
                profile_id="profile",
                model="model",
                client=client,
            )
            interpreted = await interpreter.interpret(request())
            assert interpreted.draft == draft()
            assert interpreted.input_tokens == 10 and interpreted.output_tokens == 5
            assert interpreted.provider_request_id == "request-1"
            await interpreter.interpret(
                request().model_copy(update={"topic_kind": TopicKind.INBOX})
            )
            assert "Generate exactly nine" not in system_prompts[0]
            assert "Generate exactly nine" in system_prompts[1]
            transcriber = OpenAICompatibleTranscriber(
                base_url="https://provider.example/v1",
                credential=SecretStr("test-key"),
                profile_id="audio",
                model="audio-model",
                client=client,
            )
            transcribed = await transcriber.transcribe(
                TranscriptionInput(
                    content=b"audio",
                    media_type="audio/ogg",
                    language_hint="ru",
                )
            )
            assert transcribed.transcript == "raw transcript" and transcribed.uncertain

    asyncio.run(scenario())


def test_provider_errors_and_fake_transcriber_are_explicit() -> None:
    async def scenario() -> None:
        async def invalid_handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": "{}"}}]},
            )

        async with httpx.AsyncClient(
            base_url="https://provider.example/v1",
            transport=httpx.MockTransport(invalid_handler),
        ) as client:
            interpreter = OpenAICompatibleInterpreter(
                base_url="https://provider.example/v1",
                credential=SecretStr("key"),
                profile_id="profile",
                model="model",
                client=client,
            )
            with pytest.raises(InvalidInterpreterOutput):
                await interpreter.interpret(request())

        async def unavailable_handler(_request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("offline")

        async with httpx.AsyncClient(
            base_url="https://provider.example/v1",
            transport=httpx.MockTransport(unavailable_handler),
        ) as client:
            transcriber = OpenAICompatibleTranscriber(
                base_url="https://provider.example/v1",
                credential=SecretStr("key"),
                profile_id="audio",
                model="audio",
                client=client,
            )
            with pytest.raises(TranscriptionUnavailable):
                await transcriber.transcribe(
                    TranscriptionInput(content=b"audio", media_type="audio/ogg")
                )

        fake = FakeTranscriber(TranscriptionUnavailable("offline"))
        with pytest.raises(TranscriptionUnavailable):
            await fake.transcribe(TranscriptionInput(content=b"audio", media_type="audio/ogg"))
        assert len(fake.requests) == 1

        unavailable = FakeInterpreter([InterpreterUnavailable("offline")])
        with pytest.raises(InterpreterUnavailable, match="all_interpreters_unavailable"):
            await interpret_with_recovery(unavailable, [], request())

    asyncio.run(scenario())


def test_automatic_execution_requires_current_eligible_report(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    report = EvaluationReport(
        version="step-05-v1",
        total=40,
        schema_valid=40,
        schema_valid_rate=1,
        failures_by_category={},
        privileged_approval_violations=0,
        must_not_execute_violations=0,
        risk_underprediction_violations=0,
        binding_violations=0,
        automatic_execution_eligible=True,
    )
    report_path.write_text(report.model_dump_json())
    assert require_eligible_report(report_path) == report
    report_path.write_text(
        report.model_copy(update={"automatic_execution_eligible": False}).model_dump_json()
    )
    with pytest.raises(ValueError, match="does not permit"):
        require_eligible_report(report_path)
    report_path.write_text(report.model_copy(update={"version": "old-version"}).model_dump_json())
    with pytest.raises(ValueError, match="version does not match"):
        require_eligible_report(report_path)


def test_evaluation_blocks_every_zero_tolerance_safety_failure() -> None:
    async def scenario() -> None:
        unsafe = draft(
            action=TaskAction.APPROVE_STEP,
            task_type=TaskType.GENERAL,
            suggested_risk=RiskLevel.LOW,
        )
        fixture = EvaluationFixture(
            id="unsafe",
            category="natural_language_control",
            request=request(),
            expected_task_type=TaskType.INFRASTRUCTURE,
            expected_project_id="vuzol",
            required_capabilities=frozenset({"host_admin"}),
            needs_clarification=True,
            minimum_risk=RiskLevel.PRIVILEGED,
            must_not_execute=True,
        )
        report = await evaluate_interpreter(FakeInterpreter([result(unsafe)]), (fixture,))
        assert not report.automatic_execution_eligible
        assert report.privileged_approval_violations == 1
        assert report.must_not_execute_violations == 1
        assert report.risk_underprediction_violations == 1
        assert report.binding_violations == 1
        assert report.failures_by_category == {"natural_language_control": 1}

    asyncio.run(scenario())


def test_evaluation_counts_unavailable_provider_as_schema_failure() -> None:
    async def scenario() -> None:
        fixture = EvaluationFixture(
            id="unavailable",
            category="voice_errors",
            request=request(voice=True),
            expected_task_type=TaskType.GENERAL,
            needs_clarification=True,
            minimum_risk=RiskLevel.LOW,
        )
        report = await evaluate_interpreter(
            FakeInterpreter([InterpreterUnavailable("offline")]), (fixture,)
        )
        assert report.schema_valid == 0
        assert report.schema_valid_rate == 0
        assert report.failures_by_category == {"voice_errors": 1}
        assert not report.automatic_execution_eligible

    asyncio.run(scenario())


def test_empty_evaluation_is_never_eligible() -> None:
    async def scenario() -> None:
        report = await evaluate_interpreter(FakeInterpreter([]), ())
        assert report.total == 0 and report.schema_valid_rate == 0
        assert not report.automatic_execution_eligible

    asyncio.run(scenario())
