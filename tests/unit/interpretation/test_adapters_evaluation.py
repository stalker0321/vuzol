"""Adapters evaluation tests (split for cohesion)."""

from __future__ import annotations

from ._test_interpretation_helpers import (
    Capability,
    EvaluationFixture,
    EvaluationReport,
    FakeInterpreter,
    FakeTranscriber,
    InterpretationResult,
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    OpenAICompatibleInterpreter,
    OpenAICompatibleTranscriber,
    Path,
    RiskLevel,
    SecretStr,
    TaskAction,
    TaskType,
    TopicKind,
    TranscriptionInput,
    TranscriptionUnavailable,
    asyncio,
    draft,
    evaluate_interpreter,
    httpx,
    interpret_with_recovery,
    json,
    load_fixtures,
    pytest,
    request,
    require_eligible_report,
    result,
)


def test_versioned_fixture_set_and_safety_gate() -> None:
    fixture_path = Path(__file__).parents[2] / "fixtures" / "interpretation" / "step-05-v1.json"
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
