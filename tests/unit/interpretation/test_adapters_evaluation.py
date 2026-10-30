"""Adapters evaluation tests (split for cohesion)."""

from __future__ import annotations

from typing import Any

from vuzol.config import OpenRouterProviderRouting

from ._test_interpretation_helpers import (
    EvaluationReport,
    FakeInterpreter,
    FakeTranscriber,
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    OpenAICompatibleInterpreter,
    OpenAICompatibleTranscriber,
    Path,
    SecretStr,
    TopicKind,
    TranscriptionInput,
    TranscriptionUnavailable,
    asyncio,
    draft,
    httpx,
    interpret_with_recovery,
    json,
    pytest,
    request,
    require_eligible_report,
)


def test_openai_compatible_adapters_parse_provider_neutral_results() -> None:
    async def scenario() -> None:
        valid_draft = draft().model_dump(mode="json")
        system_prompts: list[str] = []
        user_payloads: list[dict[str, Any]] = []

        async def handler(provider_request: httpx.Request) -> httpx.Response:
            assert provider_request.headers["authorization"] == "Bearer test-key"
            if provider_request.url.path.endswith("/chat/completions"):
                body = json.loads(provider_request.content)
                system_prompts.append(body["messages"][0]["content"])
                user_payloads.append(json.loads(body["messages"][1]["content"]))
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
            assert "needs_planning" not in user_payloads[0]["task_draft_schema"]["properties"]
            assert "needs_planning" not in user_payloads[0]["input"]
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


def test_openai_interpreter_sends_profile_token_and_reasoning_limits() -> None:
    async def scenario() -> None:
        bodies: list[dict[str, object]] = []

        async def handler(provider_request: httpx.Request) -> httpx.Response:
            bodies.append(json.loads(provider_request.content))
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {"message": {"content": json.dumps(draft().model_dump(mode="json"))}}
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5},
                },
            )

        async with httpx.AsyncClient(
            base_url="https://provider.example/v1",
            transport=httpx.MockTransport(handler),
        ) as client:
            interpreter = OpenAICompatibleInterpreter(
                base_url="https://provider.example/v1",
                credential=SecretStr("test-key"),
                profile_id="interpreter",
                model="model",
                output_token_limit=1_000,
                reasoning_enabled=False,
                client=client,
            )
            await interpreter.interpret(request())

        assert bodies[0]["max_tokens"] == 1_000
        assert bodies[0]["reasoning"] == {"enabled": False}

    asyncio.run(scenario())


def test_openai_interpreter_keeps_legacy_reasoning_default_for_openrouter() -> None:
    async def scenario() -> None:
        bodies: list[dict[str, object]] = []

        async def handler(provider_request: httpx.Request) -> httpx.Response:
            bodies.append(json.loads(provider_request.content))
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {"message": {"content": json.dumps(draft().model_dump(mode="json"))}}
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5},
                },
            )

        async with httpx.AsyncClient(
            base_url="https://openrouter.ai/api/v1",
            transport=httpx.MockTransport(handler),
        ) as client:
            interpreter = OpenAICompatibleInterpreter(
                base_url="https://openrouter.ai/api/v1",
                credential=SecretStr("test-key"),
                profile_id="interpreter",
                model="model",
                provider_routing=OpenRouterProviderRouting.model_validate(
                    {
                        "sort": {"by": "price", "partition": "none"},
                        "preferred_min_throughput": {"p90": 70},
                        "quantizations": ["int8"],
                    }
                ),
                client=client,
            )
            await interpreter.interpret(request())

        assert "max_tokens" not in bodies[0]
        assert bodies[0]["reasoning"] == {"effort": "none"}

    asyncio.run(scenario())


def test_gpt_transcribe_sends_languages_and_owner_keywords(tmp_path) -> None:
    from vuzol.interpretation.service import transcription_keywords

    vocabulary = tmp_path / "keywords.txt"
    vocabulary.write_text(
        "# work slang\nREADME\nлид\n\nREADME\n<bad>\nдеплой\n",  # noqa: RUF001
        encoding="utf-8",
    )
    keywords = transcription_keywords(vocabulary)
    assert keywords == ("README", "лид", "деплой")
    assert transcription_keywords(tmp_path / "missing.txt") == ()

    seen: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.content)
        return httpx.Response(200, json={"text": "Добавь пример в README"})

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://provider.example/v1", transport=httpx.MockTransport(handler)
        ) as client:
            transcriber = OpenAICompatibleTranscriber(
                base_url="https://provider.example/v1",
                credential=SecretStr("key"),
                profile_id="audio",
                model="gpt-transcribe",
                client=client,
            )
            result = await transcriber.transcribe(
                TranscriptionInput(
                    content=b"audio", media_type="audio/ogg", language_hint="ru", keywords=keywords
                )
            )
            assert result.transcript == "Добавь пример в README"

    asyncio.run(scenario())
    body = seen[0].decode("utf-8", "replace")
    assert 'name="languages[]"' in body and 'name="language"' not in body
    assert body.count('name="keywords[]"') == 3 and "деплой" in body
