"""In-memory test adapters."""

from dataclasses import dataclass, field

from vuzol.interpretation.domain import (
    InterpretationInput,
    InterpretationResult,
    TranscriptionInput,
    TranscriptionResult,
)


@dataclass(slots=True)
class FakeInterpreter:
    results: list[InterpretationResult | Exception]
    requests: list[tuple[InterpretationInput, str | None]] = field(default_factory=list, init=False)

    async def interpret(
        self, request: InterpretationInput, *, repair_error: str | None = None
    ) -> InterpretationResult:
        self.requests.append((request, repair_error))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@dataclass(slots=True)
class FakeTranscriber:
    result: TranscriptionResult | Exception
    requests: list[TranscriptionInput] = field(default_factory=list, init=False)

    async def transcribe(self, request: TranscriptionInput) -> TranscriptionResult:
        self.requests.append(request)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result
