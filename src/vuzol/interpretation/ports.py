"""Replaceable provider ports used by task services."""

from typing import Protocol

from vuzol.interpretation.domain import (
    InterpretationInput,
    InterpretationResult,
    TranscriptionInput,
    TranscriptionResult,
)


class InterpreterUnavailable(RuntimeError):
    pass


class InvalidInterpreterOutput(RuntimeError):
    pass


class TranscriptionUnavailable(RuntimeError):
    pass


class SemanticInterpreter(Protocol):
    async def interpret(
        self, request: InterpretationInput, *, repair_error: str | None = None
    ) -> InterpretationResult: ...


class Transcriber(Protocol):
    async def transcribe(self, request: TranscriptionInput) -> TranscriptionResult: ...


class AttachmentDownloader(Protocol):
    async def download(self, file_id: str) -> bytes: ...
