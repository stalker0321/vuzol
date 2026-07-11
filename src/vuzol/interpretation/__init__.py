"""Provider-neutral voice transcription and semantic interpretation boundary."""

from vuzol.interpretation.domain import (
    InterpretationInput,
    InterpretationResult,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
    TranscriptionInput,
    TranscriptionResult,
)
from vuzol.interpretation.ports import SemanticInterpreter, Transcriber

__all__ = [
    "InterpretationInput",
    "InterpretationResult",
    "SemanticInterpreter",
    "TaskAction",
    "TaskDraft",
    "TaskOperation",
    "TaskType",
    "Transcriber",
    "TranscriptionInput",
    "TranscriptionResult",
]
