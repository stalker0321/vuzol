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

__all__ = [
    "Capability",
    "EvaluationFixture",
    "EvaluationReport",
    "FakeInterpreter",
    "FakeTranscriber",
    "InterpretationInput",
    "InterpretationResult",
    "InterpreterUnavailable",
    "InvalidInterpreterOutput",
    "OpenAICompatibleInterpreter",
    "OpenAICompatibleTranscriber",
    "Path",
    "ProjectNameOption",
    "RiskLevel",
    "SecretStr",
    "SuggestedComplexity",
    "TaskAction",
    "TaskContext",
    "TaskDraft",
    "TaskOperation",
    "TaskType",
    "TopicKind",
    "TranscriptionInput",
    "TranscriptionUnavailable",
    "ValidationError",
    "asyncio",
    "draft",
    "enforce_interpretation_policy",
    "evaluate_interpreter",
    "httpx",
    "interpret_with_recovery",
    "json",
    "load_fixtures",
    "name_options",
    "pytest",
    "regenerate_project_names",
    "request",
    "require_eligible_report",
    "result",
    "uuid",
]


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
