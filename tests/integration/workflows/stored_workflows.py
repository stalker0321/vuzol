"""Frozen historical steps used to exercise upgrades without a legacy compiler."""

import json
import uuid
from pathlib import Path

from pydantic import TypeAdapter

from vuzol.interpretation.domain import TaskDraft
from vuzol.workflows.domain import MaterializedStep, MaterializedWorkflow


def stored_workflow(
    draft: TaskDraft,
    *,
    interpretation_id: uuid.UUID,
    configured_workflow: str | None = None,
) -> MaterializedWorkflow:
    name = (
        configured_workflow
        or {
            "coding": "coding.v4",
            "architecture": "architecture.v1",
            "research": "research.v1",
            "infrastructure": "infrastructure.v1",
            "file_processing": "simple_model.v1",
            "general": "simple_model.v1",
        }[draft.task_type.value]
    )
    records = json.loads(Path(__file__).with_name("stored_steps.json").read_text())[name]
    for row in records:
        if row["key"] == "interpret":
            row["payload"] = {"interpretation_id": str(interpretation_id)}
    workflow_type, version = name.rsplit(".v", 1)
    return MaterializedWorkflow(
        workflow_type,
        version,
        interpretation_id,
        TypeAdapter(tuple[MaterializedStep, ...]).validate_python(records),
    )
