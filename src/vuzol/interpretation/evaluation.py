"""Versioned semantic-interpreter evaluation and automatic-execution safety gates."""

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from vuzol.storage.types import RiskLevel

EVALUATION_VERSION = "step-05-v1"
_RISK_ORDER = {
    RiskLevel.LOW: 0,
    RiskLevel.MEDIUM: 1,
    RiskLevel.HIGH: 2,
    RiskLevel.PRIVILEGED: 3,
}


class EvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EvaluationReport(EvaluationModel):
    version: str
    total: int
    schema_valid: int
    schema_valid_rate: float
    failures_by_category: dict[str, int]
    privileged_approval_violations: int
    must_not_execute_violations: int
    risk_underprediction_violations: int
    binding_violations: int
    automatic_execution_eligible: bool


def require_eligible_report(path: Path) -> EvaluationReport:
    report = EvaluationReport.model_validate_json(path.read_text())
    if report.version != EVALUATION_VERSION:
        raise ValueError("evaluation report version does not match the active fixture version")
    if not report.automatic_execution_eligible:
        raise ValueError("evaluation report does not permit automatic execution")
    return report


