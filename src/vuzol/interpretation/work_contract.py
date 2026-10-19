"""Compatibility of semantic choices, independent of wording and providers.

A task domain is not a permission and an unknown domain is not architecture.
Contradictory choices require clarification, never a different requested effect.
"""

from vuzol.config import Capability
from vuzol.interpretation.domain import TaskOperation, TaskType

WORK_CONTRACT_VERSION = "semantic-work-contract.v1"
WORK_CONTRACT_QUESTION = (
    "Не удалось согласовать вид работы с запрошенным действием. "  # noqa: RUF001
    "Нужно реализовать изменения в проекте, только изучить/спроектировать решение "
    "или выполнить другую работу? Ничего не запущено."
)


def work_contract_error(effect: str, task_type: TaskType, operation: TaskOperation) -> str | None:
    """Validate joint meaning; never infer intent from a word or grant authority."""
    if effect != "execute_request":
        return None
    if task_type is TaskType.GENERAL:
        return "work_type_missing"
    if task_type is TaskType.CODING and operation in {
        TaskOperation.INSPECT,
        TaskOperation.EXPLAIN,
        TaskOperation.MONITOR,
    }:
        return "coding_requires_implementation"
    if task_type is TaskType.ARCHITECTURE and operation in {
        TaskOperation.MODIFY,
        TaskOperation.FIX,
        TaskOperation.DEPLOY,
        TaskOperation.MONITOR,
    }:
        return "architecture_is_read_only"
    if task_type in {TaskType.RESEARCH, TaskType.FILE_PROCESSING} and operation in {
        TaskOperation.DEPLOY,
        TaskOperation.MONITOR,
    }:
        return "operation_outside_work_domain"
    return None


def capabilities_for_work(task_type: TaskType) -> frozenset[Capability]:
    """Runtime-owned capability floor; normal project/risk/approval policy still applies."""
    return {
        TaskType.CODING: frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        TaskType.ARCHITECTURE: frozenset({Capability.REPOSITORY_READ}),
        TaskType.RESEARCH: frozenset({Capability.WEB_RESEARCH}),
        TaskType.INFRASTRUCTURE: frozenset({Capability.HOST_ADMIN}),
        TaskType.FILE_PROCESSING: frozenset(),
        TaskType.GENERAL: frozenset(),
    }[task_type]
