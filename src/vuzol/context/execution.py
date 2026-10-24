"""Configured execution constraints shared by routing and worker context."""

import hashlib
import json

from vuzol.config.models import ProjectConfig
from vuzol.providers.domain import ContextItem


def execution_constraints_context(
    project: ProjectConfig | None,
    *,
    dependency_provisioning_enabled: bool,
) -> ContextItem | None:
    """Expose actual configured checks/install policy, not a new task or plan."""
    if project is None:
        return None
    body = json.dumps(
        {
            "project_id": project.id,
            "trusted_validation_commands": [list(c.argv) for c in project.validation_commands],
            "dependency_provisioning_enabled": dependency_provisioning_enabled,
            "instruction": (
                "These are runtime facts, not permission to change the user's goal. "
                "Create meaningful tests for configured validation commands. "
                "When dependency provisioning is disabled, use available tools without introducing "
                "packages that cannot be installed under this policy. If the agreed scope needs "
                "unavailable dependencies, report the missing setup rather than claiming success. "
                "Do not publish or start servers: result apply/publication remains "
                "owner-controlled."
            ),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return ContextItem(
        source="system_execution_constraints",
        reference=f"project:{project.id}:execution",
        content=body,
        content_hash=hashlib.sha256(body.encode()).hexdigest(),
    )
