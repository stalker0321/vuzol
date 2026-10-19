"""Project-lead overlay. Inactive unless ``settings.project_lead.enabled`` is set.

The package does not import the command service at import time. Workflow code
imports leaf modules from here, and loading the service pulls the workflow
stack back in.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vuzol.leadership.service import LeadershipCommandService as LeadershipCommandService

__all__ = ["LeadershipCommandService"]


def __getattr__(name: str) -> object:
    if name == "LeadershipCommandService":
        from vuzol.leadership.service import LeadershipCommandService

        return LeadershipCommandService
    raise AttributeError(f"module {__name__!r} has no attribute {name}")
