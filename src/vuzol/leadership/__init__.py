"""Project-lead overlay. Inactive unless ``settings.project_lead.enabled`` is set."""

from vuzol.leadership.service import LeadershipCommandService

__all__ = ["LeadershipCommandService"]
