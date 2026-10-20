"""Dispatch one configured CLI profile to its role's authority boundary."""

from vuzol.config.models import ProviderProfileConfig, ProviderRole
from vuzol.leadership.invocation_context import current_turn
from vuzol.providers.domain import EffectiveProfileState, ProviderRequest, ProviderResult
from vuzol.providers.ports import ProviderAdapter
from vuzol.workflows.ports import CancellationContext


class RoleBoundCliAdapter:
    def __init__(
        self, workflow: ProviderAdapter, readonly: ProviderAdapter, turn: ProviderAdapter
    ) -> None:
        self._workflow, self._readonly, self._turn = workflow, readonly, turn

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        if current_turn.get() is not None:
            if request.role not in {ProviderRole.LEAD, ProviderRole.REVIEWER}:
                raise ValueError("unsupported role within a leadership turn")
            adapter = self._turn
        elif request.role in {ProviderRole.PLANNER, ProviderRole.REVIEWER}:
            adapter = self._readonly
        else:
            adapter = self._workflow
        return await adapter.execute(request, profile, cancellation)

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        return await self._workflow.health(profile)
