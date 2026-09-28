"""Node-scoped scheduler claim boundary (WP12).

Every database access here carries the node filter: unknown, revoked,
offline, stale-heartbeat or protocol-mismatched nodes get no claims
(fail-closed). Capability health narrows the worker's capability set before
delegating to ``leasing.claim_step`` — steps requiring an installation that
is stale/failed on this node are not eligible here. Trust scoping is an
explicit optional filter, never inferred.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.settings import Settings
from vuzol.ops.disk_pressure import FreeSpaceProbe
from vuzol.projects.installations import installation_states
from vuzol.projects.nodes import get_node, node_is_eligible
from vuzol.storage.leasing import claim_step
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import QueueClass


async def claim_node_step(
    session: AsyncSession,
    *,
    node_id: str,
    owner: str,
    lease_seconds: int,
    capabilities: frozenset[str],
    queue_classes: frozenset[QueueClass] = frozenset(QueueClass),
    class_limits: dict[QueueClass, int] | None = None,
    profile_limits: dict[str, int] | None = None,
    step_types: frozenset[str] | None = None,
    candidate_limit: int = 20,
    require_trust_class: str | None = None,
    health_ttl_seconds: int = 900,
    settings: Settings | None = None,
    free_space_probe: FreeSpaceProbe | None = None,
    now: datetime | None = None,
) -> LeaseToken | None:
    """Claim a step for one node, or nothing when the node is not eligible."""

    node = await get_node(session, node_id)
    if not node_is_eligible(node, health_ttl_seconds=health_ttl_seconds, now=now):
        return None
    assert node is not None
    if require_trust_class is not None and node.trust_class != require_trust_class:
        return None
    states = await installation_states(session, node_id=node_id, now=now)
    allowed = frozenset(
        capability
        for capability in capabilities
        if states.get(capability, "installed") == "installed"
    )
    return await claim_step(
        session,
        owner=owner,
        lease_seconds=lease_seconds,
        capabilities=allowed,
        queue_classes=queue_classes,
        class_limits=class_limits,
        profile_limits=profile_limits,
        step_types=step_types,
        candidate_limit=candidate_limit,
        settings=settings,
        free_space_probe=free_space_probe,
    )
