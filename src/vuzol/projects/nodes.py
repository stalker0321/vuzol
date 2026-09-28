"""Second-node registry: onboarding, heartbeat, revocation (WP12).

One control plane, local + one remote node. Registration is explicit;
revocation flips status (never deletes). Eligibility is fail-closed: unknown,
revoked, offline, stale-heartbeat or protocol-mismatched nodes get no claims.
``credential_ref`` is an operator-staged alias — never a value.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Node

NODE_PROTOCOL_VERSION = "node-protocol.v1"
TRUST_CLASSES = frozenset({"local", "remote"})
NODE_STATUSES = frozenset({"online", "offline", "revoked"})

_CREDENTIAL_REF_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_NODE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,99}$")


async def get_node(session: AsyncSession, node_id: str) -> Node | None:
    return cast(
        "Node | None", await session.scalar(select(Node).where(Node.node_id == node_id))
    )


async def register_node(
    session: AsyncSession,
    *,
    node_id: str,
    trust_class: str,
    protocol_version: str = NODE_PROTOCOL_VERSION,
    credential_ref: str | None = None,
    detail: str | None = None,
    now: datetime | None = None,
) -> Node:
    """Onboard (or re-onboard) a node: validated, online, heartbeat now."""

    if not _NODE_ID_PATTERN.match(node_id):
        raise ValueError(f"invalid node_id: {node_id[:60]}")
    if trust_class not in TRUST_CLASSES:
        raise ValueError(f"unknown trust class: {trust_class[:40]}")
    if protocol_version != NODE_PROTOCOL_VERSION:
        raise ValueError(f"unsupported node protocol: {protocol_version[:60]}")
    if credential_ref is not None and not _CREDENTIAL_REF_PATTERN.match(credential_ref):
        raise ValueError("credential_ref must be an operator-staged alias, never a value")
    moment = now or datetime.now(UTC)
    row = await session.scalar(
        select(Node).where(Node.node_id == node_id).with_for_update()
    )
    if row is None:
        row = Node(
            node_id=node_id,
            trust_class=trust_class,
            status="online",
            protocol_version=protocol_version,
            credential_ref=credential_ref,
            last_heartbeat_at=moment,
            detail=(detail or "")[:500],
        )
        session.add(row)
    else:
        row.trust_class = trust_class
        row.status = "online"
        row.protocol_version = protocol_version
        row.credential_ref = credential_ref
        row.last_heartbeat_at = moment
        row.detail = (detail or "")[:500]
    await session.flush()
    return row


async def heartbeat_node(
    session: AsyncSession, *, node_id: str, now: datetime | None = None
) -> None:
    """Refresh liveness. Unknown or revoked nodes fail closed."""

    row = await session.scalar(
        select(Node).where(Node.node_id == node_id).with_for_update()
    )
    if row is None:
        raise ValueError(f"unknown node: {node_id[:60]}")
    if row.status != "online":
        raise ValueError(f"node is not online: {node_id[:60]}")
    row.last_heartbeat_at = now or datetime.now(UTC)
    await session.flush()


async def mark_node_offline(
    session: AsyncSession, *, node_id: str, detail: str | None = None
) -> None:
    """Record a disconnect: the node keeps its row but gets no new claims."""

    row = await session.scalar(
        select(Node).where(Node.node_id == node_id).with_for_update()
    )
    if row is None:
        raise ValueError(f"unknown node: {node_id[:60]}")
    if row.status != "revoked":
        row.status = "offline"
    if detail is not None:
        row.detail = detail[:500]
    await session.flush()


async def revoke_node(session: AsyncSession, *, node_id: str, detail: str) -> None:
    """Terminal revocation: the row survives for audit, claims stop forever."""

    row = await session.scalar(
        select(Node).where(Node.node_id == node_id).with_for_update()
    )
    if row is None:
        raise ValueError(f"unknown node: {node_id[:60]}")
    row.status = "revoked"
    row.detail = detail[:500]
    await session.flush()


def node_is_eligible(
    node: Node | None, *, health_ttl_seconds: int = 900, now: datetime | None = None
) -> bool:
    """Fail-closed eligibility: unknown/offline/revoked/stale/protocol → False."""

    if node is None:
        return False
    if node.status != "online":
        return False
    if node.protocol_version != NODE_PROTOCOL_VERSION:
        return False
    if node.last_heartbeat_at is None:
        return False
    moment = now or datetime.now(UTC)
    age = (moment - node.last_heartbeat_at).total_seconds()
    return age >= 0 and age <= health_ttl_seconds
