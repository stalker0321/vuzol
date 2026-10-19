"""Human and agent actors. An agent principal is never an owner user id."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from vuzol.workflows.application import Principal, validate_principal


class ActorKind(StrEnum):
    HUMAN = "human"
    AGENT = "agent"


class ActorRejected(ValueError):
    """The caller tried to wear an identity the server did not assign."""


@dataclass(frozen=True, slots=True)
class Actor:
    kind: ActorKind
    subject: str
    ingress_source: str

    @property
    def user_id(self) -> int | None:
        if self.kind is not ActorKind.HUMAN:
            return None
        return int(self.subject)

    @staticmethod
    def from_principal(principal: Principal) -> Actor:
        validate_principal(principal)
        return Actor(
            kind=ActorKind.HUMAN,
            subject=str(principal.user_id),
            ingress_source=principal.ingress_source,
        )

    @staticmethod
    def agent(principal_id: str, ingress_source: str) -> Actor:
        if not principal_id.startswith("agent:") or principal_id == "agent:":
            raise ActorRejected("agent_principal_invalid")
        remainder = principal_id.removeprefix("agent:")
        if remainder.isdigit():
            raise ActorRejected("agent_principal_invalid")
        if ingress_source not in {"telegram", "cli", "api", "worker"}:
            raise ActorRejected("agent_ingress_invalid")
        return Actor(kind=ActorKind.AGENT, subject=principal_id, ingress_source=ingress_source)


def server_agent_principal(project_id: str) -> str:
    """Stable principal for one project's lead. The model does not choose this."""

    if not project_id or any(character.isspace() for character in project_id):
        raise ActorRejected("agent_principal_invalid")
    return f"agent:project-lead:{project_id}"
