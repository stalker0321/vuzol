"""Provider-independent process health."""

from typing import Literal

from pydantic import BaseModel


class HealthStatus(BaseModel):
    status: Literal["ok"] = "ok"
    service: str
    environment: str


def health_status(*, service: str, environment: str) -> HealthStatus:
    """Return readiness state for the foundation process."""

    return HealthStatus(service=service, environment=environment)
