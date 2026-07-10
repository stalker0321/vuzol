"""Application composition root."""

from fastapi import FastAPI

from vuzol import __version__
from vuzol.app.health import HealthStatus, health_status
from vuzol.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build an application with explicit settings injection."""

    resolved = settings or get_settings()
    app = FastAPI(title="Vuzol", version=__version__)

    @app.get("/health/live", response_model=HealthStatus)
    @app.get("/health/ready", response_model=HealthStatus)
    def health() -> HealthStatus:
        return health_status(service=resolved.service_name, environment=resolved.environment)

    return app
