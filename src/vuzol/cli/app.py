"""HTTP application CLI."""

import uvicorn

from vuzol.app import create_app
from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging, get_logger


def main() -> None:
    """Load settings and run the HTTP application."""

    settings = get_runtime_configuration().settings
    configure_logging(service=f"{settings.service_name}-app", level=settings.log_level)
    get_logger(__name__).info("application starting", extra={"event": "app.starting"})
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_config=None)


if __name__ == "__main__":
    main()
