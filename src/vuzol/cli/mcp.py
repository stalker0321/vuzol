"""Run the opt-in, loopback-only MCP resource server behind an HTTPS proxy."""

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    # Container deployments publish this listener on a loopback-only host port.
    parser.add_argument(
        "--host", choices=("127.0.0.1", "0.0.0.0"), default="127.0.0.1"  # noqa: S104
    )
    args = parser.parse_args()
    # Optional dependency is loaded only by this entrypoint.
    import uvicorn

    from vuzol.config import get_runtime_configuration
    from vuzol.leadership.access import ProjectApplication
    from vuzol.mcp.auth import MCPConfig
    from vuzol.mcp.server import create_http_app, create_server
    from vuzol.observability import configure_logging
    from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn

    config = MCPConfig.model_validate_json(args.config.read_text())
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    configure_logging(service="vuzol-mcp", level=runtime.settings.log_level)
    engine = create_engine(runtime.settings, resolve_database_dsn(runtime.settings))
    app = ProjectApplication(runtime, create_session_factory(engine))
    server = create_server(app, config)
    uvicorn.run(create_http_app(server, config, engine=engine), host=args.host, port=config.port)


if __name__ == "__main__":
    main()
