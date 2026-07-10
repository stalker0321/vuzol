# Vuzol

Vuzol is a personal task execution system controlled primarily through a private Telegram forum group. The MVP is a modular Python application with worker processes and PostgreSQL.

The architecture and implementation sequence are defined in [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) and [docs/implementation/00_MVP_PLAN.md](docs/implementation/00_MVP_PLAN.md).

## Requirements

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker with Compose for container checks

## Setup

```bash
uv sync --frozen
cp .env.example .env
```

## Commands

```bash
make run-app       # HTTP application on 127.0.0.1:8000
make run-worker    # foundation worker process
make test          # pytest suite
make lint          # Ruff lint
make format-check  # Ruff formatting check
make type-check    # strict mypy
make security      # dependency and secret checks
make check         # all required quality gates
```

Equivalent commands can be run directly with `uv run`. The application health endpoints are `/health/live` and `/health/ready`.

## Containers

```bash
docker compose build
docker compose up
```

The app and worker use the same image and run as a non-root user. The Compose configuration does not mount the Docker socket or use privileged mode.

## Configuration

Settings use the `VUZOL_` prefix. See [.env.example](.env.example). Invalid settings fail during process initialization with a concise error and non-zero exit status.

## Documentation

The documentation committed in this repository is canonical. Architecture changes require the ADR and documentation workflow described in [docs/ARCHITECTURE_INVARIANTS.md](docs/ARCHITECTURE_INVARIANTS.md).
