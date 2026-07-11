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
vuzol-telegram     # Telegram long-polling ingress
vuzol-telegram-delivery # Telegram outbox delivery runtime
make test          # pytest suite
make lint          # Ruff lint
make format-check  # Ruff formatting check
make type-check    # strict mypy
make security      # dependency and secret checks
make check         # all required quality gates
make test-postgres # real PostgreSQL migration and concurrency tests
```

Equivalent commands can be run directly with `uv run`. The application health endpoints are `/health/live` and `/health/ready`.

## Containers

```bash
docker compose build
docker compose up
```

The app and worker use the same image and run as a non-root user. The Compose configuration does not mount the Docker socket or use privileged mode.

Telegram is an optional profile. Configure the registry, allowlists, database DSN reference, and
one shared bot token as described in [.env.example](.env.example), run migrations, then start both
separate Telegram processes with:

```bash
docker compose --profile telegram up
```

Ingress receives updates; delivery exclusively consumes normal `telegram` outbox operations.
The base stack does not require a Telegram token.

## Configuration

Settings use the `VUZOL_` prefix. See [.env.example](.env.example). Invalid settings fail during process initialization with a concise error and non-zero exit status.

Project, provider, topic, secret-reference, revision, and reload contracts are documented in [docs/CONFIGURATION.md](docs/CONFIGURATION.md). A disabled example registry is available at [config/registries.example.toml](config/registries.example.toml).

PostgreSQL schema, migration, transaction, and test operations are documented in [docs/STORAGE.md](docs/STORAGE.md).

## Documentation

The documentation committed in this repository is canonical. Architecture changes require the ADR and documentation workflow described in [docs/ARCHITECTURE_INVARIANTS.md](docs/ARCHITECTURE_INVARIANTS.md).
