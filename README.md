# Vuzol

Vuzol is a personal task-intake system controlled through a private Telegram forum group. It uses
PostgreSQL as its source of truth and keeps Telegram messages as reconstructable projections of
durable state.

## Current flow

The implemented flow is:

```text
Telegram text or voice message
→ authorized, deduplicated durable intake
→ persisted Task and Telegram status card
→ private attachment storage and transcription when needed
→ semantic interpretation
→ validated, provider-neutral TaskDraft
```

Ingress, Telegram delivery, and interpretation run as separate processes. Their inbox/outbox and
lease records make completed delivery, transcription, and interpretation safe across process
restarts.

The current MVP foundation does **not** execute tasks. Workflow management, executor routing,
Codex integration, Git worktrees, sandbox execution, automated validation, and deployment are not
implemented or advertised as available behavior.

## Requirements and setup

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker with Compose

```bash
uv sync --frozen
cp .env.example .env
make db-up
make db-migrate
```

Settings use the `VUZOL_` prefix. Registry files contain non-secret project, provider-profile, and
Telegram-topic configuration; credentials are supplied through scoped environment or file
references. See [Configuration](docs/CONFIGURATION.md).

## Runtime commands

```bash
make run-app                  # HTTP health application on 127.0.0.1:8000
make run-worker               # foundation worker process
vuzol-telegram                # Telegram long-polling ingress
vuzol-telegram-delivery       # Telegram outbox delivery
vuzol-interpreter             # transcription and semantic interpretation
make check                    # lint, format, types, tests, and security checks
make test-postgres            # PostgreSQL migration and concurrency tests
```

The health endpoints are `/health/live` and `/health/ready`.

For containers, the base stack is available through `docker compose up`. Telegram and
interpretation are optional Compose profiles:

```bash
docker compose --profile telegram --profile interpretation up
```

Configure the registry, allowlists, database DSN reference, one shared Telegram bot token, and the
selected interpretation profiles before enabling them. The default image is non-root and the
Compose services do not mount the Docker socket or use privileged mode.

## Documentation

- [Configuration](docs/CONFIGURATION.md)
- [PostgreSQL storage](docs/STORAGE.md)
- [Telegram workspace](docs/TELEGRAM.md)
- [Voice and semantic interpretation](docs/INTERPRETATION.md)
- [Architecture invariants](docs/ARCHITECTURE_INVARIANTS.md)
- [Accepted architecture decisions](docs/decisions/)
- [Changelog](docs/CHANGELOG.md)
- [Contributing and documentation policy](CONTRIBUTING.md)

Repository documentation covers the public product, operation, stable architecture, and accepted
decisions. Internal implementation plans and agent handoffs are maintained outside the repository
and are never required by the application, build, installation, or tests.
