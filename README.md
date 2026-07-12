# Vuzol

[![CI](https://github.com/stalker0321/vuzol/actions/workflows/ci.yml/badge.svg)](https://github.com/stalker0321/vuzol/actions/workflows/ci.yml)

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
→ versioned persisted workflow and explicit steps
→ deterministic capability/health/budget-aware provider routing
→ atomic budget reservation and fenced model-only execution
→ restart recovery
```

Ingress, Telegram delivery, interpretation, and workflow management run as separate processes.
Their inbox/outbox, step, event, and fenced lease records make completed delivery, transcription,
interpretation, controls, and workflow progress safe across process restarts.

The current MVP foundation can route and execute safe model-only OpenAI-compatible workflow steps.
It does **not** yet execute repository edits or tools. Codex process execution, Git worktrees,
sandbox execution, automated validation, approvals, and deployment are not implemented or
advertised as available behavior.

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
make run-worker               # workflow dispatch, recovery, controls, and registered handlers
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

## Execution (worktrees + sandbox)

The dedicated `vuzol-executor` process (see `src/vuzol/cli/executor.py`) runs `prepare_worktree` and `execute_code` steps using per-task Git worktrees and a rootless Docker sandbox.

Systemd units:

- `deploy/systemd/user/vuzol-rootless-docker.service` — dedicated rootless Docker daemon as a linger-enabled systemd **user** unit (installed to `/etc/systemd/user/vuzol-rootless-docker.service`). Runs inside the `vuzol-executor` user's manager (with `loginctl enable-linger`). Uses `%t` for the socket path under the user's XDG_RUNTIME_DIR. No `User=`/`Group=` lines.
- `deploy/systemd/vuzol-executor.service` — the system service (with `User=vuzol-executor`) that runs the dedicated executor worker. It no longer declares a direct systemd dependency on the docker unit (user units are in a separate manager). Readiness is provided by an `ExecStartPre` socket wait plus the strict `RootlessDockerRuntime.preflight()` gate inside the process.

The executor process must not start until the user-managed rootless daemon is up, its socket is present and owned by the executor identity, and the daemon reports rootless mode + seccomp + cgroup v2 with enforceable CPU/memory limits. The preflight (and therefore the unit) fails closed on any violation and never falls back to the host root Docker socket. See `deploy/systemd/vuzol-executor.service` comments and the Step 08 handoff reports for the exact portable installation and verification steps.
- [Telegram workspace](docs/TELEGRAM.md)
- [Voice and semantic interpretation](docs/INTERPRETATION.md)
- [Provider routing and budgets](docs/PROVIDERS.md)
- [Architecture invariants](docs/ARCHITECTURE_INVARIANTS.md)
- [Accepted architecture decisions](docs/decisions/)
- [Changelog](docs/CHANGELOG.md)
- [Contributing and documentation policy](CONTRIBUTING.md)

Repository documentation covers the public product, operation, stable architecture, and accepted
decisions. Internal implementation plans and agent handoffs are maintained outside the repository
and are never required by the application, build, installation, or tests.
