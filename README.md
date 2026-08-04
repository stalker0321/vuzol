# Vuzol

[![CI](https://github.com/stalker0321/vuzol/actions/workflows/ci.yml/badge.svg)](https://github.com/stalker0321/vuzol/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

Vuzol is a self-hosted AI task orchestrator controlled from Telegram. Send a request in a project
topic, choose an AI provider, and Vuzol turns the conversation into a durable workflow that can
plan, edit code, run tests, request approval, and safely apply the result.

It is built for personal infrastructure where convenience matters, but handing an AI agent direct
access to the host is not acceptable.

> **Project status:** active development and real-world dogfooding. The core task, discussion,
> coding, validation, review, and approval flows are implemented. Deployment still assumes an
> experienced operator and is not yet a one-command install.

## Why Vuzol?

Most chat bots stop at generating an answer. Vuzol manages the work around that answer:

- **Telegram-native workspace** — each forum topic maps to a project, with status cards, history,
  model selection, approvals, and voice-message intake.
- **Multi-provider routing** — provider accounts and models are selected independently, with
  capability, health, budget, project preference, and fallback policies.
- **Durable workflows** — PostgreSQL-backed tasks, steps, leases, events, inboxes, and outboxes
  survive restarts without treating Telegram as the source of truth.
- **Isolated coding agents** — every coding attempt runs in its own Git worktree and rootless
  container with explicit filesystem, network, and resource boundaries.
- **Trusted validation** — tests run separately from the model in a pinned validation image; the
  executor cannot declare its own result valid.
- **Human approval before apply** — reviewed result commits are applied by a narrow, separate
  service using compare-and-swap protection.
- **Operational recovery** — idempotency keys, fenced leases, bounded retries, retention, backup,
  and restore paths are first-class parts of the design.

## How it works

```text
Telegram text / voice
        │
        ▼
 durable intake ──► semantic interpretation ──► task or work package
        │                                            │
        │                                            ▼
        │                                  provider routing + budget
        │                                            │
        ▼                                            ▼
 status projection                         isolated worktree execution
                                                     │
                                                     ▼
                                          trusted tests and review
                                                     │
                                                     ▼
                                           Telegram approval card
                                                     │
                                                     ▼
                                           CAS-protected local apply
```

Telegram messages are reconstructable projections. PostgreSQL remains the source of truth, so a
delivery retry or process restart cannot silently lose workflow state.

## Supported integrations

| Area | Current support |
| --- | --- |
| Interface | Telegram forum groups, text and voice intake |
| Coding providers | Codex CLI, Grok CLI, Kimi Code through an OpenAI-compatible gateway |
| Interpretation | OpenAI-compatible model profiles |
| Repositories | Local Git repositories with managed branches and isolated worktrees |
| Execution | Rootless Docker, pinned sandbox and validation images |
| Persistence | PostgreSQL 16 with Alembic migrations |
| Operations | systemd units, health checks, retention, encrypted backup and restore |

Provider credentials are kept outside the repository and scoped to the runtime identity that needs
them. See [Provider routing and budgets](docs/PROVIDERS.md) for the profile model.

## Quick start for development

### Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker with Compose

```bash
git clone https://github.com/stalker0321/vuzol.git
cd vuzol
uv sync --frozen
cp .env.example .env
make db-up
make db-migrate
make check
```

Run the health application and worker in separate terminals:

```bash
make run-app
make run-worker
```

The health endpoints are available at `http://127.0.0.1:8000/health/live` and
`http://127.0.0.1:8000/health/ready`.

The base container stack can also be started with Compose. Telegram and interpretation services
are opt-in profiles because they require credentials:

```bash
docker compose --profile telegram --profile interpretation up
```

Before enabling them, configure the Telegram allowlist, bot token, project registry, database DSN,
and provider profiles. The complete setup reference is in
[Configuration](docs/CONFIGURATION.md).

## Safety model

Vuzol deliberately separates responsibilities instead of running one all-powerful agent process:

1. The **ingress and delivery services** communicate with Telegram but do not edit repositories.
2. The **worker** owns workflow state and dispatch but has no provider sandbox or repository-write
   capability.
3. The **executor** gives a provider access only to a task worktree inside a rootless sandbox.
4. The **validator** measures the resulting Git state and runs trusted gates independently.
5. The **applier** can advance configured local branches only after an exact result is approved.

The coding path does not push branches, open pull requests, deploy applications, or grant general
host access. High-risk results require additional review and mechanical blockers fail closed.
The stable security contracts are documented in
[Architecture invariants](docs/ARCHITECTURE_INVARIANTS.md).

## Development commands

```bash
make lint           # Ruff linting
make format-check   # formatting verification
make type-check     # strict mypy checks
make test           # unit and default integration tests
make test-postgres  # PostgreSQL migration and concurrency tests
make security       # secret and dependency checks
make check          # complete local quality gate
```

CI runs the quality suite, PostgreSQL integration tests, Compose validation, and container builds.

## Repository map

```text
src/vuzol/          application, workflow, provider, execution, and Telegram modules
alembic/            database migrations
config/             non-secret registry examples
deploy/             systemd, sandbox, proxy, and validation assets
docs/               operator guides, architecture decisions, and design invariants
tests/              unit and integration test suites
```

## Documentation

- [Configuration](docs/CONFIGURATION.md)
- [Telegram workspace](docs/TELEGRAM.md)
- [Voice and semantic interpretation](docs/INTERPRETATION.md)
- [Provider routing and budgets](docs/PROVIDERS.md)
- [PostgreSQL storage](docs/STORAGE.md)
- [Architecture invariants](docs/ARCHITECTURE_INVARIANTS.md)
- [Testing policy](docs/TESTING.md)
- [Architecture decisions](docs/decisions/)
- [Changelog](docs/CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)

## Contributing

Vuzol is currently shaped around a single self-hosted deployment, but focused bug reports and pull
requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and run `make check` before
submitting a change.

## License

Vuzol is open-source software licensed under the
[Apache License 2.0](LICENSE).
