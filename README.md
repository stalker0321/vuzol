# Vuzol

[![CI](https://github.com/stalker0321/vuzol/actions/workflows/ci.yml/badge.svg)](https://github.com/stalker0321/vuzol/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

Vuzol is a self-hosted team of AI agents you run from Telegram. Each project gets a forum
topic with its own **project lead**, an AI agent that talks with you, plans the work, hands
tasks to coding agents in isolated sandboxes, has the results reviewed and asks for your
approval before anything lands in the repository. A **personal assistant** topic handles
errands and dispatches work to the right project. The private chat with the bot is an
**inbox**: anything you forward becomes a searchable knowledge base.

It is built for personal infrastructure where convenience matters, but handing an AI agent
direct access to the host is not acceptable.

> **Project status:** in daily use by its author on one deployment. Installing it elsewhere
> still assumes an experienced operator.

## What it does

- **Project leads.** Write or dictate in a project topic. The lead answers questions,
  clarifies a request, proposes a plan as a card and, after you approve it, runs the tasks
  one by one. It keeps a notebook of project facts and decisions, writes a retrospective when
  a goal ends, and tidies its notes in a nightly pass. A request that is not about the current
  goal goes into the topic's task queue.
- **Personal assistant.** A separate topic for errands (search, flights, small research) and
  for sending tasks to project topics or creating a new project with its repository.
- **Inbox.** Forward links, files, voice notes or text to the bot in private. It summarizes
  and files them, and project leads can recall them.
- **Monitor (optional).** Watches public sources (RSS, GitHub releases, Hacker News, Reddit),
  ranks items against your interests and posts a digest to a topic.
- **Isolated coding agents.** Every coding attempt runs in its own Git worktree inside a
  rootless container with explicit filesystem, network and resource limits. Network access
  goes through an allowlisting proxy. Work can also run on other machines' Docker.
- **Trusted validation and review.** Project gates run in a separate validation sandbox, and an
  independent model reviews each result; the agent cannot declare its own work done.
- **Durable by design.** PostgreSQL is the source of truth. Leases, fencing, idempotency keys
  and transactional outboxes make restarts and retries safe. Telegram messages are projections.

## How it works

```text
Telegram (project topic, assistant topic, private inbox)
        │
        ▼
 durable intake ──► project lead (model) ──► plan card ──► owner approval
                          │                                    │
                          │ notebook, decision log, recall     ▼
                          │                          task: isolated worktree
                          │                          + coding agent sandbox
                          │                                    │
                          ▼                                    ▼
                  replies, status cards ◄──── trusted gates + independent review
                                                               │
                                                               ▼
                                              CAS-protected apply to the project branch
```

## Models and providers

Leads, coding agents, reviewers and the assistant are configured independently per role.

| Kind | Supported |
| --- | --- |
| CLI agents in sandboxes | Codex CLI, Pi (OpenCode Go models), Grok CLI, Kimi Code |
| API models | OpenAI-compatible endpoints (inbox, assistant, interpretation) |
| Tools | remote MCP servers allowlisted per role; an MCP server exposing Vuzol itself |
| Voice | OpenAI transcription with a project vocabulary |

Provider credentials stay outside the repository, each readable only by the runtime identity
that needs it. See [Providers](docs/PROVIDERS.md).

## Safety model

Responsibilities are split instead of running one all-powerful agent process:

1. **Ingress and delivery** talk to Telegram and never touch repositories.
2. The **worker** and the **executor's lead lanes** own workflow state and decisions; model
   calls run in sandboxes with no host access.
3. The **executor** gives a coding agent only its task worktree, inside a rootless sandbox.
4. The **validator** measures the resulting Git state and runs trusted gates independently.
5. The **applier** advances configured local branches only for an exact approved result.

Agents never push, deploy or get general host access. The stable contracts are in
[Architecture invariants](docs/ARCHITECTURE_INVARIANTS.md).

## Development

Prerequisites: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker with Compose.

```bash
git clone https://github.com/stalker0321/vuzol.git
cd vuzol
uv sync --frozen
cp .env.example .env
make db-up
make db-migrate
make check
```

```bash
make lint           # Ruff
make format-check   # formatting
make type-check     # mypy
make test           # unit and integration tests
make test-postgres  # PostgreSQL migration and concurrency tests
make security       # secret and dependency checks
make check          # the complete local gate
```

Production runs from `deploy/production`: containers in `compose.yaml`, host services as
systemd units, and one `deploy.sh` that installs a commit. See
[Production deployment](deploy/production/README.md) and [Configuration](docs/CONFIGURATION.md).

## Repository map

```text
src/vuzol/          leadership, assistant, inbox, monitor, workflows, execution, providers, Telegram
alembic/            database migrations
config/             non-secret registry examples
deploy/             production compose, systemd units, sandbox, proxy and validation images
docs/               guides, architecture decisions and invariants
tests/              unit and integration suites
```

## Documentation

Start with the [documentation map](docs/README.md). Also: [Telegram](docs/TELEGRAM.md),
[lead memory](docs/LEAD_MEMORY.md), [monitor](docs/MONITOR.md), [storage](docs/STORAGE.md),
[testing](docs/TESTING.md), [architecture decisions](docs/decisions/),
[contributing](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE).
